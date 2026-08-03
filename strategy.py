"""
strategy.py — global multi-symbol scanning engine for the Delta Exchange bot.

Ek hi global "brain" hai jo watchlist ke saare symbols continuously scan
karta hai, DONO directions mein:

    LONG:
        1. ADX(14) >= adx_threshold
        2. ADX rising (optional)
        3. EMA(fast) crosses ABOVE EMA(slow) -- with 1-candle confirmation
        4. RSI between rsi_floor and rsi_overbought
        5. Price > VWAP (UTC-day session)
        6. Volume > volume_multiplier x rolling avg
        7. min EMA separation filter

    SHORT: mirror image

Sabse zyada SCORE wale qualifying symbol pe trade (min_score_threshold ke upar).
max_trades_per_day GLOBAL, max_concurrent_trades hard cap.
Bracket SL+TP entry ke saath (ATR-based ya fixed %, config se).

IMPROVEMENTS (this version, 2026-08-03):
  - tighter RSI band (45-65 long / 35-55 short), ADX 30, min_ema_sep 0.15
  - stop_loss_pct 1.5 / target_pct 3.0 as fixed-% fallback
  - ATR-based SL/TP (use_atr_stops)
  - 2-candle confirmation on EMA cross
  - post-exit cooldown (properly enforced)
  - min_score_threshold
  - leverage-aware qty sizing + HARD max_qty safety cap
  - ATR-distance aware risk sizing when ATR available
  - realistic default capital + clearer sizing logs

SAFETY
  - dry_run True by default
  - Position sizing + PnL contract_value-aware
  - Template only -- investment advice nahi
"""

import os
import time
import json
import logging
import threading
from math import log10, floor
from datetime import datetime, timezone

logger = logging.getLogger("delta-strategy-engine")

TRADE_LOG_FILE = "delta_strategy_trades.json"
PRODUCT_INFO_CACHE_FILE = "delta_product_info.json"

DEFAULT_CONFIG = {
    # entry trigger
    "fast_ema": 9,
    "slow_ema": 21,

    # momentum filter -- LONG
    "rsi_period": 14,
    "rsi_floor": 45,
    "rsi_overbought": 65,

    # momentum filter -- SHORT
    "rsi_short_floor": 35,
    "rsi_short_ceiling": 55,

    # trend filter
    "adx_period": 14,
    "adx_threshold": 30,
    "require_adx_rising": True,

    # avoid microscopic EMA crosses
    "min_ema_separation_pct": 0.15,

    # require 2-candle confirmation on the EMA cross
    "require_cross_confirmation": True,

    # volume filter
    "volume_lookback": 20,
    "volume_multiplier": 2.0,

    # vwap filter
    "vwap_filter": True,

    # position sizing -- risk-based
    # IMPORTANT: isko apne ACTUAL available margin ke close set karo.
    # 50000 rakhne se low-price coins me leverage_limit_exceeded aata tha.
   
    "capital": 2000,
    "risk_pct": 1.0,
    "max_leverage": 3,

    # NEW — sizing safety (flat max_qty hatao ya 0 rakh do)
    "max_qty": 0,                    # 0 = disabled (recommended)
    "max_risk_multiple": 3.0,        # qty=1 pe risk budget se 3x zyada → skip
    "min_notional_usd": 15.0,        # final notional isse kam → skip
    "min_risk_usd": 3.0,             # final risk isse kam → skip

    # --- stop / target ---------------------------------------------------
    "stop_loss_pct": 1.5,
    "target_pct": 3.0,

    # ATR-based stops
    "use_atr_stops": True,
    "atr_period": 14,
    "atr_sl_mult": 1.5,
    "atr_tp_mult": 3.0,

    # fees (Delta India taker ~0.05% each side → ~0.10% round trip)
    "fee_rate_round_trip": 0.0010,

    # direction control
    "allow_long": True,
    "allow_short": True,

    # minimum score to even consider a candidate (0-1 scale)
    "min_score_threshold": 0.55,

    # portfolio-level limits
    "max_trades_per_day": 2,
    "max_concurrent_trades": 1,
    "max_daily_loss": 100,

    "scan_interval_sec": 5,
    "monitor_interval_sec": 10,
    "failed_retry_cooldown_sec": 300,

    # cooldown after ANY exit (win or loss) — prevents immediate re-chase
    "post_exit_cooldown_sec": 900,

    "dry_run_max_hold_sec": 3600,
    "dry_run": True,
}

EDITABLE_CONFIG_KEYS = [
    "fast_ema", "slow_ema", "rsi_period", "rsi_floor", "rsi_overbought",
    "rsi_short_floor", "rsi_short_ceiling",
    "adx_period", "adx_threshold", "require_adx_rising", "min_ema_separation_pct",
    "require_cross_confirmation",
    "volume_lookback", "volume_multiplier",
    "vwap_filter", "capital", "risk_pct", "max_qty", "max_risk_multiple", "min_notional_usd", "min_risk_usd",
    "stop_loss_pct", "target_pct",
    "use_atr_stops", "atr_period", "atr_sl_mult", "atr_tp_mult",
    "max_leverage", "fee_rate_round_trip",
    "allow_long", "allow_short",
    "min_score_threshold",
    "max_trades_per_day", "max_concurrent_trades",
    "max_daily_loss", "scan_interval_sec", "monitor_interval_sec",
    "failed_retry_cooldown_sec", "post_exit_cooldown_sec", "dry_run_max_hold_sec",
]


# ---------------------------------------------------------------------
# Indicators
# ---------------------------------------------------------------------
def ema(values, period):
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def rsi(values, period=14):
    if len(values) < period + 1:
        return [None] * len(values)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains[1:period + 1]) / period
    avg_loss = sum(losses[1:period + 1]) / period
    out = [None] * period
    rs = (avg_gain / avg_loss) if avg_loss else float("inf")
    out.append(100 - (100 / (1 + rs)))
    for i in range(period + 1, len(values)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = (avg_gain / avg_loss) if avg_loss else float("inf")
        out.append(100 - (100 / (1 + rs)))
    return out


def _wilder_smooth(values, period):
    if len(values) < period:
        return [None] * len(values)
    out = [None] * (period - 1)
    out.append(sum(values[:period]))
    for i in range(period, len(values)):
        out.append(out[-1] - (out[-1] / period) + values[i])
    return out


def adx(highs, lows, closes, period=14):
    n = len(highs)
    if n < period * 2:
        return [None] * n

    trs = [highs[0] - lows[0]]
    plus_dm = [0.0]
    minus_dm = [0.0]
    for i in range(1, n):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        ))
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)

    s_tr = _wilder_smooth(trs, period)
    s_plus = _wilder_smooth(plus_dm, period)
    s_minus = _wilder_smooth(minus_dm, period)

    dx = [None] * n
    for i in range(n):
        if s_tr[i] is None or not s_tr[i]:
            continue
        pdi = 100 * (s_plus[i] / s_tr[i])
        mdi = 100 * (s_minus[i] / s_tr[i])
        denom = pdi + mdi
        dx[i] = (100 * abs(pdi - mdi) / denom) if denom else 0.0

    first_valid = next((i for i, d in enumerate(dx) if d is not None), None)
    adx_out = [None] * n
    if first_valid is None or n - first_valid < period:
        return adx_out

    start = first_valid + period - 1
    adx_out[start] = sum(dx[first_valid:first_valid + period]) / period
    for i in range(start + 1, n):
        adx_out[i] = ((adx_out[i - 1] * (period - 1)) + dx[i]) / period
    return adx_out


def atr(highs, lows, closes, period=14):
    """Wilder's ATR — used for volatility-based SL/TP."""
    n = len(highs)
    if n < 2:
        return [None] * n
    trs = [highs[0] - lows[0]]
    for i in range(1, n):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        ))
    if len(trs) < period:
        return [None] * n
    out = [None] * (period - 1)
    out.append(sum(trs[:period]) / period)
    for i in range(period, len(trs)):
        out.append((out[-1] * (period - 1) + trs[i]) / period)
    return out


def _smart_round(price, sig_digits=5):
    if price is None or price == 0:
        return price
    if price >= 1:
        return round(price, 2)
    decimals = sig_digits - int(floor(log10(abs(price)))) - 1
    return round(price, max(decimals, 2))


# ---------------------------------------------------------------------
# product info
# ---------------------------------------------------------------------
def resolve_product_info(client, symbols, use_cache=True):
    cached = {}
    if use_cache and os.path.exists(PRODUCT_INFO_CACHE_FILE):
        try:
            with open(PRODUCT_INFO_CACHE_FILE) as f:
                cached = json.load(f)
        except (OSError, json.JSONDecodeError):
            logger.warning("product info cache unreadable, re-resolving", exc_info=True)
            cached = {}

    missing = [s for s in symbols if s not in cached]
    if not missing:
        return {s: cached[s] for s in symbols}

    resolved = dict(cached)
    try:
        products = client.get_products()
        rows = products.get("result", products) if isinstance(products, dict) else products
        by_symbol = {}
        for row in rows:
            if not isinstance(row, dict) or not row.get("symbol"):
                continue
            try:
                contract_value = float(row.get("contract_value") or 1.0)
            except (TypeError, ValueError):
                contract_value = 1.0

            max_leverage = None
            for lev_key in ("max_leverage", "default_leverage", "leverage"):
                raw_lev = row.get(lev_key)
                if raw_lev is not None:
                    try:
                        max_leverage = float(raw_lev)
                        break
                    except (TypeError, ValueError):
                        pass

            by_symbol[row["symbol"].upper()] = {
                "product_id": row.get("id"),
                "contract_value": contract_value,
                "max_leverage": max_leverage,
            }
        for sym in missing:
            info = by_symbol.get(sym.upper())
            if info and info.get("product_id"):
                resolved[sym] = info
            else:
                logger.warning("Could not resolve product info for %s", sym)
    except Exception:
        logger.error("get_products() failed while resolving product info", exc_info=True)

    try:
        with open(PRODUCT_INFO_CACHE_FILE, "w") as f:
            json.dump(resolved, f, indent=2)
    except OSError:
        logger.warning("Failed to persist product info cache", exc_info=True)

    return {s: resolved[s] for s in symbols if s in resolved}


def _today_vwap(candles):
    today = datetime.now(timezone.utc).date()
    pv, vol = 0.0, 0.0
    for c in candles:
        t = c.get("time")
        try:
            ts = (datetime.fromtimestamp(t, tz=timezone.utc)
                  if isinstance(t, (int, float)) else datetime.fromisoformat(str(t)))
        except Exception:
            continue
        if ts.date() != today:
            continue
        v = c.get("volume") or 0
        pv += c["close"] * v
        vol += v
    return (pv / vol) if vol > 0 else None


def compute_diagnostics(symbol, candles, config):
    need = max(config["slow_ema"], config["rsi_period"], config["adx_period"] * 2) + 3
    empty_conditions = {
        "cross_up": False, "cross_down": False, "trend_aligned": False,
        "adx_ok": False, "adx_rising": False,
        "rsi_long_ok": False, "rsi_short_ok": False,
        "vwap_long_ok": False, "vwap_short_ok": False, "volume_ok": False,
        "ema_sep_ok": False,
    }
    if len(candles) < need:
        return {
            "symbol": symbol,
            "insufficient_data": True,
            "qualifies": False,
            "direction": None,
            "score": 0,
            "price": candles[-1]["close"] if candles else None,
            "adx": None, "rsi": None, "volume_ratio": None, "vwap": None,
            "atr": None,
            "conditions": empty_conditions,
        }

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]

    fast = ema(closes, config["fast_ema"])
    slow = ema(closes, config["slow_ema"])
    rsi_vals = rsi(closes, config["rsi_period"])
    adx_vals = adx(highs, lows, closes, config["adx_period"])
    atr_vals = atr(highs, lows, closes, config.get("atr_period", 14))

    prev_fast, curr_fast = fast[-2], fast[-1]
    prev_slow, curr_slow = slow[-2], slow[-1]
    curr_rsi = rsi_vals[-1]
    curr_adx = adx_vals[-1]
    prev_adx = adx_vals[-2] if len(adx_vals) >= 2 else None
    curr_price = closes[-1]
    curr_atr = atr_vals[-1] if atr_vals else None

    # 2-candle confirmation on the cross
    if config.get("require_cross_confirmation", True) and len(fast) >= 3:
        prior_fast, prior_slow = fast[-3], slow[-3]
        crossed_up_1_ago = prior_fast <= prior_slow and prev_fast > prev_slow
        crossed_down_1_ago = prior_fast >= prior_slow and prev_fast < prev_slow
        fresh_cross_up = crossed_up_1_ago and curr_fast > curr_slow
        fresh_cross_down = crossed_down_1_ago and curr_fast < curr_slow
    else:
        fresh_cross_up = prev_fast <= prev_slow and curr_fast > curr_slow
        fresh_cross_down = prev_fast >= prev_slow and curr_fast < curr_slow

    trend_aligned = curr_fast > curr_slow

    trend_ok = curr_adx is not None and curr_adx >= config["adx_threshold"]
    adx_rising = (
        curr_adx is not None and prev_adx is not None and curr_adx >= prev_adx
    )
    adx_rising_ok = (not config.get("require_adx_rising", True)) or adx_rising

    min_sep = float(config.get("min_ema_separation_pct", 0) or 0)
    if min_sep > 0 and curr_price:
        sep_pct = abs(curr_fast - curr_slow) / curr_price * 100.0
        ema_sep_ok = sep_pct >= min_sep
    else:
        ema_sep_ok = True

    rsi_long_ok = curr_rsi is not None and (
        config["rsi_floor"] <= curr_rsi < config["rsi_overbought"]
    )
    rsi_short_ok = curr_rsi is not None and (
        config["rsi_short_floor"] < curr_rsi <= config["rsi_short_ceiling"]
    )

    vwap = _today_vwap(candles) if config["vwap_filter"] else None
    vwap_long_ok = (not config["vwap_filter"]) or vwap is None or curr_price > vwap
    vwap_short_ok = (not config["vwap_filter"]) or vwap is None or curr_price < vwap

    lookback = config["volume_lookback"]
    recent_vols = [v for v in volumes[-(lookback + 1):-1] if v]
    avg_vol = (sum(recent_vols) / len(recent_vols)) if recent_vols else 0
    volume_ratio = (volumes[-1] / avg_vol) if avg_vol else 1.0
    volume_ok = (not recent_vols) or volume_ratio >= config["volume_multiplier"]

    long_qualifies = bool(
        config.get("allow_long", True)
        and fresh_cross_up and trend_ok and adx_rising_ok and ema_sep_ok
        and rsi_long_ok and vwap_long_ok and volume_ok
    )
    short_qualifies = bool(
        config.get("allow_short", True)
        and fresh_cross_down and trend_ok and adx_rising_ok and ema_sep_ok
        and rsi_short_ok and vwap_short_ok and volume_ok
    )

    if long_qualifies:
        direction = "long"
    elif short_qualifies:
        direction = "short"
    else:
        direction = None

    qualifies = direction is not None

    score = 0.0
    if curr_adx is not None and curr_rsi is not None:
        adx_score = min(curr_adx / 50.0, 1.0)
        vol_score = min(volume_ratio / 3.0, 1.0)
        if direction == "short":
            rsi_mid = (config["rsi_short_floor"] + config["rsi_short_ceiling"]) / 2
            rsi_span = max(config["rsi_short_ceiling"] - rsi_mid, 1)
        elif direction == "long":
            rsi_mid = (config["rsi_floor"] + config["rsi_overbought"]) / 2
            rsi_span = max(rsi_mid - config["rsi_floor"], 1)
        else:
            rsi_mid, rsi_span = 50.0, 50.0
        rsi_score = 1.0 - min(abs(curr_rsi - rsi_mid) / rsi_span, 1.0)

        extension_pen = 0.0
        if direction == "long" and curr_rsi is not None and curr_rsi > 65:
            extension_pen = min((curr_rsi - 65) / 35.0, 0.25)
        elif direction == "short" and curr_rsi is not None and curr_rsi < 35:
            extension_pen = min((35 - curr_rsi) / 35.0, 0.25)

        raw = adx_score * 0.45 + vol_score * 0.30 + rsi_score * 0.25
        score = round(max(raw - extension_pen, 0.0), 4)

    return {
        "symbol": symbol,
        "insufficient_data": False,
        "price": curr_price,
        "adx": round(curr_adx, 2) if curr_adx is not None else None,
        "rsi": round(curr_rsi, 2) if curr_rsi is not None else None,
        "volume_ratio": round(volume_ratio, 2),
        "vwap": _smart_round(vwap) if vwap else None,
        "atr": round(curr_atr, 6) if curr_atr is not None else None,
        "conditions": {
            "cross_up": fresh_cross_up,
            "cross_down": fresh_cross_down,
            "trend_aligned": trend_aligned,
            "adx_ok": trend_ok,
            "adx_rising": adx_rising,
            "rsi_long_ok": rsi_long_ok,
            "rsi_short_ok": rsi_short_ok,
            "vwap_long_ok": vwap_long_ok,
            "vwap_short_ok": vwap_short_ok,
            "volume_ok": volume_ok,
            "ema_sep_ok": ema_sep_ok,
        },
        "qualifies": qualifies,
        "direction": direction,
        "score": score,
    }


# ---------------------------------------------------------------------
# StrategyManager
# ---------------------------------------------------------------------
class StrategyManager:
    def __init__(self, feed, client, watchlist, config=None, place_order_fn=None):
        self.feed = feed
        self.client = client
        self.symbols = list(watchlist)
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.place_order_fn = place_order_fn

        self.product_info = resolve_product_info(client, self.symbols)

        self.running = False
        self.scan_thread = None
        self.monitor_thread = None
        self.lock = threading.Lock()

        self.open_trades = {}
        self.failed_symbols = {}          # entry failures
        self.exit_cooldown = {}           # post-exit cooldowns (separate)
        self.trades_today = 0
        self.realized_pnl_today = 0.0
        self.day_marker = datetime.now(timezone.utc).date()
        self.last_scan = {}
        self.trade_log = []

    # -- lifecycle -----------------------------------------------------
    def start(self, symbol=None):
        with self.lock:
            if self.running:
                return
            self.running = True

        self._sync_open_positions()

        self.scan_thread = threading.Thread(target=self._scan_loop, daemon=True)
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.scan_thread.start()
        self.monitor_thread.start()
        logger.info("Strategy scanner started over %d symbols", len(self.symbols))

    def stop(self, symbol=None):
        with self.lock:
            self.running = False
        logger.info("Strategy scanner stopped (open positions exchange par as-is rahengi)")

    def add_symbols(self, symbols):
        new_syms = [s for s in symbols if s not in self.symbols]
        if not new_syms:
            return
        self.symbols.extend(new_syms)
        info = resolve_product_info(self.client, new_syms)
        self.product_info.update(info)

    def update_config(self, patch):
        if self.running:
            raise ValueError("Config change karne se pehle scanner STOP karo")
        clean_patch = {k: v for k, v in patch.items() if k in EDITABLE_CONFIG_KEYS}
        self.config.update(clean_patch)
        return self.config



def status(self):
    with self.lock:
        return {
            "running": self.running,
            "symbols": self.symbols,
            "watchlist_count": len(self.symbols),
            "open_trades": len(self.open_trades),
            "trades_today": self.trades_today,
            "realized_pnl_today": self.realized_pnl_today,
            "config": self.config,
        }
    def _roll_day_if_needed(self):
        today = datetime.now(timezone.utc).date()
        if today != self.day_marker:
            self.day_marker = today
            self.trades_today = 0
            self.realized_pnl_today = 0.0

    def _prune_cooldowns(self):
        """Prune both failed-entry and post-exit cooldowns."""
        now = time.time()
        fail_cd = self.config.get("failed_retry_cooldown_sec", 300)
        exit_cd = self.config.get("post_exit_cooldown_sec", 900)
        with self.lock:
            self.failed_symbols = {
                s: t for s, t in self.failed_symbols.items() if now - t < fail_cd
            }
            self.exit_cooldown = {
                s: t for s, t in self.exit_cooldown.items() if now - t < exit_cd
            }

    # -- position recovery ---------------------------------------------
    def _sync_open_positions(self):
        recovered = 0
        for symbol, info in list(self.product_info.items()):
            pid = info.get("product_id")
            if not pid:
                continue
            try:
                position = self.client.get_position(pid)
                pos = position.get("result", position) if isinstance(position, dict) else position
                if not isinstance(pos, dict):
                    continue
                raw_size = pos.get("size", 0)
                try:
                    size = float(raw_size) if raw_size is not None else 0.0
                except (TypeError, ValueError):
                    size = 0.0
                if size == 0:
                    continue

                direction = "long" if size > 0 else "short"
                side = "buy" if direction == "long" else "sell"
                qty = max(abs(int(size)), 1)

                entry_price = None
                for k in ("entry_price", "average_entry_price", "avg_entry_price",
                          "open_price", "average_open_price"):
                    if pos.get(k) is not None:
                        try:
                            entry_price = float(pos[k])
                            break
                        except (TypeError, ValueError):
                            pass
                if entry_price is None:
                    candles = self.feed.get_candles(symbol, limit=2)
                    if candles:
                        entry_price = candles[-1]["close"]
                if entry_price is None:
                    logger.warning(
                        "Recovered position for %s but no entry price — skipping track",
                        symbol,
                    )
                    continue

                contract_value = float(info.get("contract_value", 1.0) or 1.0)

                with self.lock:
                    if symbol in self.open_trades:
                        continue
                    self.open_trades[symbol] = {
                        "direction": direction,
                        "side": side,
                        "product_id": pid,
                        "contract_value": contract_value,
                        "entry_price": entry_price,
                        "qty": qty,
                        "sl_price": None,
                        "tp_price": None,
                        "entry_time": datetime.now(timezone.utc).isoformat(),
                        "entry_ts": time.time(),
                        "score": None,
                        "order_result": {"recovered": True},
                        "recovered": True,
                    }
                recovered += 1
                logger.info(
                    "RECOVERED open position %s [%s] qty=%s entry≈%s "
                    "(no bracket — monitor waits for flat)",
                    symbol, direction, qty, entry_price,
                )
            except Exception as e:
                logger.warning("Could not sync position for %s: %s", symbol, e)

        if recovered:
            logger.info("Synced %d existing position(s) from exchange", recovered)

    # -- scanning ------------------------------------------------------
    def _scan_loop(self):
        while self.running:
            try:
                self._roll_day_if_needed()
                self._prune_cooldowns()
                self._scan_all_symbols()

                with self.lock:
                    n_open = len(self.open_trades)
                    trades_today = self.trades_today
                    realized = self.realized_pnl_today
                slots_free = n_open < self.config["max_concurrent_trades"]
                trades_left = trades_today < self.config["max_trades_per_day"]
                loss_ok = realized > -abs(self.config["max_daily_loss"])
                if slots_free and trades_left and loss_ok:
                    self._maybe_enter_best_candidate()
            except Exception:
                logger.error("Scan loop error", exc_info=True)
            time.sleep(self.config["scan_interval_sec"])

    def _scan_all_symbols(self):
        for symbol in list(self.symbols):
            info = self.product_info.get(symbol)
            if not info or not info.get("product_id"):
                continue
            candles = self.feed.get_candles(symbol, limit=300)
            if not candles:
                continue
            diag = compute_diagnostics(symbol, candles, self.config)
            with self.lock:
                self.last_scan[symbol] = diag

    def _maybe_enter_best_candidate(self):
        now = time.time()
        min_score = self.config.get("min_score_threshold", 0.0)
        fail_cd = self.config.get("failed_retry_cooldown_sec", 300)
        exit_cd = self.config.get("post_exit_cooldown_sec", 900)

        with self.lock:
            open_symbols = set(self.open_trades.keys())
            cooling_failed = {
                s for s, t in self.failed_symbols.items() if now - t < fail_cd
            }
            cooling_exit = {
                s for s, t in self.exit_cooldown.items() if now - t < exit_cd
            }
            scan_snapshot = dict(self.last_scan)

        candidates = [
            diag for symbol, diag in scan_snapshot.items()
            if diag.get("qualifies")
            and diag.get("score", 0) >= min_score
            and symbol not in open_symbols
            and symbol not in cooling_failed
            and symbol not in cooling_exit
        ]
        if not candidates:
            return

        best = max(candidates, key=lambda s: s["score"])
        self._enter_trade(best)

    # -- entry ---------------------------------------------------------
    def _extract_fill_price(self, order_result, fallback):
        if not isinstance(order_result, dict):
            return fallback

        candidates = [order_result]
        for nest_key in ("result", "order", "data"):
            nested = order_result.get(nest_key)
            if isinstance(nested, dict):
                candidates.append(nested)

        for obj in candidates:
            for key in (
                "average_fill_price", "average_price", "avg_fill_price",
                "fill_price", "avg_price", "price",
            ):
                val = obj.get(key)
                if val is not None:
                    try:
                        p = float(val)
                        if p > 0:
                            return p
                    except (TypeError, ValueError):
                        pass
        return fallback

    def _order_looks_rejected(self, order_result):
        if not isinstance(order_result, dict):
            return False
        if order_result.get("error"):
            return True
        status = str(
            order_result.get("status")
            or order_result.get("state")
            or (order_result.get("result") or {}).get("status")
            or (order_result.get("result") or {}).get("state")
            or ""
        ).lower()
        return status in ("rejected", "cancelled", "canceled", "failed", "expired")

    def _compute_sl_tp(self, direction, entry_price, atr_val):
        use_atr = self.config.get("use_atr_stops", False) and atr_val
        if use_atr:
            sl_mult = self.config.get("atr_sl_mult", 1.5)
            tp_mult = self.config.get("atr_tp_mult", 3.0)
            if direction == "long":
                sl_price = _smart_round(entry_price - atr_val * sl_mult)
                tp_price = _smart_round(entry_price + atr_val * tp_mult)
            else:
                sl_price = _smart_round(entry_price + atr_val * sl_mult)
                tp_price = _smart_round(entry_price - atr_val * tp_mult)
            return sl_price, tp_price

        if direction == "long":
            sl_price = _smart_round(entry_price * (1 - self.config["stop_loss_pct"] / 100))
            tp_price = _smart_round(entry_price * (1 + self.config["target_pct"] / 100))
        else:
            sl_price = _smart_round(entry_price * (1 + self.config["stop_loss_pct"] / 100))
            tp_price = _smart_round(entry_price * (1 - self.config["target_pct"] / 100))
        return sl_price, tp_price
def _enter_trade(self, sig):
    symbol = sig["symbol"]
    direction = sig["direction"]
    side = "buy" if direction == "long" else "sell"

    info = self.product_info[symbol]
    product_id = info["product_id"]
    contract_value = float(info.get("contract_value", 1.0) or 1.0)
    entry_price = sig["price"]
    atr_val = sig.get("atr")

    # ----- risk distance (prefer ATR if available) -----
    if self.config.get("use_atr_stops") and atr_val and atr_val > 0:
        sl_distance = atr_val * self.config.get("atr_sl_mult", 1.5)
    else:
        sl_distance = entry_price * (self.config["stop_loss_pct"] / 100)

    risk_budget = self.config["capital"] * (self.config["risk_pct"] / 100)  # e.g. 2000 * 0.01 = 20
    loss_per_contract = sl_distance * contract_value
    notional_per_contract = entry_price * contract_value

    if loss_per_contract <= 0 or notional_per_contract <= 0:
        logger.warning("%s invalid loss/notional per contract — skip", symbol)
        with self.lock:
            self.failed_symbols[symbol] = time.time()
        return

    # Ideal qty from risk budget
    qty = max(int(risk_budget // loss_per_contract), 1)

    # ----- leverage clamps (bot + exchange) -----
    max_lev_bot = self.config.get("max_leverage", 3)
    max_notional_bot = self.config["capital"] * max_lev_bot

    product_max_lev = info.get("max_leverage")
    max_notional_prod = None
    if product_max_lev and product_max_lev > 0:
        max_notional_prod = self.config["capital"] * product_max_lev

    # Apply the tighter of the two notional caps
    max_notional = max_notional_bot
    if max_notional_prod is not None:
        max_notional = min(max_notional, max_notional_prod)

    if qty * notional_per_contract > max_notional:
        qty = max(int(max_notional // notional_per_contract), 1)

    # ============================================================
    # SAFETY CHECKS — yeh do bugs fix karte hain
    # ============================================================

    actual_risk = qty * loss_per_contract
    actual_notional = qty * notional_per_contract

    # Bug 2 fix: even qty=1 exceeds risk budget by a large multiple
    # (BTC / high-price case). Sending this order will get rejected
    # or risk way more than intended. Skip entirely.
    max_risk_multiple = float(self.config.get("max_risk_multiple", 3.0))  # allow up to 3x budget
    if actual_risk > risk_budget * max_risk_multiple:
        logger.warning(
            "%s SKIP — even qty=%s risk≈%.2f is > %.1fx budget %.2f "
            "(price=%.4f cv=%.6f). Symbol too expensive for current capital.",
            symbol, qty, actual_risk, max_risk_multiple, risk_budget,
            entry_price, contract_value,
        )
        with self.lock:
            self.failed_symbols[symbol] = time.time()
        self._log_trade(
            "ENTRY_FAILED", symbol, entry_price, qty, sig,
            {"error": f"risk_too_large: {actual_risk:.2f} > {risk_budget * max_risk_multiple:.2f}"},
        )
        return

    # Bug 1 fix: final trade is so small that it wastes a daily slot
    # (micro-price / tiny contract-value case).
    min_notional_usd = float(self.config.get("min_notional_usd", 15.0))
    min_risk_usd = float(self.config.get("min_risk_usd", 3.0))

    if actual_notional < min_notional_usd or actual_risk < min_risk_usd:
        logger.warning(
            "%s SKIP — trade too small (notional≈%.2f risk≈%.2f). "
            "min_notional=%.1f min_risk=%.1f. Slot waste avoid.",
            symbol, actual_notional, actual_risk, min_notional_usd, min_risk_usd,
        )
        with self.lock:
            self.failed_symbols[symbol] = time.time()
        self._log_trade(
            "ENTRY_FAILED", symbol, entry_price, qty, sig,
            {"error": f"trade_too_small: notional={actual_notional:.2f} risk={actual_risk:.2f}"},
        )
        return

    # Optional soft upper cap (only as last resort, not the main protection)
    soft_max_qty = int(self.config.get("max_qty", 0) or 0)
    if soft_max_qty > 0 and qty > soft_max_qty:
        logger.info("%s soft max_qty clamp %s → %s", symbol, qty, soft_max_qty)
        qty = soft_max_qty
        # re-check after soft clamp
        actual_risk = qty * loss_per_contract
        actual_notional = qty * notional_per_contract
        if actual_notional < min_notional_usd or actual_risk < min_risk_usd:
            logger.warning("%s SKIP after soft clamp — still too small", symbol)
            with self.lock:
                self.failed_symbols[symbol] = time.time()
            return

    # ----- SL / TP -----
    sl_price, tp_price = self._compute_sl_tp(direction, entry_price, atr_val)

    order_result = self._place_bracket_entry(product_id, qty, side, sl_price, tp_price)

    if isinstance(order_result, dict) and (
        order_result.get("error") or self._order_looks_rejected(order_result)
    ):
        with self.lock:
            self.failed_symbols[symbol] = time.time()
        self._log_trade("ENTRY_FAILED", symbol, entry_price, qty, sig, order_result)
        logger.warning(
            "ENTRY FAILED %s (%s) qty=%s: %s",
            symbol, direction, qty,
            order_result.get("error") or order_result.get("status") or order_result,
        )
        return

    if not self.config.get("dry_run", True):
        fill = self._extract_fill_price(order_result, entry_price)
        if fill != entry_price:
            logger.info(
                "Fill price %.6f differs from signal %.6f for %s — adjusting SL/TP",
                fill, entry_price, symbol,
            )
            entry_price = fill
            sl_price, tp_price = self._compute_sl_tp(direction, entry_price, atr_val)

    with self.lock:
        self.open_trades[symbol] = {
            "direction": direction,
            "side": side,
            "product_id": product_id,
            "contract_value": contract_value,
            "entry_price": entry_price,
            "qty": qty,
            "sl_price": sl_price,
            "tp_price": tp_price,
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "entry_ts": time.time(),
            "score": sig["score"],
            "order_result": order_result,
            "recovered": False,
        }
        self.trades_today += 1
        self.failed_symbols.pop(symbol, None)

    self._log_trade("ENTRY", symbol, entry_price, qty, sig, order_result)
    logger.info(
        "ENTRY %s [%s] qty=%s cv=%.6f score=%.3f sl=%s tp=%s "
        "risk≈%.2f notional≈%.2f",
        symbol, direction, qty, contract_value, sig["score"],
        sl_price, tp_price, actual_risk, actual_notional,
    )
    def _place_bracket_entry(self, product_id, qty, side, sl_price, tp_price):
        if self.config.get("dry_run", True):
            return {
                "dry_run": True,
                "product_id": product_id,
                "size": qty,
                "side": side,
                "bracket_stop_loss_price": str(sl_price),
                "bracket_take_profit_price": str(tp_price),
            }

        if self.place_order_fn is None:
            err = "place_order_fn not configured on StrategyManager"
            logger.error(err)
            return {"error": err}

        order_body = {
            "product_id": product_id,
            "size": qty,
            "side": side,
            "order_type": "market_order",
            "bracket_stop_loss_price": str(sl_price),
            "bracket_take_profit_price": str(tp_price),
            "bracket_stop_trigger_method": "last_traded_price",
        }
        try:
            return self.place_order_fn(order_body)
        except Exception as e:
            logger.error("Bracket entry failed: %s", e, exc_info=True)
            return {"error": str(e)}

    # -- monitoring / exit ---------------------------------------------
    def _monitor_loop(self):
        while self.running:
            try:
                with self.lock:
                    symbols = list(self.open_trades.keys())
                for symbol in symbols:
                    if self.config.get("dry_run", True):
                        self._check_dry_run_exit(symbol)
                    else:
                        self._check_position_closed(symbol)
            except Exception:
                logger.error("Monitor loop error", exc_info=True)
            time.sleep(self.config["monitor_interval_sec"])

    def _pnl_for_trade(self, trade, exit_price):
        """Gross price PnL minus estimated round-trip fees."""
        cv = float(trade.get("contract_value", 1.0) or 1.0)
        qty = trade["qty"]
        entry = trade["entry_price"]
        if trade.get("direction") == "short":
            gross = (entry - exit_price) * qty * cv
        else:
            gross = (exit_price - entry) * qty * cv

        fee_rate = float(self.config.get("fee_rate_round_trip", 0.001) or 0.0)
        notional = entry * qty * cv
        fees = notional * fee_rate
        return gross - fees

    def _close_trade(self, symbol, trade, exit_price, note):
        pnl = self._pnl_for_trade(trade, exit_price)
        with self.lock:
            self.realized_pnl_today += pnl
            self.open_trades.pop(symbol, None)
            # Proper post-exit cooldown (separate from failed_symbols)
            self.exit_cooldown[symbol] = time.time()

        self._log_trade(
            "EXIT", symbol, exit_price, trade["qty"],
            {"direction": trade.get("direction"), "score": trade.get("score")},
            {"note": note},
            pnl=pnl,
        )
        logger.info(
            "EXIT %s [%s] %s pnl=%.4f",
            symbol, trade.get("direction"), note, pnl,
        )

    def _check_dry_run_exit(self, symbol):
        with self.lock:
            trade = self.open_trades.get(symbol)
            if not trade:
                return
            trade = dict(trade)

        candles = self.feed.get_candles(symbol, limit=5)
        if not candles:
            return
        price = candles[-1]["close"]
        direction = trade.get("direction", "long")
        sl, tp = trade.get("sl_price"), trade.get("tp_price")

        hit = None
        if sl is not None and tp is not None:
            if direction == "long":
                if price <= sl:
                    hit = ("SL", sl)
                elif price >= tp:
                    hit = ("TP", tp)
            else:
                if price >= sl:
                    hit = ("SL", sl)
                elif price <= tp:
                    hit = ("TP", tp)

        if hit is None:
            max_hold = self.config.get("dry_run_max_hold_sec", 3600)
            entry_ts = trade.get("entry_ts") or 0
            if max_hold and entry_ts and (time.time() - entry_ts) >= max_hold:
                hit = ("MAX_HOLD", price)

        if hit:
            self._close_trade(symbol, trade, hit[1], f"dry_run {hit[0]}")

    def _check_position_closed(self, symbol):
        with self.lock:
            trade = self.open_trades.get(symbol)
            if not trade:
                return
            trade = dict(trade)

        try:
            position = self.client.get_position(trade["product_id"])
            pos_result = (
                position.get("result", position)
                if isinstance(position, dict) else position
            )
            size = pos_result.get("size", 0) if isinstance(pos_result, dict) else 0
            try:
                size = float(size) if size is not None else 0.0
            except (TypeError, ValueError):
                size = 0.0

            mark = None
            if isinstance(pos_result, dict):
                for key in ("mark_price", "close", "last"):
                    if pos_result.get(key) is not None:
                        try:
                            mark = float(pos_result[key])
                            break
                        except (TypeError, ValueError):
                            pass
        except Exception as e:
            logger.warning("Could not fetch position for %s: %s", symbol, e)
            return

        if size != 0:
            return

        exit_price = mark
        if exit_price is None:
            candles = self.feed.get_candles(symbol, limit=3)
            if candles:
                exit_price = candles[-1]["close"]
        if exit_price is None:
            exit_price = (
                trade.get("tp_price")
                or trade.get("sl_price")
                or trade["entry_price"]
            )
        else:
            direction = trade.get("direction", "long")
            sl, tp = trade.get("sl_price"), trade.get("tp_price")
            if sl is not None and tp is not None:
                if direction == "long":
                    if exit_price <= sl:
                        exit_price = sl
                    elif exit_price >= tp:
                        exit_price = tp
                else:
                    if exit_price >= sl:
                        exit_price = sl
                    elif exit_price <= tp:
                        exit_price = tp

        note = (
            "recovered position closed"
            if trade.get("recovered")
            else "bracket closed"
        )
        self._close_trade(symbol, trade, exit_price, note)

    # -- logging / status ----------------------------------------------
    def _log_trade(self, action, symbol, price, qty, sig, order_result, pnl=None):
        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "symbol": symbol,
            "price": price,
            "qty": qty,
            "direction": sig.get("direction") if sig else None,
            "score": sig.get("score") if sig else None,
            "pnl": pnl,
            "order_result": order_result,
            "dry_run": self.config.get("dry_run", True),
        }
        self.trade_log.append(entry)
        self.trade_log = self.trade_log[-200:]
        try:
            existing = []
            if os.path.exists(TRADE_LOG_FILE):
                with open(TRADE_LOG_FILE) as f:
                    existing = json.load(f)
            existing.append(entry)
            with open(TRADE_LOG_FILE, "w") as f:
                json.dump(existing[-500:], f, indent=2, default=str)
        except (OSError, json.JSONDecodeError):
            logger.warning("Failed to persist trade log", exc_info=True)

    def status(self, symbol=None):
        with self.lock:
            open_trades = dict(self.open_trades)
            failed_symbols = dict(self.failed_symbols)
            exit_cooldown = dict(self.exit_cooldown)
            scan_grid = list(self.last_scan.values())
            trades_today = self.trades_today
            realized = self.realized_pnl_today
            running = self.running

        scan_grid.sort(
            key=lambda s: (not s.get("qualifies", False), -(s.get("score") or 0))
        )

        return {
            "running": running,
            "config": self.config,
            "trades_today": trades_today,
            "max_trades_per_day": self.config["max_trades_per_day"],
            "open_trades": open_trades,
            "max_concurrent_trades": self.config["max_concurrent_trades"],
            "realized_pnl_today": round(realized, 4),
            "failed_symbols_cooldown": failed_symbols,
            "exit_cooldown": exit_cooldown,
            "candidates": sorted(
                [s for s in scan_grid if s.get("qualifies")],
                key=lambda s: -s["score"],
            )[:10],
            "scan_grid": scan_grid,
            "total_symbols": len(self.symbols),
            "recent_trades": list(reversed(self.trade_log[-20:])),
        }