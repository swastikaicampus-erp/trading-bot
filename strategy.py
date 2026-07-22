"""
strategy.py — global multi-symbol scanning engine for the Delta Exchange bot.

Ek hi global "brain" hai jo watchlist ke saare symbols (ab: SAARE live
perpetual futures) continuously scan karta hai, DONO directions mein:

    LONG (buy) setup:
        1. ADX(14) >= adx_threshold -> trending, not choppy
        2. EMA(fast) crosses ABOVE EMA(slow) -> bullish entry trigger
        3. RSI(14) between rsi_floor and rsi_overbought -> bullish momentum
        4. Price > VWAP (session VWAP, UTC-day)
        5. Candle volume > volume_multiplier x rolling average volume

    SHORT (sell) setup -- bilkul mirror image:
        1. ADX(14) >= adx_threshold -> trending, not choppy
        2. EMA(fast) crosses BELOW EMA(slow) -> bearish entry trigger
        3. RSI(14) between rsi_short_floor and rsi_short_ceiling -> bearish momentum
        4. Price < VWAP (session VWAP, UTC-day)
        5. Candle volume > volume_multiplier x rolling average volume

Jitne bhi symbols in saari conditions ko (kisi bhi ek direction mein)
pass karte hain unme se SABSE ZYADA SCORE wale symbol me hi trade li
jaati hai -- baaki qualifying symbols sirf "candidates" list me dikhte
hain, trade nahi hoti jab tak unka number na aaye.

HAR symbol (chahe qualify kare ya na kare) ka diagnostic snapshot
`last_scan` me store hota hai -- taaki frontend par poora grid dikhaya
ja sake (kaunsi condition pass hui, kaunsi fail, live score, direction)
na ki sirf jo abhi qualify kar rahe hain.

Rules jo implement kiye hain:
  - Din bhar me max `max_trades_per_day` trades total -- GLOBAL,
    sabhi symbols aur dono directions milakar (per-symbol nahi).
  - Normally sirf 1 trade open rehta hai ek time pe.
  - System max `max_concurrent_trades` (default 2) trades EK SAATH
    chala sakta hai -- ye hard cap hai, norm nahi. Long aur short dono
    ek saath alag symbols par khul sakte hain is limit ke andar.
  - Jab koi open trade close ho (SL ya target hit -- exchange khud
    karta hai bracket order ke through), tabhi agla best-scoring
    candidate liya jaata hai.
  - Entry ke saath hi SL + target ek hi bracket order call me attach
    hote hain, taaki exchange khud exit manage kare -- humein sirf
    ye poll karna hai ki position abhi bhi open hai ya band ho gayi.

BUGFIXES (is version me):
  - Low-price coins (e.g. RSRUSD ~$0.0003) ke liye smart/dynamic decimal
    rounding -- pehle round(x, 2) se SL/TP 0.00 ban jaate the, Delta
    reject kar deta tha.
  - Qty calculation par notional-based sanity cap -- pehle tiny price se
    divide karne par astronomically bada qty ban jaata tha.
  - Order fail hone par (ya error aane par) trade `open_trades` me
    COMMIT NAHI hoti -- pehle phantom entry ban jaati thi jo slot block
    kar deti thi bina real position ke. Fail hone par symbol short
    cooldown (`failed_retry_cooldown_sec`) me chala jaata hai taaki
    turant wahi symbol dobara try na ho.
  - NAYA: SHORT (sell) side add kiya gaya -- pehle sirf LONG (buy) hoti
    thi. Ab bearish EMA cross + RSI + VWAP + volume conditions bhi
    scan hoti hain, aur SL/TP/side sab direction-aware hain.

ASSUMPTION -- score formula (config se tune ho sakta hai):
    score = 0.45*adx_strength + 0.30*volume_surge + 0.25*rsi_position
    Higher = zyada strong trend + zyada conviction (dono directions me).

SAFETY
    - `dry_run` True rehta hai jab tak explicitly False na kiya jaaye.
    - Position sizing aur PnL contract_value-aware hain (Delta pe 1
      "contract" 1 unit underlying nahi hota).
    - Ye ek template hai, investment advice nahi.
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

    # momentum filter -- LONG side
    "rsi_period": 14,
    "rsi_floor": 40,
    "rsi_overbought": 75,

    # momentum filter -- SHORT side (mirror of long; tune independently
    # if you want asymmetric bearish sensitivity)
    "rsi_short_floor": 25,
    "rsi_short_ceiling": 60,

    # trend filter
    "adx_period": 14,
    "adx_threshold": 20,

    # volume filter
    "volume_lookback": 20,
    "volume_multiplier": 1.2,

    # vwap filter
    "vwap_filter": True,

    # position sizing -- risk-based
    "capital": 50000,          # quote currency (USD)
    "risk_pct": 1.0,
    "stop_loss_pct": 0.5,
    "target_pct": 1.0,
    "max_leverage": 3,         # notional sanity cap = capital * max_leverage

    # direction control
    "allow_long": True,
    "allow_short": True,

    # portfolio-level limits (GLOBAL, symbol-wise nahi, direction-wise nahi)
    "max_trades_per_day": 4,
    "max_concurrent_trades": 2,
    "max_daily_loss": 1000,    # USD -- circuit breaker

    "scan_interval_sec": 5,
    "monitor_interval_sec": 10,
    "failed_retry_cooldown_sec": 300,   # order-fail hone par retry se pehle wait

    "dry_run": True,           # SAFETY DEFAULT
}

# Config keys jo frontend se edit karwane layak hain (baaki internal-only)
EDITABLE_CONFIG_KEYS = [
    "fast_ema", "slow_ema", "rsi_period", "rsi_floor", "rsi_overbought",
    "rsi_short_floor", "rsi_short_ceiling",
    "adx_period", "adx_threshold", "volume_lookback", "volume_multiplier",
    "vwap_filter", "capital", "risk_pct", "stop_loss_pct", "target_pct",
    "max_leverage", "allow_long", "allow_short",
    "max_trades_per_day", "max_concurrent_trades",
    "max_daily_loss", "scan_interval_sec", "monitor_interval_sec",
    "failed_retry_cooldown_sec",
]


# ---------------------------------------------------------------------
# Indicators -- dependency-light, same as Kotak Neo version
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


def _smart_round(price, sig_digits=5):
    """Low-price coins (RSRUSD ~$0.0003) ke liye 0.00 na bane -- price
    magnitude ke hisaab se decimal places adjust karta hai. Normal
    price (>= 1) ke liye simple 2-decimal rounding hi rehti hai."""
    if price is None or price == 0:
        return price
    if price >= 1:
        return round(price, 2)
    decimals = sig_digits - int(floor(log10(abs(price)))) - 1
    return round(price, max(decimals, 2))


# ---------------------------------------------------------------------
# product info resolution -- symbol -> {product_id, contract_value}
# ---------------------------------------------------------------------
def resolve_product_info(client, symbols, use_cache=True):
    """
    Resolves symbol -> {"product_id": int, "contract_value": float} by
    calling client.get_products() once and caching to disk.

    contract_value matters because on Delta, 1 "contract" is NOT 1 unit
    of the underlying asset. Position sizing and PnL must be multiplied
    by this or both will be wrong by orders of magnitude.
    """
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
            by_symbol[row["symbol"].upper()] = {
                "product_id": row.get("id"),
                "contract_value": contract_value,
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
    """Aaj (UTC) ke candles se hi VWAP nikalta hai -- stateless."""
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
    """
    Poora diagnostic snapshot return karta hai -- HAR symbol ke liye,
    chahe strategy ki conditions match ho ya na ho (kisi bhi direction
    me). Ye frontend ke "scan grid" (saare stocks + har condition ka
    ✓/✗) ka data source hai.

    'qualifies' == True hone par hi ye symbol trade ka candidate banta
    hai (sabse zyada score wale candidate ko hi actual trade milti hai).
    'direction' batata hai kaunsa side qualify hua -- "long", "short",
    ya None agar dono me se koi nahi.
    """
    need = max(config["slow_ema"], config["rsi_period"], config["adx_period"] * 2) + 2
    if len(candles) < need:
        return {
            "symbol": symbol,
            "insufficient_data": True,
            "qualifies": False,
            "direction": None,
            "score": 0,
            "price": candles[-1]["close"] if candles else None,
            "adx": None, "rsi": None, "volume_ratio": None, "vwap": None,
            "conditions": {
                "cross_up": False, "cross_down": False, "trend_aligned": False,
                "adx_ok": False, "rsi_long_ok": False, "rsi_short_ok": False,
                "vwap_long_ok": False, "vwap_short_ok": False, "volume_ok": False,
            },
        }

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]

    fast = ema(closes, config["fast_ema"])
    slow = ema(closes, config["slow_ema"])
    rsi_vals = rsi(closes, config["rsi_period"])
    adx_vals = adx(highs, lows, closes, config["adx_period"])

    prev_fast, curr_fast = fast[-2], fast[-1]
    prev_slow, curr_slow = slow[-2], slow[-1]
    curr_rsi = rsi_vals[-1]
    curr_adx = adx_vals[-1]
    curr_price = closes[-1]

    # -- direction triggers ------------------------------------------------
    fresh_cross_up = prev_fast <= prev_slow and curr_fast > curr_slow      # bullish
    fresh_cross_down = prev_fast >= prev_slow and curr_fast < curr_slow    # bearish
    trend_aligned = curr_fast > curr_slow  # display only -- long-biased alignment

    trend_ok = curr_adx is not None and curr_adx >= config["adx_threshold"]

    rsi_long_ok = curr_rsi is not None and (config["rsi_floor"] <= curr_rsi < config["rsi_overbought"])
    rsi_short_ok = curr_rsi is not None and (config["rsi_short_floor"] < curr_rsi <= config["rsi_short_ceiling"])

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
        and fresh_cross_up and trend_ok and rsi_long_ok and vwap_long_ok and volume_ok
    )
    short_qualifies = bool(
        config.get("allow_short", True)
        and fresh_cross_down and trend_ok and rsi_short_ok and vwap_short_ok and volume_ok
    )

    # Ek hi candle par dono trigger nahi ho sakte (cross_up aur cross_down
    # mutually exclusive hain), lekin safety ke liye long ko priority.
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
        else:
            rsi_mid = (config["rsi_floor"] + config["rsi_overbought"]) / 2
            rsi_span = max(rsi_mid - config["rsi_floor"], 1)
        rsi_score = 1.0 - min(abs(curr_rsi - rsi_mid) / rsi_span, 1.0)
        score = round(adx_score * 0.45 + vol_score * 0.30 + rsi_score * 0.25, 4)

    return {
        "symbol": symbol,
        "insufficient_data": False,
        "price": curr_price,
        "adx": round(curr_adx, 2) if curr_adx is not None else None,
        "rsi": round(curr_rsi, 2) if curr_rsi is not None else None,
        "volume_ratio": round(volume_ratio, 2),
        "vwap": _smart_round(vwap) if vwap else None,
        "conditions": {
            "cross_up": fresh_cross_up,
            "cross_down": fresh_cross_down,
            "trend_aligned": trend_aligned,
            "adx_ok": trend_ok,
            "rsi_long_ok": rsi_long_ok,
            "rsi_short_ok": rsi_short_ok,
            "vwap_long_ok": vwap_long_ok,
            "vwap_short_ok": vwap_short_ok,
            "volume_ok": volume_ok,
        },
        "qualifies": qualifies,
        "direction": direction,
        "score": score,
    }


# ---------------------------------------------------------------------
# StrategyManager -- GLOBAL scanner, ab poore perpetual-futures universe
# par (watchlist = saare live perps, app.py discover karta hai).
# Dono directions (long + short) support karta hai.
# ---------------------------------------------------------------------
class StrategyManager:
    def __init__(self, feed, client, watchlist, config=None, place_order_fn=None):
        self.feed = feed
        self.client = client
        self.symbols = list(watchlist)
        self.config = {**DEFAULT_CONFIG, **(config or {})}

        # delta_rest_client's place_order() does NOT support bracket_* kwargs
        # (confirmed: "unexpected keyword argument 'bracket_stop_loss_price'").
        # Bracket orders must go through the raw signed /v2/orders POST, same
        # as app.py already does for /place-order and /positions/close. This
        # callable is injected from app.py so the HMAC signing logic lives in
        # exactly one place instead of being duplicated here.
        self.place_order_fn = place_order_fn

        self.product_info = resolve_product_info(client, self.symbols)

        self.running = False
        self.scan_thread = None
        self.monitor_thread = None
        self.lock = threading.Lock()

        self.open_trades = {}      # symbol -> trade dict (SIRF confirmed/dry-run orders)
        self.failed_symbols = {}   # symbol -> last-fail unix timestamp (retry cooldown)
        self.trades_today = 0
        self.realized_pnl_today = 0.0
        self.day_marker = datetime.now(timezone.utc).date()
        self.last_scan = {}        # symbol -> latest diagnostics (UI grid ke liye, SAARE symbols)
        self.trade_log = []

    # -- lifecycle -----------------------------------------------------
    def start(self, symbol=None):
        # symbol param backward-compat ke liye rakha hai, ignore hota
        # hai -- ab global scan hoti hai, single-symbol start nahi.
        with self.lock:
            if self.running:
                return
            self.running = True
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
        """Runtime me naye symbols watchlist me add karne ke liye (e.g.
        /watchlist/sync se naya listed perpetual future mila)."""
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

    def _roll_day_if_needed(self):
        today = datetime.now(timezone.utc).date()
        if today != self.day_marker:
            self.day_marker = today
            self.trades_today = 0
            self.realized_pnl_today = 0.0

    # -- scanning loop -----------------------------------------------------
    def _scan_loop(self):
        while self.running:
            try:
                self._roll_day_if_needed()
                self._scan_all_symbols()

                slots_free = len(self.open_trades) < self.config["max_concurrent_trades"]
                trades_left = self.trades_today < self.config["max_trades_per_day"]
                loss_ok = self.realized_pnl_today > -abs(self.config["max_daily_loss"])
                if slots_free and trades_left and loss_ok:
                    self._maybe_enter_best_candidate()
            except Exception:
                logger.error("Scan loop error", exc_info=True)
            time.sleep(self.config["scan_interval_sec"])

    def _scan_all_symbols(self):
        """Har symbol ke liye diagnostics compute karke last_scan me
        store karta hai -- chahe wo qualify kare ya na kare (kisi bhi
        direction me). Ye frontend grid ka poora data source hai."""
        for symbol in self.symbols:
            info = self.product_info.get(symbol)
            if not info or not info.get("product_id"):
                continue
            candles = self.feed.get_candles(symbol, limit=300)
            if not candles:
                continue
            diag = compute_diagnostics(symbol, candles, self.config)
            self.last_scan[symbol] = diag

    def _maybe_enter_best_candidate(self):
        cooldown_sec = self.config.get("failed_retry_cooldown_sec", 300)
        now = time.time()
        with self.lock:
            open_symbols = set(self.open_trades.keys())
            cooling_down = {s for s, t in self.failed_symbols.items() if now - t < cooldown_sec}

        candidates = [
            diag for symbol, diag in self.last_scan.items()
            if diag.get("qualifies")
            and symbol not in open_symbols
            and symbol not in cooling_down
        ]
        if not candidates:
            return

        # Long aur short dono candidates ek hi pool me compete karte hain --
        # sabse zyada SCORE wala jeetta hai, chahe wo kisi bhi direction ka ho.
        best = max(candidates, key=lambda s: s["score"])
        self._enter_trade(best)

    # -- entry -------------------------------------------------------------
    def _enter_trade(self, sig):
        symbol = sig["symbol"]
        direction = sig["direction"]          # "long" ya "short"
        side = "buy" if direction == "long" else "sell"

        info = self.product_info[symbol]
        product_id = info["product_id"]
        contract_value = info.get("contract_value", 1.0)
        entry_price = sig["price"]

        risk_amount = self.config["capital"] * (self.config["risk_pct"] / 100)
        sl_move = entry_price * (self.config["stop_loss_pct"] / 100)
        loss_per_contract = sl_move * contract_value
        qty = max(int(risk_amount // loss_per_contract), 1) if loss_per_contract > 0 else 1

        # --- Sanity cap: notional exposure kabhi bhi capital * max_leverage
        # se zyada na ho. Ye tiny-price coins (jahan qty astronomically
        # bada ban jaata tha) ke against safety net hai. ---
        max_notional = self.config["capital"] * self.config.get("max_leverage", 3)
        notional_per_contract = entry_price * contract_value
        if notional_per_contract > 0:
            notional = qty * notional_per_contract
            if notional > max_notional:
                qty = max(int(max_notional // notional_per_contract), 1)

        # --- Direction-aware SL/TP, dynamic precision rounding ---
        if direction == "long":
            sl_price = _smart_round(entry_price * (1 - self.config["stop_loss_pct"] / 100))
            tp_price = _smart_round(entry_price * (1 + self.config["target_pct"] / 100))
        else:  # short
            sl_price = _smart_round(entry_price * (1 + self.config["stop_loss_pct"] / 100))
            tp_price = _smart_round(entry_price * (1 - self.config["target_pct"] / 100))

        order_result = self._place_bracket_entry(product_id, qty, side, sl_price, tp_price)

        # --- Order fail/error hua to open_trades me COMMIT NAHI karna
        # (phantom trade fix) -- symbol ko cooldown me daal do taaki
        # turant wahi symbol dobara try na ho. ---
        if isinstance(order_result, dict) and order_result.get("error"):
            with self.lock:
                self.failed_symbols[symbol] = time.time()
            self._log_trade("ENTRY_FAILED", symbol, entry_price, qty, sig, order_result)
            logger.warning("ENTRY FAILED %s (%s): %s", symbol, direction, order_result.get("error"))
            return

        with self.lock:
            self.open_trades[symbol] = {
                "direction": direction,
                "side": side,
                "product_id": product_id,
                "entry_price": entry_price,
                "qty": qty,
                "sl_price": sl_price,
                "tp_price": tp_price,
                "entry_time": datetime.now(timezone.utc).isoformat(),
                "score": sig["score"],
                "order_result": order_result,
            }
            self.trades_today += 1
            self.failed_symbols.pop(symbol, None)

        self._log_trade("ENTRY", symbol, entry_price, qty, sig, order_result)
        logger.info("ENTRY %s [%s] qty=%s score=%.3f sl=%s tp=%s",
                    symbol, direction, qty, sig["score"], sl_price, tp_price)

    def _place_bracket_entry(self, product_id, qty, side, sl_price, tp_price):
        """Ek hi order call -- entry + SL + target dono attach, taaki
        exchange khud exit manage kare (humein tick-by-tick dekhne ki
        zaroorat nahi). `side` "buy" (long) ya "sell" (short) hota hai;
        bracket stop-loss/take-profit price direction ke hisaab se
        pehle hi _enter_trade me invert ho chuki hoti hai."""
        if self.config.get("dry_run", True):
            return {
                "dry_run": True, "product_id": product_id, "size": qty, "side": side,
                "bracket_stop_loss_price": str(sl_price),
                "bracket_take_profit_price": str(tp_price),
            }

        if self.place_order_fn is None:
            err = "place_order_fn not configured on StrategyManager -- pass it in from app.py"
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

    # -- monitoring / exit detection ----------------------------------------
    def _monitor_loop(self):
        """Exchange khud SL/TP execute karta hai; humein sirf dekhna hai
        ki position band hui ya nahi, taaki slot free ho sake."""
        while True:
            try:
                with self.lock:
                    symbols = list(self.open_trades.keys())
                for symbol in symbols:
                    self._check_position_closed(symbol)
            except Exception:
                logger.error("Monitor loop error", exc_info=True)
            time.sleep(self.config["monitor_interval_sec"])

    def _check_position_closed(self, symbol):
        trade = self.open_trades.get(symbol)
        if not trade:
            return
        if self.config.get("dry_run", True):
            return  # dry-run me exchange par position hoti hi nahi

        try:
            position = self.client.get_position(trade["product_id"])
            pos_result = position.get("result", position) if isinstance(position, dict) else position
            size = pos_result.get("size", 0) if isinstance(pos_result, dict) else 0
        except Exception as e:
            logger.warning("Could not fetch position for %s: %s", symbol, e)
            return

        if size == 0:
            # NOTE: exact exit price /v2/fills se lena zyada accurate hoga;
            # yahan rough estimate use kiya hai simplicity ke liye.
            exit_price = trade.get("tp_price") or trade.get("sl_price") or trade["entry_price"]
            direction = trade.get("direction", "long")
            if direction == "long":
                pnl = (exit_price - trade["entry_price"]) * trade["qty"]
            else:  # short -- price girne par profit
                pnl = (trade["entry_price"] - exit_price) * trade["qty"]
            with self.lock:
                self.realized_pnl_today += pnl
                self.open_trades.pop(symbol, None)
            self._log_trade("EXIT", symbol, exit_price, trade["qty"], {}, {"note": "bracket closed"}, pnl=pnl)
            logger.info("EXIT %s [%s] (bracket closed) pnl~=%.2f", symbol, direction, pnl)

    # -- logging / status -----------------------------------------------------
    def _log_trade(self, action, symbol, price, qty, sig, order_result, pnl=None):
        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "action": action, "symbol": symbol, "price": price, "qty": qty,
            "direction": sig.get("direction") if sig else None,
            "score": sig.get("score") if sig else None,
            "pnl": pnl, "order_result": order_result,
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
        # symbol param backward-compat ke liye rakha hai, ignore hota hai
        with self.lock:
            open_trades = dict(self.open_trades)
            failed_symbols = dict(self.failed_symbols)

        scan_grid = list(self.last_scan.values())
        # Qualifying symbols pehle (score se sorted), fir baaki symbols
        scan_grid.sort(key=lambda s: (not s.get("qualifies", False), -(s.get("score") or 0)))

        return {
            "running": self.running,
            "config": self.config,
            "trades_today": self.trades_today,
            "max_trades_per_day": self.config["max_trades_per_day"],
            "open_trades": open_trades,
            "max_concurrent_trades": self.config["max_concurrent_trades"],
            "realized_pnl_today": round(self.realized_pnl_today, 2),
            "failed_symbols_cooldown": failed_symbols,
            "candidates": sorted(
                [s for s in scan_grid if s.get("qualifies")], key=lambda s: -s["score"]
            )[:10],
            "scan_grid": scan_grid,
            "total_symbols": len(self.symbols),
            "recent_trades": list(reversed(self.trade_log[-20:])),
        }