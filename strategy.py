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

IMPROVEMENTS (this version, 2026-09-29):
  - UPDATED: RSI band (40-75 long / 25-60 short), ADX 20, min_ema_sep 0.02
  - UPDATED: stop_loss_pct 1.2 / target_pct 3.6 (ATR multipliers 1.2 SL / 3.6 TP for 3:1 RR ratio)
  - ADDED: Batch single-call position recovery (/v2/positions/margined) at startup
  - ADDED: Live bracket order update endpoint routing (PUT /v2/orders/bracket) with order_id
  - ADDED: Exact execution fill price extraction (/v2/fills) for realized PnL and circuit breaker accuracy
  - ADDED: Atomic live reduce-only order execution before internal state mutation on partial TP

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
import uuid
from collections import defaultdict
from math import log10, floor
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("delta-strategy-engine")

TRADE_LOG_FILE = "delta_strategy_trades.json"
PRODUCT_INFO_CACHE_FILE = "delta_product_info.json"

DEFAULT_CONFIG = {
    # entry trigger
    "fast_ema": 9,
    "slow_ema": 21,

    # momentum filter -- LONG (widened band so momentum moves aren't instantly rejected)
    "rsi_period": 14,
    "rsi_floor": 35,
    "rsi_overbought": 85,

    # momentum filter -- SHORT
    "rsi_short_floor": 15,
    "rsi_short_ceiling": 65,

    # trend filter (ADX threshold 16 for stronger trend confirmation)
    "adx_period": 14,
    "adx_threshold": 16,
    "require_adx_rising": False,

    # microscopic EMA cross filter
    "min_ema_separation_pct": 0.01,

    # EMA cross lookback window (candles) — 5 candles (25 mins on 5m chart)
    "cross_lookback_candles": 5,
    "require_cross_confirmation": False,

    # volume filter (0.0 = volume score used in ranking, not hard-rejecting trades)
    "volume_lookback": 20,
    "volume_multiplier": 0.0,

    # vwap filter
    "vwap_filter": True,

    # position sizing -- risk-based
    "capital": 50000,
    "risk_pct": 1.0,
    "max_leverage": 3,

    # --- stop / target ---------------------------------------------------
    # fixed-% fallback (used when use_atr_stops is False or ATR unavailable)
    "stop_loss_pct": 1.2,
    "target_pct": 4.2,

    # ATR-based stops -- per-symbol volatility instead of fixed % (4.2x TP gives 3.5:1 RR)
    "use_atr_stops": True,
    "atr_period": 14,
    "atr_sl_mult": 1.2,
    "atr_tp_mult": 4.2,

    # fees (Delta India taker ~0.05% each side -> ~0.10% round trip)
    "fee_rate_round_trip": 0.0010,

    # direction control
    "allow_long": True,
    "allow_short": True,

    # symbol blacklist -- toxic or underperforming altcoins to skip
    "symbol_blacklist": [
        "BASEDUSD", "LINKUSD", "ETHUSD", "DOGEUSD", "SAGAUSD", "PENDLEUSD", "BCHUSD", "POPCATUSD", "NEARUSD",
        "LTCUSD", "ARMBUSD", "BILLUSD", "REDUSD", "SPXUSD", "SUSHIUSD", "1000BONKUSD", "WDCBUSD",
        "XMRUSD", "TRXUSD", "RIVERUSD", "SKHYBUSD", "RKLBBUSD", "AMZNXUSD", "TRUMPUSD", "PAXGUSD",
        "ZROUSD", "SLVONUSD", "SPYXUSD", "AVAAIUSD", "LABUSD", "BEATUSD", "POLUSD", "ARCUSD",
        "BMTUSD", "BBUSD", "RAREUSD", "ORDERUSD", "ETHFIUSD", "RAVEUSD", "BLESSUSD", "NEIROUSD",
        "MANAUSD", "PUMPUSD", "AKEUSD", "VELVETUSD", "HUSD", "AIOUSD", "SNDKBUSD", "DRAMBUSD",
        "AINUSD", "VVVUSD", "BUSD", "FFUSD", "EVAAUSD", "SKYAIUSD", "INJUSD", "INTCBUSD",
        "HYPEUSD", "PENGUUSD", "ESPORTSUSD", "MUBARAKUSD", "TLMUSD", "GALAUSD", "XANUSD",
        "DASHUSD", "LISTAUSD", "SUIUSD", "ENAUSD", "DYDXUSD", "DOTUSD", "SOPHUSD", "HIVEUSD",
        "XRPUSD", "METAXUSD", "GOATUSD", "FILUSD",
    ],

    # minimum score threshold -- balance quality and trade frequency
    "min_score_threshold": 0.35,

    # portfolio-level limits (optimized for small $12 account)
    "max_trades_per_day": 8,
    "max_concurrent_trades": 2,
    "max_daily_trades_per_symbol": 2,
    "max_daily_losses_per_symbol": 2,
    # Absolute dollar loss circuit-breaker (legacy, for small accounts)
    "max_daily_loss": 5,
    # Percentage-of-capital circuit-breaker (takes precedence when set > 0)
    # e.g. 5.0 = stop trading after losing 5% of current capital today
    # Set to 0 to disable and fall back to max_daily_loss (absolute $)
    "max_daily_loss_pct": 5.0,
    # Max hold time (seconds) for recovered positions with no SL/TP in live mode
    "recovered_max_hold_sec": 3600,

    "scan_interval_sec": 5,
    "monitor_interval_sec": 10,
    "failed_retry_cooldown_sec": 1800,

    # post-exit cooldown (seconds) -- 1 hour (3600s) to prevent immediate re-entry loops
    "post_exit_cooldown_sec": 3600,

    # Break-Even Stop Loss
    "enable_breakeven_sl": True,
    "breakeven_trigger_pct": 1.5,

    # Bid-Ask Spread Filter -- reject entries if spread exceeds max threshold (%)
    "enable_spread_filter": True,
    "max_spread_pct": 0.15,

    # Dynamic Trailing Stop Loss
    "enable_trailing_sl": True,
    "trailing_distance_pct": 1.0,
    "trailing_step_pct": 0.3,

    # Multi-Timeframe (1-Hour) Trend Confirmation
    "require_htf_trend": True,
    "htf_ema_fast": 20,
    "htf_ema_slow": 50,

    # 1. Bitcoin Flash Crash Filter
    "enable_btc_crash_filter": True,
    "btc_dump_threshold_pct": 1.0,
    "btc_symbol": "BTCUSD",

    # 2. Partial Take-Profit (50% exit at +1.0% profit)
    "enable_partial_tp": True,
    "partial_tp_trigger_pct": 1.0,
    "partial_tp_ratio": 0.5,

    # 3. VWAP Upper Band Overbought Filter (+1.5 StdDev)
    "enable_vwap_band_filter": True,
    "vwap_max_std_dev": 1.5,

    # 4. High Volatility Session Window Filter (IST peak trading hours)
    "enable_session_filter": True,
    "session_start_hour_ist": 9,   # 9:00 AM IST
    "session_end_hour_ist": 2,     # 2:00 AM IST (next day)

    # 5. 24h/8h Funding Rate Filter
    "enable_funding_filter": True,
    "max_funding_rate_pct": 0.05,  # +0.05% max funding rate for long entries

    "dry_run_max_hold_sec": 3600,

    "dry_run": True,
}

EDITABLE_CONFIG_KEYS = [
    "fast_ema", "slow_ema", "rsi_period", "rsi_floor", "rsi_overbought",
    "rsi_short_floor", "rsi_short_ceiling",
    "adx_period", "adx_threshold", "require_adx_rising", "min_ema_separation_pct",
    "cross_lookback_candles", "require_cross_confirmation",
    "volume_lookback", "volume_multiplier",
    "vwap_filter", "capital", "risk_pct",
    "stop_loss_pct", "target_pct",
    "use_atr_stops", "atr_period", "atr_sl_mult", "atr_tp_mult",
    "max_leverage", "fee_rate_round_trip",
    "allow_long", "allow_short", "symbol_blacklist",
    "min_score_threshold",
    "max_trades_per_day", "max_concurrent_trades",
    "max_daily_trades_per_symbol", "max_daily_losses_per_symbol",
    "max_daily_loss", "max_daily_loss_pct", "recovered_max_hold_sec",
    "scan_interval_sec", "monitor_interval_sec",
    "failed_retry_cooldown_sec", "post_exit_cooldown_sec", "dry_run_max_hold_sec",
    "enable_breakeven_sl", "breakeven_trigger_pct",
    "enable_trailing_sl", "trailing_distance_pct", "trailing_step_pct",
    "require_htf_trend", "htf_ema_fast", "htf_ema_slow",
    "enable_btc_crash_filter", "btc_dump_threshold_pct", "btc_symbol",
    "enable_partial_tp", "partial_tp_trigger_pct", "partial_tp_ratio",
    "enable_vwap_band_filter", "vwap_max_std_dev",
    "enable_session_filter", "session_start_hour_ist", "session_end_hour_ist",
    "enable_funding_filter", "max_funding_rate_pct",
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


# ADDED -----------------------------------------------------------------
def atr(highs, lows, closes, period=14):
    """Wilder's ATR -- used for volatility-based SL/TP instead of a fixed %."""
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
# -------------------------------------------------------------------------


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
PRODUCT_INFO_CACHE_TTL_SEC = 86400  # 24-hour cache TTL


def resolve_product_info(client, symbols, use_cache=True):
    cached = {}
    cache_valid = False
    if use_cache and os.path.exists(PRODUCT_INFO_CACHE_FILE):
        try:
            age = time.time() - os.path.getmtime(PRODUCT_INFO_CACHE_FILE)
            if age < PRODUCT_INFO_CACHE_TTL_SEC:
                with open(PRODUCT_INFO_CACHE_FILE) as f:
                    cached = json.load(f)
                cache_valid = True
            else:
                logger.info("Product info cache is %.1fh old — forcing refresh", age / 3600)
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

            # ADDED: capture the exchange's actual per-product max leverage.
            # Delta's product payload usually carries this either directly or
            # nested inside a leverage-bracket-like field -- fall back to None
            # (meaning "unknown, use bot's own config cap") if not found.
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
                "max_leverage": max_leverage,   # ADDED
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


def resample_candles_1h(candles):
    """Aggregates sub-hourly candles (5m/15m) into 1-Hour candles for HTF trend analysis."""
    if not candles:
        return []
    hourly = []
    curr_hour = None
    cur_c = None
    for c in candles:
        t = c.get("time")
        if t is None:
            continue
        try:
            ts = int(t) if isinstance(t, (int, float)) else int(datetime.fromisoformat(str(t)).timestamp())
        except Exception:
            continue
        hour_ts = (ts // 3600) * 3600
        if hour_ts != curr_hour:
            if cur_c is not None:
                hourly.append(cur_c)
            curr_hour = hour_ts
            cur_c = {
                "time": hour_ts,
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"],
                "volume": c.get("volume", 0),
            }
        else:
            cur_c["high"] = max(cur_c["high"], c["high"])
            cur_c["low"] = min(cur_c["low"], c["low"])
            cur_c["close"] = c["close"]
            cur_c["volume"] += c.get("volume", 0)
    if cur_c is not None:
        hourly.append(cur_c)
    return hourly

def _today_vwap_with_bands(candles, max_std_dev=1.5):
    today = datetime.now(timezone.utc).date()
    pv, vol = 0.0, 0.0
    day_candles = []
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
        day_candles.append((c["close"], v))

    if vol <= 0 or not day_candles:
        return None, None, None

    vwap = pv / vol
    var_sum = sum(v * ((p - vwap) ** 2) for p, v in day_candles)
    std_dev = (var_sum / vol) ** 0.5
    upper_band = vwap + (max_std_dev * std_dev)
    lower_band = vwap - (max_std_dev * std_dev)
    return vwap, upper_band, lower_band


def _today_vwap(candles):
    vwap, _, _ = _today_vwap_with_bands(candles)
    return vwap


def compute_diagnostics(symbol, candles, config):
    # NOTE: need +1 vs before so index -3 (confirmation candle) is always available
    need = max(config["slow_ema"], config["rsi_period"], config["adx_period"] * 2) + 3
    empty_conditions = {
        "cross_up": False, "cross_down": False, "trend_aligned": False,
        "adx_ok": False, "adx_rising": False,
        "rsi_long_ok": False, "rsi_short_ok": False,
        "vwap_long_ok": False, "vwap_short_ok": False, "volume_ok": False,
        "ema_sep_ok": False, "vwap_band_long_ok": False, "vwap_band_short_ok": False,
        "vwap_band_ok": False, "funding_long_ok": False, "funding_short_ok": False,
        "funding_ok": False,
    }
    blacklist = config.get("symbol_blacklist") or []
    if symbol in blacklist:
        return {
            "symbol": symbol,
            "insufficient_data": False,
            "qualifies": False,
            "direction": None,
            "score": 0,
            "blacklisted": True,
            "price": candles[-1]["close"] if candles else None,
            "adx": None, "rsi": None, "volume_ratio": None, "vwap": None,
            "atr": None,
            "conditions": empty_conditions,
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
    atr_vals = atr(highs, lows, closes, config.get("atr_period", 14))   # ADDED

    prev_fast, curr_fast = fast[-2], fast[-1]
    prev_slow, curr_slow = slow[-2], slow[-1]
    curr_rsi = rsi_vals[-1]
    curr_adx = adx_vals[-1]
    prev_adx = adx_vals[-2] if len(adx_vals) >= 2 else None
    curr_price = closes[-1]
    curr_atr = atr_vals[-1] if atr_vals else None   # ADDED

    # ---- EMA cross detection logic (supports lookback window) ------------
    cross_lookback = int(config.get("cross_lookback_candles", 3) or 1)
    fresh_cross_up = False
    fresh_cross_down = False
    n_fast = len(fast)
    if n_fast >= 2:
        for i in range(1, min(cross_lookback + 1, n_fast - 1)):
            if fast[-i - 1] <= slow[-i - 1] and fast[-i] > slow[-i]:
                if curr_fast > curr_slow:
                    fresh_cross_up = True
                    break
            if fast[-i - 1] >= slow[-i - 1] and fast[-i] < slow[-i]:
                if curr_fast < curr_slow:
                    fresh_cross_down = True
                    break

        if config.get("require_cross_confirmation", False) and n_fast >= 3:
            if fresh_cross_up and not (fast[-2] > slow[-2]):
                fresh_cross_up = False
            if fresh_cross_down and not (fast[-2] < slow[-2]):
                fresh_cross_down = False
    # -----------------------------------------------------------------------

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

    vwap, vwap_upper, vwap_lower = _today_vwap_with_bands(
        candles, config.get("vwap_max_std_dev", 1.5)
    ) if (config.get("vwap_filter", True) or config.get("enable_vwap_band_filter", True)) else (None, None, None)

    vwap_long_ok = (not config.get("vwap_filter", True)) or vwap is None or curr_price > vwap
    vwap_short_ok = (not config.get("vwap_filter", True)) or vwap is None or curr_price < vwap

    # 3. VWAP Upper/Lower Band Overbought/Oversold Filter
    vwap_band_long_ok = True
    vwap_band_short_ok = True
    if config.get("enable_vwap_band_filter", True):
        if vwap_upper is not None:
            vwap_band_long_ok = curr_price <= vwap_upper
        if vwap_lower is not None:
            vwap_band_short_ok = curr_price >= vwap_lower

    # 5. 24h / 8h Funding Rate Filter
    # SCALE NOTE: Delta ticker sends funding_rate as a decimal fraction
    # (e.g. 0.0005 = 0.05%), so we compare against max_funding_rate_pct / 100.0
    funding_long_ok = True
    funding_short_ok = True
    symbol_funding = config.get("_symbol_funding_rates", {}).get(symbol)
    if config.get("enable_funding_filter", True) and symbol_funding is not None:
        max_funding_pct = float(config.get("max_funding_rate_pct", 0.05) or 0.05)
        # max_funding_rate_pct is in % (e.g. 0.05 means 0.05%)
        # symbol_funding is in decimal (e.g. 0.0005 means 0.05%)
        # So threshold in decimal = max_funding_pct / 100.0
        funding_threshold = max_funding_pct / 100.0
        if symbol_funding > funding_threshold:
            funding_long_ok = False
        if symbol_funding < -funding_threshold:
            funding_short_ok = False

    lookback = config["volume_lookback"]
    recent_vols = [v for v in volumes[-(lookback + 1):-1] if v]
    avg_vol = (sum(recent_vols) / len(recent_vols)) if recent_vols else 0
    volume_ratio = (volumes[-1] / avg_vol) if avg_vol else 1.0
    volume_ok = (not recent_vols) or volume_ratio >= config["volume_multiplier"]

    htf_long_ok, htf_short_ok = True, True
    if config.get("require_htf_trend", True):
        h_candles = resample_candles_1h(candles)
        h_slow_p = config.get("htf_ema_slow", 50)
        if len(h_candles) >= h_slow_p:
            h_closes = [c["close"] for c in h_candles]
            h_fast = ema(h_closes, config.get("htf_ema_fast", 20))
            h_slow = ema(h_closes, h_slow_p)
            if h_fast and h_slow and len(h_fast) > 0 and len(h_slow) > 0:
                htf_long_ok = h_fast[-1] > h_slow[-1] and h_closes[-1] > h_slow[-1]
                htf_short_ok = h_fast[-1] < h_slow[-1] and h_closes[-1] < h_slow[-1]

    long_qualifies = bool(
        config.get("allow_long", True)
        and fresh_cross_up and trend_ok and adx_rising_ok and ema_sep_ok
        and rsi_long_ok and vwap_long_ok and vwap_band_long_ok and volume_ok and htf_long_ok and funding_long_ok
    )
    short_qualifies = bool(
        config.get("allow_short", True)
        and fresh_cross_down and trend_ok and adx_rising_ok and ema_sep_ok
        and rsi_short_ok and vwap_short_ok and vwap_band_short_ok and volume_ok and htf_short_ok and funding_short_ok
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
        # ADX sweet spot: 18-35. Above 35 represents late trend / exhaustion risk.
        if curr_adx < 18:
            adx_score = curr_adx / 18.0
        elif curr_adx <= 35:
            adx_score = 1.0
        else:
            adx_score = max(1.0 - (curr_adx - 35) / 35.0, 0.2)

        # Volume ratio sweet spot: 1.1x to 2.2x. Extreme volume (> 2.5x) is climax exhaustion.
        if volume_ratio <= 2.2:
            vol_score = min(volume_ratio / 1.5, 1.0)
        else:
            vol_score = max(1.0 - (volume_ratio - 2.2) / 3.0, 0.3)

        if direction == "short":
            rsi_mid = (config["rsi_short_floor"] + config["rsi_short_ceiling"]) / 2
            rsi_span = max(config["rsi_short_ceiling"] - rsi_mid, 1)
        elif direction == "long":
            rsi_mid = (config["rsi_floor"] + config["rsi_overbought"]) / 2
            rsi_span = max(rsi_mid - config["rsi_floor"], 1)
        else:
            rsi_mid, rsi_span = 50.0, 50.0
        rsi_score = 1.0 - min(abs(curr_rsi - rsi_mid) / rsi_span, 1.0)

        # Penalty if RSI already overextended (late entry buying top / shorting bottom)
        extension_pen = 0.0
        if direction == "long" and curr_rsi is not None and curr_rsi > 65:
            extension_pen = min((curr_rsi - 65) / 25.0, 0.35)
        elif direction == "short" and curr_rsi is not None and curr_rsi < 35:
            extension_pen = min((35 - curr_rsi) / 25.0, 0.35)

        raw = adx_score * 0.40 + vol_score * 0.30 + rsi_score * 0.30
        score = round(max(raw - extension_pen, 0.0), 4)

    return {
        "symbol": symbol,
        "insufficient_data": False,
        "price": curr_price,
        "adx": round(curr_adx, 2) if curr_adx is not None else None,
        "rsi": round(curr_rsi, 2) if curr_rsi is not None else None,
        "volume_ratio": round(volume_ratio, 2),
        "vwap": _smart_round(vwap) if vwap else None,
        "atr": round(curr_atr, 6) if curr_atr is not None else None,   # ADDED
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
            "vwap_band_long_ok": vwap_band_long_ok,
            "vwap_band_short_ok": vwap_band_short_ok,
            "vwap_band_ok": vwap_band_long_ok if direction == "long" else (vwap_band_short_ok if direction == "short" else vwap_band_long_ok),
            "funding_long_ok": funding_long_ok,
            "funding_short_ok": funding_short_ok,
            "funding_ok": funding_long_ok if direction == "long" else (funding_short_ok if direction == "short" else funding_long_ok),
        },
        "qualifies": qualifies,
        "direction": direction,
        "score": score,
    }


def _extract_order_id(order_result):
    if not isinstance(order_result, dict):
        return None
    for k in ("id", "order_id"):
        if order_result.get(k):
            return order_result[k]
    res = order_result.get("result") or order_result.get("data")
    if isinstance(res, dict):
        for k in ("id", "order_id"):
            if res.get(k):
                return res[k]
    return None


# ---------------------------------------------------------------------
# StrategyManager
# ---------------------------------------------------------------------
class StrategyManager:
    def __init__(self, feed, client, watchlist, config=None, place_order_fn=None, signed_request_fn=None):
        self.feed = feed
        self.client = client
        self.symbols = list(watchlist)
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.place_order_fn = place_order_fn
        self.signed_request_fn = signed_request_fn

        self.product_info = resolve_product_info(client, self.symbols)

        self.running = False
        self.scan_thread = None
        self.monitor_thread = None
        self.lock = threading.Lock()

        self.open_trades = {}
        self.failed_symbols = {}
        self.trades_today = 0
        self.symbol_trades_today = defaultdict(int)
        self.symbol_losses_today = defaultdict(int)
        self.realized_pnl_today = 0.0
        self.circuit_broken = False
        self.btc_crash_active = False
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

    # Safe value ranges for editable config keys.
    # These prevent dangerous API calls from setting e.g. risk_pct=100
    # or max_leverage=200 which would cause immediate liquidation.
    _CONFIG_CLAMPS = {
        "risk_pct":               (0.1,  5.0),
        "max_leverage":           (1,    10),
        "max_daily_loss":         (1.0,  10000.0),
        "max_daily_loss_pct":     (0.5,  20.0),
        "stop_loss_pct":          (0.3,  5.0),
        "target_pct":             (0.3,  15.0),
        "atr_sl_mult":            (0.5,  5.0),
        "atr_tp_mult":            (0.5,  10.0),
        "max_trades_per_day":          (1,    20),
        "max_concurrent_trades":      (1,    5),
        "max_daily_trades_per_symbol": (1,    5),
        "max_daily_losses_per_symbol": (1,    5),
        "min_score_threshold":    (0.0,  1.0),
        "adx_threshold":          (10,   50),
        "volume_multiplier":      (0.5,  5.0),
        "partial_tp_ratio":       (0.1,  0.9),
        "partial_tp_trigger_pct": (0.1,  10.0),
        "breakeven_trigger_pct":  (0.1,  10.0),
        "trailing_distance_pct":  (0.1,  10.0),
    }

    def update_config(self, patch):
        if self.running:
            raise ValueError("Config change karne se pehle scanner STOP karo")
        clean_patch = {}
        for k, v in patch.items():
            if k not in EDITABLE_CONFIG_KEYS:
                continue
            # Apply safety clamps for numeric fields
            if k in self._CONFIG_CLAMPS:
                lo, hi = self._CONFIG_CLAMPS[k]
                try:
                    v_num = type(lo)(v)  # cast to same type as bound (int/float)
                    v = max(lo, min(hi, v_num))
                except (TypeError, ValueError):
                    pass  # non-numeric; let validation happen naturally
            clean_patch[k] = v
        self.config.update(clean_patch)
        return self.config

    def _roll_day_if_needed(self):
        today = datetime.now(timezone.utc).date()
        if today != self.day_marker:
            self.day_marker = today
            self.trades_today = 0
            self.symbol_trades_today.clear()
            self.symbol_losses_today.clear()
            self.realized_pnl_today = 0.0
            self.circuit_broken = False
            # Refresh capital baseline from live equity at day-start so circuit
            # breaker uses today's actual equity, not a stale config value.
            live_eq = self._get_live_available_margin()
            if live_eq and live_eq > 0:
                self.config["capital"] = live_eq
                logger.info("Day-roll: capital updated to live equity %.2f for circuit-breaker", live_eq)

    def _prune_failed_symbols(self):
        fail_cd = self.config.get("failed_retry_cooldown_sec", 1800)
        exit_cd = self.config.get("post_exit_cooldown_sec", 300)
        now = time.time()
        with self.lock:
            kept = {}
            for s, v in self.failed_symbols.items():
                if isinstance(v, tuple):
                    t, reason = v
                    cd = exit_cd if reason == "exit" else fail_cd
                else:
                    t, cd = v, fail_cd  # backward compat
                if now - t < cd:
                    kept[s] = v
            self.failed_symbols = kept

    # -- position recovery ---------------------------------------------
    def _sync_open_positions(self):
        recovered = 0
        if not self.signed_request_fn:
            return self._sync_open_positions_fallback()

        try:
            ok, status, positions_data = self.signed_request_fn("GET", "/v2/positions/margined")
            if not ok:
                logger.warning("Batch position request failed (status %s), using fallback", status)
                return self._sync_open_positions_fallback()

            positions_list = []
            if isinstance(positions_data, list):
                positions_list = positions_data
            elif isinstance(positions_data, dict):
                res = positions_data.get("result", positions_data)
                if isinstance(res, list):
                    positions_list = res

            # BATCH CALL SUCCESSFUL: Empty list means zero open positions on exchange
            if not positions_list:
                logger.info("Batch sync: 0 open positions found on exchange")
                return

            pid_to_symbol = {info["product_id"]: sym for sym, info in self.product_info.items() if info.get("product_id")}

            for pos in positions_list:
                if not isinstance(pos, dict):
                    continue
                pid = pos.get("product_id")
                symbol = pid_to_symbol.get(pid) or pos.get("symbol")
                if not symbol or symbol not in self.product_info:
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
                    continue

                info = self.product_info.get(symbol, {})
                contract_value = float(info.get("contract_value", 1.0) or 1.0)

                # Seed internal SL/TP so recovered positions are protected immediately
                rec_sl, rec_tp = self._seed_sl_tp_for_recovery(symbol, direction, entry_price)

                # P0: Fetch existing open stop order IDs (bracket legs) for this position
                rec_sl_oid, rec_tp_oid = self._fetch_open_stop_order_ids(pid)

                # P0: If no exchange stop orders exist, place a real SL stop order
                # so position has exchange-side protection even if process restarts.
                if not rec_sl_oid and not self.config.get("dry_run", True) and self.place_order_fn:
                    stop_side = "sell" if direction == "long" else "buy"
                    try:
                        sl_order_body = {
                            "product_id": pid,
                            "size": qty,
                            "side": stop_side,
                            "order_type": "market_order",
                            "stop_order_type": "stop_loss_order",
                            "stop_price": str(rec_sl),
                            "reduce_only": True,
                        }
                        sl_res = self.place_order_fn(sl_order_body)
                        if isinstance(sl_res, dict) and not sl_res.get("error"):
                            rec_sl_oid = _extract_order_id(sl_res)
                            logger.info(
                                "RECOVERED %s: placed exchange SL stop order at %.6f, id=%s",
                                symbol, rec_sl, rec_sl_oid,
                            )
                        else:
                            logger.warning("RECOVERED %s: could not place exchange SL stop: %s", symbol, sl_res)
                    except Exception as ex:
                        logger.warning("RECOVERED %s: exception placing exchange SL stop: %s", symbol, ex)

                # Place exchange-side TP stop order for recovered position if missing
                if not rec_tp_oid and rec_tp and not self.config.get("dry_run", True) and self.place_order_fn:
                    stop_side = "sell" if direction == "long" else "buy"
                    try:
                        tp_order_body = {
                            "product_id": pid,
                            "size": qty,
                            "side": stop_side,
                            "order_type": "market_order",
                            "stop_order_type": "take_profit_order",
                            "stop_price": str(rec_tp),
                            "reduce_only": True,
                        }
                        tp_res = self.place_order_fn(tp_order_body)
                        if isinstance(tp_res, dict) and not tp_res.get("error"):
                            rec_tp_oid = _extract_order_id(tp_res)
                            logger.info(
                                "RECOVERED %s: placed exchange TP stop order at %.6f, id=%s",
                                symbol, rec_tp, rec_tp_oid,
                            )
                        else:
                            logger.warning("RECOVERED %s: could not place exchange TP stop: %s", symbol, tp_res)
                    except Exception as ex:
                        logger.warning("RECOVERED %s: exception placing exchange TP stop: %s", symbol, ex)

                # FIX 3: Assign trade_id to recovered positions so PARTIAL_TP
                # and EXIT log events can be grouped correctly in check_pnl.py.
                rec_trade_id = str(uuid.uuid4())[:12]
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
                        "sl_price": rec_sl,
                        "tp_price": rec_tp,
                        "entry_time": datetime.now(timezone.utc).isoformat(),
                        "entry_ts": time.time(),
                        "score": None,
                        "order_result": {"recovered": True},
                        "sl_order_id": rec_sl_oid,
                        "tp_order_id": rec_tp_oid,
                        "recovered": True,
                        "trade_id": rec_trade_id,
                    }
                recovered += 1
                # FIX C: Log a RECOVERED_ENTRY event so check_pnl.py can
                # treat this as the ENTRY anchor when grouping the trade
                # lifecycle (RECOVERED_ENTRY → PARTIAL_TP → EXIT) by trade_id.
                self._log_trade(
                    "RECOVERED_ENTRY", symbol, entry_price, qty,
                    {"direction": direction, "score": None},
                    {"recovered": True, "sl": rec_sl, "tp": rec_tp},
                    trade_id=rec_trade_id,
                )
                logger.info(
                    "RECOVERED open position %s [%s] qty=%s entry≈%s SL=%.6f TP=%.6f sl_oid=%s trade_id=%s",
                    symbol, direction, qty, entry_price, rec_sl, rec_tp, rec_sl_oid, rec_trade_id,
                )
        except Exception as e:
            logger.warning("Could not batch sync positions, using fallback: %s", e)
            return self._sync_open_positions_fallback()

        if recovered:
            logger.info("Synced %d existing position(s) from exchange", recovered)

    def _sync_open_positions_fallback(self):
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
                    continue

                contract_value = float(info.get("contract_value", 1.0) or 1.0)
                # Seed internal SL/TP so recovered positions are protected immediately
                rec_sl, rec_tp = self._seed_sl_tp_for_recovery(symbol, direction, entry_price)

                # P0: Fetch existing stop order IDs; place new SL stop if none exist
                rec_sl_oid, rec_tp_oid = self._fetch_open_stop_order_ids(pid)
                if not rec_sl_oid and not self.config.get("dry_run", True) and self.place_order_fn:
                    stop_side = "sell" if direction == "long" else "buy"
                    try:
                        sl_res = self.place_order_fn({
                            "product_id": pid,
                            "size": qty,
                            "side": stop_side,
                            "order_type": "market_order",
                            "stop_order_type": "stop_loss_order",
                            "stop_price": str(rec_sl),
                            "reduce_only": True,
                        })
                        if isinstance(sl_res, dict) and not sl_res.get("error"):
                            rec_sl_oid = _extract_order_id(sl_res)
                            logger.info("RECOVERED(fallback) %s: placed exchange SL at %.6f, id=%s", symbol, rec_sl, rec_sl_oid)
                        else:
                            logger.warning("RECOVERED(fallback) %s: could not place exchange SL: %s", symbol, sl_res)
                    except Exception as ex:
                        logger.warning("RECOVERED(fallback) %s: exception placing exchange SL: %s", symbol, ex)

                if not rec_tp_oid and rec_tp and not self.config.get("dry_run", True) and self.place_order_fn:
                    stop_side = "sell" if direction == "long" else "buy"
                    try:
                        tp_res = self.place_order_fn({
                            "product_id": pid,
                            "size": qty,
                            "side": stop_side,
                            "order_type": "market_order",
                            "stop_order_type": "take_profit_order",
                            "stop_price": str(rec_tp),
                            "reduce_only": True,
                        })
                        if isinstance(tp_res, dict) and not tp_res.get("error"):
                            rec_tp_oid = _extract_order_id(tp_res)
                            logger.info("RECOVERED(fallback) %s: placed exchange TP at %.6f, id=%s", symbol, rec_tp, rec_tp_oid)
                        else:
                            logger.warning("RECOVERED(fallback) %s: could not place exchange TP: %s", symbol, tp_res)
                    except Exception as ex:
                        logger.warning("RECOVERED(fallback) %s: exception placing exchange TP: %s", symbol, ex)

                # FIX 3b: Assign trade_id to fallback-recovered positions.
                rec_trade_id = str(uuid.uuid4())[:12]
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
                        "sl_price": rec_sl,
                        "tp_price": rec_tp,
                        "entry_time": datetime.now(timezone.utc).isoformat(),
                        "entry_ts": time.time(),
                        "score": None,
                        "order_result": {"recovered": True},
                        "sl_order_id": rec_sl_oid,
                        "tp_order_id": rec_tp_oid,
                        "recovered": True,
                        "trade_id": rec_trade_id,
                    }
                recovered += 1
                # FIX C (fallback): same RECOVERED_ENTRY log as batch path.
                self._log_trade(
                    "RECOVERED_ENTRY", symbol, entry_price, qty,
                    {"direction": direction, "score": None},
                    {"recovered": True, "sl": rec_sl, "tp": rec_tp},
                    trade_id=rec_trade_id,
                )
            except Exception:
                pass
        if recovered:
            logger.info("Synced %d existing position(s) from exchange (fallback)", recovered)

    def _check_btc_flash_crash(self):
        if not self.config.get("enable_btc_crash_filter", True):
            self.btc_crash_active = False
            return False

        btc_sym = self.config.get("btc_symbol", "BTCUSD")
        # FIX: Increased from 12→30 candles, lookback high from 4→20 candles
        # (4 candles = 4 min was too short to catch real market dumps)
        candles = self.feed.get_candles(btc_sym, limit=30)
        if not candles or len(candles) < 5:
            self.btc_crash_active = False
            return False

        curr_close = candles[-1]["close"]
        lookback_high = max(c["high"] for c in candles[-20:])  # 20-minute high
        thresh = float(self.config.get("btc_dump_threshold_pct", 1.0) or 1.0)
        drop_pct = ((curr_close - lookback_high) / lookback_high) * 100.0

        if drop_pct <= -abs(thresh):
            if not getattr(self, "btc_crash_active", False):
                logger.warning("🛑 BTC FLASH CRASH DETECTED: BTC dropped %.2f%% in 20min! Pausing Altcoin LONG entries.", drop_pct)
            self.btc_crash_active = True
            return True
        else:
            self.btc_crash_active = False
            return False

    def _is_within_trading_session(self):
        if not self.config.get("enable_session_filter", True):
            return True
        try:
            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc + timedelta(hours=5, minutes=30)
            curr_hour = now_ist.hour
            start_h = int(self.config.get("session_start_hour_ist", 9))
            end_h = int(self.config.get("session_end_hour_ist", 2))

            if start_h < end_h:
                return start_h <= curr_hour < end_h
            else:
                return curr_hour >= start_h or curr_hour < end_h
        except Exception:
            return True

    # -- scanning ------------------------------------------------------
    def _scan_loop(self):
        while self.running:
            try:
                self._roll_day_if_needed()
                self._prune_failed_symbols()
                self._check_btc_flash_crash()
                self._scan_all_symbols()

                with self.lock:
                    n_open = len(self.open_trades)
                    trades_today = self.trades_today
                    realized = self.realized_pnl_today
                slots_free = n_open < self.config["max_concurrent_trades"]
                trades_left = trades_today < self.config["max_trades_per_day"]
                # FIX: max_daily_loss_pct (% of capital) takes precedence over
                # max_daily_loss (absolute $) when set to a value > 0.
                daily_loss_pct = float(self.config.get("max_daily_loss_pct", 0) or 0)
                if daily_loss_pct > 0:
                    capital = float(self.config.get("capital", 50000))
                    loss_limit = -(capital * daily_loss_pct / 100.0)
                else:
                    loss_limit = -abs(self.config["max_daily_loss"])
                loss_ok = (not self.circuit_broken) and realized > loss_limit
                if not loss_ok and not self.circuit_broken:
                    logger.warning(
                        "🚨 CIRCUIT BREAKER: Daily loss limit hit (realized=%.4f, limit=%.4f). No new trades today.",
                        realized, loss_limit,
                    )
                    self.circuit_broken = True
                if slots_free and trades_left and loss_ok:
                    self._maybe_enter_best_candidate()
            except Exception:
                logger.error("Scan loop error", exc_info=True)
            time.sleep(self.config["scan_interval_sec"])

    def _scan_all_symbols(self):
        rates = self.config.setdefault("_symbol_funding_rates", {})
        logged_funding = getattr(self, "_logged_funding_scale", set())
        for symbol in list(self.symbols):
            info = self.product_info.get(symbol)
            if not info or not info.get("product_id"):
                continue
            ticker = self.feed.get_ticker(symbol)
            if ticker and ticker.get("funding_rate") is not None:
                fr = ticker["funding_rate"]
                rates[symbol] = fr
                # P2: Log raw funding rate on first observation for scale sanity-check.
                # Delta sends a decimal fraction (0.0005 = 0.05%). If you see values
                # like 0.05 here, the scale assumption is wrong and max_funding_rate_pct
                # threshold must be adjusted.
                if symbol not in logged_funding:
                    logger.info("[funding-scale] %s raw funding_rate=%s (expect ~0.0001-0.001 range)", symbol, fr)
                    logged_funding.add(symbol)
            # FIX: Reduced from 1000 → 300 candles.
            # Indicators (EMA21, ADX14, RSI14, ATR14) need ~100 candles for
            # accuracy; 300 gives a safe buffer. 1000 × 17 symbols × 5s scan
            # was unnecessary CPU/memory load.
            candles = self.feed.get_candles(symbol, limit=300)
            if not candles:
                continue
            diag = compute_diagnostics(symbol, candles, self.config)
            with self.lock:
                self.last_scan[symbol] = diag
        self._logged_funding_scale = logged_funding

    def _maybe_enter_best_candidate(self):
        # 4. High Volatility Session Window Check
        if not self._is_within_trading_session():
            return

        fail_cd = self.config.get("failed_retry_cooldown_sec", 1800)
        exit_cd = self.config.get("post_exit_cooldown_sec", 300)
        now = time.time()
        min_score = self.config.get("min_score_threshold", 0.0)
        btc_crash = getattr(self, "btc_crash_active", False)

        with self.lock:
            open_symbols = set(self.open_trades.keys())
            cooling_down = set()
            for s, v in self.failed_symbols.items():
                if isinstance(v, tuple):
                    t, reason = v
                else:
                    t, reason = v, "fail"
                cd = exit_cd if reason == "exit" else fail_cd
                if now - t < cd:
                    cooling_down.add(s)
            scan_snapshot = dict(self.last_scan)

        candidates = []
        max_sym_trades = self.config.get("max_daily_trades_per_symbol", 2)
        max_sym_losses = self.config.get("max_daily_losses_per_symbol", 2)

        for symbol, diag in scan_snapshot.items():
            if not diag.get("qualifies"):
                continue
            if diag.get("score", 0) < min_score:
                continue
            if symbol in open_symbols or symbol in cooling_down:
                continue
            # Per-symbol max daily trades and max daily losses lockout
            if self.symbol_trades_today[symbol] >= max_sym_trades:
                continue
            if self.symbol_losses_today[symbol] >= max_sym_losses:
                continue
            # 1. BTC Flash Crash Protection: pause LONG entries when BTC dumps
            if btc_crash and diag.get("direction") == "long":
                continue

            # 2. Bid-Ask Spread Filter: reject candidate if spread exceeds max_spread_pct
            if self.config.get("enable_spread_filter", True):
                max_spread = self.config.get("max_spread_pct", 0.15)
                t = self.feed.ticker.get(symbol) if self.feed and self.feed.ticker else None
                if t:
                    bid = t.get("bid") or 0.0
                    ask = t.get("ask") or 0.0
                    if bid > 0 and ask > 0 and ask >= bid:
                        spread_pct = ((ask - bid) / bid) * 100.0
                        if spread_pct > max_spread:
                            continue

            candidates.append(diag)

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

    # ADDED: compute SL/TP either from ATR or the fixed-% fallback
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

    def _seed_sl_tp_for_recovery(self, symbol, direction, entry_price):
        """Compute protective SL/TP for a recovered position so internal monitor can act."""
        candles = self.feed.get_candles(symbol, limit=50)
        atr_val = None
        if candles and len(candles) >= 15:
            atr_vals = atr(
                [c["high"] for c in candles],
                [c["low"] for c in candles],
                [c["close"] for c in candles],
                self.config.get("atr_period", 14),
            )
            atr_val = atr_vals[-1] if atr_vals else None
        return self._compute_sl_tp(direction, entry_price, atr_val)

    def _get_live_available_margin(self):
        """Fetches real-time available margin balance from Delta Exchange via client."""
        if not self.config.get("dry_run", True) and self.client and hasattr(self.client, "get_balances"):
            try:
                try:
                    bal = self.client.get_balances(1)
                except (TypeError, Exception):
                    try:
                        bal = self.client.get_balances()
                    except Exception:
                        return None
                if not bal:
                    return None
                rows = bal.get("result", bal) if isinstance(bal, dict) else bal
                if isinstance(rows, dict):
                    rows = [rows]
                if isinstance(rows, list):
                    max_bal = 0.0
                    for row in rows:
                        if isinstance(row, dict):
                            for key in ("available_balance", "available", "balance", "equity"):
                                val = row.get(key)
                                if val is not None:
                                    try:
                                        v = float(val)
                                        if v > max_bal:
                                            max_bal = v
                                    except (TypeError, ValueError):
                                        pass
                    if max_bal > 0:
                        return max_bal
            except Exception as e:
                logger.warning("Could not fetch live available margin: %s", e)
        return None

    def _enter_trade(self, sig):
        symbol = sig["symbol"]
        direction = sig["direction"]
        side = "buy" if direction == "long" else "sell"

        info = self.product_info[symbol]
        product_id = info["product_id"]
        contract_value = float(info.get("contract_value", 1.0) or 1.0)
        entry_price = sig["price"]

        # Dynamic available margin sync right before trade placement
        capital = float(self.config.get("capital", 50000))
        avail_bal = self._get_live_available_margin()
        if avail_bal is not None and avail_bal > 0:
            capital = avail_bal

        # FIX: Compute SL/TP FIRST using actual ATR or fixed-% distance,
        # then size risk off the REAL sl_move — not the fixed stop_loss_pct.
        # Previously sizing always used fixed % even when ATR stops were active,
        # causing inconsistent risk per trade.
        sl_price, tp_price = self._compute_sl_tp(direction, entry_price, sig.get("atr"))
        sl_move = abs(entry_price - sl_price)

        risk_amount = capital * (self.config["risk_pct"] / 100)
        loss_per_contract = sl_move * contract_value
        qty = max(int(risk_amount // loss_per_contract), 1) if loss_per_contract > 0 else 1

        if loss_per_contract > 0 and 1 * loss_per_contract > risk_amount * 1.05:
            logger.warning(
                "Skipping entry for %s: min 1 contract risk (%.2f) exceeds risk budget (%.2f)",
                symbol, loss_per_contract, risk_amount,
            )
            with self.lock:
                self.failed_symbols[symbol] = (time.time(), "fail")
            self._log_trade(
                "ENTRY_SKIPPED", symbol, entry_price, 0, sig,
                {"error": f"min contract risk {loss_per_contract:.4f} > budget {risk_amount:.4f}"}
            )
            return

        notional_per_contract = entry_price * contract_value

        # existing bot-level leverage cap
        max_notional = capital * self.config.get("max_leverage", 3)
        if notional_per_contract > 0:
            notional = qty * notional_per_contract
            if notional > max_notional:
                qty = max(int(max_notional // notional_per_contract), 1)

        # respect the EXCHANGE's actual per-product max leverage
        product_max_lev = info.get("max_leverage")
        if product_max_lev and notional_per_contract > 0:
            max_notional_product = capital * product_max_lev
            notional = qty * notional_per_contract
            if notional > max_notional_product:
                qty = max(int(max_notional_product // notional_per_contract), 1)
                logger.info(
                    "%s qty clamped to %s for exchange max_leverage=%s",
                    symbol, qty, product_max_lev,
                )

        # Live margin pre-flight check to prevent insufficient_margin API rejections
        if avail_bal is not None and avail_bal > 0 and notional_per_contract > 0:
            max_concurrent = max(self.config.get("max_concurrent_trades", 3), 1)
            # Apply 0.95 factor (5% fee/commission buffer) to prevent insufficient_commission errors
            avail_margin_for_trade = (avail_bal / max_concurrent) * 0.95
            effective_lev = min(
                self.config.get("max_leverage", 3),
                product_max_lev or self.config.get("max_leverage", 3)
            )
            max_notional_margin = avail_margin_for_trade * effective_lev
            max_qty_margin = int(max_notional_margin // notional_per_contract)
            if max_qty_margin < 1:
                logger.warning(
                    "INSUFFICIENT MARGIN SKIPPED %s: contract notional %.2f exceeds max allowable margin notional %.2f (available balance: %.2f)",
                    symbol, notional_per_contract, max_notional_margin, avail_bal
                )
                with self.lock:
                    self.failed_symbols[symbol] = (time.time(), "fail")
                self._log_trade("ENTRY_SKIPPED", symbol, entry_price, 0, sig, {
                    "error": f"Insufficient available balance (avail={avail_bal:.2f})"
                })
                return
            if qty > max_qty_margin:
                logger.info("%s qty clamped from %d to %d based on available wallet balance %.2f", symbol, qty, max_qty_margin, avail_bal)
                qty = max_qty_margin

        order_result = self._place_bracket_entry(product_id, qty, side, sl_price, tp_price)

        if isinstance(order_result, dict) and (
            order_result.get("error") or self._order_looks_rejected(order_result)
        ):
            with self.lock:
                self.failed_symbols[symbol] = (time.time(), "fail")
            self._log_trade("ENTRY_FAILED", symbol, entry_price, qty, sig, order_result)
            logger.warning(
                "ENTRY FAILED %s (%s): %s",
                symbol, direction,
                order_result.get("error") or order_result.get("status") or order_result,
            )
            return

        # P0: After market order fills, the entry order is closed on Delta.
        # Bracket SL/TP become separate stop orders. We MUST fetch their IDs
        # before doing any BE/trailing updates — those need the stop order id,
        # not the (now-closed) entry order id.
        sl_order_id, tp_order_id = None, None
        if not self.config.get("dry_run", True):
            fill = self._extract_fill_price(order_result, entry_price)
            if fill != entry_price:
                logger.info(
                    "Fill price %.6f differs from signal %.6f for %s — adjusting SL/TP",
                    fill, entry_price, symbol,
                )
                entry_price = fill
                sl_price, tp_price = self._compute_sl_tp(direction, entry_price, sig.get("atr"))
                # NOTE: bracket edit on filled entry order usually fails (open_order_not_found).
                # We log the adjusted levels; the stop order fetch below will get the actual IDs.
                logger.info(
                    "Recomputed SL/TP after fill for %s: SL=%s TP=%s (exchange stop orders fetched below)",
                    symbol, sl_price, tp_price,
                )

            # Fetch the open stop order IDs that Delta created as bracket legs.
            # These are needed for BE / trailing SL updates (not the entry order id).
            try:
                sl_order_id, tp_order_id = self._fetch_open_stop_order_ids(product_id)
                if sl_order_id or tp_order_id:
                    logger.info("Fetched stop order ids for %s: SL_id=%s TP_id=%s", symbol, sl_order_id, tp_order_id)
                else:
                    logger.info("No open bracket stop orders auto-created by Delta for %s after market entry fill", symbol)
            except Exception as e:
                logger.warning("Could not fetch stop order ids for %s: %s", symbol, e)

            # Fallback: If exchange bracket legs were not auto-created by Delta,
            # explicitly place exchange-side Stop Loss and Take Profit stop orders now.
            stop_side = "sell" if direction == "long" else "buy"
            if not sl_order_id and self.place_order_fn:
                try:
                    sl_res = self.place_order_fn({
                        "product_id": product_id,
                        "size": qty,
                        "side": stop_side,
                        "order_type": "market_order",
                        "stop_order_type": "stop_loss_order",
                        "stop_price": str(sl_price),
                        "reduce_only": True,
                    })
                    if isinstance(sl_res, dict) and not sl_res.get("error"):
                        sl_order_id = _extract_order_id(sl_res)
                        logger.info("Placed exchange SL stop order for %s at %s, id=%s", symbol, sl_price, sl_order_id)
                    else:
                        logger.warning("Could not place exchange SL stop for %s: %s", symbol, sl_res)
                except Exception as ex:
                    logger.warning("Exception placing exchange SL stop for %s: %s", symbol, ex)

            if not tp_order_id and self.place_order_fn:
                try:
                    tp_res = self.place_order_fn({
                        "product_id": product_id,
                        "size": qty,
                        "side": stop_side,
                        "order_type": "market_order",
                        "stop_order_type": "take_profit_order",
                        "stop_price": str(tp_price),
                        "reduce_only": True,
                    })
                    if isinstance(tp_res, dict) and not tp_res.get("error"):
                        tp_order_id = _extract_order_id(tp_res)
                        logger.info("Placed exchange TP stop order for %s at %s, id=%s", symbol, tp_price, tp_order_id)
                    else:
                        logger.warning("Could not place exchange TP stop for %s: %s", symbol, tp_res)
                except Exception as ex:
                    logger.warning("Exception placing exchange TP stop for %s: %s", symbol, ex)

        with self.lock:
            # FIX: Generate a unique trade_id per trade lifecycle.
            # This id is shared by ENTRY, all PARTIAL_TP events, and the final EXIT
            # so check_pnl.py can correctly aggregate PnL for the whole trade.
            trade_id = str(uuid.uuid4())[:12]
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
                "sl_order_id": sl_order_id,   # stop order id (not entry order id)
                "tp_order_id": tp_order_id,
                "recovered": False,
                "trade_id": trade_id,
            }
            self.trades_today += 1
            self.failed_symbols.pop(symbol, None)

        self._log_trade("ENTRY", symbol, entry_price, qty, sig, order_result, trade_id=trade_id)
        logger.info(
            "ENTRY %s [%s] qty=%s cv=%s score=%.3f sl=%s tp=%s trade_id=%s",
            symbol, direction, qty, contract_value, sig["score"], sl_price, tp_price, trade_id,
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

    def _get_last_fill_price(self, product_id, side=None, after_ts=None, order_id=None):
        """Fetch last fill price for a product. Optionally filter by side,
        after_ts (epoch float), and order_id for precise partial TP matching.
        Returns float price or None."""
        if not self.signed_request_fn:
            return None
        try:
            ok, status, fills_data = self.signed_request_fn(
                "GET", "/v2/fills",
                query_params={"product_ids": str(product_id), "page_size": 20},
            )
            if not ok or not fills_data:
                return None
            res = fills_data.get("result", fills_data) if isinstance(fills_data, dict) else fills_data
            if not isinstance(res, list):
                return None

            # P1: Sort fills by timestamp descending so we always look at the
            # most-recent fill first regardless of API return order.
            def _fill_ts(f):
                ft = f.get("created_at") or f.get("timestamp") or f.get("time")
                if ft is None:
                    return 0.0
                try:
                    if isinstance(ft, str):
                        return datetime.fromisoformat(ft.replace("Z", "+00:00")).timestamp()
                    return float(ft)
                except Exception:
                    return 0.0

            sorted_fills = sorted(res, key=_fill_ts, reverse=True)

            for f in sorted_fills:
                if not isinstance(f, dict):
                    continue
                # FIX B: If order_id provided, match exactly — avoids picking
                # up a concurrent fill from a different partial or SL hit.
                if order_id is not None:
                    fill_order_id = f.get("order_id") or f.get("id")
                    if fill_order_id is not None and str(fill_order_id) != str(order_id):
                        continue
                if side and f.get("side") and f["side"].lower() != side.lower():
                    continue
                if after_ts:
                    ft_val = _fill_ts(f)
                    if ft_val > 0 and ft_val < after_ts:
                        continue
                for k in ("price", "fill_price", "average_fill_price"):
                    if f.get(k) is not None:
                        try:
                            return float(f[k])
                        except (TypeError, ValueError):
                            pass
        except Exception as e:
            logger.warning("Failed to fetch fill price for product %s: %s", product_id, e)
        return None

    def _fetch_open_stop_order_ids(self, product_id):
        """Fetch open stop/bracket order ids for a product from Delta exchange.
        Returns (sl_order_id, tp_order_id) — either may be None."""
        if not self.signed_request_fn:
            return None, None
        try:
            ok, _, orders_resp = self.signed_request_fn(
                "GET", "/v2/orders",
                query_params={"product_ids": str(product_id), "states": "open,pending"},
            )
            if not ok or not orders_resp:
                return None, None
            o_list = orders_resp.get("result", orders_resp) if isinstance(orders_resp, dict) else orders_resp
            if not isinstance(o_list, list):
                return None, None

            sl_id, tp_id = None, None
            for o in o_list:
                if not isinstance(o, dict):
                    continue
                # Delta stop orders: stop_order_type = "stop_loss_order" / "take_profit_order"
                # Bracket legs may also appear as "bracket_stop_loss" / "bracket_take_profit" in order_type
                ot = str(o.get("stop_order_type") or o.get("order_type") or "").lower()
                oid = o.get("id") or o.get("order_id")
                if not oid:
                    continue
                if "stop_loss" in ot or "bracket_stop" in ot:
                    sl_id = oid
                elif "take_profit" in ot or "bracket_take" in ot:
                    tp_id = oid
            return sl_id, tp_id
        except Exception as e:
            logger.warning("_fetch_open_stop_order_ids failed for product %s: %s", product_id, e)
            return None, None

    # -- monitoring / exit ---------------------------------------------
    def _check_breakeven_sl(self, symbol):
        if not self.config.get("enable_breakeven_sl", True):
            return

        with self.lock:
            trade = self.open_trades.get(symbol)
            if not trade or trade.get("breakeven_activated"):
                return
            if time.time() - trade.get("last_be_update_attempt", 0) < 60:
                return
            trade_copy = dict(trade)

        entry_price = trade_copy.get("entry_price")
        direction = trade_copy.get("direction", "long")
        if not entry_price or entry_price <= 0:
            return

        candles = self.feed.get_candles(symbol, limit=3)
        if not candles:
            return
        curr_price = candles[-1]["close"]

        trigger_pct = float(self.config.get("breakeven_trigger_pct", 1.0) or 1.0)
        should_activate = False

        if direction == "long" and curr_price >= entry_price * (1 + trigger_pct / 100):
            should_activate = True
            new_sl = _smart_round(entry_price * 1.0005)  # Cover slight round-trip fee
        elif direction == "short" and curr_price <= entry_price * (1 - trigger_pct / 100):
            should_activate = True
            new_sl = _smart_round(entry_price * 0.9995)

        if should_activate:
            with self.lock:
                if symbol in self.open_trades:
                    self.open_trades[symbol]["last_be_update_attempt"] = time.time()

            # P0: Use sl_order_id (the stop leg's order id) for updates, NOT the
            # entry order id (which is closed after fill).
            if not self.config.get("dry_run", True) and self.place_order_fn:
                try:
                    product_id = trade_copy.get("product_id")
                    if not product_id:
                        logger.warning("Cannot update live break-even SL for %s: missing product_id", symbol)
                        return
                    # Prefer sl_order_id; fall back to a fresh fetch if missing
                    sl_oid = trade_copy.get("sl_order_id")
                    if not sl_oid:
                        sl_oid, _ = self._fetch_open_stop_order_ids(product_id)
                        if sl_oid:
                            with self.lock:
                                if symbol in self.open_trades:
                                    self.open_trades[symbol]["sl_order_id"] = sl_oid
                    if sl_oid:
                        order_body = {
                            "id": sl_oid,
                            "product_id": product_id,
                            "stop_price": str(new_sl),
                        }
                        logger.info("Sending break-even SL update for %s via stop order %s", symbol, sl_oid)
                        res = self.place_order_fn(order_body)
                        if isinstance(res, dict) and (res.get("error") or self._order_looks_rejected(res)):
                            logger.warning("Live break-even SL update REJECTED for %s: %s", symbol, res)
                            return  # ABORT: Do not update internal state if exchange update failed!
                    else:
                        logger.info("No stop order id for %s (recovered/unfound) — updating internal SL to %.6f only", symbol, new_sl)
                except Exception as e:
                    logger.warning("Could not update live bracket SL on Delta for %s: %s", symbol, e)
                    return  # ABORT: Do not update internal state if exchange call raised exception!

            with self.lock:
                if symbol in self.open_trades:
                    self.open_trades[symbol]["sl_price"] = new_sl
                    self.open_trades[symbol]["breakeven_activated"] = True

            logger.info(
                "BREAKEVEN SL ACTIVATED for %s [%s]: entry=%.6f, curr=%.6f, new SL=%.6f",
                symbol, direction, entry_price, curr_price, new_sl
            )
            self._log_trade(
                "BREAKEVEN_SL", symbol, curr_price, trade_copy.get("qty", 1),
                {"direction": direction, "score": trade_copy.get("score")},
                {"note": f"SL moved to break-even ({new_sl})"}
            )

    def _check_trailing_sl(self, symbol):
        if not self.config.get("enable_trailing_sl", True):
            return

        with self.lock:
            trade = self.open_trades.get(symbol)
            if not trade:
                return
            if time.time() - trade.get("last_trailing_update_attempt", 0) < 60:
                return
            trade_copy = dict(trade)

        entry_price = trade_copy.get("entry_price")
        current_sl = trade_copy.get("sl_price")
        direction = trade_copy.get("direction", "long")
        if not entry_price or not current_sl or entry_price <= 0:
            return

        candles = self.feed.get_candles(symbol, limit=3)
        if not candles:
            return
        curr_price = candles[-1]["close"]

        dist_pct = float(self.config.get("trailing_distance_pct", 1.0) or 1.0)
        step_pct = float(self.config.get("trailing_step_pct", 0.3) or 0.3)

        updated_sl = None
        if direction == "long":
            target_sl = _smart_round(curr_price * (1 - dist_pct / 100))
            min_new_sl = max(_smart_round(current_sl * (1 + step_pct / 100)), current_sl)
            if target_sl >= min_new_sl and target_sl > current_sl:
                updated_sl = target_sl
        elif direction == "short":
            target_sl = _smart_round(curr_price * (1 + dist_pct / 100))
            max_new_sl = min(_smart_round(current_sl * (1 - step_pct / 100)), current_sl)
            if target_sl <= max_new_sl and target_sl < current_sl:
                updated_sl = target_sl

        if updated_sl is not None:
            with self.lock:
                if symbol in self.open_trades:
                    self.open_trades[symbol]["last_trailing_update_attempt"] = time.time()

            # P0: Use sl_order_id (the stop leg) for trailing SL updates.
            if not self.config.get("dry_run", True) and self.place_order_fn:
                try:
                    product_id = trade_copy.get("product_id")
                    if not product_id:
                        logger.warning("Cannot update live trailing SL for %s: missing product_id", symbol)
                        return
                    sl_oid = trade_copy.get("sl_order_id")
                    if not sl_oid:
                        sl_oid, _ = self._fetch_open_stop_order_ids(product_id)
                        if sl_oid:
                            with self.lock:
                                if symbol in self.open_trades:
                                    self.open_trades[symbol]["sl_order_id"] = sl_oid
                    if sl_oid:
                        order_body = {
                            "id": sl_oid,
                            "product_id": product_id,
                            "stop_price": str(updated_sl),
                        }
                        res = self.place_order_fn(order_body)
                        if isinstance(res, dict) and (res.get("error") or self._order_looks_rejected(res)):
                            logger.warning("Live trailing SL update REJECTED for %s: %s", symbol, res)
                            return  # ABORT: Do not update internal state if exchange update failed!
                    else:
                        logger.info("No stop order id for %s — updating internal trailing SL to %.6f only", symbol, updated_sl)
                except Exception as e:
                    logger.warning("Could not update live trailing SL for %s: %s", symbol, e)
                    return  # ABORT: Do not update internal state if exchange call raised exception!

            with self.lock:
                if symbol in self.open_trades:
                    self.open_trades[symbol]["sl_price"] = updated_sl

            logger.info(
                "TRAILING SL UPDATED for %s [%s]: curr=%.6f, old SL=%.6f, new SL=%.6f",
                symbol, direction, curr_price, current_sl, updated_sl
            )
            self._log_trade(
                "TRAILING_SL", symbol, curr_price, trade_copy.get("qty", 1),
                {"direction": direction, "score": trade_copy.get("score")},
                {"note": f"Trailing SL moved to {updated_sl}"}
            )

    def _check_partial_tp(self, symbol):
        if not self.config.get("enable_partial_tp", True):
            return

        with self.lock:
            trade = self.open_trades.get(symbol)
            # Low: in_closing guard prevents partial TP racing with a concurrent full exit
            if not trade or trade.get("partial_tp_done") or trade.get("in_closing"):
                return
            trade_copy = dict(trade)

        entry_price = trade_copy.get("entry_price")
        direction = trade_copy.get("direction", "long")
        qty = trade_copy.get("qty", 1)
        if not entry_price or entry_price <= 0 or qty <= 1:
            return

        candles = self.feed.get_candles(symbol, limit=3)
        if not candles:
            return
        curr_price = candles[-1]["close"]

        trigger_pct = float(self.config.get("partial_tp_trigger_pct", 1.0) or 1.0)
        ratio = float(self.config.get("partial_tp_ratio", 0.5) or 0.5)

        triggered = False
        if direction == "long" and curr_price >= entry_price * (1 + trigger_pct / 100.0):
            triggered = True
        elif direction == "short" and curr_price <= entry_price * (1 - trigger_pct / 100.0):
            triggered = True

        if triggered:
            exit_qty = max(1, int(qty * ratio))
            rem_qty = qty - exit_qty
            be_sl = _smart_round(entry_price * 1.0005) if direction == "long" else _smart_round(entry_price * 0.9995)
            sl_moved = True

            if not self.config.get("dry_run", True) and self.place_order_fn:
                product_id = trade_copy.get("product_id")
                if not product_id:
                    return
                exit_side = "sell" if direction == "long" else "buy"
                try:
                    res = self.place_order_fn({
                        "product_id": product_id,
                        "size": exit_qty,
                        "side": exit_side,
                        "order_type": "market_order",
                        "reduce_only": True,
                    })
                except Exception as e:
                    logger.error("Partial TP order raised for %s (%s) -- marking done to avoid duplicate", symbol, e)
                    with self.lock:
                        if symbol in self.open_trades:
                            self.open_trades[symbol]["partial_tp_done"] = True
                    return

                if isinstance(res, dict) and (res.get("error") or self._order_looks_rejected(res)):
                    logger.warning("Live partial TP reduce-only order failed for %s: %s", symbol, res)
                    return

                # FIX 1: Use sl_order_id for BE SL update after partial TP.
                # entry order_result contains the ENTRY order id which is
                # closed after fill — bracket edits need the stop order id.
                # Pattern mirrors _check_breakeven_sl / _check_trailing_sl.
                sl_moved = False
                try:
                    sl_oid, tp_oid = self._fetch_open_stop_order_ids(product_id)
                    if not sl_oid:
                        sl_oid = trade_copy.get("sl_order_id")
                    if sl_oid:
                        sl_body = {
                            "id": sl_oid,
                            "product_id": product_id,
                            "stop_price": str(be_sl),
                            "size": rem_qty,
                        }
                        sl_res = self.place_order_fn(sl_body)
                        if isinstance(sl_res, dict) and (sl_res.get("error") or self._order_looks_rejected(sl_res)):
                            logger.warning("BE SL update after partial TP rejected for %s: %s", symbol, sl_res)
                        else:
                            sl_moved = True
                    else:
                        logger.info("Partial TP: no stop order id for %s — internal SL set to BE %.6f", symbol, be_sl)
                        sl_moved = True

                    if tp_oid and rem_qty > 0:
                        tp_body = {
                            "id": tp_oid,
                            "product_id": product_id,
                            "size": rem_qty,
                        }
                        try:
                            self.place_order_fn(tp_body)
                            logger.info("Updated exchange TP size to %d after partial TP for %s", rem_qty, symbol)
                        except Exception as e:
                            logger.warning("Could not update exchange TP size for %s: %s", symbol, e)
                except Exception as e:
                    logger.warning("BE SL / TP update after partial TP failed for %s: %s", symbol, e)

            if sl_moved:
                logger.info(
                    "💰 PARTIAL TAKE-PROFIT TRIGGERED for %s [%s]: Exiting %d of %d contracts at %.6f (+%.2f%%). Setting BE SL=%.6f for remaining %d",
                    symbol, direction, exit_qty, qty, curr_price, trigger_pct, be_sl, rem_qty
                )
            else:
                logger.info(
                    "💰 PARTIAL TAKE-PROFIT TRIGGERED for %s [%s]: Exiting %d of %d contracts at %.6f (+%.2f%%). Exchange BE SL update failed (will retry in 60s)",
                    symbol, direction, exit_qty, qty, curr_price, trigger_pct, rem_qty
                )

            # FIX 2 + FIX B: Fetch actual partial TP fill price using order_id
            # for exact match. Falls back to side+after_ts if order_id unavailable.
            # Previously curr_price (candle close) was used — live market orders
            # have slippage between trigger price and actual fill price.
            effective_price = curr_price  # fallback if fill fetch fails
            if not self.config.get("dry_run", True) and self.place_order_fn:
                try:
                    # Extract partial TP order_id from the order response
                    partial_oid = _extract_order_id(res) if isinstance(res, dict) else None
                    fill_p = self._get_last_fill_price(
                        product_id,
                        side=exit_side,
                        after_ts=trade_copy.get("entry_ts"),
                        order_id=partial_oid,  # FIX B: exact match by order_id
                    )
                    if fill_p is not None:
                        effective_price = fill_p
                        logger.info(
                            "Partial TP fill price for %s: %.6f (candle was %.6f, diff=%.6f, order_id=%s)",
                            symbol, fill_p, curr_price, fill_p - curr_price, partial_oid
                        )
                except Exception as e:
                    logger.warning("Could not fetch partial TP fill price for %s: %s — using candle price", symbol, e)
            pnl = self._pnl_for_trade({**trade_copy, "qty": exit_qty}, effective_price)

            with self.lock:
                if symbol in self.open_trades:
                    self.open_trades[symbol]["qty"] = rem_qty
                    self.open_trades[symbol]["partial_tp_done"] = True
                    self.open_trades[symbol]["last_be_update_attempt"] = time.time()
                    if sl_moved:
                        self.open_trades[symbol]["sl_price"] = be_sl
                        self.open_trades[symbol]["breakeven_activated"] = True
                    self.realized_pnl_today += pnl

            self._log_trade(
                "PARTIAL_TP", symbol, effective_price, exit_qty,
                {"direction": direction, "score": trade_copy.get("score")},
                {"note": f"Partial exit {exit_qty}/{qty} @ {effective_price:.6f} (trigger={curr_price:.6f})" + (f", SL moved to BE ({be_sl})" if sl_moved else ", SL update pending")},
                pnl=pnl,
                trade_id=trade_copy.get("trade_id"),
            )

    def _check_recovered_position_exits(self, symbol):
        """Monitors recovered or missing-order-id positions in live mode and executes a reduce-only market close when internal SL/TP is breached."""
        with self.lock:
            trade = self.open_trades.get(symbol)
            if not trade or trade.get("in_closing"):
                return
            trade_copy = dict(trade)

        order_id = _extract_order_id(trade_copy.get("order_result"))
        if order_id and not trade_copy.get("recovered"):
            return

        direction = trade_copy.get("direction", "long")
        sl, tp = trade_copy.get("sl_price"), trade_copy.get("tp_price")
        if sl is None and tp is None:
            return

        candles = self.feed.get_candles(symbol, limit=3)
        if not candles:
            return
        curr_price = candles[-1]["close"]

        hit_reason = None
        if direction == "long":
            if sl is not None and curr_price <= sl:
                hit_reason = f"Internal SL hit ({curr_price:.6f} <= {sl:.6f})"
            elif tp is not None and curr_price >= tp:
                hit_reason = f"Internal TP hit ({curr_price:.6f} >= {tp:.6f})"
        else:
            if sl is not None and curr_price >= sl:
                hit_reason = f"Internal SL hit ({curr_price:.6f} >= {sl:.6f})"
            elif tp is not None and curr_price <= tp:
                hit_reason = f"Internal TP hit ({curr_price:.6f} <= {tp:.6f})"

        if hit_reason:
            logger.info("⚡ Live internal trigger for %s [%s]: %s — placing market exit", symbol, direction, hit_reason)
            with self.lock:
                if symbol in self.open_trades:
                    self.open_trades[symbol]["in_closing"] = True
            if not self.config.get("dry_run", True) and self.place_order_fn:
                exit_side = "sell" if direction == "long" else "buy"
                try:
                    res = self.place_order_fn({
                        "product_id": trade_copy["product_id"],
                        "size": max(abs(int(trade_copy.get("qty", 1))), 1),
                        "side": exit_side,
                        "order_type": "market_order",
                        "reduce_only": True
                    })
                    fill = self._get_last_fill_price(trade_copy["product_id"], side=exit_side, after_ts=trade_copy.get("entry_ts"))
                    exit_p = fill if fill is not None else curr_price
                    self._close_trade(symbol, trade_copy, exit_p, hit_reason)
                except Exception as e:
                    logger.error("Failed to execute live internal exit for %s: %s", symbol, e)
                    with self.lock:
                        if symbol in self.open_trades:
                            self.open_trades[symbol].pop("in_closing", None)
            else:
                self._close_trade(symbol, trade_copy, curr_price, hit_reason)

    def _monitor_loop(self):
        while self.running:
            try:
                with self.lock:
                    symbols = list(self.open_trades.keys())
                for symbol in symbols:
                    self._check_partial_tp(symbol)
                    self._check_breakeven_sl(symbol)
                    self._check_trailing_sl(symbol)
                    self._check_recovered_position_exits(symbol)
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
            self.symbol_trades_today[symbol] += 1
            if pnl < 0:
                self.symbol_losses_today[symbol] += 1
            self.open_trades.pop(symbol, None)
            # FIX: Use same loss-limit logic as _scan_loop — max_daily_loss_pct
            # (% of capital) takes precedence over absolute max_daily_loss.
            # Previously this used only the absolute $, causing inconsistency
            # when max_daily_loss_pct was set.
            daily_loss_pct = float(self.config.get("max_daily_loss_pct", 0) or 0)
            if daily_loss_pct > 0:
                capital = float(self.config.get("capital", 50000))
                loss_limit = -(capital * daily_loss_pct / 100.0)
            else:
                loss_limit = -abs(float(self.config.get("max_daily_loss", 1000.0) or 1000.0))
            if self.realized_pnl_today <= loss_limit:
                self.circuit_broken = True
                logger.warning(
                    "CIRCUIT BREAKER TRIGGERED: Daily loss limit (%.4f) reached! Realized PnL today: %.4f",
                    loss_limit, self.realized_pnl_today
                )
            self.failed_symbols[symbol] = (time.time(), "exit")
        self._log_trade(
            "EXIT", symbol, exit_price, trade["qty"],
            {"direction": trade.get("direction"), "score": trade.get("score")},
            {"note": note},
            pnl=pnl,
            trade_id=trade.get("trade_id"),
        )
        logger.info(
            "EXIT %s [%s] %s pnl=%.4f",
            symbol, trade.get("direction"), note, pnl,
        )

    def _check_dry_run_exit(self, symbol):
        with self.lock:
            trade = self.open_trades.get(symbol)
            if not trade or trade.get("in_closing"):
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
            if not trade or trade.get("in_closing"):
                return
            trade = dict(trade)

        # FIX: Recovered positions have no SL/TP (exchange bracket unknown).
        # Force-close them after recovered_max_hold_sec to avoid hanging forever.
        # In live mode: send a real reduce-only market order to actually close
        # the position on the exchange before logging the internal close.
        if trade.get("recovered"):
            max_hold = self.config.get("recovered_max_hold_sec", 3600)
            entry_ts = trade.get("entry_ts") or 0
            if max_hold and entry_ts and (time.time() - entry_ts) >= max_hold:
                candles = self.feed.get_candles(symbol, limit=3)
                exit_price = candles[-1]["close"] if candles else trade["entry_price"]
                logger.warning(
                    "RECOVERED position %s hit max hold (%ds) with no bracket — forcing close at %.6f",
                    symbol, max_hold, exit_price,
                )
                # FIX: In live mode, send reduce-only market order to actually
                # close the position on the exchange.
                if not self.config.get("dry_run", True) and self.place_order_fn is not None:
                    close_side = "sell" if trade.get("direction") == "long" else "buy"
                    close_body = {
                        "product_id": trade["product_id"],
                        "size": max(abs(int(trade.get("qty", 1))), 1),
                        "side": close_side,
                        "order_type": "market_order",
                        "reduce_only": True,
                    }
                    try:
                        close_result = self.place_order_fn(close_body)
                        if isinstance(close_result, dict) and close_result.get("error"):
                            logger.error(
                                "RECOVERED force-close order FAILED for %s: %s — position still open on exchange!",
                                symbol, close_result["error"],
                            )
                            return  # don't remove from internal state if order failed
                        logger.info("RECOVERED force-close order placed for %s: %s", symbol, close_result)
                        # Use actual fill price if available
                        fill = close_result.get("average_fill_price") if isinstance(close_result, dict) else None
                        if fill:
                            try:
                                exit_price = float(fill)
                            except (TypeError, ValueError):
                                pass
                    except Exception as e:
                        logger.error(
                            "RECOVERED force-close order EXCEPTION for %s: %s — position still open on exchange!",
                            symbol, e,
                        )
                        return
                self._close_trade(symbol, trade, exit_price, "recovered max_hold")
                return

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

        fill_price = self._get_last_fill_price(trade["product_id"], after_ts=trade.get("entry_ts"))
        if fill_price is not None:
            exit_price = fill_price
        else:
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
    def _log_trade(self, action, symbol, price, qty, sig, order_result, pnl=None, trade_id=None):
        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "symbol": symbol,
            "price": price,
            "qty": qty,
            "direction": sig.get("direction") if sig else None,
            "score": sig.get("score") if sig else None,
            "pnl": pnl,
            # FIX: trade_id groups ENTRY → PARTIAL_TP → EXIT into one logical
            # trade so check_pnl.py can compute correct trade-level PnL.
            "trade_id": trade_id,
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
            scan_grid = list(self.last_scan.values())
            trades_today = self.trades_today
            realized = self.realized_pnl_today
            running = self.running
            circuit_broken = self.circuit_broken

        scan_grid.sort(
            key=lambda s: (not s.get("qualifies", False), -(s.get("score") or 0))
        )

        return {
            "running": running,
            "is_running": running,
            "strategy_running": running,
            "circuit_broken": circuit_broken,
            "btc_crash_active": getattr(self, "btc_crash_active", False),
            "session_active": self._is_within_trading_session(),
            "config": self.config,
            "trades_today": trades_today,
            "max_trades_per_day": self.config["max_trades_per_day"],
            "open_trades": open_trades,
            "open_positions": open_trades,
            "active_trades_count": len(open_trades),
            "max_concurrent_trades": self.config["max_concurrent_trades"],
            "realized_pnl_today": round(realized, 4),
            "failed_symbols_cooldown": {
                s: (v[0] if isinstance(v, tuple) else v)
                for s, v in failed_symbols.items()
                if not (isinstance(v, tuple) and v[1] == "exit")
            },
            "exit_cooldown": {
                s: v[0]
                for s, v in failed_symbols.items()
                if isinstance(v, tuple) and v[1] == "exit"
            },
            "candidates": sorted(
                [s for s in scan_grid if s.get("qualifies")],
                key=lambda s: -s["score"],
            )[:10],
            "scan_grid": scan_grid,
            "total_symbols": len(self.symbols),
            "recent_trades": list(reversed(self.trade_log[-20:])),
        }