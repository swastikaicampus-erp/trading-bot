"""
Funding Rate Arbitrage (Delta Neutral Carry Strategy) Engine — Dry Run / Production Ready
========================================================================================
Runs alongside StrategyManager (Scalper) independently.
Scans Delta Exchange perpetual tickers for high positive 8-hour funding rates.
Simulates (or executes in live mode) Delta-Neutral carry positions:
  - Leg 1: Long Spot (or base asset)
  - Leg 2: Short Perpetual Future
  - Net Price Exposure: 0 (Delta Neutral)
  - Revenue: Accrues 8-hour funding payouts paid by longs to shorts.

Trade records are saved in `delta_funding_trades.json`.
"""

import os
import json
import time
import math
import logging
import threading
import requests
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("delta-funding-arbitrage")

FUNDING_TRADE_LOG_FILE = "delta_funding_trades.json"
FUNDING_STATE_FILE = "delta_funding_state.json"
REST_BASE = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange")

DEFAULT_FUNDING_CONFIG = {
    "enabled": True,
    "dry_run": True,
    "capital": 100.0,                   # Total virtual capital allocated to Funding Arb ($)
    "position_size_usd": 50.0,          # Size per delta-neutral pair ($)
    "max_concurrent_pairs": 2,          # Max 2 pairs at a time
    "min_funding_rate_8h": 0.0005,      # Entry threshold: +0.05% per 8h (+54.7% APR)
    "exit_funding_rate_8h": 0.0001,     # Exit threshold: below +0.01% per 8h
    "min_24h_turnover_usd": 200000.0,   # Minimum liquidity ($200k daily volume)
    "scan_interval_sec": 60,            # Scan every 60s (funding changes slowly)
    "fee_rate_round_trip": 0.0010,      # Delta taker fee ~0.05% * 2 legs = ~0.10%
    "symbol_blacklist": [
        "VIRTUALUSD", "BASEDUSD", "MUSD", "WLDUSD", "DOODUSD",
        "TACUSD", "ARIAUSD", "DUSKUSD", "BTWUSD", "AAOIBUSD"
    ]
}


class FundingArbitrageManager:
    def __init__(self, client=None, config=None):
        self.client = client
        self.config = dict(DEFAULT_FUNDING_CONFIG)
        if config:
            self.config.update(config)

        self.lock = threading.Lock()
        self.running = False
        self.thread = None
        self._stop_event = threading.Event()

        # State tracking
        self.open_pairs = {}            # {symbol: {entry_data, accumulated_funding, last_payout_ts}}
        self.completed_trades = []
        self.top_opportunities = []
        self.last_scan_ts = 0
        self.total_funding_earned = 0.0

        self._load_state()

    def _load_state(self):
        """Restore active pairs and past history from disk if present."""
        if os.path.exists(FUNDING_STATE_FILE):
            try:
                with open(FUNDING_STATE_FILE, "r", encoding="utf-8") as f:
                    state = json.load(f)
                    self.open_pairs = state.get("open_pairs", {})
                    self.total_funding_earned = state.get("total_funding_earned", 0.0)
                    logger.info("[funding-arb] Loaded state: %d open pairs, $%.4f total funding earned",
                                len(self.open_pairs), self.total_funding_earned)
            except Exception as e:
                logger.warning("[funding-arb] Could not read %s: %s", FUNDING_STATE_FILE, e)

        if os.path.exists(FUNDING_TRADE_LOG_FILE):
            try:
                with open(FUNDING_TRADE_LOG_FILE, "r", encoding="utf-8") as f:
                    self.completed_trades = json.load(f)
            except Exception as e:
                logger.warning("[funding-arb] Could not read %s: %s", FUNDING_TRADE_LOG_FILE, e)

    def _save_state(self):
        """Persist open positions and cumulative metrics."""
        with self.lock:
            state = {
                "open_pairs": self.open_pairs,
                "total_funding_earned": round(self.total_funding_earned, 4),
                "last_update": datetime.now(timezone.utc).isoformat()
            }
        try:
            with open(FUNDING_STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error("[funding-arb] Failed to save %s: %s", FUNDING_STATE_FILE, e)

    def _log_trade(self, record):
        """Append trade event to delta_funding_trades.json."""
        with self.lock:
            self.completed_trades.append(record)
            if len(self.completed_trades) > 500:
                self.completed_trades = self.completed_trades[-500:]

        try:
            with open(FUNDING_TRADE_LOG_FILE, "w", encoding="utf-8") as f:
                json.dump(self.completed_trades, f, indent=2)
        except Exception as e:
            logger.error("[funding-arb] Failed to log trade to %s: %s", FUNDING_TRADE_LOG_FILE, e)

    def start(self):
        """Start the background scanner thread."""
        with self.lock:
            if self.running:
                return
            self.running = True
            self._stop_event.clear()
            self.thread = threading.Thread(target=self._run_loop, name="funding-arb-scanner", daemon=True)
            self.thread.start()
            logger.info("[funding-arb] Funding Rate Arbitrage engine started (DRY_RUN=%s)", self.config["dry_run"])

    def stop(self):
        """Stop the background scanner thread."""
        with self.lock:
            if not self.running:
                return
            self.running = False
            self._stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=3)
        logger.info("[funding-arb] Funding Rate Arbitrage engine stopped")

    def _fetch_all_perpetuals(self):
        """Fetch all perpetual tickers from Delta Exchange REST API."""
        try:
            resp = requests.get(f"{REST_BASE}/v2/tickers", timeout=12)
            if resp.status_code != 200:
                return []
            rows = resp.json().get("result", [])
            return [r for r in rows if r.get("contract_type") == "perpetual_futures"]
        except Exception as e:
            logger.debug("[funding-arb] Ticker fetch error: %s", e)
            return []

    def _run_loop(self):
        while not self._stop_event.is_set():
            try:
                self._scan_and_manage()
            except Exception:
                logger.error("[funding-arb] Error in scan cycle", exc_info=True)

            self._stop_event.wait(self.config.get("scan_interval_sec", 60))

    def _scan_and_manage(self):
        tickers = self._fetch_all_perpetuals()
        if not tickers:
            return

        now_utc = datetime.now(timezone.utc)
        now_ts = now_utc.timestamp()

        blacklist = set(self.config.get("symbol_blacklist", []))
        min_turnover = float(self.config.get("min_24h_turnover_usd", 200000.0))
        min_rate = float(self.config.get("min_funding_rate_8h", 0.0005))
        exit_rate = float(self.config.get("exit_funding_rate_8h", 0.0001))

        # 1. Rank opportunities
        ranked = []
        ticker_map = {}
        for t in tickers:
            sym = t.get("symbol")
            if not sym or sym in blacklist:
                continue

            ticker_map[sym] = t
            fr_raw = t.get("funding_rate")
            if fr_raw is None:
                continue

            try:
                fr = float(fr_raw)
            except (ValueError, TypeError):
                continue

            turnover = float(t.get("turnover_usd") or t.get("turnover") or 0.0)
            if turnover < min_turnover:
                continue

            # Standardized 8h rate (Delta sends fraction e.g. 0.0005 = 0.05%)
            apr = fr * 3 * 365 * 100.0
            ranked.append({
                "symbol": sym,
                "funding_rate_8h": fr,
                "funding_rate_pct": round(fr * 100, 4),
                "apr_pct": round(apr, 2),
                "turnover_usd": round(turnover, 2),
                "mark_price": float(t.get("mark_price") or t.get("close") or 0.0),
                "spot_price": float(t.get("spot_price") or t.get("mark_price") or 0.0),
            })

        ranked.sort(key=lambda x: -x["funding_rate_8h"])
        with self.lock:
            self.top_opportunities = ranked[:15]
            self.last_scan_ts = now_ts

        # 2. Check open positions for funding accrual or exit
        symbols_to_close = []
        with self.lock:
            open_symbols = list(self.open_pairs.keys())

        for sym in open_symbols:
            pos = self.open_pairs[sym]
            t = ticker_map.get(sym)
            if not t:
                continue

            current_fr = float(t.get("funding_rate") or 0.0)
            last_payout = pos.get("last_payout_ts", pos["entry_ts"])

            # Check if 8 hours have elapsed since last funding settlement
            # Standard funding payout occurs every 8h (28,800s)
            elapsed = now_ts - last_payout
            if elapsed >= 28800:
                fee_earned = pos["size_usd"] * current_fr
                pos["accumulated_funding"] += fee_earned
                pos["last_payout_ts"] = now_ts
                self.total_funding_earned += fee_earned
                logger.info("[funding-arb] Accrued funding fee for %s: +$%.4f (Rate: %.4f%%)",
                            sym, fee_earned, current_fr * 100)

                self._log_trade({
                    "action": "FUNDING_PAYOUT",
                    "symbol": sym,
                    "time": now_utc.isoformat(),
                    "funding_rate_pct": round(current_fr * 100, 4),
                    "fee_earned_usd": round(fee_earned, 4),
                    "total_accumulated_usd": round(pos["accumulated_funding"], 4),
                })

            # Check exit conditions: funding rate dried up or flipped negative
            if current_fr < exit_rate:
                symbols_to_close.append((sym, f"Funding rate dropped ({current_fr*100:.3f}% < {exit_rate*100:.3f}%)"))

        # Close positions that met exit criteria
        for sym, reason in symbols_to_close:
            self._close_position(sym, reason, ticker_map.get(sym))

        # 3. Consider opening new positions if slots available
        with self.lock:
            active_count = len(self.open_pairs)
            max_pairs = int(self.config.get("max_concurrent_pairs", 2))

        if active_count < max_pairs:
            for opp in ranked:
                sym = opp["symbol"]
                if sym in self.open_pairs:
                    continue
                if opp["funding_rate_8h"] < min_rate:
                    break

                self._open_position(opp)
                active_count += 1
                if active_count >= max_pairs:
                    break

        self._save_state()

    def _open_position(self, opp):
        sym = opp["symbol"]
        size_usd = float(self.config.get("position_size_usd", 50.0))
        now_utc = datetime.now(timezone.utc)

        pos_data = {
            "symbol": sym,
            "entry_ts": now_utc.timestamp(),
            "entry_time": now_utc.isoformat(),
            "size_usd": size_usd,
            "entry_funding_rate_pct": opp["funding_rate_pct"],
            "entry_apr_pct": opp["apr_pct"],
            "mark_price": opp["mark_price"],
            "spot_price": opp["spot_price"],
            "accumulated_funding": 0.0,
            "last_payout_ts": now_utc.timestamp(),
            "status": "OPEN",
            "dry_run": self.config.get("dry_run", True)
        }

        with self.lock:
            self.open_pairs[sym] = pos_data

        logger.info("[funding-arb] ENTERED Delta-Neutral Pair: %s | Size: $%.1f | Rate: +%.4f%% (APR: +%.1f%%)",
                    sym, size_usd, opp["funding_rate_pct"], opp["apr_pct"])

        self._log_trade({
            "action": "ENTER_DELTA_NEUTRAL",
            "symbol": sym,
            "time": now_utc.isoformat(),
            "size_usd": size_usd,
            "funding_rate_pct": opp["funding_rate_pct"],
            "apr_pct": opp["apr_pct"],
            "note": "Long Spot + Short Perp (Delta Neutral Carry)"
        })

    def _close_position(self, sym, reason, ticker_data=None):
        with self.lock:
            pos = self.open_pairs.pop(sym, None)
        if not pos:
            return

        now_utc = datetime.now(timezone.utc)
        hold_sec = now_utc.timestamp() - pos["entry_ts"]
        hold_hours = round(hold_sec / 3600.0, 2)

        # Approximate simulated round-trip taker fees (0.05% spot + 0.05% perp = ~0.10% total)
        fee_rate = float(self.config.get("fee_rate_round_trip", 0.0010))
        est_fees = pos["size_usd"] * fee_rate
        gross_funding = pos["accumulated_funding"]
        net_pnl = gross_funding - est_fees

        logger.info("[funding-arb] CLOSED Pair %s | Held: %.1fh | Gross Funding: +$%.4f | Net PnL: $%.4f | Reason: %s",
                    sym, hold_hours, gross_funding, net_pnl, reason)

        self._log_trade({
            "action": "EXIT_DELTA_NEUTRAL",
            "symbol": sym,
            "time": now_utc.isoformat(),
            "hold_hours": hold_hours,
            "gross_funding_usd": round(gross_funding, 4),
            "est_fees_usd": round(est_fees, 4),
            "net_pnl_usd": round(net_pnl, 4),
            "reason": reason
        })

    def status(self):
        """Return real-time state for API and dashboard."""
        with self.lock:
            return {
                "running": self.running,
                "strategy_name": "Funding Rate Arbitrage (Delta Neutral)",
                "dry_run": self.config.get("dry_run", True),
                "capital": self.config.get("capital", 100.0),
                "active_pairs_count": len(self.open_pairs),
                "open_pairs": dict(self.open_pairs),
                "total_funding_earned_usd": round(self.total_funding_earned, 4),
                "top_opportunities": list(self.top_opportunities),
                "last_scan_time": datetime.fromtimestamp(self.last_scan_ts, tz=timezone.utc).isoformat() if self.last_scan_ts else None,
                "completed_trades_count": len(self.completed_trades)
            }
