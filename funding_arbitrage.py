"""
Funding Rate Arbitrage Engine (Short-Perp Funding Farm)
=======================================================
Runs independently of StrategyManager.

Strategy:
  - Scan Delta perpetual tickers for high positive 8h funding rates.
  - Open short perpetual positions to collect funding paid by longs.
  - Exit when funding rate collapses or turns negative.
  - Supports both dry-run simulation and live execution.

Note: This is NOT true delta-neutral (long spot + short perp).
True delta-neutral requires a reliable long spot/margin leg, which is
limited on Delta India for many assets. This engine focuses on the
high-probability side: shorting high positive funding perps.
"""

import os
import json
import time
import logging
import threading
import tempfile
from datetime import datetime, timezone
from typing import Optional, Callable, Dict, Any, List

import requests

logger = logging.getLogger("delta-funding-arbitrage")

FUNDING_TRADE_LOG_FILE = "delta_funding_trades.json"
FUNDING_STATE_FILE = "delta_funding_state.json"
REST_BASE = os.getenv("DELTA_BASE_URL", "https://api.india.delta.exchange")

DEFAULT_FUNDING_CONFIG = {
    "enabled": True,
    "dry_run": True,
    "capital": 100.0,                    # Total capital allocated to this strategy ($)
    "position_size_usd": 50.0,           # Notional size per position ($)
    "max_concurrent_pairs": 2,           # Max open short positions
    "min_funding_rate_8h": 0.0005,       # Entry: +0.05% per 8h
    "exit_funding_rate_8h": 0.0001,      # Exit: below +0.01% per 8h
    "min_24h_turnover_usd": 200_000.0,   # Minimum liquidity filter
    "scan_interval_sec": 60,
    "fee_rate_round_trip": 0.0010,       # Approx taker round-trip (0.05% * 2)
    "max_leverage": 3,                   # Soft leverage used when placing live orders
    "symbol_blacklist": [
        "VIRTUALUSD", "BASEDUSD", "MUSD", "WLDUSD", "DOODUSD",
        "TACUSD", "ARIAUSD", "DUSKUSD", "BTWUSD", "AAOIBUSD",
        "OILUSD", "GRAMUSD", "UNIUSD", "EDENUSD",
    ],
}


def _atomic_write_json(path: str, data: Any) -> None:
    """Write JSON atomically to avoid corruption on crash."""
    directory = os.path.dirname(path) or "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


class FundingArbitrageManager:
    def __init__(
        self,
        client=None,
        config: Optional[Dict] = None,
        place_order_fn: Optional[Callable] = None,
        signed_request_fn: Optional[Callable] = None,
        product_id_lookup: Optional[Callable[[str], Optional[int]]] = None,
    ):
        """
        Args:
            client: DeltaRestClient (optional, used only if place_order_fn not given)
            config: overrides for DEFAULT_FUNDING_CONFIG
            place_order_fn: callable(order_body) -> result dict  (preferred)
            signed_request_fn: callable(method, path, query_params=None, body=None)
            product_id_lookup: callable(symbol) -> product_id or None
        """
        self.client = client
        self.config = dict(DEFAULT_FUNDING_CONFIG)
        if config:
            self.config.update(config)

        self.place_order_fn = place_order_fn
        self.signed_request_fn = signed_request_fn
        self.product_id_lookup = product_id_lookup

        self.lock = threading.Lock()
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Runtime state
        self.open_pairs: Dict[str, Dict] = {}
        self.completed_trades: List[Dict] = []
        self.top_opportunities: List[Dict] = []
        self.last_scan_ts = 0.0
        self.total_funding_earned = 0.0
        self.realized_pnl = 0.0

        self._load_state()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _load_state(self) -> None:
        if os.path.exists(FUNDING_STATE_FILE):
            try:
                with open(FUNDING_STATE_FILE, "r", encoding="utf-8") as f:
                    state = json.load(f)
                self.open_pairs = state.get("open_pairs", {})
                self.total_funding_earned = float(state.get("total_funding_earned", 0.0))
                self.realized_pnl = float(state.get("realized_pnl", 0.0))
                logger.info(
                    "[funding-arb] Loaded state: %d open pairs, funding=$%.4f, realized_pnl=$%.4f",
                    len(self.open_pairs),
                    self.total_funding_earned,
                    self.realized_pnl,
                )
            except Exception as e:
                logger.warning("[funding-arb] Could not read %s: %s", FUNDING_STATE_FILE, e)

        if os.path.exists(FUNDING_TRADE_LOG_FILE):
            try:
                with open(FUNDING_TRADE_LOG_FILE, "r", encoding="utf-8") as f:
                    self.completed_trades = json.load(f)
                if not isinstance(self.completed_trades, list):
                    self.completed_trades = []
            except Exception as e:
                logger.warning("[funding-arb] Could not read %s: %s", FUNDING_TRADE_LOG_FILE, e)
                self.completed_trades = []

    def _save_state(self) -> None:
        with self.lock:
            state = {
                "open_pairs": self.open_pairs,
                "total_funding_earned": round(self.total_funding_earned, 6),
                "realized_pnl": round(self.realized_pnl, 6),
                "last_update": datetime.now(timezone.utc).isoformat(),
            }
        try:
            _atomic_write_json(FUNDING_STATE_FILE, state)
        except Exception as e:
            logger.error("[funding-arb] Failed to save %s: %s", FUNDING_STATE_FILE, e)

    def _log_trade(self, record: Dict) -> None:
        with self.lock:
            self.completed_trades.append(record)
            if len(self.completed_trades) > 500:
                self.completed_trades = self.completed_trades[-500:]
            trades_snapshot = list(self.completed_trades)

        try:
            _atomic_write_json(FUNDING_TRADE_LOG_FILE, trades_snapshot)
        except Exception as e:
            logger.error("[funding-arb] Failed to log trade: %s", e)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        with self.lock:
            if self.running:
                return
            self.running = True
            self._stop_event.clear()
            self.thread = threading.Thread(
                target=self._run_loop,
                name="funding-arb-scanner",
                daemon=True,
            )
            self.thread.start()
            logger.info(
                "[funding-arb] Started (dry_run=%s, capital=$%.1f, size=$%.1f)",
                self.config["dry_run"],
                self.config["capital"],
                self.config["position_size_usd"],
            )

    def stop(self) -> None:
        with self.lock:
            if not self.running:
                return
            self.running = False
            self._stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        logger.info("[funding-arb] Stopped")

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------
    def _fetch_all_perpetuals(self) -> List[Dict]:
        try:
            resp = requests.get(f"{REST_BASE}/v2/tickers", timeout=12)
            if resp.status_code != 200:
                logger.warning("[funding-arb] tickers HTTP %s", resp.status_code)
                return []
            rows = resp.json().get("result", [])
            return [r for r in rows if r.get("contract_type") == "perpetual_futures"]
        except Exception as e:
            logger.debug("[funding-arb] Ticker fetch error: %s", e)
            return []

    def _resolve_product_id(self, symbol: str) -> Optional[int]:
        if self.product_id_lookup:
            try:
                return self.product_id_lookup(symbol)
            except Exception:
                pass
        # Fallback: try client if available
        if self.client and hasattr(self.client, "get_products"):
            try:
                products = self.client.get_products()
                rows = products.get("result", products) if isinstance(products, dict) else products
                for row in rows or []:
                    if row.get("symbol") == symbol:
                        return row.get("id")
            except Exception as e:
                logger.debug("[funding-arb] product_id lookup failed for %s: %s", symbol, e)
        return None

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._scan_and_manage()
            except Exception:
                logger.exception("[funding-arb] Error in scan cycle")
            self._stop_event.wait(self.config.get("scan_interval_sec", 60))

    def _scan_and_manage(self) -> None:
        tickers = self._fetch_all_perpetuals()
        if not tickers:
            return

        now_utc = datetime.now(timezone.utc)
        now_ts = now_utc.timestamp()

        blacklist = set(self.config.get("symbol_blacklist", []))
        min_turnover = float(self.config.get("min_24h_turnover_usd", 200_000.0))
        min_rate = float(self.config.get("min_funding_rate_8h", 0.0005))
        exit_rate = float(self.config.get("exit_funding_rate_8h", 0.0001))

        # ---- Rank opportunities ----
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

            # Optimistic annualised rate (for ranking only)
            apr = fr * 3 * 365 * 100.0
            ranked.append({
                "symbol": sym,
                "funding_rate_8h": fr,
                "funding_rate_pct": round(fr * 100, 4),
                "apr_pct": round(apr, 2),
                "turnover_usd": round(turnover, 2),
                "mark_price": float(t.get("mark_price") or t.get("close") or 0.0),
                "product_id": t.get("product_id") or t.get("id"),
            })

        ranked.sort(key=lambda x: -x["funding_rate_8h"])
        with self.lock:
            self.top_opportunities = ranked[:15]
            self.last_scan_ts = now_ts

        # ---- Manage open positions ----
        symbols_to_close = []
        with self.lock:
            open_symbols = list(self.open_pairs.keys())

        for sym in open_symbols:
            pos = self.open_pairs.get(sym)
            if not pos:
                continue
            t = ticker_map.get(sym)
            if not t:
                continue

            current_fr = float(t.get("funding_rate") or 0.0)
            last_payout = float(pos.get("last_payout_ts", pos["entry_ts"]))

            # Accrue every 8h (28 800 s) using the rate observed near settlement.
            # This is still an approximation; real settlement uses the rate at the
            # official funding timestamp.
            elapsed = now_ts - last_payout
            if elapsed >= 28_800:
                fee_earned = pos["size_usd"] * current_fr
                with self.lock:
                    if sym in self.open_pairs:
                        self.open_pairs[sym]["accumulated_funding"] += fee_earned
                        self.open_pairs[sym]["last_payout_ts"] = now_ts
                        self.total_funding_earned += fee_earned

                logger.info(
                    "[funding-arb] Accrued %s: +$%.4f (rate %.4f%%)",
                    sym, fee_earned, current_fr * 100,
                )
                self._log_trade({
                    "action": "FUNDING_PAYOUT",
                    "symbol": sym,
                    "time": now_utc.isoformat(),
                    "funding_rate_pct": round(current_fr * 100, 4),
                    "fee_earned_usd": round(fee_earned, 4),
                    "total_accumulated_usd": round(
                        self.open_pairs.get(sym, {}).get("accumulated_funding", 0.0), 4
                    ),
                })

            if current_fr < exit_rate:
                symbols_to_close.append(
                    (sym, f"Funding rate dropped to {current_fr*100:.4f}%")
                )

        for sym, reason in symbols_to_close:
            self._close_position(sym, reason, ticker_map.get(sym))

        # ---- Open new positions if capacity remains ----
        with self.lock:
            active_count = len(self.open_pairs)
            max_pairs = int(self.config.get("max_concurrent_pairs", 2))
            capital = float(self.config.get("capital", 100.0))
            size_usd = float(self.config.get("position_size_usd", 50.0))
            used_notional = sum(p["size_usd"] for p in self.open_pairs.values())

        if active_count >= max_pairs or (used_notional + size_usd) > capital:
            self._save_state()
            return

        for opp in ranked:
            if active_count >= max_pairs:
                break
            if (used_notional + size_usd) > capital:
                break
            if opp["funding_rate_8h"] < min_rate:
                break
            if opp["symbol"] in self.open_pairs:
                continue

            opened = self._open_position(opp)
            if opened:
                active_count += 1
                used_notional += size_usd

        self._save_state()

    # ------------------------------------------------------------------
    # Position management
    # ------------------------------------------------------------------
    def _open_position(self, opp: Dict) -> bool:
        sym = opp["symbol"]
        size_usd = float(self.config.get("position_size_usd", 50.0))
        now_utc = datetime.now(timezone.utc)
        dry_run = bool(self.config.get("dry_run", True))

        mark = opp.get("mark_price") or 0.0
        if mark <= 0:
            logger.warning("[funding-arb] Invalid mark price for %s, skip", sym)
            return False

        # Approximate contracts from USD notional
        # Most Delta perps are linear USD-settled; size is in contracts.
        # We use a simple notional → size conversion and let the exchange
        # reject if the size is invalid. Live path should ideally query
        # product specs (contract_value / tick_size).
        try:
            size_contracts = max(1, int(round(size_usd / mark)))
        except Exception:
            size_contracts = 1

        product_id = opp.get("product_id")
        if not product_id:
            product_id = self._resolve_product_id(sym)

        order_result = None
        if not dry_run:
            if not product_id:
                logger.error("[funding-arb] No product_id for %s – cannot place live order", sym)
                return False

            order_body = {
                "product_id": int(product_id),
                "size": size_contracts,
                "side": "sell",                 # short the perp
                "order_type": "market_order",
                "time_in_force": "ioc",
                "reduce_only": False,
            }

            try:
                if self.place_order_fn:
                    order_result = self.place_order_fn(order_body)
                elif self.signed_request_fn:
                    ok, status, data = self.signed_request_fn(
                        "POST", "/v2/orders", body=order_body
                    )
                    if not ok:
                        logger.error(
                            "[funding-arb] Live order failed for %s: %s",
                            sym, data,
                        )
                        return False
                    order_result = data.get("result", data)
                else:
                    logger.error("[funding-arb] No place_order_fn / signed_request_fn – cannot trade live")
                    return False

                if isinstance(order_result, dict) and order_result.get("error"):
                    logger.error("[funding-arb] Order error for %s: %s", sym, order_result)
                    return False

            except Exception as e:
                logger.exception("[funding-arb] Exception placing order for %s: %s", sym, e)
                return False

        pos_data = {
            "symbol": sym,
            "product_id": product_id,
            "entry_ts": now_utc.timestamp(),
            "entry_time": now_utc.isoformat(),
            "size_usd": size_usd,
            "size_contracts": size_contracts,
            "entry_funding_rate_pct": opp["funding_rate_pct"],
            "entry_apr_pct": opp["apr_pct"],
            "mark_price": mark,
            "accumulated_funding": 0.0,
            "last_payout_ts": now_utc.timestamp(),
            "status": "OPEN",
            "dry_run": dry_run,
            "order_result": order_result,
        }

        with self.lock:
            self.open_pairs[sym] = pos_data

        logger.info(
            "[funding-arb] ENTERED %s | $%.1f | rate +%.4f%% (APR ~%.1f%%) | dry_run=%s",
            sym, size_usd, opp["funding_rate_pct"], opp["apr_pct"], dry_run,
        )

        self._log_trade({
            "action": "ENTER_SHORT_PERP",
            "symbol": sym,
            "time": now_utc.isoformat(),
            "size_usd": size_usd,
            "size_contracts": size_contracts,
            "funding_rate_pct": opp["funding_rate_pct"],
            "apr_pct": opp["apr_pct"],
            "dry_run": dry_run,
            "note": "Short perpetual to collect positive funding",
        })
        return True

    def _close_position(self, sym: str, reason: str, ticker_data: Optional[Dict] = None) -> None:
        with self.lock:
            pos = self.open_pairs.pop(sym, None)
        if not pos:
            return

        now_utc = datetime.now(timezone.utc)
        hold_sec = now_utc.timestamp() - pos["entry_ts"]
        hold_hours = round(hold_sec / 3600.0, 2)

        fee_rate = float(self.config.get("fee_rate_round_trip", 0.0010))
        est_fees = pos["size_usd"] * fee_rate
        gross_funding = float(pos.get("accumulated_funding", 0.0))
        net_pnl = gross_funding - est_fees

        dry_run = pos.get("dry_run", True)
        close_result = None

        if not dry_run:
            product_id = pos.get("product_id")
            size_contracts = pos.get("size_contracts", 1)
            if product_id:
                order_body = {
                    "product_id": int(product_id),
                    "size": int(size_contracts),
                    "side": "buy",              # close the short
                    "order_type": "market_order",
                    "time_in_force": "ioc",
                    "reduce_only": True,
                }
                try:
                    if self.place_order_fn:
                        close_result = self.place_order_fn(order_body)
                    elif self.signed_request_fn:
                        ok, status, data = self.signed_request_fn(
                            "POST", "/v2/orders", body=order_body
                        )
                        close_result = data.get("result", data) if ok else data
                except Exception as e:
                    logger.exception("[funding-arb] Failed to close live position %s: %s", sym, e)

        with self.lock:
            self.realized_pnl += net_pnl

        logger.info(
            "[funding-arb] CLOSED %s | held %.1fh | gross $%.4f | net $%.4f | %s",
            sym, hold_hours, gross_funding, net_pnl, reason,
        )

        self._log_trade({
            "action": "EXIT_SHORT_PERP",
            "symbol": sym,
            "time": now_utc.isoformat(),
            "hold_hours": hold_hours,
            "gross_funding_usd": round(gross_funding, 4),
            "est_fees_usd": round(est_fees, 4),
            "net_pnl_usd": round(net_pnl, 4),
            "reason": reason,
            "dry_run": dry_run,
            "close_result": close_result,
        })

        self._save_state()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def status(self) -> Dict:
        with self.lock:
            return {
                "running": self.running,
                "strategy_name": "Funding Rate Arbitrage (Short Perp)",
                "dry_run": self.config.get("dry_run", True),
                "capital": self.config.get("capital", 100.0),
                "position_size_usd": self.config.get("position_size_usd", 50.0),
                "active_pairs_count": len(self.open_pairs),
                "open_pairs": dict(self.open_pairs),
                "total_funding_earned_usd": round(self.total_funding_earned, 4),
                "realized_pnl_usd": round(self.realized_pnl, 4),
                "top_opportunities": list(self.top_opportunities),
                "last_scan_time": (
                    datetime.fromtimestamp(self.last_scan_ts, tz=timezone.utc).isoformat()
                    if self.last_scan_ts else None
                ),
                "completed_trades_count": len(self.completed_trades),
            }

    def update_config(self, updates: Dict) -> Dict:
        """Allow runtime config changes (e.g. from Flask route)."""
        with self.lock:
            for k, v in updates.items():
                if k in self.config:
                    self.config[k] = v
            return dict(self.config)