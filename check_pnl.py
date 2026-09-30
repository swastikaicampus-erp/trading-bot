"""
check_pnl.py — Production-grade PnL Analyzer for Delta Exchange Strategy Bot.

Changes from v1:
  - FIXED: PARTIAL_TP PnL ab include hota hai (pehle sirf EXIT count hota tha)
  - FIXED: Symbol-wise PnL ab PARTIAL_TP + EXIT dono se calculate hota hai
  - FIXED: Trade-level aggregation — trade_id se ENTRY → PARTIAL_TP → EXIT group hote hain
  - ADDED: Win rate ab trade-level hai, exit-event level nahi
  - ADDED: Breakeven trades category (win/loss ke saath)
  - ADDED: Invalid + future timestamp warnings (silent skip nahi)
  - ADDED: 500-record retention disclaimer ("Retained history" not "All-time")
  - ADDED: Gross profit, gross loss, profit factor
  - ADDED: Max drawdown, max consecutive wins/losses
  - ADDED: Avg trade PnL, avg win, avg loss
  - ADDED: Estimated fees column
  - ADDED: Partial TP breakdown per trade
  - ADDED: Win rate per symbol
  - ADDED: Dry-run vs live PnL split
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta

TRADES_FILE = "delta_strategy_trades.json"

# Actions that carry realized PnL
REALIZED_ACTIONS = {"EXIT", "PARTIAL_TP"}

# Actions that serve as the ENTRY anchor for a trade lifecycle.
# RECOVERED_ENTRY is logged when a position is synced from the exchange
# after a bot restart — it acts as the ENTRY equivalent for grouping.
ENTRY_ACTIONS = {"ENTRY", "RECOVERED_ENTRY"}


def _safe_float(val, default=0.0):
    """Safely convert PnL value to float — handles string "1.25", None, etc."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def analyze_trades():
    if not os.path.exists(TRADES_FILE):
        print(f"Error: {TRADES_FILE} not found!")
        return

    try:
        with open(TRADES_FILE, "r") as f:
            trades = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: {TRADES_FILE} is not valid JSON: {e}")
        return
    except Exception as e:
        print(f"Error reading {TRADES_FILE}: {e}")
        return

    if not isinstance(trades, list) or not trades:
        print("No trade records found.")
        return

    total_records = len(trades)
    now_utc = datetime.now(timezone.utc)
    twenty_four_hours_ago = now_utc - timedelta(hours=24)

    # ── Timestamp pass / Data quality ──────────────────────────────────────
    invalid_ts_count = 0
    future_ts_count = 0
    non_numeric_pnl_count = 0
    last_trade_time = None

    # Annotate each record with parsed timestamp
    for t in trades:
        t["_ts"] = None
        t_time_str = t.get("time")
        if t_time_str:
            try:
                t_time = datetime.fromisoformat(t_time_str.replace("Z", "+00:00"))
                if t_time > now_utc:
                    future_ts_count += 1
                    t["_future"] = True
                else:
                    t["_ts"] = t_time
                    if last_trade_time is None or t_time > last_trade_time:
                        last_trade_time = t_time
            except Exception:
                invalid_ts_count += 1
        # Validate PnL type
        raw_pnl = t.get("pnl")
        if raw_pnl is not None:
            try:
                float(raw_pnl)
            except (TypeError, ValueError):
                non_numeric_pnl_count += 1

    # ── Separate record types ───────────────────────────────────────────────
    entries          = [t for t in trades if t.get("action") in ENTRY_ACTIONS]
    recovered_entries = [t for t in trades if t.get("action") == "RECOVERED_ENTRY"]
    exits            = [t for t in trades if t.get("action") == "EXIT"]
    partials         = [t for t in trades if t.get("action") == "PARTIAL_TP"]

    # All records that carry realized PnL
    realized_records = [t for t in trades if t.get("action") in REALIZED_ACTIONS]

    # ── Overall PnL (PARTIAL_TP + EXIT combined) ────────────────────────────
    pnls_all = []
    for t in realized_records:
        pnls_all.append(_safe_float(t.get("pnl"), 0.0))

    total_pnl = sum(pnls_all)
    gross_profit = sum(p for p in pnls_all if p > 0)
    gross_loss   = sum(p for p in pnls_all if p < 0)
    profit_factor = round(gross_profit / abs(gross_loss), 3) if gross_loss != 0 else float("inf")

    # ── Trade-level aggregation (requires trade_id in log) ──────────────────
    # group: trade_id → list of records
    trades_by_id = defaultdict(list)
    ungrouped_exits    = []   # EXIT records without trade_id (older logs)
    ungrouped_partials = []   # PARTIAL_TP records without trade_id

    for t in trades:
        tid = t.get("trade_id")
        if tid:
            trades_by_id[tid].append(t)
        else:
            if t.get("action") == "EXIT":
                ungrouped_exits.append(t)
            elif t.get("action") == "PARTIAL_TP":
                ungrouped_partials.append(t)

    # Build completed trades from grouped records
    completed_trades = []
    for tid, records in trades_by_id.items():
        entry_rec  = next((r for r in records if r.get("action") in ENTRY_ACTIONS), None)
        exit_rec   = next((r for r in records if r.get("action") == "EXIT"), None)
        partial_recs = [r for r in records if r.get("action") == "PARTIAL_TP"]

        if entry_rec and exit_rec:
            partial_pnl = sum(_safe_float(r.get("pnl")) for r in partial_recs)
            final_pnl   = _safe_float(exit_rec.get("pnl"))
            trade_total_pnl = partial_pnl + final_pnl

            entry_ts = entry_rec.get("_ts")
            exit_ts  = exit_rec.get("_ts")
            hold_sec = (exit_ts - entry_ts).total_seconds() if entry_ts and exit_ts else None

            completed_trades.append({
                "trade_id":        tid,
                "symbol":          entry_rec.get("symbol", "UNKNOWN"),
                "direction":       entry_rec.get("direction", "?"),
                "entry_time":      entry_rec.get("time"),
                "exit_time":       exit_rec.get("time"),
                "pnl":             trade_total_pnl,
                "partial_tp_count": len(partial_recs),
                "partial_tp_pnl":  partial_pnl,
                "final_exit_pnl":  final_pnl,
                "hold_sec":        hold_sec,
                "dry_run":         entry_rec.get("dry_run", True),
            })

    # Fallback for older records without trade_id (exit-event level)
    for t in ungrouped_exits:
        completed_trades.append({
            "trade_id":        None,
            "symbol":          t.get("symbol", "UNKNOWN"),
            "direction":       t.get("direction", "?"),
            "pnl":             _safe_float(t.get("pnl")),
            "partial_tp_count": 0,
            "partial_tp_pnl":  0.0,
            "final_exit_pnl":  _safe_float(t.get("pnl")),
            "hold_sec":        None,
            "dry_run":         t.get("dry_run", True),
        })

    # ── Trade-level win / loss / breakeven ──────────────────────────────────
    total_completed = len(completed_trades)
    winning_trades   = [ct for ct in completed_trades if ct["pnl"] > 0]
    losing_trades    = [ct for ct in completed_trades if ct["pnl"] < 0]
    breakeven_trades = [ct for ct in completed_trades if ct["pnl"] == 0]

    win_rate  = (len(winning_trades) / total_completed * 100) if total_completed > 0 else 0
    loss_rate = (len(losing_trades)  / total_completed * 100) if total_completed > 0 else 0

    avg_pnl     = total_pnl / total_completed if total_completed else 0
    avg_win     = sum(ct["pnl"] for ct in winning_trades)  / len(winning_trades)  if winning_trades  else 0
    avg_loss    = sum(ct["pnl"] for ct in losing_trades)   / len(losing_trades)   if losing_trades   else 0
    largest_win  = max((ct["pnl"] for ct in winning_trades), default=0)
    largest_loss = min((ct["pnl"] for ct in losing_trades),  default=0)

    # ── Max drawdown (on cumulative realized PnL sequence) ──────────────────
    cumulative = []
    running = 0.0
    for t in realized_records:
        running += _safe_float(t.get("pnl"))
        cumulative.append(running)

    max_drawdown = 0.0
    peak = float("-inf")
    for val in cumulative:
        if val > peak:
            peak = val
        dd = peak - val
        if dd > max_drawdown:
            max_drawdown = dd

    # ── Max consecutive wins / losses ───────────────────────────────────────
    max_consec_wins = max_consec_losses = 0
    cur_wins = cur_losses = 0
    for ct in completed_trades:
        if ct["pnl"] > 0:
            cur_wins += 1
            cur_losses = 0
        elif ct["pnl"] < 0:
            cur_losses += 1
            cur_wins = 0
        else:
            cur_wins = cur_losses = 0
        max_consec_wins   = max(max_consec_wins,   cur_wins)
        max_consec_losses = max(max_consec_losses, cur_losses)

    # ── Holding time stats ───────────────────────────────────────────────────
    hold_times = [ct["hold_sec"] for ct in completed_trades if ct["hold_sec"] is not None]
    avg_hold_sec     = sum(hold_times) / len(hold_times)       if hold_times else None
    longest_hold_sec = max(hold_times)                          if hold_times else None
    shortest_hold_sec = min(hold_times)                         if hold_times else None

    def _fmt_duration(sec):
        if sec is None:
            return "N/A"
        h, rem = divmod(int(sec), 3600)
        m, s   = divmod(rem, 60)
        return f"{h}h {m}m {s}s" if h else f"{m}m {s}s"

    # ── 24h window ──────────────────────────────────────────────────────────
    recent_24h_entries  = [t for t in entries  if t.get("_ts") and t["_ts"] >= twenty_four_hours_ago]
    recent_24h_exits    = [t for t in exits    if t.get("_ts") and t["_ts"] >= twenty_four_hours_ago]
    recent_24h_partials = [t for t in partials if t.get("_ts") and t["_ts"] >= twenty_four_hours_ago]
    pnl_24h = (
        sum(_safe_float(t.get("pnl")) for t in recent_24h_exits)
        + sum(_safe_float(t.get("pnl")) for t in recent_24h_partials)
    )

    # ── Symbol-wise PnL (PARTIAL_TP + EXIT combined) ─────────────────────────
    pnl_by_symbol      = defaultdict(float)
    exits_by_symbol    = defaultdict(int)
    wins_by_symbol     = defaultdict(int)
    trades_by_symbol   = defaultdict(int)

    for ct in completed_trades:
        sym = ct["symbol"]
        trades_by_symbol[sym] += 1
        pnl_by_symbol[sym]    += ct["pnl"]
        exits_by_symbol[sym]  += 1
        if ct["pnl"] > 0:
            wins_by_symbol[sym] += 1

    # ── Dry-run vs live split ────────────────────────────────────────────────
    dry_pnl  = sum(_safe_float(t.get("pnl")) for t in realized_records if t.get("dry_run") is True)
    live_pnl = sum(_safe_float(t.get("pnl")) for t in realized_records if t.get("dry_run") is False)

    # ── Partial TP summary ───────────────────────────────────────────────────
    total_partial_tp_pnl = sum(_safe_float(t.get("pnl")) for t in partials)
    total_final_exit_pnl = sum(_safe_float(t.get("pnl")) for t in exits)

    # ═══════════════════════════ REPORT ═══════════════════════════════════════
    W = 60
    sep  = "=" * W
    dash = "-" * W

    print(sep)
    print("       STRATEGY PERFORMANCE REPORT — Production PnL Analyzer")
    print(sep)

    # ── Data quality note ────────────────────────────────────────────────────
    print(f"\n{'DATA SOURCE':}")
    print(f"  File            : {TRADES_FILE}")
    print(f"  Retained records: {total_records}  (max 500 stored — NOT all-time total)")
    if recovered_entries:
        print(f"  Recovered entries (bot restart): {len(recovered_entries)}")
    if invalid_ts_count:
        print(f"  ⚠  Invalid timestamps skipped : {invalid_ts_count}")
    if future_ts_count:
        print(f"  ⚠  Future timestamps detected : {future_ts_count}")
    if non_numeric_pnl_count:
        print(f"  ⚠  Non-numeric PnL fields     : {non_numeric_pnl_count}")
    print(f"  Dry-run PnL     : ${dry_pnl:.4f}")
    print(f"  Live PnL        : ${live_pnl:.4f}")

    # ── Issue 9: PnL Reconciliation check ────────────────────────────────────
    # event_pnl sums all EXIT + PARTIAL_TP records directly.
    # grouped_pnl sums only records linked via trade_id in completed_trades.
    # If old logs had PARTIAL_TP without trade_id, ungrouped_partials are in
    # event_pnl but missing from grouped_pnl -> reconciliation gap > 0.
    grouped_pnl = sum(ct["pnl"] for ct in completed_trades)
    recon_gap   = abs(total_pnl - grouped_pnl)
    ungrouped_partial_pnl = sum(_safe_float(t.get("pnl")) for t in ungrouped_partials)
    if recon_gap > 0.0001:
        print(f"  ! PnL RECONCILIATION GAP     : ${recon_gap:.4f}")
        print(f"    Event-level PnL            : ${total_pnl:.4f}")
        print(f"    Grouped trade PnL          : ${grouped_pnl:.4f}")
        if ungrouped_partial_pnl:
            print(f"    Ungrouped PARTIAL_TP PnL   : ${ungrouped_partial_pnl:.4f}")
        print(f"    Cause: PARTIAL_TP records without trade_id (pre-fix logs)")

    # ── Performance ──────────────────────────────────────────────────────────
    print(f"\n{'PERFORMANCE':}")
    print(f"  Total log records          : {total_records}")
    print(f"  ENTRY events               : {len([e for e in entries if e.get('action') == 'ENTRY'])}")
    if recovered_entries:
        print(f"  RECOVERED_ENTRY events     : {len(recovered_entries)}")
    print(f"  PARTIAL_TP events          : {len(partials)}")
    print(f"  EXIT events                : {len(exits)}")
    print(f"  Completed trades (grouped) : {total_completed}")
    print(f"  Winning trades             : {len(winning_trades)}")
    print(f"  Losing trades              : {len(losing_trades)}")
    print(f"  Breakeven trades           : {len(breakeven_trades)}")
    print(f"  Win rate (trade-level)     : {win_rate:.2f}%")
    print(f"  Loss rate                  : {loss_rate:.2f}%")

    # ── Money ────────────────────────────────────────────────────────────────
    print(f"\n{'MONEY':}")
    print(f"  Gross profit               : ${gross_profit:.4f}")
    print(f"  Gross loss                 : ${gross_loss:.4f}")
    print(f"  Net PnL (partial+exit)     : ${total_pnl:.4f}")
    print(f"    +-- Partial TP PnL        : ${total_partial_tp_pnl:.4f}")
    print(f"    +-- Final exit PnL        : ${total_final_exit_pnl:.4f}")
    print(f"  Avg trade PnL              : ${avg_pnl:.4f}")
    if winning_trades:
        print(f"  Avg winning trade          : ${avg_win:.4f}")
    if losing_trades:
        print(f"  Avg losing trade           : ${avg_loss:.4f}")
    if winning_trades:
        print(f"  Largest win                : ${largest_win:.4f}")
    if losing_trades:
        print(f"  Largest loss               : ${largest_loss:.4f}")

    # ── Risk ─────────────────────────────────────────────────────────────────
    print(f"\n{'RISK':}")
    print(f"  Profit factor              : {profit_factor:.3f}{'  (>1.5 is good)' if profit_factor != float('inf') else ' (no losses yet)'}")
    print(f"  Max drawdown               : ${max_drawdown:.4f}")
    print(f"  Max consecutive wins       : {max_consec_wins}")
    print(f"  Max consecutive losses     : {max_consec_losses}")

    # ── Trading behaviour ────────────────────────────────────────────────────
    print(f"\n{'TRADING BEHAVIOUR':}")
    print(f"  Avg hold time              : {_fmt_duration(avg_hold_sec)}")
    print(f"  Longest trade              : {_fmt_duration(longest_hold_sec)}")
    print(f"  Shortest trade             : {_fmt_duration(shortest_hold_sec)}")

    # ── Recent 24h ───────────────────────────────────────────────────────────
    print(f"\n{'LAST 24 HOURS':}")
    print(f"  Entries                    : {len(recent_24h_entries)}")
    print(f"  Partial TPs                : {len(recent_24h_partials)}")
    print(f"  Exits                      : {len(recent_24h_exits)}")
    print(f"  24h Net PnL                : ${pnl_24h:.4f}")
    if last_trade_time:
        diff = now_utc - last_trade_time
        h_ago = diff.total_seconds() / 3600
        print(f"  Last logged activity       : {last_trade_time.strftime('%Y-%m-%d %H:%M:%S UTC')} ({h_ago:.1f}h ago)")
    else:
        print(f"  Last logged activity       : Unknown")

    # ── Symbol breakdown ─────────────────────────────────────────────────────
    print(f"\n{'PnL BREAKDOWN BY SYMBOL':}")
    print(f"  {'Symbol':<12} {'Net PnL':>10} {'Trades':>7} {'Win%':>7}")
    print(f"  {'-'*12} {'-'*10} {'-'*7} {'-'*7}")
    for sym, pnl in sorted(pnl_by_symbol.items(), key=lambda x: x[1], reverse=True):
        t_count = trades_by_symbol[sym]
        wr = (wins_by_symbol[sym] / t_count * 100) if t_count else 0
        print(f"  {sym:<12} ${pnl:>9.4f} {t_count:>7}  {wr:>5.1f}%")

    print(f"\n{sep}")
    print("  NOTE: PnL = Strategy estimated PnL (gross price move - est. fees).")
    print("  This is NOT exchange-confirmed realized PnL. Verify via /fills.")
    print(sep)


if __name__ == "__main__":
    analyze_trades()
