import json
import os
from datetime import datetime, timezone, timedelta

TRADES_FILE = "delta_strategy_trades.json"

def analyze_trades():
    if not os.path.exists(TRADES_FILE):
        print(f"Error: {TRADES_FILE} not found!")
        return

    try:
        with open(TRADES_FILE, "r") as f:
            trades = json.load(f)
    except Exception as e:
        print(f"Error reading {TRADES_FILE}: {e}")
        return

    total_records = len(trades)
    exits = [t for t in trades if t.get("action") == "EXIT"]
    entries = [t for t in trades if t.get("action") == "ENTRY"]
    
    total_exits = len(exits)
    if total_exits == 0:
        print("No completed trade exits found.")
        return

    pnls = [t.get("pnl", 0.0) for t in exits if t.get("pnl") is not None]
    winning = [p for p in pnls if p > 0]
    losing = [p for p in pnls if p < 0]
    
    win_rate = (len(winning) / total_exits * 100) if total_exits > 0 else 0
    total_pnl = sum(pnls)

    # Calculate Last Trade Time & 24h Stats
    now_utc = datetime.now(timezone.utc)
    twenty_four_hours_ago = now_utc - timedelta(hours=24)
    
    recent_24h_exits = []
    recent_24h_entries = []
    last_trade_time = None
    
    for t in trades:
        t_time_str = t.get("time")
        if t_time_str:
            try:
                # Handle ISO format
                t_time = datetime.fromisoformat(t_time_str.replace("Z", "+00:00"))
                if last_trade_time is None or t_time > last_trade_time:
                    last_trade_time = t_time
                if t_time >= twenty_four_hours_ago:
                    if t.get("action") == "EXIT":
                        recent_24h_exits.append(t)
                    elif t.get("action") == "ENTRY":
                        recent_24h_entries.append(t)
            except Exception:
                pass

    pnl_by_symbol = {}
    exits_by_symbol = {}
    for t in exits:
        sym = t.get("symbol", "UNKNOWN")
        pnl_by_symbol[sym] = pnl_by_symbol.get(sym, 0.0) + (t.get("pnl") or 0.0)
        exits_by_symbol[sym] = exits_by_symbol.get(sym, 0) + 1

    print("================ STRATEGY PERFORMANCE REPORT ================")
    print(f"Total Trade Records Logged : {total_records}")
    print(f"Total Completed Exits      : {total_exits}")
    print(f"Winning Trades             : {len(winning)}")
    print(f"Losing Trades              : {len(losing)}")
    print(f"Win Rate                   : {win_rate:.2f}%")
    print(f"Total Net PnL              : ${total_pnl:.4f}")
    if winning:
        print(f"Max Single Win             : ${max(winning):.4f}")
    if losing:
        print(f"Max Single Loss            : ${min(losing):.4f}")

    print("\n--- RECENT ACTIVITY (LAST 24 HOURS) ---")
    print(f"Entries in Last 24 Hours   : {len(recent_24h_entries)}")
    print(f"Exits in Last 24 Hours     : {len(recent_24h_exits)}")
    pnl_24h = sum(t.get("pnl", 0.0) for t in recent_24h_exits if t.get("pnl") is not None)
    print(f"24h Net PnL                : ${pnl_24h:.4f}")
    
    if last_trade_time:
        time_diff = now_utc - last_trade_time
        hours_ago = time_diff.total_seconds() / 3600
        print(f"Last Logged Activity Time  : {last_trade_time.strftime('%Y-%m-%d %H:%M:%S UTC')} ({hours_ago:.1f} hours ago)")
    else:
        print("Last Logged Activity Time  : Unknown")

    print("\n--- PnL Breakdown by Symbol ---")
    sorted_symbols = sorted(pnl_by_symbol.items(), key=lambda x: x[1], reverse=True)
    for sym, pnl in sorted_symbols:
        count = exits_by_symbol[sym]
        print(f"  {sym:<10}: PnL = $ {pnl:>7.4f} | Total Exits = {count}")
    print("=============================================================")

if __name__ == "__main__":
    analyze_trades()
