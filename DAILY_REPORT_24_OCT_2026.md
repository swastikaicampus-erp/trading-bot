# 📋 Delta Strategy Bot — Daily Report & Verification Checklist
**Date:** 24 October 2026  
**Server:** `187.127.130.77` (`srv1489298`)  
**Service:** `delta-service` (PM2 Process ID: 1)  
**Timeframe:** 🚀 **5-Minute (`5m`) Timeframe**  
**Current Balance:** `$18.5136`  
**Scan Grid:** `15 Curated Liquid Symbols Active`  
**Commit Deployed:** `8eaaec0`  
**Status:** ✅ Unblocked & Live | WebSocket Watchdog Stable | Zero Deadlocks  

---

## 1. 🔍 Root Cause Analysis (Kal Se 0-Trade Kyun Thi?)

23 October 14:25 UTC (7:55 PM IST) ke baad se poore 25+ ghante ek bhi trade trigger nahi hui thi. Iske piche 3 exact reasons the:

### A. Core Deadlock: `cross_lookback_candles: 1` + `require_cross_confirmation: True`
Kal commit `d0f024a` me lookback ko `1` candle kar diya gaya tha jabki confirmation filter `True` tha:
1. `cross_lookback = 1` par cross hone ke liye rule tha: `fast[-2] <= slow[-2]` aur `fast[-1] > slow[-1]`.
2. Theek agle line par confirmation check tha: `if not (fast[-2] > slow[-2]): fresh_cross_up = False`.
3. Kyunki `fast[-2]` pehle se niche tha, confirmation check 100% false ho jata tha aur cross ko turant `False` bana deta tha.
4. **Nateeja**: Kisi bhi coin par cross condition `True` hona mathematically impossible ho gaya tha.

### B. Over-tight Filters:
* `min_score_threshold` ko `0.58` se badha kar `0.68` kiya gaya tha (A+ score bhi 0.65 ke aaspaas aata hai).
* `max_spread_pct` ko `0.15%` se ghata kar `0.08%` kiya gaya tha (Delta par altcoins ka spread aksar 0.09% - 0.14% hota hai, jisse saare candidate drop ho rahe the).
* `rsi_overbought` band 70 se ghata kar 65 kar diya gaya tha.

### C. WebSocket Watchdog Thrashing:
* `_on_open` me `self._last_msg_ts` reset nahi ho raha tha, jisse naya socket connect hote hi purane connection ke timestamp ke chalte watchdog 90s timeout samajh kar connection dobara kaat deta tha.

---

## 2. 🛠️ Aaj Jo Changes Apply Kiye Gaye (Fixed & Deployed)

| File | Change Made | Purpose |
| :--- | :--- | :--- |
| `strategy.py` | `cross_lookback_candles = 3` | 5m chart par pichhle 15 minute ke fresh breakout captures allow honge. |
| `strategy.py` | `require_cross_confirmation = False` | Mathematical contradiction/deadlock khatam, direct clean entry. |
| `strategy.py` | `min_score_threshold = 0.58` | Realistic quality threshold (pehle 0.68 impossible tha). |
| `strategy.py` | `max_spread_pct = 0.15` | Altcoins ke normal 0.09%-0.14% spread survive ho sakein. |
| `strategy.py` | `rsi_overbought = 70` | Strong upward breakout runners miss na hon. |
| `strategy.py` | `max_trades_per_day = 4` | Daily trades cap balanced at 4. |
| `market_data.py` | `self._last_msg_ts = time.time()` on `_on_open` | Fresh connection par watchdog false-trigger nahi karega. |
| `market_data.py` | Watchdog timeout `90s` ➔ `120s` | Reconnect thrashing band, 2-minute latency resilience. |

---

## 3. ☀️ Kal Subah Check Karne Ke Liye Step-By-Step Checklist

Kal jab aap server par login karein:
```bash
ssh root@187.127.130.77
cd /var/www/delta-service
```
Toh aapko yeh **3 steps** sequential order me run karke check karne hain:

---

### 🔹 STEP 1: PnL & Last 24 Hours Activity Report Check Karein
Command:
```bash
python3 check_pnl.py
```

#### 🔍 Isme Kya Dekhna Hai?
1. **`LAST 24 HOURS` Section Dekhein**:
   * **`Entries` & `Exits`**: Dekhein ki trades ka number 0 se badha ya nahi (Expected: 1 se 4 trades).
   * **`24h Net PnL`**: Dekhein ki profit/loss kaisa raha.
2. **`PARTIAL TP` vs `FINAL EXIT`**:
   * Verify karein ki +2.0% par 60% partial profit lock hua ya nahi.
3. **Loss Limits**:
   * Verify karein ki koi bhi trade account ke 3.5% hard cap (~$0.65) se badi loss me na gayi ho.

---

### 🔹 STEP 2: Live Strategy Grid & Candidates Check Karein
Command:
```bash
curl -s http://localhost:5002/strategy/status | python3 -c '
import sys, json
d = json.load(sys.stdin).get("data", {})
print("=== STRATEGY STATUS ===")
print("Running:", d.get("running"), "| Trades Today:", d.get("trades_today"), "| Active Trades:", d.get("active_trades_count"))
print("Open Positions:", d.get("open_trades", {}))
print("Current Candidates:", [(c["symbol"], c["score"], c["direction"]) for c in d.get("candidates", [])])
print("-" * 65)
for s in d.get("scan_grid", [])[:7]:
    cond = s.get("conditions", {})
    print(f"{s.get(\"symbol\"):<11} | Score: {s.get(\"score\", 0):.2f} | RSI: {s.get(\"rsi\")} | ADX: {s.get(\"adx\")} | Qual: {s.get(\"qualifies\")}")
'
```

#### 🔍 Expected Result:
* `Running: True`
* `Trades Today`: 1 se 4 ke beech
* Agar koi trade live chal rahi ho toh `Open Positions` me uski entry, SL aur TP dikhegi.

---

### 🔹 STEP 3: Logs Verification (WebSocket & Scanner Liveness)
Command:
```bash
pm2 logs delta-service --lines 40 --nostream
```

#### 🔍 Isme Kya Dekhna Hai?
* `[market_data] ws connected (#1)` stable rehna chahiye (baar baar disconnect nahi hona chahiye).
* `[balance-sync]` wallet balance update karta dikhna chahiye.
* `Candles` backfill ya indicators me koi exception nahi aani chahiye.

---

## 📊 4. Active Strategy Parameters Snapshot

| Parameter | Value | Description |
| :--- | :--- | :--- |
| **Timeframe** | `5m` | Noise-free 5-minute trend candles |
| **Capital** | `$18.51` | Auto-synced from Delta exchange balance |
| **Risk Pct** | `2.5%` | Sizing budget (~$0.46) |
| **Max Single Trade Loss** | `3.5%` | Hard loss cap (~$0.65) |
| **Max Concurrent Trades** | `1` | Small account focus on 1 best trade |
| **Max Trades Per Day** | `4` | Overtrading prevention |
| **Lookback Candles** | `3` (15 min) | Crossover detection window |
| **Cross Confirmation** | `False` | Clean direct breakout entry |
| **Score Threshold** | `0.58` | Realistic quality setup filter |
| **Spread Filter** | `0.15%` | Rejects illiquid altcoin slippage |
| **Partial TP** | `60% @ +2.0%` | Fast profit booking |
| **Break-Even SL** | `+2.4%` | Locks stop-loss to entry price on runners |
| **Session Filter** | `9:00 AM – 2:00 AM IST` | Trades during peak liquidity hours only |
