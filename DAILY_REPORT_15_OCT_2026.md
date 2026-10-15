# 📋 Delta Strategy Bot — Daily Report & Tomorrow's Checklist
**Date:** 15 October 2026  
**Server:** `187.127.130.77` (`srv1489298`)  
**Service:** `delta-service` (PM2 Process ID: 1)  
**Status:** ✅ Live & Operational | WebSocket Rock-Solid | Scan Grid Clean (30 Symbols)

---

## 1. 🛠️ Aaj Jo Changes Kiye Gaye (What Was Changed Today)

Aaj system ke do sabse bade bottlenecks — **Continuous WebSocket Crash Loop** aur **Heavy Account-Draining Altcoins** — ko fix kiya gaya:

### A. Toxic / Loss-Making Coins Blacklist Kiye Gaye (`strategy.py`)
Aapke live PnL Analyzer report ke basis par, jin coins ne account ko consistently drain kiya tha, unhe `strategy.py` ke `symbol_blacklist` mein add kar diya gaya:

| Symbol | Historic PnL | Win Rate | Action Taken | Reason |
| :--- | :--- | :--- | :--- | :--- |
| **`SOLVUSD`** | **-$0.5218** | 25.0% | 🔴 **Blacklisted** | Active scan grid mein #4 tha, 4th biggest loser |
| **`RSRUSD`** | **-$0.5450** | **0.0%** | 🔴 **Blacklisted** | 3 trades liye, ek bhi trade win nahi hua |
| **`GIGGLEUSD`**| **-$0.2539** | **0.0%** | 🔴 **Blacklisted** | 100% loss rate, highly volatile & low liquidity |
| **`TSLAXUSD`** | **-$0.2280** | 20.0% | 🔴 **Blacklisted** | 5 trades mein sirf 1 win |
| **`ALLOUSD`**  | **-$0.2043** | **0.0%** | 🔴 **Blacklisted** | 0% win rate, account drag |
| **`NVDAXUSD`** | **-$0.1692** | **0.0%** | 🔴 **Blacklisted** | Synthetic token, choppy price action |
| **`XAUTUSD`**  | **-$0.1294** | 25.0% | 🔴 **Blacklisted** | Gold perpetual contract, low volatility / low win rate |
| **`SPCXXUSD`** | **-$0.0240** | 25.0% | 🔴 **Blacklisted** | Low win rate (25%) |

> **Profitable Coins Retained:**
> `BLURUSD` (+$0.3258, 75% win rate) aur `AMDBUSD` (+$0.0366, 100% win rate) ko blacklist **nahi** kiya gaya kyunki yeh account ko profit de rahe hain.

---

### B. WebSocket Ping/Pong Timeout Fix (`market_data.py`)
* **Problem**: Har 25-30 second mein bot ka connection `ping/pong timed out` throw karke disconnect ho raha tha (`ws connected (#16) -> (#17) -> (#18)`). Har disconnect par bot 30 parallel REST requests se Delta Exchange ko hammer kar raha tha.
* **Root Cause**: Python ka `websocket-client` low-level RFC Ping bhej kar 10 second mein RFC Pong expect kar raha tha (`ping_timeout=10`). Jabki Delta Exchange application-level JSON heartbeat (`enable_heartbeat`) use karta hai.
* **Fix Applied**: `market_data.py` mein `ping_timeout=None` set kiya gaya. Is se false disconnects band ho gaye aur socket disconnect loop permanently khatam ho gaya.

---

### C. Live Server Deployment Verification
Git commit `ec4f914` server par successfully pull aur restart kiya gaya:
* **PM2 Status**: `delta-service` online (restart #39).
* **Scan Grid Status**: **30 symbols actively scanning**.
* **Top 5 Symbols in Grid**: `['DUSKUSD', 'BTWUSD', 'ARIAUSD', 'BLURUSD', 'UNIUSD']` (Saare high-win-rate aur clean symbols aa gaye, toxic coins grid se gayab ho gaye).
* **WS Stream**: Single stable connection `ws connected (#1)`.

---

## 2. ☀️ Kal Subah Check Karne Ke Liye Step-By-Step Checklist

Kal jab aap server par login karein:
```bash
ssh root@187.127.130.77
cd /var/www/delta-service
```
Toh aapko niche diye gaye 3 steps sequential order mein check karne hain:

---

### 🔹 STEP 1: PnL Aur Last 24 Hours Performance Report Check Karein
Command:
```bash
python3 check_pnl.py
```

#### 🔍 Isme Kya Dekhna Hai?
1. **`LAST 24 HOURS` Section Dekhein (Sabse Important)**:
   * **`Entries` & `Exits`**: Dekhein ki pichhle 24 ghante mein kitne trades execute hue (8 trades daily cap ke aas-paas hone chahiye).
   * **`24h Net PnL`**: Dekhein ki 24h PnL positive green hai ya near-breakeven.
2. **`PnL BREAKDOWN BY SYMBOL` Section Dekhein**:
   * Verify karein ki jo coins aaj blacklist kiye gaye (`SOLVUSD`, `RSRUSD`, `XAUTUSD`, etc.), unka koi naya trade open **nahi** hua hai.
   * Dekhein ki top performers jaise `UNIUSD`, `BTWUSD`, `BLURUSD`, `TIAUSD` ne kaisa perform kiya.

---

### 🔹 STEP 2: WebSocket Stability & Disconnect Loop Check Karein
Command:
```bash
pm2 logs delta-service --lines 30 --nostream
```

#### 🔍 Isme Kya Dekhna Hai?
1. **Connection Stability**:
   * Logs mein `ws error: ping/pong timed out` **nahi** aana chahiye.
   * `ws connected (#...)` ka number bada nahi hona chahiye (jaise kal #19 tak pahunch gaya tha). Agar sab sahi raha toh yeh `#1` ya `#2` par stable hona chahiye.
2. **Balance Sync**:
   * Dekhein ki `[balance-sync] strategy capital updated from balance: $XX.XXXX` regularly update ho raha hai bina kisi error ke.

---

### 🔹 STEP 3: Live Scanner Grid & Open Positions Check Karein
Command:
```bash
curl -s http://localhost:5002/strategy/status | python3 -c "import sys, json; d=json.load(sys.stdin).get('data', {}); print('Strategy Running:', d.get('running')); print('Scan Grid Count:', len(d.get('scan_grid', []))); print('Open Positions Count:', len(d.get('open_trades', {}))); print('Top 5 Symbols:', [s.get('symbol') for s in d.get('scan_grid', [])][:5])"
```

#### 🔍 Expected Result:
* **Strategy Running:** `True`
* **Scan Grid Count:** `25 se 30` ke beech hona chahiye (`0` nahi hona chahiye).
* **Open Positions Count:** `0, 1 ya 2` (Max concurrent 2 trades allowed hain).
* **Top 5 Symbols:** Clean coins hone chahiye (jaise `UNIUSD`, `BTWUSD`, `BLURUSD`, etc.).

---

## 3. 📌 Quick Summary of Useful Commands

| Task | Command |
| :--- | :--- |
| **PnL Report** | `python3 check_pnl.py` |
| **Realtime Logs** | `pm2 logs delta-service --lines 40 --nostream` |
| **Scanner Grid Status**| `curl -s http://localhost:5002/strategy/status \| python3 -c "import sys, json; d=json.load(sys.stdin).get('data', {}); print('Running:', d.get('running')); print('Scan Grid:', len(d.get('scan_grid', []))); print('Open:', len(d.get('open_trades', {})))"` |
| **Bot Restart (Agar jarurat pade)** | `pm2 restart delta-service` |
| **PM2 Process Health** | `pm2 status` |
