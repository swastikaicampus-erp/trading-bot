# 📋 Delta Strategy Bot — Daily Report & Tomorrow's Checklist
**Date:** 20 October 2026  
**Server:** `187.127.130.77` (`srv1489298`)  
**Service:** `delta-service` (PM2 Process ID: 1)  
**Timeframe:** 🚀 **5-Minute (`5m`) Timeframe** (Upgraded from 1m)  
**Current Balance:** `$19.3331`  
**Scan Grid:** `30 Symbols Active`  
**Status:** ✅ Live & Operational | 5m WebSockets Stable | Zero Deadlocks  

---

## 1. 🛠️ Aaj Jo Sabse Bade Changes Kiye Gaye (What Was Done Today)

### A. 0-Trade Deadlock & Sizing Issue Fix
* **Problem**: Pichhle 19 ghante se ek bhi trade nahi aayi thi.
* **Root Causes Identified**:
  1. `cross_lookback_candles = 3` aur `min_ema_separation = 0.08%` ka aapas mein mathematical contradiction (Candle 1-3 mein separation 0.08% nahi hota, aur candle 4 par lookback expire ho jaata tha).
  2. $19 balance par `risk_pct = 1%` ($0.19) hone ki wajah se har altcoin `ENTRY_SKIPPED` ho raha tha.
* **Fix Applied**:
  * `risk_pct` ko **`2.5%`** kiya gaya (~$0.48 budget).
  * **Small Account Hard Cap Rule**: Agar 1 contract ka SL loss budget se thoda upar bhi ho, toh **3.5% capital ($0.67)** tak 1 contract allow hoga.
  * `max_concurrent_trades` ko **`1`** kiya gaya (Chhota account split nahi hoga).

---

### B. 🚀 Major Upgrade: 1-Minute ➔ 5-Minute Timeframe Switch
* **Kyu Kiya Gaya**: 1-minute chart par random wicks aur Delta Exchange ki 0.10% taker fee profits ko nichod leti thi.
* **5m Architecture Implementation**:
  * `market_data.py`: `candlestick_5m` channel subscribe kiya gaya.
  * **200 Candles Dynamic Backfill**: Startup par har symbol ke 200 candles (16.6 ghante ka data) aur reconnect par 80 candles automatically scale ho gaye.
  * **Profit Targets Scaled**: Partial TP ko **+1.8%** (40% exit) aur Trailing SL ko **2.0%** kiya gaya.
  * **Outcome**: Scanner scores turant **0.73 se 0.86** ke prime zone mein aa gaye!

---

## 2. ☀️ Kal Subah Check Karne Ke Liye Step-By-Step Checklist

Kal jab aap server par login karein:
```bash
ssh root@187.127.130.77
cd /var/www/delta-service
```
Toh aapko niche diye gaye **3 steps sequential order** mein check karne hain:

---

### 🔹 STEP 1: PnL Aur Last 24 Hours Performance Report Check Karein
Command:
```bash
python3 check_pnl.py
```

#### 🔍 Isme Kya Dekhna Hai?
1. **`LAST 24 HOURS` Section Dekhein (Sabse Important)**:
   * **`Entries` & `Exits`**: Dekhein ki 5m chart par kitne trades execute hue (Expected: 1 se 4 trades).
   * **`24h Net PnL`**: Dekhein ki PnL kaisa hai.
2. **`PARTIAL TP` vs `FINAL EXIT`**:
   * Dekhein ki partial TP ne **+1.8%** par profit lock kiya ya nahi.
3. **Average Loss**:
   * Verify karein ki koi bhi losing trade **-$0.50 se -$0.65** ke risk cap se badi na ho.

---

### 🔹 STEP 2: Live Candidates & Open Trades Check Karein
Command:
```bash
curl -s http://localhost:5002/strategy/status | python3 -c "import sys, json; d=json.load(sys.stdin).get('data', {}); print('Strategy Running:', d.get('running')); print('Active Trades:', d.get('active_trades_count')); print('Open Trades Info:', d.get('open_trades', {})); print('Top Candidates (5m):', [(s['symbol'], s['score'], s['direction']) for s in d.get('candidates', [])])"
```

#### 🔍 Expected Result:
* **Strategy Running:** `True`
* **Active Trades:** `0 ya 1` (Max concurrent 1 trade allowed hai).
* Agar koi open position chal rahi hai toh uska symbol, entry price, aur SL/TP dikhega.

---

### 🔹 STEP 3: WebSocket Stability & Logs Check Karein
Command:
```bash
pm2 logs delta-service --lines 30 --nostream
```

#### 🔍 Isme Kya Dekhna Hai?
* Process online rehni chahiye (`fork, status: online`).
* Logs mein `candlestick_5m` stream uninterrupted honi chahiye.
* `[balance-sync]` wallet balance regularly update karta dikhna chahiye.

---

## 📊 3. Active Strategy Configuration Reference (5-Minute Mode)

| Parameter | Configured Value | Purpose |
| :--- | :--- | :--- |
| **Timeframe** | `5m` | Low noise, high-trend reliability |
| **Risk Pct** | `2.5%` | Small account ($19) sizing budget |
| **Max Single Trade Loss** | `3.5%` (~$0.67) | Hard loss cap per trade |
| **Max Concurrent Trades** | `1` | Capital focus on 1 best trade |
| **Max Trades Per Day** | `4` | Zero overtrading |
| **Lookback Candles** | `3` (15 min) | Crossover confirmation window |
| **Min EMA Separation** | `0.05%` | Trend divergence confirmation |
| **ADX Threshold** | `22` | Trend momentum confirmation |
| **Volume Multiplier** | `1.15x` | Breakout confirmation |
| **Min Score Threshold** | `0.58` | Top quality filter |
| **Partial Take Profit** | `+1.8%` (40% exit) | Early profit secure |
| **Trailing Stop Loss** | `2.0%` distance | Runner profit maximize |
