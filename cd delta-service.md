cd delta-service
.\venv\Scripts\Activate.ps1
python app.py  



3&9?Jta)?zH(HITk

backup

5CXLIIPYNH6OIVPN


API Key
MnxBidMh6euXNiVm6uNH8KlCSSLXkh

API Secret
Gc6o0sPFcvLm5OqB64y0F1Efq4Sh3zcufsqC3c9JuIFYKZkxbyLv9RuGIVI6



API key generated! Please note the secret key in a safe place.
Write this down somewhere safe. This secret key will not be available once you leave this page. Don’t share this with anyone.
API Key
:
MnxBidMh6euXNiVm6uNH8KlCSSLXkh
API Secret
:
Gc6o0sPFcvLm5OqB64y0F1Efq4Sh3zcufsqC3c9JuIFYKZkxbyLv9RuGIVI6
Note: Please wait for 5 mins for the API key to become operational



vps



pm2 logs delta-service --lines 30

cd /var/www/delta-service
cat .env

# Kaunse ports already use ho rahe hain
sudo ss -tulpn | grep LISTEN

# systemd me kaunsi services chal rahi hain
sudo systemctl list-units --type=service --state=running

# nginx ke existing site configs
ls -la /etc/nginx/sites-enabled/

# PM2 se koi Node/MERN app chal rahi ho to
pm2 list





1. Dashboard (Home)

Sabse pehla screen.

Cards:

API Status (/health)
Account Balance (/balance)
Open Orders Count (/orders)
Open Positions Count (/positions/all)
Running Strategies Count (/strategy/status)

Layout:

--------------------------------
 Health   Balance   Orders
--------------------------------
 Positions  Strategy Status
--------------------------------
 Market Overview
--------------------------------
2. Trading Panel

Manual trading ke liye.

Left Side

Market Selector

BTCUSD
ETHUSD
SOLUSD

Data:

GET /products
Right Side

Order Form

Product
Side (Buy/Sell)
Size
Order Type
Limit Price
Leverage

[Place Order]

Use:

POST /place-order
POST /leverage/:product_id
3. Orders Management

Ye sabse important page hoga.

Table:

Order ID
Symbol
Side
Price
Size
Status
Actions

Actions:

Edit
Cancel

Routes:

GET /orders
GET /orders/history
PUT /orders/:id
POST /cancel-order



4. Positions Management

Live positions.

Table:

Symbol
Size
Entry Price
PnL
Margin
Action

Actions:

Close Position
Add Margin
Auto Topup

Routes:

GET /positions/all
POST /positions/close
POST /positions/margin
PUT /positions/auto-topup


5. Market Scanner

Bahut useful feature.

Cards:

BTCUSD
ETHUSD
SOLUSD

Live Data:

GET /tickers
GET /ticker/:symbol
GET /candles/:symbol

Charts:

recharts

ya

lightweight-charts
6. Strategy Control

Bot control panel.

Table:

Symbol
Status
Started At
PnL

Buttons:

Start Strategy
Stop Strategy

Routes:

POST /strategy/start
POST /strategy/stop
GET /strategy/status
7. System & Safety

Admin page.

Heartbeat
GET /heartbeat/status
POST /heartbeat/start
POST /heartbeat/stop
Rate Limits
GET /rate-limit
Watchlist
GET /watchlist
POST /watchlist
DELETE /watchlist/:symbol
Folder Structure
src/

├── pages/
│   ├── Dashboard.jsx
│   ├── Trading.jsx
│   ├── Orders.jsx
│   ├── Positions.jsx
│   ├── Scanner.jsx
│   ├── Strategies.jsx
│   └── Settings.jsx
│
├── components/
│   ├── Navbar.jsx
│   ├── Sidebar.jsx
│   ├── BalanceCard.jsx
│   ├── HealthCard.jsx
│   ├── OrderForm.jsx
│   ├── OrdersTable.jsx
│   ├── PositionsTable.jsx
│   ├── MarketChart.jsx
│   ├── StrategyCard.jsx
│   └── WatchlistManager.jsx
│
├── services/
│   └── api.js
│
├── hooks/
│   ├── useOrders.js
│   ├── usePositions.js
│   └── useTicker.js
│
└── App.jsx