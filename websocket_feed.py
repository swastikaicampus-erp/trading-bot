"""
websocket_feed.py -- Delta Exchange WebSocket client (public + private).

This is a drop-in companion/replacement for a REST-polling MarketDataFeed.
Instead of hitting /v2/tickers, /v2/history/candles etc. on a timer, this
keeps two persistent connections open and updates in-memory state as
messages arrive:

  - Public socket  (wss://public-socket.india.delta.exchange)
      channels: ticker, candlestick_<resolution>, ob_l1, mark_price
  - Private socket (wss://socket.india.delta.exchange)
      channels: orders, positions, margins  (needs key-auth)

Why switch from REST polling to this:
  - Saves rate-limit quota (REST tickers/candles calls cost weight 3 each,
    multiplied across your whole Nifty50-style watchlist, every poll).
  - Much lower latency -- ticker pushes every 5s, ob_l1 every 100ms,
    fills/positions are pushed the instant they happen instead of waiting
    for the next poll cycle.

Usage:
    from websocket_feed import DeltaWebSocketFeed

    feed = DeltaWebSocketFeed(
        symbols=["BTCUSD", "ETHUSD"],
        candle_resolutions=["1m", "5m"],
        api_key=os.getenv("DELTA_API_KEY"),
        api_secret=os.getenv("DELTA_API_SECRET"),
        enable_private=True,
    )
    feed.start()

    feed.get_ticker("BTCUSD")
    feed.get_candles("BTCUSD", "1m")
    feed.get_positions()
    feed.get_orders()
"""

import json
import time
import hmac
import hashlib
import threading

import websocket  # pip install websocket-client

PUBLIC_WS_URL = "wss://public-socket.india.delta.exchange"
PRIVATE_WS_URL = "wss://socket.india.delta.exchange"

RECONNECT_DELAY_SEC = 5
HEARTBEAT_TIMEOUT_SEC = 35  # server sends heartbeat every 30s; 35s = 5s buffer


def _sign(secret, message):
    return hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()


class DeltaWebSocketFeed:
    def __init__(self, symbols, candle_resolutions=None, api_key=None, api_secret=None,
                 enable_private=False, on_order_update=None, on_position_update=None):
        self.symbols = [s.upper() for s in symbols]
        self.candle_resolutions = candle_resolutions or ["1m"]
        self.api_key = api_key
        self.api_secret = api_secret
        self.enable_private = enable_private and bool(api_key and api_secret)

        # optional callbacks so strategy.py can react immediately to
        # fills/position changes instead of polling get_positions()/get_orders()
        self.on_order_update = on_order_update
        self.on_position_update = on_position_update

        self._lock = threading.Lock()
        self._tickers = {}          # symbol -> latest ticker dict
        self._candles = {}          # (symbol, resolution) -> latest candle dict
        self._ob_l1 = {}            # symbol -> best bid/ask dict
        self._positions = {}        # symbol -> position dict
        self._orders = {}           # order_id -> order dict

        self._public_ws = None
        self._private_ws = None
        self._public_last_heartbeat = time.time()
        self._stop = False

    # -----------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------
    def start(self):
        self._stop = False
        threading.Thread(target=self._run_public, daemon=True).start()
        if self.enable_private:
            threading.Thread(target=self._run_private, daemon=True).start()

    def stop(self):
        self._stop = True
        if self._public_ws:
            self._public_ws.close()
        if self._private_ws:
            self._private_ws.close()

    # -----------------------------------------------------------------
    # Public socket
    # -----------------------------------------------------------------
    def _run_public(self):
        while not self._stop:
            try:
                self._public_ws = websocket.WebSocketApp(
                    PUBLIC_WS_URL,
                    on_open=self._on_public_open,
                    on_message=self._on_public_message,
                    on_error=lambda ws, err: print(f"[ws-public] error: {err}"),
                    on_close=lambda ws, code, msg: print(f"[ws-public] closed: {code} {msg}"),
                )
                self._public_ws.run_forever(ping_interval=25, ping_timeout=10)
            except Exception as e:
                print(f"[ws-public] crashed: {e}")
            if not self._stop:
                time.sleep(RECONNECT_DELAY_SEC)

    def _on_public_open(self, ws):
        print("[ws-public] connected")
        self._subscribe(ws, "ticker", self.symbols)
        self._subscribe(ws, "ob_l1", self.symbols)
        for res in self.candle_resolutions:
            self._subscribe(ws, f"candlestick_{res}", self.symbols)
        ws.send(json.dumps({"type": "enable_heartbeat"}))
        threading.Thread(target=self._watch_public_heartbeat, daemon=True).start()

    def _watch_public_heartbeat(self):
        """If no heartbeat arrives for HEARTBEAT_TIMEOUT_SEC, force a
        reconnect -- some networks silently drop the TCP connection
        without a clean close event."""
        self._public_last_heartbeat = time.time()
        while not self._stop and self._public_ws:
            time.sleep(5)
            if time.time() - self._public_last_heartbeat > HEARTBEAT_TIMEOUT_SEC:
                print("[ws-public] heartbeat timeout, forcing reconnect")
                try:
                    self._public_ws.close()
                except Exception:
                    pass
                return

    def _on_public_message(self, ws, message):
        try:
            data = json.loads(message)
        except ValueError:
            return

        msg_type = data.get("type")
        if msg_type == "heartbeat":
            self._public_last_heartbeat = time.time()
            return

        with self._lock:
            if msg_type == "ticker":
                symbol = data.get("sy")
                if symbol:
                    self._tickers[symbol] = data
            elif msg_type and msg_type.startswith("candlestick_"):
                symbol = data.get("sy")
                resolution = data.get("res")
                if symbol and resolution:
                    self._candles[(symbol, resolution)] = data
            elif msg_type == "ob_l1":
                symbol = data.get("sy")
                if symbol:
                    self._ob_l1[symbol] = data
            elif msg_type == "subscriptions":
                print(f"[ws-public] subscribed: {data.get('channels')}")

    # -----------------------------------------------------------------
    # Private socket (orders, positions, margins)
    # -----------------------------------------------------------------
    def _run_private(self):
        while not self._stop:
            try:
                self._private_ws = websocket.WebSocketApp(
                    PRIVATE_WS_URL,
                    on_open=self._on_private_open,
                    on_message=self._on_private_message,
                    on_error=lambda ws, err: print(f"[ws-private] error: {err}"),
                    on_close=lambda ws, code, msg: print(f"[ws-private] closed: {code} {msg}"),
                )
                self._private_ws.run_forever(ping_interval=25, ping_timeout=10)
            except Exception as e:
                print(f"[ws-private] crashed: {e}")
            if not self._stop:
                time.sleep(RECONNECT_DELAY_SEC)

    def _on_private_open(self, ws):
        print("[ws-private] connected, authenticating...")
        timestamp = str(int(time.time()))
        signature = _sign(self.api_secret, "GET" + timestamp + "/live")
        ws.send(json.dumps({
            "type": "key-auth",
            "payload": {
                "api-key": self.api_key,
                "signature": signature,
                "timestamp": timestamp,
            }
        }))

    def _on_private_message(self, ws, message):
        try:
            data = json.loads(message)
        except ValueError:
            return

        msg_type = data.get("type")

        if msg_type == "key-auth":
            if data.get("success"):
                print("[ws-private] authenticated")
                self._subscribe(ws, "orders", self.symbols)
                self._subscribe(ws, "positions", self.symbols)
                self._subscribe(ws, "margins", [])
            else:
                print(f"[ws-private] auth failed: {data}")
            return

        with self._lock:
            if msg_type == "orders":
                order_id = data.get("order_id")
                action = data.get("action")
                if order_id is not None:
                    if action == "delete":
                        self._orders.pop(order_id, None)
                    else:
                        self._orders[order_id] = data
                if self.on_order_update:
                    self.on_order_update(data)
            elif msg_type == "positions":
                symbol = data.get("symbol")
                action = data.get("action")
                if symbol:
                    if action == "delete":
                        self._positions.pop(symbol, None)
                    else:
                        self._positions[symbol] = data
                if self.on_position_update:
                    self.on_position_update(data)

    # -----------------------------------------------------------------
    # Shared helpers
    # -----------------------------------------------------------------
    @staticmethod
    def _subscribe(ws, channel_name, symbols):
        payload = {"name": channel_name}
        if symbols:
            payload["symbols"] = symbols
        ws.send(json.dumps({"type": "subscribe", "payload": {"channels": [payload]}}))

    # -----------------------------------------------------------------
    # Public read accessors -- safe to call from Flask routes
    # -----------------------------------------------------------------
    def get_ticker(self, symbol):
        with self._lock:
            return self._tickers.get(symbol.upper())

    def get_all_tickers(self):
        with self._lock:
            return dict(self._tickers)

    def get_candle(self, symbol, resolution="1m"):
        with self._lock:
            return self._candles.get((symbol.upper(), resolution))

    def get_ob_l1(self, symbol):
        with self._lock:
            return self._ob_l1.get(symbol.upper())

    def get_positions(self):
        with self._lock:
            return dict(self._positions)

    def get_position(self, symbol):
        with self._lock:
            return self._positions.get(symbol.upper())

    def get_orders(self):
        with self._lock:
            return dict(self._orders)

    def add_symbol(self, symbol):
        """Subscribe to a new symbol at runtime without reconnecting."""
        symbol = symbol.upper()
        if symbol in self.symbols:
            return False
        self.symbols.append(symbol)
        if self._public_ws:
            self._subscribe(self._public_ws, "ticker", [symbol])
            self._subscribe(self._public_ws, "ob_l1", [symbol])
            for res in self.candle_resolutions:
                self._subscribe(self._public_ws, f"candlestick_{res}", [symbol])
        if self._private_ws and self.enable_private:
            self._subscribe(self._private_ws, "orders", [symbol])
            self._subscribe(self._private_ws, "positions", [symbol])
        return True

    def remove_symbol(self, symbol):
        symbol = symbol.upper()
        if symbol not in self.symbols:
            return False
        self.symbols.remove(symbol)
        if self._public_ws:
            self._public_ws.send(json.dumps({
                "type": "unsubscribe",
                "payload": {"channels": [{"name": "ticker", "symbols": [symbol]}]}
            }))
        return True