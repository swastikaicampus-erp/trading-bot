"""
websocket_feed.py -- Delta Exchange WebSocket client (public + private).
FIXED: rolling candle history buffer + normalized OHLCV keys + safe float
conversion, so this is a true drop-in replacement for the REST-polling feed
that strategy.py expects (get_candles(symbol, limit) returning a list of
{"time","open","high","low","close","volume"} dicts, oldest first).
"""

import json
import time
import hmac
import hashlib
import threading
from collections import deque

import websocket  # pip install websocket-client

PUBLIC_WS_URL = "wss://public-socket.india.delta.exchange"
PRIVATE_WS_URL = "wss://socket.india.delta.exchange"

RECONNECT_DELAY_SEC = 5
HEARTBEAT_TIMEOUT_SEC = 35  # server sends heartbeat every 30s; 35s = 5s buffer
CANDLE_HISTORY_MAXLEN = 500  # enough for EMA/RSI/ADX warmup + headroom


def _sign(secret, message):
    return hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()


def _safe_float(val):
    """Never let a None/garbage value blow up downstream indicator math --
    return None instead of raising, and let the caller decide to skip."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _normalize_candle(raw):
    """
    Delta's public websocket candlestick_<res> payload uses abbreviated
    keys. This maps them onto the same shape strategy.py already expects
    from the REST feed (open/high/low/close/volume/time), so nothing
    downstream needs to change.

    NOTE: confirm the exact abbreviated key names against Delta's current
    WS docs before going live -- if they differ, only this function needs
    updating, everything else stays the same.
    """
    o = _safe_float(raw.get("o") if "o" in raw else raw.get("open"))
    h = _safe_float(raw.get("h") if "h" in raw else raw.get("high"))
    l = _safe_float(raw.get("l") if "l" in raw else raw.get("low"))
    c = _safe_float(raw.get("c") if "c" in raw else raw.get("close"))
    v = _safe_float(raw.get("v") if "v" in raw else raw.get("volume"))
    t = raw.get("t") if "t" in raw else raw.get("time")

    # A candle missing close/high/low is useless (and is exactly what
    # would previously have caused a float(None) crash downstream) --
    # signal "skip this one" by returning None instead of a half-built dict.
    if c is None or h is None or l is None or t is None:
        return None

    return {
        "time": t,
        "open": o if o is not None else c,
        "high": h,
        "low": l,
        "close": c,
        "volume": v if v is not None else 0.0,
    }


class DeltaWebSocketFeed:
    def __init__(self, symbols, candle_resolutions=None, api_key=None, api_secret=None,
                 enable_private=False, on_order_update=None, on_position_update=None):
        self.symbols = [s.upper() for s in symbols]
        self.candle_resolutions = candle_resolutions or ["1m"]
        self.default_resolution = self.candle_resolutions[0]
        self.api_key = api_key
        self.api_secret = api_secret
        self.enable_private = enable_private and bool(api_key and api_secret)

        self.on_order_update = on_order_update
        self.on_position_update = on_position_update

        self._lock = threading.Lock()
        self._tickers = {}          # symbol -> latest ticker dict
        # ADDED: rolling history instead of "latest only"
        self._candle_history = {}   # (symbol, resolution) -> deque of normalized candles
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
        # ADDED: wrap the whole handler -- a single malformed message must
        # never be able to kill this callback (and by extension silently
        # stall the feed) again.
        try:
            data = json.loads(message)
        except ValueError:
            return

        try:
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
                    resolution = data.get("res") or msg_type.replace("candlestick_", "")
                    if symbol and resolution:
                        candle = _normalize_candle(data)
                        if candle is None:
                            # incomplete/garbage tick -- skip, don't crash,
                            # don't poison the history buffer
                            return
                        key = (symbol, resolution)
                        if key not in self._candle_history:
                            self._candle_history[key] = deque(maxlen=CANDLE_HISTORY_MAXLEN)
                        hist = self._candle_history[key]
                        # replace last candle if same timestamp (still forming),
                        # else append a new one (candle closed)
                        if hist and hist[-1]["time"] == candle["time"]:
                            hist[-1] = candle
                        else:
                            hist.append(candle)
                elif msg_type == "ob_l1":
                    symbol = data.get("sy")
                    if symbol:
                        self._ob_l1[symbol] = data
                elif msg_type == "subscriptions":
                    print(f"[ws-public] subscribed: {data.get('channels')}")
        except Exception as e:
            # ADDED: last-resort guard so a single bad message can't kill
            # the on_message callback / stall the feed silently
            print(f"[ws-public] message handling error: {e}")

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

        try:
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
        except Exception as e:
            print(f"[ws-private] message handling error: {e}")

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
    # Public read accessors -- safe to call from Flask routes / strategy.py
    # -----------------------------------------------------------------
    def get_ticker(self, symbol):
        with self._lock:
            return self._tickers.get(symbol.upper())

    def get_all_tickers(self):
        with self._lock:
            return dict(self._tickers)

    def get_candle(self, symbol, resolution=None):
        """Latest single candle (kept for backward compat)."""
        resolution = resolution or self.default_resolution
        with self._lock:
            hist = self._candle_history.get((symbol.upper(), resolution))
            return hist[-1] if hist else None

    # ADDED: this is the method strategy.py actually calls --
    # feed.get_candles(symbol, limit=300) / feed.get_candles(symbol, limit=2)
    def get_candles(self, symbol, limit=None, resolution=None):
        resolution = resolution or self.default_resolution
        with self._lock:
            hist = self._candle_history.get((symbol.upper(), resolution))
            if not hist:
                return []
            data = list(hist)
        if limit:
            data = data[-limit:]
        return data

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

    def get_symbols(self):
        return list(self.symbols)

    def add_symbol(self, symbol):
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