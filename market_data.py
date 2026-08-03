import json
import time
import threading
import requests
import websocket

WS_URL = "wss://socket.india.delta.exchange"
REST_BASE = "https://api.india.delta.exchange"

RESOLUTION = "1m"          # candle timeframe
BACKFILL_MINUTES = 200     # how many past candles to preload per symbol
SUBSCRIBE_CHUNK_SIZE = 50  # Delta ko ek hi message me 100+ symbols bhejna
                            # risky hai (message size / server behaviour
                            # undocumented hai) -- isliye chunks me bhejte hain.


def _safe_float(val, default=0.0):
    """ADDED: Delta's WS occasionally sends a candle field as an explicit
    null (key present, value None) rather than omitting the key -- dict.get's
    default only kicks in when the key is MISSING, not when it's None. That
    mismatch was causing float(None) -> TypeError inside _on_message, which
    (being unwrapped) could kill/destabilize the ws callback and trigger the
    crash -> reconnect -> crash loop. This helper makes every numeric field
    null-safe."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def discover_perpetual_futures_symbols(quote_assets=None):
    """
    Delta ke /v2/products se saare LIVE perpetual futures symbols nikalta
    hai (BTCUSD, ETHUSD, SOLUSD, aur baaki saare alt-coin perps).

    quote_assets: optional filter, e.g. {"USD"} agar sirf USD-settled
    perps chahiye (USDT-settled ya doosre quote assets exclude karne ke
    liye). Default None = sabhi quote assets allowed.

    Options, spreads, move contracts, futures (dated, non-perp), spot
    -- ye sab explicitly exclude kiye jaate hain, sirf perpetual_futures
    rehta hai kyunki ye hi actually leverage ke saath trade hote hain.
    """
    resp = requests.get(f"{REST_BASE}/v2/products", timeout=15)
    resp.raise_for_status()
    rows = resp.json().get("result", [])

    symbols = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("contract_type") != "perpetual_futures":
            continue
        if row.get("state") != "live":
            continue
        if row.get("trading_status") not in (None, "operational"):
            continue
        symbol = row.get("symbol")
        if not symbol:
            continue
        if quote_assets:
            quoting = (row.get("quoting_asset") or {}).get("symbol")
            if quoting not in quote_assets:
                continue
        symbols.append(symbol)

    return sorted(set(symbols))


def _chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


class MarketDataFeed:
    """
    Maintains live candles + ticker for a watchlist of symbols using
    Delta Exchange's public WebSocket feed, with REST backfill on startup.
    Thread-safe in-memory store, read via get_candles() / get_ticker().

    Designed to comfortably handle the FULL perpetual-futures universe
    (100-150+ symbols), not just a handful -- subscriptions are sent in
    chunks of SUBSCRIBE_CHUNK_SIZE to avoid oversized websocket frames.

    Symbols can be added/removed at runtime via add_symbol() /
    remove_symbol() -- the feed subscribes/unsubscribes on the live
    websocket connection immediately, and also backfills history for
    newly added symbols.
    """

    def __init__(self, symbols):
        self.symbols = list(symbols)
        self.candles = {s: [] for s in self.symbols}   # list of dicts, oldest -> newest
        self.ticker = {s: {} for s in self.symbols}     # latest ticker snapshot
        self._lock = threading.Lock()
        self._ws = None
        self._ws_lock = threading.Lock()   # guards send() calls on self._ws
        self._running = False

    # ---------- public read API ----------

    def get_candles(self, symbol, limit=None):
        with self._lock:
            data = list(self.candles.get(symbol, []))
        if limit:
            data = data[-limit:]
        return data

    def get_ticker(self, symbol):
        with self._lock:
            return dict(self.ticker.get(symbol, {}))

    def get_all_tickers(self):
        with self._lock:
            return {s: dict(v) for s, v in self.ticker.items()}

    def get_symbols(self):
        with self._lock:
            return list(self.symbols)

    # ---------- dynamic watchlist management ----------

    def add_symbol(self, symbol):
        """Add a symbol to the live feed: backfill history, register
        storage, and subscribe on the current websocket connection
        (if one is open). Safe to call while the feed is running."""
        symbol = symbol.upper()
        with self._lock:
            if symbol in self.symbols:
                return False
            self.symbols.append(symbol)
            self.candles[symbol] = []
            self.ticker[symbol] = {}

        self._backfill_one(symbol)

        with self._ws_lock:
            if self._ws is not None:
                try:
                    self._subscribe(self._ws, "v2/ticker", [symbol])
                    self._subscribe(self._ws, f"candlestick_{RESOLUTION}", [symbol])
                except Exception as e:
                    print(f"[market_data] live subscribe failed for {symbol}: {e}")
        return True

    def add_symbols(self, symbols):
        """Bulk version of add_symbol -- backfills sequentially but
        subscribes in chunks (used at startup / on /watchlist/sync)."""
        added = []
        for symbol in symbols:
            symbol = symbol.upper()
            with self._lock:
                if symbol in self.symbols:
                    continue
                self.symbols.append(symbol)
                self.candles[symbol] = []
                self.ticker[symbol] = {}
            added.append(symbol)

        for symbol in added:
            self._backfill_one(symbol)

        with self._ws_lock:
            if self._ws is not None:
                for chunk in _chunks(added, SUBSCRIBE_CHUNK_SIZE):
                    try:
                        self._subscribe(self._ws, "v2/ticker", chunk)
                        self._subscribe(self._ws, f"candlestick_{RESOLUTION}", chunk)
                    except Exception as e:
                        print(f"[market_data] live subscribe failed for chunk {chunk}: {e}")
        return added

    def remove_symbol(self, symbol):
        """Remove a symbol from the live feed and unsubscribe on the
        current websocket connection (if one is open)."""
        symbol = symbol.upper()
        with self._lock:
            if symbol not in self.symbols:
                return False
            self.symbols.remove(symbol)
            self.candles.pop(symbol, None)
            self.ticker.pop(symbol, None)

        with self._ws_lock:
            if self._ws is not None:
                try:
                    self._unsubscribe(self._ws, "v2/ticker", [symbol])
                    self._unsubscribe(self._ws, f"candlestick_{RESOLUTION}", [symbol])
                except Exception as e:
                    print(f"[market_data] live unsubscribe failed for {symbol}: {e}")
        return True

    # ---------- backfill ----------

    def _backfill_one(self, symbol):
        end = int(time.time())
        start = end - BACKFILL_MINUTES * 60
        try:
            r = requests.get(
                f"{REST_BASE}/v2/history/candles",
                params={"symbol": symbol, "resolution": RESOLUTION, "start": start, "end": end},
                timeout=10,
            )
            r.raise_for_status()
            result = r.json().get("result", [])
            candles = sorted(
                [
                    {
                        "time": c["time"],
                        "open": _safe_float(c.get("open")),
                        "high": _safe_float(c.get("high")),
                        "low": _safe_float(c.get("low")),
                        "close": _safe_float(c.get("close")),
                        "volume": _safe_float(c.get("volume")),
                    }
                    for c in result
                    if c.get("time") is not None  # ADDED: skip rows with no timestamp
                ],
                key=lambda c: c["time"],
            )
            with self._lock:
                if symbol in self.candles:   # could've been removed meanwhile
                    self.candles[symbol] = candles
            print(f"[market_data] backfilled {len(candles)} candles for {symbol}")
        except Exception as e:
            print(f"[market_data] backfill failed for {symbol}: {e}")

    def backfill(self):
        """Backfills all symbols in parallel-ish using a small thread
        pool -- sequential REST calls for 100-150 symbols would take too
        long one-by-one at startup."""
        symbols = self.get_symbols()
        threads = []
        max_parallel = 10
        for chunk in _chunks(symbols, max_parallel):
            chunk_threads = [threading.Thread(target=self._backfill_one, args=(s,)) for s in chunk]
            for t in chunk_threads:
                t.start()
            for t in chunk_threads:
                t.join()
        _ = threads  # (kept for clarity, no-op)

    # ---------- websocket lifecycle ----------

    def start(self):
        self._running = True
        self.backfill()
        threading.Thread(target=self._run_forever, daemon=True).start()

    def stop(self):
        self._running = False
        with self._ws_lock:
            if self._ws:
                self._ws.close()

    def _run_forever(self):
        while self._running:
            try:
                ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                with self._ws_lock:
                    self._ws = ws
                ws.run_forever(ping_interval=25, ping_timeout=10)
            except Exception as e:
                print(f"[market_data] ws crashed: {e}")
            with self._ws_lock:
                self._ws = None
            if self._running:
                print("[market_data] reconnecting in 3s...")
                time.sleep(3)

    def _subscribe(self, ws, channel, symbols):
        if not symbols:
            return
        payload = {
            "type": "subscribe",
            "payload": {"channels": [{"name": channel, "symbols": symbols}]},
        }
        ws.send(json.dumps(payload))

    def _unsubscribe(self, ws, channel, symbols):
        if not symbols:
            return
        payload = {
            "type": "unsubscribe",
            "payload": {"channels": [{"name": channel, "symbols": symbols}]},
        }
        ws.send(json.dumps(payload))

    def _on_open(self, ws):
        symbols = self.get_symbols()   # always subscribes to the CURRENT list,
                                        # so reconnects pick up added/removed symbols
        print(f"[market_data] ws connected, subscribing {len(symbols)} symbols in chunks of {SUBSCRIBE_CHUNK_SIZE}")
        for chunk in _chunks(symbols, SUBSCRIBE_CHUNK_SIZE):
            self._subscribe(ws, "v2/ticker", chunk)
            self._subscribe(ws, f"candlestick_{RESOLUTION}", chunk)

    def _on_message(self, ws, message):
        # ADDED: wrap the entire handler -- a single malformed message
        # (e.g. a candle field sent as explicit null) must never be able
        # to propagate an unhandled exception out of this callback. That
        # was the actual trigger for the crash -> reconnect -> crash loop.
        try:
            self._handle_message(message)
        except Exception as e:
            print(f"[market_data] on_message error (ignored, feed continues): {e}")

    def _handle_message(self, message):
        try:
            msg = json.loads(message)
        except Exception:
            return

        msg_type = msg.get("type", "")
        symbol = msg.get("symbol")
        if not symbol or symbol not in self.candles:
            return

        if msg_type == "v2/ticker":
            with self._lock:
                if symbol not in self.ticker:
                    return
                self.ticker[symbol] = {
                    "close": msg.get("close"),
                    "mark_price": msg.get("mark_price"),
                    "high": msg.get("high"),
                    "low": msg.get("low"),
                    "volume": msg.get("volume"),
                    "timestamp": msg.get("timestamp"),
                }

        elif msg_type.startswith("candlestick_"):
            candle_time = msg.get("candle_start_time", msg.get("timestamp"))
            if candle_time is None:
                # CHANGED: no usable timestamp -> this tick can't be placed
                # in the series at all, skip it instead of storing garbage
                return

            candle = {
                "time": candle_time,
                # CHANGED: float(msg.get("open", 0)) -> _safe_float(msg.get("open"))
                # Root cause: when Delta sends a field as an explicit null
                # (key present, value None), dict.get's default does NOT
                # kick in (it only applies when the key is missing), so
                # float(None) was raised here uncaught.
                "open": _safe_float(msg.get("open")),
                "high": _safe_float(msg.get("high")),
                "low": _safe_float(msg.get("low")),
                "close": _safe_float(msg.get("close")),
                "volume": _safe_float(msg.get("volume")),
            }
            with self._lock:
                series = self.candles.get(symbol)
                if series is None:
                    return
                if series and series[-1]["time"] == candle["time"]:
                    series[-1] = candle           # update forming candle
                else:
                    series.append(candle)         # new candle closed
                    if len(series) > 1000:
                        del series[0]

    def _on_error(self, ws, error):
        print(f"[market_data] ws error: {error}")

    def _on_close(self, ws, code, msg):
        print(f"[market_data] ws closed: {code} {msg}")