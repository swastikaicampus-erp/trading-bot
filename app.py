import os
import json
import time
import hmac
import hashlib
import threading
from email.utils import parsedate_to_datetime

import requests
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
from delta_rest_client import DeltaRestClient

from market_data import MarketDataFeed, discover_perpetual_futures_symbols
from strategy import StrategyManager

load_dotenv()

app = Flask(__name__)
CORS(app, origins=os.getenv("ALLOWED_ORIGINS", "*").split(","))

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
WATCHLIST_FILE = "watchlist.json"

# Agar sirf USD-settled perps chahiye to yahan {"USD"} set karo. None =
# sabhi quote assets allowed (USD + USDT dono type ke perps aa jaayenge).
PERP_QUOTE_ASSET_FILTER = None

BASE_URL = os.getenv("DELTA_BASE_URL")
API_KEY = os.getenv("DELTA_API_KEY")
API_SECRET = os.getenv("DELTA_API_SECRET")

client = DeltaRestClient(
    base_url=BASE_URL,
    api_key=API_KEY,
    api_secret=API_SECRET
)


def _load_or_discover_watchlist():
    """
    Watchlist ab manually curate nahi karni -- startup par Delta ke saare
    LIVE perpetual futures khud discover ho jaate hain (BTCUSD, ETHUSD,
    SOLUSD + saare alt-coin perps). Agar watchlist.json already exist
    karti hai aur usme symbols hain (pichli run se, ya manually kisi ko
    exclude kiya gaya tha), wahi use hoti hai -- taaki restart par
    tumhara manual watchlist/<symbol> DELETE wapas na aa jaaye.
    """
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE) as f:
                data = json.load(f)
                symbols = data.get("symbols") or []
                if symbols:
                    print(f"[startup] loaded {len(symbols)} symbols from {WATCHLIST_FILE}")
                    return symbols
        except (OSError, json.JSONDecodeError):
            print(f"[startup] {WATCHLIST_FILE} unreadable, re-discovering from Delta")

    try:
        symbols = discover_perpetual_futures_symbols(quote_assets=PERP_QUOTE_ASSET_FILTER)
        print(f"[startup] discovered {len(symbols)} live perpetual futures from Delta")
        return symbols
    except Exception as e:
        print(f"[startup] discovery failed ({e}), falling back to empty watchlist")
        return []


WATCHLIST = _load_or_discover_watchlist()

# ---- live market data feed (candles + ticker) ----
feed = MarketDataFeed(WATCHLIST)
feed.start()

feed = MarketDataFeed(WATCHLIST)
feed.start()

# pehle function
def _place_order_for_strategy(order_body):
    ok, status, data = _signed_request("POST", "/v2/orders", body=order_body)
    if ok:
        return data.get("result", data)
    return {"error": _friendly_error(data).get("message") or str(data)}

# phir StrategyManager
strategy = StrategyManager(
    feed=feed,
    client=client,
    watchlist=WATCHLIST,
    config={
        "dry_run": DRY_RUN
    },
    place_order_fn=_place_order_for_strategy,
)

# ---- all-products cache (full Delta symbol universe) ----
_products_cache = {"data": [], "fetched_at": 0}
_products_lock = threading.Lock()
PRODUCTS_CACHE_TTL = 300  # seconds


# =======================================================================
# Delta signing helper -- used for every endpoint that isn't already
# well covered by delta_rest_client (orders, bracket orders, batch
# orders, positions, fills, leverage). This follows the exact signing
# spec from Delta's docs: HMAC-SHA256(secret, method+timestamp+path+
# query_string+body), sent as api-key/signature/timestamp headers.
#
# IMPORTANT: the query string used in the signature must be byte-for-
# byte identical to the one actually sent on the wire. We build it once
# and append it straight onto the URL -- we do NOT also pass it via
# requests' params=, because requests URL-encodes values (e.g. "," ->
# "%2C") which would silently desync the signed string from the actual
# request and cause "Signature Mismatch" on any filter with commas
# (product_ids, contract_types, states, etc.).
# =======================================================================
DELTA_ERROR_MESSAGES = {
    # Auth / infra errors
    "ip_not_whitelisted_for_api_key": "Server ka IP Delta API key ke whitelist me nahi hai. API Management me is IP ko add karo.",
    "Signature Mismatch": "Signature match nahi hua. api_secret, timestamp ya payload check karo.",
    "expired_signature": "Signature expire ho gaya -- system clock drift ho sakta hai. NTP sync check karo.",
    "InvalidApiKey": "API key invalid hai ya galat environment (prod/testnet) ki hai.",
    "UnauthorizedApiAccess": "Is API key ko is endpoint ki permission nahi hai (Trading permission on hai ya nahi check karo).",
    "Forbidden": "Request CDN ne block kar diya -- User-Agent header missing ho sakta hai.",
    # Order placement errors
    "insufficient_margin": "Order place karne ke liye margin kam hai selected leverage/size ke hisaab se.",
    "order_size_exceed_available": "Orderbook me itni liquidity nahi hai is order (jaise IOC) ko fill karne ke liye.",
    "risk_limits_breached": "Ye order account ke risk limits todega, isliye reject hua.",
    "invalid_contract": "Ye product/contract exist nahi karta ya expire ho chuka hai.",
    "immediate_liquidation": "Ye order turant liquidation trigger kar dega, isliye reject hua.",
    "out_of_bankruptcy": "Order price position ke bankruptcy price se bahar hai.",
    "self_matching_disrupted_post_only": "Auction ke dauran self-matching allowed nahi hai.",
    "immediate_execution_post_only": "Post-only order turant execute ho jaata, isliye reject hua.",
    "limit_price": "limit_price 0 ya negative nahi ho sakta -- agar zaroorat nahi hai to field hi mat bhejo ya null bhejo.",
}


def _friendly_error(payload):
    """Translate Delta's error codes into something actionable, but keep
    the raw error too so nothing gets silently swallowed."""
    if not isinstance(payload, dict):
        return {"raw": payload, "message": str(payload)}
    err = payload.get("error")
    code = None
    context = None
    if isinstance(err, dict):
        code = err.get("code")
        context = err.get("context")
    elif isinstance(err, str):
        code = err
    message = DELTA_ERROR_MESSAGES.get(code, payload.get("message") or code or "Unknown Delta API error")
    return {"code": code, "message": message, "context": context, "raw": err}


def _generate_signature(secret, message):
    return hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()


def _signed_request(method, path, query_params=None, body=None, timeout=(3, 27)):
    """Makes a signed request to Delta's REST API per their docs.
    Returns (ok: bool, status_code: int, data: dict)."""
    query_params = {k: v for k, v in (query_params or {}).items() if v is not None}
    timestamp = str(int(time.time()))

    query_string = ""
    if query_params:
        query_string = "?" + "&".join(f"{k}={v}" for k, v in query_params.items())

    payload = json.dumps(body) if body is not None else ""
    signature_data = method + timestamp + path + query_string + payload
    signature = _generate_signature(API_SECRET, signature_data)

    headers = {
        "api-key": API_KEY,
        "timestamp": timestamp,
        "signature": signature,
        "User-Agent": "delta-service-python",
        "Content-Type": "application/json",
    }

    # NOTE: query_string is already part of the URL here -- do NOT also
    # pass query_params via requests' params= kwarg, or requests will
    # re-encode it differently from what was signed above.
    url = f"{BASE_URL}{path}{query_string}"
    resp = requests.request(
        method, url,
        data=payload if body is not None else None,
        headers=headers,
        timeout=timeout,
    )

    try:
        data = resp.json()
    except ValueError:
        data = {"success": False, "error": resp.text}

    ok = resp.ok and data.get("success", False)
    return ok, resp.status_code, data


def _delta_json_response(ok, status_code, data):
    """Wraps a _signed_request result into our Flask JSON response shape."""
    if ok:
        return jsonify({"success": True, "data": data.get("result", data)})
    return jsonify({"success": False, "error": _friendly_error(data)}), status_code if status_code >= 400 else 400

def _place_order_for_strategy(order_body):
    """StrategyManager (strategy.py) isi ke through bracket orders bhejta
    hai -- delta_rest_client library ka place_order() bracket_* kwargs
    accept nahi karta, isliye same raw signed request use karte hain jo
    /place-order route bhi use karta hai."""
    ok, status, data = _signed_request("POST", "/v2/orders", body=order_body)
    if ok:
        return data.get("result", data)
    return {"error": _friendly_error(data).get("message") or str(data)}

def _check_clock_drift():
    """Compares local system time to Delta server time (via response
    Date header) so drift shows up in /health before it causes an
    expired_signature error on a real order."""
    try:
        resp = requests.get(f"{BASE_URL}/v2/rate_limits/quota", timeout=5)
        server_date = resp.headers.get("Date")
        if server_date:
            server_time = parsedate_to_datetime(server_date).timestamp()
            drift = round(server_time - time.time(), 2)
            return {"drift_seconds": drift, "ok": abs(drift) < 3}
    except Exception as e:
        return {"drift_seconds": None, "ok": None, "error": str(e)}
    return {"drift_seconds": None, "ok": None}


def _save_watchlist():
    with open(WATCHLIST_FILE, "w") as f:
        json.dump({"symbols": feed.get_symbols()}, f, indent=2)


def _fetch_all_products(force=False):
    """Fetches the full list of tradable products from Delta and caches
    it in memory for PRODUCTS_CACHE_TTL seconds, since this call returns
    hundreds of rows and shouldn't be hit on every dropdown open."""
    with _products_lock:
        if not force and (time.time() - _products_cache["fetched_at"] < PRODUCTS_CACHE_TTL):
            return _products_cache["data"]

        products = client.get_products()
        rows = products.get("result", products) if isinstance(products, dict) else products

        cleaned = []
        for row in rows:
            if not isinstance(row, dict) or not row.get("symbol"):
                continue
            state = row.get("state", "live")
            if state != "live":
                continue  # skip expired/delisted contracts
            cleaned.append({
                "symbol": row.get("symbol"),
                "id": row.get("id"),
                "contract_type": row.get("contract_type"),
                "underlying_asset": (row.get("underlying_asset") or {}).get("symbol"),
                "quoting_asset": (row.get("quoting_asset") or {}).get("symbol"),
            })
        cleaned.sort(key=lambda p: p["symbol"])

        _products_cache["data"] = cleaned
        _products_cache["fetched_at"] = time.time()
        return cleaned


# =======================================================================
# Health
# =======================================================================
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "dry_run": DRY_RUN,
        "watchlist": feed.get_symbols(),
        "clock": _check_clock_drift(),
    })


@app.route("/rate-limit", methods=["GET"])
def rate_limit_quota():
    """GET /v2/rate_limits/quota -- unauthenticated. Useful to poll from
    the dashboard when MultiSymbolScanner is hammering multiple symbols,
    so you see quota draining before a 429 actually hits."""
    try:
        resp = requests.get(f"{BASE_URL}/v2/rate_limits/quota", timeout=5)
        return jsonify({"success": True, "data": resp.json()})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =======================================================================
# Wallet / Balance
# =======================================================================
@app.route("/balance", methods=["GET"])
def get_balance():
    try:
        balance = client.get_balances(asset_id=3)  # 3 = USD asset_id on Delta
        return jsonify({"success": True, "data": balance})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =======================================================================
# Positions
# =======================================================================
@app.route("/positions", methods=["GET"])
def get_positions():
    """Single product position (real-time). Pass ?product_id=123"""
    try:
        product_id = request.args.get("product_id", type=int)
        positions = client.get_position(product_id)
        return jsonify({"success": True, "data": positions})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/positions/all", methods=["GET"])
def get_all_positions():
    """GET /v2/positions/margined -- all open positions at once (may lag
    up to ~10s; use /positions for a single real-time product)."""
    product_ids = request.args.get("product_ids")
    contract_types = request.args.get("contract_types")
    ok, status, data = _signed_request(
        "GET", "/v2/positions/margined",
        query_params={"product_ids": product_ids, "contract_types": contract_types},
    )
    return _delta_json_response(ok, status, data)


@app.route("/positions/close", methods=["POST"])
def close_position():
    """Closes a single position by placing a reduce_only market order
    for the full open size, in the opposite direction."""
    data = request.json or {}
    product_id = data.get("product_id")
    if not product_id:
        return jsonify({"success": False, "error": "product_id is required"}), 400

    try:
        position = client.get_position(product_id)
        pos_result = position.get("result", position) if isinstance(position, dict) else position
        size = pos_result.get("size", 0) if isinstance(pos_result, dict) else 0
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not fetch position: {e}"}), 500

    if not size:
        return jsonify({"success": False, "error": "No open position for this product_id"}), 400

    side = "sell" if size > 0 else "buy"
    order_body = {
        "product_id": product_id,
        "size": abs(size),
        "side": side,
        "order_type": "market_order",
        "reduce_only": True,
    }

    if DRY_RUN:
        return jsonify({"success": True, "dry_run": True, "order": order_body})

    ok, status, resp_data = _signed_request("POST", "/v2/orders", body=order_body)
    return _delta_json_response(ok, status, resp_data)


@app.route("/positions/close-all", methods=["POST"])
def close_all_positions():
    """POST /v2/positions/close_all"""
    data = request.json or {}
    body = {
        "close_all_portfolio": data.get("close_all_portfolio", True),
        "close_all_isolated": data.get("close_all_isolated", True),
        "user_id": data.get("user_id"),
    }
    if DRY_RUN:
        return jsonify({"success": True, "dry_run": True, "request": body})

    ok, status, resp_data = _signed_request("POST", "/v2/positions/close_all", body=body)
    return _delta_json_response(ok, status, resp_data)


@app.route("/positions/margin", methods=["POST"])
def change_position_margin():
    """POST /v2/positions/change_margin -- delta_margin positive to add,
    negative to remove."""
    data = request.json or {}
    body = {
        "product_id": data.get("product_id"),
        "delta_margin": str(data.get("delta_margin")),
    }
    ok, status, resp_data = _signed_request("POST", "/v2/positions/change_margin", body=body)
    return _delta_json_response(ok, status, resp_data)


@app.route("/positions/auto-topup", methods=["PUT"])
def set_auto_topup():
    """PUT /v2/positions/auto_topup"""
    data = request.json or {}
    body = {
        "product_id": data.get("product_id"),
        "auto_topup": data.get("auto_topup", False),
    }
    ok, status, resp_data = _signed_request("PUT", "/v2/positions/auto_topup", body=body)
    return _delta_json_response(ok, status, resp_data)


# =======================================================================
# Deadman Switch (Heartbeat) -- safety net so a crashed/disconnected bot
# doesn't leave orphaned orders sitting on the book. Flow:
#   1. POST /heartbeat/create once (usually at startup)
#   2. Background thread pings POST /heartbeat every HEARTBEAT_INTERVAL
#      seconds with a TTL slightly larger than the interval
#   3. If the process dies, Delta stops receiving acks and after
#      `unhealthy_count` misses it auto-cancels open orders for you
# =======================================================================
HEARTBEAT_ID = os.getenv("HEARTBEAT_ID", "delta_service_bot")
HEARTBEAT_TTL_MS = int(os.getenv("HEARTBEAT_TTL_MS", "30000"))  # 30s
HEARTBEAT_INTERVAL_SEC = int(os.getenv("HEARTBEAT_INTERVAL_SEC", "25"))  # send before TTL expires
HEARTBEAT_ENABLED = os.getenv("HEARTBEAT_ENABLED", "false").lower() == "true"

_heartbeat_state = {
    "running": False,
    "last_ack_at": None,
    "last_error": None,
}
_heartbeat_lock = threading.Lock()


def _heartbeat_loop():
    while True:
        with _heartbeat_lock:
            if not _heartbeat_state["running"]:
                return
        try:
            ok, status, data = _signed_request(
                "POST", "/v2/heartbeat",
                body={"heartbeat_id": HEARTBEAT_ID, "ttl": HEARTBEAT_TTL_MS},
            )
            with _heartbeat_lock:
                if ok:
                    _heartbeat_state["last_ack_at"] = time.time()
                    _heartbeat_state["last_error"] = None
                else:
                    _heartbeat_state["last_error"] = _friendly_error(data)
        except Exception as e:
            with _heartbeat_lock:
                _heartbeat_state["last_error"] = str(e)
        time.sleep(HEARTBEAT_INTERVAL_SEC)


@app.route("/heartbeat/setup", methods=["POST"])
def setup_heartbeat():
    """POST /v2/heartbeat/create -- register the deadman switch config.
    Call this once. Example body:
    {
      "impact": "contracts",
      "contract_types": ["perpetual_futures"],
      "product_symbols": [],
      "config": [{"action": "cancel_orders", "unhealthy_count": 1}]
    }
    """
    data = request.json or {}
    body = {
        "heartbeat_id": data.get("heartbeat_id", HEARTBEAT_ID),
        "impact": data.get("impact", "contracts"),
        "contract_types": data.get("contract_types", ["perpetual_futures"]),
        "config": data.get("config", [{"action": "cancel_orders", "unhealthy_count": 1}]),
    }
    if data.get("underlying_assets"):
        body["underlying_assets"] = data["underlying_assets"]
    if data.get("product_symbols"):
        body["product_symbols"] = data["product_symbols"]

    ok, status, resp_data = _signed_request("POST", "/v2/heartbeat/create", body=body)
    return _delta_json_response(ok, status, resp_data)


@app.route("/heartbeat/start", methods=["POST"])
def start_heartbeat_loop():
    """Starts the background thread that pings /v2/heartbeat every
    HEARTBEAT_INTERVAL_SEC seconds. Call /heartbeat/setup first."""
    with _heartbeat_lock:
        if _heartbeat_state["running"]:
            return jsonify({"success": False, "error": "Heartbeat loop already running"}), 409
        _heartbeat_state["running"] = True
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    return jsonify({"success": True, "message": f"Heartbeat loop started (every {HEARTBEAT_INTERVAL_SEC}s, ttl {HEARTBEAT_TTL_MS}ms)"})


@app.route("/heartbeat/stop", methods=["POST"])
def stop_heartbeat_loop():
    """Stops the background ack loop. Note: this does NOT disable the
    heartbeat on Delta's side -- send ttl=0 via /heartbeat/ack for that,
    otherwise Delta will still cancel your orders once acks stop."""
    with _heartbeat_lock:
        _heartbeat_state["running"] = False
    return jsonify({"success": True, "message": "Heartbeat loop stopped locally"})


@app.route("/heartbeat/ack", methods=["POST"])
def manual_heartbeat_ack():
    """Manual single ack -- pass {"ttl": 0} to fully disable the
    deadman switch on Delta's side (graceful shutdown)."""
    data = request.json or {}
    ttl = data.get("ttl", HEARTBEAT_TTL_MS)
    ok, status, resp_data = _signed_request(
        "POST", "/v2/heartbeat", body={"heartbeat_id": HEARTBEAT_ID, "ttl": ttl}
    )
    return _delta_json_response(ok, status, resp_data)


@app.route("/heartbeat/status", methods=["GET"])
def heartbeat_status():
    """Local loop status + live status pulled from Delta.
    GET /v2/heartbeat requires user_id, so we fetch it from /v2/profile
    first (small extra call, but keeps this endpoint self-contained)."""
    with _heartbeat_lock:
        local_state = dict(_heartbeat_state)

    profile_ok, _, profile_data = _signed_request("GET", "/v2/profile")
    if not profile_ok:
        return jsonify({"success": True, "local": local_state, "delta": None,
                         "note": "Could not fetch user_id from /v2/profile to query heartbeat status"})

    user_id = (profile_data.get("result") or {}).get("id")
    ok, status, data = _signed_request(
        "GET", "/v2/heartbeat",
        query_params={"user_id": user_id, "heartbeat_id": HEARTBEAT_ID},
    )
    return jsonify({
        "success": True,
        "local": local_state,
        "delta": data.get("result") if ok else _friendly_error(data),
    })


# =======================================================================
# Orders -- full CreateOrderRequest support (stop orders, bracket
# params, reduce_only, client_order_id, time_in_force, mmp, post_only)
# =======================================================================
@app.route("/place-order", methods=["POST"])
def place_order():
    data = request.json or {}
    product_id = data.get("product_id")
    side = data.get("side")
    size = data.get("size")
    order_type = data.get("order_type", "market_order")

    if not product_id or not side or not size:
        return jsonify({"success": False, "error": "product_id, side, size are required"}), 400

    order_body = {
        "product_id": product_id,
        "size": size,
        "side": side,
        "order_type": order_type,
    }

    # Limit price required for limit orders
    if data.get("limit_price") is not None:
        order_body["limit_price"] = str(data["limit_price"])

    # Stop-loss / take-profit single-leg stop order support
    if data.get("stop_order_type"):
        order_body["stop_order_type"] = data["stop_order_type"]
    if data.get("stop_price") is not None:
        order_body["stop_price"] = str(data["stop_price"])
    if data.get("trail_amount") is not None:
        order_body["trail_amount"] = str(data["trail_amount"])
    if data.get("stop_trigger_method"):
        order_body["stop_trigger_method"] = data["stop_trigger_method"]

    # Bracket order params (SL+TP attached to the entry order itself)
    for key in (
        "bracket_stop_trigger_method", "bracket_stop_loss_limit_price",
        "bracket_stop_loss_price", "bracket_trail_amount",
        "bracket_take_profit_limit_price", "bracket_take_profit_price",
    ):
        if data.get(key) is not None:
            order_body[key] = str(data[key])

    # Execution controls
    order_body["time_in_force"] = data.get("time_in_force", "gtc")
    order_body["mmp"] = data.get("mmp", "disabled")
    order_body["post_only"] = data.get("post_only", False)
    order_body["reduce_only"] = data.get("reduce_only", False)
    if data.get("client_order_id"):
        order_body["client_order_id"] = data["client_order_id"]
    if data.get("cancel_orders_accepted") is not None:
        order_body["cancel_orders_accepted"] = data["cancel_orders_accepted"]

    if DRY_RUN:
        return jsonify({"success": True, "dry_run": True, "order": order_body})

    ok, status, resp_data = _signed_request("POST", "/v2/orders", body=order_body)
    return _delta_json_response(ok, status, resp_data)


@app.route("/orders/<int:order_id>", methods=["PUT"])
def edit_order(order_id):
    """PUT /v2/orders -- edit size/limit_price/stop_price/trail_amount
    on an existing order without cancel+recreate."""
    data = request.json or {}
    product_id = data.get("product_id")
    if not product_id:
        return jsonify({"success": False, "error": "product_id is required"}), 400

    body = {"id": order_id, "product_id": product_id}
    for key in ("limit_price", "size", "mmp", "post_only", "stop_price", "trail_amount"):
        if data.get(key) is not None:
            body[key] = data[key]

    if DRY_RUN:
        return jsonify({"success": True, "dry_run": True, "order": body})

    ok, status, resp_data = _signed_request("PUT", "/v2/orders", body=body)
    return _delta_json_response(ok, status, resp_data)


@app.route("/cancel-order", methods=["POST"])
def cancel_order():
    data = request.json or {}
    try:
        response = client.cancel_order(data.get("product_id"), data.get("order_id"))
        return jsonify({"success": True, "data": response})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/orders/cancel-all", methods=["POST"])
def cancel_all_orders():
    """DELETE /v2/orders/all -- cancel every order for a product, or by
    contract type, or literally everything if neither is given."""
    data = request.json or {}
    body = {
        "product_id": data.get("product_id"),
        "contract_types": data.get("contract_types"),
        "cancel_limit_orders": data.get("cancel_limit_orders", False),
        "cancel_stop_orders": data.get("cancel_stop_orders", False),
        "cancel_reduce_only_orders": data.get("cancel_reduce_only_orders", False),
    }
    ok, status, resp_data = _signed_request("DELETE", "/v2/orders/all", body=body)
    return _delta_json_response(ok, status, resp_data)


@app.route("/orders", methods=["GET"])
def get_open_orders():
    """GET /v2/orders -- active (open/pending) orders, with pagination
    and filter passthrough."""
    params = {
        "product_ids": request.args.get("product_ids"),
        "states": request.args.get("states"),
        "contract_types": request.args.get("contract_types"),
        "order_types": request.args.get("order_types"),
        "start_time": request.args.get("start_time"),
        "end_time": request.args.get("end_time"),
        "after": request.args.get("after"),
        "before": request.args.get("before"),
        "page_size": request.args.get("page_size"),
    }
    ok, status, data = _signed_request("GET", "/v2/orders", query_params=params)
    if ok:
        return jsonify({"success": True, "data": data.get("result"), "meta": data.get("meta")})
    return jsonify({"success": False, "error": _friendly_error(data)}), status if status >= 400 else 400


@app.route("/orders/history", methods=["GET"])
def get_order_history():
    """GET /v2/orders/history -- closed and cancelled orders."""
    params = {
        "product_ids": request.args.get("product_ids"),
        "contract_types": request.args.get("contract_types"),
        "order_types": request.args.get("order_types"),
        "start_time": request.args.get("start_time"),
        "end_time": request.args.get("end_time"),
        "after": request.args.get("after"),
        "before": request.args.get("before"),
        "page_size": request.args.get("page_size"),
    }
    ok, status, data = _signed_request("GET", "/v2/orders/history", query_params=params)
    if ok:
        return jsonify({"success": True, "data": data.get("result"), "meta": data.get("meta")})
    return jsonify({"success": False, "error": _friendly_error(data)}), status if status >= 400 else 400


@app.route("/orders/id/<int:order_id>", methods=["GET"])
def get_order_by_id(order_id):
    ok, status, data = _signed_request("GET", f"/v2/orders/{order_id}")
    return _delta_json_response(ok, status, data)


@app.route("/orders/client/<client_oid>", methods=["GET"])
def get_order_by_client_oid(client_oid):
    ok, status, data = _signed_request("GET", f"/v2/orders/client_order_id/{client_oid}")
    return _delta_json_response(ok, status, data)


# =======================================================================
# Bracket orders -- attach/edit SL+TP on an existing position/order in
# one call. Useful for the multi-confirmation signal stack: place entry,
# then immediately lock in SL/TP without a second raw order.
# =======================================================================
@app.route("/orders/bracket", methods=["POST"])
def place_bracket_order():
    data = request.json or {}
    product_id = data.get("product_id")
    if not product_id:
        return jsonify({"success": False, "error": "product_id is required"}), 400

    body = {"product_id": product_id}
    if data.get("stop_loss_order"):
        body["stop_loss_order"] = data["stop_loss_order"]
    if data.get("take_profit_order"):
        body["take_profit_order"] = data["take_profit_order"]
    if data.get("bracket_stop_trigger_method"):
        body["bracket_stop_trigger_method"] = data["bracket_stop_trigger_method"]

    if DRY_RUN:
        return jsonify({"success": True, "dry_run": True, "bracket_order": body})

    ok, status, resp_data = _signed_request("POST", "/v2/orders/bracket", body=body)
    return _delta_json_response(ok, status, resp_data)


@app.route("/orders/bracket", methods=["PUT"])
def edit_bracket_order():
    data = request.json or {}
    order_id = data.get("id")
    product_id = data.get("product_id")
    if not order_id or not product_id:
        return jsonify({"success": False, "error": "id and product_id are required"}), 400

    body = {"id": order_id, "product_id": product_id}
    for key in (
        "bracket_stop_loss_limit_price", "bracket_stop_loss_price",
        "bracket_take_profit_limit_price", "bracket_take_profit_price",
        "bracket_trail_amount", "bracket_stop_trigger_method",
    ):
        if data.get(key) is not None:
            body[key] = data[key]

    ok, status, resp_data = _signed_request("PUT", "/v2/orders/bracket", body=body)
    return _delta_json_response(ok, status, resp_data)


# =======================================================================
# Batch orders -- place/edit/delete up to 50 orders for one product in
# a single call. Handy when the scanner fires signals on several
# symbols at once and you want to save on rate-limit weight.
# Note: batch endpoints don't support stop orders, only plain limit
# orders, and don't accept time_in_force (Delta assumes gtc).
# =======================================================================
@app.route("/orders/batch", methods=["POST"])
def create_batch_orders():
    data = request.json or {}
    product_id = data.get("product_id")
    orders = data.get("orders", [])
    if not product_id or not orders:
        return jsonify({"success": False, "error": "product_id and orders[] are required"}), 400

    body = {"product_id": product_id, "orders": orders}

    if DRY_RUN:
        return jsonify({"success": True, "dry_run": True, "batch": body})

    ok, status, resp_data = _signed_request("POST", "/v2/orders/batch", body=body)
    return _delta_json_response(ok, status, resp_data)


@app.route("/orders/batch", methods=["PUT"])
def edit_batch_orders():
    data = request.json or {}
    product_id = data.get("product_id")
    orders = data.get("orders", [])
    if not product_id or not orders:
        return jsonify({"success": False, "error": "product_id and orders[] are required"}), 400

    ok, status, resp_data = _signed_request(
        "PUT", "/v2/orders/batch", body={"product_id": product_id, "orders": orders}
    )
    return _delta_json_response(ok, status, resp_data)


@app.route("/orders/batch", methods=["DELETE"])
def delete_batch_orders():
    data = request.json or {}
    product_id = data.get("product_id")
    orders = data.get("orders", [])
    if not product_id or not orders:
        return jsonify({"success": False, "error": "product_id and orders[] are required"}), 400

    ok, status, resp_data = _signed_request(
        "DELETE", "/v2/orders/batch", body={"product_id": product_id, "orders": orders}
    )
    return _delta_json_response(ok, status, resp_data)


# =======================================================================
# Fills -- actual executions, useful for journaling / slippage tracking
# =======================================================================
@app.route("/fills", methods=["GET"])
def get_fills():
    params = {
        "product_ids": request.args.get("product_ids"),
        "contract_types": request.args.get("contract_types"),
        "start_time": request.args.get("start_time"),
        "end_time": request.args.get("end_time"),
        "after": request.args.get("after"),
        "before": request.args.get("before"),
        "page_size": request.args.get("page_size"),
    }
    ok, status, data = _signed_request("GET", "/v2/fills", query_params=params)
    if ok:
        return jsonify({"success": True, "data": data.get("result"), "meta": data.get("meta")})
    return jsonify({"success": False, "error": _friendly_error(data)}), status if status >= 400 else 400


# =======================================================================
# Leverage
# =======================================================================
@app.route("/leverage/<int:product_id>", methods=["GET"])
def get_leverage(product_id):
    ok, status, data = _signed_request("GET", f"/v2/products/{product_id}/orders/leverage")
    return _delta_json_response(ok, status, data)


@app.route("/leverage/<int:product_id>", methods=["POST"])
def set_leverage(product_id):
    data = request.json or {}
    leverage = data.get("leverage")
    if leverage is None:
        return jsonify({"success": False, "error": "leverage is required"}), 400

    ok, status, resp_data = _signed_request(
        "POST", f"/v2/products/{product_id}/orders/leverage", body={"leverage": leverage}
    )
    return _delta_json_response(ok, status, resp_data)


# =======================================================================
# Market data (local feed) -- unchanged
# =======================================================================
@app.route("/candles/<symbol>", methods=["GET"])
def get_candles(symbol):
    symbol = symbol.upper()
    if symbol not in feed.get_symbols():
        return jsonify({"success": False, "error": f"{symbol} not in watchlist"}), 404
    limit = request.args.get("limit", type=int)
    return jsonify({"success": True, "symbol": symbol, "data": feed.get_candles(symbol, limit)})


@app.route("/ticker/<symbol>", methods=["GET"])
def get_ticker(symbol):
    symbol = symbol.upper()
    if symbol not in feed.get_symbols():
        return jsonify({"success": False, "error": f"{symbol} not in watchlist"}), 404
    return jsonify({"success": True, "symbol": symbol, "data": feed.get_ticker(symbol)})


@app.route("/tickers", methods=["GET"])
def get_tickers():
    return jsonify({"success": True, "data": feed.get_all_tickers()})


# ---------------------------------------------------------------------
# Products -- the FULL symbol universe listed on Delta Exchange.
# Use this to populate a searchable dropdown; use /watchlist to control
# which of these are actually subscribed to on the live feed.
# ---------------------------------------------------------------------
@app.route("/products", methods=["GET"])
def get_products():
    try:
        contract_type = request.args.get("contract_type")  # e.g. perpetual_futures
        force = request.args.get("refresh") == "true"
        products = _fetch_all_products(force=force)
        if contract_type:
            products = [p for p in products if p["contract_type"] == contract_type]
        return jsonify({"success": True, "count": len(products), "data": products})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ---------------------------------------------------------------------
# Watchlist management -- add/remove symbols on the live feed at runtime.
# ---------------------------------------------------------------------
@app.route("/watchlist", methods=["GET"])
def get_watchlist():
    return jsonify({"success": True, "data": feed.get_symbols()})


@app.route("/watchlist", methods=["POST"])
def add_to_watchlist():
    data = request.json or {}
    symbol = (data.get("symbol") or "").upper().strip()
    if not symbol:
        return jsonify({"success": False, "error": "symbol is required"}), 400

    # validate against Delta's live product list so typos don't sneak in
    valid_symbols = {p["symbol"] for p in _fetch_all_products()}
    if symbol not in valid_symbols:
        return jsonify({"success": False, "error": f"{symbol} is not a live symbol on Delta"}), 400

    added = feed.add_symbol(symbol)
    if not added:
        return jsonify({"success": False, "error": f"{symbol} already in watchlist"}), 409

    strategy.add_symbols([symbol])
    _save_watchlist()
    return jsonify({"success": True, "data": feed.get_symbols()})


@app.route("/watchlist/<symbol>", methods=["DELETE"])
def remove_from_watchlist(symbol):
    symbol = symbol.upper()
    removed = feed.remove_symbol(symbol)
    if not removed:
        return jsonify({"success": False, "error": f"{symbol} not in watchlist"}), 404

    _save_watchlist()
    return jsonify({"success": True, "data": feed.get_symbols()})


@app.route("/watchlist/sync", methods=["POST"])
def sync_watchlist():
    """
    Re-discovers all LIVE perpetual futures from Delta and adds any new
    listings to the watchlist + strategy + live feed. Deliberately does
    NOT remove existing symbols (a symbol could still have an open trade
    or just be temporarily filtered out on Delta's side) -- use the
    DELETE /watchlist/<symbol> route to remove something manually.
    """
    try:
        live_symbols = discover_perpetual_futures_symbols(quote_assets=PERP_QUOTE_ASSET_FILTER)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

    current = set(feed.get_symbols())
    new_symbols = [s for s in live_symbols if s not in current]

    if new_symbols:
        feed.add_symbols(new_symbols)
        strategy.add_symbols(new_symbols)
        _save_watchlist()

    return jsonify({"success": True, "added": new_symbols, "total": len(feed.get_symbols())})


# ---------------------------------------------------------------------
# Strategy control -- multi-confirmation signal strategy, start/stop/status.
# See strategy.py for the actual logic; swap it out for whatever real
# strategy you want, the routes below just drive that runner.
# ---------------------------------------------------------------------

@app.route("/strategy/status", methods=["GET"])
def strategy_status():
    return jsonify({"success": True, "data": strategy.status()})


@app.route("/strategy/config", methods=["GET"])
def get_strategy_config():
    return jsonify({"success": True, "data": strategy.config})


@app.route("/strategy/config", methods=["POST"])
def update_strategy_config():
    data = request.json or {}
    try:
        updated = strategy.update_config(data)
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 409
    return jsonify({"success": True, "data": updated})


@app.route("/strategy/start", methods=["POST"])
def strategy_start():
    strategy.start()
    return jsonify({"success": True, "data": strategy.status()})


@app.route("/strategy/stop", methods=["POST"])
def strategy_stop():
    strategy.stop()
    return jsonify({"success": True, "data": strategy.status()})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5002))

    # If HEARTBEAT_ENABLED=true in .env, assume /heartbeat/setup was
    # already called once (or config never changes) and auto-start the
    # ack loop so the deadman switch is live from process start.
    if HEARTBEAT_ENABLED and not DRY_RUN:
        with _heartbeat_lock:
            _heartbeat_state["running"] = True
        threading.Thread(target=_heartbeat_loop, daemon=True).start()
        print(f"[heartbeat] auto-started: id={HEARTBEAT_ID}, ttl={HEARTBEAT_TTL_MS}ms, "
              f"interval={HEARTBEAT_INTERVAL_SEC}s")

    # debug=False: this process is bound to 0.0.0.0 and handles real API
    # keys -- Werkzeug's interactive debugger is a remote-code-execution
    # risk if left on and must never be enabled here.
    # use_reloader=False: the reloader would start `feed` twice.
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)