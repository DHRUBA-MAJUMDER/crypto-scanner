import logging
import math
import os
import threading
import time
import traceback
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from flask import Flask, jsonify
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bot")

# ==========================================
# 0. WEB SERVER & RENDER 24/7 SELF-PING
# ==========================================
app = Flask(__name__)

@app.route("/")
@app.route("/health")
def health():
    return jsonify({"status": "running", "timestamp": int(time.time())}), 200

def keep_alive_pinger():
    render_url = os.getenv("RENDER_EXTERNAL_URL")
    while True:
        time.sleep(240)
        try:
            target = (
                f"{render_url}/health"
                if render_url
                else f"http://127.0.0.1:{os.environ.get('PORT', 10000)}/health"
            )
            requests.get(target, timeout=5)
        except Exception:
            pass

# ==========================================
# 1. CREDENTIALS & RISK PARAMETERS
# ==========================================
API_KEY = (os.getenv("COINSWITCH_API_KEY") or "").strip()
SECRET_KEY = (os.getenv("COINSWITCH_SECRET_KEY") or "").strip()
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

if not API_KEY or not SECRET_KEY:
    raise ValueError("CRITICAL: API_KEY or SECRET_KEY is missing!")

BASE_URL = "https://coinswitch.co"
EXCHANGE_NAME = "EXCHANGE_2"
TIMEFRAME = "15m"

# ENTERPRISE RISK SETTINGS (8 USDT Margin | 6x Leverage | 4 USDT Daily Loss Cap)
LEVERAGE = 6
MAX_MARGIN_PER_TRADE_USDT = 8.0
MAX_DAILY_LOSS_USDT = 4.0

MIN_INR_REQUIRED = 150.0
ROUND_TRIP_FEE_PCT = 0.0012
MAX_CONCURRENT_TRADES = 2
MAX_SAME_DIRECTION_TRADES = 2
MAX_CONSECUTIVE_ERRORS = 5

LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING_ENABLED", "true").strip().lower() in ("1", "true", "yes")

# ---------- AI DYNAMIC SETTINGS ----------
DYNAMIC_SETTINGS = {
    "atr_buffer": 0.50
}

BREAKOUT_LOOKBACK = 12
VOL_MULT = 1.0
RSI_LONG_MIN, RSI_LONG_MAX = 50, 75
RSI_SHORT_MIN, RSI_SHORT_MAX = 25, 50
EDGE_MIN_WINRATE = 0.40
REQUIRE_EMA_TREND = True

RECONCILE_GRACE_SECONDS = 45
MISSING_CONFIRMATIONS = 2
MAX_SIGNAL_AGE_SEC = 1200

SUPPORTED_COINSWITCH_SYMBOLS = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT",
    "POLUSDT", "RENDERUSDT", "1000PEPEUSDT", "1000SHIBUSDT", "1000BONKUSDT", "1000FLOKIUSDT",
    "AVAXUSDT", "LINKUSDT", "DOTUSDT", "NEARUSDT", "LTCUSDT", "UNIUSDT",
    "ATOMUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "SUIUSDT", "FILUSDT",
    "INJUSDT", "STXUSDT", "IMXUSDT", "ETCUSDT", "AAVEUSDT", "GALAUSDT",
    "SANDUSDT", "FETUSDT", "TIAUSDT", "SEIUSDT", "ORDIUSDT", "WLDUSDT",
    "JUPUSDT", "PENDLEUSDT", "ENAUSDT", "BCHUSDT", "TRXUSDT", "WIFUSDT",
    "FTMUSDT", "LDOUSDT", "RUNEUSDT", "CRVUSDT", "MANAUSDT", "AXSUSDT",
    "ALGOUSDT", "HBARUSDT", "THETAUSDT", "EGLDUSDT", "QNTUSDT", "SNXUSDT",
    "MKRUSDT", "GRTUSDT", "XLMUSDT", "ARUSDT", "DYDXUSDT", "BLURUSDT",
    "GMXUSDT", "AGIXUSDT", "COMPUSDT", "ICPUSDT"
}

# ==========================================
# 1b. INSTRUMENT PRECISION TABLE & HELPERS
# ==========================================
INSTRUMENT_SPECS = {
    "BTCUSDT": {"step": "0.001", "tick": "0.1"},
    "ETHUSDT": {"step": "0.001", "tick": "0.01"},
    "BNBUSDT": {"step": "0.01", "tick": "0.01"},
    "BCHUSDT": {"step": "0.001", "tick": "0.01"},
    "MKRUSDT": {"step": "0.001", "tick": "0.1"},
    "SOLUSDT": {"step": "0.01", "tick": "0.01"},
    "AAVEUSDT": {"step": "0.01", "tick": "0.01"},
    "QNTUSDT": {"step": "0.01", "tick": "0.01"},
    "COMPUSDT": {"step": "0.01", "tick": "0.01"},
    "GMXUSDT": {"step": "0.01", "tick": "0.01"},
    "AVAXUSDT": {"step": "0.1", "tick": "0.01"},
    "LINKUSDT": {"step": "0.1", "tick": "0.001"},
    "LTCUSDT": {"step": "0.01", "tick": "0.01"},
    "UNIUSDT": {"step": "0.1", "tick": "0.001"},
    "ATOMUSDT": {"step": "0.1", "tick": "0.001"},
    "APTUSDT": {"step": "0.1", "tick": "0.001"},
    "INJUSDT": {"step": "0.1", "tick": "0.001"},
    "ETCUSDT": {"step": "0.1", "tick": "0.001"},
    "TIAUSDT": {"step": "0.1", "tick": "0.0001"},
    "ORDIUSDT": {"step": "0.1", "tick": "0.001"},
    "EGLDUSDT": {"step": "0.1", "tick": "0.01"},
    "ICPUSDT": {"step": "0.1", "tick": "0.001"},
    "RENDERUSDT": {"step": "0.1", "tick": "0.001"},
    "DOTUSDT": {"step": "0.1", "tick": "0.001"},
    "NEARUSDT": {"step": "0.1", "tick": "0.001"},
    "FILUSDT": {"step": "0.1", "tick": "0.001"},
    "RUNEUSDT": {"step": "0.1", "tick": "0.001"},
    "AXSUSDT": {"step": "0.1", "tick": "0.001"},
    "THETAUSDT": {"step": "0.1", "tick": "0.0001"},
    "SNXUSDT": {"step": "0.1", "tick": "0.001"},
    "ARUSDT": {"step": "0.1", "tick": "0.001"},
    "DYDXUSDT": {"step": "0.1", "tick": "0.001"},
    "WLDUSDT": {"step": "0.1", "tick": "0.0001"},
    "PENDLEUSDT": {"step": "0.1", "tick": "0.0001"},
    "WIFUSDT": {"step": "0.1", "tick": "0.0001"},
    "LDOUSDT": {"step": "0.1", "tick": "0.0001"},
    "SUIUSDT": {"step": "0.1", "tick": "0.0001"},
    "OPUSDT": {"step": "0.1", "tick": "0.0001"},
    "ARBUSDT": {"step": "0.1", "tick": "0.0001"},
    "STXUSDT": {"step": "0.1", "tick": "0.0001"},
    "IMXUSDT": {"step": "0.1", "tick": "0.0001"},
    "FETUSDT": {"step": "1", "tick": "0.0001"},
    "SEIUSDT": {"step": "1", "tick": "0.0001"},
    "JUPUSDT": {"step": "1", "tick": "0.0001"},
    "ENAUSDT": {"step": "1", "tick": "0.0001"},
    "TRXUSDT": {"step": "1", "tick": "0.00001"},
    "FTMUSDT": {"step": "1", "tick": "0.0001"},
    "CRVUSDT": {"step": "0.1", "tick": "0.0001"},
    "MANAUSDT": {"step": "1", "tick": "0.0001"},
    "SANDUSDT": {"step": "1", "tick": "0.0001"},
    "GALAUSDT": {"step": "1", "tick": "0.00001"},
    "ALGOUSDT": {"step": "1", "tick": "0.0001"},
    "HBARUSDT": {"step": "1", "tick": "0.00001"},
    "GRTUSDT": {"step": "1", "tick": "0.00001"},
    "XLMUSDT": {"step": "1", "tick": "0.00001"},
    "BLURUSDT": {"step": "1", "tick": "0.0001"},
    "AGIXUSDT": {"step": "1", "tick": "0.0001"},
    "XRPUSDT": {"step": "0.1", "tick": "0.0001"},
    "DOGEUSDT": {"step": "1", "tick": "0.00001"},
    "ADAUSDT": {"step": "1", "tick": "0.0001"},
    "POLUSDT": {"step": "1", "tick": "0.0001"},
    "1000PEPEUSDT": {"step": "1", "tick": "0.000001"},
    "1000SHIBUSDT": {"step": "1", "tick": "0.000001"},
    "1000BONKUSDT": {"step": "1", "tick": "0.000001"},
    "1000FLOKIUSDT": {"step": "1", "tick": "0.00001"},
}

balance_state = {"avail": 0.0}
reject_stats = Counter()
reject_lock = threading.Lock()
bad_symbols = {}

local_daily_pnl = {"date": "", "pnl": 0.0, "synced": False}

def reject(reason: str):
    with reject_lock:
        reject_stats[reason] += 1

def pop_reject_stats() -> dict:
    with reject_lock:
        data = dict(reject_stats)
        reject_stats.clear()
        return data

def normalize_symbol(sym) -> str:
    s = str(sym or "").upper().strip()
    for ch in ("/", "-", "_"):
        s = s.replace(ch, "")
    if s.endswith("PERP"):
        s = s[:-4]
    return s

def safe_float(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

def _pick(d: dict, *keys):
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def quantize_by_step(val: float, step_str: str, round_mode=ROUND_DOWN) -> float:
    try:
        d_val = Decimal(str(val))
        d_step = Decimal(str(step_str))
        if d_step <= 0:
            return val
        q = (d_val / d_step).to_integral_value(rounding=round_mode) * d_step
        return float(q)
    except Exception:
        return val

def format_price(val: float, symbol: str = "") -> float:
    sym = normalize_symbol(symbol)
    spec = INSTRUMENT_SPECS.get(sym)
    if spec and spec.get("tick"):
        return quantize_by_step(val, spec["tick"], ROUND_HALF_UP)
    if val >= 1000:
        return round(val, 2)
    elif val >= 10:
        return round(val, 3)
    elif val >= 1:
        return round(val, 4)
    elif val >= 0.01:
        return round(val, 5)
    else:
        return round(val, 6)

def format_quantity(val: float, symbol: str, entry_price: float) -> float:
    sym = normalize_symbol(symbol)
    spec = INSTRUMENT_SPECS.get(sym)
    if spec and spec.get("step"):
        return quantize_by_step(val, spec["step"], ROUND_DOWN)
    if entry_price >= 1000:
        return math.floor(val * 1000) / 1000.0
    elif entry_price >= 10:
        return math.floor(val * 100) / 100.0
    elif entry_price >= 0.5:
        return math.floor(val * 10) / 10.0
    else:
        return float(math.floor(val))

# ==========================================
# 2. PERSISTENT FIREBASE DATABASE (Fail-Safe)
# ==========================================
FIREBASE_URL = "https://tradefix-939af-default-rtdb.firebaseio.com"

class FirebaseDatabase:
    def __init__(self):
        self.db_lock = threading.Lock()

    def save_position(self, pos: dict) -> bool:
        with self.db_lock:
            try:
                r = requests.put(f"{FIREBASE_URL}/active_positions/{pos['symbol']}.json", json=pos, timeout=5)
                if r.status_code == 200:
                    return True
                logger.error(f"Firebase Save HTTP {r.status_code}: {r.text}")
            except Exception as e:
                logger.error(f"Firebase Save Error: {e}")
            return False

    def delete_position(self, symbol: str) -> bool:
        with self.db_lock:
            try:
                r = requests.delete(f"{FIREBASE_URL}/active_positions/{symbol}.json", timeout=5)
                if r.status_code == 200:
                    return True
                logger.error(f"Firebase Delete HTTP {r.status_code}: {r.text}")
            except Exception as e:
                logger.error(f"Firebase Delete Error: {e}")
            return False

    def load_all_positions(self) -> dict:
        with self.db_lock:
            try:
                r = requests.get(f"{FIREBASE_URL}/active_positions.json", timeout=5)
                if r.status_code == 200:
                    data = r.json()
                    return data if isinstance(data, dict) else {}
            except Exception as e:
                logger.error(f"Firebase Load Error: {e}")
            return {}

    def record_closed_trade(self, symbol, side, entry_price, exit_price, qty, net_pnl, reason):
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        record = {
            "symbol": symbol, "side": side, "entry_price": entry_price,
            "exit_price": exit_price, "quantity": qty, "net_pnl_usdt": net_pnl,
            "reason": reason, "closed_date": today_str, "closed_at": int(time.time())
        }
        with self.db_lock:
            if local_daily_pnl["date"] != today_str:
                local_daily_pnl["date"] = today_str
                local_daily_pnl["pnl"] = 0.0
                local_daily_pnl["synced"] = True
            local_daily_pnl["pnl"] += net_pnl
            try:
                requests.post(f"{FIREBASE_URL}/trade_history.json", json=record, timeout=5)
                requests.put(
                    f"{FIREBASE_URL}/daily_pnl/{today_str}.json",
                    json={"pnl": local_daily_pnl["pnl"], "updated_at": int(time.time())},
                    timeout=5
                )
            except Exception as e:
                logger.error(f"Firebase Record Closed Trade Error: {e}")

    def get_today_realized_pnl(self):
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self.db_lock:
            try:
                r = requests.get(f"{FIREBASE_URL}/trade_history.json", timeout=8)
                if r.status_code == 200:
                    history = r.json() or {}
                    if isinstance(history, dict):
                        fb_pnl = sum(
                            safe_float(v.get("net_pnl_usdt", 0.0))
                            for v in history.values()
                            if isinstance(v, dict) and v.get("closed_date") == today_str
                        )
                        local_daily_pnl["date"] = today_str
                        local_daily_pnl["pnl"] = fb_pnl
                        local_daily_pnl["synced"] = True
                        return fb_pnl, True
            except Exception as e:
                logger.error(f"Firebase PnL Fetch Error: {e}")

            if local_daily_pnl["date"] == today_str and local_daily_pnl["synced"]:
                return local_daily_pnl["pnl"], True

            return 0.0, False

    def is_daily_loss_breached(self) -> bool:
        pnl, ok = self.get_today_realized_pnl()
        if not ok:
            logger.warning("Daily PnL could not be verified (fail-closed). Blocking new entries.")
            reject("daily_pnl_unverified_block")
            return True
        if pnl <= -MAX_DAILY_LOSS_USDT:
            reject("daily_loss_limit_reached")
            return True
        return False

db = FirebaseDatabase()

# ==========================================
# 3. TELEGRAM ALERTS
# ==========================================
def notify(msg: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=8)
            if r.status_code == 200 and r.json().get("ok"):
                return True
        except Exception:
            pass
        time.sleep(1.5 * (attempt + 1))
    return False

# ==========================================
# 4. API ENGINE & RATE LIMITER
# ==========================================
class APIGuard:
    def __init__(self):
        self.lock = threading.Lock()
        self.consecutive_errors = 0
        self.circuit_tripped_until = 0
        self.last_call_ts = 0.0

    def throttle(self, min_interval=0.18):
        with self.lock:
            now = time.time()
            wait = min_interval - (now - self.last_call_ts)
            if wait > 0:
                time.sleep(wait)
            self.last_call_ts = time.time()

    def record_success(self):
        with self.lock:
            self.consecutive_errors = 0

    def record_error(self, err_detail: str):
        with self.lock:
            self.consecutive_errors += 1
            if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS and time.time() > self.circuit_tripped_until:
                self.circuit_tripped_until = time.time() + 900
                notify(f"🚨 *CIRCUIT BREAKER TRIPPED!*\nLast error: `{err_detail[:120]}`\nPaused for 15 mins.")

    def is_circuit_open(self) -> bool:
        return time.time() < self.circuit_tripped_until

api_guard = APIGuard()

def coinswitch_request(method: str, endpoint: str, payload: dict = None, track_errors: bool = True) -> dict:
    api_guard.throttle(0.25)
    url = f"{BASE_URL}{endpoint}"
    epoch = str(int(time.time() * 1000))
    msg = f"{method}{endpoint}{epoch}".encode("utf-8")
    priv_key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(SECRET_KEY))
    signature = priv_key.sign(msg).hex()

    headers = {
        "X-AUTH-APIKEY": API_KEY,
        "X-AUTH-SIGNATURE": signature,
        "X-AUTH-EPOCH": epoch,
        "Content-Type": "application/json",
        "User-Agent": "CoinSwitchPro-Enterprise-Bot",
    }
    try:
        if method == "GET":
            res = requests.get(url, headers=headers, timeout=10)
        elif method == "DELETE":
            res = requests.delete(url, headers=headers, json=payload, timeout=10)
        else:
            res = requests.post(url, headers=headers, json=payload, timeout=10)

        if res.status_code in (200, 201):
            api_guard.record_success()
            return {"ok": True, "status": res.status_code, "body": res.json()}
        else:
            err_txt = res.text[:250]
            if track_errors:
                api_guard.record_error(f"HTTP {res.status_code}: {err_txt}")
            return {"ok": False, "status": res.status_code, "error": err_txt}
    except Exception as e:
        if track_errors:
            api_guard.record_error(str(e))
        return {"ok": False, "status": 0, "error": str(e)}

def load_exchange_precision():
    resp = coinswitch_request("GET", f"/trade/api/v2/futures/instrument_info?exchange={EXCHANGE_NAME}", track_errors=False)
    if not resp.get("ok"):
        return
    data = resp["body"].get("data", resp["body"])
    items = data.values() if isinstance(data, dict) else (data if isinstance(data, list) else [])
    for item in items:
        if not isinstance(item, dict):
            continue
        sym = normalize_symbol(_pick(item, "symbol", "instrument"))
        if not sym:
            continue
        step = _pick(item, "step_size", "quantity_increment", "lot_size", "min_qty")
        tick = _pick(item, "tick_size", "price_increment", "min_price_increment")
        if step or tick:
            cur = INSTRUMENT_SPECS.get(sym, {"step": "0.1", "tick": "0.001"}).copy()
            if step and safe_float(step) > 0:
                cur["step"] = str(step)
            if tick and safe_float(tick) > 0:
                cur["tick"] = str(tick)
            INSTRUMENT_SPECS[sym] = cur

# ==========================================
# 5. BALANCE, LEVERAGE & ORDER PARSING
# ==========================================
def verify_sufficient_balance() -> bool:
    resp = coinswitch_request("GET", "/trade/api/v2/futures/wallet_balance", track_errors=False)
    if resp.get("ok"):
        body = resp["body"]
        data = body.get("data", body) if isinstance(body, dict) else {}
        for item in data.get("base_asset_balances", []):
            if str(item.get("base_asset", "")).upper() == "USDT":
                usdt_bal = safe_float(item.get("balances", {}).get("total_available_balance"))
                if usdt_bal >= (MAX_MARGIN_PER_TRADE_USDT * 0.8):
                    balance_state["avail"] = usdt_bal
                    return True
    return False

def set_and_verify_leverage(symbol: str, leverage: int) -> bool:
    payload = {"symbol": symbol, "exchange": EXCHANGE_NAME, "leverage": leverage}
    resp = coinswitch_request("POST", "/trade/api/v2/futures/leverage", payload)
    if not resp.get("ok"):
        logger.error(f"Leverage POST failed for {symbol}: {resp.get('error')}")
        return False

    body_data = resp.get("body", {}).get("data", resp.get("body", {}))
    if isinstance(body_data, dict):
        post_lev = safe_float(_pick(body_data, "leverage", "max_leverage"))
        if post_lev > 0 and int(round(post_lev)) != int(leverage):
            logger.error(f"Leverage mismatch in POST response for {symbol}: got {post_lev}, expected {leverage}")
            return False

    get_resp = coinswitch_request(
        "GET",
        f"/trade/api/v2/futures/leverage?exchange={EXCHANGE_NAME}&symbol={symbol}",
        track_errors=False
    )
    if get_resp.get("ok"):
        g_data = get_resp.get("body", {}).get("data", get_resp.get("body", {}))
        if isinstance(g_data, list) and g_data:
            g_data = g_data[0]
        if isinstance(g_data, dict):
            actual_lev = safe_float(_pick(g_data, "leverage", "user_leverage", "current_leverage"))
            if actual_lev > 0 and int(round(actual_lev)) != int(leverage):
                logger.error(f"Leverage GET readback mismatch for {symbol}: {actual_lev} != {leverage}")
                return False

    return True

def extract_order_details(resp_body: dict) -> dict:
    if not isinstance(resp_body, dict):
        return {"status": "", "avg_price": 0.0, "exec_qty": 0.0, "order_id": ""}
    data = resp_body.get("data", resp_body)
    order_obj = data.get("order", data) if isinstance(data, dict) else {}
    if not isinstance(order_obj, dict):
        order_obj = {}

    status = str(_pick(order_obj, "status", "order_status", "state") or "").upper().strip()
    avg_price = safe_float(_pick(order_obj, "avg_execution_price", "average_price", "avg_price", "fill_price", "price"))
    exec_qty = safe_float(_pick(order_obj, "executed_quantity", "executed_qty", "filled_quantity", "filled_qty", "cum_qty"))
    order_id = str(_pick(order_obj, "order_id", "id", "client_order_id") or "")
    return {
        "status": status,
        "avg_price": avg_price,
        "exec_qty": exec_qty,
        "order_id": order_id
    }

# ==========================================
# 6. INDICATORS & SIGNAL (MACD, CMF, ATR)
# ==========================================
KLINE_INTERVAL_MIN = "15"
CANDLE_MS = 15 * 60 * 1000
_kline_throttle_lock = threading.Lock()
_kline_last_call = [0.0]

def fetch_klines(symbol: str, limit=110) -> list:
    with _kline_throttle_lock:
        wait = 2.2 - (time.time() - _kline_last_call[0])
        if wait > 0:
            time.sleep(wait)
        _kline_last_call[0] = time.time()

    end_ms = int(time.time() * 1000) + CANDLE_MS
    start_ms = end_ms - (limit + 2) * CANDLE_MS
    endpoint = f"/trade/api/v2/futures/klines?symbol={symbol}&exchange={EXCHANGE_NAME}&interval={KLINE_INTERVAL_MIN}&start_time={start_ms}&end_time={end_ms}"
    resp = coinswitch_request("GET", endpoint, track_errors=False)

    if not resp.get("ok"):
        return []
    data = resp["body"].get("data", resp["body"])
    out = []
    for r in data if isinstance(data, list) else []:
        try:
            ts = int(float(_pick(r, "start_time", "timestamp", "t")))
            if ts < 10**11:
                ts *= 1000
            out.append({
                "ts": ts,
                "open": float(_pick(r, "o", "open")),
                "high": float(_pick(r, "h", "high")),
                "low": float(_pick(r, "l", "low")),
                "close": float(_pick(r, "c", "close")),
                "volume": float(_pick(r, "v", "volume"))
            })
        except Exception:
            continue
    out.sort(key=lambda x: x["ts"])
    return out

def get_all_prices() -> dict:
    resp = coinswitch_request("GET", f"/trade/api/v2/futures/all-pairs/ticker?exchange={EXCHANGE_NAME}", track_errors=False)
    prices = {}
    if not resp.get("ok"):
        return prices
    data = resp["body"].get("data", resp["body"])
    items = data.values() if isinstance(data, dict) else (data if isinstance(data, list) else [])
    for v in items:
        if not isinstance(v, dict):
            continue
        sym = v.get("symbol")
        px = safe_float(_pick(v, "last_price", "mark_price", "c"))
        if sym and px > 0:
            prices[normalize_symbol(sym)] = px
    return prices

def calculate_ema(prices: list, period: int):
    if not prices or len(prices) < period:
        return None
    sma = sum(prices[:period]) / period
    multiplier = 2.0 / (period + 1.0)
    ema = sma
    for p in prices[period:]:
        ema = (p - ema) * multiplier + ema
    return ema

def calculate_rsi(closes: list, period=14):
    if not closes or len(closes) <= period:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

def calculate_atr(candles: list, period=14):
    if not candles or len(candles) <= period:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr if atr > 0 else None

def calculate_macd_current(closes: list):
    if not closes or len(closes) < 35:
        return None, None
    def get_ema_series(data, p):
        res = [sum(data[:p]) / p]
        mult = 2.0 / (p + 1.0)
        for val in data[p:]:
            res.append((val - res[-1]) * mult + res[-1])
        return res
    ema12, ema26 = get_ema_series(closes, 12), get_ema_series(closes, 26)
    macd_series = [ema12[i + (len(ema12) - len(ema26))] - ema26[i] for i in range(len(ema26))]
    signal_series = get_ema_series(macd_series, 9)
    return macd_series[-1], signal_series[-1]

def calculate_cmf(candles: list, period=20):
    if not candles or len(candles) < period:
        return None
    mfv_sum, vol_sum = 0.0, 0.0
    for c in candles[-period:]:
        h, l, close_px, v = c["high"], c["low"], c["close"], c["volume"]
        if h == l:
            continue
        mfm = ((close_px - l) - (h - close_px)) / (h - l)
        mfv_sum += mfm * v
        vol_sum += v
    return mfv_sum / vol_sum if vol_sum > 0 else 0.0

def verify_directional_edge(candles: list, direction: str) -> bool:
    wins, losses = 0, 0
    for idx in range(45, len(candles) - 5):
        sub = candles[:idx]
        c_price = sub[-1]["close"]
        prev_high = max(x["high"] for x in sub[-(BREAKOUT_LOOKBACK + 1):-1])
        prev_low = min(x["low"] for x in sub[-(BREAKOUT_LOOKBACK + 1):-1])
        future_high = max(x["high"] for x in candles[idx:idx + 4])
        future_low = min(x["low"] for x in candles[idx:idx + 4])

        if direction == "LONG" and c_price > prev_high:
            if (future_high - c_price) >= (c_price - future_low):
                wins += 1
            else:
                losses += 1
        elif direction == "SHORT" and c_price < prev_low:
            if (c_price - future_low) >= (future_high - c_price):
                wins += 1
            else:
                losses += 1
    if (wins + losses) == 0:
        return False
    return (wins / (wins + losses)) >= EDGE_MIN_WINRATE

# ==========================================
# 7. AI ULTRA-SAFE CONFIRMATION LOGIC
# ==========================================
def analyze_closed_candle(symbol: str):
    candles = fetch_klines(symbol, 110)
    now_ms = int(time.time() * 1000)
    closed_candles = [c for c in candles if c["ts"] + CANDLE_MS <= now_ms]
    if len(closed_candles) < 72:
        return None

    c1_breakout = closed_candles[-2]
    c2_confirm = closed_candles[-1]

    age_sec = (now_ms - (c2_confirm["ts"] + CANDLE_MS)) / 1000.0
    if age_sec > MAX_SIGNAL_AGE_SEC:
        return None

    prev_closed = closed_candles[-(BREAKOUT_LOOKBACK + 2):-2]
    closes = [c["close"] for c in closed_candles]
    volumes = [c["volume"] for c in closed_candles]

    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(closed_candles, 14)
    macd_val, macd_sig = calculate_macd_current(closes)
    cmf = calculate_cmf(closed_candles, 20)

    if None in (ema20, ema50, rsi, atr, macd_val, cmf) or atr <= 0:
        return None

    support = min(c["low"] for c in prev_closed)
    resistance = max(c["high"] for c in prev_closed)
    avg_vol = sum(volumes[-22:-2]) / 20.0

    breakout_buffer = DYNAMIC_SETTINGS["atr_buffer"] * atr

    if c1_breakout["volume"] < (VOL_MULT * avg_vol):
        reject("c1_low_volume")
        return None

    direction = None
    if c1_breakout["close"] > c1_breakout["open"] and c1_breakout["close"] > resistance + breakout_buffer:
        direction = "LONG"
    elif c1_breakout["close"] < c1_breakout["open"] and c1_breakout["close"] < support - breakout_buffer:
        direction = "SHORT"

    if not direction:
        reject("no_breakout_c1")
        return None

    c2_body = abs(c2_confirm["close"] - c2_confirm["open"])

    if direction == "LONG":
        if not (c2_confirm["close"] > c1_breakout["close"]):
            reject("c2_no_follow_through")
            return None
        if not (c2_confirm["close"] > c2_confirm["open"]):
            reject("c2_not_green")
            return None
        upper_wick = c2_confirm["high"] - c2_confirm["close"]
        if upper_wick > c2_body * 1.2:
            reject("c2_bearish_wick_trap")
            return None

        if REQUIRE_EMA_TREND and not (ema20 > ema50):
            reject("long_ema_trend")
            return None
        if not (RSI_LONG_MIN <= rsi <= RSI_LONG_MAX):
            reject("long_rsi")
            return None
        if not (macd_val > macd_sig):
            reject("long_macd_momentum_weak")
            return None
        if cmf < 0.10:
            reject("cmf_long_weak")
            return None

    else:
        if not (c2_confirm["close"] < c1_breakout["close"]):
            reject("c2_no_follow_through")
            return None
        if not (c2_confirm["close"] < c2_confirm["open"]):
            reject("c2_not_red")
            return None
        lower_wick = c2_confirm["close"] - c2_confirm["low"]
        if lower_wick > c2_body * 1.2:
            reject("c2_bullish_wick_trap")
            return None

        if REQUIRE_EMA_TREND and not (ema20 < ema50):
            reject("short_ema_trend")
            return None
        if not (RSI_SHORT_MIN <= rsi <= RSI_SHORT_MAX):
            reject("short_rsi")
            return None
        if not (macd_val < macd_sig):
            reject("short_macd_momentum_weak")
            return None
        if cmf > -0.10:
            reject("cmf_short_weak")
            return None

    if not verify_directional_edge(closed_candles, direction):
        reject("edge_fail")
        return None

    reject("SIGNAL_FOUND")

    vol_ratio = c2_confirm["volume"] / avg_vol if avg_vol > 0 else 1.0
    logical_sl = c1_breakout["low"] if direction == "LONG" else c1_breakout["high"]

    # FIX: Use 70-candle macro highs/lows for Smart TP wall detection so broken 12-candle levels don't choke TP
    major_support = min(c["low"] for c in closed_candles[:-2])
    major_resistance = max(c["high"] for c in closed_candles[:-2])

    return {
        "symbol": symbol, "signal": direction, "signal_close": c2_confirm["close"],
        "atr": atr, "support": major_support, "resistance": major_resistance,
        "logical_sl": logical_sl,
        "rsi": round(rsi, 2), "macd_val": round(macd_val, 5), "cmf": round(cmf, 3),
        "candle_ts": c2_confirm["ts"], "age_sec": int(age_sec), "vol_ratio": round(vol_ratio, 2)
    }

# ==========================================
# 8. RISK-BASED SIZING
# ==========================================
def calculate_risk_based_quantity(symbol: str, entry_price: float, sl_distance: float, vol_ratio: float) -> float:
    if entry_price <= 0 or sl_distance <= 0:
        return 0.0
    if vol_ratio >= 2.0:
        dynamic_risk_usdt = 1.50
    elif vol_ratio >= 1.5:
        dynamic_risk_usdt = 0.75
    else:
        dynamic_risk_usdt = 0.40

    margin_cap = min(MAX_MARGIN_PER_TRADE_USDT, balance_state["avail"] * 0.8)
    risk_qty = dynamic_risk_usdt / sl_distance
    max_notional_qty = (margin_cap * LEVERAGE) / entry_price
    raw_qty = min(risk_qty, max_notional_qty)

    return format_quantity(raw_qty, symbol, entry_price)

# ==========================================
# 9. ENTERPRISE POSITION MANAGER
# ==========================================
class EnterprisePositionManager:
    def __init__(self):
        self.execution_lock = threading.Lock()
        self.active_trades = db.load_all_positions()
        self.processed_candles = {}
        self.missing_counts = Counter()

    def _safe_cancel(self, symbol, order_id):
        if order_id:
            coinswitch_request(
                "DELETE",
                "/trade/api/v2/futures/order",
                {"symbol": symbol, "exchange": EXCHANGE_NAME, "order_id": order_id},
                track_errors=False
            )

    def _get_order_info(self, order_id: str) -> dict:
        if not order_id:
            return {"status": "", "avg_price": 0.0, "exec_qty": 0.0, "order_id": ""}
        st = coinswitch_request("GET", f"/trade/api/v2/futures/order?order_id={order_id}", track_errors=False)
        if st.get("ok"):
            return extract_order_details(st["body"])
        return {"status": "", "avg_price": 0.0, "exec_qty": 0.0, "order_id": ""}

    def _place_trigger_order(self, symbol: str, close_side: str, trigger_px: float, order_type: str, quantity: float) -> str:
        payload_qty = {
            "exchange": EXCHANGE_NAME,
            "symbol": symbol,
            "side": close_side,
            "type": order_type,
            "quantity": quantity,
            "trigger_price": trigger_px,
            "reduce_only": True
        }
        r = coinswitch_request("POST", "/trade/api/v2/futures/order", payload_qty)
        if r.get("ok"):
            det = extract_order_details(r["body"])
            if det["order_id"]:
                return det["order_id"]

        logger.warning(f"{order_type} with qty={quantity} rejected for {symbol} ({r.get('error')}). Retrying with qty=0.")
        payload_zero = dict(payload_qty, quantity=0)
        r2 = coinswitch_request("POST", "/trade/api/v2/futures/order", payload_zero)
        if r2.get("ok"):
            det2 = extract_order_details(r2["body"])
            if det2["order_id"]:
                return det2["order_id"]

        logger.error(f"Trigger placement {order_type} failed completely for {symbol}: {r2.get('error')}")
        return ""

    def reconcile_with_exchange(self):
        snapshot_time = time.time()

        pos_resp = coinswitch_request("GET", f"/trade/api/v2/futures/positions?exchange={EXCHANGE_NAME}", track_errors=False)
        if not pos_resp.get("ok"):
            logger.warning(f"Reconcile skipped: could not fetch positions ({pos_resp.get('error')})")
            return

        body = pos_resp.get("body", {})
        data = body.get("data", body) if isinstance(body, dict) else body

        if data is None or data == {}:
            pos_list = []
        elif isinstance(data, list):
            pos_list = data
        elif isinstance(data, dict):
            if "positions" in data and isinstance(data["positions"], list):
                pos_list = data["positions"]
            elif "symbol" in data:
                pos_list = [data]
            else:
                pos_list = [v for v in data.values() if isinstance(v, dict)]
        else:
            logger.warning(f"Reconcile skipped: unexpected positions payload format ({type(data).__name__})")
            return

        exchange_positions = {}
        for pos in pos_list:
            if isinstance(pos, dict) and str(pos.get("status", "OPEN")).upper() == "OPEN":
                sym = normalize_symbol(pos.get("symbol"))
                sz = abs(safe_float(_pick(pos, "position_size", "size", "quantity")))
                if sym and sz > 0:
                    exchange_positions[sym] = sz

        with self.execution_lock:
            local_items = list(self.active_trades.items())
            confirmed_closed = []

            for local_key, trade in local_items:
                norm_key = normalize_symbol(local_key)
                if snapshot_time - safe_float(trade.get("opened_at"), 0.0) < RECONCILE_GRACE_SECONDS:
                    exchange_positions.pop(norm_key, None)
                    continue

                if norm_key not in exchange_positions:
                    single_resp = coinswitch_request(
                        "GET",
                        f"/trade/api/v2/futures/positions?exchange={EXCHANGE_NAME}&symbol={local_key}",
                        track_errors=False
                    )
                    if not single_resp.get("ok"):
                        continue
                    s_body = single_resp.get("body", {})
                    s_raw = s_body.get("data", s_body) if isinstance(s_body, dict) else s_body
                    s_list = s_raw if isinstance(s_raw, list) else ([s_raw] if isinstance(s_raw, dict) and "symbol" in s_raw else [])
                    s_size = 0.0
                    for p in s_list:
                        if isinstance(p, dict) and str(p.get("status", "OPEN")).upper() == "OPEN":
                            s_size += abs(safe_float(_pick(p, "position_size", "size", "quantity")))

                    if s_size > 0:
                        self.missing_counts[local_key] = 0
                        continue

                    self.missing_counts[local_key] += 1
                    if self.missing_counts[local_key] >= MISSING_CONFIRMATIONS:
                        closed = self.active_trades.pop(local_key, None)
                        self.missing_counts.pop(local_key, None)
                        if closed:
                            confirmed_closed.append((local_key, closed))
                else:
                    self.missing_counts[local_key] = 0
                    exchange_positions.pop(norm_key, None)

            for sym, trade in confirmed_closed:
                sl_id = trade.get("sl_order_id", "")
                tp_id = trade.get("tp_order_id", "")
                sl_info = self._get_order_info(sl_id)
                tp_info = self._get_order_info(tp_id)

                self._safe_cancel(sym, sl_id)
                self._safe_cancel(sym, tp_id)

                FILLED_STATES = {"FILLED", "EXECUTED", "COMPLETED", "CLOSED"}
                if sl_info["status"] in FILLED_STATES or sl_info["avg_price"] > 0:
                    reason = "🛑 SL Hit (Exchange)"
                    exit_price = sl_info["avg_price"] if sl_info["avg_price"] > 0 else safe_float(trade.get("stop_loss"))
                elif tp_info["status"] in FILLED_STATES or tp_info["avg_price"] > 0:
                    reason = "🎯 TP Hit (Exchange)"
                    exit_price = tp_info["avg_price"] if tp_info["avg_price"] > 0 else safe_float(trade.get("take_profit"))
                else:
                    prices = get_all_prices()
                    curr_px = prices.get(normalize_symbol(sym), safe_float(trade.get("stop_loss")))
                    is_sl = (
                        (trade["side"] == "LONG" and curr_px <= safe_float(trade["stop_loss"]))
                        or (trade["side"] == "SHORT" and curr_px >= safe_float(trade["stop_loss"]))
                    )
                    reason = "🛑 SL Hit (Exchange)" if is_sl else "🎯 TP Hit (Exchange)"
                    exit_price = safe_float(trade["stop_loss"]) if is_sl else safe_float(trade["take_profit"])

                entry, qty = safe_float(trade.get("entry_price", 0)), safe_float(trade.get("quantity", 0))
                gross_pnl = (exit_price - entry) * qty if trade["side"] == "LONG" else (entry - exit_price) * qty
                net_pnl = round(gross_pnl - ((entry + exit_price) * qty * (ROUND_TRIP_FEE_PCT / 2.0)), 3)

                db.delete_position(sym)
                db.record_closed_trade(sym, trade.get("side", "LONG"), entry, exit_price, qty, net_pnl, reason)
                notify(
                    f"🔔 *Position Closed: {reason}*\nCoin: `{sym}`\n"
                    f"Entry: `{entry}` | Exit: `{exit_price}`\n"
                    f"💵 Net PnL: `{net_pnl:+.2f} USDT`\n"
                    f"📅 Today: `{round(db.get_today_realized_pnl()[0], 2):+.2f} USDT`"
                )

            for orphan_sym, orphan_sz in exchange_positions.items():
                logger.warning(f"Orphan Position found on Exchange: {orphan_sym} size {orphan_sz}")
                if int(snapshot_time) % 300 < 25:
                    notify(
                        f"⚠️ *ORPHAN POSITION DETECTED*\nExchange has `{orphan_sym}` open "
                        f"(Size: `{orphan_sz}`), not tracked in local state. Check CoinSwitch!"
                    )

    def execute_entry(self, signal_data: dict):
        symbol = signal_data["symbol"]
        side_signal = signal_data["signal"]
        candle_ts = signal_data["candle_ts"]
        vol_ratio = signal_data["vol_ratio"]

        if api_guard.is_circuit_open() or db.is_daily_loss_breached():
            return

        with self.execution_lock:
            if symbol in self.active_trades or len(self.active_trades) >= MAX_CONCURRENT_TRADES:
                return
            if sum(1 for t in self.active_trades.values() if t["side"] == side_signal) >= MAX_SAME_DIRECTION_TRADES:
                return
            if self.processed_candles.get(symbol) == candle_ts:
                return

            ref_price = signal_data["signal_close"]
            atr = signal_data["atr"]
            logical_sl = signal_data["logical_sl"]

            dynamic_cushion = atr * DYNAMIC_SETTINGS["atr_buffer"]
            if side_signal == "LONG":
                base_sl = min(logical_sl - dynamic_cushion, ref_price - (0.5 * atr))
            else:
                base_sl = max(logical_sl + dynamic_cushion, ref_price + (0.5 * atr))

            sl_distance = abs(ref_price - base_sl)
            if sl_distance <= 0:
                sl_distance = 1.0 * atr

            est_stop_loss = format_price(base_sl, symbol)
            fee_buffer_est = ref_price * ROUND_TRIP_FEE_PCT
            tp_multiplier = 1.5
            support_level = signal_data.get("support", ref_price)
            resistance_level = signal_data.get("resistance", ref_price)

            # FIX: Clean Smart TP calculation without halving reward
            if side_signal == "LONG":
                math_tp = ref_price + (tp_multiplier * (ref_price - est_stop_loss)) + fee_buffer_est
                if resistance_level > (ref_price + (atr * 0.3)) and math_tp > resistance_level:
                    final_tp = resistance_level - (atr * 0.10)
                else:
                    final_tp = math_tp
            else:
                math_tp = ref_price - (tp_multiplier * (est_stop_loss - ref_price)) - fee_buffer_est
                if support_level < (ref_price - (atr * 0.3)) and math_tp < support_level:
                    final_tp = support_level + (atr * 0.10)
                else:
                    final_tp = math_tp

            est_take_profit = format_price(final_tp, symbol)
            risk_amount = abs(ref_price - est_stop_loss)
            reward_amount = abs(est_take_profit - ref_price)

            # 1. STRICT R:R FILTER (Allow slight rounding tolerance of 0.95x)
            if reward_amount < (risk_amount * 0.95):
                reject("Bad_RR_Trap_Filter")
                return
            # 2. COMMISSION TRAP FILTER (Min 0.75% Target Move)
            if (reward_amount / ref_price) < 0.0075:
                reject("Target_Too_Small_Fee_Trap")
                return

            if not LIVE_TRADING_ENABLED:
                self.processed_candles[symbol] = candle_ts
                notify(
                    f"🧪 *DRY RUN SIGNAL* | `{symbol}` ({side_signal})\n"
                    f"Ref Price: `{ref_price}` | SL: `{est_stop_loss}` | TP: `{est_take_profit}`\n"
                    f"Vol Spike: `{vol_ratio}x`"
                )
                return

            if not verify_sufficient_balance():
                reject("insufficient_balance")
                return

            if not set_and_verify_leverage(symbol, LEVERAGE):
                reject("leverage_set_failed")
                return

            req_quantity = calculate_risk_based_quantity(symbol, ref_price, sl_distance, vol_ratio)
            if req_quantity <= 0:
                reject("qty_zero_after_precision")
                return

            client_oid = str(uuid.uuid4())
            entry_payload = {
                "symbol": symbol,
                "exchange": EXCHANGE_NAME,
                "side": "BUY" if side_signal == "LONG" else "SELL",
                "type": "MARKET",
                "quantity": req_quantity,
                "client_order_id": client_oid,
            }

            resp = coinswitch_request("POST", "/trade/api/v2/futures/order", entry_payload)
            if not resp.get("ok"):
                reject("order_post_failed")
                return

            init_det = extract_order_details(resp["body"])
            order_id = init_det["order_id"]
            if not order_id:
                reject("missing_order_id")
                return

            actual_fill = 0.0
            filled_qty = 0.0
            FILLED_STATES = {"FILLED", "EXECUTED", "COMPLETED", "CLOSED"}
            PARTIAL_STATES = {"PARTIALLY_FILLED", "PARTIAL_FILLED", "PARTIAL"}

            for _ in range(5):
                time.sleep(1.0)
                info = self._get_order_info(order_id)
                if info["avg_price"] > 0:
                    actual_fill = info["avg_price"]
                if info["exec_qty"] > 0:
                    filled_qty = info["exec_qty"]

                if info["status"] in FILLED_STATES and filled_qty > 0:
                    break
                if info["status"] in PARTIAL_STATES and filled_qty > 0:
                    self._safe_cancel(symbol, order_id)
                    break

            if filled_qty <= 0 or actual_fill <= 0:
                pos_check = coinswitch_request(
                    "GET",
                    f"/trade/api/v2/futures/positions?exchange={EXCHANGE_NAME}&symbol={symbol}",
                    track_errors=False
                )
                if pos_check.get("ok"):
                    p_body = pos_check.get("body", {})
                    p_raw = p_body.get("data", p_body) if isinstance(p_body, dict) else p_body
                    p_list = p_raw if isinstance(p_raw, list) else ([p_raw] if isinstance(p_raw, dict) and "symbol" in p_raw else [])
                    for p in p_list:
                        if isinstance(p, dict) and str(p.get("status", "OPEN")).upper() == "OPEN":
                            p_sz = abs(safe_float(_pick(p, "position_size", "size", "quantity")))
                            p_ep = safe_float(_pick(p, "entry_price", "avg_entry_price", "average_price"))
                            if p_sz > 0 and p_ep > 0:
                                filled_qty = p_sz
                                actual_fill = p_ep
                                break

            if filled_qty <= 0 or actual_fill <= 0:
                self._safe_cancel(symbol, order_id)
                reject("entry_order_not_filled_properly")
                return

            quantity = format_quantity(filled_qty, symbol, actual_fill)
            if quantity <= 0:
                quantity = filled_qty

            fee_buffer = actual_fill * ROUND_TRIP_FEE_PCT
            stop_loss = format_price(base_sl, symbol)

            if side_signal == "LONG":
                math_tp = actual_fill + (tp_multiplier * (actual_fill - stop_loss)) + fee_buffer
                if resistance_level > (actual_fill + (atr * 0.3)) and math_tp > resistance_level:
                    final_tp = resistance_level - (atr * 0.10)
                else:
                    final_tp = math_tp
                take_profit = format_price(final_tp, symbol)
            else:
                math_tp = actual_fill - (tp_multiplier * (stop_loss - actual_fill)) - fee_buffer
                if support_level < (actual_fill - (atr * 0.3)) and math_tp < support_level:
                    final_tp = support_level + (atr * 0.10)
                else:
                    final_tp = math_tp
                take_profit = format_price(final_tp, symbol)

            close_dir = "SELL" if side_signal == "LONG" else "BUY"
            sl_id = self._place_trigger_order(symbol, close_dir, stop_loss, "STOP_MARKET", quantity)
            tp_id = self._place_trigger_order(symbol, close_dir, take_profit, "TAKE_PROFIT_MARKET", quantity)

            if not sl_id or not tp_id:
                self._safe_cancel(symbol, sl_id)
                self._safe_cancel(symbol, tp_id)
                self._close_position_locked(symbol, "Emergency Close: Protection Order Failed", actual_fill, quantity, side_signal)
                reject("sl_tp_placement_failed")
                return

            pos_record = {
                "symbol": symbol, "side": side_signal, "entry_price": actual_fill, "quantity": quantity,
                "stop_loss": stop_loss, "take_profit": take_profit, "highest_price": actual_fill, "lowest_price": actual_fill,
                "atr": atr, "sl_order_id": sl_id, "tp_order_id": tp_id, "opened_at": int(time.time()),
            }

            self.active_trades[symbol] = pos_record
            self.processed_candles[symbol] = candle_ts
            if not db.save_position(pos_record):
                notify(f"⚠️ Warning: `{symbol}` opened with SL/TP but Firebase save failed. Tracked in memory.")

        emoji = "🟢 *LONG*" if side_signal == "LONG" else "🔴 *SHORT*"
        notify(
            f"{emoji} | Coin: `{symbol}`\nFill: `{actual_fill}` | Qty: `{quantity}`\n"
            f"🛡️ Smart Stop Loss: `{stop_loss}`\n🎯 TP (Smart Anti-Dump): `{take_profit}`\n"
            f"✅ Confirmation Candlestick Validated\n"
            f"📊 AI Volume Spike: `{vol_ratio}x`"
        )

    def monitor_and_trail_positions(self):
        with self.execution_lock:
            symbols = list(self.active_trades.keys())
        prices = get_all_prices()
        for sym in symbols:
            px = prices.get(normalize_symbol(sym))
            if px:
                self._update_single_position(sym, px)

    def _update_single_position(self, symbol: str, current_price: float):
        with self.execution_lock:
            trade = self.active_trades.get(symbol)
            if not trade:
                return

            side, atr, entry, tp, qty = (
                trade["side"], trade["atr"], trade["entry_price"], trade["take_profit"], trade["quantity"]
            )

            trail_threshold = entry + ((tp - entry) * 0.75) if side == "LONG" else entry - ((entry - tp) * 0.75)
            close_side = "SELL" if side == "LONG" else "BUY"

            if side == "LONG":
                trade["highest_price"] = max(trade["highest_price"], current_price)
                if current_price >= trail_threshold:
                    candidate_sl = format_price(entry + (0.5 * atr), symbol)
                    if (candidate_sl - trade["stop_loss"]) > 0:
                        new_id = self._place_trigger_order(symbol, close_side, candidate_sl, "STOP_MARKET", qty)
                        if new_id:
                            self._safe_cancel(symbol, trade.get("sl_order_id", ""))
                            trade["stop_loss"], trade["sl_order_id"] = candidate_sl, new_id
                            db.save_position(trade)

                if current_price >= trade["take_profit"]:
                    self._close_position_locked(symbol, "🎯 TP Hit (Trailing)!", current_price, qty, side)
                elif current_price <= trade["stop_loss"]:
                    self._close_position_locked(symbol, "🛑 SL Hit (Trailing)!", current_price, qty, side)

            elif side == "SHORT":
                trade["lowest_price"] = min(trade["lowest_price"], current_price)
                if current_price <= trail_threshold:
                    candidate_sl = format_price(entry - (0.5 * atr), symbol)
                    if (trade["stop_loss"] - candidate_sl) > 0:
                        new_id = self._place_trigger_order(symbol, close_side, candidate_sl, "STOP_MARKET", qty)
                        if new_id:
                            self._safe_cancel(symbol, trade.get("sl_order_id", ""))
                            trade["stop_loss"], trade["sl_order_id"] = candidate_sl, new_id
                            db.save_position(trade)

                if current_price <= trade["take_profit"]:
                    self._close_position_locked(symbol, "🎯 TP Hit (Trailing)!", current_price, qty, side)
                elif current_price >= trade["stop_loss"]:
                    self._close_position_locked(symbol, "🛑 SL Hit (Trailing)!", current_price, qty, side)

    def _close_position_locked(self, symbol: str, reason: str, exit_price: float, quantity: float, side: str):
        trade = self.active_trades.pop(symbol, None)
        entry = trade["entry_price"] if trade else exit_price
        qty = trade["quantity"] if trade else quantity
        t_side = trade["side"] if trade else side
        close_side = "SELL" if t_side == "LONG" else "BUY"

        if trade:
            self._safe_cancel(symbol, trade.get("sl_order_id", ""))
            self._safe_cancel(symbol, trade.get("tp_order_id", ""))

        resp = coinswitch_request("POST", "/trade/api/v2/futures/order", {
            "symbol": symbol,
            "exchange": EXCHANGE_NAME,
            "side": close_side,
            "type": "MARKET",
            "quantity": qty,
            "reduce_only": True,
        })

        time.sleep(1.5)

        pos_resp = coinswitch_request(
            "GET",
            f"/trade/api/v2/futures/positions?exchange={EXCHANGE_NAME}&symbol={symbol}",
            track_errors=False
        )
        if not pos_resp.get("ok"):
            logger.error(f"Cannot verify close status for {symbol}; retaining local recovery record.")
            if trade:
                self.active_trades[symbol] = trade
            notify(f"🚨 Cannot verify whether `{symbol}` is closed. Retained in recovery state.")
            return

        p_body = pos_resp.get("body", {})
        p_raw = p_body.get("data", p_body) if isinstance(p_body, dict) else p_body
        p_list = p_raw if isinstance(p_raw, list) else ([p_raw] if isinstance(p_raw, dict) and "symbol" in p_raw else [])
        size = 0.0
        for p in p_list:
            if isinstance(p, dict) and str(p.get("status", "OPEN")).upper() == "OPEN":
                size += abs(safe_float(_pick(p, "position_size", "size", "quantity")))

        if size > 0:
            logger.warning(f"Position {symbol} still open (size={size}) after close attempt.")
            if trade:
                self.active_trades[symbol] = trade
            notify(f"⚠️ `{symbol}` is still open after close request (Size={size}). Retrying next cycle.")
            return

        if resp.get("ok"):
            c_det = extract_order_details(resp["body"])
            if c_det["avg_price"] > 0:
                exit_price = c_det["avg_price"]
            elif c_det["order_id"]:
                info = self._get_order_info(c_det["order_id"])
                if info["avg_price"] > 0:
                    exit_price = info["avg_price"]

        gross_pnl = (exit_price - entry) * qty if t_side == "LONG" else (entry - exit_price) * qty
        net_pnl = round(gross_pnl - ((entry + exit_price) * qty * (ROUND_TRIP_FEE_PCT / 2.0)), 3)

        db.delete_position(symbol)
        db.record_closed_trade(symbol, t_side, entry, exit_price, qty, net_pnl, reason)

        notify(
            f"🔔 *Position Closed: {reason}*\nCoin: `{symbol}`\n"
            f"Entry: `{entry}` | Exit: `{exit_price}`\n"
            f"💵 Net PnL: `{net_pnl:+.2f} USDT`\n"
            f"📅 Today: `{round(db.get_today_realized_pnl()[0], 2):+.2f} USDT`"
        )

manager = EnterprisePositionManager()

# ==========================================
# 10. WORKER THREADS
# ==========================================
def self_learning_loop():
    while True:
        time.sleep(3600)

def position_monitor_loop():
    reconcile_counter = 0
    while True:
        try:
            if manager.active_trades:
                manager.monitor_and_trail_positions()
            reconcile_counter += 1
            if reconcile_counter >= 12:
                manager.reconcile_with_exchange()
                reconcile_counter = 0
        except Exception as e:
            logger.error(f"Position monitor loop error: {e}")
        time.sleep(10)

def parallel_scanner_loop():
    load_exchange_precision()
    db.get_today_realized_pnl()
    manager.reconcile_with_exchange()

    if LIVE_TRADING_ENABLED:
        if verify_sufficient_balance():
            notify("✅ *CoinSwitch Bot Started in LIVE mode (8 USDT Margin | 6x | R:R Wall Bug Fixed).*")
        else:
            notify("⛔ LIVE mode enabled, but wallet balance < 6.4 USDT. Waiting for deposit...")
    else:
        notify("🧪 *Signal-only DRY RUN started.* Set LIVE_TRADING_ENABLED=true for live orders.")

    while True:
        try:
            if api_guard.is_circuit_open():
                time.sleep(30)
                continue

            watchlist = [
                s for s in sorted(SUPPORTED_COINSWITCH_SYMBOLS)
                if bad_symbols.get(s, 0) < 5 and s not in manager.active_trades
            ]
            print(f"🔍 [Scan] Checking {len(watchlist)} coins | Open Trades: {len(manager.active_trades)}", flush=True)

            with ThreadPoolExecutor(max_workers=2) as executor:
                for future in as_completed({executor.submit(analyze_closed_candle, coin): coin for coin in watchlist}):
                    try:
                        setup = future.result()
                        if setup:
                            manager.execute_entry(setup)
                    except Exception:
                        logger.error(f"Signal exception: {traceback.format_exc()}")

            print(f"📊 Scan summary: {pop_reject_stats()}", flush=True)
            time.sleep(90)
        except Exception as e:
            print(f"🚨 Scanner crash: {e}", flush=True)
            time.sleep(15)

if __name__ == "__main__":
    logger.warning(
        "BOT STARTING | mode=%s | leverage=%sx | margin_cap=%s USDT | daily_loss_cap=%s USDT",
        "LIVE" if LIVE_TRADING_ENABLED else "SIGNAL-ONLY DRY RUN",
        LEVERAGE, MAX_MARGIN_PER_TRADE_USDT, MAX_DAILY_LOSS_USDT
    )
    threading.Thread(target=keep_alive_pinger, daemon=True).start()
    threading.Thread(target=self_learning_loop, daemon=True).start()
    threading.Thread(target=position_monitor_loop, daemon=True).start()
    threading.Thread(target=parallel_scanner_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
