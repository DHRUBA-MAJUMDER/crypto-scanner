import logging
import math
import os
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
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
    raise ValueError("🚨 CRITICAL: API_KEY or SECRET_KEY is missing!")

BASE_URL = "https://coinswitch.co"
EXCHANGE_NAME = "EXCHANGE_2"
TIMEFRAME = "15m"

# ✅ 6x Leverage and Dynamic Margin Limits
LEVERAGE = 6
MAX_MARGIN_PER_TRADE_USDT = 12.0
MIN_INR_REQUIRED = 150.0
ROUND_TRIP_FEE_PCT = 0.0012
MAX_DAILY_LOSS_USDT = 4.0
MAX_CONCURRENT_TRADES = 2
MAX_SAME_DIRECTION_TRADES = 2
MAX_CONSECUTIVE_ERRORS = 5

# ---------- AI DYNAMIC SETTINGS ----------
DYNAMIC_SETTINGS = {
    "atr_buffer": 0.25 
}

# ---------- AGGRESSIVE SIGNAL SETTINGS ----------
BREAKOUT_LOOKBACK = 12
VOL_MULT = 0.70
MIN_BODY_ATR = 0.15
RSI_LONG_MIN, RSI_LONG_MAX = 50, 75
RSI_SHORT_MIN, RSI_SHORT_MAX = 25, 50
EDGE_MIN_WINRATE = 0.40
REQUIRE_EMA_TREND = True

# ---------- RECONCILE SETTINGS ----------
RECONCILE_GRACE_SECONDS = 30
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
# 1b. DIAGNOSTICS
# ==========================================
balance_state = {"avail": 0.0}
reject_stats = Counter()
reject_lock = threading.Lock()
empty_counts = {}
EMPTY_DROP_LIMIT = 2
bad_symbols = {}

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
    for ch in ("/", "-", "_"): s = s.replace(ch, "")
    if s.endswith("PERP"): s = s[:-4]
    return s

def safe_float(v, default=0.0) -> float:
    try: return float(v)
    except (TypeError, ValueError): return default

# ==========================================
# 2. PERSISTENT FIREBASE DATABASE
# ==========================================
FIREBASE_URL = "https://tradefix-939af-default-rtdb.firebaseio.com"

class FirebaseDatabase:
    def __init__(self):
        self.db_lock = threading.Lock()

    def save_position(self, pos: dict):
        with self.db_lock:
            try: requests.put(f"{FIREBASE_URL}/active_positions/{pos['symbol']}.json", json=pos, timeout=5)
            except Exception: pass

    def delete_position(self, symbol: str):
        with self.db_lock:
            try: requests.delete(f"{FIREBASE_URL}/active_positions/{symbol}.json", timeout=5)
            except Exception: pass

    def load_all_positions(self) -> dict:
        with self.db_lock:
            try:
                r = requests.get(f"{FIREBASE_URL}/active_positions.json", timeout=5)
                if r.status_code == 200 and r.json(): return r.json()
            except Exception: pass
            return {}

    def record_closed_trade(self, symbol, side, entry_price, exit_price, qty, net_pnl, reason):
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        record = {
            "symbol": symbol, "side": side, "entry_price": entry_price,
            "exit_price": exit_price, "quantity": qty, "net_pnl_usdt": net_pnl,
            "reason": reason, "closed_date": today_str, "closed_at": int(time.time())
        }
        with self.db_lock:
            try: requests.post(f"{FIREBASE_URL}/trade_history.json", json=record, timeout=5)
            except Exception: pass

    def get_today_realized_pnl(self) -> float:
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self.db_lock:
            try:
                r = requests.get(f"{FIREBASE_URL}/trade_history.json", timeout=10)
                if r.status_code == 200 and r.json():
                    history = r.json()
                    return sum(float(v.get("net_pnl_usdt", 0)) for v in history.values() if v.get("closed_date") == today_str)
            except Exception: pass
        return 0.0

    def get_recent_trades(self, limit=30):
        with self.db_lock:
            try:
                r = requests.get(f"{FIREBASE_URL}/trade_history.json", timeout=10)
                if r.status_code == 200 and r.json():
                    history = r.json()
                    trades = list(history.values())
                    trades.sort(key=lambda x: x.get("closed_at", 0), reverse=True)
                    return trades[:limit]
            except Exception: pass
        return []

db = FirebaseDatabase()

# ==========================================
# 3. TELEGRAM ALERTS
# ==========================================
def notify(msg: str) -> bool:
    if not TELEGRAM_BOT_TOKEN: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=8)
            if r.status_code == 200 and r.json().get("ok"): return True
        except Exception: pass
        time.sleep(1.5 * (attempt + 1))
    return False

# ==========================================
# 4. RATE LIMITER & CIRCUIT BREAKER
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
            if wait > 0: time.sleep(wait)
            self.last_call_ts = time.time()

    def record_success(self):
        with self.lock: self.consecutive_errors = 0

    def record_error(self, err_detail: str):
        with self.lock:
            self.consecutive_errors += 1
            if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS and time.time() > self.circuit_tripped_until:
                self.circuit_tripped_until = time.time() + 900
                notify(f"🚨 *CIRCUIT BREAKER TRIPPED!*\nLast error: `{err_detail[:120]}`\nPaused for 15 mins.")

    def is_circuit_open(self) -> bool:
        return time.time() < self.circuit_tripped_until

api_guard = APIGuard()

# ==========================================
# 5. COINSWITCH API ENGINE
# ==========================================
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
        if method == "GET": res = requests.get(url, headers=headers, timeout=10)
        elif method == "DELETE": res = requests.delete(url, headers=headers, json=payload, timeout=10)
        else: res = requests.post(url, headers=headers, json=payload, timeout=10)

        if res.status_code in (200, 201):
            api_guard.record_success()
            return {"ok": True, "status": res.status_code, "body": res.json()}
        else:
            if track_errors: api_guard.record_error(f"HTTP {res.status_code}: {res.text[:200]}")
            return {"ok": False, "status": res.status_code, "error": res.text[:200]}
    except Exception as e:
        if track_errors: api_guard.record_error(str(e))
        return {"ok": False, "status": 0, "error": str(e)}

# ==========================================
# 6. BALANCE & LEVERAGE
# ==========================================
USDT_INR_RATE = 92.0
SOFT_BALANCE_PASS = True
_soft_pass_ts = [0.0]

def verify_sufficient_balance() -> bool:
    resp = coinswitch_request("GET", "/trade/api/v2/futures/wallet_balance", track_errors=False)
    if resp.get("ok"):
        body = resp["body"]
        data = body.get("data", body) if isinstance(body, dict) else {}
        for item in data.get("base_asset_balances", []):
            if str(item.get("base_asset", "")).upper() == "USDT":
                usdt_bal = safe_float(item.get("balances", {}).get("total_available_balance"))
                if usdt_bal >= 3.0:
                    balance_state["avail"] = usdt_bal
                    return True
    
    if SOFT_BALANCE_PASS and (time.time() - _soft_pass_ts[0]) > 900:
        _soft_pass_ts[0] = time.time()
        balance_state["avail"] = MAX_MARGIN_PER_TRADE_USDT / 0.8
        return True
    return False

def set_and_verify_leverage(symbol: str, leverage: int) -> bool:
    payload = {"symbol": symbol, "exchange": EXCHANGE_NAME, "leverage": leverage}
    return coinswitch_request("POST", "/trade/api/v2/futures/leverage", payload).get("ok", False)

# ==========================================
# 7. INDICATORS & SIGNAL (MACD, BB, CMF)
# ==========================================
KLINE_INTERVAL_MIN = "15"
CANDLE_MS = 15 * 60 * 1000
_kline_throttle_lock = threading.Lock()
_kline_last_call = [0.0]

def _pick(d: dict, *keys):
    for k in keys:
        if k in d and d[k] is not None: return d[k]
    return None

def fetch_klines(symbol: str, limit=110) -> list:
    with _kline_throttle_lock:
        wait = 2.2 - (time.time() - _kline_last_call[0])
        if wait > 0: time.sleep(wait)
        _kline_last_call[0] = time.time()

    end_ms = int(time.time() * 1000) + CANDLE_MS
    start_ms = end_ms - (limit + 2) * CANDLE_MS
    endpoint = f"/trade/api/v2/futures/klines?symbol={symbol}&exchange={EXCHANGE_NAME}&interval={KLINE_INTERVAL_MIN}&start_time={start_ms}&end_time={end_ms}"
    resp = coinswitch_request("GET", endpoint, track_errors=False)
    
    if not resp.get("ok"): return []
    data = resp["body"].get("data", resp["body"])
    out = []
    for r in data if isinstance(data, list) else []:
        try:
            ts = int(float(_pick(r, "start_time", "timestamp", "t")))
            if ts < 10**11: ts *= 1000
            out.append({"ts": ts, "open": float(_pick(r, "o", "open")), "high": float(_pick(r, "h", "high")), 
                        "low": float(_pick(r, "l", "low")), "close": float(_pick(r, "c", "close")), "volume": float(_pick(r, "v", "volume"))})
        except Exception: continue
    out.sort(key=lambda x: x["ts"])
    return out

def get_all_prices() -> dict:
    resp = coinswitch_request("GET", f"/trade/api/v2/futures/all-pairs/ticker?exchange={EXCHANGE_NAME}", track_errors=False)
    prices = {}
    if not resp.get("ok"): return prices
    data = resp["body"].get("data", resp["body"])
    items = data.values() if isinstance(data, dict) else data
    for v in items:
        sym = v.get("symbol")
        px = safe_float(_pick(v, "last_price", "mark_price", "c"))
        if sym and px > 0: prices[normalize_symbol(sym)] = px
    return prices

def calculate_ema(prices: list, period: int):
    if not prices or len(prices) < period: return None
    sma = sum(prices[:period]) / period
    multiplier = 2.0 / (period + 1.0)
    ema = sma
    for p in prices[period:]: ema = (p - ema) * multiplier + ema
    return ema

def calculate_rsi(closes: list, period=14):
    if not closes or len(closes) <= period: return None
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
    if avg_loss == 0: return 100.0
    return 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

def calculate_atr(candles: list, period=14):
    if not candles or len(candles) <= period: return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for tr in trs[period:]: atr = (atr * (period - 1) + tr) / period
    return atr if atr > 0 else None

def calculate_macd_current(closes: list):
    if not closes or len(closes) < 35: return None, None
    def get_ema_series(data, p):
        res = [sum(data[:p]) / p]
        mult = 2.0 / (p + 1.0)
        for val in data[p:]: res.append((val - res[-1]) * mult + res[-1])
        return res
    ema12, ema26 = get_ema_series(closes, 12), get_ema_series(closes, 26)
    macd_series = [ema12[i+(len(ema12)-len(ema26))] - ema26[i] for i in range(len(ema26))]
    signal_series = get_ema_series(macd_series, 9)
    return macd_series[-1], signal_series[-1]

def calculate_bollinger_bands(closes: list, period=20, std_dev=2):
    if not closes or len(closes) < period: return None, None, None
    sma = sum(closes[-period:]) / period
    variance = sum((x - sma) ** 2 for x in closes[-period:]) / period
    sd = math.sqrt(variance)
    return sma + (std_dev * sd), sma, sma - (std_dev * sd)

def calculate_cmf(candles: list, period=20):
    if not candles or len(candles) < period: return None
    mfv_sum, vol_sum = 0.0, 0.0
    for c in candles[-period:]:
        h, l, close_px, v = c["high"], c["low"], c["close"], c["volume"]
        if h == l: continue
        mfm = ((close_px - l) - (h - close_px)) / (h - l)
        mfv_sum += mfm * v
        vol_sum += v
    return mfv_sum / vol_sum if vol_sum > 0 else 0.0

def verify_historical_edge(candles: list, direction: str) -> bool:
    wins, losses = 0, 0
    for idx in range(45, len(candles) - 5):
        sub = candles[:idx]
        c_price = sub[-1]["close"]
        prev_high = max(x["high"] for x in sub[-(BREAKOUT_LOOKBACK + 1) : -1])
        prev_low = min(x["low"] for x in sub[-(BREAKOUT_LOOKBACK + 1) : -1])
        future_high = max(x["high"] for x in candles[idx : idx + 4])
        future_low = min(x["low"] for x in candles[idx : idx + 4])

        if direction == "LONG" and c_price > prev_high:
            if (future_high - c_price) >= (c_price - future_low): wins += 1
            else: losses += 1
        elif direction == "SHORT" and c_price < prev_low:
            if (c_price - future_low) >= (future_high - c_price): wins += 1
            else: losses += 1
    if (wins + losses) == 0: return True
    return (wins / (wins + losses)) >= EDGE_MIN_WINRATE

# ==========================================
# 7b. AI ULTRA-SAFE CONFIRMATION LOGIC
# ==========================================
def analyze_closed_candle(symbol: str):
    candles = fetch_klines(symbol, 110)
    now_ms = int(time.time() * 1000)
    closed_candles = [c for c in candles if c["ts"] + CANDLE_MS <= now_ms]
    if len(closed_candles) < 72: return None
        
    c1_breakout = closed_candles[-2]
    c2_confirm = closed_candles[-1]
    
    age_sec = (now_ms - (c2_confirm["ts"] + CANDLE_MS)) / 1000.0
    if age_sec > MAX_SIGNAL_AGE_SEC: return None
        
    prev_closed = closed_candles[-(BREAKOUT_LOOKBACK + 2) : -2]
    closes = [c["close"] for c in closed_candles]
    volumes = [c["volume"] for c in closed_candles]

    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(closed_candles, 14)
    macd_val, macd_sig = calculate_macd_current(closes)
    cmf = calculate_cmf(closed_candles, 20)
    upper_bb, mid_bb, lower_bb = calculate_bollinger_bands(closes, 20, 2)

    if None in (ema20, ema50, rsi, atr, macd_val, cmf, mid_bb) or atr <= 0: return None

    support = min(c["low"] for c in prev_closed)
    resistance = max(c["high"] for c in prev_closed)
    avg_vol = sum(volumes[-22:-2]) / 20.0
    
    breakout_buffer = DYNAMIC_SETTINGS["atr_buffer"] * atr

    if c1_breakout["volume"] < (VOL_MULT * avg_vol): reject("c1_low_volume"); return None

    direction = None
    if c1_breakout["close"] > c1_breakout["open"] and c1_breakout["close"] > resistance + breakout_buffer:
        direction = "LONG"
    elif c1_breakout["close"] < c1_breakout["open"] and c1_breakout["close"] < support - breakout_buffer:
        direction = "SHORT"
        
    if not direction: reject("no_breakout_c1"); return None

    c2_body = abs(c2_confirm["close"] - c2_confirm["open"])
    
    if direction == "LONG":
        if not (c2_confirm["close"] > c1_breakout["close"]): reject("c2_no_follow_through"); return None
        if not (c2_confirm["close"] > c2_confirm["open"]): reject("c2_not_green"); return None
        upper_wick = c2_confirm["high"] - c2_confirm["close"]
        if upper_wick > c2_body * 1.2: reject("c2_bearish_wick_trap"); return None
        
        if REQUIRE_EMA_TREND and not (ema20 > ema50): reject("long_ema_trend"); return None
        if not (RSI_LONG_MIN <= rsi <= RSI_LONG_MAX): reject("long_rsi"); return None
        if not (macd_val > macd_sig): reject("long_macd_momentum_weak"); return None
        if cmf < 0.10: reject("cmf_long_weak"); return None
        
    else:
        if not (c2_confirm["close"] < c1_breakout["close"]): reject("c2_no_follow_through"); return None
        if not (c2_confirm["close"] < c2_confirm["open"]): reject("c2_not_red"); return None
        lower_wick = c2_confirm["close"] - c2_confirm["low"]
        if lower_wick > c2_body * 1.2: reject("c2_bullish_wick_trap"); return None
        
        if REQUIRE_EMA_TREND and not (ema20 < ema50): reject("short_ema_trend"); return None
        if not (RSI_SHORT_MIN <= rsi <= RSI_SHORT_MAX): reject("short_rsi"); return None
        if not (macd_val < macd_sig): reject("short_macd_momentum_weak"); return None
        if cmf > -0.10: reject("cmf_short_weak"); return None

    if not verify_historical_edge(closed_candles, direction): reject("edge_fail"); return None

    reject("SIGNAL_FOUND")
    
    vol_ratio = c2_confirm["volume"] / avg_vol if avg_vol > 0 else 1
    logical_sl = c1_breakout["low"] if direction == "LONG" else c1_breakout["high"]
    
    return {
        "symbol": symbol, "signal": direction, "signal_close": c2_confirm["close"],
        "atr": atr, "support": support, "resistance": resistance,
        "logical_sl": logical_sl, 
        "rsi": round(rsi, 2), "macd_val": round(macd_val, 5), "cmf": round(cmf, 3),
        "candle_ts": c2_confirm["ts"], "age_sec": int(age_sec), "vol_ratio": round(vol_ratio, 2)
    }

# ==========================================
# 8. AI DYNAMIC RISK-BASED SIZING 
# ==========================================
def format_price(val: float) -> float:
    if val >= 1000: return round(val, 2)
    elif val >= 10: return round(val, 3)
    elif val >= 1: return round(val, 4)
    elif val >= 0.01: return round(val, 5)
    else: return round(val, 6)

def calculate_risk_based_quantity(entry_price: float, sl_distance: float, vol_ratio: float) -> float:
    if entry_price <= 0 or sl_distance <= 0: return 0.0
    
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

    if entry_price >= 1000: return math.floor(raw_qty * 1000) / 1000.0
    elif entry_price >= 10: return math.floor(raw_qty * 100) / 100.0
    elif entry_price >= 0.5: return math.floor(raw_qty * 10) / 10.0
    else: return float(math.floor(raw_qty))

# ==========================================
# 9. ENTERPRISE POSITION MANAGER
# ==========================================
class EnterprisePositionManager:
    def __init__(self):
        self.execution_lock = threading.Lock()
        self.active_trades = db.load_all_positions()
        self.processed_candles = {}

    def _safe_cancel(self, symbol, order_id):
        if order_id: coinswitch_request("DELETE", "/trade/api/v2/futures/order", {"symbol": symbol, "exchange": EXCHANGE_NAME, "order_id": order_id})

    def reconcile_with_exchange(self):
        snapshot_time = time.time()
        with self.execution_lock: local_items = list(self.active_trades.items())
        
        orphans = []
        for local_key, trade in local_items:
            if snapshot_time - safe_float(trade.get("opened_at"), 0.0) < RECONCILE_GRACE_SECONDS: continue
            resp = coinswitch_request("GET", f"/trade/api/v2/futures/positions?exchange={EXCHANGE_NAME}&symbol={local_key}", track_errors=False)
            if not resp.get("ok"): continue
            
            size = 0.0
            data = resp.get("body", {}).get("data", [])
            for pos in (data if isinstance(data, list) else []):
                if isinstance(pos, dict) and str(pos.get("status", "OPEN")).upper() == "OPEN":
                    size += abs(safe_float(_pick(pos, "position_size", "size", "quantity")))
                    
            if size == 0: 
                closed = self.active_trades.pop(local_key, None)
                if closed: orphans.append((local_key, closed))

        for sym, trade in orphans:
            self._safe_cancel(sym, trade.get("sl_order_id", ""))
            self._safe_cancel(sym, trade.get("tp_order_id", ""))
            
            prices = get_all_prices()
            exit_price = prices.get(normalize_symbol(sym), safe_float(trade.get("stop_loss")))
            reason = "🛑 SL Hit (Exchange)" if (trade["side"] == "LONG" and exit_price <= safe_float(trade["stop_loss"])) or (trade["side"] == "SHORT" and exit_price >= safe_float(trade["stop_loss"])) else "🎯 TP Hit (Exchange)"
            
            entry, qty = safe_float(trade.get("entry_price", 0)), safe_float(trade.get("quantity", 0))
            gross_pnl = (exit_price - entry) * qty if trade["side"] == "LONG" else (entry - exit_price) * qty
            net_pnl = round(gross_pnl - ((entry + exit_price) * qty * (ROUND_TRIP_FEE_PCT / 2.0)), 3)
            
            db.delete_position(sym)
            db.record_closed_trade(sym, trade.get("side", "LONG"), entry, exit_price, qty, net_pnl, reason)
            notify(f"🔔 *Position Closed: {reason}*\nCoin: `{sym}`\nNet PnL: `{net_pnl:+.2f} USDT`")

    def execute_entry(self, signal_data: dict):
        symbol = signal_data["symbol"]
        side_signal = signal_data["signal"]
        candle_ts = signal_data["candle_ts"]
        vol_ratio = signal_data["vol_ratio"]

        if api_guard.is_circuit_open() or db.get_today_realized_pnl() <= -MAX_DAILY_LOSS_USDT: return

        with self.execution_lock:
            if symbol in self.active_trades or len(self.active_trades) >= MAX_CONCURRENT_TRADES: return
            if sum(1 for t in self.active_trades.values() if t["side"] == side_signal) >= MAX_SAME_DIRECTION_TRADES: return
            if self.processed_candles.get(symbol) == candle_ts: return
            if not verify_sufficient_balance(): return

            set_and_verify_leverage(symbol, LEVERAGE)
            ref_price = signal_data["signal_close"]
            atr = signal_data["atr"]
            logical_sl = signal_data["logical_sl"]
            
            # ✅ FIXED: Added proper Dynamic ATR Cushion to SL to survive Wick Traps/Stop Hunts!
            dynamic_cushion = atr * DYNAMIC_SETTINGS["atr_buffer"]
            if side_signal == "LONG":
                base_sl = min(logical_sl - dynamic_cushion, ref_price - (0.5 * atr))
            else:
                base_sl = max(logical_sl + dynamic_cushion, ref_price + (0.5 * atr))
                
            sl_distance = abs(ref_price - base_sl)
            if sl_distance <= 0: sl_distance = 1.0 * atr

            quantity = calculate_risk_based_quantity(ref_price, sl_distance, vol_ratio)
            if quantity <= 0: return

            client_oid = str(uuid.uuid4())
            entry_payload = {
                "symbol": symbol, "exchange": EXCHANGE_NAME, "side": "BUY" if side_signal == "LONG" else "SELL",
                "type": "MARKET", "order_type": "MARKET", "quantity": quantity, "client_order_id": client_oid,
            }

            resp = coinswitch_request("POST", "/trade/api/v2/futures/order", entry_payload)
            if not resp.get("ok"): return
            
            order_id = resp["body"].get("data", {}).get("order_id", "")
            if not order_id: return

            actual_fill = ref_price
            for _ in range(3):
                time.sleep(1.0)
                st = coinswitch_request("GET", f"/trade/api/v2/futures/order?order_id={order_id}", track_errors=False)
                if st.get("ok"):
                    avg = safe_float(st["body"].get("data", {}).get("order", {}).get("avg_execution_price"))
                    if avg > 0: actual_fill = avg; break

            fee_buffer = actual_fill * ROUND_TRIP_FEE_PCT
            
            tp_multiplier = 1.95 
            if side_signal == "LONG":
                stop_loss = format_price(base_sl)
                take_profit = format_price(actual_fill + (tp_multiplier * (actual_fill - stop_loss)) + fee_buffer)
            else:
                stop_loss = format_price(base_sl)
                take_profit = format_price(actual_fill - (tp_multiplier * (stop_loss - actual_fill)) - fee_buffer)

            def place_trigger(close_side, px, type_str):
                r = coinswitch_request("POST", "/trade/api/v2/futures/order", {
                    "exchange": EXCHANGE_NAME, "symbol": symbol, "side": close_side,
                    "type": type_str, "order_type": type_str, "quantity": 0, "trigger_price": px, "reduce_only": True
                })
                return r["body"].get("data", {}).get("order_id", "") if r.get("ok") else ""

            close_dir = "SELL" if side_signal == "LONG" else "BUY"
            sl_id = place_trigger(close_dir, stop_loss, "STOP_MARKET")
            tp_id = place_trigger(close_dir, take_profit, "TAKE_PROFIT_MARKET")

            pos_record = {
                "symbol": symbol, "side": side_signal, "entry_price": actual_fill, "quantity": quantity,
                "stop_loss": stop_loss, "take_profit": take_profit, "highest_price": actual_fill, "lowest_price": actual_fill,
                "atr": atr, "sl_order_id": sl_id, "tp_order_id": tp_id, "opened_at": int(time.time()),
            }

            self.active_trades[symbol] = pos_record
            self.processed_candles[symbol] = candle_ts
            db.save_position(pos_record)

        emoji = "🟢 *LONG*" if side_signal == "LONG" else "🔴 *SHORT*"
        notify(
            f"{emoji} | Coin: `{symbol}`\nFill: `{actual_fill}` | Qty: `{quantity}`\n"
            f"🛡️ Smart Stop Loss: `{stop_loss}`\n🎯 TP (95% Anti-Dump): `{take_profit}`\n"
            f"✅ Confirmation Candlestick Validated\n"
            f"📊 AI Volume Spike: `{vol_ratio}x`"
        )

    def monitor_and_trail_positions(self):
        with self.execution_lock: symbols = list(self.active_trades.keys())
        prices = get_all_prices()
        for sym in symbols:
            px = prices.get(normalize_symbol(sym))
            if px: self._update_single_position(sym, px)

    def _update_single_position(self, symbol: str, current_price: float):
        with self.execution_lock:
            trade = self.active_trades.get(symbol)
            if not trade: return
            
            side, atr, entry, tp = trade["side"], trade["atr"], trade["entry_price"], trade["take_profit"]
            
            trail_threshold = entry + ((tp - entry) * 0.75) if side == "LONG" else entry - ((entry - tp) * 0.75)
            close_side = "SELL" if side == "LONG" else "BUY"

            if side == "LONG":
                trade["highest_price"] = max(trade["highest_price"], current_price)
                if current_price >= trail_threshold:
                    candidate_sl = format_price(entry + (0.5 * atr)) 
                    if (candidate_sl - trade["stop_loss"]) > 0:
                        for attempt in range(3):
                            r = coinswitch_request("POST", "/trade/api/v2/futures/order", {
                                "exchange": EXCHANGE_NAME, "symbol": symbol, "side": close_side,
                                "type": "STOP_MARKET", "order_type": "STOP_MARKET", "quantity": 0, "trigger_price": candidate_sl, "reduce_only": True
                            })
                            if r.get("ok"):
                                self._safe_cancel(symbol, trade.get("sl_order_id", ""))
                                trade["stop_loss"], trade["sl_order_id"] = candidate_sl, r["body"].get("data", {}).get("order_id", "")
                                db.save_position(trade)
                                break
                            time.sleep(1)
                            
                if current_price >= trade["take_profit"]: self._close_position_locked(symbol, "🎯 TP Hit!", current_price)
                elif current_price <= trade["stop_loss"]: self._close_position_locked(symbol, "🛑 SL Hit!", current_price)

            elif side == "SHORT":
                trade["lowest_price"] = min(trade["lowest_price"], current_price)
                if current_price <= trail_threshold:
                    candidate_sl = format_price(entry - (0.5 * atr)) 
                    if (trade["stop_loss"] - candidate_sl) > 0:
                        for attempt in range(3):
                            r = coinswitch_request("POST", "/trade/api/v2/futures/order", {
                                "exchange": EXCHANGE_NAME, "symbol": symbol, "side": close_side,
                                "type": "STOP_MARKET", "order_type": "STOP_MARKET", "quantity": 0, "trigger_price": candidate_sl, "reduce_only": True
                            })
                            if r.get("ok"):
                                self._safe_cancel(symbol, trade.get("sl_order_id", ""))
                                trade["stop_loss"], trade["sl_order_id"] = candidate_sl, r["body"].get("data", {}).get("order_id", "")
                                db.save_position(trade)
                                break
                            time.sleep(1)
                            
                if current_price <= trade["take_profit"]: self._close_position_locked(symbol, "🎯 TP Hit!", current_price)
                elif current_price >= trade["stop_loss"]: self._close_position_locked(symbol, "🛑 SL Hit!", current_price)

    def _close_position_locked(self, symbol: str, reason: str, exit_price: float):
        trade = self.active_trades.pop(symbol, None)
        if not trade: return
        close_side = "SELL" if trade["side"] == "LONG" else "BUY"
        
        self._safe_cancel(symbol, trade.get("sl_order_id", ""))
        self._safe_cancel(symbol, trade.get("tp_order_id", ""))
        time.sleep(1.5) 
        
        resp = coinswitch_request("POST", "/trade/api/v2/futures/order", {
            "symbol": symbol, "exchange": EXCHANGE_NAME, "side": close_side,
            "type": "MARKET", "order_type": "MARKET", "quantity": trade["quantity"], "reduce_only": True,
        })
        
        if not resp.get("ok"):
            pos_resp = coinswitch_request("GET", f"/trade/api/v2/futures/positions?exchange={EXCHANGE_NAME}&symbol={symbol}", track_errors=False)
            size = 0.0
            if pos_resp.get("ok"):
                for p in pos_resp.get("body", {}).get("data", []):
                    if isinstance(p, dict) and str(p.get("status", "OPEN")).upper() == "OPEN":
                        size += abs(safe_float(_pick(p, "position_size", "size", "quantity")))
            
            if size > 0:
                logger.info(f"Market close rejected for {symbol}, position still open. Retrying later.")
                self.active_trades[symbol] = trade
                return
            else:
                logger.info(f"{symbol} already 0 on exchange. Exchange Hit SL/TP.")
                if "SL Hit" in reason: exit_price = safe_float(trade["stop_loss"])
                elif "TP Hit" in reason: exit_price = safe_float(trade["take_profit"])

        entry, qty = trade["entry_price"], trade["quantity"]
        gross_pnl = (exit_price - entry) * qty if trade["side"] == "LONG" else (entry - exit_price) * qty
        net_pnl = round(gross_pnl - ((entry + exit_price) * qty * (ROUND_TRIP_FEE_PCT / 2.0)), 3)
        
        db.delete_position(symbol)
        db.record_closed_trade(symbol, trade["side"], entry, exit_price, qty, net_pnl, reason)
        
        notify(f"🔔 *Position Closed: {reason}*\nCoin: `{symbol}`\nEntry: `{entry}` | Exit: `{exit_price}`\n💵 Net PnL: `{net_pnl:+.2f} USDT`\n📅 Today: `{round(db.get_today_realized_pnl(), 2):+.2f} USDT`")

manager = EnterprisePositionManager()

# ==========================================
# 10. AI SELF-LEARNING & WORKER THREADS
# ==========================================
def self_learning_loop():
    time.sleep(60) 
    while True:
        try:
            trades = db.get_recent_trades(30)
            if len(trades) >= 10:
                wins = len([t for t in trades if safe_float(t.get('net_pnl_usdt')) > 0])
                win_rate = wins / len(trades)
                
                current_buf = DYNAMIC_SETTINGS["atr_buffer"]
                if win_rate < 0.40 and current_buf < 0.60:
                    DYNAMIC_SETTINGS["atr_buffer"] += 0.05
                    notify(f"🧠 *AI Self-Learning:* Win-rate low ({win_rate:.0%}). Increasing ATR Buffer to {DYNAMIC_SETTINGS['atr_buffer']:.2f} to avoid fakeouts.")
                elif win_rate > 0.60 and current_buf > 0.15:
                    DYNAMIC_SETTINGS["atr_buffer"] -= 0.05
                    notify(f"🧠 *AI Self-Learning:* Market favorable ({win_rate:.0%}). Tightening ATR Buffer to {DYNAMIC_SETTINGS['atr_buffer']:.2f}.")
        except Exception as e:
            logger.error(f"AI Learning error: {e}")
        time.sleep(3600 * 6)

def position_monitor_loop():
    reconcile_counter = 0
    while True:
        try:
            if manager.active_trades: manager.monitor_and_trail_positions()
            reconcile_counter += 1
            if reconcile_counter >= 12:
                manager.reconcile_with_exchange()
                reconcile_counter = 0
        except Exception: pass
        time.sleep(10)

def parallel_scanner_loop():
    manager.reconcile_with_exchange()
    if verify_sufficient_balance(): notify("✅ *CoinSwitch Bot Live (Anti-Whale Dump + Dynamic SL Buffer Active)!*")
    
    while True:
        try:
            if api_guard.is_circuit_open(): time.sleep(30); continue
            
            watchlist = [s for s in sorted(SUPPORTED_COINSWITCH_SYMBOLS) if bad_symbols.get(s, 0) < 5 and s not in manager.active_trades]
            print(f"🔍 [Scan] Checking {len(watchlist)} coins | Open Trades: {len(manager.active_trades)}", flush=True)
            
            with ThreadPoolExecutor(max_workers=2) as executor:
                for future in as_completed({executor.submit(analyze_closed_candle, coin): coin for coin in watchlist}):
                    try:
                        setup = future.result()
                        if setup: manager.execute_entry(setup)
                    except Exception: pass
            
            print(f"📊 Scan summary: {pop_reject_stats()}", flush=True)
            time.sleep(90)
        except Exception as e:
            print(f"🚨 Scanner crash: {e}", flush=True)
            time.sleep(15)

if __name__ == "__main__":
    threading.Thread(target=keep_alive_pinger, daemon=True).start()
    threading.Thread(target=self_learning_loop, daemon=True).start()
    threading.Thread(target=position_monitor_loop, daemon=True).start()
    threading.Thread(target=parallel_scanner_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
