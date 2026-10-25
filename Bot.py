import os
import time
import json
import uuid
import math
import sqlite3
import logging
import requests
import threading
from collections import Counter
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bot")

# ==========================================
# 0. WEB SERVER & RENDER 24/7 SELF-PING
# ==========================================
app = Flask(__name__)

@app.route('/')
@app.route('/health')
def health():
    return jsonify({"status": "running", "timestamp": int(time.time())}), 200

def keep_alive_pinger():
    render_url = os.getenv("RENDER_EXTERNAL_URL")
    while True:
        time.sleep(240)
        try:
            target = f"{render_url}/health" if render_url else f"http://127.0.0.1:{os.environ.get('PORT', 10000)}/health"
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
    raise ValueError("🚨 CRITICAL: API_KEY or SECRET_KEY is missing in Render Environment Variables! Bot execution stopped.")

BASE_URL = "https://coinswitch.co"
EXCHANGE_NAME = "EXCHANGE_2"
TIMEFRAME = "15m"
LEVERAGE = 3

# Risk, Sizing, Fees & Circuit Breaker Limits (RISK WALA PART SAME RAKHA HAI)
MAX_MARGIN_PER_TRADE_USDT = 15.0
RISK_PER_TRADE_USDT = 1.50
MIN_USDT_REQUIRED = 12.0
MIN_INR_REQUIRED = 200.0
ROUND_TRIP_FEE_PCT = 0.0012
MAX_DAILY_LOSS_USDT = 10.0
MAX_CONCURRENT_TRADES = 6
MAX_SAME_DIRECTION_TRADES = 3
MAX_CONSECUTIVE_ERRORS = 5

# ---------- AGGRESSIVE SIGNAL SETTINGS (yahan se tune karo) ----------
# Purana (strict) value comment mein likha hai
BREAKOUT_LOOKBACK = 12        # purana: 20  (chhota = breakout zyada baar banega)
BREAKOUT_BUFFER_ATR = 0.05    # purana: 0.15
VOL_MULT = 0.85               # purana: 1.15 (0.85 = avg volume ka 85% bhi chalega)
MIN_BODY_ATR = 0.15           # purana: 0.25
RSI_LONG_MIN, RSI_LONG_MAX = 50, 75    # purana: 52-70
RSI_SHORT_MIN, RSI_SHORT_MAX = 25, 50  # purana: 30-48
EDGE_MIN_WINRATE = 0.40       # purana: 0.45
REQUIRE_EMA_TREND = True      # False karoge to EMA20/50 filter band (zyada trades, zyada risk)

# ---------- RECONCILE SETTINGS ----------
RECONCILE_GRACE_SECONDS = 30
MISSING_CONFIRMATIONS = 2
MAX_API_FAILS_BEFORE_ALERT = 3

DB_PATH = "bot_state.db"

SUPPORTED_COINSWITCH_SYMBOLS = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT",
    "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", "MATICUSDT", "NEARUSDT",
    "LTCUSDT", "UNIUSDT", "ATOMUSDT", "APTUSDT", "ARBUSDT", "OPUSDT",
    "SUIUSDT", "FILUSDT", "INJUSDT", "RNDRUSDT", "STXUSDT", "IMXUSDT",
    "ETCUSDT", "AAVEUSDT", "GALAUSDT", "SANDUSDT", "FETUSDT", "TIAUSDT",
    "SEIUSDT", "ORDIUSDT", "WLDUSDT", "JUPUSDT", "PENDLEUSDT", "ENAUSDT",
    "BCHUSDT", "TRXUSDT", "SHIBUSDT", "PEPEUSDT", "WIFUSDT", "BONKUSDT"
}

# ==========================================
# 1b. DIAGNOSTICS (kaun sa filter trade rok raha hai)
# ==========================================
reject_stats = Counter()
reject_lock = threading.Lock()
kline_error_prints = {"n": 0}

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

# ==========================================
# 2. PERSISTENT SQLITE DATABASE
# ==========================================
class StateDatabase:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.db_lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self.db_lock, sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS active_positions (
                    symbol TEXT PRIMARY KEY,
                    side TEXT,
                    entry_price REAL,
                    quantity REAL,
                    stop_loss REAL,
                    take_profit REAL,
                    highest_price REAL,
                    lowest_price REAL,
                    atr REAL,
                    entry_order_id TEXT,
                    sl_order_id TEXT,
                    tp_order_id TEXT,
                    candle_timestamp INTEGER,
                    opened_at INTEGER
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS trade_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    side TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    quantity REAL,
                    net_pnl_usdt REAL,
                    reason TEXT,
                    closed_date TEXT,
                    closed_at INTEGER
                )
            """)
            conn.commit()

    def save_position(self, pos: dict):
        with self.db_lock, sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute("""
                INSERT OR REPLACE INTO active_positions VALUES (
                    :symbol, :side, :entry_price, :quantity, :stop_loss, :take_profit,
                    :highest_price, :lowest_price, :atr, :entry_order_id, :sl_order_id,
                    :tp_order_id, :candle_timestamp, :opened_at
                )
            """, pos)
            conn.commit()

    def delete_position(self, symbol: str):
        with self.db_lock, sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM active_positions WHERE symbol = ?", (symbol,))
            conn.commit()

    def load_all_positions(self) -> dict:
        with self.db_lock, sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM active_positions").fetchall()
            return {row["symbol"]: dict(row) for row in rows}

    def record_closed_trade(self, symbol, side, entry_price, exit_price, qty, net_pnl, reason):
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self.db_lock, sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO trade_history (symbol, side, entry_price, exit_price, quantity, net_pnl_usdt, reason, closed_date, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (symbol, side, entry_price, exit_price, qty, net_pnl, reason, today_str, int(time.time())))
            conn.commit()

    def get_today_realized_pnl(self) -> float:
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self.db_lock, sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT SUM(net_pnl_usdt) FROM trade_history WHERE closed_date = ?", (today_str,)).fetchone()
            return float(row[0]) if row and row[0] is not None else 0.0

db = StateDatabase()

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
        except Exception as e:
            print(f"Telegram attempt {attempt+1} failed: {e}", flush=True)
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
            if wait > 0:
                time.sleep(wait)
            self.last_call_ts = time.time()

    def record_success(self):
        with self.lock:
            self.consecutive_errors = 0

    def record_error(self, err_detail: str):
        with self.lock:
            self.consecutive_errors += 1
            count = self.consecutive_errors
            if count >= MAX_CONSECUTIVE_ERRORS and time.time() > self.circuit_tripped_until:
                self.circuit_tripped_until = time.time() + 900
                notify(f"🚨 *EMERGENCY CIRCUIT BREAKER TRIPPED!*\n`{count}` consecutive API errors detected.\nLast error: `{err_detail[:120]}`\nNew entries paused for 15 minutes.")

    def is_circuit_open(self) -> bool:
        return time.time() < self.circuit_tripped_until

api_guard = APIGuard()

# ==========================================
# 5. COINSWITCH Ed25519 ENGINE
# ==========================================
def coinswitch_request(method: str, endpoint: str, payload: dict = None) -> dict:
    api_guard.throttle(0.25)
    url = f"{BASE_URL}{endpoint}"
    epoch = str(int(time.time() * 1000))

    msg = f"{method}{endpoint}{epoch}".encode('utf-8')
    priv_key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(SECRET_KEY))
    signature = priv_key.sign(msg).hex()

    headers = {
        "X-AUTH-APIKEY": API_KEY,
        "X-AUTH-SIGNATURE": signature,
        "X-AUTH-EPOCH": epoch,
        "Content-Type": "application/json",
        "User-Agent": "CoinSwitchPro-Enterprise-Bot"
    }

    try:
        if method == "GET":
            res = requests.get(url, headers=headers, timeout=10)
        elif method == "DELETE":
            res = requests.delete(url, headers=headers, json=payload, timeout=10)
        else:
            res = requests.post(url, headers=headers, json=payload, timeout=10)

        if res.status_code in (200, 201):
            data = res.json()
            api_guard.record_success()
            return {"ok": True, "status": res.status_code, "body": data}
        else:
            err_text = res.text[:200]
            api_guard.record_error(f"HTTP {res.status_code}: {err_text}")
            return {"ok": False, "status": res.status_code, "error": err_text}
    except Exception as e:
        api_guard.record_error(str(e))
        return {"ok": False, "status": 0, "error": str(e)}

# ==========================================
# 6. BALANCE, LEVERAGE (ab logs ke saath)
# ==========================================
def verify_sufficient_balance() -> bool:
    """Fail-closed. Ab reason bhi print hota hai taaki pata chale kyun block hua."""
    resp = coinswitch_request("GET", "/trade/api/v2/user/portfolio")
    if not resp.get("ok"):
        print(f"⚠️ Balance check failed: {resp.get('error')}", flush=True)
        reject("balance_api_fail")
        return False

    try:
        body = resp["body"]
        data = body.get("data", body)
        usdt_bal, inr_bal = 0.0, 0.0
        if isinstance(data, list):
            for item in data:
                curr = str(item.get("currency", "")).upper()
                bal = safe_float(item.get("main_balance", 0.0))
                if curr == "USDT":
                    usdt_bal = bal
                elif curr == "INR":
                    inr_bal = bal
        elif isinstance(data, dict):
            usdt_bal = safe_float(data.get("USDT", {}).get("available", data.get("available_balance", 0.0)))
            inr_bal = safe_float(data.get("INR", {}).get("available", 0.0))

        ok = (usdt_bal >= MIN_USDT_REQUIRED) or (inr_bal >= MIN_INR_REQUIRED)
        if not ok:
            print(f"⚠️ Balance low/not found: USDT={usdt_bal} (need {MIN_USDT_REQUIRED}), "
                  f"INR={inr_bal} (need {MIN_INR_REQUIRED}). Raw={str(data)[:200]}", flush=True)
            reject("balance_low")
        return ok
    except Exception as e:
        print(f"Balance parse error: {e}", flush=True)
        reject("balance_parse_error")
    return False

def set_and_verify_leverage(symbol: str, leverage: int) -> bool:
    payload = {"symbol": symbol, "exchange": EXCHANGE_NAME, "leverage": leverage}
    resp = coinswitch_request("POST", "/trade/api/v2/futures/leverage", payload)
    if not resp.get("ok"):
        print(f"⚠️ Leverage set failed {symbol}: {resp.get('error')}", flush=True)
    return resp.get("ok", False)

# ==========================================
# 7. INDICATORS & SIGNAL
# ==========================================
def fetch_klines(symbol: str, interval="15m", limit=120) -> list:
    api_guard.throttle(0.12)
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=6)
        if res.status_code == 200:
            return [
                {
                    'ts': int(c[0]),
                    'open': float(c[1]),
                    'high': float(c[2]),
                    'low': float(c[3]),
                    'close': float(c[4]),
                    'volume': float(c[5])
                }
                for c in res.json()
            ]
        # Pehle silent tha -> ab dikhega (451 = Binance ne server ka region block kiya)
        reject(f"klines_http_{res.status_code}")
        if kline_error_prints["n"] < 5:
            kline_error_prints["n"] += 1
            print(f"⚠️ Binance klines {symbol} HTTP {res.status_code}: {res.text[:150]}", flush=True)
    except Exception as e:
        reject("klines_exception")
        if kline_error_prints["n"] < 5:
            kline_error_prints["n"] += 1
            print(f"⚠️ Binance klines {symbol} error: {e}", flush=True)
    return []

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
        gains.append(diff if diff >= 0 else 0.0)
        losses.append(abs(diff) if diff < 0 else 0.0)

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def calculate_atr(candles: list, period=14):
    if not candles or len(candles) <= period:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]['high'], candles[i]['low'], candles[i - 1]['close']
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr if atr > 0 else None

def verify_historical_edge(candles: list, direction: str) -> bool:
    wins, losses = 0, 0
    for idx in range(45, len(candles) - 5):
        sub = candles[:idx]
        c_price = sub[-1]['close']
        prev_high = max(x['high'] for x in sub[-(BREAKOUT_LOOKBACK + 1):-1])
        prev_low = min(x['low'] for x in sub[-(BREAKOUT_LOOKBACK + 1):-1])
        future_high = max(x['high'] for x in candles[idx:idx + 4])
        future_low = min(x['low'] for x in candles[idx:idx + 4])

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
        return True
    return (wins / (wins + losses)) >= EDGE_MIN_WINRATE

def analyze_closed_candle(symbol: str):
    """Sirf fully-closed candle pe signal. Har reject ka reason count hota hai."""
    candles = fetch_klines(symbol, TIMEFRAME, 110)
    if len(candles) < 80:
        reject("no_data")
        return None

    closed_candles = candles[:-1]
    last_closed = closed_candles[-1]
    prev_closed = closed_candles[-(BREAKOUT_LOOKBACK + 1):-1]

    closes = [c['close'] for c in closed_candles]
    volumes = [c['volume'] for c in closed_candles]

    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(closed_candles, 14)

    if None in (ema20, ema50, rsi, atr) or atr <= 0:
        reject("indicator_none")
        return None

    support = min(c['low'] for c in prev_closed)
    resistance = max(c['high'] for c in prev_closed)
    avg_vol = sum(volumes[-21:-1]) / 20.0

    confirmed_close = last_closed['close']
    candle_body = abs(last_closed['close'] - last_closed['open'])
    breakout_buffer = BREAKOUT_BUFFER_ATR * atr

    if last_closed['volume'] < (VOL_MULT * avg_vol):
        reject("low_volume")
        return None
    if candle_body < (MIN_BODY_ATR * atr):
        reject("weak_body")
        return None

    if last_closed['close'] > last_closed['open']:
        direction = "LONG"
    elif last_closed['close'] < last_closed['open']:
        direction = "SHORT"
    else:
        reject("doji")
        return None

    if direction == "LONG":
        if REQUIRE_EMA_TREND and not (ema20 > ema50):
            reject("long_ema_trend")
            return None
        if not (confirmed_close > resistance + breakout_buffer):
            reject("long_no_breakout")
            return None
        if not (RSI_LONG_MIN <= rsi <= RSI_LONG_MAX):
            reject("long_rsi")
            return None
    else:
        if REQUIRE_EMA_TREND and not (ema20 < ema50):
            reject("short_ema_trend")
            return None
        if not (confirmed_close < support - breakout_buffer):
            reject("short_no_breakout")
            return None
        if not (RSI_SHORT_MIN <= rsi <= RSI_SHORT_MAX):
            reject("short_rsi")
            return None

    if not verify_historical_edge(closed_candles, direction):
        reject("edge_fail")
        return None

    reject("SIGNAL_FOUND")
    return {
        "symbol": symbol,
        "signal": direction,
        "signal_close": confirmed_close,
        "atr": atr,
        "support": support,
        "resistance": resistance,
        "rsi": round(rsi, 2),
        "candle_ts": last_closed['ts']
    }

# ==========================================
# 8. PRECISION & RISK-BASED SIZING
# ==========================================
def format_price(val: float) -> float:
    if val >= 1000: return round(val, 2)
    elif val >= 10: return round(val, 3)
    elif val >= 1: return round(val, 4)
    elif val >= 0.01: return round(val, 5)
    else: return round(val, 6)

def calculate_risk_based_quantity(entry_price: float, sl_distance: float) -> float:
    if entry_price <= 0 or sl_distance <= 0:
        return 0.0

    risk_qty = RISK_PER_TRADE_USDT / sl_distance
    max_notional_qty = (MAX_MARGIN_PER_TRADE_USDT * LEVERAGE) / entry_price
    raw_qty = min(risk_qty, max_notional_qty)

    if entry_price >= 1000:
        qty = math.floor(raw_qty * 1000) / 1000.0
    elif entry_price >= 10:
        qty = math.floor(raw_qty * 100) / 100.0
    elif entry_price >= 0.5:
        qty = math.floor(raw_qty * 10) / 10.0
    else:
        qty = float(math.floor(raw_qty))

    return qty

# ==========================================
# 9. ENTERPRISE POSITION MANAGER
# ==========================================
class EnterprisePositionManager:
    def __init__(self):
        self.execution_lock = threading.Lock()
        self.active_trades = db.load_all_positions()
        self.processed_candles = {}

        self._reconcile_missing_count = {}
        self._reconcile_api_fails = 0

    def _safe_cancel(self, symbol, order_id):
        if not order_id:
            return
        try:
            self.cancel_exchange_order(symbol, order_id)
        except Exception:
            logger.warning("Cancel failed/ignored: %s order=%s", symbol, order_id, exc_info=True)

    def reconcile_with_exchange(self):
        """Local state ko CoinSwitch open positions ke saath sync karta hai (safe version)."""
        snapshot_time = time.time()
        try:
            resp = coinswitch_request(
                "GET", f"/trade/api/v2/futures/positions?exchange={EXCHANGE_NAME}"
            )
        except Exception:
            logger.exception("Reconcile: API request crashed")
            self._reconcile_api_fails += 1
            return

        if not resp or not resp.get("ok"):
            self._reconcile_api_fails += 1
            logger.warning("Reconcile skipped: API failed (%s in a row) resp=%s",
                           self._reconcile_api_fails, resp)
            if self._reconcile_api_fails >= MAX_API_FAILS_BEFORE_ALERT:
                logger.error("ALERT: Reconcile API %s baar lagataar fail!", self._reconcile_api_fails)
            return

        body = resp.get("body") or {}
        positions = body.get("data")
        if not isinstance(positions, list):
            self._reconcile_api_fails += 1
            logger.warning("Reconcile skipped: bad payload (%r)", type(positions))
            return
        self._reconcile_api_fails = 0

        exchange_open = {}
        for p in positions:
            try:
                sym = normalize_symbol(p.get("symbol"))
                raw = p.get("size")
                if raw is None:
                    raw = p.get("quantity")
                size = abs(safe_float(raw))
                if sym and size > 0:
                    exchange_open[sym] = size
            except Exception:
                logger.exception("Reconcile skipped: bad position row %s", p)
                return

        orphans = []  # (symbol, sl_id, tp_id) -> lock ke BAHAR cancel honge

        with self.execution_lock:
            # 1) Zombie cleanup
            for local_key in list(self.active_trades.keys()):
                trade = self.active_trades.get(local_key) or {}
                norm = normalize_symbol(local_key)

                opened_at = safe_float(trade.get("opened_at"), 0.0)
                if opened_at and (snapshot_time - opened_at) < RECONCILE_GRACE_SECONDS:
                    continue
                if trade.get("pending") or trade.get("status") in ("PENDING", "OPENING"):
                    continue

                if norm in exchange_open:
                    self._reconcile_missing_count.pop(local_key, None)
                    continue

                cnt = self._reconcile_missing_count.get(local_key, 0) + 1
                self._reconcile_missing_count[local_key] = cnt
                if cnt < MISSING_CONFIRMATIONS:
                    logger.info("Reconcile: %s missing (%s/%s), waiting", local_key, cnt, MISSING_CONFIRMATIONS)
                    continue

                logger.warning("Reconcile: %s closed on exchange. Cleaning local state.", local_key)
                orphans.append((local_key, trade.get("sl_order_id", ""), trade.get("tp_order_id", "")))

                self.active_trades.pop(local_key, None)
                self._reconcile_missing_count.pop(local_key, None)
                try:
                    db.delete_position(local_key)
                except Exception:
                    logger.exception("Reconcile: DB delete failed for %s", local_key)

            # 2) Reverse check
            local_norm = {normalize_symbol(k) for k in self.active_trades}
            for sym, size in exchange_open.items():
                if sym not in local_norm:
                    logger.error("UNTRACKED exchange position: %s size=%s -> bot manage nahi kar raha!", sym, size)

            # 3) Size mismatch
            for local_key, trade in self.active_trades.items():
                norm = normalize_symbol(local_key)
                if norm in exchange_open:
                    local_size = abs(safe_float(trade.get("quantity", trade.get("size"))))
                    if local_size and abs(local_size - exchange_open[norm]) / local_size > 0.01:
                        logger.warning("Size mismatch %s: local=%s exchange=%s",
                                       local_key, local_size, exchange_open[norm])

        # 4) Orphan SL/TP orders cancel (lock ke bahar)
        for sym, sl_id, tp_id in orphans:
            self._safe_cancel(sym, sl_id)
            self._safe_cancel(sym, tp_id)

    def can_take_new_trade(self, symbol: str, side: str, candle_ts: int) -> bool:
        if api_guard.is_circuit_open():
            reject("blocked_circuit_breaker")
            return False

        today_pnl = db.get_today_realized_pnl()
        if today_pnl <= -MAX_DAILY_LOSS_USDT:
            reject("blocked_daily_loss_limit")
            return False

        with self.execution_lock:
            if symbol in self.active_trades:
                reject("blocked_already_open")
                return False
            if len(self.active_trades) >= MAX_CONCURRENT_TRADES:
                reject("blocked_max_trades")
                return False
            same_dir_count = sum(1 for t in self.active_trades.values() if t["side"] == side)
            if same_dir_count >= MAX_SAME_DIRECTION_TRADES:
                reject("blocked_same_direction")
                return False
            if self.processed_candles.get(symbol) == candle_ts:
                reject("blocked_same_candle")
                return False

        return True

    def place_exchange_trigger_order(self, symbol: str, close_side: str, qty: float, trigger_price: float, order_type: str) -> str:
        payload = {
            "symbol": symbol,
            "exchange": EXCHANGE_NAME,
            "side": close_side,
            "type": order_type,
            "quantity": qty,
            "trigger_price": trigger_price,
            "reduce_only": True
        }
        resp = coinswitch_request("POST", "/trade/api/v2/futures/order", payload)
        if resp.get("ok"):
            data = resp["body"].get("data", resp["body"])
            if isinstance(data, dict):
                return str(data.get("order_id", ""))
        else:
            print(f"⚠️ {order_type} order failed {symbol}: {resp.get('error')}", flush=True)
        return ""

    def cancel_exchange_order(self, symbol: str, order_id: str):
        if not order_id:
            return
        coinswitch_request("DELETE", "/trade/api/v2/futures/order", {"symbol": symbol, "exchange": EXCHANGE_NAME, "order_id": order_id})

    def execute_entry(self, signal_data: dict):
        symbol = signal_data["symbol"]
        side_signal = signal_data["signal"]
        candle_ts = signal_data["candle_ts"]

        print(f"🎯 Signal: {symbol} {side_signal} (RSI {signal_data['rsi']})", flush=True)

        if not self.can_take_new_trade(symbol, side_signal, candle_ts):
            print(f"⛔ {symbol} entry blocked by risk rules", flush=True)
            return

        with self.execution_lock:
            if symbol in self.active_trades:
                return

            if not verify_sufficient_balance():
                print(f"⛔ {symbol} entry blocked: balance check", flush=True)
                return

            set_and_verify_leverage(symbol, LEVERAGE)

            atr = signal_data["atr"]
            ref_price = signal_data["signal_close"]
            sl_distance = 1.5 * atr

            quantity = calculate_risk_based_quantity(ref_price, sl_distance)
            if quantity <= 0:
                reject("qty_zero")
                print(f"⛔ {symbol} qty=0 (price {ref_price} bahut high / margin cap chhota). "
                      f"MAX_MARGIN_PER_TRADE_USDT badhao ya is coin ko skip maano.", flush=True)
                return

            order_side = "BUY" if side_signal == "LONG" else "SELL"
            close_side = "SELL" if side_signal == "LONG" else "BUY"
            client_oid = f"cs_{symbol}_{candle_ts}_{uuid.uuid4().hex[:6]}"

            entry_payload = {
                "symbol": symbol,
                "exchange": EXCHANGE_NAME,
                "side": order_side,
                "type": "MARKET",
                "quantity": quantity,
                "leverage": LEVERAGE,
                "client_order_id": client_oid
            }

            resp = coinswitch_request("POST", "/trade/api/v2/futures/order", entry_payload)
            if not resp.get("ok"):
                reject("order_rejected")
                print(f"❌ Order rejected for {symbol}: {resp.get('error')}", flush=True)
                return

            body = resp["body"]
            order_data = body.get("data", body) if isinstance(body, dict) else {}
            order_id = str(order_data.get("order_id", ""))
            status_str = str(order_data.get("status", "")).upper()

            if not order_id or status_str in ("REJECTED", "CANCELLED", "FAILED", "EXPIRED"):
                reject("order_unconfirmed")
                print(f"❌ Unconfirmed or rejected order on {symbol}: {body}", flush=True)
                return

            actual_fill = safe_float(order_data.get("avg_execution_price") or order_data.get("price") or ref_price)
            if actual_fill <= 0:
                actual_fill = ref_price

            fee_buffer = actual_fill * ROUND_TRIP_FEE_PCT
            if side_signal == "LONG":
                stop_loss = format_price(actual_fill - sl_distance)
                real_risk = actual_fill - stop_loss
                take_profit = format_price(actual_fill + (2.0 * real_risk) + (2.0 * fee_buffer))
            else:
                stop_loss = format_price(actual_fill + sl_distance)
                real_risk = stop_loss - actual_fill
                take_profit = format_price(actual_fill - (2.0 * real_risk) - (2.0 * fee_buffer))

            sl_order_id = self.place_exchange_trigger_order(symbol, close_side, quantity, stop_loss, "STOP_MARKET")
            tp_order_id = self.place_exchange_trigger_order(symbol, close_side, quantity, take_profit, "TAKE_PROFIT_MARKET")

            if not sl_order_id:
                notify(f"🚨 *WARNING:* `{symbol}` ka exchange SL place NAHI hua! Position sirf bot ke software SL pe chal rahi hai. Manually check karo.")

            pos_record = {
                "symbol": symbol,
                "side": side_signal,
                "entry_price": actual_fill,
                "quantity": quantity,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "highest_price": actual_fill,
                "lowest_price": actual_fill,
                "atr": atr,
                "entry_order_id": order_id,
                "sl_order_id": sl_order_id,
                "tp_order_id": tp_order_id,
                "candle_timestamp": candle_ts,
                "opened_at": int(time.time())
            }

            self.active_trades[symbol] = pos_record
            self.processed_candles[symbol] = candle_ts
            db.save_position(pos_record)
            open_count = len(self.active_trades)

        emoji = "🟢 *CONFIRMED LONG OPENED*" if side_signal == "LONG" else "🔴 *CONFIRMED SHORT OPENED*"
        notify(
            f"{emoji} (`{open_count}/{MAX_CONCURRENT_TRADES}`)\n"
            f"Coin: `{symbol}` | Order ID: `{order_id[:10]}`\n"
            f"Actual Fill: `{actual_fill}` | Qty: `{quantity}` ({LEVERAGE}x)\n"
            f"🛑 Exchange SL (1.5x ATR): `{stop_loss}`\n"
            f"🎯 Net 1:2 TP (Fee-Adjusted): `{take_profit}`\n"
            f"📊 Closed Candle RSI: `{signal_data['rsi']}`"
        )

    def monitor_and_trail_positions(self):
        with self.execution_lock:
            symbols = list(self.active_trades.keys())

        for sym in symbols:
            candles = fetch_klines(sym, TIMEFRAME, 2)
            if not candles:
                continue
            current_price = candles[-1]['close']
            self._update_single_position(sym, current_price)

    def _update_single_position(self, symbol: str, current_price: float):
        with self.execution_lock:
            trade = self.active_trades.get(symbol)
            if not trade:
                return

            side = trade["side"]
            atr = trade["atr"]
            close_side = "SELL" if side == "LONG" else "BUY"
            min_trail_step = 0.25 * atr

            if side == "LONG":
                if current_price > trade["highest_price"]:
                    trade["highest_price"] = current_price
                    candidate_sl = format_price(current_price - (1.2 * atr))
                    if (candidate_sl - trade["stop_loss"]) >= min_trail_step:
                        self.cancel_exchange_order(symbol, trade.get("sl_order_id", ""))
                        new_sl_id = self.place_exchange_trigger_order(symbol, close_side, trade["quantity"], candidate_sl, "STOP_MARKET")
                        trade["stop_loss"] = candidate_sl
                        trade["sl_order_id"] = new_sl_id
                        db.save_position(trade)

                if current_price >= trade["take_profit"]:
                    self._close_position_locked(symbol, "🎯 Take Profit Hit!", current_price)
                elif current_price <= trade["stop_loss"]:
                    self._close_position_locked(symbol, "🛑 Trailing SL Hit!", current_price)

            elif side == "SHORT":
                if current_price < trade["lowest_price"]:
                    trade["lowest_price"] = current_price
                    candidate_sl = format_price(current_price + (1.2 * atr))
                    if (trade["stop_loss"] - candidate_sl) >= min_trail_step:
                        self.cancel_exchange_order(symbol, trade.get("sl_order_id", ""))
                        new_sl_id = self.place_exchange_trigger_order(symbol, close_side, trade["quantity"], candidate_sl, "STOP_MARKET")
                        trade["stop_loss"] = candidate_sl
                        trade["sl_order_id"] = new_sl_id
                        db.save_position(trade)

                if current_price <= trade["take_profit"]:
                    self._close_position_locked(symbol, "🎯 Take Profit Hit!", current_price)
                elif current_price >= trade["stop_loss"]:
                    self._close_position_locked(symbol, "🛑 Trailing SL Hit!", current_price)

    def _close_position_locked(self, symbol: str, reason: str, exit_price: float):
        trade = self.active_trades.pop(symbol, None)
        if not trade:
            return

        close_side = "SELL" if trade["side"] == "LONG" else "BUY"
        self.cancel_exchange_order(symbol, trade.get("sl_order_id", ""))
        self.cancel_exchange_order(symbol, trade.get("tp_order_id", ""))

        close_payload = {
            "symbol": symbol,
            "exchange": EXCHANGE_NAME,
            "side": close_side,
            "type": "MARKET",
            "quantity": trade["quantity"],
            "reduce_only": True
        }
        coinswitch_request("POST", "/trade/api/v2/futures/order", close_payload)

        entry = trade["entry_price"]
        qty = trade["quantity"]
        gross_pnl = (exit_price - entry) * qty if trade["side"] == "LONG" else (entry - exit_price) * qty
        fees = (entry + exit_price) * qty * (ROUND_TRIP_FEE_PCT / 2.0)
        net_pnl = round(gross_pnl - fees, 3)

        db.delete_position(symbol)
        db.record_closed_trade(symbol, trade["side"], entry, exit_price, qty, net_pnl, reason)
        today_total = round(db.get_today_realized_pnl(), 2)

        notify(
            f"🔔 *Position Closed: {reason}*\n"
            f"Coin: `{symbol}` (`{trade['side']}`)\n"
            f"Entry: `{entry}` | Exit: `{exit_price}`\n"
            f"💵 Net PnL (After Fees): `{net_pnl:+.2f} USDT`\n"
            f"📅 Today's Cumulative PnL: `{today_total:+.2f} USDT`"
        )

manager = EnterprisePositionManager()

# ==========================================
# 10. PARALLEL SCANNER & WORKER THREADS
# ==========================================
def get_compatible_watchlist() -> list:
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    try:
        res = requests.get(url, timeout=8)
        if res.status_code == 200:
            tickers = res.json()
            compatible = [t for t in tickers if t['symbol'] in SUPPORTED_COINSWITCH_SYMBOLS]
            compatible.sort(key=lambda x: abs(float(x.get('priceChangePercent', 0))), reverse=True)
            return [t['symbol'] for t in compatible]
        print(f"⚠️ Watchlist HTTP {res.status_code}: {res.text[:150]}", flush=True)
    except Exception as e:
        print(f"Watchlist error: {e}", flush=True)
    return list(SUPPORTED_COINSWITCH_SYMBOLS)

def position_monitor_loop():
    reconcile_counter = 0
    while True:
        try:
            if manager.active_trades:
                manager.monitor_and_trail_positions()
            reconcile_counter += 1
            if reconcile_counter >= 20:
                manager.reconcile_with_exchange()
                reconcile_counter = 0
        except Exception as e:
            print(f"Monitor error: {e}", flush=True)
        time.sleep(6)

def parallel_scanner_loop():
    print("🤖 Enterprise Bot Starting (AGGRESSIVE mode)...", flush=True)
    manager.reconcile_with_exchange()

    resp = coinswitch_request("GET", "/trade/api/v2/user/portfolio")
    if resp.get("ok"):
        notify(
            f"✅ *CoinSwitch Enterprise Bot Live (Aggressive)!*\n"
            f"• *Recovered Positions:* `{len(manager.active_trades)}`\n"
            f"• *Breakout:* `{BREAKOUT_LOOKBACK}` candles + `{BREAKOUT_BUFFER_ATR}x ATR`, Vol `{VOL_MULT}x`\n"
            f"• *Safety:* Exchange-Side SL/TP, SQLite State, Fee-Adjusted 1:2 RR & Daily Loss Guard"
        )
    else:
        notify(f"⚠️ *Startup Notice:* `{resp.get('error')}`")

    while True:
        try:
            if api_guard.is_circuit_open():
                time.sleep(30)
                continue

            watchlist = get_compatible_watchlist()
            print(f"🔍 [Scan] {len(watchlist)} coins | Open Trades: {len(manager.active_trades)}", flush=True)

            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_coin = {
                    executor.submit(analyze_closed_candle, coin): coin
                    for coin in watchlist
                    if coin not in manager.active_trades
                }
                for future in as_completed(future_to_coin):
                    try:
                        setup = future.result()
                    except Exception as e:
                        reject("analyze_exception")
                        print(f"Analyze error {future_to_coin[future]}: {e}", flush=True)
                        continue
                    if setup:
                        manager.execute_entry(setup)

            # YE LINE BATAYEGI KI TRADE KYUN NAHI AA RAHA
            print(f"📊 Scan summary (reject reasons): {pop_reject_stats()}", flush=True)

            time.sleep(25)
        except Exception as e:
            print(f"Scanner loop error: {e}", flush=True)
            time.sleep(15)

if __name__ == "__main__":
    threading.Thread(target=keep_alive_pinger, daemon=True).start()
    threading.Thread(target=position_monitor_loop, daemon=True).start()
    threading.Thread(target=parallel_scanner_loop, daemon=True).start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
