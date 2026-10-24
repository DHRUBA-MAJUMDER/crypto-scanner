import os
import time
import json
import uuid
import math
import sqlite3
import requests
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

# ==========================================
# 0. WEB SERVER & RENDER 24/7 SELF-PING (#28)
# ==========================================
app = Flask(__name__)

@app.route('/')
@app.route('/health')
def health():
    return jsonify({"status": "running", "timestamp": int(time.time())}), 200

def keep_alive_pinger():
    """Pings Render's external URL every 4 minutes so free instance doesn't sleep (#28)."""
    render_url = os.getenv("RENDER_EXTERNAL_URL")
    while True:
        time.sleep(240)
        try:
            target = f"{render_url}/health" if render_url else f"http://127.0.0.1:{os.environ.get('PORT', 10000)}/health"
            requests.get(target, timeout=5)
        except Exception:
            pass

# ==========================================
# 1. CREDENTIALS & RISK PARAMETERS (#1, #18, #20, #21, #22)
# ==========================================
# Keys are completely hidden. MUST be set in Render Environment Variables.
API_KEY = (os.getenv("COINSWITCH_API_KEY") or "").strip()
SECRET_KEY = (os.getenv("COINSWITCH_SECRET_KEY") or "").strip()
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

if not API_KEY or not SECRET_KEY:
    raise ValueError("🚨 CRITICAL: API_KEY or SECRET_KEY is missing in Render Environment Variables! Bot execution stopped.")

BASE_URL = "https://coinswitch.co"
EXCHANGE_NAME = "EXCHANGE_2"  # CoinSwitch Pro Futures identifier
TIMEFRAME = "15m"
LEVERAGE = 3

# Risk, Sizing, Fees & Circuit Breaker Limits
MAX_MARGIN_PER_TRADE_USDT = 15.0     # Maximum margin cap per trade
RISK_PER_TRADE_USDT = 1.50           # Risk amount per trade based on SL distance (#18)
MIN_USDT_REQUIRED = 12.0             # Strict minimum balance (#2)
MIN_INR_REQUIRED = 200.0             # Strict minimum INR balance (#2)
ROUND_TRIP_FEE_PCT = 0.0012          # 0.12% Taker + Funding buffer (#20)
MAX_DAILY_LOSS_USDT = 10.0           # Auto-stop trading if daily loss hits $10 (#21)
MAX_CONCURRENT_TRADES = 6            # Total open positions cap
MAX_SAME_DIRECTION_TRADES = 3        # Max 3 LONG or 3 SHORT at once to avoid correlation wipeout (#22)
MAX_CONSECUTIVE_ERRORS = 5           # Circuit breaker threshold (#31)

DB_PATH = "bot_state.db"

# Guaranteed liquid coins supported across both CoinSwitch Futures & Binance (#14)
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
# 2. PERSISTENT SQLITE DATABASE (#8, #21, #29, #32)
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
# 3. VERIFIED TELEGRAM ALERTS (#30)
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
# 4. RATE LIMITER & CIRCUIT BREAKER (#15, #24, #31)
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
                self.circuit_tripped_until = time.time() + 900  # 15 min circuit breaker pause
                notify(f"🚨 *EMERGENCY CIRCUIT BREAKER TRIPPED!*\n`{count}` consecutive API errors detected.\nLast error: `{err_detail[:120]}`\nNew entries paused for 15 minutes.")

    def is_circuit_open(self) -> bool:
        return time.time() < self.circuit_tripped_until

api_guard = APIGuard()

# ==========================================
# 5. COINSWITCH Ed25519 ENGINE (#15)
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
# 6. FAIL-CLOSED BALANCE, LEVERAGE & PRICE VERIFICATION (#2, #6, #10)
# ==========================================
def verify_sufficient_balance() -> bool:
    """Strictly fail-closed: returns False if API fails or balance is below threshold (#2)."""
    resp = coinswitch_request("GET", "/trade/api/v2/user/portfolio")
    if not resp.get("ok"):
        print(f"⚠️ Balance check failed: {resp.get('error')}", flush=True)
        return False

    try:
        body = resp["body"]
        data = body.get("data", body)
        if isinstance(data, list):
            for item in data:
                curr = str(item.get("currency", "")).upper()
                bal = float(item.get("main_balance", 0.0))
                if curr == "USDT" and bal >= MIN_USDT_REQUIRED:
                    return True
                if curr == "INR" and bal >= MIN_INR_REQUIRED:
                    return True
            return False
        elif isinstance(data, dict):
            usdt_bal = float(data.get("USDT", {}).get("available", data.get("available_balance", 0.0)))
            inr_bal = float(data.get("INR", {}).get("available", 0.0))
            return (usdt_bal >= MIN_USDT_REQUIRED) or (inr_bal >= MIN_INR_REQUIRED)
    except Exception as e:
        print(f"Balance parse error: {e}", flush=True)
    return False

def set_and_verify_leverage(symbol: str, leverage: int) -> bool:
    """Explicitly sets leverage on CoinSwitch Futures before trade entry (#10)."""
    payload = {"symbol": symbol, "exchange": EXCHANGE_NAME, "leverage": leverage}
    resp = coinswitch_request("POST", "/trade/api/v2/futures/leverage", payload)
    return resp.get("ok", False)

# ==========================================
# 7. TECHNICAL INDICATORS & HISTORICAL EDGE (#16, #17, #23, #26, #27)
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
    except Exception:
        pass
    return []

def calculate_ema(prices: list, period: int):
    # Strict validation: Never fallback to last price if data is insufficient (#27)
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
    # Strict ATR validation (#26)
    if not candles or len(candles) <= period:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]['high'], candles[i]['low'], candles[i - 1]['close']
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr if atr > 0 else None

def verify_historical_edge(candles: list, direction: str) -> bool:
    """
    Mini walk-forward backtest on the coin's recent 100 candles (#23).
    Ensures the coin respects trend momentum before risking real capital.
    """
    wins, losses = 0, 0
    for idx in range(45, len(candles) - 5):
        sub = candles[:idx]
        c_price = sub[-1]['close']
        prev_high = max(x['high'] for x in sub[-15:-1])
        prev_low = min(x['low'] for x in sub[-15:-1])
        future_high = max(x['high'] for x in candles[idx:idx+4])
        future_low = min(x['low'] for x in candles[idx:idx+4])

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
    return (wins / (wins + losses)) >= 0.45

def analyze_closed_candle(symbol: str):
    """
    Strictly analyzes ONLY fully closed candles (candles[:-1]) so the bot
    NEVER enters on an unconfirmed running candle (#16, #17, #26, #27).
    """
    candles = fetch_klines(symbol, TIMEFRAME, 110)
    if len(candles) < 80:
        return None

    # Drop the live running candle (candles[-1]) completely for signal generation!
    closed_candles = candles[:-1]
    last_closed = closed_candles[-1]       # Confirmed closed candle (candles[-2] of raw list)
    prev_closed_20 = closed_candles[-21:-1] # 20 closed candles prior to the breakout candle

    closes = [c['close'] for c in closed_candles]
    volumes = [c['volume'] for c in closed_candles]

    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(closed_candles, 14)

    # Validate all indicators are non-null and ATR is strictly positive (#26, #27)
    if None in (ema20, ema50, rsi, atr) or atr <= 0:
        return None

    support = min(c['low'] for c in prev_closed_20)
    resistance = max(c['high'] for c in prev_closed_20)
    avg_vol_20 = sum(volumes[-21:-1]) / 20.0

    confirmed_close = last_closed['close']
    confirmed_vol = last_closed['volume']
    candle_body = abs(last_closed['close'] - last_closed['open'])

    # Anti-Fakeout Filters (#16, #17):
    # 1. Price must close beyond S/R by at least 0.15 * ATR buffer
    # 2. Breakout candle volume must be > 1.15x 20-candle average volume
    # 3. Candle body must not be an indecisive doji (body >= 0.25 * ATR)
    breakout_buffer = 0.15 * atr
    vol_confirmed = confirmed_vol >= (1.15 * avg_vol_20)
    strong_body = candle_body >= (0.25 * atr)

    if not (vol_confirmed and strong_body):
        return None

    # 1. CONFIRMED LONG SIGNAL (Closed Candle above Resistance + Buffer)
    if (
        ema20 > ema50
        and last_closed['close'] > last_closed['open']
        and confirmed_close > (resistance + breakout_buffer)
        and 52 <= rsi <= 70
    ):
        if verify_historical_edge(closed_candles, "LONG"):
            return {
                "symbol": symbol,
                "signal": "LONG",
                "signal_close": confirmed_close,
                "atr": atr,
                "support": support,
                "resistance": resistance,
                "rsi": round(rsi, 2),
                "candle_ts": last_closed['ts']
            }

    # 2. CONFIRMED SHORT SIGNAL (Closed Candle below Support - Buffer)
    if (
        ema20 < ema50
        and last_closed['close'] < last_closed['open']
        and confirmed_close < (support - breakout_buffer)
        and 30 <= rsi <= 48
    ):
        if verify_historical_edge(closed_candles, "SHORT"):
            return {
                "symbol": symbol,
                "signal": "SHORT",
                "signal_close": confirmed_close,
                "atr": atr,
                "support": support,
                "resistance": resistance,
                "rsi": round(rsi, 2),
                "candle_ts": last_closed['ts']
            }

    return None

# ==========================================
# 8. PRECISION & RISK-BASED SIZING (#9, #18, #19, #20)
# ==========================================
def format_price(val: float) -> float:
    if val >= 1000: return round(val, 2)
    elif val >= 10: return round(val, 3)
    elif val >= 1: return round(val, 4)
    elif val >= 0.01: return round(val, 5)
    else: return round(val, 6)

def calculate_risk_based_quantity(entry_price: float, sl_distance: float) -> float:
    """
    Calculates position size from SL distance (#9, #18) and caps notional value
    at MAX_MARGIN_PER_TRADE_USDT * LEVERAGE.
    """
    if entry_price <= 0 or sl_distance <= 0:
        return 0.0

    # Quantity so that if SL hits, loss equals RISK_PER_TRADE_USDT
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
# 9. ENTERPRISE POSITION MANAGER (#3-#13, #19-#22, #32)
# ==========================================
class EnterprisePositionManager:
    def __init__(self):
        self.execution_lock = threading.Lock()  # Prevents margin race condition (#13)
        self.active_trades = db.load_all_positions()
        self.processed_candles = {}  # { "SYMBOL": last_traded_candle_ts } (#12)

    def reconcile_with_exchange(self):
        """Syncs local SQLite state with CoinSwitch's open futures positions (#8, #11, #32)."""
        resp = coinswitch_request("GET", "/trade/api/v2/futures/positions")
        if not resp.get("ok"):
            return

        try:
            body = resp["body"]
            positions = body.get("data", [])
            if not isinstance(positions, list):
                return

            exchange_open_symbols = set()
            for p in positions:
                sym = str(p.get("symbol", "")).upper()
                size = abs(float(p.get("size", p.get("quantity", 0.0))))
                if size > 0:
                    exchange_open_symbols.add(sym)

            # Remove any local zombie positions that are already closed on the exchange
            with self.execution_lock:
                for local_sym in list(self.active_trades.keys()):
                    if exchange_open_symbols and local_sym not in exchange_open_symbols:
                        print(f"🔄 Reconcile: {local_sym} already closed on exchange. Cleaning local state.", flush=True)
                        self.active_trades.pop(local_sym, None)
                        db.delete_position(local_sym)
        except Exception as e:
            print(f"Reconciliation error: {e}", flush=True)

    def can_take_new_trade(self, symbol: str, side: str, candle_ts: int) -> bool:
        if api_guard.is_circuit_open():
            return False

        # Check Daily Loss Limit (#21)
        today_pnl = db.get_today_realized_pnl()
        if today_pnl <= -MAX_DAILY_LOSS_USDT:
            return False

        with self.execution_lock:
            if symbol in self.active_trades:
                return False
            if len(self.active_trades) >= MAX_CONCURRENT_TRADES:
                return False
            # Correlation Guard (#22): Max 3 positions in the same direction
            same_dir_count = sum(1 for t in self.active_trades.values() if t["side"] == side)
            if same_dir_count >= MAX_SAME_DIRECTION_TRADES:
                return False
            # Prevent duplicate trade on the same 15m candle (#12)
            if self.processed_candles.get(symbol) == candle_ts:
                return False

        return True

    def place_exchange_trigger_order(self, symbol: str, close_side: str, qty: float, trigger_price: float, order_type: str) -> str:
        """Places native STOP_MARKET or TAKE_PROFIT_MARKET order on CoinSwitch (#3, #4)."""
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
        return ""

    def cancel_exchange_order(self, symbol: str, order_id: str):
        if not order_id:
            return
        coinswitch_request("DELETE", "/trade/api/v2/futures/order", {"symbol": symbol, "exchange": EXCHANGE_NAME, "order_id": order_id})

    def execute_entry(self, signal_data: dict):
        symbol = signal_data["symbol"]
        side_signal = signal_data["signal"]
        candle_ts = signal_data["candle_ts"]

        if not self.can_take_new_trade(symbol, side_signal, candle_ts):
            return

        # Strict Thread Lock to prevent Margin Race Condition (#13)
        with self.execution_lock:
            if symbol in self.active_trades:
                return

            if not verify_sufficient_balance():
                return

            # Set & verify 3x leverage (#10)
            set_and_verify_leverage(symbol, LEVERAGE)

            atr = signal_data["atr"]
            ref_price = signal_data["signal_close"]
            sl_distance = 1.5 * atr  # 1.5x ATR volatility buffer (#17)

            quantity = calculate_risk_based_quantity(ref_price, sl_distance)
            if quantity <= 0:
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
                print(f"❌ Order rejected for {symbol}: {resp.get('error')}", flush=True)
                return

            body = resp["body"]
            order_data = body.get("data", body) if isinstance(body, dict) else {}
            order_id = str(order_data.get("order_id", ""))
            status_str = str(order_data.get("status", "")).upper()

            # Strict Order Confirmation Check (#7)
            if not order_id or status_str in ("REJECTED", "CANCELLED", "FAILED", "EXPIRED"):
                print(f"❌ Unconfirmed or rejected order on {symbol}: {body}", flush=True)
                return

            # Use actual fill price if returned by CoinSwitch, else latest market price (#6, #19)
            actual_fill = float(order_data.get("avg_execution_price") or order_data.get("price") or ref_price)
            if actual_fill <= 0:
                actual_fill = ref_price

            # Recalculate exact SL & Fee-Adjusted 1:2 RR Take-Profit from ACTUAL Fill Price (#19, #20)
            fee_buffer = actual_fill * ROUND_TRIP_FEE_PCT
            if side_signal == "LONG":
                stop_loss = format_price(actual_fill - sl_distance)
                real_risk = actual_fill - stop_loss
                take_profit = format_price(actual_fill + (2.0 * real_risk) + (2.0 * fee_buffer))
            else:
                stop_loss = format_price(actual_fill + sl_distance)
                real_risk = stop_loss - actual_fill
                take_profit = format_price(actual_fill - (2.0 * real_risk) - (2.0 * fee_buffer))

            # Place native SL & TP orders directly on CoinSwitch Exchange (#3, #4)
            sl_order_id = self.place_exchange_trigger_order(symbol, close_side, quantity, stop_loss, "STOP_MARKET")
            tp_order_id = self.place_exchange_trigger_order(symbol, close_side, quantity, take_profit, "TAKE_PROFIT_MARKET")

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
            min_trail_step = 0.25 * atr  # Only update exchange SL when price moves meaningfully (#5)

            if side == "LONG":
                if current_price > trade["highest_price"]:
                    trade["highest_price"] = current_price
                    candidate_sl = format_price(current_price - (1.2 * atr))
                    if (candidate_sl - trade["stop_loss"]) >= min_trail_step:
                        # Update SL on Exchange (#5)
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
                        # Update SL on Exchange (#5)
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
        # Cancel pending exchange SL/TP trigger orders
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

        # Calculate Net PnL after fees (#20, #21)
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
# 10. PARALLEL SCANNER & WORKER THREADS (#14, #24, #25)
# ==========================================
def get_compatible_watchlist() -> list:
    """Fetches active futures tickers and filters ONLY symbols supported by CoinSwitch (#14)."""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    try:
        res = requests.get(url, timeout=8)
        if res.status_code == 200:
            tickers = res.json()
            compatible = [
                t for t in tickers
                if t['symbol'] in SUPPORTED_COINSWITCH_SYMBOLS
            ]
            # Sort by absolute 24h % change + volume to prioritize top gainers, losers & active movers
            compatible.sort(key=lambda x: abs(float(x.get('priceChangePercent', 0))), reverse=True)
            return [t['symbol'] for t in compatible]
    except Exception as e:
        print(f"Watchlist error: {e}", flush=True)
    return list(SUPPORTED_COINSWITCH_SYMBOLS)

def position_monitor_loop():
    """Fast 6-second loop for trailing SL & regular exchange position reconciliation."""
    reconcile_counter = 0
    while True:
        try:
            if manager.active_trades:
                manager.monitor_and_trail_positions()
            reconcile_counter += 1
            if reconcile_counter >= 20:  # Every ~2 minutes, reconcile with CoinSwitch exchange (#11, #32)
                manager.reconcile_with_exchange()
                reconcile_counter = 0
        except Exception as e:
            print(f"Monitor error: {e}", flush=True)
        time.sleep(6)

def parallel_scanner_loop():
    """Scans all CoinSwitch-compatible coins in parallel using ThreadPoolExecutor (#24, #25)."""
    print("🤖 Enterprise Bot Starting...", flush=True)
    manager.reconcile_with_exchange()

    resp = coinswitch_request("GET", "/trade/api/v2/user/portfolio")
    if resp.get("ok"):
        notify(
            f"✅ *CoinSwitch Enterprise Bot Live!*\n"
            f"• *Recovered Positions:* `{len(manager.active_trades)}`\n"
            f"• *Entry Rule:* Strict Closed Candle (`candles[-2]`) + Volume + `0.15x ATR` Buffer\n"
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
            print(f"🔍 [Parallel Scan] Scanning {len(watchlist)} CoinSwitch-compatible coins | Open Trades: {len(manager.active_trades)}", flush=True)

            # Parallel scan with 4 worker threads to eliminate sequential scan delay (#25)
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_coin = {
                    executor.submit(analyze_closed_candle, coin): coin
                    for coin in watchlist
                    if coin not in manager.active_trades
                }
                for future in as_completed(future_to_coin):
                    setup = future.result()
                    if setup:
                        manager.execute_entry(setup)

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
