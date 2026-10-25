import logging
import math
import os
import sqlite3
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
  raise ValueError(
      "🚨 CRITICAL: API_KEY or SECRET_KEY is missing in Render Environment"
      " Variables! Bot execution stopped."
  )

BASE_URL = "https://coinswitch.co"
EXCHANGE_NAME = "EXCHANGE_2"
TIMEFRAME = "15m"
LEVERAGE = 3

# Risk, Sizing, Fees & Circuit Breaker Limits
MAX_MARGIN_PER_TRADE_USDT = 3.5
RISK_PER_TRADE_USDT = 0.20
MIN_INR_REQUIRED = 150.0
ROUND_TRIP_FEE_PCT = 0.0012
MAX_DAILY_LOSS_USDT = 1.0
MAX_CONCURRENT_TRADES = 2
MAX_SAME_DIRECTION_TRADES = 2
MAX_CONSECUTIVE_ERRORS = 5

# ---------- AGGRESSIVE SIGNAL SETTINGS ----------
BREAKOUT_LOOKBACK = 12
BREAKOUT_BUFFER_ATR = 0.05
VOL_MULT = 0.70
MIN_BODY_ATR = 0.15
RSI_LONG_MIN, RSI_LONG_MAX = 50, 75
RSI_SHORT_MIN, RSI_SHORT_MAX = 25, 50
EDGE_MIN_WINRATE = 0.40
REQUIRE_EMA_TREND = True

# ---------- RECONCILE SETTINGS ----------
RECONCILE_GRACE_SECONDS = 30
MISSING_CONFIRMATIONS = 2
MAX_API_FAILS_BEFORE_ALERT = 3

DB_PATH = "bot_state.db"

SUPPORTED_COINSWITCH_SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "DOTUSDT",
    "MATICUSDT",
    "NEARUSDT",
    "LTCUSDT",
    "UNIUSDT",
    "ATOMUSDT",
    "APTUSDT",
    "ARBUSDT",
    "OPUSDT",
    "SUIUSDT",
    "FILUSDT",
    "INJUSDT",
    "RNDRUSDT",
    "STXUSDT",
    "IMXUSDT",
    "ETCUSDT",
    "AAVEUSDT",
    "GALAUSDT",
    "SANDUSDT",
    "FETUSDT",
    "TIAUSDT",
    "SEIUSDT",
    "ORDIUSDT",
    "WLDUSDT",
    "JUPUSDT",
    "PENDLEUSDT",
    "ENAUSDT",
    "BCHUSDT",
    "TRXUSDT",
    "SHIBUSDT",
    "PEPEUSDT",
    "WIFUSDT",
    "BONKUSDT",
    "1000PEPEUSDT",
    "1000SHIBUSDT",
    "1000BONKUSDT",
    "POLUSDT",
    "RENDERUSDT",
}

# ==========================================
# 1b. DIAGNOSTICS
# ==========================================
balance_state = {"avail": 0.0}
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
      c.execute(
          """
                INSERT OR REPLACE INTO active_positions VALUES (
                    :symbol, :side, :entry_price, :quantity, :stop_loss, :take_profit,
                    :highest_price, :lowest_price, :atr, :entry_order_id, :sl_order_id,
                    :tp_order_id, :candle_timestamp, :opened_at
                )
            """,
          pos,
      )
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

  def record_closed_trade(
      self, symbol, side, entry_price, exit_price, qty, net_pnl, reason
  ):
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with self.db_lock, sqlite3.connect(self.db_path) as conn:
      conn.execute(
          """
                INSERT INTO trade_history (symbol, side, entry_price, exit_price, quantity, net_pnl_usdt, reason, closed_date, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
          (
              symbol,
              side,
              entry_price,
              exit_price,
              qty,
              net_pnl,
              reason,
              today_str,
              int(time.time()),
          ),
      )
      conn.commit()

  def get_today_realized_pnl(self) -> float:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with self.db_lock, sqlite3.connect(self.db_path) as conn:
      row = conn.execute(
          "SELECT SUM(net_pnl_usdt) FROM trade_history WHERE closed_date = ?",
          (today_str,),
      ).fetchone()
      return float(row[0]) if row and row[0] is not None else 0.0


db = StateDatabase()


# ==========================================
# 3. TELEGRAM ALERTS
# ==========================================
def notify(msg: str) -> bool:
  if not TELEGRAM_BOT_TOKEN:
    return False
  url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
  payload = {
      "chat_id": TELEGRAM_CHAT_ID,
      "text": msg,
      "parse_mode": "Markdown",
  }
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
      if (
          count >= MAX_CONSECUTIVE_ERRORS
          and time.time() > self.circuit_tripped_until
      ):
        self.circuit_tripped_until = time.time() + 900
        notify(
            "🚨 *EMERGENCY CIRCUIT BREAKER TRIPPED!*\n"
            f"`{count}` consecutive API errors detected.\n"
            f"Last error: `{err_detail[:120]}`\n"
            "New entries paused for 15 minutes."
        )

  def is_circuit_open(self) -> bool:
    return time.time() < self.circuit_tripped_until


api_guard = APIGuard()


# ==========================================
# 5. COINSWITCH Ed25519 ENGINE
# ==========================================
def coinswitch_request(
    method: str, endpoint: str, payload: dict = None, track_errors: bool = True
) -> dict:
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
      data = res.json()
      api_guard.record_success()
      return {"ok": True, "status": res.status_code, "body": data}
    else:
      err_text = res.text[:200]
      if track_errors:
        api_guard.record_error(f"HTTP {res.status_code}: {err_text}")
      return {"ok": False, "status": res.status_code, "error": err_text}
  except Exception as e:
    if track_errors:
      api_guard.record_error(str(e))
    return {"ok": False, "status": 0, "error": str(e)}


# ==========================================
# 6. BALANCE, LEVERAGE
# ==========================================
USDT_INR_RATE = 92.0
SOFT_BALANCE_PASS = True
_soft_pass_ts = [0.0]


def get_futures_balances():
  resp = coinswitch_request("GET", "/trade/api/v2/futures/wallet_balance")
  if not resp.get("ok"):
    print(f"⚠️ wallet_balance failed: {resp.get('error')}", flush=True)
    return None, None
  body = resp["body"]
  out = {}
  try:
    data = body.get("data", body) if isinstance(body, dict) else {}
    for item in data.get("base_asset_balances", []):
      asset = str(item.get("base_asset", "")).upper()
      bal = item.get("balances") or {}
      out[asset] = safe_float(bal.get("total_available_balance"))
  except Exception as e:
    print(f"wallet_balance parse error: {e}", flush=True)
  return out, body


def get_futures_available_inr():
  out, _ = get_futures_balances()
  if out is None:
    return None
  return out.get("INR", 0.0)


def get_portfolio_balances():
  resp = coinswitch_request(
      "GET", "/trade/api/v2/user/portfolio", track_errors=False
  )
  if not resp.get("ok"):
    return 0.0, 0.0, None
  body = resp["body"]
  inr, usdt = 0.0, 0.0
  try:
    data = body.get("data", body) if isinstance(body, dict) else body
    if isinstance(data, dict):
      data = _pick(data, "balances", "portfolio", "assets") or data
    if isinstance(data, list):
      for it in data:
        if not isinstance(it, dict):
          continue
        cur = str(_pick(it, "currency", "asset", "coin") or "").upper()
        bal = safe_float(_pick(it, "main_balance", "available", "balance"))
        if cur == "INR":
          inr = bal
        elif cur == "USDT":
          usdt = bal
    elif isinstance(data, dict):
      for cur in ("INR", "USDT"):
        v = data.get(cur)
        bal = (
            safe_float(_pick(v, "main_balance", "available", "balance"))
            if isinstance(v, dict)
            else safe_float(v)
        )
        if cur == "INR":
          inr = bal
        else:
          usdt = bal
  except Exception as e:
    print(f"portfolio parse error: {e}", flush=True)
  return inr, usdt, body


def verify_sufficient_balance() -> bool:
  out, body = get_futures_balances()
  if out is None:
    reject("balance_api_fail")
    return False

  f_inr, f_usdt = out.get("INR", 0.0), out.get("USDT", 0.0)
  if f_inr >= MIN_INR_REQUIRED or f_usdt >= 3.0:
    balance_state["avail"] = f_inr / USDT_INR_RATE + f_usdt
    return True

  p_inr, p_usdt, p_body = get_portfolio_balances()
  if p_inr >= MIN_INR_REQUIRED or p_usdt >= 3.0:
    balance_state["avail"] = p_inr / USDT_INR_RATE + p_usdt
    print(
        f"ℹ️ Futures wallet khaali (parsed={out}) par spot portfolio mein"
        f" INR={p_inr}, USDT={p_usdt}. Order try kar raha hoon, exchange margin"
        " decide karega.",
        flush=True,
    )
    return True

  print(
      f"⚠️ Balance low. futures_parsed={out} spot_INR={p_inr}"
      f" spot_USDT={p_usdt}\nFUTURES_RAW={str(body)[:400]}\nPORTFOLIO_RAW={str(p_body)[:400]}",
      flush=True,
  )
  reject("balance_low")
  if SOFT_BALANCE_PASS and (time.time() - _soft_pass_ts[0]) > 900:
    _soft_pass_ts[0] = time.time()
    balance_state["avail"] = MAX_MARGIN_PER_TRADE_USDT / 0.8
    print(
        "⚠️ Balance confirm nahi hua -> ek try kar raha hoon (exchange khud"
        " margin check karega)",
        flush=True,
    )
    return True
  return False


def set_and_verify_leverage(symbol: str, leverage: int) -> bool:
  payload = {
      "symbol": symbol,
      "exchange": EXCHANGE_NAME,
      "leverage": leverage,
  }
  resp = coinswitch_request("POST", "/trade/api/v2/futures/leverage", payload)
  if not resp.get("ok"):
    print(f"⚠️ Leverage set failed {symbol}: {resp.get('error')}", flush=True)
  return resp.get("ok", False)


# ==========================================
# 7. INDICATORS & SIGNAL
# ==========================================
KLINE_INTERVAL_MIN = "15"
CANDLE_MS = 15 * 60 * 1000
MAX_SIGNAL_AGE_SEC = 3600
MAX_AGE_IF_NO_LIVE_PRICE = 1800
MAX_CHASE_ATR = 1.5
SCAN_PAUSE_SEC = 90
_ticker_raw_logged = {"n": 0}
age_log = []
empty_counts = {}
EMPTY_DROP_LIMIT = 2
_kline_throttle_lock = threading.Lock()
_kline_last_call = [0.0]
_kline_raw_logged = {"n": 0}
bad_symbols = {}


def _pick(d: dict, *keys):
  for k in keys:
    if k in d and d[k] is not None:
      return d[k]
  return None


def parse_kline_rows(body) -> list:
  data = body.get("data", body) if isinstance(body, dict) else body
  if isinstance(data, dict):
    data = _pick(data, "klines", "candles", "data", "result") or []
  out = []
  for r in data if isinstance(data, list) else []:
    try:
      if isinstance(r, dict):
        ts = _pick(r, "start_time", "open_time", "timestamp", "t", "time")
        o = _pick(r, "open", "o")
        h = _pick(r, "high", "h")
        l = _pick(r, "low", "l")
        c = _pick(r, "close", "c")
        v = _pick(r, "volume", "v")
      elif isinstance(r, (list, tuple)) and len(r) >= 6:
        ts, o, h, l, c, v = r[:6]
      else:
        continue
      ts = int(float(ts))
      if ts < 10**11:
        ts *= 1000
      out.append({
          "ts": ts,
          "open": float(o),
          "high": float(h),
          "low": float(l),
          "close": float(c),
          "volume": float(v),
      })
    except (TypeError, ValueError):
      continue
  out.sort(key=lambda x: x["ts"])
  dedup = {c["ts"]: c for c in out}
  return [dedup[k] for k in sorted(dedup)]


def fetch_klines(symbol: str, interval=None, limit=110) -> list:
  with _kline_throttle_lock:
    wait = 2.2 - (time.time() - _kline_last_call[0])
    if wait > 0:
      time.sleep(wait)
    _kline_last_call[0] = time.time()

  end_ms = int(time.time() * 1000) + CANDLE_MS
  start_ms = end_ms - (limit + 2) * CANDLE_MS
  endpoint = (
      f"/trade/api/v2/futures/klines?symbol={symbol}&exchange={EXCHANGE_NAME}"
      f"&interval={KLINE_INTERVAL_MIN}&start_time={start_ms}&end_time={end_ms}"
  )
  resp = coinswitch_request("GET", endpoint, track_errors=False)
  if not resp.get("ok"):
    reject(f"klines_http_{resp.get('status')}")
    bad_symbols[symbol] = bad_symbols.get(symbol, 0) + 1
    if kline_error_prints["n"] < 5:
      kline_error_prints["n"] += 1
      print(
          f"⚠️ CoinSwitch klines {symbol} failed: {resp.get('status')}"
          f" {resp.get('error')}",
          flush=True,
      )
    return []

  candles = parse_kline_rows(resp["body"])
  if not candles:
    reject("klines_empty_or_unparsed")
    empty_counts[symbol] = empty_counts.get(symbol, 0) + 1
    if empty_counts[symbol] == EMPTY_DROP_LIMIT:
      print(
          f"🚫 {symbol}: CoinSwitch futures pe data nahi"
          " (symbol naam badla/delist). Skip kar raha hoon.",
          flush=True,
      )
    if _kline_raw_logged["n"] < 3:
      _kline_raw_logged["n"] += 1
      print(
          f"⚠️ Klines parse empty {symbol}. Raw={str(resp['body'])[:300]}",
          flush=True,
      )
    return []
  bad_symbols[symbol] = 0
  empty_counts[symbol] = 0
  return candles


def get_all_prices() -> dict:
  resp = coinswitch_request(
      "GET",
      f"/trade/api/v2/futures/all-pairs/ticker?exchange={EXCHANGE_NAME}",
      track_errors=False,
  )
  prices = {}
  if not resp.get("ok"):
    return prices
  body = resp["body"]
  data = body.get("data", body) if isinstance(body, dict) else body
  items = []
  if isinstance(data, dict):
    for k, v in data.items():
      if isinstance(v, dict):
        items.append((v.get("symbol", k), v))
  elif isinstance(data, list):
    for v in data:
      if isinstance(v, dict):
        items.append((v.get("symbol"), v))
  for sym, v in items:
    px = safe_float(
        _pick(
            v, "last_price", "lastPrice", "price", "close", "c", "mark_price"
        )
    )
    if sym and px > 0:
      prices[normalize_symbol(sym)] = px
  if not prices and _ticker_raw_logged["n"] < 2:
    _ticker_raw_logged["n"] += 1
    print(f"⚠️ Ticker prices parse nahi hue. Raw={str(body)[:400]}", flush=True)
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
    h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
    trs.append(max(h - l, abs(h - pc), abs(l - pc)))

  atr = sum(trs[:period]) / period
  for tr in trs[period:]:
    atr = (atr * (period - 1) + tr) / period
  return atr if atr > 0 else None


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
  candles = fetch_klines(symbol, TIMEFRAME, 110)
  now_ms = int(time.time() * 1000)
  closed_candles = [c for c in candles if c["ts"] + CANDLE_MS <= now_ms]
  if len(closed_candles) < 70:
    reject("no_data")
    return None
  last_closed = closed_candles[-1]
  age_sec = (now_ms - (last_closed["ts"] + CANDLE_MS)) / 1000.0
  if len(age_log) < 3:
    last_utc = datetime.fromtimestamp(
        last_closed["ts"] / 1000, timezone.utc
    ).strftime("%H:%M")
    raw_last = datetime.fromtimestamp(
        candles[-1]["ts"] / 1000, timezone.utc
    ).strftime("%H:%M")
    now_utc = datetime.fromtimestamp(now_ms / 1000, timezone.utc).strftime(
        "%H:%M:%S"
    )
    age_log.append(
        f"{symbol}: now={now_utc}UTC raw_last_candle={raw_last}"
        f" last_closed={last_utc} age={int(age_sec)}s"
    )
  if age_sec > MAX_SIGNAL_AGE_SEC:
    reject("stale_candle")
    return None
  prev_closed = closed_candles[-(BREAKOUT_LOOKBACK + 1) : -1]

  closes = [c["close"] for c in closed_candles]
  volumes = [c["volume"] for c in closed_candles]

  ema20 = calculate_ema(closes, 20)
  ema50 = calculate_ema(closes, 50)
  rsi = calculate_rsi(closes, 14)
  atr = calculate_atr(closed_candles, 14)

  if None in (ema20, ema50, rsi, atr) or atr <= 0:
    reject("indicator_none")
    return None

  support = min(c["low"] for c in prev_closed)
  resistance = max(c["high"] for c in prev_closed)
  avg_vol = sum(volumes[-21:-1]) / 20.0

  confirmed_close = last_closed["close"]
  candle_body = abs(last_closed["close"] - last_closed["open"])
  breakout_buffer = BREAKOUT_BUFFER_ATR * atr

  if last_closed["volume"] < (VOL_MULT * avg_vol):
    reject("low_volume")
    return None
  if candle_body < (MIN_BODY_ATR * atr):
    reject("weak_body")
    return None

  if last_closed["close"] > last_closed["open"]:
    direction = "LONG"
  elif last_closed["close"] < last_closed["open"]:
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
      "candle_ts": last_closed["ts"],
      "age_sec": int(age_sec),
  }


# ==========================================
# 8. PRECISION & RISK-BASED SIZING
# ==========================================
def format_price(val: float) -> float:
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


def calculate_risk_based_quantity(
    entry_price: float, sl_distance: float, margin_cap=None, risk_cap=None
) -> float:
  if entry_price <= 0 or sl_distance <= 0:
    return 0.0

  risk_usdt = risk_cap if risk_cap else RISK_PER_TRADE_USDT
  margin_usdt = margin_cap if margin_cap else MAX_MARGIN_PER_TRADE_USDT
  risk_qty = risk_usdt / sl_distance
  max_notional_qty = (margin_usdt * LEVERAGE) / entry_price
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
      logger.warning(
          "Cancel failed/ignored: %s order=%s", symbol, order_id, exc_info=True
      )

  def reconcile_with_exchange(self):
    snapshot_time = time.time()
    with self.execution_lock:
      local_items = list(self.active_trades.items())
    if not local_items:
      self._reconcile_missing_count.clear()
      return

    orphans = []
    for local_key, trade in local_items:
      opened_at = safe_float(trade.get("opened_at"), 0.0)
      if opened_at and (snapshot_time - opened_at) < RECONCILE_GRACE_SECONDS:
        continue

      resp = coinswitch_request(
          "GET",
          f"/trade/api/v2/futures/positions?exchange={EXCHANGE_NAME}&symbol={local_key}",
          track_errors=False,
      )
      if not resp.get("ok"):
        logger.warning(
            "Reconcile %s skipped: API failed %s", local_key, resp.get("error")
        )
        continue

      body = resp.get("body") or {}
      data = body.get("data") if isinstance(body, dict) else None
      if isinstance(data, dict):
        data = _pick(data, "positions", "data", "list", "result")
      if not isinstance(data, list):
        logger.warning(
            "Reconcile %s skipped: bad payload raw=%s",
            local_key,
            str(body)[:200],
        )
        continue

      size = 0.0
      last_px = 0.0
      for pos in data:
        if (
            isinstance(pos, dict)
            and str(pos.get("status", "OPEN")).upper() == "OPEN"
        ):
          size += abs(
              safe_float(_pick(pos, "position_size", "size", "quantity"))
          )
          last_px = (
              safe_float(_pick(pos, "last_price", "mark_price")) or last_px
          )

      with self.execution_lock:
        if local_key not in self.active_trades:
          continue
        if size > 0:
          self._reconcile_missing_count.pop(local_key, None)
          lq = abs(safe_float(trade.get("quantity")))
          if lq and abs(lq - size) / lq > 0.05:
            logger.warning(
                "Size mismatch %s: local=%s exchange=%s", local_key, lq, size
            )
          continue

        cnt = self._reconcile_missing_count.get(local_key, 0) + 1
        self._reconcile_missing_count[local_key] = cnt
        if cnt < MISSING_CONFIRMATIONS:
          logger.info(
              "Reconcile: %s exchange pe nahi dikha (%s/%s)",
              local_key,
              cnt,
              MISSING_CONFIRMATIONS,
          )
          continue

        logger.warning(
            "Reconcile: %s exchange pe band ho chuka. Local state saaf.",
            local_key,
        )
        closed = self.active_trades.pop(local_key, None)
        self._reconcile_missing_count.pop(local_key, None)
        try:
          db.delete_position(local_key)
        except Exception:
          logger.exception("Reconcile: DB delete failed for %s", local_key)
        orphans.append((local_key, closed))

    if orphans:
      prices = get_all_prices()
    for sym, closed in orphans:
      closed = closed or {}
      self._safe_cancel(sym, closed.get("sl_order_id", ""))
      self._safe_cancel(sym, closed.get("tp_order_id", ""))
      try:
        entry = safe_float(closed.get("entry_price"))
        qty = safe_float(closed.get("quantity"))
        side = closed.get("side", "LONG")
        exit_px = (
            prices.get(normalize_symbol(sym))
            or safe_float(closed.get("stop_loss"))
            or entry
        )
        gross = (
            (exit_px - entry) * qty if side == "LONG" else (entry - exit_px) * qty
        )
        fees = (entry + exit_px) * qty * (ROUND_TRIP_FEE_PCT / 2.0)
        net = round(gross - fees, 3)
        db.record_closed_trade(
            sym, side, entry, exit_px, qty, net, "Closed on exchange (estimated)"
        )
        notify(
            f"🔔 *{sym} exchange pe band hui* (SL/TP hit)\nEstimated Net PnL:"
            f" `{net:+.3f} USDT`\n_(exit price andaza hai, app mein asli check"
            " karo)_"
        )
      except Exception:
        logger.exception("Reconcile: PnL record failed for %s", sym)

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
      same_dir_count = sum(
          1 for t in self.active_trades.values() if t["side"] == side
      )
      if same_dir_count >= MAX_SAME_DIRECTION_TRADES:
        reject("blocked_same_direction")
        return False
      if self.processed_candles.get(symbol) == candle_ts:
        reject("blocked_same_candle")
        return False

    return True

  def place_exchange_trigger_order(
      self,
      symbol: str,
      close_side: str,
      qty: float,
      trigger_price: float,
      order_type: str,
  ) -> str:
    # Safe dual-key mapping: pass both 'type' and 'order_type'
    payload = {
        "exchange": EXCHANGE_NAME,
        "symbol": symbol,
        "side": close_side,
        "type": order_type,
        "order_type": order_type,
        "quantity": 0,
        "trigger_price": trigger_price,
        "reduce_only": True,
    }
    resp = coinswitch_request("POST", "/trade/api/v2/futures/order", payload)
    if resp.get("ok"):
      data = resp["body"].get("data", resp["body"])
      if isinstance(data, dict):
        return str(data.get("order_id", ""))
    else:
      print(
          f"⚠️ {order_type} order failed {symbol}: {resp.get('error')}",
          flush=True,
      )
    return ""

  def cancel_exchange_order(self, symbol: str, order_id: str):
    if not order_id:
      return
    coinswitch_request(
        "DELETE",
        "/trade/api/v2/futures/order",
        {
            "symbol": symbol,
            "exchange": EXCHANGE_NAME,
            "order_id": order_id,
        },
    )

  def execute_entry(self, signal_data: dict):
    symbol = signal_data["symbol"]
    side_signal = signal_data["signal"]
    candle_ts = signal_data["candle_ts"]

    print(
        f"🎯 Signal: {symbol} {side_signal} (RSI {signal_data['rsi']})",
        flush=True,
    )

    if not self.can_take_new_trade(symbol, side_signal, candle_ts):
      print(f"⛔ {symbol} entry blocked by risk rules", flush=True)
      return

    atr_v = signal_data["atr"]
    px = get_all_prices().get(normalize_symbol(symbol))
    if px:
      buf = BREAKOUT_BUFFER_ATR * atr_v
      if side_signal == "LONG":
        level = signal_data["resistance"] + buf
        inside, extended = px <= level, (px - level) > MAX_CHASE_ATR * atr_v
      else:
        level = signal_data["support"] - buf
        inside, extended = px >= level, (level - px) > MAX_CHASE_ATR * atr_v
      if inside:
        reject("price_back_inside")
        print(
            f"⛔ {symbol} {side_signal}: live price {px} breakout level"
            f" {level:.6g} ke andar wapas aa gaya",
            flush=True,
        )
        return
      if extended:
        reject("chased_too_far")
        print(
            f"⛔ {symbol} {side_signal}: price {px} bahut aage nikal chuka"
            f" (>{MAX_CHASE_ATR} ATR), skip",
            flush=True,
        )
        return
      signal_data["signal_close"] = px
    elif signal_data.get("age_sec", 0) > MAX_AGE_IF_NO_LIVE_PRICE:
      reject("no_live_price_and_stale")
      print(f"⛔ {symbol}: live price nahi mila aur candle purani, skip", flush=True)
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

      avail = balance_state["avail"]
      margin_cap = min(MAX_MARGIN_PER_TRADE_USDT, avail * 0.8)
      risk_cap = min(RISK_PER_TRADE_USDT, avail * 0.05)
      quantity = calculate_risk_based_quantity(
          ref_price, sl_distance, margin_cap, risk_cap
      )
      if quantity <= 0:
        reject("qty_zero")
        print(
            f"⛔ {symbol} qty=0 (price {ref_price} bahut high / margin cap"
            " chhota). MAX_MARGIN_PER_TRADE_USDT badhao ya is coin ko skip"
            " maano.",
            flush=True,
        )
        return

      order_side = "BUY" if side_signal == "LONG" else "SELL"
      close_side = "SELL" if side_signal == "LONG" else "BUY"
      client_oid = str(uuid.uuid4())

      # FIX: Added both 'type' and 'order_type' so CoinSwitch accepts seamlessly
      entry_payload = {
          "symbol": symbol,
          "exchange": EXCHANGE_NAME,
          "side": order_side,
          "type": "MARKET",
          "order_type": "MARKET",
          "quantity": quantity,
          "client_order_id": client_oid,
      }

      resp = coinswitch_request(
          "POST", "/trade/api/v2/futures/order", entry_payload
      )
      if not resp.get("ok"):
        reject("order_rejected")
        self.processed_candles[symbol] = candle_ts
        print(f"❌ Order rejected for {symbol}: {resp.get('error')}", flush=True)
        return

      body = resp["body"]
      order_data = body.get("data", body) if isinstance(body, dict) else {}
      order_id = str(order_data.get("order_id", ""))
      status_str = str(order_data.get("status", "")).upper()

      if not order_id or status_str in (
          "REJECTED",
          "CANCELLED",
          "FAILED",
          "EXPIRED",
      ):
        reject("order_unconfirmed")
        self.processed_candles[symbol] = candle_ts
        print(
            f"❌ Unconfirmed or rejected order on {symbol}: {body}", flush=True
        )
        return

      actual_fill = 0.0
      final_status = status_str
      for _ in range(5):
        time.sleep(1.0)
        st = coinswitch_request(
            "GET",
            f"/trade/api/v2/futures/order?order_id={order_id}",
            track_errors=False,
        )
        if not st.get("ok"):
          continue
        sb = (
            st["body"].get("data", st["body"])
            if isinstance(st["body"], dict)
            else {}
        )
        o = sb.get("order", sb) if isinstance(sb, dict) else {}
        final_status = str(o.get("status", "")).upper()
        avg = safe_float(o.get("avg_execution_price"))
        exq = safe_float(o.get("exec_quantity"))
        if avg > 0:
          actual_fill = avg
        if final_status in ("EXECUTED", "PARTIALLY_EXECUTED"):
          if final_status == "PARTIALLY_EXECUTED" and exq > 0:
            quantity = exq
          break
        if final_status in ("CANCELLED", "REJECTED", "FAILED", "EXPIRED"):
          break
      if final_status in ("CANCELLED", "REJECTED", "FAILED", "EXPIRED"):
        reject("entry_not_filled")
        self.processed_candles[symbol] = candle_ts
        print(
            f"❌ {symbol} entry fill nahi hui, status={final_status}", flush=True
        )
        return
      if actual_fill <= 0:
        actual_fill = ref_price
      if final_status not in ("EXECUTED", "PARTIALLY_EXECUTED"):
        notify(
            f"⚠️ `{symbol}` order status `{final_status}` (confirm nahi hua)."
            " CoinSwitch app mein position check karo."
        )

      fee_buffer = actual_fill * ROUND_TRIP_FEE_PCT
      if side_signal == "LONG":
        stop_loss = format_price(actual_fill - sl_distance)
        real_risk = actual_fill - stop_loss
        take_profit = format_price(
            actual_fill + (2.0 * real_risk) + (2.0 * fee_buffer)
        )
      else:
        stop_loss = format_price(actual_fill + sl_distance)
        real_risk = stop_loss - actual_fill
        take_profit = format_price(
            actual_fill - (2.0 * real_risk) - (2.0 * fee_buffer)
        )

      sl_order_id = self.place_exchange_trigger_order(
          symbol, close_side, quantity, stop_loss, "STOP_MARKET"
      )
      tp_order_id = self.place_exchange_trigger_order(
          symbol, close_side, quantity, take_profit, "TAKE_PROFIT_MARKET"
      )

      if not sl_order_id:
        notify(
            f"🚨 *WARNING:* `{symbol}` ka exchange SL place NAHI hua! Position"
            " sirf bot ke software SL pe chal rahi hai. Manually check karo."
        )

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
          "opened_at": int(time.time()),
      }

      self.active_trades[symbol] = pos_record
      self.processed_candles[symbol] = candle_ts
      db.save_position(pos_record)
      open_count = len(self.active_trades)

    emoji = (
        "🟢 *CONFIRMED LONG OPENED*"
        if side_signal == "LONG"
        else "🔴 *CONFIRMED SHORT OPENED*"
    )
    notify(
        f"{emoji} (`{open_count}/{MAX_CONCURRENT_TRADES}`)\nCoin: `{symbol}` |"
        f" Order ID: `{order_id[:10]}`\nActual Fill: `{actual_fill}` | Qty:"
        f" `{quantity}` ({LEVERAGE}x)\n🛑 Exchange SL (1.5x ATR):"
        f" `{stop_loss}`\n🎯 Net 1:2 TP (Fee-Adjusted): `{take_profit}`\n📊"
        f" Closed Candle RSI: `{signal_data['rsi']}` | Signal age:"
        f" `{signal_data.get('age_sec', '?')}s`"
    )

  def monitor_and_trail_positions(self):
    with self.execution_lock:
      symbols = list(self.active_trades.keys())

    prices = get_all_prices()
    for sym in symbols:
      current_price = prices.get(normalize_symbol(sym))
      if not current_price:
        candles = fetch_klines(sym, TIMEFRAME, 3)
        if not candles:
          continue
        if candles[-1]["ts"] < int(time.time() * 1000) - 2 * CANDLE_MS:
          continue
        current_price = candles[-1]["close"]
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
            new_sl_id = self.place_exchange_trigger_order(
                symbol, close_side, trade["quantity"], candidate_sl, "STOP_MARKET"
            )
            trade["stop_loss"] = candidate_sl
            trade["sl_order_id"] = new_sl_id
            db.save_position(trade)

        if current_price >= trade["take_profit"]:
          self._close_position_locked(
              symbol, "🎯 Take Profit Hit!", current_price
          )
        elif current_price <= trade["stop_loss"]:
          self._close_position_locked(
              symbol, "🛑 Trailing SL Hit!", current_price
          )

      elif side == "SHORT":
        if current_price < trade["lowest_price"]:
          trade["lowest_price"] = current_price
          candidate_sl = format_price(current_price + (1.2 * atr))
          if (trade["stop_loss"] - candidate_sl) >= min_trail_step:
            self.cancel_exchange_order(symbol, trade.get("sl_order_id", ""))
            new_sl_id = self.place_exchange_trigger_order(
                symbol, close_side, trade["quantity"], candidate_sl, "STOP_MARKET"
            )
            trade["stop_loss"] = candidate_sl
            trade["sl_order_id"] = new_sl_id
            db.save_position(trade)

        if current_price <= trade["take_profit"]:
          self._close_position_locked(
              symbol, "🎯 Take Profit Hit!", current_price
          )
        elif current_price >= trade["stop_loss"]:
          self._close_position_locked(
              symbol, "🛑 Trailing SL Hit!", current_price
          )

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
        "order_type": "MARKET",
        "quantity": trade["quantity"],
        "reduce_only": True,
    }
    coinswitch_request("POST", "/trade/api/v2/futures/order", close_payload)

    entry = trade["entry_price"]
    qty = trade["quantity"]
    gross_pnl = (
        (exit_price - entry) * qty
        if trade["side"] == "LONG"
        else (entry - exit_price) * qty
    )
    fees = (entry + exit_price) * qty * (ROUND_TRIP_FEE_PCT / 2.0)
    net_pnl = round(gross_pnl - fees, 3)

    db.delete_position(symbol)
    db.record_closed_trade(
        symbol, trade["side"], entry, exit_price, qty, net_pnl, reason
    )
    today_total = round(db.get_today_realized_pnl(), 2)

    notify(
        f"🔔 *Position Closed: {reason}*\nCoin: `{symbol}`"
        f" (`{trade['side']}`)\nEntry: `{entry}` | Exit: `{exit_price}`\n💵 Net"
        f" PnL (After Fees): `{net_pnl:+.2f} USDT`\n📅 Today's Cumulative PnL:"
        f" `{today_total:+.2f} USDT`"
    )


manager = EnterprisePositionManager()


# ==========================================
# 10. PARALLEL SCANNER & WORKER THREADS
# ==========================================
def get_compatible_watchlist() -> list:
  return [
      s
      for s in sorted(SUPPORTED_COINSWITCH_SYMBOLS)
      if bad_symbols.get(s, 0) < 5 and empty_counts.get(s, 0) < EMPTY_DROP_LIMIT
  ]


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
      print(f"Monitor error: {e}", flush=True)
    time.sleep(10)


def parallel_scanner_loop():
  print("🤖 Enterprise Bot Starting (AGGRESSIVE, CoinSwitch data)...", flush=True)
  manager.reconcile_with_exchange()

  avail = get_futures_available_inr()
  if avail is not None:
    notify(
        "✅ *CoinSwitch Bot Live (Aggressive)!*\n"
        f"• *Futures INR available:* `₹{avail}`\n"
        f"• *Recovered Positions:* `{len(manager.active_trades)}`\n"
        "• *Data:* CoinSwitch klines (15m) | Scan har ~3 min (candle data late"
        " aata hai)"
    )
  else:
    notify(
        "⚠️ *Startup Notice:* futures wallet_balance fetch fail hua. Logs check"
        " karo."
    )

  try:
    f_out, f_body = get_futures_balances()
    p_inr, p_usdt, p_body = get_portfolio_balances()
    print(f"🔎 STARTUP FUTURES_RAW={str(f_body)[:600]}", flush=True)
    print(f"🔎 STARTUP PORTFOLIO_RAW={str(p_body)[:600]}", flush=True)
    notify(
        f"🔎 *Balance debug*\nFutures wallet: `{f_out}`\nSpot INR: `{p_inr}` |"
        f" Spot USDT: `{p_usdt}`"
    )
  except Exception as e:
    print(f"Balance debug error: {e}", flush=True)

  while True:
    try:
      if api_guard.is_circuit_open():
        time.sleep(30)
        continue

      watchlist = [
          c
          for c in get_compatible_watchlist()
          if c not in manager.active_trades
      ]
      print(
          f"🔍 [Scan] {len(watchlist)} coins | Open Trades:"
          f" {len(manager.active_trades)}",
          flush=True,
      )

      with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_coin = {
            executor.submit(analyze_closed_candle, coin): coin
            for coin in watchlist
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

      stats = pop_reject_stats()
      print(f"📊 Scan summary (reject reasons): {stats}", flush=True)
      print(f"🕒 Candle age samples: {list(age_log)}", flush=True)
      age_log.clear()

      bad = stats.get("no_data", 0) + stats.get("klines_empty_or_unparsed", 0)
      if watchlist and bad > 0.5 * len(watchlist):
        print("⚠️ Zyadatar coins ka data nahi aaya, 60s baad retry", flush=True)
        time.sleep(60)
        continue
      time.sleep(SCAN_PAUSE_SEC)
    except Exception as e:
      print(f"Scanner loop error: {e}", flush=True)
      time.sleep(15)


if __name__ == "__main__":
  threading.Thread(target=keep_alive_pinger, daemon=True).start()
  threading.Thread(target=position_monitor_loop, daemon=True).start()
  threading.Thread(target=parallel_scanner_loop, daemon=True).start()

  port = int(os.environ.get("PORT", 10000))
  app.run(host="0.0.0.0", port=port)
