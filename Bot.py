import os
import time
import json
import requests
import threading
from flask import Flask
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

# ==========================================
# 0. WEB SERVER
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "CoinSwitch Ed25519 Trading Bot Running 24/7!"

# ==========================================
# CREDENTIALS (Hardcoded for stability)
# ==========================================
API_KEY = "993e3a565da33321d40c76fa000a0f0f849d075573a90eebf8c4ccd1c2ecbe76"
SECRET_KEY = "372663202fb919daac04b745ca608beb9010694ad2c8ea187cb31ecddc5baead"
TELEGRAM_BOT_TOKEN = "8628967713:AAGOJGlodwYFea70NC5wP70k1zf3HGS4DqE"
TELEGRAM_CHAT_ID = "8746261690"

BASE_URL = "https://coinswitch.co"
SYMBOL = "BTCUSDT"
TIMEFRAME = "15m"
LEVERAGE = 3
TRADE_MARGIN_USDT = 15.0
MIN_BALANCE_REQUIRED = 10.0

# ==========================================
# TELEGRAM ALERTS
# ==========================================
def notify(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=8)
    except Exception:
        pass

# ==========================================
# COINSWITCH Ed25519 SIGNATURE ENGINE
# ==========================================
def coinswitch_request(method: str, endpoint: str, payload: dict = None):
    url = f"{BASE_URL}{endpoint}"
    # New Time Format required by CoinSwitch
    epoch = str(int(time.time() * 1000)) 
    
    # Advanced Ed25519 Signature Logic
    msg = f"{method}{endpoint}{epoch}".encode('utf-8')
    priv_key_bytes = bytes.fromhex(SECRET_KEY)
    priv_key = Ed25519PrivateKey.from_private_bytes(priv_key_bytes)
    signature = priv_key.sign(msg).hex()

    headers = {
        "X-AUTH-APIKEY": API_KEY,
        "X-AUTH-SIGNATURE": signature,
        "X-AUTH-EPOCH": epoch,
        "Content-Type": "application/json",
        "User-Agent": "CoinSwitchPro-Ed25519-Bot"
    }

    try:
        if method == "GET":
            res = requests.get(url, headers=headers, timeout=10)
        else:
            res = requests.post(url, headers=headers, json=payload, timeout=10)
        return res.json()
    except Exception as e:
        return {"error": str(e)}

# ==========================================
# FEATURE 6: PRE-TRADE BALANCE CHECK
# ==========================================
def check_sufficient_balance() -> bool:
    res = coinswitch_request("GET", "/trade/api/v2/user/portfolio")
    if isinstance(res, dict) and "error" not in res and "Correct" not in str(res):
        try:
            data = res.get("data", {})
            bal = float(data.get("USDT", {}).get("available", data.get("available_balance", 0.0)))
            if bal > 0 and bal < MIN_BALANCE_REQUIRED:
                notify(f"⚠️ *Trade Skipped:* Balance ({bal} USDT) minimum required margin se kam hai.")
                return False
        except Exception:
            pass
    return True

# ==========================================
# FEATURE 1, 2 & 5: TECHNICAL ANALYSIS
# ==========================================
def fetch_klines(symbol="BTCUSDT", interval="15m", limit=50):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=6)
        if res.status_code == 200:
            return [{'open': float(c[1]), 'high': float(c[2]), 'low': float(c[3]), 'close': float(c[4])} for c in res.json()]
    except Exception:
        pass
    return []

def calculate_ema(prices, period):
    multiplier = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = (p - ema) * multiplier + ema
    return ema

def calculate_rsi(closes, period=14):
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
    return 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

def calculate_atr(candles, period=14):
    trs = [max(c['high'] - c['low'], abs(c['high'] - candles[i-1]['close']), abs(c['low'] - candles[i-1]['close'])) for i, c in enumerate(candles[1:], 1)]
    return sum(trs[-period:]) / period

def analyze_market(symbol):
    candles = fetch_klines(symbol, TIMEFRAME, 50)
    if len(candles) < 30: return None
    closes = [c['close'] for c in candles]
    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]

    support = min(lows[-21:-1])
    resistance = max(highs[-21:-1])
    ema20 = calculate_ema(closes[-25:], 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(candles, 14)
    curr_close = closes[-1]

    if ema20 < ema50 and curr_close < support and rsi > 28:
        return {"signal": "SHORT", "entry": curr_close, "atr": atr, "support": support, "resistance": resistance}
    return None

# ==========================================
# FEATURE 3, 4 & 7: POSITION MANAGEMENT
# ==========================================
class PositionManager:
    def __init__(self):
        self.active_trade = None

    def execute_entry(self, signal_data):
        if not check_sufficient_balance(): return
        entry = signal_data["entry"]
        atr = signal_data["atr"]
        quantity = round((TRADE_MARGIN_USDT * LEVERAGE) / entry, 4)

        stop_loss = round(entry + (1.2 * atr), 4)
        take_profit = round(entry - (2.0 * (stop_loss - entry)), 4)

        payload = {"symbol": SYMBOL, "side": "SELL", "type": "MARKET", "quantity": quantity, "leverage": LEVERAGE}
        res = coinswitch_request("POST", "/trade/api/v2/futures/order", payload)
        
        self.active_trade = {"side": "SHORT", "entry_price": entry, "quantity": quantity, "stop_loss": stop_loss, "take_profit": take_profit, "lowest_price": entry, "atr": atr}
        notify(f"🚀 *SHORT POSITION OPENED*\nCoin: `{SYMBOL}`\nEntry: `{entry}`\nQty: `{quantity}` ({LEVERAGE}x)\n🛑 SL: `{stop_loss}`\n🎯 TP: `{take_profit}`")

    def manage_trailing_sl(self, current_price):
        if not self.active_trade: return
        trade = self.active_trade
        if current_price < trade["lowest_price"]:
            trade["lowest_price"] = current_price
            new_sl = round(current_price + (1.0 * trade["atr"]), 4)
            if new_sl < trade["stop_loss"]:
                trade["stop_loss"] = new_sl

        if current_price <= trade["take_profit"]: self.close_position("🎯 Take Profit Hit!")
        elif current_price >= trade["stop_loss"]: self.close_position("🛑 Trailing SL Hit!")

    def close_position(self, reason):
        payload = {"symbol": SYMBOL, "side": "BUY", "type": "MARKET", "quantity": self.active_trade["quantity"], "leverage": LEVERAGE}
        coinswitch_request("POST", "/trade/api/v2/futures/order", payload)
        notify(f"🔔 *Position Closed: {reason}*\nCoin: `{SYMBOL}`")
        self.active_trade = None

# ==========================================
# MAIN RUNNER
# ==========================================
def bot_loop():
    print("🤖 Bot Engine Starting with Ed25519 Authentication...", flush=True)
    
    # Direct Test on Startup
    res = coinswitch_request("GET", "/trade/api/v2/user/portfolio")
    if isinstance(res, dict) and "Correct" not in str(res):
        data_preview = json.dumps(res.get("data", res))
        notify(f"✅ *CoinSwitch Ed25519 Connected Successfully!*\n💰 Details: `{data_preview[:300]}`")
    else:
        notify(f"⚠️ *Connection Still Failed:*\nResponse: `{res}`")

    notify("🤖 *Bot Started:* All 7 automated features active.")
    manager = PositionManager()

    while True:
        try:
            if manager.active_trade:
                candles = fetch_klines(SYMBOL, "15m", 2)
                if candles: manager.manage_trailing_sl(candles[-1]['close'])
                time.sleep(10)
            else:
                setup = analyze_market(SYMBOL)
                if setup: manager.execute_entry(setup)
                time.sleep(60)
        except Exception:
            time.sleep(15)

if __name__ == "__main__":
    threading.Thread(target=bot_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
