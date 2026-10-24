import os
import sys
import time
import json
import hmac
import hashlib
import requests
import threading
from flask import Flask

# ==========================================
# 0. WEB SERVER (Render Free Tier 24/7)
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "CoinSwitch Automated Trading Bot is Running 24/7!"

# ==========================================
# CONFIGURATION & CREDENTIALS (AUTO-CLEANED)
# ==========================================
# Automatically removes accidental spaces, newlines or 'key-' text from mobile copy-paste
API_KEY = os.getenv("COINSWITCH_API_KEY", "").replace("key-", "").replace("\n", "").strip()
SECRET_KEY = os.getenv("COINSWITCH_SECRET_KEY", "").replace("\n", "").strip()

TELEGRAM_BOT_TOKEN = "8628967713:AAGOJGlodwYFea70NC5wP70k1zf3HGS4DqE"
TELEGRAM_CHAT_ID = "8746261690"

BASE_URL = "https://coinswitch.co"
SYMBOL = "BTCUSDT"
TIMEFRAME = "15m"
LEVERAGE = 3
TRADE_MARGIN_USDT = 15.0
MIN_BALANCE_REQUIRED = 10.0

# Global format lock for Auto-Tuner
WORKING_SIG_FORMAT = 1  
WORKING_ENDPOINT = "/trade/api/v2/user/portfolio"

# ==========================================
# TELEGRAM ALERTS
# ==========================================
def notify(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=8)
    except Exception:
        pass

# ==========================================
# COINSWITCH API & SIGNATURE TUNER
# ==========================================
def generate_signature(method: str, endpoint: str, payload_str: str, timestamp: int, sig_format: int) -> str:
    # Format 1: Standard
    # Format 2: Forces "{}" for empty GET requests (Common in Indian Exchanges)
    p_str = payload_str
    if sig_format == 2 and not payload_str:
        p_str = "{}"
        
    message = f"{method}{endpoint}{p_str}{timestamp}"
    return hmac.new(SECRET_KEY.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).hexdigest()

def coinswitch_request(method: str, endpoint: str, payload: dict = None, custom_format: int = None):
    url = f"{BASE_URL}{endpoint}"
    timestamp = int(time.time() * 1000)
    payload_str = json.dumps(payload, separators=(',', ':')) if payload else ""
    
    # Use specified format or fallback to the auto-tuned global format
    sig_fmt = custom_format if custom_format else WORKING_SIG_FORMAT
    signature = generate_signature(method, endpoint, payload_str, timestamp, sig_fmt)

    headers = {
        "X-AUTH-APIKEY": API_KEY,
        "X-AUTH-SIGNATURE": signature,
        "X-AUTH-TIMESTAMP": str(timestamp),
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }

    try:
        if method == "GET":
            res = requests.get(url, headers=headers, timeout=10)
        else:
            res = requests.post(url, headers=headers, data=payload_str, timeout=10)
        return res.json()
    except Exception as e:
        return {"error": str(e)}

# ==========================================
# FEATURE 6: PRE-TRADE BALANCE CHECK
# ==========================================
def check_sufficient_balance() -> bool:
    res = coinswitch_request("GET", WORKING_ENDPOINT)
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

    print(f"📊 [Scan BTC] Price: {curr_close} | Support: {support} | EMA20: {round(ema20, 1)} | RSI: {round(rsi, 1)}", flush=True)

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
        print(f"Order Result: {res}", flush=True)

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
# MAIN RUNNER & AUTO-TUNER
# ==========================================
def bot_loop():
    global WORKING_SIG_FORMAT, WORKING_ENDPOINT
    print("🤖 Bot Engine Starting...", flush=True)
    
    # 1. AUTO-TUNER: Find the exact format CoinSwitch expects right now
    endpoints_to_test = ["/trade/api/v2/user/portfolio", "/trade/api/v2/portfolio/balances"]
    formats_to_test = [1, 2] # 1: Blank payload, 2: "{}" payload
    
    success = False
    best_res = None
    
    for ep in endpoints_to_test:
        for fmt in formats_to_test:
            res = coinswitch_request("GET", ep, custom_format=fmt)
            res_str = str(res)
            # If the response doesn't yell about keys or 404, we cracked it!
            if "Correct" not in res_str and "Not Found" not in res_str and "error" not in res:
                WORKING_SIG_FORMAT = fmt
                WORKING_ENDPOINT = ep
                success = True
                best_res = res
                break
        if success: break
            
    if success:
        data_preview = json.dumps(best_res.get("data", best_res))
        notify(f"✅ *CoinSwitch Auto-Tuned & Connected!*\nFormat Locked: `Type {WORKING_SIG_FORMAT}`\n💰 Details: `{data_preview[:250]}`")
    else:
        notify(f"⚠️ *Keys Still Rejected after Auto-Clean:*\nCheck if CoinSwitch Pro API requires IP Whitelisting.\nResponse: `{best_res}`")

    # 2. Start Scanner
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
        except Exception as e:
            time.sleep(15)

if __name__ == "__main__":
    worker = threading.Thread(target=bot_loop, daemon=True)
    worker.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
