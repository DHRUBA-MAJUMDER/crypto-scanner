import os
import time
import json
import requests
import threading
from flask import Flask
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

# ==========================================
# 0. WEB SERVER (Render 24/7 Keep-Alive)
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "CoinSwitch Ed25519 Dual-Direction (Long/Short) Bot Running 24/7!"

# ==========================================
# CREDENTIALS & CONFIG
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
    except Exception as e:
        print(f"Telegram Error: {e}", flush=True)

# ==========================================
# COINSWITCH Ed25519 SIGNATURE ENGINE
# ==========================================
def coinswitch_request(method: str, endpoint: str, payload: dict = None):
    url = f"{BASE_URL}{endpoint}"
    epoch = str(int(time.time() * 1000))
    
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
# PRE-TRADE BALANCE CHECK
# ==========================================
def check_sufficient_balance() -> bool:
    res = coinswitch_request("GET", "/trade/api/v2/user/portfolio")
    if isinstance(res, dict) and "error" not in res and "Correct" not in str(res):
        try:
            data = res.get("data", {})
            if isinstance(data, dict):
                bal = float(data.get("USDT", {}).get("available", data.get("available_balance", 0.0)))
                if bal > 0 and bal < MIN_BALANCE_REQUIRED:
                    notify(f"⚠️ *Trade Skipped:* Available USDT ({bal}) minimum required ({MIN_BALANCE_REQUIRED} USDT) se kam hai.")
                    return False
        except Exception:
            pass
    return True

# ==========================================
# ACCURATE TECHNICAL ANALYSIS & INDICATORS
# ==========================================
def fetch_klines(symbol="BTCUSDT", interval="15m", limit=100):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=6)
        if res.status_code == 200:
            return [
                {'open': float(c[1]), 'high': float(c[2]), 'low': float(c[3]), 'close': float(c[4])}
                for c in res.json()
            ]
    except Exception as e:
        print(f"Kline error: {e}", flush=True)
    return []

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1]
    # Seed with Simple Moving Average (SMA) of first 'period' candles for 100% accuracy
    sma = sum(prices[:period]) / period
    multiplier = 2 / (period + 1)
    ema = sma
    for p in prices[period:]:
        ema = (p - ema) * multiplier + ema
    return ema

def calculate_rsi(closes, period=14):
    if len(closes) <= period:
        return 50.0
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

def calculate_atr(candles, period=14):
    trs = []
    for i in range(1, len(candles)):
        high = candles[i]['high']
        low = candles[i]['low']
        prev_close = candles[i - 1]['close']
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        
    # Wilder's Smoothing for ATR
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr

def analyze_market(symbol):
    candles = fetch_klines(symbol, TIMEFRAME, 100)
    if len(candles) < 60:
        return None

    closes = [c['close'] for c in candles]
    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]

    # Support & Resistance of last 20 completed candles (excluding current forming candle)
    support = min(lows[-21:-1])
    resistance = max(highs[-21:-1])

    # Technical Indicators on full series
    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(candles, 14)
    curr_close = closes[-1]

    print(
        f"📊 [Scan {symbol}] Price: {curr_close} | S: {support} | R: {resistance} | "
        f"EMA20: {round(ema20, 2)} | EMA50: {round(ema50, 2)} | RSI: {round(rsi, 2)} | ATR: {round(atr, 2)}",
        flush=True
    )

    # 1. LONG SIGNAL: Uptrend + Resistance Breakout + RSI Room to grow (< 72)
    if ema20 > ema50 and curr_close > resistance and rsi < 72:
        return {
            "signal": "LONG",
            "entry": curr_close,
            "atr": atr,
            "support": support,
            "resistance": resistance,
            "rsi": round(rsi, 2)
        }

    # 2. SHORT SIGNAL: Downtrend + Support Breakdown + RSI Room to fall (> 28)
    if ema20 < ema50 and curr_close < support and rsi > 28:
        return {
            "signal": "SHORT",
            "entry": curr_close,
            "atr": atr,
            "support": support,
            "resistance": resistance,
            "rsi": round(rsi, 2)
        }

    return None

# ==========================================
# POSITION & DUAL TRAILING STOP-LOSS MANAGER
# ==========================================
class PositionManager:
    def __init__(self):
        self.active_trade = None

    def execute_entry(self, signal_data):
        if not check_sufficient_balance():
            return

        side_signal = signal_data["signal"] # "LONG" or "SHORT"
        entry = signal_data["entry"]
        atr = signal_data["atr"]
        quantity = round((TRADE_MARGIN_USDT * LEVERAGE) / entry, 4)

        if side_signal == "LONG":
            order_side = "BUY"
            stop_loss = round(entry - (1.2 * atr), 2)
            risk = entry - stop_loss
            take_profit = round(entry + (2.0 * risk), 2)
        else:
            order_side = "SELL"
            stop_loss = round(entry + (1.2 * atr), 2)
            risk = stop_loss - entry
            take_profit = round(entry - (2.0 * risk), 2)

        payload = {
            "symbol": SYMBOL,
            "side": order_side,
            "type": "MARKET",
            "quantity": quantity,
            "leverage": LEVERAGE
        }
        res = coinswitch_request("POST", "/trade/api/v2/futures/order", payload)
        print(f"Order Execution Response: {res}", flush=True)

        self.active_trade = {
            "side": side_signal,
            "entry_price": entry,
            "quantity": quantity,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "highest_price": entry,
            "lowest_price": entry,
            "atr": atr
        }

        emoji = "🟢 *LONG (BUY) POSITION OPENED*" if side_signal == "LONG" else "🔴 *SHORT (SELL) POSITION OPENED*"
        notify(
            f"{emoji}\n"
            f"Coin: `{SYMBOL}`\n"
            f"Entry Price: `{entry}`\n"
            f"Quantity: `{quantity}` ({LEVERAGE}x)\n"
            f"🛑 Initial SL: `{stop_loss}`\n"
            f"🎯 Target TP (1:2): `{take_profit}`\n"
            f"📐 ATR: `{round(atr, 2)}` | RSI: `{signal_data['rsi']}`"
        )

    def manage_trailing_sl(self, current_price):
        if not self.active_trade:
            return

        trade = self.active_trade
        side = trade["side"]
        atr = trade["atr"]

        # ---------------- LONG TRADE MANAGEMENT ----------------
        if side == "LONG":
            if current_price > trade["highest_price"]:
                trade["highest_price"] = current_price
                new_sl = round(current_price - (1.0 * atr), 2)
                if new_sl > trade["stop_loss"]:
                    trade["stop_loss"] = new_sl
                    print(f"📈 [LONG] Trailing SL moved up to: {new_sl}", flush=True)

            if current_price >= trade["take_profit"]:
                self.close_position("🎯 Take Profit Hit!", current_price)
            elif current_price <= trade["stop_loss"]:
                self.close_position("🛑 Trailing SL Hit!", current_price)

        # ---------------- SHORT TRADE MANAGEMENT ----------------
        elif side == "SHORT":
            if current_price < trade["lowest_price"]:
                trade["lowest_price"] = current_price
                new_sl = round(current_price + (1.0 * atr), 2)
                if new_sl < trade["stop_loss"]:
                    trade["stop_loss"] = new_sl
                    print(f"📉 [SHORT] Trailing SL moved down to: {new_sl}", flush=True)

            if current_price <= trade["take_profit"]:
                self.close_position("🎯 Take Profit Hit!", current_price)
            elif current_price >= trade["stop_loss"]:
                self.close_position("🛑 Trailing SL Hit!", current_price)

    def close_position(self, reason, exit_price):
        trade = self.active_trade
        close_side = "SELL" if trade["side"] == "LONG" else "BUY"

        payload = {
            "symbol": SYMBOL,
            "side": close_side,
            "type": "MARKET",
            "quantity": trade["quantity"],
            "leverage": LEVERAGE
        }
        coinswitch_request("POST", "/trade/api/v2/futures/order", payload)
        notify(
            f"🔔 *Position Closed: {reason}*\n"
            f"Direction: `{trade['side']}`\n"
            f"Entry: `{trade['entry_price']}` | Exit: `{exit_price}`"
        )
        self.active_trade = None

# ==========================================
# MAIN BOT LOOP
# ==========================================
def bot_loop():
    print("🤖 Dual-Direction Bot Starting...", flush=True)
    
    res = coinswitch_request("GET", "/trade/api/v2/user/portfolio")
    if isinstance(res, dict) and "Correct" not in str(res):
        data_preview = json.dumps(res.get("data", res))
        notify(f"✅ *CoinSwitch Ed25519 Connected!*\n💰 Portfolio: `{data_preview[:250]}`")
    else:
        notify(f"⚠️ *Connection Notice:* `{res}`")

    notify("🤖 *Bot Active:* Scanning 24/7 for both *LONG* & *SHORT* setups with 1:2 RR & Trailing SL.")
    manager = PositionManager()

    while True:
        try:
            if manager.active_trade:
                candles = fetch_klines(SYMBOL, TIMEFRAME, 2)
                if candles:
                    manager.manage_trailing_sl(candles[-1]['close'])
                time.sleep(10)
            else:
                setup = analyze_market(SYMBOL)
                if setup:
                    manager.execute_entry(setup)
                time.sleep(60)
        except Exception as e:
            print(f"Loop Error: {e}", flush=True)
            time.sleep(15)

if __name__ == "__main__":
    threading.Thread(target=bot_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
