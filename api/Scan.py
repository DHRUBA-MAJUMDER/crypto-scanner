import time
import requests
from http.server import BaseHTTPRequestHandler

TELEGRAM_BOT_TOKEN = "8628967713:AAF-N_ap0GHCgC9uDJNccDyHAVpaQoeYdME"
TELEGRAM_CHAT_ID = "8746261690"

def send_telegram_alert(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Telegram alert error: {e}")

# Binance Public Futures API se direct candles fetch karna (Super fast)
def fetch_klines(symbol="BTCUSDT", interval="15m", limit=50):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    res = requests.get(url, timeout=5)
    if res.status_code == 200:
        raw = res.json()
        candles = []
        for c in raw:
            candles.append({
                'open': float(c[1]),
                'high': float(c[2]),
                'low': float(c[3]),
                'close': float(c[4]),
                'volume': float(c[5])
            })
        return candles
    return []

# Technical Math Helpers (No external heavy libs needed)
def calculate_ema(prices, period):
    multiplier = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = (p - ema) * multiplier + ema
    return ema

def calculate_rsi(closes, period=14):
    gains = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        if diff >= 0:
            gains.append(diff)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(diff))

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
        hl = candles[i]['high'] - candles[i]['low']
        h_pc = abs(candles[i]['high'] - candles[i - 1]['close'])
        l_pc = abs(candles[i]['low'] - candles[i - 1]['close'])
        trs.append(max(hl, h_pc, l_pc))
    return sum(trs[-period:]) / period

def analyze_symbol(symbol):
    candles = fetch_klines(symbol, "15m", 50)
    if len(candles) < 30:
        return None

    closes = [c['close'] for c in candles]
    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]

    # Support / Resistance (Last 20 candles before current open candle)
    recent_support = min(lows[-21:-1])
    recent_resistance = max(highs[-21:-1])

    curr_close = closes[-1]
    ema20 = calculate_ema(closes[-25:], 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(candles, 14)

    is_downtrend = ema20 < ema50

    # Rule 1: Solid breakdown close below support
    if is_downtrend and curr_close < recent_support and rsi > 28:
        entry = curr_close
        stop_loss = round(entry + (1.2 * atr), 4)
        risk = stop_loss - entry
        tp1 = round(entry - (1.5 * risk), 4)
        tp2 = round(entry - (2.0 * risk), 4)

        return (
            f"🚨 *NEW SETUP: SHORT BREAKDOWN*\n"
            f"🪙 *Coin:* `{symbol}`\n"
            f"📉 *Trend:* Downtrend (EMA 20 < 50)\n"
            f"🎯 *Entry:* `{entry}`\n"
            f"🛑 *Stop Loss:* `{stop_loss}` (With ATR Buffer)\n"
            f"💰 *TP 1 (1:1.5):* `{tp1}`\n"
            f"💰 *TP 2 (1:2):* `{tp2}`\n"
            f"📊 *RSI:* `{round(rsi, 2)}`\n"
            f"⚠️ *Leverage:* 3x - 4x max"
        )

    # Rule 2: Rejection near resistance
    elif is_downtrend and abs(curr_close - recent_resistance) <= (0.6 * atr) and rsi > 52:
        entry = curr_close
        stop_loss = round(recent_resistance + (0.8 * atr), 4)
        risk = stop_loss - entry
        tp1 = round(entry - (1.5 * risk), 4)
        tp2 = round(entry - (2.0 * risk), 4)

        return (
            f"🚨 *NEW SETUP: SHORT REJECTION*\n"
            f"🪙 *Coin:* `{symbol}`\n"
            f"📉 *Trend:* Bearish Pullback Rejection\n"
            f"🎯 *Entry:* `{entry}`\n"
            f"🛑 *Stop Loss:* `{stop_loss}`\n"
            f"💰 *TP 1 (1:1.5):* `{tp1}`\n"
            f"💰 *TP 2 (1:2):* `{tp2}`\n"
            f"📊 *RSI:* `{round(rsi, 2)}`\n"
            f"⚠️ *Leverage:* 3x - 4x max"
        )

    return None

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Top high-volume futures pairs (Quick 2-second scan batch)
        target_coins = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XMRUSDT", "DOGEUSDT", "BNBUSDT"]
        alerts = 0

        for coin in target_coins:
            try:
                alert_text = analyze_symbol(coin)
                if alert_text:
                    send_telegram_alert(alert_text)
                    alerts += 1
            except Exception as e:
                print(f"Error on {coin}: {e}")

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(f'{{"status": "ok", "scanned": {len(target_coins)}, "alerts_sent": {alerts}}}'.encode('utf-8'))
