import os
import time
import requests
import ccxt
import pandas as pd
import pandas_ta as ta
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
        requests.post(url, json=payload, timeout=8)
    except Exception as e:
        print(f"Telegram alert error: {e}")

exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

def get_top_pairs(limit=15):
    try:
        tickers = exchange.fetch_tickers()
        usdt_pairs = [
            symbol for symbol, t in tickers.items()
            if symbol.endswith('/USDT') and t.get('quoteVolume') is not None
        ]
        sorted_pairs = sorted(
            usdt_pairs,
            key=lambda s: tickers[s]['quoteVolume'],
            reverse=True
        )
        return sorted_pairs[:limit]
    except Exception as e:
        print(f"Tickers fetch error: {e}")
        return ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XMR/USDT']

def fetch_candles(symbol, timeframe='15m', limit=50):
    bars = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

def analyze_market(symbol, df):
    df['rsi'] = ta.rsi(df['close'], length=14)
    df['ema20'] = ta.ema(df['close'], length=20)
    df['ema50'] = ta.ema(df['close'], length=50)
    df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)

    recent_support = df['low'].iloc[-21:-1].min()
    recent_resistance = df['high'].iloc[-21:-1].max()

    curr_close = df['close'].iloc[-1]
    curr_rsi = df['rsi'].iloc[-1]
    curr_atr = df['atr'].iloc[-1]
    is_downtrend = df['ema20'].iloc[-1] < df['ema50'].iloc[-1]

    # Rule 1: Solid breakdown close below support
    if is_downtrend and curr_close < recent_support and curr_rsi > 28:
        entry = curr_close
        stop_loss = round(entry + (1.2 * curr_atr), 4)
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
            f"📊 *RSI:* `{round(curr_rsi, 2)}`\n"
            f"⚠️ *Leverage:* 3x - 4x max"
        )

    # Rule 2: Rejection near resistance
    elif is_downtrend and abs(curr_close - recent_resistance) <= (0.6 * curr_atr) and curr_rsi > 52:
        entry = curr_close
        stop_loss = round(recent_resistance + (0.8 * curr_atr), 4)
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
            f"📊 *RSI:* `{round(curr_rsi, 2)}`\n"
            f"⚠️ *Leverage:* 3x - 4x max"
        )

    return None

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        pairs = get_top_pairs(limit=15)
        alerts_found = 0

        for symbol in pairs:
            try:
                df = fetch_candles(symbol, timeframe='15m', limit=50)
                msg = analyze_market(symbol, df)
                if msg:
                    send_telegram_alert(msg)
                    alerts_found += 1
                time.sleep(0.2)
            except Exception as e:
                print(f"Error analyzing {symbol}: {e}")

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response_msg = f'{{"status": "ok", "scanned": {len(pairs)}, "alerts_sent": {alerts_found}}}'
        self.wfile.write(response_msg.encode('utf-8'))
