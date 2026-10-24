import os
import time
import json
import random
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
    return "CoinSwitch Ed25519 Multi-Trade Omni-Scanner Bot Running 24/7!"

# ==========================================
# CREDENTIALS & CONFIG
# ==========================================
API_KEY = "993e3a565da33321d40c76fa000a0f0f849d075573a90eebf8c4ccd1c2ecbe76"
SECRET_KEY = "372663202fb919daac04b745ca608beb9010694ad2c8ea187cb31ecddc5baead"
TELEGRAM_BOT_TOKEN = "8628967713:AAGOJGlodwYFea70NC5wP70k1zf3HGS4DqE"
TELEGRAM_CHAT_ID = "8746261690"

BASE_URL = "https://coinswitch.co"
TIMEFRAME = "15m"
LEVERAGE = 3
TRADE_MARGIN_USDT = 15.0        # Margin per trade
MIN_USDT_REQUIRED = 10.0        # Min USDT balance if wallet is in USDT
MIN_INR_REQUIRED = 150.0        # Min INR balance if wallet is in INR
MAX_CONCURRENT_TRADES = 10      # Ek sath maximum open trades ki limit

FALLBACK_COINS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT",
    "XRPUSDT", "SUIUSDT", "PEPEUSDT", "NEARUSDT", "AVAXUSDT",
    "LINKUSDT", "ADAUSDT", "WIFUSDT", "APTUSDT", "FETUSDT"
]

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
        "User-Agent": "CoinSwitchPro-OmniScanner-Bot"
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
# PRE-TRADE BALANCE CHECK (INR & USDT SUPPORT)
# ==========================================
def has_free_margin() -> bool:
    res = coinswitch_request("GET", "/trade/api/v2/user/portfolio")
    if not isinstance(res, dict) or "error" in res or "Correct" in str(res):
        return True  # Failsafe: allow order attempt if portfolio check times out

    try:
        data = res.get("data", [])
        # Format 1: List of currency objects [{"currency": "INR", "main_balance": "422.13", ...}]
        if isinstance(data, list):
            for item in data:
                curr = item.get("currency", "").upper()
                bal = float(item.get("main_balance", 0.0))
                if curr == "USDT" and bal >= MIN_USDT_REQUIRED:
                    return True
                if curr == "INR" and bal >= MIN_INR_REQUIRED:
                    return True
            # If list parsed properly and neither INR nor USDT has minimum funds
            if len(data) > 0:
                return False

        # Format 2: Dictionary portfolio
        elif isinstance(data, dict):
            usdt_bal = float(data.get("USDT", {}).get("available", data.get("available_balance", 0.0)))
            if usdt_bal > 0 and usdt_bal < MIN_USDT_REQUIRED:
                return False
    except Exception:
        pass
    return True

# ==========================================
# OMNI-MARKET SCANNER (TOP + BOTTOM + RANDOM)
# ==========================================
def get_omni_market_watchlist():
    """
    Fetches Top Volume, Top Gainers (Top Trending), Top Losers (Bottom Trending),
    and Random Coins from the active Futures market.
    """
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    try:
        res = requests.get(url, timeout=8)
        if res.status_code == 200:
            tickers = res.json()
            # Filter liquid USDT perpetual futures (> $10M 24h volume)
            valid_pairs = [
                t for t in tickers
                if t['symbol'].endswith('USDT')
                and not t['symbol'].startswith('USDC')
                and float(t.get('quoteVolume', 0)) > 10000000
            ]

            if not valid_pairs:
                return FALLBACK_COINS

            # 1. Top 10 High Volume Leaders
            by_volume = sorted(valid_pairs, key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
            top_volume = [t['symbol'] for t in by_volume[:10]]

            # 2. Top 8 Gainers (Top Trending Coins)
            by_change = sorted(valid_pairs, key=lambda x: float(x.get('priceChangePercent', 0)), reverse=True)
            top_gainers = [t['symbol'] for t in by_change[:8]]

            # 3. Top 8 Losers (Bottom Trending / Heavy Dump Coins)
            top_losers = [t['symbol'] for t in by_change[-8:]]

            # Combine trending lists without duplicates
            priority_coins = list(dict.fromkeys(top_volume + top_gainers + top_losers))

            # 4. Pick 15 Random Coins from the rest of the market
            remaining_pool = [t['symbol'] for t in valid_pairs if t['symbol'] not in priority_coins]
            random_count = min(15, len(remaining_pool))
            random_coins = random.sample(remaining_pool, random_count) if remaining_pool else []

            final_watchlist = priority_coins + random_coins
            return final_watchlist
    except Exception as e:
        print(f"Market Fetch Error: {e}", flush=True)
    return FALLBACK_COINS

# ==========================================
# ACCURATE TECHNICAL ANALYSIS & INDICATORS
# ==========================================
def fetch_klines(symbol, interval="15m", limit=100):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = requests.get(url, timeout=6)
        if res.status_code == 200:
            return [
                {'open': float(c[1]), 'high': float(c[2]), 'low': float(c[3]), 'close': float(c[4])}
                for c in res.json()
            ]
    except Exception:
        pass
    return []

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1]
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

    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr

def format_price(val):
    if val >= 100:
        return round(val, 2)
    elif val >= 1:
        return round(val, 4)
    else:
        return round(val, 6)

def format_quantity(raw_qty, entry_price):
    if entry_price >= 1000:
        return round(raw_qty, 4)
    elif entry_price >= 10:
        return round(raw_qty, 2)
    elif entry_price >= 0.5:
        return round(raw_qty, 1)
    else:
        return int(raw_qty)

def analyze_market(symbol):
    candles = fetch_klines(symbol, TIMEFRAME, 100)
    if len(candles) < 60:
        return None

    closes = [c['close'] for c in candles]
    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]

    # Support & Resistance of last 20 closed candles
    support = min(lows[-21:-1])
    resistance = max(highs[-21:-1])

    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes, 14)
    atr = calculate_atr(candles, 14)
    curr_close = closes[-1]

    # 1. LONG SIGNAL: Uptrend + Resistance Breakout + Not Overbought
    if ema20 > ema50 and curr_close > resistance and rsi < 72:
        return {
            "symbol": symbol,
            "signal": "LONG",
            "entry": curr_close,
            "atr": atr,
            "support": support,
            "resistance": resistance,
            "rsi": round(rsi, 2)
        }

    # 2. SHORT SIGNAL: Downtrend + Support Breakdown + Not Oversold
    if ema20 < ema50 and curr_close < support and rsi > 28:
        return {
            "symbol": symbol,
            "signal": "SHORT",
            "entry": curr_close,
            "atr": atr,
            "support": support,
            "resistance": resistance,
            "rsi": round(rsi, 2)
        }

    return None

# ==========================================
# MULTI-TRADE POSITION & TRAILING SL MANAGER
# ==========================================
class MultiPositionManager:
    def __init__(self):
        self.active_trades = {}  # { "SYMBOL": { trade_data } }
        self.lock = threading.Lock()
        self.margin_cooldown_until = 0

    def can_open_more_trades(self, symbol):
        with self.lock:
            if symbol in self.active_trades:
                return False
            if len(self.active_trades) >= MAX_CONCURRENT_TRADES:
                return False
        if time.time() < self.margin_cooldown_until:
            return False
        return True

    def execute_entry(self, signal_data):
        symbol = signal_data["symbol"]
        if not self.can_open_more_trades(symbol):
            return

        if not has_free_margin():
            print(f"⚠️ Margin full or low balance, skipping new entry on {symbol}.", flush=True)
            self.margin_cooldown_until = time.time() + 180  # Pause new entries for 3 mins
            return

        side_signal = signal_data["signal"]
        entry = signal_data["entry"]
        atr = signal_data["atr"]

        raw_qty = (TRADE_MARGIN_USDT * LEVERAGE) / entry
        quantity = format_quantity(raw_qty, entry)
        if quantity <= 0:
            quantity = round(raw_qty, 4)

        # 1.2x ATR Volatility Buffer for SL & 1:2 Risk-Reward for TP
        if side_signal == "LONG":
            order_side = "BUY"
            stop_loss = format_price(entry - (1.2 * atr))
            risk = entry - stop_loss
            take_profit = format_price(entry + (2.0 * risk))
        else:
            order_side = "SELL"
            stop_loss = format_price(entry + (1.2 * atr))
            risk = stop_loss - entry
            take_profit = format_price(entry - (2.0 * risk))

        payload = {
            "symbol": symbol,
            "side": order_side,
            "type": "MARKET",
            "quantity": quantity,
            "leverage": LEVERAGE
        }
        res = coinswitch_request("POST", "/trade/api/v2/futures/order", payload)
        print(f"Order Response [{symbol}]: {res}", flush=True)

        # Check if exchange rejected order due to margin/balance or unsupported pair
        res_str = str(res).lower()
        if "insufficient" in res_str or "balance" in res_str or "margin" in res_str:
            notify(f"⚠️ *Margin Full Alert:* `{symbol}` ({side_signal}) signal mila par free balance kam hai. Open trades: `{len(self.active_trades)}`")
            self.margin_cooldown_until = time.time() + 300  # 5 min cooldown
            return

        with self.lock:
            self.active_trades[symbol] = {
                "symbol": symbol,
                "side": side_signal,
                "entry_price": entry,
                "quantity": quantity,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "highest_price": entry,
                "lowest_price": entry,
                "atr": atr
            }
            total_open = len(self.active_trades)

        emoji = "🟢 *LONG POSITION OPENED*" if side_signal == "LONG" else "🔴 *SHORT POSITION OPENED*"
        notify(
            f"{emoji} (Active Trades: `{total_open}`)\n"
            f"Coin: `{symbol}`\n"
            f"Entry: `{entry}`\n"
            f"Qty: `{quantity}` ({LEVERAGE}x)\n"
            f"🛑 SL (1.2x ATR Buffer): `{stop_loss}`\n"
            f"🎯 TP (1:2 RR): `{take_profit}`\n"
            f"📐 RSI: `{signal_data['rsi']}` | S: `{signal_data['support']}` | R: `{signal_data['resistance']}`"
        )

    def update_all_positions(self):
        with self.lock:
            symbols = list(self.active_trades.keys())

        for sym in symbols:
            candles = fetch_klines(sym, TIMEFRAME, 2)
            if not candles:
                continue
            current_price = candles[-1]['close']
            self.manage_single_trailing_sl(sym, current_price)
            time.sleep(0.4)

    def manage_single_trailing_sl(self, symbol, current_price):
        with self.lock:
            trade = self.active_trades.get(symbol)
            if not trade:
                return

        side = trade["side"]
        atr = trade["atr"]

        # ---------------- LONG TRADE TRAILING ----------------
        if side == "LONG":
            if current_price > trade["highest_price"]:
                trade["highest_price"] = current_price
                new_sl = format_price(current_price - (1.0 * atr))
                if new_sl > trade["stop_loss"]:
                    trade["stop_loss"] = new_sl
                    print(f"📈 [{symbol} LONG] Trailing SL moved up to: {new_sl}", flush=True)

            if current_price >= trade["take_profit"]:
                self.close_position(symbol, "🎯 Take Profit Hit!", current_price)
            elif current_price <= trade["stop_loss"]:
                self.close_position(symbol, "🛑 Trailing SL Hit!", current_price)

        # ---------------- SHORT TRADE TRAILING ----------------
        elif side == "SHORT":
            if current_price < trade["lowest_price"]:
                trade["lowest_price"] = current_price
                new_sl = format_price(current_price + (1.0 * atr))
                if new_sl < trade["stop_loss"]:
                    trade["stop_loss"] = new_sl
                    print(f"📉 [{symbol} SHORT] Trailing SL moved down to: {new_sl}", flush=True)

            if current_price <= trade["take_profit"]:
                self.close_position(symbol, "🎯 Take Profit Hit!", current_price)
            elif current_price >= trade["stop_loss"]:
                self.close_position(symbol, "🛑 Trailing SL Hit!", current_price)

    def close_position(self, symbol, reason, exit_price):
        with self.lock:
            trade = self.active_trades.pop(symbol, None)
            remaining = len(self.active_trades)

        if not trade:
            return

        close_side = "SELL" if trade["side"] == "LONG" else "BUY"
        payload = {
            "symbol": symbol,
            "side": close_side,
            "type": "MARKET",
            "quantity": trade["quantity"],
            "leverage": LEVERAGE
        }
        coinswitch_request("POST", "/trade/api/v2/futures/order", payload)
        notify(
            f"🔔 *Position Closed: {reason}*\n"
            f"Coin: `{symbol}` | Side: `{trade['side']}`\n"
            f"Entry: `{trade['entry_price']}` | Exit: `{exit_price}`\n"
            f"📊 Remaining Active Trades: `{remaining}`"
        )

# ==========================================
# PARALLEL THREADS: MONITOR & OMNI-SCANNER
# ==========================================
manager = MultiPositionManager()

def position_monitor_worker():
    """Dedicated fast loop to trail SL & hit TP on all active positions every 8s."""
    while True:
        try:
            if manager.active_trades:
                manager.update_all_positions()
        except Exception as e:
            print(f"Monitor Error: {e}", flush=True)
        time.sleep(8)

def omni_scanner_worker():
    """Continuously scans Top Volume, Top Gainers, Top Losers, and Random Coins."""
    print("🤖 Omni-Market Multi-Trade Bot Starting...", flush=True)

    res = coinswitch_request("GET", "/trade/api/v2/user/portfolio")
    if isinstance(res, dict) and "Correct" not in str(res):
        data_preview = json.dumps(res.get("data", res))
        notify(f"✅ *CoinSwitch Ed25519 Connected!*\n💰 Portfolio: `{data_preview[:250]}`")
    else:
        notify(f"⚠️ *Connection Notice:* `{res}`")

    notify(
        "🚀 *Omni-Market Multi-Trade Scanner Active!*\n"
        "• *Mode:* Top Gainers + Top Losers + High Volume + Random Coins\n"
        "• *Direction:* Dual (LONG & SHORT)\n"
        "• *Multi-Trade:* Enabled (Auto-scales with available balance)"
    )

    while True:
        try:
            watchlist = get_omni_market_watchlist()
            print(f"🔍 [Omni-Scan] Scanning {len(watchlist)} coins (Trending + Losers + Random) | Active Trades: {len(manager.active_trades)}", flush=True)

            for coin in watchlist:
                # Skip if already in a trade on this coin
                if not manager.can_open_more_trades(coin):
                    continue

                setup = analyze_market(coin)
                if setup:
                    manager.execute_entry(setup)

                time.sleep(1.2)  # Smooth rate-limit delay between coins

            time.sleep(20)  # Short pause before pulling a fresh random + trending list
        except Exception as e:
            print(f"Scanner Loop Error: {e}", flush=True)
            time.sleep(15)

if __name__ == "__main__":
    # Start dedicated SL/TP trailing monitor thread
    threading.Thread(target=position_monitor_worker, daemon=True).start()
    # Start full-market continuous scanner thread
    threading.Thread(target=omni_scanner_worker, daemon=True).start()

    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
