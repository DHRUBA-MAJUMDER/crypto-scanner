import os
import time
import hmac
import hashlib
import requests
import threading
from flask import Flask

app = Flask(__name__)
@app.route('/')
def home():
    return "Diagnostic Bot Active"

# Hardcoded Keys
API_KEY = "993e3a565da33321d40c76fa000a0f0f849d075573a90eebf8c4ccd1c2ecbe76"
SECRET_KEY = "372663202fb919daac04b745ca608beb9010694ad2c8ea187cb31ecddc5baead"
TELEGRAM_BOT_TOKEN = "8628967713:AAGOJGlodwYFea70NC5wP70k1zf3HGS4DqE"
TELEGRAM_CHAT_ID = "8746261690"

def notify(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"})

def run_diagnostics():
    notify("🔍 *Running CoinSwitch Signature Diagnostics...*")
    time.sleep(2)
    
    timestamp = str(int(time.time() * 1000))
    results = []
    
    # ----------------------------------------------------
    # TEST 1: Standard format (Empty string payload)
    # ----------------------------------------------------
    url1 = "https://coinswitch.co/trade/api/v2/user/portfolio"
    ep1 = "/trade/api/v2/user/portfolio"
    msg1 = "GET" + ep1 + "" + timestamp
    sig1 = hmac.new(SECRET_KEY.encode('utf-8'), msg1.encode('utf-8'), hashlib.sha256).hexdigest()
    
    # ----------------------------------------------------
    # TEST 2: Empty JSON format {}
    # ----------------------------------------------------
    msg2 = "GET" + ep1 + "{}" + timestamp
    sig2 = hmac.new(SECRET_KEY.encode('utf-8'), msg2.encode('utf-8'), hashlib.sha256).hexdigest()
    
    # ----------------------------------------------------
    # TEST 3: Trade-API Subdomain V1 (Often used for futures)
    # ----------------------------------------------------
    url3 = "https://trade-api.coinswitch.co/v1/user/balance"
    ep3 = "/v1/user/balance"
    msg3 = "GET" + ep3 + "" + timestamp
    sig3 = hmac.new(SECRET_KEY.encode('utf-8'), msg3.encode('utf-8'), hashlib.sha256).hexdigest()

    # ----------------------------------------------------
    # Execute Tests
    # ----------------------------------------------------
    combos = [
        (url1, sig1, "Test 1: Standard /v2"),
        (url1, sig2, "Test 2: JSON {} /v2"),
        (url3, sig3, "Test 3: Trade-API /v1")
    ]

    for u, sig, name in combos:
        headers = {
            "X-AUTH-APIKEY": API_KEY,
            "X-AUTH-SIGNATURE": sig,
            "X-AUTH-TIMESTAMP": timestamp,
            "Content-Type": "application/json"
        }
        try:
            r = requests.get(u, headers=headers)
            results.append(f"*{name}*\nStatus: `{r.status_code}`\nResponse: `{r.text}`")
        except Exception as e:
            results.append(f"*{name}*\nError: `{str(e)}`")
            
    final_msg = "🛠 *Raw API Responses:*\n\n" + "\n\n".join(results)
    notify(final_msg)

if __name__ == "__main__":
    threading.Thread(target=run_diagnostics, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
