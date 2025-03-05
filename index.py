import requests
import pandas as pd
from datetime import datetime
import os
import time
import schedule

BINANCE_API = "https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT"
DATA_FILE = "btc_file.csv"

def fetch_trade_volume():
    try:
        response = requests.get(BINANCE_API).json()
        volume = float(response["volume"])
        price = float(response["lastPrice"])
        high = float(response["highPrice"])
        low = float(response["lowPrice"])
        timestamp = datetime.now()

        data = {
            "timestamp": timestamp,
            "price": price,
            "volume": volume,
            "high": high,
            "low": low
        }

        print(f"{timestamp} | Price: {price} | Volume: {volume}")

        if not os.path.exists(DATA_FILE):
            pd.DataFrame([data]).to_csv(DATA_FILE, index=False)
            print("✅ New File Created")

        # Agar File Hai Lekin Empty Hai
        elif os.stat(DATA_FILE).st_size == 0:
            pd.DataFrame([data]).to_csv(DATA_FILE, index=False)
            print("✅ Empty File Re-created")

        # Normal Append Logic
        else:
            df = pd.read_csv(DATA_FILE)
            df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
            df.to_csv(DATA_FILE, index=False)

    except Exception as e:
        print("API Error:", e)

# Schedule to Run Every Minute
schedule.every(1).minutes.do(fetch_trade_volume)

if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(1)
