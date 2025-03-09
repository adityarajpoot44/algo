import aiohttp
import asyncio
import datetime
import psycopg2
import psycopg2.extras
from psycopg2 import pool
from contextlib import asynccontextmanager

# ✅ Database Connection Pool
DATABASE_URL = "postgresql://algo:eiKZ9YyA0nOGq198T98Kn4Ng6lotwVIo@dpg-cv68b5rtq21c73dgdng0-a.oregon-postgres.render.com/crypto_algo"

try:
    db_pool = pool.SimpleConnectionPool(1, 10, DATABASE_URL)
    print("✅ Database Connection Pool Created")
except Exception as e:
    print(f"❌ Database Connection Error: {e}")

# ✅ Get and Release DB Connection
def get_db_connection():
    try:
        return db_pool.getconn()
    except Exception as e:
        print(f"❌ Database Connection Failed: {e}")
        return None

def release_db_connection(conn):
    if conn:
        db_pool.putconn(conn)

# ✅ Bulk Insert Function
def insert_market_data_bulk(data_list):
    conn = get_db_connection()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        query = """
            INSERT INTO crypto_market_data (
                symbol, price_change, price_change_percent, weighted_avg_price,
                prev_close_price, last_price, last_qty, bid_price, bid_qty,
                ask_price, ask_qty, open_price, high_price, low_price, volume,
                quote_volume, open_time, close_time, first_id, last_id, count
            ) VALUES %s;
        """

        values = [
            (
                data.get("symbol", ""),
                data.get("priceChange", 0),
                data.get("priceChangePercent", 0),
                data.get("weightedAvgPrice", 0),
                data.get("prevClosePrice", 0),
                data.get("lastPrice", 0),
                data.get("lastQty", 0),
                data.get("bidPrice", 0),
                data.get("bidQty", 0),
                data.get("askPrice", 0),
                data.get("askQty", 0),
                data.get("openPrice", 0),
                data.get("highPrice", 0),
                data.get("lowPrice", 0),
                data.get("volume", 0),
                data.get("quoteVolume", 0),
                data.get("openTime", 0),
                data.get("closeTime", 0),
                data.get("firstId", 0),
                data.get("lastId", 0),
                data.get("count", 0)
            )
            for data in data_list
        ]

        psycopg2.extras.execute_values(cursor, query, values)
        conn.commit()
        print(f"✅ {len(values)} Market Data Entries Inserted")

    except Exception as e:
        print(f"❌ Bulk Insert Error: {e}")
    finally:
        cursor.close()
        release_db_connection(conn)

# ✅ Async Function to Fetch Market Data
async def fetch_market_data(session, symbol):
    BINANCE_API = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
    try:
        async with session.get(BINANCE_API, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                data["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return data
            else:
                print(f"❌ API Error {response.status} for {symbol}")
                return None
    except Exception as e:
        print(f"❌ Error Fetching {symbol}: {e}")
        return None

# ✅ Fetch Data for All Coins
async def fetch_all_market():
    coins = [
        "BTCUSDT", "ETHUSDT", "EURUSDT", "GBPUSDT", "USDJPY", "USDCAD", "AUDUSD", "USDCHF",
    "NZDUSD", "EURGBP", "EURJPY", "GBPJPY", "AUDJPY"
    ]

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_market_data(session, coin) for coin in coins]
        results = await asyncio.gather(*tasks)

    market_data = [res for res in results if res is not None]

    if market_data:
        insert_market_data_bulk(market_data)
    else:
        print("❌ No Market Data Collected")

# ✅ Background Task Scheduler
async def run_scheduler():
    while True:
        await fetch_all_market()
        print("✅ One Minute Completed")
        await asyncio.sleep(5)

# ✅ FastAPI Integration (If Needed)
from fastapi import FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("✅ Data Collection Service Started")
    asyncio.create_task(run_scheduler())  # Schedule background task
    yield
    print("🛑 Service Stopped")

app = FastAPI(lifespan=lifespan)

@app.api_route("/", methods=["GET", "POST"])
async def home():
    return {"message": "Crypto Data Collection System Running 🚀"}

# ✅ Run Standalone (If Not Using FastAPI)
if __name__ == "__main__":
    print("✅ Starting Data Collection Service")
    asyncio.run(run_scheduler())
