import aiohttp
import asyncio
import datetime
import psycopg2
import psycopg2.extras 

# Database Credentials
DATABASE_URL = "postgresql://algo:eiKZ9YyA0nOGq198T98Kn4Ng6lotwVIo@dpg-cv68b5rtq21c73dgdng0-a.oregon-postgres.render.com/crypto_algo"

# Connect to PostgreSQL
def connect_db():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ Database Connection Failed: {e}")
        return None

# Function to Insert Data in Bulk
def insert_market_data_bulk(data_list):
    conn = connect_db()
    if not conn:
        print("❌ No database connection available.")
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
                data["symbol"], data["priceChange"], data["priceChangePercent"], data["weightedAvgPrice"],
                data["prevClosePrice"], data["lastPrice"], data["lastQty"], data["bidPrice"], data["bidQty"],
                data["askPrice"], data["askQty"], data["openPrice"], data["highPrice"], data["lowPrice"], data["volume"],
                data["quoteVolume"], data["openTime"], data["closeTime"], data["firstId"], data["lastId"], data["count"]
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
        conn.close()

# Fetch market data for a single symbol
async def fetch_market_data(session, symbol):
    BINANCE_API = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
    async with session.get(BINANCE_API) as response:
        try:
            data = await response.json()
            data["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return data
        except Exception as e:
            print(f"❌ Error Fetching {symbol}: {e}")
            return None

# Fetch all market data in parallel
async def fetch_all_market():
    coins = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "DOGEUSDT", "XRPUSDT", "SOLUSDT", "MATICUSDT", "ADAUSDT",
        "DOTUSDT", "SHIBUSDT", "LTCUSDT", "AVAXUSDT", "LINKUSDT", "TRXUSDT", "ATOMUSDT", "UNIUSDT",
        "BCHUSDT", "APTUSDT", "OPUSDT", "FILUSDT", "LDOUSDT", "AAVEUSDT", "GRTUSDT", "NEARUSDT",
        "FTMUSDT", "VETUSDT", "MANAUSDT", "SANDUSDT", "ALGOUSDT", "AXSUSDT", "ICPUSDT", "APEUSDT",
        "QNTUSDT", "WAVESUSDT", "XTZUSDT", "RNDRUSDT", "FLOWUSDT", "ENSUSDT", "RLCUSDT", "IMXUSDT"
    ]

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_market_data(session, coin) for coin in coins]
        results = await asyncio.gather(*tasks)

    # Filter out failed requests
    market_data = [res for res in results if res is not None]

    if market_data:
        insert_market_data_bulk(market_data)

async def main():
    while True:
        await fetch_all_market()
        print("✅ One Minute Completed")
        await asyncio.sleep(60)  # Run every 1 minute

if __name__ == "__main__":
    asyncio.run(main())
