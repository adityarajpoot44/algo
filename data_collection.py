import aiohttp
import asyncio
import datetime
import json
from redis_client import redis_cli

async def fetch_all_market(symbol):
    BINANCE_API = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
    async with aiohttp.ClientSession() as session:
        async with session.get(BINANCE_API) as response:
            data = await response.json()
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            data["timestamp"] = timestamp

            json_data = json.dumps(data)

            redis_cli.set(symbol, json_data)
            redis_cli.expire(symbol, 3600)

async def main():
    coins = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "DOGEUSDT", "XRPUSDT", "SOLUSDT", "MATICUSDT", "ADAUSDT",
    "DOTUSDT", "SHIBUSDT", "LTCUSDT", "AVAXUSDT", "LINKUSDT", "TRXUSDT", "ATOMUSDT", "UNIUSDT",
    "BCHUSDT", "APTUSDT", "OPUSDT", "FILUSDT", "LDOUSDT", "AAVEUSDT", "GRTUSDT", "NEARUSDT",
    "FTMUSDT", "VETUSDT", "MANAUSDT", "SANDUSDT", "ALGOUSDT", "AXSUSDT", "ICPUSDT", "APEUSDT",
    "QNTUSDT", "WAVESUSDT", "XTZUSDT", "RNDRUSDT", "FLOWUSDT", "ENSUSDT", "RLCUSDT", "IMXUSDT"
    ]
    while True: 
        tasks = [fetch_all_market(coin) for coin in coins]
        await asyncio.gather(*tasks)
        print("✅ One Second Completed")
        await asyncio.sleep(1)  

if __name__ == "__main__":
    asyncio.run(main())
