import redis

redis_cli =redis.Redis(
  host='adjusted-cricket-56330.upstash.io',
  port=6379,
  password='AdwKAAIjcDE2YjNmMTU1ZWY1YzU0NGJmOTkyOTFjNzFhYzhjY2MzMXAxMA',
  ssl=True
)
if redis_cli.ping():
    print("Redis Connected Successfully")
else:
    print("Redis Connection Failed")

def store_coin_data_in_redis(symbol, price, volume, rsi, macd, timestamp):
    redis_cli.hset(symbol, mapping={
        "price": price,
        "volume": volume,
        "rsi": rsi,
        "macd": macd,
        "timestamp": timestamp
    })


