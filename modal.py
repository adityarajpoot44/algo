# PostgreSQL Connection & Data Insertion in Python
import psycopg2
from psycopg2 import sql
import datetime

# Database Credentials
DB_NAME = "algo"
DB_USER = "postgres"
DB_PASSWORD = "Akr@8279"
DB_HOST = "localhost"
DB_PORT = "5432"

# Connect to PostgreSQL
def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("✅ Database Connected Successfully")
        return conn
    except Exception as e:
        print("❌ Database Connection Error:", e)
        return None

# Insert Data into Table
def insert_trade(symbol, price, volume, signal, model_prediction):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            query = sql.SQL(
                """
                INSERT INTO crypto_trade_data (symbol, price, volume, signal, model_prediction)
                VALUES (%s, %s, %s, %s, %s);
                """
            )
            cursor.execute(query, (symbol, price, volume, signal, model_prediction))
            conn.commit()
            print(f"✅ Data Inserted: {symbol} - {price} - {signal}")
        except Exception as e:
            print("❌ Insert Error:", e)
        finally:
            cursor.close()
            conn.close()

# Fetch Latest Trade Data
def fetch_latest_trades():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM crypto_trade_data ORDER BY timestamp DESC LIMIT 10;")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
        except Exception as e:
            print("❌ Fetch Error:", e)
        finally:
            cursor.close()
            conn.close()

# Example Usage
if __name__ == "__main__":
    insert_trade("BTCUSDT", 65000.0, 350.0, "BUY", 65200.0)
    fetch_latest_trades()
