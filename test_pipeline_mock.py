import os
import time
import json
import random
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from confluent_kafka import Producer
import psycopg2

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("PipelineMockTest")

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
BASE_PRICES = {
    "AAPL": 185.00,
    "MSFT": 415.00,
    "GOOGL": 175.00,
    "AMZN": 180.00,
    "TSLA": 220.00
}

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Kafka Mock Delivery Failed: {err}")
    else:
        logger.info(f"Mock tick sent to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def generate_mock_ticks(num_days: int = 30):
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    logger.info(f"Initializing Kafka producer connecting to {bootstrap_servers}...")
    
    try:
        producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'pipeline-mock-test-producer'
        })
    except Exception as e:
        logger.warning(f"Could not connect to Kafka broker at {bootstrap_servers}: {e}. Will attempt direct DB seeding.")
        producer = None

    now = datetime.now(timezone.utc)
    ticks = []

    for day_offset in range(num_days, -1, -1):
        trade_date = now - timedelta(days=day_offset)
        # Skip weekends
        if trade_date.weekday() >= 5:
            continue

        for symbol in SYMBOLS:
            base_p = BASE_PRICES[symbol]
            # Simulate daily random walk price fluctuation
            change_pct = random.uniform(-0.03, 0.03)
            close_p = round(base_p * (1 + change_pct), 2)
            open_p = round(base_p * (1 + random.uniform(-0.01, 0.01)), 2)
            high_p = round(max(open_p, close_p) * (1 + random.uniform(0.001, 0.015)), 2)
            low_p = round(min(open_p, close_p) * (1 - random.uniform(0.001, 0.015)), 2)
            volume = random.randint(500000, 5000000)

            # Update base price for next day random walk
            BASE_PRICES[symbol] = close_p

            tick = {
                "symbol": symbol,
                "timestamp": trade_date.strftime("%Y-%m-%d %H:%M:%S"),
                "open": f"{open_p:.6f}",
                "high": f"{high_p:.6f}",
                "low": f"{low_p:.6f}",
                "close": f"{close_p:.6f}",
                "volume": volume,
                "producer_timestamp": datetime.now(timezone.utc).isoformat()
            }
            ticks.append(tick)

            if producer:
                payload = json.dumps(tick).encode('utf-8')
                producer.produce(
                    topic="market.raw",
                    key=symbol.encode('utf-8'),
                    value=payload,
                    callback=delivery_report
                )
                producer.poll(0)

    if producer:
        producer.flush(timeout=10)
        logger.info(f"Successfully produced {len(ticks)} synthetic ticks to Kafka topic 'market.raw'.")

    return ticks

def seed_database_mock(ticks):
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = int(os.getenv("POSTGRES_PORT", "5433"))
    pg_db = os.getenv("POSTGRES_DB", "market_db")
    pg_user = os.getenv("POSTGRES_USER", "market_user")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "market_pass")

    logger.info(f"Connecting to PostgreSQL at {pg_host}:{pg_port}/{pg_db} for direct database mock seeding...")
    try:
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=pg_db,
            user=pg_user,
            password=pg_pass
        )
        cur = conn.cursor()

        inserted_bronze = 0
        inserted_silver = 0

        for t in ticks:
            # 1. Insert into Bronze
            raw_payload_json = json.dumps(t)
            cur.execute("""
                INSERT INTO bronze.stock_ticks (symbol, timestamp, open, high, low, close, volume, raw_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (t["symbol"], t["timestamp"], t["open"], t["high"], t["low"], t["close"], t["volume"], raw_payload_json))
            bronze_id = cur.fetchone()[0]
            inserted_bronze += 1

            # 2. Insert into Silver with deduplication ON CONFLICT DO NOTHING
            cur.execute("""
                INSERT INTO silver.cleaned_stock_ticks (bronze_id, symbol, timestamp, open, high, low, close, volume, quality_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pass')
                ON CONFLICT (symbol, timestamp) DO NOTHING;
            """, (bronze_id, t["symbol"], t["timestamp"], t["open"], t["high"], t["low"], t["close"], t["volume"]))
            if cur.rowcount > 0:
                inserted_silver += 1

        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"Mock database seeding completed: {inserted_bronze} Bronze rows, {inserted_silver} Silver rows inserted.")

    except Exception as e:
        logger.error(f"Error seeding mock database directly: {e}")

if __name__ == "__main__":
    logger.info("Starting mock pipeline test generator (30 days synthetic financial data)...")
    ticks = generate_mock_ticks(num_days=30)
    seed_database_mock(ticks)
    logger.info("Pipeline mock test generator finished.")
