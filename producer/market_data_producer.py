import os
import time
import json
import random
import logging
from datetime import datetime, timezone
import yfinance as yf
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("YahooFinanceMarketProducer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "market.raw"
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def get_kafka_producer():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    try:
        return Producer(conf)
    except Exception as e:
        logger.error(f"Failed to create Kafka Producer: {e}")
        return None

def fetch_yfinance_quote(symbol: str):
    try:
        fast = yf.Ticker(symbol).fast_info
        last_price = float(fast.last_price) if fast.last_price else 150.0
        open_price = float(fast.open) if hasattr(fast, 'open') and fast.open else last_price
        day_high = float(fast.day_high) if hasattr(fast, 'day_high') and fast.day_high else max(open_price, last_price)
        day_low = float(fast.day_low) if hasattr(fast, 'day_low') and fast.day_low else min(open_price, last_price)
        volume = int(fast.last_volume) if hasattr(fast, 'last_volume') and fast.last_volume else 1000000

        fluctuation = random.uniform(-0.0015, 0.0015)
        current_close = round(last_price * (1 + fluctuation), 2)

        return {
            "symbol": symbol,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "open": f"{open_price:.6f}",
            "high": f"{max(day_high, current_close):.6f}",
            "low": f"{min(day_low, current_close):.6f}",
            "close": f"{current_close:.6f}",
            "volume": volume
        }
    except Exception as e:
        logger.error(f"Error fetching quote for {symbol}: {e}")
        return None

def produce_market_data():
    producer = get_kafka_producer()
    logger.info(f"Starting Yahoo Finance Kafka Streaming Producer for {SYMBOLS}...")

    while True:
        for symbol in SYMBOLS:
            quote = fetch_yfinance_quote(symbol)
            if quote and producer:
                payload = json.dumps(quote).encode('utf-8')
                producer.produce(TOPIC_NAME, key=symbol.encode('utf-8'), value=payload, callback=delivery_report)
                producer.poll(0)
                logger.info(f"Produced tick for {symbol}: Close=${quote['close']}")
        
        if producer:
            producer.flush()
        time.sleep(30)

if __name__ == "__main__":
    produce_market_data()
