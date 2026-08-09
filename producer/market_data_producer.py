import os
import sys
import time
import json
import signal
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

from confluent_kafka import Producer
from twelvedata import TDClient
from twelvedata.exceptions import TwelveDataError

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("MarketDataProducer")

load_dotenv()

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
KAFKA_TOPIC = "market.raw"
POLL_INTERVAL_SECONDS = 150

running = True

def sig_handler(sig, frame):
    global running
    logger.info("Termination signal received. Shutting down producer gracefully...")
    running = False

signal.signal(signal.SIGINT, sig_handler)
signal.signal(signal.SIGTERM, sig_handler)

def is_market_open() -> bool:
    """
    Checks if NYSE market is open: Mon-Fri 13:30 - 20:00 UTC.
    """
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:  # Saturday or Sunday
        return False
    
    time_minutes = now.hour * 60 + now.minute
    market_start = 13 * 60 + 30  # 13:30 UTC
    market_end = 20 * 60         # 20:00 UTC
    
    return market_start <= time_minutes <= market_end

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def create_kafka_producer() -> Producer:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'market-data-producer',
        'acks': 'all',
        'retries': 3
    }
    logger.info(f"Initializing Kafka producer with bootstrap.servers={bootstrap_servers}")
    return Producer(config)

def run_producer(ignore_market_hours: bool = False):
    global running
    api_key = os.getenv("TWELVE_DATA_API_KEY", "")
    
    if not api_key or api_key == "mock_key":
        logger.warning("No valid TWELVE_DATA_API_KEY found. Please set TWELVE_DATA_API_KEY in .env or run test_pipeline_mock.py.")
        return

    td = TDClient(apikey=api_key)
    producer = create_kafka_producer()

    logger.info(f"Starting producer loop for symbols: {SYMBOLS}")

    while running:
        if not ignore_market_hours and not is_market_open():
            logger.info("Market is currently closed (Mon-Fri 13:30-20:00 UTC). Sleeping for 5 minutes...")
            time.sleep(300)
            continue

        for symbol in SYMBOLS:
            if not running:
                break
            
            try:
                logger.info(f"Fetching price for {symbol} from Twelve Data...")
                ts = td.time_series(symbol=symbol, interval="1min", outputsize=1)
                data = ts.as_json()

                latest_bar = None
                if isinstance(data, (list, tuple)) and len(data) > 0:
                    latest_bar = data[0]
                elif isinstance(data, dict) and "values" in data and len(data["values"]) > 0:
                    latest_bar = data["values"][0]

                if not latest_bar or not isinstance(latest_bar, dict):
                    logger.warning(f"No price data returned for {symbol}: {data}")
                    continue
                # Format payload preserving string representation for NUMERIC(18,6) precision
                payload = {
                    "symbol": symbol,
                    "timestamp": latest_bar["datetime"],
                    "open": str(latest_bar["open"]),
                    "high": str(latest_bar["high"]),
                    "low": str(latest_bar["low"]),
                    "close": str(latest_bar["close"]),
                    "volume": int(latest_bar.get("volume", 0)),
                    "producer_timestamp": datetime.now(timezone.utc).isoformat()
                }

                payload_bytes = json.dumps(payload).encode('utf-8')
                key_bytes = symbol.encode('utf-8')

                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=key_bytes,
                    value=payload_bytes,
                    callback=delivery_report
                )
                producer.poll(0)

            except TwelveDataError as e:
                if "429" in str(e) or "rate limit" in str(e).lower():
                    logger.error(f"Twelve Data Rate Limit (429) hit! Bailing out cycle: {e}")
                    time.sleep(POLL_INTERVAL_SECONDS)
                    break
                else:
                    logger.error(f"Twelve Data API error for {symbol}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error producing tick for {symbol}: {e}", exc_info=True)

        producer.flush(timeout=5)
        logger.info(f"Cycle completed. Sleeping for {POLL_INTERVAL_SECONDS} seconds...")
        
        # Sleep in small increments for quick signal handling
        for _ in range(POLL_INTERVAL_SECONDS):
            if not running:
                break
            time.sleep(1)

    producer.flush(timeout=10)
    logger.info("Producer shutdown complete.")

if __name__ == "__main__":
    ignore_hours = "--ignore-market-hours" in sys.argv
    run_producer(ignore_market_hours=ignore_hours)
