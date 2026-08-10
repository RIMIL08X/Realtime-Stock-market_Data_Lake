import os
import json
import logging
from datetime import datetime, timezone
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("LiveTwelveDataIngest")

TWELVE_DATA_KEY = os.getenv("TWELVE_DATA_API_KEY", "f4d4870f17ac47b5853dae067132eb5e")
NEON_URL = "postgresql://neondb_owner:npg_9mbkxBlLq2CQ@ep-still-heart-a6iddbcp.us-west-2.aws.neon.tech/neondb?sslmode=require"
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

def fetch_live_quote(symbol: str):
    url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TWELVE_DATA_KEY}"
    resp = requests.get(url, timeout=10)
    if resp.status_code == 200:
        return resp.json()
    else:
        logger.error(f"Failed to fetch {symbol} from Twelve Data: {resp.status_code} {resp.text}")
        return None

def get_neon_conn():
    import time
    for i in range(5):
        try:
            return psycopg2.connect(NEON_URL, sslmode="require", connect_timeout=15)
        except Exception as e:
            logger.warning(f"Neon DB connection attempt {i+1} waiting for compute wake-up: {e}")
            time.sleep(3)
    raise Exception("Failed connecting to Neon DB after 5 retries")

def main():
    logger.info("Starting live Twelve Data ingestion for active portfolio symbols...")
    conn = get_neon_conn()
    conn.autocommit = True
    cur = conn.cursor()

    inserted = 0
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for symbol in SYMBOLS:
        data = fetch_live_quote(symbol)
        if not data or "close" not in data:
            logger.warning(f"No valid price payload for {symbol}: {data}")
            continue

        try:
            close_p = float(data.get("close", 0.0))
            open_p = float(data.get("open", close_p))
            high_p = float(data.get("high", max(open_p, close_p)))
            low_p = float(data.get("low", min(open_p, close_p)))
            volume = int(float(data.get("volume", 100000)))

            tick = {
                "symbol": symbol,
                "timestamp": now_str,
                "open": f"{open_p:.6f}",
                "high": f"{high_p:.6f}",
                "low": f"{low_p:.6f}",
                "close": f"{close_p:.6f}",
                "volume": volume
            }

            raw_payload = json.dumps(data)

            # Insert Bronze
            cur.execute("""
                INSERT INTO bronze.stock_ticks (symbol, timestamp, open, high, low, close, volume, raw_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (symbol, now_str, open_p, high_p, low_p, close_p, volume, raw_payload))
            bronze_id = cur.fetchone()[0]

            # Insert Silver
            cur.execute("""
                INSERT INTO silver.cleaned_stock_ticks (bronze_id, symbol, timestamp, open, high, low, close, volume, quality_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pass')
                ON CONFLICT (symbol, timestamp) DO NOTHING;
            """, (bronze_id, symbol, now_str, open_p, high_p, low_p, close_p, volume))
            
            logger.info(f"Successfully ingested real Twelve Data tick for {symbol}: Close=${close_p:.2f}")
            inserted += 1

        except Exception as e:
            logger.error(f"Error parsing/inserting live tick for {symbol}: {e}")

    logger.info(f"Ingested {inserted} live Twelve Data ticks into Neon DB!")

    # Re-trigger Gold metric calculations
    logger.info("Updating Gold metric tables with fresh live data...")
    cur.execute("""
        WITH daily_close AS (
            SELECT DISTINCT ON (symbol, timestamp::date)
                symbol, timestamp::date AS trade_date, close
            FROM silver.cleaned_stock_ticks
            ORDER BY symbol, timestamp::date, timestamp DESC
        ),
        returns_calc AS (
            SELECT symbol, trade_date, close,
                LAG(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_close
            FROM daily_close
        )
        INSERT INTO gold.daily_returns (symbol, trade_date, close, prev_close, daily_return, calculated_at)
        SELECT symbol, trade_date, close, prev_close,
            CASE WHEN prev_close IS NOT NULL AND prev_close > 0 THEN (close - prev_close) / prev_close ELSE NULL END, NOW()
        FROM returns_calc
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            close = EXCLUDED.close, prev_close = EXCLUDED.prev_close, daily_return = EXCLUDED.daily_return, calculated_at = EXCLUDED.calculated_at;
    """)

    cur.close()
    conn.close()
    logger.info("Live Twelve Data ingestion & Gold metrics update completed successfully!")

if __name__ == "__main__":
    main()
