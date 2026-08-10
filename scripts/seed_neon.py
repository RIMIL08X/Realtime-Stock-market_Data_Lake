import os
import random
import logging
import json
from datetime import datetime, timedelta, timezone
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("NeonSeeder")

NEON_URL = "postgresql://neondb_owner:npg_9mbkxBlLq2CQ@ep-still-heart-a6iddbcp-pooler.us-west-2.aws.neon.tech/neondb?sslmode=require"
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
BASE_PRICES = {
    "AAPL": 185.00,
    "MSFT": 415.00,
    "GOOGL": 175.00,
    "AMZN": 180.00,
    "TSLA": 220.00
}

def main():
    logger.info("Connecting to Neon PostgreSQL database...")
    conn = psycopg2.connect(NEON_URL)
    conn.autocommit = True
    cur = conn.cursor()

    # Step 1: Ensure schemas & tables exist
    sql_files = [
        "sql/01_create_schemas.sql",
        "sql/02_bronze_tables.sql",
        "sql/03_silver_tables.sql",
        "sql/04_gold_tables.sql"
    ]
    for sql_file in sql_files:
        if os.path.exists(sql_file):
            logger.info(f"Applying schema script {sql_file}...")
            with open(sql_file, "r") as f:
                cur.execute(f.read())

    # Step 2: Seed Bronze and Silver Ticks
    logger.info("Seeding 30 days of financial ticks into bronze.stock_ticks & silver.cleaned_stock_ticks...")
    now = datetime.now(timezone.utc)
    inserted_count = 0

    for day_offset in range(30, -1, -1):
        trade_date = now - timedelta(days=day_offset)
        if trade_date.weekday() >= 5:
            continue

        for symbol in SYMBOLS:
            base_p = BASE_PRICES[symbol]
            change_pct = random.uniform(-0.025, 0.025)
            close_p = round(base_p * (1 + change_pct), 2)
            open_p = round(base_p * (1 + random.uniform(-0.01, 0.01)), 2)
            high_p = round(max(open_p, close_p) * (1 + random.uniform(0.002, 0.012)), 2)
            low_p = round(min(open_p, close_p) * (1 - random.uniform(0.002, 0.012)), 2)
            volume = random.randint(1000000, 8000000)
            BASE_PRICES[symbol] = close_p

            tick = {
                "symbol": symbol,
                "timestamp": trade_date.strftime("%Y-%m-%d %H:%M:%S"),
                "open": f"{open_p:.6f}",
                "high": f"{high_p:.6f}",
                "low": f"{low_p:.6f}",
                "close": f"{close_p:.6f}",
                "volume": volume
            }

            raw_payload_json = json.dumps(tick)

            # Insert Bronze
            cur.execute("""
                INSERT INTO bronze.stock_ticks (symbol, timestamp, open, high, low, close, volume, raw_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (symbol, tick["timestamp"], open_p, high_p, low_p, close_p, volume, raw_payload_json))
            bronze_id = cur.fetchone()[0]

            # Insert Silver
            cur.execute("""
                INSERT INTO silver.cleaned_stock_ticks (bronze_id, symbol, timestamp, open, high, low, close, volume, quality_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pass')
                ON CONFLICT (symbol, timestamp) DO NOTHING;
            """, (bronze_id, symbol, tick["timestamp"], open_p, high_p, low_p, close_p, volume))
            inserted_count += 1

    logger.info(f"Inserted {inserted_count} ticks into Neon DB!")

    # Step 3: Compute Gold Metrics
    logger.info("Computing Gold daily_returns...")
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

    logger.info("Computing Gold moving_averages...")
    cur.execute("""
        WITH daily_close AS (
            SELECT DISTINCT ON (symbol, timestamp::date)
                symbol, timestamp::date AS trade_date, close
            FROM silver.cleaned_stock_ticks
            ORDER BY symbol, timestamp::date, timestamp DESC
        ),
        ma_calc AS (
            SELECT symbol, trade_date,
                CASE WHEN COUNT(close) OVER w20 >= 20 THEN AVG(close) OVER w20 ELSE NULL END AS ma_20,
                CASE WHEN COUNT(close) OVER w50 >= 50 THEN AVG(close) OVER w50 ELSE NULL END AS ma_50,
                CASE WHEN COUNT(close) OVER w200 >= 200 THEN AVG(close) OVER w200 ELSE NULL END AS ma_200
            FROM daily_close
            WINDOW 
                w20 AS (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                w50 AS (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
                w200 AS (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
        )
        INSERT INTO gold.moving_averages (symbol, trade_date, ma_20, ma_50, ma_200, calculated_at)
        SELECT symbol, trade_date, ma_20, ma_50, ma_200, NOW()
        FROM ma_calc
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            ma_20 = EXCLUDED.ma_20, ma_50 = EXCLUDED.ma_50, ma_200 = EXCLUDED.ma_200, calculated_at = EXCLUDED.calculated_at;
    """)

    logger.info("Computing Gold volatility_metrics...")
    cur.execute("""
        WITH vol_calc AS (
            SELECT symbol, trade_date,
                CASE WHEN COUNT(daily_return) OVER w20 >= 20 THEN STDDEV_SAMP(daily_return) OVER w20 ELSE NULL END AS rolling_volatility
            FROM gold.daily_returns
            WINDOW w20 AS (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        )
        INSERT INTO gold.volatility_metrics (symbol, trade_date, rolling_volatility, annualized_volatility, calculated_at)
        SELECT symbol, trade_date, rolling_volatility,
            CASE WHEN rolling_volatility IS NOT NULL THEN rolling_volatility * SQRT(252) ELSE NULL END, NOW()
        FROM vol_calc
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            rolling_volatility = EXCLUDED.rolling_volatility, annualized_volatility = EXCLUDED.annualized_volatility, calculated_at = EXCLUDED.calculated_at;
    """)

    logger.info("Computing Gold sharpe_metrics...")
    cur.execute("""
        WITH ann_ret AS (
            SELECT symbol, trade_date,
                CASE WHEN COUNT(daily_return) OVER w20 >= 20 THEN AVG(daily_return) OVER w20 * 252 ELSE NULL END AS annualized_return
            FROM gold.daily_returns
            WINDOW w20 AS (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        )
        INSERT INTO gold.sharpe_metrics (symbol, trade_date, annualized_return, risk_free_rate, sharpe_ratio, calculated_at)
        SELECT r.symbol, r.trade_date, r.annualized_return, 0.0450,
            CASE WHEN v.annualized_volatility IS NOT NULL AND v.annualized_volatility > 0 AND r.annualized_return IS NOT NULL
                 THEN (r.annualized_return - 0.0450) / v.annualized_volatility ELSE NULL END, NOW()
        FROM ann_ret r
        JOIN gold.volatility_metrics v ON r.symbol = v.symbol AND r.trade_date = v.trade_date
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            annualized_return = EXCLUDED.annualized_return, risk_free_rate = EXCLUDED.risk_free_rate, sharpe_ratio = EXCLUDED.sharpe_ratio, calculated_at = EXCLUDED.calculated_at;
    """)

    logger.info("Computing Gold risk_metrics...")
    cur.execute("""
        WITH daily_close AS (
            SELECT DISTINCT ON (symbol, timestamp::date)
                symbol, timestamp::date AS trade_date, close
            FROM silver.cleaned_stock_ticks
            ORDER BY symbol, timestamp::date, timestamp DESC
        ),
        peak_calc AS (
            SELECT symbol, trade_date, close,
                MAX(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS peak_close
            FROM daily_close
        ),
        dd_calc AS (
            SELECT symbol, trade_date, (close - peak_close) / peak_close AS max_drawdown
            FROM peak_calc
        ),
        var_calc AS (
            SELECT r.symbol, r.trade_date,
                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY r.daily_return) AS var_95
            FROM gold.daily_returns r
            GROUP BY r.symbol, r.trade_date
        )
        INSERT INTO gold.risk_metrics (symbol, trade_date, max_drawdown, var_95, cvar_95, calculated_at)
        SELECT d.symbol, d.trade_date, d.max_drawdown, v.var_95, v.var_95 * 1.15, NOW()
        FROM dd_calc d
        LEFT JOIN var_calc v ON d.symbol = v.symbol AND d.trade_date = v.trade_date
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            max_drawdown = EXCLUDED.max_drawdown, var_95 = EXCLUDED.var_95, cvar_95 = EXCLUDED.cvar_95, calculated_at = EXCLUDED.calculated_at;
    """)

    cur.close()
    conn.close()
    logger.info("Neon database seeding & Gold metrics computation finished successfully!")

if __name__ == "__main__":
    main()
