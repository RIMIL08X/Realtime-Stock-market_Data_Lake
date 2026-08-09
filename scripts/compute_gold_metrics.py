import os
import math
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("GoldMetricsComputer")

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "market_db"),
        user=os.getenv("POSTGRES_USER", "market_user"),
        password=os.getenv("POSTGRES_PASSWORD", "market_pass"),
        cursor_factory=RealDictCursor
    )

def compute_all_gold_metrics():
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    logger.info("1. Calculating gold.daily_returns...")
    cur.execute("""
        WITH daily_close AS (
            SELECT DISTINCT ON (symbol, timestamp::date)
                symbol,
                timestamp::date AS trade_date,
                close
            FROM silver.cleaned_stock_ticks
            ORDER BY symbol, timestamp::date, timestamp DESC
        ),
        returns_calc AS (
            SELECT
                symbol,
                trade_date,
                close,
                LAG(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_close
            FROM daily_close
        )
        INSERT INTO gold.daily_returns (symbol, trade_date, close, prev_close, daily_return, calculated_at)
        SELECT
            symbol,
            trade_date,
            close,
            prev_close,
            CASE 
                WHEN prev_close IS NOT NULL AND prev_close > 0 THEN (close - prev_close) / prev_close 
                ELSE NULL 
            END AS daily_return,
            NOW()
        FROM returns_calc
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            close = EXCLUDED.close,
            prev_close = EXCLUDED.prev_close,
            daily_return = EXCLUDED.daily_return,
            calculated_at = EXCLUDED.calculated_at;
    """)

    logger.info("2. Calculating gold.moving_averages...")
    cur.execute("""
        WITH daily_close AS (
            SELECT DISTINCT ON (symbol, timestamp::date)
                symbol,
                timestamp::date AS trade_date,
                close
            FROM silver.cleaned_stock_ticks
            ORDER BY symbol, timestamp::date, timestamp DESC
        ),
        ma_calc AS (
            SELECT
                symbol,
                trade_date,
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
            ma_20 = EXCLUDED.ma_20,
            ma_50 = EXCLUDED.ma_50,
            ma_200 = EXCLUDED.ma_200,
            calculated_at = EXCLUDED.calculated_at;
    """)

    logger.info("3. Calculating gold.volatility_metrics...")
    cur.execute("""
        WITH vol_calc AS (
            SELECT
                symbol,
                trade_date,
                CASE 
                    WHEN COUNT(daily_return) OVER w20 >= 20 THEN STDDEV_SAMP(daily_return) OVER w20 
                    ELSE NULL 
                END AS rolling_volatility
            FROM gold.daily_returns
            WINDOW w20 AS (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        )
        INSERT INTO gold.volatility_metrics (symbol, trade_date, rolling_volatility, annualized_volatility, calculated_at)
        SELECT
            symbol,
            trade_date,
            rolling_volatility,
            CASE WHEN rolling_volatility IS NOT NULL THEN rolling_volatility * SQRT(252) ELSE NULL END AS annualized_volatility,
            NOW()
        FROM vol_calc
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            rolling_volatility = EXCLUDED.rolling_volatility,
            annualized_volatility = EXCLUDED.annualized_volatility,
            calculated_at = EXCLUDED.calculated_at;
    """)

    logger.info("4. Calculating gold.sharpe_metrics...")
    cur.execute("""
        WITH ann_ret AS (
            SELECT
                symbol,
                trade_date,
                CASE WHEN COUNT(daily_return) OVER w20 >= 20 THEN AVG(daily_return) OVER w20 * 252 ELSE NULL END AS annualized_return
            FROM gold.daily_returns
            WINDOW w20 AS (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        )
        INSERT INTO gold.sharpe_metrics (symbol, trade_date, annualized_return, risk_free_rate, sharpe_ratio, calculated_at)
        SELECT
            r.symbol,
            r.trade_date,
            r.annualized_return,
            0.0450 AS risk_free_rate,
            CASE 
                WHEN v.annualized_volatility IS NOT NULL AND v.annualized_volatility > 0 AND r.annualized_return IS NOT NULL
                THEN (r.annualized_return - 0.0450) / v.annualized_volatility
                ELSE NULL
            END AS sharpe_ratio,
            NOW()
        FROM ann_ret r
        JOIN gold.volatility_metrics v ON r.symbol = v.symbol AND r.trade_date = v.trade_date
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            annualized_return = EXCLUDED.annualized_return,
            risk_free_rate = EXCLUDED.risk_free_rate,
            sharpe_ratio = EXCLUDED.sharpe_ratio,
            calculated_at = EXCLUDED.calculated_at;
    """)

    logger.info("5. Calculating gold.risk_metrics...")
    cur.execute("""
        WITH daily_close AS (
            SELECT DISTINCT ON (symbol, timestamp::date)
                symbol,
                timestamp::date AS trade_date,
                close
            FROM silver.cleaned_stock_ticks
            ORDER BY symbol, timestamp::date, timestamp DESC
        ),
        peak_calc AS (
            SELECT
                symbol,
                trade_date,
                close,
                MAX(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS peak_close
            FROM daily_close
        ),
        dd_calc AS (
            SELECT
                symbol,
                trade_date,
                (close - peak_close) / peak_close AS max_drawdown
            FROM peak_calc
        ),
        var_calc AS (
            SELECT
                r.symbol,
                r.trade_date,
                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY r.daily_return) AS var_95
            FROM gold.daily_returns r
            GROUP BY r.symbol, r.trade_date
        )
        INSERT INTO gold.risk_metrics (symbol, trade_date, max_drawdown, var_95, cvar_95, calculated_at)
        SELECT
            d.symbol,
            d.trade_date,
            d.max_drawdown,
            v.var_95,
            v.var_95 * 1.15 AS cvar_95, -- Tail mean approximation for 95% CVaR
            NOW()
        FROM dd_calc d
        LEFT JOIN var_calc v ON d.symbol = v.symbol AND d.trade_date = v.trade_date
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            max_drawdown = EXCLUDED.max_drawdown,
            var_95 = EXCLUDED.var_95,
            cvar_95 = EXCLUDED.cvar_95,
            calculated_at = EXCLUDED.calculated_at;
    """)

    cur.close()
    conn.close()
    logger.info("All 5 Gold metric tables computed and populated successfully!")

if __name__ == "__main__":
    compute_all_gold_metrics()
