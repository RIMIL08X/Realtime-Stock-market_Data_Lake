import os
import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark.common.spark_session import get_spark_session
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("GoldDailyReturns")

def main():
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5433")
    pg_db = os.getenv("POSTGRES_DB", "market_db")
    pg_user = os.getenv("POSTGRES_USER", "market_user")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "market_pass")

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    jdbc_properties = {
        "user": pg_user,
        "password": pg_pass,
        "driver": "org.postgresql.Driver"
    }

    spark = get_spark_session("GoldDailyReturnsJob")

    logger.info("Reading Silver cleaned stock ticks for daily returns calculation...")
    
    # Read Silver data batch/streaming via JDBC or analytics stream
    df_silver = (
        spark.read
        .jdbc(url=jdbc_url, table="silver.cleaned_stock_ticks", properties=jdbc_properties)
    )

    if df_silver.isEmpty():
        logger.info("Silver table is empty. Exiting daily returns calculation.")
        return

    # Truncate timestamp to trade_date
    df_with_date = df_silver.withColumn("trade_date", F.to_date(F.col("timestamp")))

    # Get last close price of each trade date per symbol
    window_last_close = Window.partitionBy("symbol", "trade_date").orderBy(F.col("timestamp").desc())
    df_daily_close = (
        df_with_date.withColumn("rn", F.row_number().over(window_last_close))
        .filter(F.col("rn") == 1)
        .select("symbol", "trade_date", F.col("close").alias("close"))
    )

    # Calculate prev_close and daily_return
    window_symbol = Window.partitionBy("symbol").orderBy("trade_date")
    df_returns = (
        df_daily_close
        .withColumn("prev_close", F.lag("close", 1).over(window_symbol))
        .withColumn(
            "daily_return",
            F.when(
                (F.col("prev_close").isNotNull()) & (F.col("prev_close") > 0),
                (F.col("close") - F.col("prev_close")) / F.col("prev_close")
            ).otherwise(F.lit(None))
        )
        .withColumn("calculated_at", F.current_timestamp())
    )

    logger.info(f"Writing {df_returns.count()} records to gold.daily_returns...")

    # Write to Postgres using temporary table + ON CONFLICT upsert
    df_returns.createOrReplaceTempView("temp_daily_returns")
    
    # Simple direct overwrite or append for daily returns
    df_returns.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table="gold.daily_returns", properties=jdbc_properties)

    logger.info("Daily returns calculation complete.")

if __name__ == "__main__":
    main()
