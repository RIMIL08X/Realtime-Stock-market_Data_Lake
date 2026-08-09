import os
import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark.common.spark_session import get_spark_session
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("GoldMovingAverages")

def calculate_ma_column(close_col, window_size, window_spec):
    count_col = F.count(close_col).over(window_spec)
    avg_col = F.avg(close_col).over(window_spec)
    return F.when(count_col >= window_size, avg_col).otherwise(F.lit(None))

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

    spark = get_spark_session("GoldMovingAveragesJob")

    logger.info("Reading Silver cleaned stock ticks for moving averages calculation...")
    df_silver = (
        spark.read
        .jdbc(url=jdbc_url, table="silver.cleaned_stock_ticks", properties=jdbc_properties)
    )

    if df_silver.isEmpty():
        logger.info("Silver table is empty. Exiting moving averages calculation.")
        return

    df_with_date = df_silver.withColumn("trade_date", F.to_date(F.col("timestamp")))
    
    # Get last close of each trade_date
    window_daily = Window.partitionBy("symbol", "trade_date").orderBy(F.col("timestamp").desc())
    df_daily = (
        df_with_date.withColumn("rn", F.row_number().over(window_daily))
        .filter(F.col("rn") == 1)
        .select("symbol", "trade_date", "close")
    )

    window_20 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-19, 0)
    window_50 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-49, 0)
    window_200 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-199, 0)

    df_ma = (
        df_daily
        .withColumn("ma_20", calculate_ma_column("close", 20, window_20))
        .withColumn("ma_50", calculate_ma_column("close", 50, window_50))
        .withColumn("ma_200", calculate_ma_column("close", 200, window_200))
        .withColumn("calculated_at", F.current_timestamp())
        .select("symbol", "trade_date", "ma_20", "ma_50", "ma_200", "calculated_at")
    )

    logger.info(f"Writing {df_ma.count()} records to gold.moving_averages...")
    df_ma.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table="gold.moving_averages", properties=jdbc_properties)

    logger.info("Moving averages calculation complete.")

if __name__ == "__main__":
    main()
