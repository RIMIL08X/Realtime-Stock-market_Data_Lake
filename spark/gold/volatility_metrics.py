import os
import math
import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark.common.spark_session import get_spark_session
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("GoldVolatilityMetrics")

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

    spark = get_spark_session("GoldVolatilityMetricsJob")

    logger.info("Reading Gold daily returns for volatility calculation...")
    df_returns = (
        spark.read
        .jdbc(url=jdbc_url, table="gold.daily_returns", properties=jdbc_properties)
    )

    if df_returns.isEmpty():
        logger.info("Daily returns table is empty. Exiting volatility calculation.")
        return

    window_20 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-19, 0)

    sqrt_252 = math.sqrt(252)

    df_vol = (
        df_returns
        .withColumn("valid_return_count", F.count("daily_return").over(window_20))
        .withColumn("sample_stddev", F.stddev_samp("daily_return").over(window_20))
        .withColumn(
            "rolling_volatility",
            F.when(F.col("valid_return_count") >= 20, F.col("sample_stddev")).otherwise(F.lit(None))
        )
        .withColumn(
            "annualized_volatility",
            F.when(F.col("rolling_volatility").isNotNull(), F.col("rolling_volatility") * F.lit(sqrt_252)).otherwise(F.lit(None))
        )
        .withColumn("calculated_at", F.current_timestamp())
        .select("symbol", "trade_date", "rolling_volatility", "annualized_volatility", "calculated_at")
    )

    logger.info(f"Writing {df_vol.count()} records to gold.volatility_metrics...")
    df_vol.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table="gold.volatility_metrics", properties=jdbc_properties)

    logger.info("Volatility metrics calculation complete.")

if __name__ == "__main__":
    main()
