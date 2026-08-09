import os
import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark.common.spark_session import get_spark_session
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("GoldSharpeMetrics")

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

    spark = get_spark_session("GoldSharpeMetricsJob")

    logger.info("Reading Gold daily returns and volatility metrics for Sharpe calculation...")
    df_returns = spark.read.jdbc(url=jdbc_url, table="gold.daily_returns", properties=jdbc_properties)
    df_vol = spark.read.jdbc(url=jdbc_url, table="gold.volatility_metrics", properties=jdbc_properties)

    if df_returns.isEmpty() or df_vol.isEmpty():
        logger.info("Input tables empty. Exiting Sharpe calculation.")
        return

    window_20 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-19, 0)

    df_ann_return = (
        df_returns
        .withColumn("count_20", F.count("daily_return").over(window_20))
        .withColumn("mean_daily_return", F.avg("daily_return").over(window_20))
        .withColumn(
            "annualized_return",
            F.when(F.col("count_20") >= 20, F.col("mean_daily_return") * F.lit(252)).otherwise(F.lit(None))
        )
        .select("symbol", "trade_date", "annualized_return")
    )

    df_joined = (
        df_ann_return.alias("r")
        .join(
            df_vol.alias("v"),
            (F.col("r.symbol") == F.col("v.symbol")) & (F.col("r.trade_date") == F.col("v.trade_date")),
            "inner"
        )
        .select("r.symbol", "r.trade_date", "r.annualized_return", "v.annualized_volatility")
    )

    rf_rate = 0.0450  # 4.5% annual risk-free rate

    df_sharpe = (
        df_joined
        .withColumn("risk_free_rate", F.lit(rf_rate))
        .withColumn(
            "sharpe_ratio",
            F.when(
                (F.col("annualized_volatility").isNotNull()) & 
                (F.col("annualized_volatility") > 0) & 
                (F.col("annualized_return").isNotNull()),
                (F.col("annualized_return") - F.col("risk_free_rate")) / F.col("annualized_volatility")
            ).otherwise(F.lit(None))
        )
        .withColumn("calculated_at", F.current_timestamp())
        .select("symbol", "trade_date", "annualized_return", "risk_free_rate", "sharpe_ratio", "calculated_at")
    )

    logger.info(f"Writing {df_sharpe.count()} records to gold.sharpe_metrics...")
    df_sharpe.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table="gold.sharpe_metrics", properties=jdbc_properties)

    logger.info("Sharpe metrics calculation complete.")

if __name__ == "__main__":
    main()
