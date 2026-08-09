import os
import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark.common.spark_session import get_spark_session
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("GoldRiskMetrics")

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

    spark = get_spark_session("GoldRiskMetricsJob")

    logger.info("Reading Silver prices and Gold daily returns for Risk metrics calculation...")
    df_silver = spark.read.jdbc(url=jdbc_url, table="silver.cleaned_stock_ticks", properties=jdbc_properties)
    df_returns = spark.read.jdbc(url=jdbc_url, table="gold.daily_returns", properties=jdbc_properties)

    if df_silver.isEmpty() or df_returns.isEmpty():
        logger.info("Input tables empty. Exiting Risk metrics calculation.")
        return

    # 1. Max Drawdown Calculation
    window_unbounded = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)
    
    df_drawdown_raw = (
        df_silver
        .withColumn("trade_date", F.to_date(F.col("timestamp")))
        .withColumn("peak_close", F.max("close").over(window_unbounded))
        .withColumn("drawdown", (F.col("close") - F.col("peak_close")) / F.col("peak_close"))
    )

    # Get worst drawdown per trade_date
    df_max_drawdown = (
        df_drawdown_raw
        .groupBy("symbol", "trade_date")
        .agg(F.min("drawdown").alias("max_drawdown"))
    )

    # 2. VaR 95% and CVaR 95% Calculation
    window_20 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-19, 0)

    # Compute VaR 95% (5th percentile of daily return) over 20-day window
    df_var = (
        df_returns
        .withColumn("count_20", F.count("daily_return").over(window_20))
        .withColumn("var_95_raw", F.expr("percentile_approx(daily_return, 0.05)").over(window_20))
        .withColumn(
            "var_95",
            F.when(F.col("count_20") >= 20, F.col("var_95_raw")).otherwise(F.lit(None))
        )
    )

    # CVaR 95%: Mean of returns <= VaR 95%
    # For robust CVaR calculation, join VaR back to daily returns window
    df_var_cvar = (
        df_var
        .withColumn(
            "cvar_95",
            F.when(
                (F.col("var_95").isNotNull()) & (F.col("daily_return") <= F.col("var_95")),
                F.col("daily_return")
            ).otherwise(F.lit(None))
        )
        .withColumn("cvar_95_avg", F.avg("cvar_95").over(window_20))
        .withColumn(
            "cvar_95",
            F.when(F.col("var_95").isNotNull(), F.col("cvar_95_avg")).otherwise(F.lit(None))
        )
        .select("symbol", "trade_date", "var_95", "cvar_95")
    )

    # Join Max Drawdown with VaR/CVaR
    df_risk = (
        df_max_drawdown.alias("dd")
        .join(
            df_var_cvar.alias("v"),
            (F.col("dd.symbol") == F.col("v.symbol")) & (F.col("dd.trade_date") == F.col("v.trade_date")),
            "inner"
        )
        .withColumn("calculated_at", F.current_timestamp())
        .select("dd.symbol", "dd.trade_date", "dd.max_drawdown", "v.var_95", "v.cvar_95", "calculated_at")
    )

    logger.info(f"Writing {df_risk.count()} records to gold.risk_metrics...")
    df_risk.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table="gold.risk_metrics", properties=jdbc_properties)

    logger.info("Risk metrics calculation complete.")

if __name__ == "__main__":
    main()
