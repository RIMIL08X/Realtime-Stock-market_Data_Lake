import os
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DecimalType
)
from spark.common.spark_session import get_spark_session
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("SilverTransformer")

TICK_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("open", StringType(), False),
    StructField("high", StringType(), False),
    StructField("low", StringType(), False),
    StructField("close", StringType(), False),
    StructField("volume", LongType(), False),
])

def main():
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
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

    spark = get_spark_session("SilverTransformerJob")

    logger.info(f"Subscribing to Kafka topic 'market.cleaned' on {kafka_servers}...")

    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", "market.cleaned")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    df_raw = df_kafka.select(F.col("value").cast("string").alias("raw_payload"))

    df_parsed = df_raw.withColumn("parsed", F.from_json(F.col("raw_payload"), TICK_SCHEMA)).select(
        F.col("parsed.symbol").alias("symbol"),
        F.to_timestamp(F.col("parsed.timestamp")).alias("timestamp"),
        F.col("parsed.open").cast(DecimalType(18, 6)).alias("open"),
        F.col("parsed.high").cast(DecimalType(18, 6)).alias("high"),
        F.col("parsed.low").cast(DecimalType(18, 6)).alias("low"),
        F.col("parsed.close").cast(DecimalType(18, 6)).alias("close"),
        F.col("parsed.volume").alias("volume"),
        F.col("raw_payload")
    )

    # 10 min watermark for late data handling
    df_watermarked = df_parsed.withWatermark("timestamp", "10 minutes")

    # Deduplicate within watermark window
    df_dedup = df_watermarked.dropDuplicates(["symbol", "timestamp"])

    # OHLC Validation condition
    valid_cond = (
        (F.col("high") >= F.col("low")) &
        (F.col("close") > 0) &
        (F.col("open") > 0) &
        (F.col("high") >= F.col("open")) &
        (F.col("high") >= F.col("close")) &
        (F.col("low") <= F.col("open")) &
        (F.col("low") <= F.col("close"))
    )

    df_validated = df_dedup.withColumn(
        "quality_flag",
        F.when(valid_cond, F.lit("pass")).otherwise(F.lit("fail"))
    ).withColumn("processed_at", F.current_timestamp())

    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        logger.info(f"Processing Silver batch {batch_id} with {batch_df.count()} records...")

        # Separate passed and failed records
        df_passed = batch_df.filter(F.col("quality_flag") == "pass")
        df_failed = batch_df.filter(F.col("quality_flag") == "fail")

        # Route failed records to DLQ topic
        if not df_failed.isEmpty():
            logger.warning(f"Routing {df_failed.count()} failed records to Dead Letter Queue 'market.dlq'...")
            dlq_pub_df = df_failed.select(
                F.col("symbol").cast("string").alias("key"),
                F.col("raw_payload").alias("value")
            )
            dlq_pub_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_servers) \
                .option("topic", "market.dlq") \
                .save()

        # Write clean passed records to PostgreSQL silver.cleaned_stock_ticks
        if not df_passed.isEmpty():
            logger.info(f"Writing {df_passed.count()} validated ticks to silver.cleaned_stock_ticks...")
            
            # Write via JDBC append (Postgres constraints will safeguard against duplicates)
            db_df = df_passed.select("symbol", "timestamp", "open", "high", "low", "close", "volume", "quality_flag", "processed_at")
            
            db_df.write \
                .mode("append") \
                .jdbc(url=jdbc_url, table="silver.cleaned_stock_ticks", properties=jdbc_properties)

            # Publish clean ticks to market.analytics topic for Gold jobs
            analytics_pub_df = df_passed.select(
                F.col("symbol").cast("string").alias("key"),
                F.to_json(F.struct("symbol", "timestamp", "open", "high", "low", "close", "volume")).alias("value")
            )
            
            analytics_pub_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_servers) \
                .option("topic", "market.analytics") \
                .save()

            logger.info(f"Silver batch {batch_id} clean ticks routed to Postgres & market.analytics.")

    checkpoint_dir = os.path.join(".", "checkpoints", "silver_transform")

    query = (
        df_validated.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    logger.info("Silver transformer stream started. Awaiting termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
