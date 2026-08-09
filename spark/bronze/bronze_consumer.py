import os
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DecimalType, TimestampType
)
from spark.common.spark_session import get_spark_session
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("BronzeConsumer")

# Define JSON schema for raw market ticks
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

    spark = get_spark_session("BronzeConsumerJob")

    logger.info(f"Subscribing to Kafka topic 'market.raw' on {kafka_servers}...")

    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", "market.raw")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Cast value to string raw_payload
    df_raw = df_kafka.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_payload")
    )

    # Parse JSON fields
    df_parsed = df_raw.withColumn("parsed", F.from_json(F.col("raw_payload"), TICK_SCHEMA)).select(
        F.col("parsed.symbol").alias("symbol"),
        F.to_timestamp(F.col("parsed.timestamp")).alias("timestamp"),
        F.col("parsed.open").cast(DecimalType(18, 6)).alias("open"),
        F.col("parsed.high").cast(DecimalType(18, 6)).alias("high"),
        F.col("parsed.low").cast(DecimalType(18, 6)).alias("low"),
        F.col("parsed.close").cast(DecimalType(18, 6)).alias("close"),
        F.col("parsed.volume").alias("volume"),
        F.col("raw_payload"),
        F.current_timestamp().alias("ingested_at")
    )

    # Apply 5 minute watermark
    df_watermarked = df_parsed.withWatermark("timestamp", "5 minutes")

    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        
        logger.info(f"Processing Bronze batch {batch_id} with {batch_df.count()} records...")
        
        # Write to Postgres bronze.stock_ticks
        db_writer_df = batch_df.select("symbol", "timestamp", "open", "high", "low", "close", "volume", "raw_payload", "ingested_at")
        db_writer_df.write \
            .mode("append") \
            .jdbc(url=jdbc_url, table="bronze.stock_ticks", properties=jdbc_properties)
        
        logger.info(f"Bronze batch {batch_id} successfully written to PostgreSQL bronze.stock_ticks.")

        # Re-publish valid JSON to market.cleaned topic
        kafka_pub_df = batch_df.select(
            F.col("symbol").cast("string").alias("key"),
            F.col("raw_payload").alias("value")
        )
        
        kafka_pub_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("topic", "market.cleaned") \
            .save()

    checkpoint_dir = os.path.join(".", "checkpoints", "bronze_ingest")

    query = (
        df_watermarked.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    logger.info("Bronze consumer stream started. Awaiting termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
