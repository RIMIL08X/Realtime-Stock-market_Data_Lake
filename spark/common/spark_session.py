import os
import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("SparkSessionFactory")

def get_spark_session(app_name: str) -> SparkSession:
    """
    Returns a configured SparkSession instance for streaming jobs.
    Single factory for all Spark jobs in the platform.
    """
    spark_master = os.getenv("SPARK_MASTER_URL", "local[*]")
    
    # Packages for Kafka integration and PostgreSQL JDBC driver
    kafka_pkg = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    postgres_pkg = "org.postgresql:postgresql:42.7.1"
    packages = f"{kafka_pkg},{postgres_pkg}"

    logger.info(f"Creating SparkSession '{app_name}' on master '{spark_master}' with packages: {packages}")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.jars.packages", packages)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.streaming.backpressure.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
