"""
Shared schema definitions and SparkSession factory for the VitalSync pipeline.
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, LongType, BooleanType, DateType
)
from dotenv import load_dotenv

# Load environment variables from project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# --- Shared schema matching Kafka JSON payloads ---
HEALTH_EVENT_SCHEMA = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
    StructField("heart_rate", IntegerType(), nullable=True),
    StructField("steps", IntegerType(), nullable=True),
    StructField("spo2", DoubleType(), nullable=True),
])

# --- Environment helpers ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "health-events")
S3_BUCKET = os.getenv("S3_BUCKET", "vitalsync-data")

BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/"
SILVER_PATH = f"s3a://{S3_BUCKET}/silver/"
GOLD_PATH = f"s3a://{S3_BUCKET}/gold/"

BRONZE_CHECKPOINT = f"s3a://{S3_BUCKET}/checkpoints/bronze/"
SILVER_CHECKPOINT = f"s3a://{S3_BUCKET}/checkpoints/silver/"
GOLD_CHECKPOINT = f"s3a://{S3_BUCKET}/checkpoints/gold/"

BRONZE_SCHEMA = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
    StructField("heart_rate", IntegerType(), nullable=True),
    StructField("steps", IntegerType(), nullable=True),
    StructField("spo2", DoubleType(), nullable=True),
    StructField("kafka_partition", IntegerType(), nullable=True),
    StructField("kafka_offset", LongType(), nullable=True),
    StructField("ingestion_timestamp", TimestampType(), nullable=True),
    StructField("date", DateType(), nullable=True),
])

SILVER_SCHEMA = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
    StructField("heart_rate", IntegerType(), nullable=True),
    StructField("steps", IntegerType(), nullable=True),
    StructField("spo2", DoubleType(), nullable=True),
    StructField("kafka_partition", IntegerType(), nullable=True),
    StructField("kafka_offset", LongType(), nullable=True),
    StructField("ingestion_timestamp", TimestampType(), nullable=True),
    StructField("date", DateType(), nullable=True),
    StructField("is_anomaly", BooleanType(), nullable=True),
])

SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])


def get_spark_session(app_name: str) -> SparkSession:
    """Create a SparkSession pre-configured for S3 access and Kafka."""
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print(f"[VitalSync] SparkSession '{app_name}' created.")
    return spark
