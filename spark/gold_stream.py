"""
VitalSync Gold Layer -- Hourly aggregations per user.
Usage:
    python gold_stream.py            # streaming mode (watermarked)
    python gold_stream.py --batch    # batch mode for testing
"""

import sys

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from schemas import (
    get_spark_session,
    SILVER_PATH,
    GOLD_PATH,
    GOLD_CHECKPOINT,
)


def _build_aggregation(df):
    """Shared aggregation logic for both streaming and batch modes."""
    return (
        df
        .groupBy(F.window("event_timestamp", "1 hour"), "user_id")
        .agg(
            F.avg("heart_rate").alias("avg_heart_rate"),
            F.avg("spo2").alias("avg_spo2"),
            F.sum("steps").alias("total_steps"),
            F.count("*").alias("event_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .withColumn("date", F.to_date("window_start"))
    )


def run_streaming(spark):
    """Production streaming path with watermarking."""
    print(f"[Gold] STREAMING mode -- reading silver from {SILVER_PATH}")

    silver_stream = (
        spark.readStream
        .format("parquet")
        .option("path", SILVER_PATH)
        .option("maxFilesPerTrigger", 100)
        .load()
    )

    with_ts = silver_stream.withColumn(
        "event_timestamp", F.col("timestamp").cast(TimestampType())
    )

    watermarked = with_ts.withWatermark("event_timestamp", "15 minutes")
    aggregated = _build_aggregation(watermarked)

    print(f"[Gold] Writing Parquet to {GOLD_PATH}")

    query = (
        aggregated
        .coalesce(2)
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", GOLD_PATH)
        .option("checkpointLocation", GOLD_CHECKPOINT)
        .partitionBy("date")
        .trigger(processingTime="30 seconds")
        .queryName("gold_health_agg")
        .start()
    )

    print("[Gold] Stream started. Waiting for termination...")
    query.awaitTermination()


def run_batch(spark):
    """Batch path for testing -- no watermark, no waiting 75 min for output."""
    print(f"[Gold] BATCH mode -- reading silver from {SILVER_PATH}")

    silver_df = spark.read.parquet(SILVER_PATH)
    print(f"[Gold] Read {silver_df.count()} rows from silver.")

    with_ts = silver_df.withColumn(
        "event_timestamp", F.col("timestamp").cast(TimestampType())
    )

    aggregated = _build_aggregation(with_ts)

    print(f"[Gold] Writing batch Parquet to {GOLD_PATH}")

    (
        aggregated
        .coalesce(2)
        .write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(GOLD_PATH)
    )

    row_count = spark.read.parquet(GOLD_PATH).count()
    print(f"[Gold] Batch complete. {row_count} aggregated rows written.")


def main():
    batch_mode = "--batch" in sys.argv
    spark = get_spark_session("VitalSync-Gold")

    if batch_mode:
        run_batch(spark)
        spark.stop()
    else:
        run_streaming(spark)


if __name__ == "__main__":
    main()
