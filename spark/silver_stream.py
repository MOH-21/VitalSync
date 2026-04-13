"""
VitalSync Silver Layer -- Cleaned and validated health events.
Usage: python silver_stream.py
"""

from pyspark.sql import functions as F

from schemas import (
    get_spark_session,
    BRONZE_PATH,
    BRONZE_SCHEMA,
    SILVER_PATH,
    SILVER_CHECKPOINT,
)


def main():
    spark = get_spark_session("VitalSync-Silver")

    print(f"[Silver] Reading bronze Parquet from {BRONZE_PATH}")

    bronze_stream = (
        spark.readStream
        .schema(BRONZE_SCHEMA)
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("maxFilesPerTrigger", 100)
        .load()
    )

    # Drop rows with nulls in critical columns
    cleaned = bronze_stream.dropna(subset=["heart_rate", "steps", "spo2"])

    # Apply validity filters
    filtered = cleaned.filter(
        (F.col("heart_rate").between(30, 220))
        & (F.col("spo2").between(70.0, 100.0))
        & (F.col("steps") >= 0)
    )

    # Add anomaly flag
    silver = filtered.withColumn(
        "is_anomaly",
        (F.col("heart_rate") > 130) | (F.col("spo2") < 92.0),
    )

    print(f"[Silver] Writing Parquet to {SILVER_PATH}")

    query = (
        silver
        .coalesce(4)
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", SILVER_CHECKPOINT)
        .partitionBy("date")
        .trigger(processingTime="30 seconds")
        .queryName("silver_health_events")
        .start()
    )

    print("[Silver] Stream started. Waiting for termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
