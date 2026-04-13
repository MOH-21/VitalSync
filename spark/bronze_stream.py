"""
VitalSync Bronze Layer -- Raw ingestion from Kafka to S3 (Parquet).
Usage: python bronze_stream.py
"""

from pyspark.sql import functions as F

from schemas import (
    get_spark_session,
    HEALTH_EVENT_SCHEMA,
    KAFKA_BROKER,
    KAFKA_TOPIC,
    BRONZE_PATH,
    BRONZE_CHECKPOINT,
)


def main():
    spark = get_spark_session("VitalSync-Bronze")

    print(f"[Bronze] Reading from Kafka topic '{KAFKA_TOPIC}' at {KAFKA_BROKER}")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(
            F.from_json(F.col("value").cast("string"), HEALTH_EVENT_SCHEMA).alias("data"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .select(
            "data.*",
            "kafka_partition",
            "kafka_offset",
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("date", F.to_date(F.col("timestamp")))
    )

    print(f"[Bronze] Writing Parquet to {BRONZE_PATH}")

    query = (
        parsed
        .coalesce(4)
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .partitionBy("date")
        .trigger(processingTime="30 seconds")
        .queryName("bronze_health_events")
        .start()
    )

    print("[Bronze] Stream started. Waiting for termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
