"""Data quality assertions for the VitalSync medallion pipeline."""

import io
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import os
import pytest
from dotenv import load_dotenv

from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")


def _read_layer(s3_client, bucket, prefix):
    """Read all Parquet files under a prefix into a single DataFrame."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    if not keys:
        pytest.skip(f"No parquet files under {prefix}")

    s3_fs = pafs.S3FileSystem(
        region=os.getenv("AWS_REGION"),
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    frames = []
    for key in keys:
        table = pq.read_table(f"{bucket}/{key}", filesystem=s3_fs)
        frames.append(table.to_pandas())
    return pd.concat(frames, ignore_index=True)


# --- Bronze tests ---

class TestBronze:
    def test_schema(self, s3_client, s3_bucket):
        df = _read_layer(s3_client, s3_bucket, "bronze/")
        required = {"user_id", "timestamp", "heart_rate", "steps", "spo2",
                     "kafka_partition", "kafka_offset", "ingestion_timestamp"}
        assert required.issubset(set(df.columns))

    def test_no_null_keys(self, s3_client, s3_bucket):
        df = _read_layer(s3_client, s3_bucket, "bronze/")
        assert df["user_id"].notna().all()
        assert df["timestamp"].notna().all()

    def test_has_data(self, s3_client, s3_bucket):
        df = _read_layer(s3_client, s3_bucket, "bronze/")
        assert len(df) > 0


# --- Silver tests ---

class TestSilver:
    def test_schema(self, s3_client, s3_bucket):
        df = _read_layer(s3_client, s3_bucket, "silver/")
        required = {"user_id", "timestamp", "heart_rate", "steps", "spo2", "is_anomaly"}
        assert required.issubset(set(df.columns))

    def test_no_nulls(self, s3_client, s3_bucket):
        df = _read_layer(s3_client, s3_bucket, "silver/")
        for col in ["heart_rate", "steps", "spo2"]:
            assert df[col].notna().all(), f"Null values in {col}"

    def test_range_heart_rate(self, s3_client, s3_bucket):
        df = _read_layer(s3_client, s3_bucket, "silver/")
        assert (df["heart_rate"] >= 30).all() and (df["heart_rate"] <= 220).all()

    def test_range_spo2(self, s3_client, s3_bucket):
        df = _read_layer(s3_client, s3_bucket, "silver/")
        assert (df["spo2"] >= 70.0).all() and (df["spo2"] <= 100.0).all()

    def test_range_steps(self, s3_client, s3_bucket):
        df = _read_layer(s3_client, s3_bucket, "silver/")
        assert (df["steps"] >= 0).all()

    def test_anomaly_flag(self, s3_client, s3_bucket):
        df = _read_layer(s3_client, s3_bucket, "silver/")
        anomalies = df[df["is_anomaly"] == True]
        for _, row in anomalies.iterrows():
            assert row["heart_rate"] > 130 or row["spo2"] < 92


# --- Gold tests ---

class TestGold:
    def test_schema(self, s3_client, s3_bucket):
        df = _read_layer(s3_client, s3_bucket, "gold/")
        required = {"user_id", "avg_heart_rate", "avg_spo2", "total_steps",
                     "event_count", "window_start", "window_end"}
        assert required.issubset(set(df.columns))

    def test_aggregation_exists(self, s3_client, s3_bucket):
        df = _read_layer(s3_client, s3_bucket, "gold/")
        assert len(df) > 0

    def test_user_coverage(self, s3_client, s3_bucket):
        df = _read_layer(s3_client, s3_bucket, "gold/")
        users = set(df["user_id"].unique())
        assert len(users) >= 1
