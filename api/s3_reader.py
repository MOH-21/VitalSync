"""Read gold-layer Parquet data from S3 for the recommendation API."""

import os
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv(dotenv_path="/home/bashr/projects/VitalSync/.env")

S3_BUCKET = os.getenv("S3_BUCKET", "vitalsync-data")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
GOLD_PREFIX = "gold/"


def _get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def _list_parquet_keys(s3_client, prefix: str) -> list[str]:
    """List all .parquet object keys under the given S3 prefix."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def _read_parquet_from_s3(s3_client, key: str, filters=None) -> pd.DataFrame:
    """Read a single Parquet file from S3 with optional predicate-pushdown filters."""
    import pyarrow.fs as pafs

    s3_uri = f"s3://{S3_BUCKET}/{key}"

    s3_fs = pafs.S3FileSystem(
        region=AWS_REGION,
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    table = pq.read_table(
        f"{S3_BUCKET}/{key}",
        filesystem=s3_fs,
        filters=filters,
    )
    return table.to_pandas()


def _compute_trend(values: pd.Series) -> str:
    """Determine whether a time-ordered series is increasing, decreasing, or stable."""
    if len(values) < 2:
        return "stable"
    first_half = values[: len(values) // 2].mean()
    second_half = values[len(values) // 2 :].mean()
    pct_change = (second_half - first_half) / (first_half + 1e-9)
    if pct_change > 0.05:
        return "increasing"
    elif pct_change < -0.05:
        return "decreasing"
    return "stable"


def get_user_data(user_id: str, metric_name: str) -> dict:
    """Return last-24-hour summary stats for a user and metric from gold Parquet.

    Returns dict with keys: min, max, avg, latest, trend, window_count.
    Raises ValueError if no data is found.
    """
    s3_client = _get_s3_client()
    keys = _list_parquet_keys(s3_client, GOLD_PREFIX)

    if not keys:
        raise ValueError("No gold-layer data available in S3")

    # Build predicate-pushdown filters for user_id
    filters = [("user_id", "=", user_id)]

    frames = []
    for key in keys:
        try:
            df = _read_parquet_from_s3(s3_client, key, filters=filters)
            if not df.empty:
                frames.append(df)
        except Exception:
            continue

    if not frames:
        raise ValueError(f"No data found for user_id={user_id}")

    df = pd.concat(frames, ignore_index=True)

    # Filter to the requested metric
    if "metric_name" in df.columns:
        df = df[df["metric_name"] == metric_name]
    elif metric_name not in df.columns:
        raise ValueError(f"Metric '{metric_name}' not found in data")

    if df.empty:
        raise ValueError(f"No {metric_name} data for user_id={user_id}")

    # Filter to last 24 hours if a timestamp/window column exists
    time_col = None
    for candidate in ["window_end", "window_start", "timestamp", "event_time"]:
        if candidate in df.columns:
            time_col = candidate
            break

    if time_col:
        df[time_col] = pd.to_datetime(df[time_col], utc=True)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        df = df[df[time_col] >= cutoff]

    if df.empty:
        raise ValueError(f"No recent {metric_name} data for user_id={user_id}")

    # Determine the value column
    if "metric_name" in df.columns:
        value_col = None
        for candidate in ["avg", "mean", "value", "avg_value"]:
            if candidate in df.columns:
                value_col = candidate
                break
        if value_col is None:
            raise ValueError("Cannot identify value column in data")
    else:
        value_col = metric_name

    # Sort by time for trend calculation
    if time_col:
        df = df.sort_values(time_col)

    values = df[value_col].dropna()

    return {
        "min": round(float(values.min()), 2),
        "max": round(float(values.max()), 2),
        "avg": round(float(values.mean()), 2),
        "latest": round(float(values.iloc[-1]), 2),
        "trend": _compute_trend(values),
        "window_count": len(values),
    }


def get_available_users() -> list[str]:
    """Return distinct user_ids present in the gold Parquet layer."""
    s3_client = _get_s3_client()
    keys = _list_parquet_keys(s3_client, GOLD_PREFIX)

    if not keys:
        return []

    s3_fs = __import__("pyarrow.fs", fromlist=["S3FileSystem"]).S3FileSystem(
        region=AWS_REGION,
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    user_ids = set()
    for key in keys:
        try:
            table = pq.read_table(
                f"{S3_BUCKET}/{key}",
                filesystem=s3_fs,
                columns=["user_id"],
            )
            user_ids.update(table.column("user_id").to_pylist())
        except Exception:
            continue

    return sorted(user_ids)
