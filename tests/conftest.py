import os
import boto3
import pytest
from dotenv import load_dotenv

from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")


@pytest.fixture(scope="session")
def s3_client():
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


@pytest.fixture(scope="session")
def s3_bucket():
    return os.getenv("S3_BUCKET", "vitalsync-data")


@pytest.fixture(scope="session")
def api_base_url():
    return "http://localhost:8000"
