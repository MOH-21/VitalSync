#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "=== VitalSync Setup ==="

# Python venv
if [ ! -d "venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv venv
fi
source venv/bin/activate

echo "Installing Python dependencies..."
pip install -q -r simulator/requirements.txt \
    -r spark/requirements.txt \
    -r api/requirements.txt \
    -r tests/requirements.txt \
    lz4

# Frontend
echo "Installing frontend dependencies..."
npm install --prefix frontend

# Docker
echo "Starting Kafka..."
docker compose up -d
echo "Waiting for Kafka to be healthy..."
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1 || sleep 10
docker exec vitalsync-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --topic health-events --partitions 3 --replication-factor 1 \
    --bootstrap-server localhost:9092

# S3 bucket
echo "Creating S3 bucket (if needed)..."
python3 -c "
import boto3, os
from dotenv import load_dotenv
load_dotenv()
s3 = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'), region_name=os.getenv('AWS_REGION'))
try:
    s3.create_bucket(Bucket=os.getenv('S3_BUCKET'))
    print('Bucket created')
except s3.exceptions.BucketAlreadyOwnedByYou:
    print('Bucket already exists')
"

echo "=== Setup complete ==="
