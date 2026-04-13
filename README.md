# VitalSync

A real-time health data streaming pipeline that ingests simulated biometric data, processes it through a medallion architecture on S3, and delivers AI-powered health recommendations via a browser dashboard.

## Architecture

```
Simulator (Python)
    │ JSON events (heart rate, steps, SpO2)
    ▼
Kafka (Docker · KRaft · topic: health-events)
    │
    ▼
Spark Structured Streaming
    ├── Bronze  → raw Parquet → S3 bronze/
    ├── Silver  → cleaned & validated → S3 silver/
    └── Gold    → hourly aggregates per user → S3 gold/
                        │
                        ▼
              FastAPI (port 8000)
              reads gold layer · calls Anthropic API
                        │
                        ▼
              Express Dashboard (port 3000)
              patient selector · metric tabs · recommendations
```

## Stack

| Layer | Tech |
|-------|------|
| Ingestion | Python · kafka-python-ng |
| Broker | Apache Kafka 3.7 (KRaft, Docker) |
| Processing | PySpark 3.5.1 Structured Streaming |
| Storage | AWS S3 · Snappy Parquet |
| API | FastAPI · boto3 · PyArrow · Anthropic SDK |
| Frontend | Express · TypeScript · Vanilla JS/CSS |
| Tests | pytest |

## Quickstart

### Prerequisites
- Docker + Docker Compose
- Python 3.12+
- Node 18+
- Java 11+
- AWS account (free tier)

### Setup

```bash
cp .env.example .env
# Fill in AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, ANTHROPIC_API_KEY
```

```bash
./scripts/setup.sh
```

### Run

```bash
./scripts/start_all.sh
```

Dashboard → http://localhost:3000
API → http://localhost:8000

### Run individually

```bash
# Start Kafka
docker compose up -d

# Simulator (default: 10 users, 5s interval)
source venv/bin/activate
python simulator/simulator.py --users 10 --interval 5

# Spark layers (each in a separate terminal)
cd spark
python bronze_stream.py
python silver_stream.py
python gold_stream.py          # streaming mode
python gold_stream.py --batch  # batch mode (skip watermark wait)

# API
uvicorn api.main:app --port 8000

# Frontend
cd frontend && npx ts-node src/index.ts
```

## Data Pipeline

### Simulator
Generates 10 users (`user_001`–`user_010`) emitting health events every 5 seconds:

| Metric | Normal Range | Anomaly |
|--------|-------------|---------|
| heart_rate | 55–120 bpm | >130 bpm (~5%) |
| steps | 0–50 / interval | — |
| spo2 | 94.0–100.0% | <88% (~5%) |

### Medallion Layers

**Bronze** — raw ingestion with Kafka metadata (`partition`, `offset`, `ingestion_timestamp`). One file per 30s trigger, partitioned by date.

**Silver** — cleaned layer. Drops nulls, validates ranges (`heart_rate` 30–220, `spo2` 70–100, `steps` ≥ 0). Adds `is_anomaly` flag.

**Gold** — hourly aggregates per user with 15-minute watermark for late-arriving events. Columns: `avg_heart_rate`, `avg_spo2`, `total_steps`, `event_count`, `window_start`, `window_end`.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/users` | List users with gold data |
| GET | `/api/metrics` | Available metrics |
| GET | `/api/recommendations?user_id=X&metric_name=Y` | AI recommendation |

## Tests

```bash
source venv/bin/activate

# Data quality (requires gold data in S3)
pytest tests/test_data_quality.py -v

# API integration (requires API running on port 8000)
pytest tests/test_api.py -v
```

19 tests total — schema validation, null checks, range checks, anomaly flag logic, and all API endpoints.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `KAFKA_BROKER` | Kafka bootstrap server (default: `localhost:9092`) |
| `KAFKA_TOPIC` | Topic name (default: `health-events`) |
| `AWS_ACCESS_KEY_ID` | AWS credentials |
| `AWS_SECRET_ACCESS_KEY` | AWS credentials |
| `AWS_REGION` | S3 region (default: `us-east-1`) |
| `S3_BUCKET` | S3 bucket name (default: `vitalsync-data`) |
| `ANTHROPIC_API_KEY` | Anthropic API key |
