#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"
source venv/bin/activate

echo "=== Starting VitalSync Pipeline ==="

# 1. Ensure Kafka is running
echo "[1/5] Checking Kafka..."
docker compose up -d
sleep 5

# 2. Start simulator in background
echo "[2/5] Starting data simulator (10 users, 5s interval)..."
python -u simulator/simulator.py &
SIM_PID=$!

# 3. Start Spark streams in background
echo "[3/5] Starting Spark streams..."
cd spark
python -u bronze_stream.py &
BRONZE_PID=$!
sleep 5
python -u silver_stream.py &
SILVER_PID=$!
sleep 5
python -u gold_stream.py &
GOLD_PID=$!
cd "$PROJECT_DIR"

# 4. Start API
echo "[4/5] Starting API server..."
uvicorn api.main:app --port 8000 &
API_PID=$!
sleep 2

# 5. Start frontend
echo "[5/5] Starting frontend..."
cd frontend && npx ts-node src/index.ts &
FE_PID=$!
cd "$PROJECT_DIR"

echo ""
echo "=== VitalSync is running ==="
echo "  Dashboard: http://localhost:3000"
echo "  API:       http://localhost:8000"
echo ""
echo "PIDs: simulator=$SIM_PID bronze=$BRONZE_PID silver=$SILVER_PID gold=$GOLD_PID api=$API_PID frontend=$FE_PID"
echo "Press Ctrl+C to stop all..."

trap "kill $SIM_PID $BRONZE_PID $SILVER_PID $GOLD_PID $API_PID $FE_PID 2>/dev/null; exit" INT TERM
wait
