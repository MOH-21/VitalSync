"""VitalSync health data simulator — publishes synthetic patient events to Kafka."""

import argparse
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

import os

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]


def generate_event(user_id: str) -> dict:
    """Return a single simulated health event for *user_id*."""

    # heart_rate: ~5 % chance of a spike to 130-140, otherwise 55-120
    if random.random() < 0.05:
        heart_rate = random.randint(130, 140)
    else:
        heart_rate = random.randint(55, 120)

    # steps: 0-50 per interval
    steps = random.randint(0, 50)

    # spo2: ~5 % chance of a dip to 88.0-91.0, otherwise 94.0-100.0
    if random.random() < 0.05:
        spo2 = round(random.uniform(88.0, 91.0), 1)
    else:
        spo2 = round(random.uniform(94.0, 100.0), 1)

    return {
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "heart_rate": heart_rate,
        "steps": steps,
        "spo2": spo2,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VitalSync health data simulator")
    parser.add_argument(
        "--users",
        type=int,
        default=10,
        help="Number of simulated users (default: 10)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Seconds between batches (default: 5.0)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    user_ids = [f"user_{i:03d}" for i in range(1, args.users + 1)]

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=100,
        batch_size=32768,
        compression_type="lz4",
    )

    print(
        f"Simulator started — {len(user_ids)} users, "
        f"{args.interval}s interval, broker={KAFKA_BROKER}, topic={KAFKA_TOPIC}"
    )

    batch_num = 0
    try:
        while True:
            batch_num += 1
            for uid in user_ids:
                event = generate_event(uid)
                producer.send(KAFKA_TOPIC, key=uid, value=event)

            print(f"Sent batch {batch_num}: {len(user_ids)} events")
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nShutting down — flushing producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")


if __name__ == "__main__":
    main()
