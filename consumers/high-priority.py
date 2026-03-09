import json
import time
from kafka import KafkaConsumer
from datetime import datetime, UTC

# High priority consumer group
consumer = KafkaConsumer(
    "transactions_high_priority",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="high-priority-group"
)

print("🚨 High Priority Service started...")

for msg in consumer:
    record = msg.value

    # Simulate FAST processing (high SLA)
    time.sleep(0.005)

    # Get original producer timestamp
    producer_time = datetime.fromisoformat(
        record["transaction"]["producer_timestamp"]
    )

    # Current time when processing completes
    processed_time = datetime.now(UTC)

    # True end-to-end latency
    end_to_end_latency = (
        processed_time - producer_time
    ).total_seconds() * 1000

    print(
        f"[HIGH] Processed at {processed_time.isoformat()} | "
        f"End-to-End Latency = {end_to_end_latency:.2f} ms"
    )