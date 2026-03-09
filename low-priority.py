import json
import time
from kafka import KafkaConsumer
from datetime import datetime, UTC

# Low priority consumer group
consumer = KafkaConsumer(
    "transactions_low_priority",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="low-priority-group"
)

print("🟢 Low Priority Service started...")

for msg in consumer:
    record = msg.value

    # Simulate SLOW processing
    time.sleep(0.5)

    producer_time = datetime.fromisoformat(
        record["transaction"]["producer_timestamp"]
    )

    processed_time = datetime.now(UTC)

    end_to_end_latency = (
        processed_time - producer_time
    ).total_seconds() * 1000

    print(
        f"[LOW] Processed at {processed_time.isoformat()} | "
        f"End-to-End Latency = {end_to_end_latency:.2f} ms"
    )