import json
import time
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime, UTC

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv("paysim.csv")

print("🚀 Streaming started...")

for _, row in df.iterrows():
    event = row.to_dict()

    # Add timezone-aware timestamp
    event["producer_timestamp"] = datetime.now(UTC).isoformat()

    producer.send("transactions_raw", event)

    # Slow down for readability
    time.sleep(0.2)

producer.flush()
print("✅ Streaming completed.")