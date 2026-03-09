import json
import pickle
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from datetime import datetime, UTC

# Load ML model
with open("fraud_pipeline.pkl", "rb") as f:
    pipeline = pickle.load(f)

consumer = KafkaConsumer(
    "transactions_raw",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="ml-service-group"
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client["fraud_system"]
collection = db["transactions_scored"]

FEATURES = [
    "type",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest"
]

print("🧠 ML Service started...")

for message in consumer:
    event = message.value
    now = datetime.now(UTC)

    # Calculate latency
    producer_time = datetime.fromisoformat(event["producer_timestamp"])
    latency_ms = (now - producer_time).total_seconds() * 1000

    # Feature extraction
    features = {f: event.get(f) for f in FEATURES}
    df = pd.DataFrame([features])

    risk_score = pipeline.predict_proba(df)[0][1]

    priority = "LOW"
    if risk_score > 0.8 or event.get("type") in ["TRANSFER", "CASH_OUT"]:
        priority = "HIGH"

    record = {
        "transaction": event,
        "risk_score": float(risk_score),
        "priority": priority,
        "latency_ms": round(latency_ms, 2),
        "ingested_at": now.isoformat()
    }

    if priority == "HIGH":
        producer.send("transactions_high_priority", record)
    else:
        producer.send("transactions_low_priority", record)

    collection.insert_one(record)

    print(
        f"Processed | Risk={risk_score:.2f} | "
        f"Priority={priority} | Latency={latency_ms:.2f} ms"
    )