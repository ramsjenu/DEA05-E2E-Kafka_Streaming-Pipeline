import json
import time
import pandas as pd
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def produce_orders_from_csv(csv_path, topic="topic_orders", delay=1):
    """Read CSV and produce records to Kafka"""
    try:
        df = pd.read_csv(csv_path)
        print(f"✅ Loaded {len(df)} records from {csv_path}")
    except Exception as e:
        print(f"❌ Failed to read CSV file: {e}")
        return

    for _, row in df.iterrows():
        # Convert row to dict (JSON serializable)
        order = row.to_dict()
        producer.send(topic, value=order)
        producer.flush()
        print(f"✅ Sent: {order}")
        time.sleep(delay)

    producer.flush()
    print(f"\n✨ Successfully sent {len(df)} messages to topic '{topic}'.")

if __name__ == "__main__":
    # Example usage
    csv_path = "../data/orders.csv"  # Replace with your actual CSV file path
    produce_orders_from_csv(csv_path=csv_path, topic="topic_orders", delay=1)
