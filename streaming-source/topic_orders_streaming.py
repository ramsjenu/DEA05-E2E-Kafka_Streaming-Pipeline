import json
import random
import time
from faker import Faker
from kafka import KafkaProducer

# Initialize Faker and Kafka producer
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def generate_order():
    """Generate one random order record"""
    return {
        "order_id": random.randint(1, 50),
        "order_date": str(fake.date_between(start_date='-1y', end_date='today')),
        "order_amount": random.randint(1, 1000),
        "customer_id": random.randint(1, 50),
    }

def produce_orders(topic="topic_orders", num_messages=20, delay=1):
    """Produce random orders to Kafka"""
    for i in range(num_messages):
        order = generate_order()
        producer.send(topic, value=order)
        producer.flush()
        print(f"✅ Sent: {order}")
        time.sleep(delay)

    producer.flush()
    print(f"\n✨ Successfully sent {num_messages} messages to topic '{topic}'.")

if __name__ == "__main__":
    produce_orders(num_messages=20, delay=1)
