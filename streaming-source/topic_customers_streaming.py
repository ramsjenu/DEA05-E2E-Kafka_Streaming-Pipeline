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

def generate_customer():
    """Generate one random order record"""
    return {
        "customer_id": random.randint(1, 50),
        "name": str(fake.name()),
        "email": str(fake.email()),
        "region": str(fake.state()),
        "customer_tenure_days": random.randint(1, 100),
    }

def produce_customers(topic="topic_customers", num_messages=20, delay=1):
    """Produce random customers to Kafka"""
    for i in range(num_messages):
        customer = generate_customer()
        producer.send(topic, value=customer)
        producer.flush()
        print(f"✅ Sent: {customer}")
        time.sleep(delay)

    producer.flush()
    print(f"\n✨ Successfully sent {num_messages} messages to topic '{topic}'.")

if __name__ == "__main__":
    produce_customers(num_messages=20, delay=1)
