import json
import time
import io
import boto3
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime

class KafkaS3JsonConsumer:
    def __init__(self, topic, s3_bucket, s3_prefix, batch_size=10, flush_interval=5):
        self.topic = topic
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer = []

        # S3 client
        self.s3 = boto3.client("s3")

        # Kafka consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=f"s3-consumer-group-{int(time.time())}",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        print(f"✅ Initialized Kafka consumer for topic '{self.topic}'")

    def _write_to_s3(self):
        if not self.buffer:
            return

        # Add ingest time to each record
        ingest_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for record in self.buffer:
            record["ingest_time"] = ingest_time

        partition_date = datetime.utcnow().strftime("%Y-%m-%d")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        s3_key = f"{self.s3_prefix}/{self.topic}/date={partition_date}/{self.topic}_{timestamp}.json"

        # Convert buffer to JSON lines
        json_buffer = "\n".join([json.dumps(record) for record in self.buffer])

        # Upload to S3
        self.s3.put_object(
            Bucket=self.s3_bucket,
            Key=s3_key,
            Body=json_buffer.encode("utf-8"),
            ServerSideEncryption="AES256"
        )

        print(f"🪣 Uploaded {len(self.buffer)} records → s3://{self.s3_bucket}/{s3_key}")
        self.buffer.clear()

    def consume_and_write(self):
        print(f"🚀 Streaming from Kafka topic '{self.topic}' to S3 continuously...")
        last_flush_time = time.time()

        try:
            while True:
                msgs = self.consumer.poll(timeout_ms=500, max_records=10)
                for tp, messages in msgs.items():
                    for msg in messages:
                        self.buffer.append(msg.value)

                current_time = time.time()
                if len(self.buffer) >= self.batch_size or (current_time - last_flush_time) >= self.flush_interval:
                    self._write_to_s3()
                    last_flush_time = current_time

        except KeyboardInterrupt:
            print("\n🛑 Interrupted — flushing remaining records...")
            if self.buffer:
                self._write_to_s3()

        finally:
            self.consumer.close()
            print("✅ Consumer stopped cleanly.")


# ------------------- Usage -------------------
if __name__ == "__main__":
    consumer = KafkaS3JsonConsumer(
        topic="topic_orders",
        s3_bucket="vrams-data-lake-inbound",
        s3_prefix="inbound",
        batch_size=5,
        flush_interval=5
    )
    consumer.consume_and_write()
