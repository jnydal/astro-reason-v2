"""
One-off: seek astro-interpreter consumer group to end of topic so all pending
messages are skipped. Run with the astro-interpreter worker stopped.

  docker compose stop astro-interpreter
  docker compose run --rm astro-interpreter python -m service.worker_astro_interpret.app.seek_astro_end
  docker compose start astro-interpreter
"""
from __future__ import annotations

import os

from confluent_kafka import Consumer, TopicPartition

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "astro-interpreter")
KAFKA_TOPIC = os.getenv("KAFKA_ASTRO_TOPIC", "astro")


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    # Get partition assignment
    for _ in range(50):
        consumer.poll(1.0)
    parts = list(consumer.assignment())
    if not parts:
        print("No partition assignment (topic may be empty or broker unreachable). Exiting.")
        consumer.close()
        return
    for tp in parts:
        low, high = consumer.get_watermark_offsets(tp)
        consumer.seek(TopicPartition(tp.topic, tp.partition, high))
    consumer.commit()
    print(f"Seeked group '{KAFKA_GROUP_ID}' to end of topic '{KAFKA_TOPIC}' (partitions: {parts}).")
    consumer.close()


if __name__ == "__main__":
    main()
