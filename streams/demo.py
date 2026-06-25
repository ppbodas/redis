"""
Basic Streams demo: one producer, one consumer.

Key difference from Pub/Sub:
- Messages are PERSISTED in the stream — consumer can read past messages.
- Acknowledgement (XACK) prevents redelivery on crash/restart.
"""
import time
from producer import Producer
from consumer import Consumer

STREAM = "orders"
GROUP = "order-processors"


def main():
    # Clean up stream from previous runs
    client = __import__("redis").Redis(host="localhost", port=6379, decode_responses=True)
    client.delete(STREAM)

    consumer = Consumer(stream=STREAM, group=GROUP, name="worker-1")
    consumer.start()
    time.sleep(0.3)

    producer = Producer()

    orders = [
        {"id": "101", "item": "keyboard", "qty": "1"},
        {"id": "102", "item": "monitor",  "qty": "2"},
        {"id": "103", "item": "mouse",    "qty": "3"},
    ]

    print("\n--- Producing messages ---\n")
    for order in orders:
        producer.send(STREAM, order)
        time.sleep(0.2)

    time.sleep(1)

    # Show stream is still there — messages persisted
    length = client.xlen(STREAM)
    print(f"\n--- Stream '{STREAM}' still holds {length} message(s) after consumption ---")
    print("(Unlike Pub/Sub, messages are not deleted on delivery)\n")

    consumer.stop()
    print("Done.")


if __name__ == "__main__":
    main()
