import time
import threading
import redis


class BlockedSubscriber(threading.Thread):
    def __init__(self, host="localhost", port=6379, channel="flood"):
        super().__init__(daemon=True)
        self.client = redis.Redis(host=host, port=port, decode_responses=True)
        self.pubsub = self.client.pubsub()
        self.channel = channel
        self.received = 0

    def run(self):
        self.pubsub.subscribe(self.channel)
        print("[Subscriber] Subscribed — blocked for 60s per message (never drains buffer)\n")

        try:
            for message in self.pubsub.listen():
                if message["type"] == "message":
                    self.received += 1
                    mb = len(message["data"]) / 1_000_000
                    print(f"[Subscriber] Received message #{self.received} ({mb:.1f} MB) — blocking for 60s...")
                    time.sleep(60)  # effectively never processes the next message
        except Exception as e:
            print(f"\n[Subscriber] DISCONNECTED by Redis: {type(e).__name__}: {e}")
            print(f"[Subscriber] Only received {self.received} message(s) before disconnect")


def main():
    channel = "flood"
    sub = BlockedSubscriber(channel=channel)
    sub.start()
    time.sleep(0.3)

    client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    payload = "X" * 1_000_000  # 1 MB per message — need ~33 to hit 32 MB hard limit

    print(f"[Publisher] Sending 1 MB messages until Redis disconnects the subscriber...\n")

    total_sent = 0
    total_mb = 0
    while sub.is_alive():
        receivers = client.publish(channel, payload)
        total_sent += 1
        total_mb += 1
        print(f"[Publisher] Sent #{total_sent} ({total_mb} MB total) | Receivers: {receivers}")

        if receivers == 0:
            print(f"\n[Publisher] Receiver count dropped to 0 — subscriber was disconnected by Redis!")
            break

        time.sleep(0.05)  # fast but not instant — let Redis buffer fill up

    # Give subscriber thread time to print its disconnect message
    time.sleep(1)

    print(f"\n[Demo] Done. Sent {total_sent} x 1MB = {total_mb} MB before overflow.")


if __name__ == "__main__":
    main()