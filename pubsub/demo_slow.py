import time
import threading
import redis
from publisher import Publisher


class SlowSubscriber(threading.Thread):
    def __init__(self, host="localhost", port=6379, channels=None, process_delay=2):
        super().__init__(daemon=True)
        self.client = redis.Redis(host=host, port=port, decode_responses=True)
        self.pubsub = self.client.pubsub()
        self.channels = channels or ["default"]
        self.process_delay = process_delay
        self._stop_event = threading.Event()

    def run(self):
        self.pubsub.subscribe(*self.channels)
        print(f"[SlowSubscriber] Subscribed to: {self.channels}")

        try:
            for message in self.pubsub.listen():
                if self._stop_event.is_set():
                    break
                if message["type"] == "message":
                    print(f"[SlowSubscriber] Received: {message['data']!r} — processing for {self.process_delay}s...")
                    time.sleep(self.process_delay)  # simulate slow work
                    print(f"[SlowSubscriber] Done processing: {message['data']!r}")
        except Exception:
            pass

    def stop(self):
        self._stop_event.set()
        self.pubsub.unsubscribe()
        self.pubsub.close()


def main():
    sub = SlowSubscriber(channels=["tasks"], process_delay=2)
    sub.start()
    time.sleep(0.3)

    pub = Publisher()

    # Publish 4 messages rapidly — subscriber takes 2s each
    print("\n[Demo] Publishing 4 messages rapidly...\n")
    for i in range(1, 5):
        pub.publish("tasks", f"Task-{i}")
        time.sleep(0.1)  # near-simultaneous

    # Wait long enough for all 4 to be processed (4 * 2s = 8s)
    print("\n[Demo] Waiting for slow subscriber to drain...\n")
    time.sleep(9)

    sub.stop()
    print("\n[Demo] Done. All 4 messages were buffered and processed in order.")


if __name__ == "__main__":
    main()