import threading
import redis
import time


class Subscriber(threading.Thread):
    def __init__(self, host="localhost", port=6379, channels=None):
        super().__init__(daemon=True)
        self.client = redis.Redis(host=host, port=port, decode_responses=True)
        self.pubsub = self.client.pubsub()
        self.channels = channels or ["default"]
        self._stop_event = threading.Event()

    def run(self):
        self.pubsub.subscribe(*self.channels)
        print(f"[Subscriber] Subscribed to: {self.channels}")

        try:
            for message in self.pubsub.listen():
                if self._stop_event.is_set():
                    print("Received stop event")
                    break
                if message["type"] == "message":
                    print(
                        f"[Subscriber] Channel: {message['channel']} | "
                        f"Data: {message['data']}"
                    )
        except Exception:
            pass

    def stop(self):
        self._stop_event.set()
        self.pubsub.unsubscribe()
        self.pubsub.close()