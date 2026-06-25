import redis
import time


class Publisher:
    def __init__(self, host="localhost", port=6379):
        self.client = redis.Redis(host=host, port=port, decode_responses=True)

    def publish(self, channel: str, message: str):
        receivers = self.client.publish(channel, message)
        print(f"[Publisher] Channel: {channel} | Message: {message!r} | Receivers: {receivers}")
        return receivers