import redis


class Producer:
    def __init__(self, host="localhost", port=6379):
        self.client = redis.Redis(host=host, port=port, decode_responses=True)

    def send(self, stream: str, data: dict) -> str:
        """Append an entry to the stream. Returns the auto-generated message ID."""
        msg_id = self.client.xadd(stream, data)
        print(f"[Producer] Stream: {stream} | ID: {msg_id} | Data: {data}")
        return msg_id

    def trim(self, stream: str, max_len: int):
        """Keep only the latest max_len entries in the stream."""
        self.client.xtrim(stream, maxlen=max_len)
