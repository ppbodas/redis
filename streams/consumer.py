import threading
import redis


class Consumer(threading.Thread):
    """
    Reads from a Redis Stream using a consumer group.
    Each instance is one named consumer within the group.
    Messages are acknowledged after processing so they won't be redelivered.
    """

    def __init__(self, stream: str, group: str, name: str,
                 host="localhost", port=6379, process_fn=None):
        super().__init__(daemon=True, name=name)
        self.client = redis.Redis(host=host, port=port, decode_responses=True)
        self.stream = stream
        self.group = group
        self.name = name
        self.process_fn = process_fn or self._default_process
        self._stop_event = threading.Event()

    def _ensure_group(self):
        try:
            # "0" starts reading from the beginning; use "$" to read only new messages
            self.client.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def _default_process(self, msg_id: str, data: dict):
        print(f"[{self.name}] Received {msg_id} | {data}")

    def run(self):
        self._ensure_group()
        print(f"[{self.name}] Ready — group: {self.group}, stream: {self.stream}")

        while not self._stop_event.is_set():
            # ">" means "give me messages not yet delivered to any consumer in this group"
            results = self.client.xreadgroup(
                groupname=self.group,
                consumername=self.name,
                streams={self.stream: ">"},
                count=1,
                block=1000,  # ms — unblocks every second to check stop_event
            )
            if not results:
                continue

            for _stream, messages in results:
                for msg_id, data in messages:
                    self.process_fn(msg_id, data)
                    self.client.xack(self.stream, self.group, msg_id)

    def stop(self):
        self._stop_event.set()
