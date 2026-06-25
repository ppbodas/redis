"""
Consumer group demo: one producer, three consumers in the same group.

Each message goes to exactly ONE consumer (load-balanced), not all of them.
This is the opposite of Pub/Sub where every subscriber gets every message.
"""
import time
import threading
from producer import Producer
from consumer import Consumer

STREAM = "tasks"
GROUP = "task-workers"

# Track which consumer handled which message
results: dict[str, list] = {"worker-1": [], "worker-2": [], "worker-3": []}
lock = threading.Lock()


def make_handler(name: str):
    def handle(msg_id: str, data: dict):
        with lock:
            results[name].append(data["task"])
        print(f"  [{name}] ✓ {msg_id} | task={data['task']}")
    return handle


def main():
    client = __import__("redis").Redis(host="localhost", port=6379, decode_responses=True)
    client.delete(STREAM)

    consumers = [
        Consumer(stream=STREAM, group=GROUP, name=n, process_fn=make_handler(n))
        for n in ["worker-1", "worker-2", "worker-3"]
    ]
    for c in consumers:
        c.start()
    time.sleep(0.3)

    producer = Producer()
    total = 9

    print(f"\n--- Sending {total} tasks to {len(consumers)} workers ---\n")
    for i in range(1, total + 1):
        producer.send(STREAM, {"task": f"task-{i}"})
        time.sleep(0.05)

    time.sleep(1.5)

    print("\n--- Distribution (each task delivered to exactly one worker) ---")
    for name, tasks in results.items():
        print(f"  {name}: {tasks}")

    for c in consumers:
        c.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
