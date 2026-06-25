"""
Race condition demo: what happens when you do read-modify-write manually
instead of using INCR.

Each worker reads the counter, adds 1 in Python, then writes it back.
Under concurrency this loses increments because two workers can read the
same value before either writes back.

Run demo.py to see INCR fix this completely.
"""
import threading
import redis

KEY = "page_views_naive"
WORKERS = 10
INCREMENTS_PER_WORKER = 100
EXPECTED = WORKERS * INCREMENTS_PER_WORKER


def worker(client: redis.Redis, worker_id: int, barrier: threading.Barrier):
    barrier.wait()  # all workers start together to maximise contention
    for _ in range(INCREMENTS_PER_WORKER):
        # Non-atomic: read → increment in Python → write back
        current = int(client.get(KEY) or 0)
        client.set(KEY, current + 1)
    print(f"  [worker-{worker_id:02d}] done ({INCREMENTS_PER_WORKER} increments)")


def main():
    client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    client.delete(KEY)
    client.set(KEY, 0)

    barrier = threading.Barrier(WORKERS)
    threads = [
        threading.Thread(target=worker, args=(client, i, barrier), daemon=True)
        for i in range(1, WORKERS + 1)
    ]

    print(f"\n--- {WORKERS} workers, {INCREMENTS_PER_WORKER} increments each (non-atomic) ---")
    print(f"--- Expected final count: {EXPECTED} ---\n")

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = int(client.get(KEY))
    lost = EXPECTED - final

    print(f"\n--- Result ---")
    print(f"  Expected : {EXPECTED}")
    print(f"  Actual   : {final}")
    print(f"  Lost     : {lost}")
    print(f"\n✗ {lost} increments lost to race conditions.")
    print("  Use INCR (demo.py) to eliminate this entirely.\n")


if __name__ == "__main__":
    main()
