"""
Distributed counter demo: multiple workers atomically incrementing a shared counter.

Redis INCR is atomic — no two clients can read-modify-write at the same time,
so every increment is guaranteed to land even under heavy concurrency.
"""
import threading
import redis

KEY = "page_views"
WORKERS = 10
INCREMENTS_PER_WORKER = 100
EXPECTED = WORKERS * INCREMENTS_PER_WORKER


def worker(client: redis.Redis, worker_id: int, barrier: threading.Barrier):
    barrier.wait()  # all workers start together to maximise contention
    for _ in range(INCREMENTS_PER_WORKER):
        client.incr(KEY)
    print(f"  [worker-{worker_id:02d}] done ({INCREMENTS_PER_WORKER} increments)")


def main():
    client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    client.delete(KEY)

    barrier = threading.Barrier(WORKERS)
    threads = [
        threading.Thread(target=worker, args=(client, i, barrier), daemon=True)
        for i in range(1, WORKERS + 1)
    ]

    print(f"\n--- {WORKERS} workers, {INCREMENTS_PER_WORKER} increments each ---")
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
    print(f"\n{'✓ No lost increments — INCR is atomic.' if lost == 0 else f'✗ {lost} increments lost.'}\n")


if __name__ == "__main__":
    main()
