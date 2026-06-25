"""
Client-side sharding demo using a consistent hash ring.

Three Redis DBs (0, 1, 2) act as independent shards. A HashRing maps
each key to exactly one shard via consistent hashing so:
  - Keys are spread evenly without a central coordinator.
  - Adding/removing a shard only moves ~1/N of keys (not all of them).

This is how redis-py's now-removed ShardedRedis worked and how tools
like Twemproxy/Nutcracker implement transparent sharding.
"""
import redis
from hash_ring import HashRing

SHARD_IDS = ["shard-0", "shard-1", "shard-2"]

# Simulate three shards using different Redis DB indices
SHARD_CLIENTS = {
    "shard-0": redis.Redis(host="localhost", port=6379, db=0, decode_responses=True),
    "shard-1": redis.Redis(host="localhost", port=6379, db=1, decode_responses=True),
    "shard-2": redis.Redis(host="localhost", port=6379, db=2, decode_responses=True),
}

SAMPLE_KEYS = [
    "user:1001", "user:1002", "user:1003", "user:1004", "user:1005",
    "session:abc", "session:def", "session:ghi", "session:jkl",
    "product:42", "product:43", "product:100", "product:101",
    "order:9001", "order:9002", "order:9003",
    "cache:homepage", "cache:trending", "cache:featured",
    "rate:ip:10.0.0.1", "rate:ip:10.0.0.2",
]


def get_client(ring: HashRing, key: str) -> redis.Redis:
    return SHARD_CLIENTS[ring.get_shard(key)]


def flush_all_shards():
    for client in SHARD_CLIENTS.values():
        client.flushdb()


def main():
    ring = HashRing(SHARD_IDS)
    flush_all_shards()

    # ── Write keys across shards ─────────────────────────────────────────
    print(f"\n--- Writing {len(SAMPLE_KEYS)} keys across {len(SHARD_IDS)} shards ---\n")
    for key in SAMPLE_KEYS:
        shard = ring.get_shard(key)
        get_client(ring, key).set(key, f"value:{key}")
        print(f"  SET  {key:30s} → {shard}")

    # ── Distribution summary ─────────────────────────────────────────────
    print("\n--- Key distribution ---\n")
    for shard_id, client in SHARD_CLIENTS.items():
        count = client.dbsize()
        bar = "█" * count
        print(f"  {shard_id}  {bar}  ({count} keys)")

    # ── Read a key — must route to the same shard ────────────────────────
    print("\n--- Reading keys (each routed to its shard) ---\n")
    for key in SAMPLE_KEYS[:5]:
        shard = ring.get_shard(key)
        value = get_client(ring, key).get(key)
        print(f"  GET  {key:30s} ← {shard}  →  {value}")

    # ── Show what happens when a shard is added ──────────────────────────
    print("\n--- Adding a 4th shard: how many keys move? ---\n")
    new_shard_ids = SHARD_IDS + ["shard-3"]
    new_ring = HashRing(new_shard_ids)

    moved = sum(
        1 for key in SAMPLE_KEYS
        if new_ring.get_shard(key) != ring.get_shard(key)
    )
    pct = moved / len(SAMPLE_KEYS) * 100
    print(f"  Keys that move to new shard : {moved}/{len(SAMPLE_KEYS)}  ({pct:.0f}%)")
    print(f"  Keys that stay in place     : {len(SAMPLE_KEYS) - moved}/{len(SAMPLE_KEYS)}")
    print(f"  Ideal (1/N of keys)         : ~{100/len(new_shard_ids):.0f}%")
    print()

    flush_all_shards()


if __name__ == "__main__":
    main()
