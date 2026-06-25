"""
Consistent hash ring for client-side sharding.

Each physical shard is represented by several virtual nodes so that
adding or removing a shard redistributes only ~1/N of the keys instead
of rehashing everything (the classic modular-hashing problem).
"""
import hashlib
from typing import Any


class HashRing:
    def __init__(self, shards: list[str], virtual_nodes: int = 150):
        self._ring: dict[int, str] = {}
        self._sorted_keys: list[int] = []

        for shard in shards:
            for i in range(virtual_nodes):
                point = self._hash(f"{shard}:{i}")
                self._ring[point] = shard
        self._sorted_keys = sorted(self._ring)

    def get_shard(self, key: str) -> str:
        h = self._hash(key)
        for ring_key in self._sorted_keys:
            if h <= ring_key:
                return self._ring[ring_key]
        return self._ring[self._sorted_keys[0]]  # wrap around

    @staticmethod
    def _hash(value: str) -> int:
        return int(hashlib.md5(value.encode()).hexdigest(), 16)
