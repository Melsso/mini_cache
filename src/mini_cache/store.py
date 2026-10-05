from __future__ import annotations

import heapq
import time
from dataclasses import dataclass
from typing import Any


@dataclass
class _Entry:
    value: Any
    expire_at: float | None


class Store:
    def __init__(self) -> None:
        self._data: dict[str, _Entry] = {}
        self._expiry_heap: list[tuple[float, str]] = []

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        expire_at = (time.monotonic() + ttl) if ttl is not None else None
        self._data[key] = _Entry(value=value, expire_at=expire_at)
        if expire_at is not None:
            self._schedule(key, expire_at)

    def get(self, key: str) -> Any | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        if self._is_expired(entry):
            del self._data[key]
            return None
        return entry.value

    def delete(self, key: str) -> bool:
        entry = self._data.get(key)
        if entry is None:
            return False
        del self._data[key]
        return not self._is_expired(entry)

    def exists(self, key: str) -> bool:
        return self.get(key) is not None

    def expire(self, key: str, ttl: float) -> bool:
        entry = self._data.get(key)
        if entry is None or self._is_expired(entry):
            self._data.pop(key, None)
            return False
        entry.expire_at = time.monotonic() + ttl
        self._schedule(key, entry.expire_at)
        return True

    def expire_at_unix(self, key: str, unix_seconds: float) -> bool:
        return self.expire(key, unix_seconds - time.time())

    def ttl(self, key: str) -> float | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        if self._is_expired(entry):
            del self._data[key]
            return None
        if entry.expire_at is None:
            return -1.0
        return max(0.0, entry.expire_at - time.monotonic())

    def persist(self, key: str) -> bool:
        entry = self._data.get(key)
        if entry is None or self._is_expired(entry):
            self._data.pop(key, None)
            return False
        if entry.expire_at is None:
            return False
        entry.expire_at = None
        return True

    def keys(self) -> list[str]:
        self.sweep_expired()
        return list(self._data.keys())

    def flush(self) -> None:
        self._data.clear()
        self._expiry_heap.clear()

    def __len__(self) -> int:
        self.sweep_expired()
        return len(self._data)

    def sweep_expired(self) -> int:
        now = time.monotonic()
        heap = self._expiry_heap
        removed = 0
        while heap and heap[0][0] <= now:
            expire_at, key = heapq.heappop(heap)
            entry = self._data.get(key)
            if entry is not None and entry.expire_at == expire_at:
                del self._data[key]
                removed += 1
        return removed

    def _schedule(self, key: str, expire_at: float) -> None:
        heapq.heappush(self._expiry_heap, (expire_at, key))
        if len(self._expiry_heap) > 4 * len(self._data) + 1024:
            rebuilt: list[tuple[float, str]] = []
            for k, e in self._data.items():
                if e.expire_at is not None:
                    rebuilt.append((e.expire_at, k))
            heapq.heapify(rebuilt)
            self._expiry_heap = rebuilt

    @staticmethod
    def _is_expired(entry: _Entry) -> bool:
        return entry.expire_at is not None and entry.expire_at <= time.monotonic()
