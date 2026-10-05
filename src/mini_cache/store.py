from __future__ import annotations

import heapq
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

POLICIES = ("allkeys-lru", "noeviction")
ENTRY_OVERHEAD = 96

_INT_RE = re.compile(r"-?[0-9]+")
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def _size(item: Any) -> int:
    return len(item) if isinstance(item, (bytes, bytearray, str)) else 8


def _stored_int(value: Any) -> int:
    if isinstance(value, (bytes, bytearray)):
        text = bytes(value).decode("ascii", errors="replace")
    elif isinstance(value, str):
        text = value
    else:
        raise ValueError("value is not an integer or out of range")
    if not _INT_RE.fullmatch(text):
        raise ValueError("value is not an integer or out of range")
    number = int(text)
    if not _INT64_MIN <= number <= _INT64_MAX:
        raise ValueError("value is not an integer or out of range")
    return number


@dataclass
class _Entry:
    value: Any
    expire_at: float | None
    size: int = 0


class Store:
    def __init__(self, maxmemory: int = 0, policy: str = "allkeys-lru") -> None:
        if policy not in POLICIES:
            raise ValueError(f"policy must be one of {POLICIES}, got {policy!r}")
        if maxmemory < 0:
            raise ValueError("maxmemory must be >= 0 (0 means unlimited)")
        self._data: OrderedDict[Any, _Entry] = OrderedDict()
        self._expiry_heap: list[tuple[float, Any]] = []
        self.maxmemory = maxmemory
        self.policy = policy
        self.used_memory = 0
        self.writes = 0
        self.hits = 0
        self.misses = 0
        self.expired_keys = 0
        self.evicted_keys = 0
        self._evicted: list[Any] = []

    def set(self, key: Any, value: Any, ttl: float | None = None) -> None:
        self.writes += 1
        expire_at = (time.monotonic() + ttl) if ttl is not None else None
        self._put(key, _Entry(value=value, expire_at=expire_at))
        if expire_at is not None:
            self._schedule(key, expire_at)
        self._evict()

    def incr(self, key: Any, delta: int) -> int:
        entry = self._data.get(key)
        if entry is not None and self._is_expired(entry):
            self._remove(key, expired=True)
            entry = None
        sample = key if entry is None else entry.value
        current = 0 if entry is None else _stored_int(entry.value)
        new = current + delta
        if not _INT64_MIN <= new <= _INT64_MAX:
            raise ValueError("increment or decrement would overflow")
        text = str(new)
        value: Any = text.encode() if isinstance(sample, (bytes, bytearray)) else text
        self.writes += 1
        if entry is None:
            self._put(key, _Entry(value=value, expire_at=None))
        else:
            self._data.move_to_end(key)
            self.used_memory -= entry.size
            entry.value = value
            entry.size = self._cost(key, value)
            self.used_memory += entry.size
        self._evict()
        return new

    def delete(self, key: Any) -> bool:
        entry = self._data.get(key)
        if entry is None:
            return False
        expired = self._is_expired(entry)
        self._remove(key, expired=expired)
        return not expired

    def expire(self, key: Any, ttl: float) -> bool:
        entry = self._data.get(key)
        if entry is None:
            return False
        if self._is_expired(entry):
            self._remove(key, expired=True)
            return False
        entry.expire_at = time.monotonic() + ttl
        self._schedule(key, entry.expire_at)
        return True

    def expire_at_unix(self, key: Any, unix_seconds: float) -> bool:
        return self.expire(key, unix_seconds - time.time())

    def persist(self, key: Any) -> bool:
        entry = self._data.get(key)
        if entry is None:
            return False
        if self._is_expired(entry):
            self._remove(key, expired=True)
            return False
        if entry.expire_at is None:
            return False
        entry.expire_at = None
        return True

    def flush(self) -> None:
        self._data.clear()
        self._expiry_heap.clear()
        self.used_memory = 0

    def get(self, key: Any) -> Any | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        if self._is_expired(entry):
            self._remove(key, expired=True)
            return None
        self._data.move_to_end(key)
        return entry.value

    def exists(self, key: Any) -> bool:
        return self.get(key) is not None

    def ttl(self, key: Any) -> float | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        if self._is_expired(entry):
            self._remove(key, expired=True)
            return None
        if entry.expire_at is None:
            return -1.0
        return max(0.0, entry.expire_at - time.monotonic())

    def keys(self) -> list[Any]:
        self.sweep_expired()
        return list(self._data.keys())

    def snapshot(self) -> list[tuple[Any, Any, float | None]]:
        self.sweep_expired()
        now = time.monotonic()
        out: list[tuple[Any, Any, float | None]] = []
        for key, entry in self._data.items():
            if entry.expire_at is not None and entry.expire_at <= now:
                continue
            ttl = None if entry.expire_at is None else entry.expire_at - now
            out.append((key, entry.value, ttl))
        return out

    def __len__(self) -> int:
        self.sweep_expired()
        return len(self._data)

    def over_limit(self) -> bool:
        return self.maxmemory > 0 and self.used_memory > self.maxmemory

    def drain_evictions(self) -> list[Any]:
        evicted, self._evicted = self._evicted, []
        return evicted

    def sweep_expired(self) -> int:
        now = time.monotonic()
        heap = self._expiry_heap
        removed = 0
        while heap and heap[0][0] <= now:
            expire_at, key = heapq.heappop(heap)
            entry = self._data.get(key)
            if entry is not None and entry.expire_at == expire_at:
                self._remove(key, expired=True)
                removed += 1
        return removed

    @staticmethod
    def _cost(key: Any, value: Any) -> int:
        return ENTRY_OVERHEAD + _size(key) + _size(value)

    def _put(self, key: Any, entry: _Entry) -> None:
        old = self._data.get(key)
        if old is not None:
            self.used_memory -= old.size
        entry.size = self._cost(key, entry.value)
        self.used_memory += entry.size
        self._data[key] = entry
        self._data.move_to_end(key)

    def _remove(self, key: Any, *, expired: bool = False) -> None:
        entry = self._data.pop(key, None)
        if entry is not None:
            self.used_memory -= entry.size
            if expired:
                self.expired_keys += 1

    def _evict(self) -> None:
        if self.policy != "allkeys-lru" or not self.over_limit():
            return
        self.sweep_expired()
        while self.over_limit() and len(self._data) > 1:
            key = next(iter(self._data))
            self._remove(key)
            self.evicted_keys += 1
            self._evicted.append(key)

    def _schedule(self, key: Any, expire_at: float) -> None:
        heapq.heappush(self._expiry_heap, (expire_at, key))
        if len(self._expiry_heap) > 4 * len(self._data) + 1024:
            rebuilt: list[tuple[float, Any]] = []
            for k, e in self._data.items():
                if e.expire_at is not None:
                    rebuilt.append((e.expire_at, k))
            heapq.heapify(rebuilt)
            self._expiry_heap = rebuilt

    @staticmethod
    def _is_expired(entry: _Entry) -> bool:
        return entry.expire_at is not None and entry.expire_at <= time.monotonic()
