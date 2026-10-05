import time

from mini_cache.store import Store

COST = 96 + 2 + 10


def fill(store, count=3):
    for i in range(count):
        store.set(f"k{i}", b"x" * 10)


def test_lru_eviction_drops_the_least_recently_used_key():
    store = Store(maxmemory=3 * COST)
    fill(store)
    store.get("k0")
    store.set("k3", b"x" * 10)
    assert store.get("k1") is None
    assert all(store.get(k) is not None for k in ("k0", "k2", "k3"))
    assert store.evicted_keys == 1
    assert store.used_memory <= store.maxmemory
    assert store.drain_evictions() == ["k1"]
    assert store.drain_evictions() == []


def test_snapshot_does_not_change_the_lru_order():
    store = Store(maxmemory=3 * COST)
    fill(store)
    store.snapshot()
    store.set("k3", b"x" * 10)
    assert store.get("k0") is None


def test_noeviction_keeps_everything_and_reports_over_limit():
    store = Store(maxmemory=200, policy="noeviction")
    store.set("a", b"x" * 100)
    store.set("b", b"x" * 100)
    assert len(store) == 2 and store.over_limit() and store.evicted_keys == 0


def test_a_single_oversized_entry_is_kept():
    store = Store(maxmemory=10)
    store.set("a", b"x" * 100)
    assert store.get("a") == b"x" * 100


def test_used_memory_tracks_overwrites_and_removals():
    store = Store()
    store.set("a", b"1")
    assert store.used_memory == 96 + 1 + 1
    store.set("a", b"12345")
    assert store.used_memory == 96 + 1 + 5
    store.set("t", b"v", ttl=0.01)
    store.incr("n", 1)
    time.sleep(0.02)
    store.sweep_expired()
    assert store.expired_keys == 1
    store.delete("a")
    store.delete("n")
    assert store.used_memory == 0
    store.set("z", b"1")
    store.flush()
    assert store.used_memory == 0
