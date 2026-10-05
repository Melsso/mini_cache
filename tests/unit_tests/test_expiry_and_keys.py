import time

import pytest

from mini_cache.commands import dispatch
from mini_cache.protocol import Error
from mini_cache.store import Store


@pytest.fixture
def store():
    return Store()


def fake_clock(monkeypatch, start=1000.0):
    now = [start]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    return now


def test_sweep_removes_only_expired_keys_in_deadline_order(monkeypatch):
    now = fake_clock(monkeypatch)
    store = Store()
    for i in range(100):
        store.set(f"k{i}", i, ttl=i + 1)
    store.set("forever", "x")

    now[0] += 50.5
    assert store.sweep_expired() == 50
    assert store.get("k49") is None
    assert store.get("k50") == 50
    assert store.get("forever") == "x"


def test_overwriting_a_key_without_ttl_cancels_its_old_deadline(monkeypatch):
    now = fake_clock(monkeypatch)
    store = Store()
    store.set("a", 1, ttl=5)
    store.set("a", 2)

    now[0] += 10
    assert store.sweep_expired() == 0
    assert store.get("a") == 2


def test_persist_cancels_deadline(monkeypatch):
    now = fake_clock(monkeypatch)
    store = Store()
    store.set("a", 1, ttl=5)
    assert store.persist("a") is True

    now[0] += 10
    assert store.sweep_expired() == 0
    assert store.get("a") == 1


def test_re_expire_extends_deadline(monkeypatch):
    now = fake_clock(monkeypatch)
    store = Store()
    store.set("a", 1, ttl=5)
    store.expire("a", 100)

    now[0] += 6
    assert store.sweep_expired() == 0
    assert store.get("a") == 1


def test_stale_heap_entries_are_compacted(monkeypatch):
    fake_clock(monkeypatch)
    store = Store()
    store.set("k", "v")
    for i in range(10_000):
        store.expire("k", 1000 + i)
    assert len(store._expiry_heap) < 2_000
    assert store.get("k") == "v"


def test_flush_clears_expiry_heap():
    store = Store()
    store.set("a", 1, ttl=10)
    store.flush()
    assert store._expiry_heap == []


def test_expire_at_unix_sets_remaining_ttl():
    store = Store()
    store.set("a", 1)
    assert store.expire_at_unix("a", time.time() + 100) is True
    remaining = store.ttl("a")
    assert remaining is not None
    assert 98 <= remaining <= 100


def test_expire_at_unix_in_the_past_removes_key():
    store = Store()
    store.set("a", 1)
    assert store.expire_at_unix("a", time.time() - 1) is True
    assert store.get("a") is None


def test_keys_with_glob_pattern(store):
    for key in ("user:1", "user:2", "order:1"):
        dispatch(store, ["SET", key, "x"])
    assert set(dispatch(store, ["KEYS", "user:*"])) == {"user:1", "user:2"}
    assert set(dispatch(store, ["KEYS", "*:1"])) == {"user:1", "order:1"}
    assert set(dispatch(store, ["KEYS", "*"])) == {"user:1", "user:2", "order:1"}
    assert dispatch(store, ["KEYS", "nomatch*"]) == []


def test_keys_without_pattern_still_returns_everything(store):
    dispatch(store, ["SET", "a", "1"])
    assert dispatch(store, ["KEYS"]) == ["a"]


def test_keys_with_too_many_args_is_an_error(store):
    assert isinstance(dispatch(store, ["KEYS", "a", "b"]), Error)


def test_pexpireat_command(store):
    dispatch(store, ["SET", "a", "1"])
    deadline_ms = int((time.time() + 100) * 1000)
    assert dispatch(store, ["PEXPIREAT", "a", str(deadline_ms)]) is True
    assert 98 <= dispatch(store, ["TTL", "a"]) <= 100


def test_pexpireat_on_missing_key_and_bad_argument(store):
    assert dispatch(store, ["PEXPIREAT", "nope", "123"]) is False
    assert isinstance(dispatch(store, ["PEXPIREAT", "a", "soon"]), Error)
    assert isinstance(dispatch(store, ["PEXPIREAT", "a"]), Error)
