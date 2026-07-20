import time

from mini_cache.store import Store


def test_set_then_get():
    store = Store()
    store.set("foo", "bar")
    assert store.get("foo") == "bar"


def test_get_missing_key_returns_none():
    store = Store()
    assert store.get("nope") is None


def test_set_overwrites_existing_value():
    store = Store()
    store.set("foo", "bar")
    store.set("foo", "baz")
    assert store.get("foo") == "baz"


def test_delete_existing_key_returns_true():
    store = Store()
    store.set("foo", "bar")
    assert store.delete("foo") is True
    assert store.get("foo") is None


def test_delete_missing_key_returns_false():
    store = Store()
    assert store.delete("nope") is False


def test_exists():
    store = Store()
    store.set("foo", "bar")
    assert store.exists("foo") is True
    assert store.exists("nope") is False


def test_len_and_keys():
    store = Store()
    store.set("a", 1)
    store.set("b", 2)
    assert len(store) == 2
    assert set(store.keys()) == {"a", "b"}


def test_flush():
    store = Store()
    store.set("a", 1)
    store.set("b", 2)
    store.flush()
    assert len(store) == 0


def test_set_with_ttl_then_immediate_get_succeeds():
    store = Store()
    store.set("foo", "bar", ttl=10)
    assert store.get("foo") == "bar"


def test_key_expires_after_ttl(monkeypatch):
    store = Store()
    fake_time = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: fake_time[0])

    store.set("foo", "bar", ttl=5)
    fake_time[0] += 5.1

    assert store.get("foo") is None


def test_key_without_ttl_never_expires(monkeypatch):
    store = Store()
    fake_time = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: fake_time[0])

    store.set("foo", "bar")
    fake_time[0] += 10_000

    assert store.get("foo") == "bar"


def test_expire_sets_ttl_on_existing_key(monkeypatch):
    store = Store()
    fake_time = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: fake_time[0])

    store.set("foo", "bar")
    assert store.expire("foo", 5) is True

    fake_time[0] += 5.1
    assert store.get("foo") is None


def test_expire_on_missing_key_returns_false():
    store = Store()
    assert store.expire("nope", 5) is False


def test_ttl_returns_none_for_missing_key():
    store = Store()
    assert store.ttl("nope") is None


def test_ttl_returns_negative_one_for_key_without_expiry():
    store = Store()
    store.set("foo", "bar")
    assert store.ttl("foo") == -1.0


def test_ttl_returns_remaining_seconds(monkeypatch):
    store = Store()
    fake_time = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: fake_time[0])

    store.set("foo", "bar", ttl=10)
    fake_time[0] += 4
    remaining = store.ttl("foo")
    assert remaining is not None
    assert 5.9 <= remaining <= 6.0


def test_persist_removes_ttl(monkeypatch):
    store = Store()
    fake_time = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: fake_time[0])

    store.set("foo", "bar", ttl=5)
    assert store.persist("foo") is True
    assert store.ttl("foo") == -1.0

    fake_time[0] += 10
    assert store.get("foo") == "bar"


def test_persist_on_key_without_ttl_returns_false():
    store = Store()
    store.set("foo", "bar")
    assert store.persist("foo") is False


def test_persist_on_missing_key_returns_false():
    store = Store()
    assert store.persist("nope") is False


def test_expired_key_removed_from_delete(monkeypatch):
    store = Store()
    fake_time = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: fake_time[0])

    store.set("foo", "bar", ttl=5)
    fake_time[0] += 5.1

    assert store.delete("foo") is False


def test_sweep_expired_removes_dead_keys_without_read(monkeypatch):
    store = Store()
    fake_time = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: fake_time[0])

    store.set("a", 1, ttl=5)
    store.set("b", 2)
    fake_time[0] += 5.1

    removed = store.sweep_expired()
    assert removed == 1
    assert "a" not in store._data
    assert "b" in store._data


def test_set_resets_ttl_from_previous_call(monkeypatch):
    store = Store()
    fake_time = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: fake_time[0])

    store.set("foo", "bar", ttl=5)
    store.set("foo", "baz")

    fake_time[0] += 10
    assert store.get("foo") == "baz"
