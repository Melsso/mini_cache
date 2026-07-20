from mini_cache.replication import build_snapshot_commands
from mini_cache.store import Store


def test_snapshot_of_empty_store():
    store = Store()
    assert build_snapshot_commands(store) == []


def test_snapshot_includes_set_for_each_key():
    store = Store()
    store.set("a", "1")
    store.set("b", "2")
    commands = build_snapshot_commands(store)
    sets = [c for c in commands if c[0] == "SET"]
    assert {(c[1], c[2]) for c in sets} == {("a", "1"), ("b", "2")}


def test_snapshot_includes_expire_for_keys_with_ttl():
    store = Store()
    store.set("session", "abc", ttl=100)
    commands = build_snapshot_commands(store)
    assert commands[0] == ["SET", "session", "abc"]
    assert commands[1][0] == "EXPIRE"
    assert commands[1][1] == "session"
    assert 90 <= int(commands[1][2]) <= 101


def test_snapshot_excludes_expire_for_keys_without_ttl():
    store = Store()
    store.set("permanent", "value")
    commands = build_snapshot_commands(store)
    assert commands == [["SET", "permanent", "value"]]
