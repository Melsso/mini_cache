from mini_cache.aof import AOFLog, is_write_command
from mini_cache.store import Store


def test_is_write_command():
    assert is_write_command("SET") is True
    assert is_write_command("set") is True
    assert is_write_command("DEL") is True
    assert is_write_command("EXPIRE") is True
    assert is_write_command("PERSIST") is True
    assert is_write_command("FLUSHDB") is True
    assert is_write_command("GET") is False
    assert is_write_command("PING") is False
    assert is_write_command("TTL") is False


async def test_append_then_replay_restores_state(tmp_path):
    path = tmp_path / "test.aof"
    log = AOFLog(path)

    log.append(["SET", "foo", "bar"])
    log.append(["SET", "baz", "qux"])
    log.append(["DEL", "foo"])
    log.close()

    replay_log = AOFLog(path)
    store = Store()
    count = await replay_log.replay(store)
    replay_log.close()

    assert count == 3
    assert store.get("foo") is None
    assert store.get("baz") == "qux"


async def test_replay_empty_file_returns_zero(tmp_path):
    path = tmp_path / "empty.aof"
    path.touch()
    log = AOFLog(path)
    store = Store()
    count = await log.replay(store)
    log.close()
    assert count == 0


async def test_replay_missing_file_returns_zero(tmp_path):
    path = tmp_path / "missing.aof"
    log = AOFLog(path)
    store = Store()
    count = await log.replay(store)
    log.close()
    assert count == 0


async def test_replay_preserves_ttl_semantics(tmp_path):
    path = tmp_path / "ttl.aof"
    log = AOFLog(path)
    log.append(["SET", "session", "abc", "EX", "1000"])
    log.close()

    replay_log = AOFLog(path)
    store = Store()
    await replay_log.replay(store)
    replay_log.close()

    ttl = store.ttl("session")
    assert ttl is not None
    assert 999 <= ttl <= 1000
