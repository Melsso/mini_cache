import asyncio
import time

from mini_cache import aof as aof_module
from mini_cache.aof import AOFLog, to_aof_commands
from mini_cache.commands import dispatch
from mini_cache.store import Store


def write(log, store, *command):
    dispatch(store, list(command))
    for aof_command in to_aof_commands(list(command)):
        log.append(aof_command)


async def replay(path):
    log = AOFLog(path)
    store = Store()
    await log.replay(store)
    log.close()
    return store


async def test_rewrite_compacts_and_preserves_state_and_ttls(tmp_path):
    path = tmp_path / "r.aof"
    log, store = AOFLog(path), Store()
    for i in range(500):
        write(log, store, "SET", "k", str(i))
    write(log, store, "SET", "t", "v", "EX", "100")
    before = log.size

    assert await log.rewrite(store) == 3
    assert log.size < before / 10
    write(log, store, "SET", "after", "1")
    log.close()

    restored = await replay(path)
    assert restored.get(b"k") == b"499"
    assert restored.get(b"after") == b"1"
    assert 98 <= restored.ttl(b"t") <= 100


async def test_writes_during_a_rewrite_are_not_lost(tmp_path, monkeypatch):
    path = tmp_path / "w.aof"
    real = aof_module._write_durable

    def slow(target, payload):
        time.sleep(0.2)
        real(target, payload)

    monkeypatch.setattr(aof_module, "_write_durable", slow)
    log, store = AOFLog(path), Store()
    write(log, store, "SET", "a", "1")

    task = asyncio.create_task(log.rewrite(store))
    await asyncio.sleep(0.05)
    log.append(["SET", "b", "2"])
    await task
    log.close()

    restored = await replay(path)
    assert restored.get(b"a") == b"1" and restored.get(b"b") == b"2"
