import asyncio

from mini_cache.cluster import ClusterClient
from mini_cache.protocol import read_reply
from mini_cache.server import Server


async def start(**kwargs):
    server = Server(host="127.0.0.1", port=0, **kwargs)
    await server.start()
    return server


async def raw(server, payload):
    reader, writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
    writer.write(payload)
    await writer.drain()
    return reader, writer


async def test_binary_values_survive_the_wire_and_the_aof(tmp_path):
    path = str(tmp_path / "bin.aof")
    blob = bytes(range(256))
    server = await start(aof_path=path)
    async with ClusterClient([("127.0.0.1", server.bound_port)]) as client:
        assert await client.set(b"\xff\x00key", blob) is True
        assert await client.get(b"\xff\x00key") == blob
    await server.stop()

    server = await start(aof_path=path)
    assert server.store.get(b"\xff\x00key") == blob
    await server.stop()


async def test_maxclients_rejects_extra_connections():
    server = await start(maxclients=1)
    r1, w1 = await raw(server, b"PING\r\n")
    assert await r1.readline() == b"+PONG\r\n"
    r2, w2 = await raw(server, b"")
    assert (await r2.readline()).startswith(b"-ERR max number of clients")
    assert await r2.read() == b""
    w1.close()
    w2.close()
    await server.stop()


async def test_idle_clients_are_disconnected():
    server = await start(client_timeout=0.2)
    reader, writer = await raw(server, b"")
    assert await asyncio.wait_for(reader.read(), 2) == b""
    writer.close()
    await server.stop()


async def test_noeviction_rejects_writes_when_full():
    server = await start(maxmemory=300, maxmemory_policy="noeviction")
    async with ClusterClient([("127.0.0.1", server.bound_port)]) as client:
        await client.set("a", b"x" * 200)
        await client.set("b", b"x" * 200)
        from mini_cache.cluster import ClusterError
        import pytest

        with pytest.raises(ClusterError, match="OOM"):
            await client.set("c", "1")
        assert await client.delete("a") == 1
        assert await client.set("c", "1") is True
    await server.stop()


async def test_evictions_are_replicated_and_logged(tmp_path):
    path = str(tmp_path / "evict.aof")
    primary = await start(aof_path=path, maxmemory=1000)
    replica = await start(replica_of=("127.0.0.1", primary.bound_port))
    async with ClusterClient([("127.0.0.1", primary.bound_port)]) as client:
        for i in range(30):
            await client.set(f"big{i}", b"x" * 100)
        assert primary.store.evicted_keys > 0
        for _ in range(100):
            if set(replica.store.keys()) == set(primary.store.keys()):
                break
            await asyncio.sleep(0.05)
        assert set(replica.store.keys()) == set(primary.store.keys())
    await replica.stop()
    await primary.stop()

    restarted = await start(aof_path=path, maxmemory=1000)
    assert len(restarted.store) > 0
    await restarted.stop()


async def test_stuck_replica_is_dropped_instead_of_slowing_the_primary(monkeypatch):
    from mini_cache import server as server_module

    monkeypatch.setattr(server_module, "REPLICA_BACKLOG_LIMIT", 1000)
    primary = await start()
    reader, writer = await raw(primary, b"SYNC\r\n")
    for _ in range(100):
        if len(primary._replicas) == 1:
            break
        await asyncio.sleep(0.01)
    assert len(primary._replicas) == 1

    async with ClusterClient([("127.0.0.1", primary.bound_port)]) as client:
        assert await asyncio.wait_for(client.set("k", b"x" * 10_000), 1) is True
    assert len(primary._replicas) == 0
    writer.close()
    await primary.stop()


async def test_info_reports_stats_memory_and_persistence(tmp_path):
    server = await start(
        aof_path=str(tmp_path / "i.aof"), maxmemory=1000, maxmemory_policy="allkeys-lru"
    )
    async with ClusterClient([("127.0.0.1", server.bound_port)]) as client:
        await client.set("a", "1")
        await client.get("a")
        await client.get("nope")
        for i in range(20):
            await client.set(f"big{i}", b"x" * 100)
    reader, writer = await raw(server, b"INFO\r\n")
    info = (await read_reply(reader)).decode()
    stats = dict(line.split(":", 1) for line in info.splitlines() if ":" in line)
    assert stats["keyspace_hits"] == "1" and stats["keyspace_misses"] == "1"
    assert int(stats["evicted_keys"]) > 0
    assert int(stats["used_memory"]) <= 1000 and stats["maxmemory"] == "1000"
    assert stats["aof_enabled"] == "1" and "aof_current_size" in stats
    writer.close()
    await server.stop()


async def test_bgrewriteaof_shrinks_the_file_and_survives_restart(tmp_path):
    path = tmp_path / "bg.aof"
    server = await start(aof_path=str(path))
    async with ClusterClient([("127.0.0.1", server.bound_port)]) as client:
        for i in range(200):
            await client.set("k", str(i))
    before = path.stat().st_size

    reader, writer = await raw(server, b"BGREWRITEAOF\r\n")
    assert (await reader.readline()).startswith(b"+Background")
    await server._rewrite_task
    writer.close()
    assert path.stat().st_size < before
    await server.stop()

    server = await start(aof_path=str(path))
    assert server.store.get(b"k") == b"199"
    await server.stop()
