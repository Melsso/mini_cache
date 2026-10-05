import pytest

from mini_cache.cluster import ClusterClient, ClusterError
from mini_cache.server import Server


async def start_shards(count):
    servers = []
    for _ in range(count):
        server = Server(host="127.0.0.1", port=0)
        await server.start()
        servers.append(server)
    return servers, [("127.0.0.1", s.bound_port) for s in servers]


async def stop_all(servers):
    for server in servers:
        await server.stop()


async def test_typed_round_trip_and_nx():
    servers, shards = await start_shards(1)
    async with ClusterClient(shards) as client:
        assert await client.set("foo", "bar") is True
        assert await client.get("foo") == b"bar"
        assert await client.get("nope") is None
        assert await client.set("lock", "a", nx=True) is True
        assert await client.set("lock", "b", nx=True) is False
        assert await client.get("lock") == b"a"
        assert await client.incr("n") == 1
        assert await client.incr("n", 4) == 5
        assert await client.delete("foo") == 1
        assert await client.exists("foo") == 0
        assert await client.expire("n", 100) is True
        assert 0 <= await client.ttl("n") <= 100
    await stop_all(servers)


async def test_keys_route_to_correct_shard():
    servers, shards = await start_shards(2)
    async with ClusterClient(shards) as client:
        by_port = {s.bound_port: s for s in servers}
        for i in range(50):
            await client.set(f"key{i}", f"value-{i}")
        for i in range(50):
            _, port = client.node_for(f"key{i}").rsplit(":", 1)
            assert (
                by_port[int(port)].store.get(f"key{i}".encode())
                == f"value-{i}".encode()
            )
    await stop_all(servers)


async def test_mget_mset_keep_order_across_shards():
    servers, shards = await start_shards(3)
    async with ClusterClient(shards) as client:
        await client.mset({f"k{i}": f"v{i}" for i in range(60)})
        keys = [f"k{i}" for i in range(60)] + ["missing"]
        values = await client.mget(keys)
        assert values == [f"v{i}".encode() for i in range(60)] + [None]
        sizes = await client.dbsize_per_shard()
        assert sum(sizes.values()) == 60 and all(n > 0 for n in sizes.values())
    await stop_all(servers)


async def test_pipeline_preserves_order_and_reports_errors():
    servers, shards = await start_shards(3)
    async with ClusterClient(shards) as client:
        pipe = client.pipeline()
        pipe.set("a", "1").incr("n").get("a").incr("n", 2)
        assert await pipe.execute() == ["OK", 1, b"1", 3]

        bad = client.pipeline().command("INCRBY", "a", "notint")
        with pytest.raises(ClusterError):
            await bad.execute()
        replies = (
            await client.pipeline()
            .command("INCRBY", "a", "x")
            .execute(raise_on_error=False)
        )
        assert str(replies[0]).startswith("ERR")
    await stop_all(servers)


async def test_keys_delete_pattern_and_flush_all():
    servers, shards = await start_shards(3)
    async with ClusterClient(shards) as client:
        for i in range(30):
            await client.set(f"user:{i}", "x")
        for i in range(10):
            await client.set(f"other:{i}", "x")
        assert len(await client.keys("user:*")) == 30
        assert await client.delete_pattern("user:*") == 30
        assert await client.dbsize() == 10
        assert sorted(await client.keys("other:*")) == sorted(
            f"other:{i}".encode() for i in range(10)
        )
        await client.flush_all()
        assert await client.dbsize() == 0
    await stop_all(servers)


async def test_pool_uses_several_connections_under_concurrency():
    servers, shards = await start_shards(1)
    async with ClusterClient(shards, pool_size=3) as client:
        await client.set("k", "v")
        results = await __import__("asyncio").gather(
            *(client.get("k") for _ in range(50))
        )
        assert results == [b"v"] * 50
        pool = next(iter(client._pools.values()))
        assert 1 < len(pool.idle) <= 3
    await stop_all(servers)
