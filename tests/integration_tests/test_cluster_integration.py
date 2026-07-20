from mini_cache.cluster import ClusterClient
from mini_cache.server import Server


async def start_shard():
    server = Server(host="127.0.0.1", port=0)
    await server.start()
    port = server._asyncio_server.sockets[0].getsockname()[1]
    return server, port


async def test_set_and_get_round_trip():
    server, port = await start_shard()
    client = ClusterClient([("127.0.0.1", port)])

    assert await client.set("foo", "bar") == "OK"
    assert await client.get("foo") == "bar"

    await client.close()
    await server.stop()


async def test_keys_route_to_correct_shard():
    server_a, port_a = await start_shard()
    server_b, port_b = await start_shard()
    client = ClusterClient([("127.0.0.1", port_a), ("127.0.0.1", port_b)])

    keys = [f"key{i}" for i in range(50)]
    for key in keys:
        await client.set(key, f"value-{key}")

    for key in keys:
        expected_node = client.node_for(key)
        expected_host, expected_port_str = expected_node.rsplit(":", 1)
        expected_port = int(expected_port_str)

        if expected_port == port_a:
            direct_server = server_a
        else:
            direct_server = server_b

        assert direct_server.store.get(key) == f"value-{key}"

    await client.close()
    await server_a.stop()
    await server_b.stop()


async def test_delete_through_cluster():
    server, port = await start_shard()
    client = ClusterClient([("127.0.0.1", port)])

    await client.set("foo", "bar")
    assert await client.delete("foo") == 1
    assert await client.get("foo") is None

    await client.close()
    await server.stop()


async def test_ttl_through_cluster():
    server, port = await start_shard()
    client = ClusterClient([("127.0.0.1", port)])

    await client.set("foo", "bar", ttl=100)
    ttl = await client.ttl("foo")
    assert 0 <= ttl <= 100

    await client.close()
    await server.stop()


async def test_dbsize_per_shard_reports_each_node():
    server_a, port_a = await start_shard()
    server_b, port_b = await start_shard()
    client = ClusterClient([("127.0.0.1", port_a), ("127.0.0.1", port_b)])

    for i in range(20):
        await client.set(f"key{i}", "value")

    sizes = await client.dbsize_per_shard()
    assert sum(sizes.values()) == 20
    assert len(sizes) == 2

    await client.close()
    await server_a.stop()
    await server_b.stop()


async def test_multiple_keys_spread_across_three_shards():
    server_a, port_a = await start_shard()
    server_b, port_b = await start_shard()
    server_c, port_c = await start_shard()
    client = ClusterClient(
        [("127.0.0.1", port_a), ("127.0.0.1", port_b), ("127.0.0.1", port_c)]
    )

    for i in range(300):
        await client.set(f"key{i}", "value")

    sizes = await client.dbsize_per_shard()
    assert sum(sizes.values()) == 300
    for count in sizes.values():
        assert count > 0

    await client.close()
    await server_a.stop()
    await server_b.stop()
    await server_c.stop()
