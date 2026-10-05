import asyncio

import pytest

from mini_cache.cluster import ClusterClient, ClusterError
from mini_cache.server import Server


async def start(**kwargs):
    server = Server(host="127.0.0.1", port=kwargs.pop("port", 0), **kwargs)
    await server.start()
    return server, server._asyncio_server.sockets[0].getsockname()[1]


async def call(reader, writer, *parts):
    frame = f"*{len(parts)}\r\n" + "".join(f"${len(p)}\r\n{p}\r\n" for p in parts)
    writer.write(frame.encode())
    await writer.drain()
    return (await reader.readline())[:-2].decode()


async def test_commands_are_refused_until_auth():
    server, port = await start(requirepass="s3cret")
    reader, writer = await asyncio.open_connection("127.0.0.1", port)

    assert (await call(reader, writer, "PING")).startswith("-NOAUTH")
    assert (await call(reader, writer, "SET", "k", "v")).startswith("-NOAUTH")
    assert (await call(reader, writer, "AUTH", "wrong")).startswith("-WRONGPASS")
    assert (await call(reader, writer, "PING")).startswith("-NOAUTH")
    assert await call(reader, writer, "AUTH", "s3cret") == "+OK"
    assert await call(reader, writer, "PING") == "+PONG"

    writer.close()
    await server.stop()


async def test_auth_without_a_configured_password_is_an_error():
    server, port = await start()
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    assert (await call(reader, writer, "AUTH", "x")).startswith("-ERR")
    assert await call(reader, writer, "PING") == "+PONG"
    writer.close()
    await server.stop()


async def test_cluster_client_authenticates():
    server, port = await start(requirepass="pw")

    async with ClusterClient([("127.0.0.1", port)], password="pw") as client:
        assert await client.set("k", "v") == "OK"
    async with ClusterClient([("127.0.0.1", port)], password="bad") as client:
        with pytest.raises(ClusterError):
            await client.get("k")
    await server.stop()


async def test_replica_syncs_from_a_password_protected_primary():
    primary, port = await start(requirepass="pw")
    async with ClusterClient([("127.0.0.1", port)], password="pw") as client:
        await client.set("foo", "bar")

    replica, _ = await start(requirepass="pw", replica_of=("127.0.0.1", port))
    for _ in range(100):
        if replica.store.get("foo") == "bar":
            break
        await asyncio.sleep(0.02)
    assert replica.store.get("foo") == "bar"

    await replica.stop()
    await primary.stop()
