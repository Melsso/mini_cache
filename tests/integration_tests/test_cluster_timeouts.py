import asyncio

import pytest

from mini_cache.cluster import ClusterClient, ClusterTimeoutError
from mini_cache.protocol import encode, read_command


async def start_fake(handler):
    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


async def test_hung_shard_times_out_and_releases_the_lock():
    async def hang(reader, writer):
        await reader.read()

    server, port = await start_fake(hang)
    client = ClusterClient([("127.0.0.1", port)], read_timeout=0.2)

    with pytest.raises(ClusterTimeoutError):
        await client.get("k")
    assert client._connections == {}
    with pytest.raises(ClusterTimeoutError):
        await asyncio.wait_for(client.get("k"), 2)

    await client.close()
    server.close()


async def test_cancelled_request_does_not_poison_the_next_one():
    async def echo_key_slowly(reader, writer):
        first = True
        try:
            while (command := await read_command(reader)) is not None:
                if first:
                    await asyncio.sleep(0.3)
                    first = False
                writer.write(encode(command[1]))
                await writer.drain()
        except OSError:
            pass

    server, port = await start_fake(echo_key_slowly)
    client = ClusterClient([("127.0.0.1", port)])

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(client.get("a"), 0.1)
    assert client._connections == {}
    assert await client.get("b") == "b"

    await client.close()
    server.close()
