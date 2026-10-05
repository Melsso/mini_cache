import asyncio

import pytest

from mini_cache.cluster import ClusterClient, ClusterError
from mini_cache.server import Server


async def open_client(port):
    return await asyncio.open_connection("127.0.0.1", port)


async def send(writer, *parts):
    frame = f"*{len(parts)}\r\n"
    for part in parts:
        frame += f"${len(part)}\r\n{part}\r\n"
    writer.write(frame.encode())
    await writer.drain()


async def read_reply(reader):
    line = await reader.readline()
    prefix = chr(line[0])
    body = line[1:-2].decode()

    if prefix == "+":
        return body
    if prefix == "-":
        return Exception(body)
    if prefix == ":":
        return int(body)
    if prefix == "$":
        length = int(body)
        if length == -1:
            return None
        data = await reader.readexactly(length)
        await reader.readexactly(2)
        return data.decode()
    raise ValueError(f"unknown reply prefix: {prefix!r}")


async def wait_until(condition, timeout=6.0, interval=0.05):
    elapsed = 0.0
    while elapsed < timeout:
        if condition():
            return True
        await asyncio.sleep(interval)
        elapsed += interval
    return False


def port_of(server):
    return server._asyncio_server.sockets[0].getsockname()[1]


async def start_server(**kwargs):
    server = Server(host="127.0.0.1", port=kwargs.pop("port", 0), **kwargs)
    await server.start()
    return server


async def test_malformed_request_gets_an_error_and_does_not_hurt_the_server():
    server = await start_server()
    reader, writer = await open_client(port_of(server))

    writer.write(b"*1\r\n$abc\r\n")
    await writer.drain()
    reply = await read_reply(reader)
    assert isinstance(reply, Exception) and "Protocol error" in str(reply)
    assert await reader.read() == b""

    reader2, writer2 = await open_client(port_of(server))
    await send(writer2, "PING")
    assert await read_reply(reader2) == "PONG"
    writer2.close()
    await server.stop()


async def test_client_dropping_mid_command_does_not_hurt_the_server():
    server = await start_server()
    reader, writer = await open_client(port_of(server))
    writer.write(b"*3\r\n$3\r\nSET\r\n$3\r\nfo")
    await writer.drain()
    writer.close()
    await writer.wait_closed()

    reader2, writer2 = await open_client(port_of(server))
    await send(writer2, "PING")
    assert await read_reply(reader2) == "PONG"
    writer2.close()
    await server.stop()


async def test_quit_replies_ok_and_closes_the_connection():
    server = await start_server()
    reader, writer = await open_client(port_of(server))
    await send(writer, "QUIT")
    assert await read_reply(reader) == "OK"
    assert await reader.read() == b""
    writer.close()
    await server.stop()


async def test_replica_refuses_to_act_as_a_primary_for_others():
    primary = await start_server()
    replica = await start_server(replica_of=("127.0.0.1", port_of(primary)))

    reader, writer = await open_client(port_of(replica))
    await send(writer, "SYNC")
    reply = await read_reply(reader)
    assert isinstance(reply, Exception) and "chained" in str(reply)
    writer.close()

    await replica.stop()
    await primary.stop()


async def test_ttls_survive_a_restart_as_absolute_deadlines(tmp_path):
    aof_path = str(tmp_path / "ttl.aof")

    server1 = await start_server(aof_path=aof_path)
    reader, writer = await open_client(port_of(server1))
    await send(writer, "SET", "long", "1", "EX", "100")
    assert await read_reply(reader) == "OK"
    await send(writer, "SET", "short", "2", "EX", "1")
    assert await read_reply(reader) == "OK"
    await send(writer, "SET", "later", "3")
    await read_reply(reader)
    await send(writer, "EXPIRE", "later", "1")
    await read_reply(reader)
    writer.close()
    await server1.stop()

    await asyncio.sleep(1.3)

    server2 = await start_server(aof_path=aof_path)
    assert server2.store.get("short") is None
    assert server2.store.get("later") is None
    assert server2.store.get("long") == "1"
    remaining = server2.store.ttl("long")
    assert remaining is not None and 90 < remaining < 99.5
    await server2.stop()


async def test_server_starts_after_a_crash_left_a_torn_aof(tmp_path):
    aof_path = tmp_path / "crash.aof"

    server1 = await start_server(aof_path=str(aof_path))
    reader, writer = await open_client(port_of(server1))
    await send(writer, "SET", "foo", "bar")
    await read_reply(reader)
    writer.close()
    await server1.stop()

    with open(aof_path, "ab") as f:
        f.write(b"*3\r\n$3\r\nSET\r\n$3\r\nba")

    server2 = await start_server(aof_path=str(aof_path))
    assert server2.store.get("foo") == "bar"
    assert (tmp_path / "crash.aof.corrupt").exists()
    reader, writer = await open_client(port_of(server2))
    await send(writer, "SET", "after", "crash")
    await read_reply(reader)
    writer.close()
    await server2.stop()

    server3 = await start_server(aof_path=str(aof_path))
    assert server3.store.get("foo") == "bar"
    assert server3.store.get("after") == "crash"
    await server3.stop()


async def test_everysec_policy_persists_data(tmp_path):
    aof_path = str(tmp_path / "everysec.aof")
    server1 = await start_server(aof_path=aof_path, aof_fsync="everysec")
    reader, writer = await open_client(port_of(server1))
    await send(writer, "SET", "foo", "bar")
    await read_reply(reader)
    writer.close()
    await server1.stop()

    server2 = await start_server(aof_path=aof_path)
    assert server2.store.get("foo") == "bar"
    await server2.stop()


def test_invalid_fsync_policy_is_rejected_by_the_server():
    with pytest.raises(ValueError):
        Server(aof_fsync="bogus")


async def test_replica_ignores_aof_path(tmp_path):
    primary = await start_server()
    aof_path = tmp_path / "replica.aof"
    replica = await start_server(
        aof_path=str(aof_path), replica_of=("127.0.0.1", port_of(primary))
    )
    assert replica.aof is None
    assert not aof_path.exists()
    await replica.stop()
    await primary.stop()


async def test_reconnecting_replica_drops_keys_deleted_while_it_was_away():
    primary1 = await start_server()
    port = port_of(primary1)
    reader, writer = await open_client(port)
    await send(writer, "SET", "foo", "bar")
    await read_reply(reader)
    writer.close()

    replica = await start_server(replica_of=("127.0.0.1", port))
    assert await wait_until(lambda: replica.store.get("foo") == "bar")

    await primary1.stop()
    assert await wait_until(lambda: not replica.replica_client.connected.is_set())

    primary2 = await start_server(port=port)
    reader, writer = await open_client(port)
    await send(writer, "SET", "fresh", "value")
    await read_reply(reader)
    writer.close()

    assert await wait_until(lambda: replica.store.get("fresh") == "value")
    assert replica.store.get("foo") is None

    await replica.stop()
    await primary2.stop()


async def test_cluster_client_handles_concurrent_calls():
    server_a = await start_server()
    server_b = await start_server()
    client = ClusterClient(
        [("127.0.0.1", port_of(server_a)), ("127.0.0.1", port_of(server_b))]
    )

    await asyncio.gather(*(client.set(f"k{i}", str(i)) for i in range(200)))
    values = await asyncio.gather(*(client.get(f"k{i}") for i in range(200)))
    assert values == [str(i) for i in range(200)]

    await client.close()
    await server_a.stop()
    await server_b.stop()


async def test_cluster_client_reconnects_after_a_shard_restarts():
    server = await start_server()
    port = port_of(server)
    client = ClusterClient([("127.0.0.1", port)])
    assert await client.set("a", "1") == "OK"

    await server.stop()
    server = await start_server(port=port)

    assert await client.set("b", "2") == "OK"
    assert await client.get("b") == "2"

    await client.close()
    await server.stop()


async def test_cluster_client_raises_on_error_replies_and_works_as_context_manager():
    server = await start_server()
    async with ClusterClient([("127.0.0.1", port_of(server))]) as client:
        with pytest.raises(ClusterError):
            await client.expire("foo", "not-a-number")
        assert await client.set("foo", "bar") == "OK"
    assert client._connections == {}
    await server.stop()
