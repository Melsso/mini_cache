import asyncio

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


async def wait_until(condition, timeout=2.0, interval=0.02):
    elapsed = 0.0
    while elapsed < timeout:
        if condition():
            return True
        await asyncio.sleep(interval)
        elapsed += interval
    return False


async def test_replica_receives_initial_snapshot():
    primary = Server(host="127.0.0.1", port=0)
    await primary.start()
    primary_port = primary._asyncio_server.sockets[0].getsockname()[1]

    reader, writer = await open_client(primary_port)
    await send(writer, "SET", "foo", "bar")
    await read_reply(reader)
    writer.close()
    await writer.wait_closed()

    replica = Server(host="127.0.0.1", port=0, replica_of=("127.0.0.1", primary_port))
    await replica.start()

    ok = await wait_until(lambda: replica.store.get("foo") == "bar")
    assert ok

    await replica.stop()
    await primary.stop()


async def test_replica_receives_live_writes():
    primary = Server(host="127.0.0.1", port=0)
    await primary.start()
    primary_port = primary._asyncio_server.sockets[0].getsockname()[1]

    replica = Server(host="127.0.0.1", port=0, replica_of=("127.0.0.1", primary_port))
    await replica.start()

    ok = await wait_until(lambda: replica.replica_client.connected.is_set())
    assert ok

    reader, writer = await open_client(primary_port)
    await send(writer, "SET", "live", "value")
    await read_reply(reader)
    writer.close()
    await writer.wait_closed()

    ok = await wait_until(lambda: replica.store.get("live") == "value")
    assert ok

    await replica.stop()
    await primary.stop()


async def test_replica_rejects_writes_from_normal_clients():
    primary = Server(host="127.0.0.1", port=0)
    await primary.start()
    primary_port = primary._asyncio_server.sockets[0].getsockname()[1]

    replica = Server(host="127.0.0.1", port=0, replica_of=("127.0.0.1", primary_port))
    await replica.start()
    replica_port = replica._asyncio_server.sockets[0].getsockname()[1]

    reader, writer = await open_client(replica_port)
    await send(writer, "SET", "foo", "bar")
    reply = await read_reply(reader)
    assert isinstance(reply, Exception)
    assert "READONLY" in str(reply)
    writer.close()
    await writer.wait_closed()

    await replica.stop()
    await primary.stop()


async def test_replica_still_serves_reads():
    primary = Server(host="127.0.0.1", port=0)
    await primary.start()
    primary_port = primary._asyncio_server.sockets[0].getsockname()[1]

    reader, writer = await open_client(primary_port)
    await send(writer, "SET", "foo", "bar")
    await read_reply(reader)
    writer.close()
    await writer.wait_closed()

    replica = Server(host="127.0.0.1", port=0, replica_of=("127.0.0.1", primary_port))
    await replica.start()
    replica_port = replica._asyncio_server.sockets[0].getsockname()[1]

    ok = await wait_until(lambda: replica.store.get("foo") == "bar")
    assert ok

    reader, writer = await open_client(replica_port)
    await send(writer, "GET", "foo")
    assert await read_reply(reader) == "bar"
    writer.close()
    await writer.wait_closed()

    await replica.stop()
    await primary.stop()


async def test_multiple_replicas_receive_same_writes():
    primary = Server(host="127.0.0.1", port=0)
    await primary.start()
    primary_port = primary._asyncio_server.sockets[0].getsockname()[1]

    replica_a = Server(host="127.0.0.1", port=0, replica_of=("127.0.0.1", primary_port))
    replica_b = Server(host="127.0.0.1", port=0, replica_of=("127.0.0.1", primary_port))
    await replica_a.start()
    await replica_b.start()

    reader, writer = await open_client(primary_port)
    await send(writer, "SET", "shared", "value")
    await read_reply(reader)
    writer.close()
    await writer.wait_closed()

    ok_a = await wait_until(lambda: replica_a.store.get("shared") == "value")
    ok_b = await wait_until(lambda: replica_b.store.get("shared") == "value")
    assert ok_a
    assert ok_b

    await replica_a.stop()
    await replica_b.stop()
    await primary.stop()


async def test_delete_propagates_to_replica():
    primary = Server(host="127.0.0.1", port=0)
    await primary.start()
    primary_port = primary._asyncio_server.sockets[0].getsockname()[1]

    reader, writer = await open_client(primary_port)
    await send(writer, "SET", "foo", "bar")
    await read_reply(reader)
    writer.close()
    await writer.wait_closed()

    replica = Server(host="127.0.0.1", port=0, replica_of=("127.0.0.1", primary_port))
    await replica.start()
    ok = await wait_until(lambda: replica.store.get("foo") == "bar")
    assert ok

    reader, writer = await open_client(primary_port)
    await send(writer, "DEL", "foo")
    await read_reply(reader)
    writer.close()
    await writer.wait_closed()

    ok = await wait_until(lambda: replica.store.get("foo") is None)
    assert ok

    await replica.stop()
    await primary.stop()
