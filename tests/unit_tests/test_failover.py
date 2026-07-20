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


async def wait_until(condition, timeout=6.0, interval=0.05):
    elapsed = 0.0
    while elapsed < timeout:
        if condition():
            return True
        await asyncio.sleep(interval)
        elapsed += interval
    return False


async def test_heartbeats_do_not_corrupt_replica_state():
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

    await asyncio.sleep(2.2)

    assert replica.store.get("foo") == "bar"
    assert len(replica.store) == 1
    assert replica.replica_client.connected.is_set()

    await replica.stop()
    await primary.stop()


async def test_replica_reconnects_after_primary_restarts_on_same_port():
    primary1 = Server(host="127.0.0.1", port=0)
    await primary1.start()
    port = primary1._asyncio_server.sockets[0].getsockname()[1]

    reader, writer = await open_client(port)
    await send(writer, "SET", "foo", "bar")
    await read_reply(reader)
    writer.close()
    await writer.wait_closed()

    replica = Server(host="127.0.0.1", port=0, replica_of=("127.0.0.1", port))
    await replica.start()
    ok = await wait_until(lambda: replica.store.get("foo") == "bar")
    assert ok

    await primary1.stop()

    ok = await wait_until(lambda: not replica.replica_client.connected.is_set())
    assert ok

    primary2 = Server(host="127.0.0.1", port=port)
    await primary2.start()

    reader, writer = await open_client(port)
    await send(writer, "SET", "after_restart", "value")
    await read_reply(reader)
    writer.close()
    await writer.wait_closed()

    ok = await wait_until(lambda: replica.store.get("after_restart") == "value")
    assert ok
    assert replica.replica_client.connected.is_set()

    await replica.stop()
    await primary2.stop()


async def test_primary_drops_replica_after_link_timeout(monkeypatch):
    from mini_cache import server as server_module

    monkeypatch.setattr(server_module, "LINK_TIMEOUT", 0.3)

    primary = Server(host="127.0.0.1", port=0)
    await primary.start()
    primary_port = primary._asyncio_server.sockets[0].getsockname()[1]

    reader, writer = await open_client(primary_port)
    await send(writer, "SYNC")
    await asyncio.sleep(0.1)
    assert len(primary._replicas) == 1

    await asyncio.sleep(0.5)

    assert len(primary._replicas) == 0

    writer.close()
    await writer.wait_closed()
    await primary.stop()
