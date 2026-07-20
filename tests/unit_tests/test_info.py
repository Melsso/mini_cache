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


async def test_info_reports_master_role_by_default():
    server = Server(host="127.0.0.1", port=0)
    await server.start()
    port = server._asyncio_server.sockets[0].getsockname()[1]

    reader, writer = await open_client(port)
    await send(writer, "INFO")
    info = await read_reply(reader)
    assert "role:master" in info
    assert "connected_clients:1" in info

    writer.close()
    await writer.wait_closed()
    await server.stop()


async def test_info_reports_replica_role_and_link_status():
    primary = Server(host="127.0.0.1", port=0)
    await primary.start()
    primary_port = primary._asyncio_server.sockets[0].getsockname()[1]

    replica = Server(host="127.0.0.1", port=0, replica_of=("127.0.0.1", primary_port))
    await replica.start()
    replica_port = replica._asyncio_server.sockets[0].getsockname()[1]

    elapsed = 0.0
    while elapsed < 2.0 and not replica.replica_client.connected.is_set():
        await asyncio.sleep(0.05)
        elapsed += 0.05

    reader, writer = await open_client(replica_port)
    await send(writer, "INFO")
    info = await read_reply(reader)
    assert "role:replica" in info
    assert f"master_port:{primary_port}" in info
    assert "master_link_status:up" in info

    writer.close()
    await writer.wait_closed()
    await replica.stop()
    await primary.stop()


async def test_info_reports_connected_replica_count():
    primary = Server(host="127.0.0.1", port=0)
    await primary.start()
    primary_port = primary._asyncio_server.sockets[0].getsockname()[1]

    replica = Server(host="127.0.0.1", port=0, replica_of=("127.0.0.1", primary_port))
    await replica.start()

    elapsed = 0.0
    while elapsed < 2.0 and len(primary._replicas) == 0:
        await asyncio.sleep(0.05)
        elapsed += 0.05

    reader, writer = await open_client(primary_port)
    await send(writer, "INFO")
    info = await read_reply(reader)
    assert "connected_replicas:1" in info

    writer.close()
    await writer.wait_closed()
    await replica.stop()
    await primary.stop()


async def test_info_reports_keyspace_size():
    server = Server(host="127.0.0.1", port=0)
    await server.start()
    port = server._asyncio_server.sockets[0].getsockname()[1]

    reader, writer = await open_client(port)
    await send(writer, "SET", "a", "1")
    await read_reply(reader)
    await send(writer, "SET", "b", "2")
    await read_reply(reader)
    await send(writer, "INFO")
    info = await read_reply(reader)
    assert "db0:keys=2" in info

    writer.close()
    await writer.wait_closed()
    await server.stop()
