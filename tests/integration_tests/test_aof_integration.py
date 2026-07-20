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


async def test_data_survives_server_restart(tmp_path):
    aof_path = str(tmp_path / "restart.aof")

    server1 = Server(host="127.0.0.1", port=0, aof_path=aof_path)
    await server1.start()
    port1 = server1._asyncio_server.sockets[0].getsockname()[1]

    reader, writer = await open_client(port1)
    await send(writer, "SET", "foo", "bar")
    assert await read_reply(reader) == "OK"
    await send(writer, "SET", "counter", "1")
    assert await read_reply(reader) == "OK"
    await send(writer, "DEL", "counter")
    assert await read_reply(reader) == 1
    writer.close()
    await writer.wait_closed()
    await server1.stop()

    server2 = Server(host="127.0.0.1", port=0, aof_path=aof_path)
    await server2.start()
    port2 = server2._asyncio_server.sockets[0].getsockname()[1]

    reader, writer = await open_client(port2)
    await send(writer, "GET", "foo")
    assert await read_reply(reader) == "bar"
    await send(writer, "GET", "counter")
    assert await read_reply(reader) is None
    writer.close()
    await writer.wait_closed()
    await server2.stop()


async def test_read_commands_are_not_persisted(tmp_path):
    aof_path = str(tmp_path / "readonly.aof")

    server1 = Server(host="127.0.0.1", port=0, aof_path=aof_path)
    await server1.start()
    port1 = server1._asyncio_server.sockets[0].getsockname()[1]

    reader, writer = await open_client(port1)
    await send(writer, "SET", "foo", "bar")
    await read_reply(reader)
    await send(writer, "GET", "foo")
    await read_reply(reader)
    await send(writer, "EXISTS", "foo")
    await read_reply(reader)
    writer.close()
    await writer.wait_closed()
    await server1.stop()

    server2 = Server(host="127.0.0.1", port=0, aof_path=aof_path)
    await server2.start()
    assert server2.store.get("foo") == "bar"
    await server2.stop()


async def test_failed_write_command_not_persisted(tmp_path):
    aof_path = str(tmp_path / "failed.aof")

    server1 = Server(host="127.0.0.1", port=0, aof_path=aof_path)
    await server1.start()
    port1 = server1._asyncio_server.sockets[0].getsockname()[1]

    reader, writer = await open_client(port1)
    await send(writer, "SET", "foo")
    reply = await read_reply(reader)
    assert isinstance(reply, Exception)
    writer.close()
    await writer.wait_closed()
    await server1.stop()

    server2 = Server(host="127.0.0.1", port=0, aof_path=aof_path)
    await server2.start()
    assert len(server2.store) == 0
    await server2.stop()
