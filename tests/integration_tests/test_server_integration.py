import asyncio

import pytest

from mini_cache.server import Server


@pytest.fixture
async def running_server():
    server = Server(host="127.0.0.1", port=0)
    await server.start()
    port = server._asyncio_server.sockets[0].getsockname()[1]
    yield port
    await server.stop()


async def open_client(port):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    return reader, writer


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
    if prefix == "*":
        count = int(body)
        if count == -1:
            return None
        return [await read_reply(reader) for _ in range(count)]
    raise ValueError(f"unknown reply prefix: {prefix!r}")


async def test_ping_over_real_socket(running_server):
    reader, writer = await open_client(running_server)
    await send(writer, "PING")
    reply = await read_reply(reader)
    assert reply == "PONG"
    writer.close()
    await writer.wait_closed()


async def test_set_get_del_over_real_socket(running_server):
    reader, writer = await open_client(running_server)

    await send(writer, "SET", "foo", "bar")
    assert await read_reply(reader) == "OK"

    await send(writer, "GET", "foo")
    assert await read_reply(reader) == "bar"

    await send(writer, "DEL", "foo")
    assert await read_reply(reader) == 1

    await send(writer, "GET", "foo")
    assert await read_reply(reader) is None

    writer.close()
    await writer.wait_closed()


async def test_ttl_expiry_over_real_socket(running_server):
    reader, writer = await open_client(running_server)

    await send(writer, "SET", "foo", "bar", "EX", "1")
    assert await read_reply(reader) == "OK"

    await send(writer, "GET", "foo")
    assert await read_reply(reader) == "bar"

    await asyncio.sleep(1.2)

    await send(writer, "GET", "foo")
    assert await read_reply(reader) is None

    writer.close()
    await writer.wait_closed()


async def test_multiple_concurrent_clients(running_server):
    reader_a, writer_a = await open_client(running_server)
    reader_b, writer_b = await open_client(running_server)

    await send(writer_a, "SET", "shared", "from_a")
    assert await read_reply(reader_a) == "OK"

    await send(writer_b, "GET", "shared")
    assert await read_reply(reader_b) == "from_a"

    writer_a.close()
    writer_b.close()
    await writer_a.wait_closed()
    await writer_b.wait_closed()


async def test_unknown_command_returns_error_not_disconnect(running_server):
    reader, writer = await open_client(running_server)

    await send(writer, "NOTACOMMAND")
    reply = await read_reply(reader)
    assert isinstance(reply, Exception)

    await send(writer, "PING")
    assert await read_reply(reader) == "PONG"

    writer.close()
    await writer.wait_closed()


async def test_client_disconnect_does_not_crash_server(running_server):
    reader, writer = await open_client(running_server)
    writer.close()
    await writer.wait_closed()

    reader2, writer2 = await open_client(running_server)
    await send(writer2, "PING")
    assert await read_reply(reader2) == "PONG"
    writer2.close()
    await writer2.wait_closed()


async def test_inline_ping_from_raw_client(running_server):
    reader, writer = await open_client(running_server)
    writer.write(b"PING\r\n")
    await writer.drain()
    reply = await read_reply(reader)
    assert reply == "PONG"
    writer.close()
    await writer.wait_closed()


async def test_flushdb_and_dbsize_over_real_socket(running_server):
    reader, writer = await open_client(running_server)

    await send(writer, "SET", "a", "1")
    await read_reply(reader)
    await send(writer, "SET", "b", "2")
    await read_reply(reader)

    await send(writer, "DBSIZE")
    assert await read_reply(reader) == 2

    await send(writer, "FLUSHDB")
    assert await read_reply(reader) == "OK"

    await send(writer, "DBSIZE")
    assert await read_reply(reader) == 0

    writer.close()
    await writer.wait_closed()
