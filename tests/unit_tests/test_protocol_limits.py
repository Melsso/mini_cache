import asyncio

import pytest

from mini_cache.protocol import ProtocolError, read_command, read_reply


def make_reader(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


@pytest.mark.parametrize(
    "raw",
    [
        b"*1\r\n$abc\r\nfoo\r\n",
        b"*1\r\n$-5\r\n",
        b"*1\r\n$99999999999\r\n",
        b"*99999999\r\n",
    ],
)
async def test_malformed_or_oversized_commands_raise_protocol_error(raw):
    with pytest.raises(ProtocolError):
        await read_command(make_reader(raw))


async def test_overlong_line_raises_protocol_error():
    with pytest.raises(ProtocolError):
        await read_command(make_reader(b"A" * 200_000 + b"\r\n"))


@pytest.mark.parametrize(
    "raw",
    [b"\r\n", b":abc\r\n", b"$abc\r\n", b"$-9\r\n", b"*abc\r\n", b"?what\r\n"],
)
async def test_malformed_replies_raise_protocol_error(raw):
    with pytest.raises(ProtocolError):
        await read_reply(make_reader(raw))
