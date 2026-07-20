import asyncio

import pytest

from mini_cache.protocol import (
    Error,
    ProtocolError,
    SimpleString,
    encode,
    read_command,
)


def make_reader(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


@pytest.mark.asyncio
async def test_read_simple_set_command():
    raw = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    reader = make_reader(raw)
    cmd = await read_command(reader)
    assert cmd == ["SET", "foo", "bar"]


@pytest.mark.asyncio
async def test_read_multiple_commands_from_same_stream():
    raw = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
    reader = make_reader(raw)
    first = await read_command(reader)
    second = await read_command(reader)
    assert first == ["PING"]
    assert second == ["GET", "foo"]


@pytest.mark.asyncio
async def test_read_inline_command():
    reader = make_reader(b"PING\r\n")
    cmd = await read_command(reader)
    assert cmd == ["PING"]


@pytest.mark.asyncio
async def test_read_returns_none_on_clean_eof():
    reader = make_reader(b"")
    cmd = await read_command(reader)
    assert cmd is None


@pytest.mark.asyncio
async def test_read_empty_bulk_string_arg():
    raw = b"*2\r\n$3\r\nSET\r\n$0\r\n\r\n"
    reader = make_reader(raw)
    cmd = await read_command(reader)
    assert cmd == ["SET", ""]


@pytest.mark.asyncio
async def test_malformed_array_length_raises():
    reader = make_reader(b"*notanumber\r\n")
    with pytest.raises(ProtocolError):
        await read_command(reader)


@pytest.mark.asyncio
async def test_malformed_bulk_header_raises():
    raw = b"*1\r\nnotadollar\r\n"
    reader = make_reader(raw)
    with pytest.raises(ProtocolError):
        await read_command(reader)


def test_encode_simple_string():
    assert encode(SimpleString("OK")) == b"+OK\r\n"


def test_encode_error():
    assert encode(Error("ERR bad thing")) == b"-ERR bad thing\r\n"


def test_encode_integer():
    assert encode(42) == b":42\r\n"


def test_encode_bool_as_integer():
    assert encode(True) == b":1\r\n"
    assert encode(False) == b":0\r\n"


def test_encode_none_as_null_bulk_string():
    assert encode(None) == b"$-1\r\n"


def test_encode_string_as_bulk_string():
    assert encode("hello") == b"$5\r\nhello\r\n"


def test_encode_empty_string():
    assert encode("") == b"$0\r\n\r\n"


def test_encode_list_as_array():
    assert encode(["a", "bb"]) == b"*2\r\n$1\r\na\r\n$2\r\nbb\r\n"


def test_encode_nested_array():
    assert encode([1, ["x"]]) == b"*2\r\n:1\r\n*1\r\n$1\r\nx\r\n"


def test_encode_unicode_string_uses_byte_length_not_char_length():
    assert encode("café") == b"$5\r\ncaf\xc3\xa9\r\n"


@pytest.mark.asyncio
async def test_round_trip_encode_then_decode():
    command = ["SET", "key", "value with spaces"]
    encoded = encode(command)
    reader = make_reader(encoded)
    decoded = await read_command(reader)
    assert decoded == command
