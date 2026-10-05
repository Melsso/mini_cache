from __future__ import annotations

import asyncio
from typing import Any

CRLF = b"\r\n"

MAX_BULK_LENGTH = 64 * 1024 * 1024
MAX_ARRAY_LENGTH = 1024 * 1024


class ProtocolError(Exception):
    """Raised when the peer sends malformed RESP."""


def _parse_int(text: str, what: str) -> int:
    try:
        return int(text)
    except ValueError as exc:
        raise ProtocolError(f"invalid {what}: {text!r}") from exc


async def read_command(reader: asyncio.StreamReader) -> list[str] | None:
    line = await _read_line(reader)
    if line is None:
        return None

    if not line.startswith("*"):
        return line.split()

    num_args = _parse_int(line[1:], "array length")

    if num_args < 0:
        return []
    if num_args > MAX_ARRAY_LENGTH:
        raise ProtocolError(f"array length too large: {num_args}")

    args: list[str] = []
    for _ in range(num_args):
        args.append(await _read_bulk_string(reader))
    return args


async def _read_line(reader: asyncio.StreamReader) -> str | None:
    try:
        raw = await reader.readline()
    except ValueError as exc:
        raise ProtocolError("line too long") from exc
    if raw == b"":
        return None
    if not raw.endswith(CRLF):
        raise ProtocolError(f"line not CRLF-terminated: {raw!r}")
    return raw[:-2].decode("utf-8", errors="replace")


def _check_bulk_length(length: int) -> None:
    if length < -1 or length > MAX_BULK_LENGTH:
        raise ProtocolError(f"invalid bulk string length: {length}")


async def _read_bulk_string(reader: asyncio.StreamReader) -> str:
    header = await _read_line(reader)
    if header is None:
        raise ProtocolError("unexpected EOF reading bulk string header")
    if not header.startswith("$"):
        raise ProtocolError(f"expected bulk string, got: {header!r}")

    length = _parse_int(header[1:], "bulk string length")
    _check_bulk_length(length)
    if length == -1:
        return ""

    data = await reader.readexactly(length)
    trailer = await reader.readexactly(2)
    if trailer != CRLF:
        raise ProtocolError("bulk string missing trailing CRLF")
    return data.decode("utf-8", errors="replace")


def encode(value: Any) -> bytes:
    if isinstance(value, SimpleString):
        return b"+" + value.encode("utf-8") + CRLF
    if isinstance(value, Error):
        return b"-" + value.encode("utf-8") + CRLF
    if isinstance(value, bool):
        return b":" + (b"1" if value else b"0") + CRLF
    if isinstance(value, int):
        return b":" + str(value).encode("ascii") + CRLF
    if value is None:
        return b"$-1" + CRLF
    if isinstance(value, (bytes, bytearray)):
        return b"$" + str(len(value)).encode("ascii") + CRLF + bytes(value) + CRLF
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return b"$" + str(len(raw)).encode("ascii") + CRLF + raw + CRLF
    if isinstance(value, (list, tuple)):
        parts = [b"*" + str(len(value)).encode("ascii") + CRLF]
        parts.extend(encode(item) for item in value)
        return b"".join(parts)

    raise TypeError(f"cannot encode value of type {type(value)!r} as RESP")


class SimpleString(str):
    __slots__ = ()


class Error(str):
    __slots__ = ()


async def read_reply(reader: asyncio.StreamReader) -> object:
    line = await _read_line(reader)
    if line is None:
        raise ProtocolError("unexpected EOF reading reply")
    if not line:
        raise ProtocolError("empty reply line")

    prefix, body = line[0], line[1:]

    if prefix == "+":
        return body
    if prefix == "-":
        return Error(body)
    if prefix == ":":
        return _parse_int(body, "integer reply")
    if prefix == "$":
        length = _parse_int(body, "bulk string length")
        _check_bulk_length(length)
        if length == -1:
            return None
        data = await reader.readexactly(length)
        await reader.readexactly(2)
        return data.decode("utf-8", errors="replace")
    if prefix == "*":
        count = _parse_int(body, "array length")
        if count == -1:
            return None
        if count < 0 or count > MAX_ARRAY_LENGTH:
            raise ProtocolError(f"invalid array length: {count}")
        return [await read_reply(reader) for _ in range(count)]

    raise ProtocolError(f"unknown reply type: {line!r}")
