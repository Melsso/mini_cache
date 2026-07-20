from __future__ import annotations

import asyncio
from typing import Any

CRLF = b"\r\n"


class ProtocolError(Exception):
    """Raised when the client sends malformed RESP."""


async def read_command(reader: asyncio.StreamReader) -> list[str] | None:
    line = await _read_line(reader)
    if line is None:
        return None

    if not line.startswith("*"):
        return line.split()

    try:
        num_args = int(line[1:])
    except ValueError as exc:
        raise ProtocolError(f"invalid array length: {line!r}") from exc

    if num_args < 0:
        return []

    args: list[str] = []
    for _ in range(num_args):
        args.append(await _read_bulk_string(reader))
    return args


async def _read_line(reader: asyncio.StreamReader) -> str | None:
    raw = await reader.readline()
    if raw == b"":
        return None
    if not raw.endswith(CRLF):
        raise ProtocolError(f"line not CRLF-terminated: {raw!r}")
    return raw[:-2].decode("utf-8", errors="replace")


async def _read_bulk_string(reader: asyncio.StreamReader) -> str:
    header = await _read_line(reader)
    if header is None:
        raise ProtocolError("unexpected EOF reading bulk string header")
    if not header.startswith("$"):
        raise ProtocolError(f"expected bulk string, got: {header!r}")

    length = int(header[1:])
    if length == -1:
        return ""

    data = await reader.readexactly(length)
    trailer = await reader.readexactly(2)
    if trailer != CRLF:
        raise ProtocolError("bulk string missing trailing CRLF")
    return data.decode("utf-8", errors="replace")


def encode(value: Any) -> bytes:
    """
    Encode a Python value as a RESP reply.

    Convention used by command handlers:
      - str            -> simple string, EXCEPT the special OKType/ErrType below
      - SimpleString    -> "+...\r\n"
      - Error           -> "-...\r\n"
      - int / bool      -> ":...\r\n"
      - None            -> "$-1\r\n"      (null bulk string)
      - bytes           -> "$len\r\n...\r\n" (bulk string)
      - list / tuple    -> "*len\r\n" + encode(each item)
    """
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

    prefix, body = line[0], line[1:]

    if prefix == "+":
        return body
    if prefix == "-":
        return Error(body)
    if prefix == ":":
        return int(body)
    if prefix == "$":
        length = int(body)
        if length == -1:
            return None
        data = await reader.readexactly(length)
        await reader.readexactly(2)
        return data.decode("utf-8", errors="replace")
    if prefix == "*":
        count = int(body)
        if count == -1:
            return None
        return [await read_reply(reader) for _ in range(count)]

    raise ProtocolError(f"unknown reply type: {line!r}")
