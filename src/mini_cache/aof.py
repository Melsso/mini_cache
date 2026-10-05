from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path

from mini_cache.commands import dispatch
from mini_cache.protocol import ProtocolError, encode, read_command
from mini_cache.store import Store

logger = logging.getLogger("mini_cache.aof")

WRITE_COMMANDS = {"SET", "DEL", "EXPIRE", "PEXPIREAT", "PERSIST", "FLUSHDB"}
FSYNC_POLICIES = ("always", "everysec", "no")


def is_write_command(name: str) -> bool:
    return name.upper() in WRITE_COMMANDS


def _deadline_ms(now: float, seconds: str) -> int:
    return int(round((now + float(seconds)) * 1000))


def to_aof_commands(command: list[str], now: float | None = None) -> list[list[str]]:
    name = command[0].upper()
    now = time.time() if now is None else now
    if name == "SET" and len(command) == 5 and command[3].upper() == "EX":
        key, value = command[1], command[2]
        return [
            ["SET", key, value],
            ["PEXPIREAT", key, str(_deadline_ms(now, command[4]))],
        ]
    if name == "EXPIRE" and len(command) == 3:
        return [["PEXPIREAT", command[1], str(_deadline_ms(now, command[2]))]]
    return [command]


class AOFLog:
    def __init__(self, path: str | Path, fsync: str = "always") -> None:
        if fsync not in FSYNC_POLICIES:
            raise ValueError(f"fsync must be one of {FSYNC_POLICIES}, got {fsync!r}")
        self.path = Path(path)
        self.fsync = fsync
        self._dirty = False
        self._file = open(self.path, "ab")

    def append(self, command: list[str]) -> None:
        self._file.write(encode(command))
        self._file.flush()
        if self.fsync == "always":
            os.fsync(self._file.fileno())
        else:
            self._dirty = True

    def sync(self) -> None:
        if self._dirty and not self._file.closed:
            self._dirty = False
            os.fsync(self._file.fileno())

    async def replay(self, store: Store) -> int:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return 0

        data = self.path.read_bytes()
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()

        count = 0
        good = 0
        while True:
            try:
                command = await read_command(reader)
            except (asyncio.IncompleteReadError, ProtocolError) as exc:
                self._discard_tail(data, good, exc)
                break
            if command is None:
                break
            good += len(encode(command))
            if not command:
                continue
            dispatch(store, command)
            count += 1
        return count

    def _discard_tail(self, data: bytes, good: int, exc: Exception) -> None:
        tail = data[good:]
        logger.warning(
            "AOF %s has an incomplete or corrupt tail (%d bytes after offset %d: %s); "
            "truncating it (saved to %s.corrupt)",
            self.path,
            len(tail),
            good,
            exc,
            self.path,
        )
        Path(f"{self.path}.corrupt").write_bytes(tail)
        self._file.flush()
        self._file.truncate(good)

    def close(self) -> None:
        if self._file.closed:
            return
        self.sync()
        self._file.close()
