from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path

from mini_cache.commands import dispatch, parse_set
from mini_cache.protocol import ProtocolError, encode, read_command
from mini_cache.store import Store


logger = logging.getLogger("mini_cache.aof")

WRITE_COMMANDS = {
    "SET",
    "MSET",
    "DEL",
    "INCR",
    "DECR",
    "INCRBY",
    "DECRBY",
    "EXPIRE",
    "PEXPIRE",
    "PEXPIREAT",
    "PERSIST",
    "FLUSHDB",
}
FSYNC_POLICIES = ("always", "everysec", "no")
DEFAULT_FSYNC = "everysec"


def is_write_command(name: str) -> bool:
    return name.upper() in WRITE_COMMANDS


def _deadline_ms(now: float, seconds: float) -> int:
    return int(round((now + seconds) * 1000))


def to_aof_commands(command: list[str], now: float | None = None) -> list[list[str]]:
    name = command[0].upper()
    now = time.time() if now is None else now
    if name == "SET":
        try:
            opts = parse_set(command[1:])
        except ValueError:
            return [command]
        set_command = ["SET", opts.key, opts.value]
        if opts.ttl is None:
            return [set_command]
        deadline = _deadline_ms(now, opts.ttl)
        return [set_command, ["PEXPIREAT", opts.key, str(deadline)]]
    if name == "EXPIRE" and len(command) == 3:
        deadline = _deadline_ms(now, float(command[2]))
        return [["PEXPIREAT", command[1], str(deadline)]]
    if name == "PEXPIRE" and len(command) == 3:
        deadline = int(now * 1000) + int(command[2])
        return [["PEXPIREAT", command[1], str(deadline)]]
    return [command]


class AOFLog:
    def __init__(self, path: str | Path, fsync: str = DEFAULT_FSYNC) -> None:
        if fsync not in FSYNC_POLICIES:
            raise ValueError(f"fsync must be one of {FSYNC_POLICIES}, got {fsync!r}")
        self.path = Path(path)
        self.fsync = fsync
        self._dirty = False
        self._seq = 0
        self._durable_seq = 0
        self._commit_lock = asyncio.Lock()
        self._file = open(self.path, "ab")

    def append(self, command: list[str]) -> None:
        self._file.write(encode(command))
        self._file.flush()
        self._seq += 1
        self._dirty = True

    async def commit(self) -> None:
        if self.fsync != "always":
            return
        seq = self._seq
        if self._durable_seq >= seq:
            return
        async with self._commit_lock:
            if self._durable_seq >= seq:
                return
            upto = self._seq
            await asyncio.to_thread(os.fsync, self._file.fileno())
            self._durable_seq = upto

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
