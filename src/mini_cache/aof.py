from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Sequence
from pathlib import Path

from mini_cache.commands import dispatch, parse_set
from mini_cache.protocol import Arg, ProtocolError, encode, read_command, to_text
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


def to_aof_commands(
    command: Sequence[Arg], now: float | None = None
) -> list[list[Arg]]:
    name = to_text(command[0]).upper()
    now = time.time() if now is None else now
    if name == "SET":
        try:
            opts = parse_set(command[1:])
        except ValueError:
            return [list(command)]
        set_command: list[Arg] = ["SET", opts.key, opts.value]
        if opts.ttl is None:
            return [set_command]
        deadline = _deadline_ms(now, opts.ttl)
        return [set_command, ["PEXPIREAT", opts.key, str(deadline)]]
    if name == "EXPIRE" and len(command) == 3:
        deadline = _deadline_ms(now, float(to_text(command[2])))
        return [["PEXPIREAT", command[1], str(deadline)]]
    if name == "PEXPIRE" and len(command) == 3:
        deadline = int(now * 1000) + int(to_text(command[2]))
        return [["PEXPIREAT", command[1], str(deadline)]]
    return [list(command)]


def build_rewrite_commands(store: Store, now: float | None = None) -> list[list[Arg]]:
    now = time.time() if now is None else now
    commands: list[list[Arg]] = []
    for key, value, ttl in store.snapshot():
        commands.append(["SET", key, value])
        if ttl is not None:
            commands.append(["PEXPIREAT", key, str(_deadline_ms(now, ttl))])
    return commands


def _write_durable(path: Path, payload: bytes) -> None:
    with open(path, "wb") as f:
        f.write(payload)
        f.flush()
        os.fsync(f.fileno())


class AOFLog:
    def __init__(self, path: str | Path, fsync: str = DEFAULT_FSYNC) -> None:
        if fsync not in FSYNC_POLICIES:
            raise ValueError(f"fsync must be one of {FSYNC_POLICIES}, got {fsync!r}")
        self.path = Path(path)
        self.fsync = fsync
        self.rewrites = 0
        self._dirty = False
        self._seq = 0
        self._durable_seq = 0
        self._commit_lock = asyncio.Lock()
        self._rewrite_buffer: list[bytes] | None = None
        self._file = open(self.path, "ab")

    @property
    def size(self) -> int:
        return self.path.stat().st_size

    @property
    def rewriting(self) -> bool:
        return self._rewrite_buffer is not None

    def append(self, command: Sequence[Arg]) -> None:
        data = encode(command)
        self._file.write(data)
        self._file.flush()
        if self._rewrite_buffer is not None:
            self._rewrite_buffer.append(data)
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

    async def sync_async(self) -> None:
        async with self._commit_lock:
            await asyncio.to_thread(self.sync)

    async def rewrite(self, store: Store) -> int:
        if self._rewrite_buffer is not None:
            raise RuntimeError("AOF rewrite already in progress")
        commands = build_rewrite_commands(store)
        payload = b"".join(encode(c) for c in commands)
        self._rewrite_buffer = []
        tmp = Path(f"{self.path}.rewrite")
        try:
            await asyncio.to_thread(_write_durable, tmp, payload)
            async with self._commit_lock:
                tail = b"".join(self._rewrite_buffer)
                with open(tmp, "ab") as f:
                    f.write(tail)
                    f.flush()
                    os.fsync(f.fileno())
                self._file.close()
                try:
                    os.replace(tmp, self.path)
                finally:
                    self._file = open(self.path, "ab")
                self._durable_seq = self._seq
                self._dirty = False
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        finally:
            self._rewrite_buffer = None
        self.rewrites += 1
        return len(commands)

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
        store.drain_evictions()
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
