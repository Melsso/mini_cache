from __future__ import annotations

import asyncio
import os
from pathlib import Path

from mini_cache.commands import dispatch
from mini_cache.protocol import encode, read_command
from mini_cache.store import Store

WRITE_COMMANDS = {"SET", "DEL", "EXPIRE", "PERSIST", "FLUSHDB"}


def is_write_command(name: str) -> bool:
    return name.upper() in WRITE_COMMANDS


class AOFLog:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._file = open(self.path, "ab")

    def append(self, command: list[str]) -> None:
        self._file.write(encode(command))
        self._file.flush()
        os.fsync(self._file.fileno())

    async def replay(self, store: Store) -> int:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return 0

        reader = asyncio.StreamReader()
        reader.feed_data(self.path.read_bytes())
        reader.feed_eof()

        count = 0
        while True:
            command = await read_command(reader)
            if command is None:
                break
            if not command:
                continue
            dispatch(store, command)
            count += 1
        return count

    def close(self) -> None:
        self._file.close()
