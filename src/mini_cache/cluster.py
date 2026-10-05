from __future__ import annotations

import asyncio
import math
import bisect
import hashlib
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import TypeVar

from mini_cache.protocol import Arg, Error, ProtocolError, encode, read_reply

DEFAULT_VIRTUAL_NODES = 150
DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_READ_TIMEOUT = 5.0
DEFAULT_POOL_SIZE = 4
DEL_BATCH = 500

T = TypeVar("T")


class ClusterError(Exception):
    """Raised when a shard answers a command with an error reply."""


class ClusterTimeoutError(ClusterError, TimeoutError):
    """Raised when a shard does not accept a connection or answer in time."""


def _ttl_args(ttl: float) -> list[Arg]:
    if not math.isfinite(ttl) or ttl <= 0:
        raise ValueError("ttl must be a positive, finite number of seconds")
    if float(ttl).is_integer():
        return ["EX", str(int(ttl))]
    return ["PX", str(max(1, round(ttl * 1000)))]


def _expire_command(key: Arg, seconds: float) -> list[Arg]:
    if not math.isfinite(seconds):
        raise ValueError("seconds must be finite")
    if float(seconds).is_integer():
        return ["EXPIRE", key, str(int(seconds))]
    return ["PEXPIRE", key, str(round(seconds * 1000))]


class ConsistentHashRing:
    def __init__(
        self, nodes: list[str], virtual_nodes: int = DEFAULT_VIRTUAL_NODES
    ) -> None:
        self.virtual_nodes = virtual_nodes
        self._ring: dict[int, str] = {}
        self._sorted_hashes: list[int] = []
        for node in nodes:
            self.add_node(node)

    def add_node(self, node: str) -> None:
        if node in self.nodes():
            return
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node}#{i}")
            self._ring[h] = node
            bisect.insort(self._sorted_hashes, h)

    def remove_node(self, node: str) -> None:
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node}#{i}")
            self._ring.pop(h, None)
            idx = bisect.bisect_left(self._sorted_hashes, h)
            if idx < len(self._sorted_hashes) and self._sorted_hashes[idx] == h:
                del self._sorted_hashes[idx]

    def get_node(self, key: Arg) -> str:
        if not self._sorted_hashes:
            raise ValueError("no nodes in ring")
        h = self._hash(key)
        idx = bisect.bisect(self._sorted_hashes, h)
        if idx == len(self._sorted_hashes):
            idx = 0
        return self._ring[self._sorted_hashes[idx]]

    def nodes(self) -> set[str]:
        return set(self._ring.values())

    @staticmethod
    def _hash(value: Arg) -> int:
        raw = value if isinstance(value, bytes) else value.encode("utf-8")
        return int(hashlib.md5(raw, usedforsecurity=False).hexdigest(), 16)


def _integer(reply: object) -> int:
    if isinstance(reply, int) and not isinstance(reply, bool):
        return reply
    raise ClusterError(f"unexpected reply: {reply!r}")


def _opt_bytes(reply: object) -> bytes | None:
    if reply is None or isinstance(reply, bytes):
        return reply
    raise ClusterError(f"unexpected reply: {reply!r}")


def _bytes_list(reply: object) -> list[bytes]:
    if isinstance(reply, list) and all(isinstance(item, bytes) for item in reply):
        return reply
    raise ClusterError(f"unexpected reply: {reply!r}")


@dataclass
class _Conn:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class _Pool:
    def __init__(self, size: int) -> None:
        self.idle: list[_Conn] = []
        self.slots = asyncio.Semaphore(size)


class ClusterPipeline:
    def __init__(self, client: ClusterClient) -> None:
        self._client = client
        self._commands: list[list[Arg]] = []

    def command(self, *parts: Arg) -> ClusterPipeline:
        if len(parts) < 2:
            raise ValueError("pipelined commands need a key as their first argument")
        self._commands.append(list(parts))
        return self

    def set(self, key: Arg, value: Arg, ttl: float | None = None) -> ClusterPipeline:
        parts: list[Arg] = ["SET", key, value]
        if ttl is not None:
            parts += _ttl_args(ttl)
        return self.command(*parts)

    def get(self, key: Arg) -> ClusterPipeline:
        return self.command("GET", key)

    def delete(self, key: Arg) -> ClusterPipeline:
        return self.command("DEL", key)

    def incr(self, key: Arg, amount: int = 1) -> ClusterPipeline:
        return self.command("INCRBY", key, str(amount))

    async def execute(self, raise_on_error: bool = True) -> list[object]:
        commands, self._commands = self._commands, []
        return await self._client._execute_many(commands, raise_on_error)


class ClusterClient:
    def __init__(
        self,
        shards: list[tuple[str, int]],
        virtual_nodes: int = DEFAULT_VIRTUAL_NODES,
        *,
        connect_timeout: float | None = DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float | None = DEFAULT_READ_TIMEOUT,
        password: str | None = None,
        pool_size: int = DEFAULT_POOL_SIZE,
    ) -> None:
        if pool_size < 1:
            raise ValueError("pool_size must be >= 1")
        self.shards = shards
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.password = password
        self.pool_size = pool_size
        self.ring = ConsistentHashRing(
            [self._node_id(h, p) for h, p in shards], virtual_nodes
        )
        self._pools: dict[str, _Pool] = {}
        self._closed = False

    async def __aenter__(self) -> ClusterClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    def node_for(self, key: Arg) -> str:
        return self.ring.get_node(key)

    async def set(
        self,
        key: Arg,
        value: Arg,
        ttl: float | None = None,
        *,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        parts: list[Arg] = ["SET", key, value]
        if ttl is not None:
            parts += _ttl_args(ttl)
        if nx:
            parts.append("NX")
        if xx:
            parts.append("XX")
        return await self._execute(key, parts) == "OK"

    async def get(self, key: Arg) -> bytes | None:
        return _opt_bytes(await self._execute(key, ["GET", key]))

    async def delete(self, key: Arg) -> int:
        return _integer(await self._execute(key, ["DEL", key]))

    async def exists(self, key: Arg) -> int:
        return _integer(await self._execute(key, ["EXISTS", key]))

    async def incr(self, key: Arg, amount: int = 1) -> int:
        return _integer(await self._execute(key, ["INCRBY", key, str(amount)]))

    async def expire(self, key: Arg, seconds: float) -> bool:
        return _integer(await self._execute(key, _expire_command(key, seconds))) == 1

    async def ttl(self, key: Arg) -> int:
        return _integer(await self._execute(key, ["TTL", key]))

    async def mget(self, keys: Sequence[Arg]) -> list[bytes | None]:
        groups: dict[str, list[int]] = {}
        for i, key in enumerate(keys):
            groups.setdefault(self.node_for(key), []).append(i)
        results: list[bytes | None] = [None] * len(keys)

        async def fetch(node_id: str, indexes: list[int]) -> None:
            parts: list[Arg] = ["MGET", *(keys[i] for i in indexes)]
            reply = await self._roundtrip(node_id, parts)
            if not isinstance(reply, list) or len(reply) != len(indexes):
                raise ClusterError(f"{node_id}: unexpected MGET reply")
            for i, value in zip(indexes, reply):
                results[i] = _opt_bytes(value)

        await asyncio.gather(*(fetch(n, ix) for n, ix in groups.items()))
        return results

    async def mset(self, mapping: Mapping[Arg, Arg]) -> None:
        groups: dict[str, list[Arg]] = {}
        for key, value in mapping.items():
            groups.setdefault(self.node_for(key), []).extend([key, value])
        await asyncio.gather(
            *(self._roundtrip(n, ["MSET", *flat]) for n, flat in groups.items())
        )

    def pipeline(self) -> ClusterPipeline:
        return ClusterPipeline(self)

    async def keys(self, pattern: Arg = "*") -> list[bytes]:
        replies = await self._each_shard(["KEYS", pattern])
        found: list[bytes] = []
        for reply in replies.values():
            found.extend(_bytes_list(reply))
        return found

    async def delete_pattern(self, pattern: Arg) -> int:
        async def purge(node_id: str) -> int:
            keys = _bytes_list(await self._roundtrip(node_id, ["KEYS", pattern]))
            deleted = 0
            for i in range(0, len(keys), DEL_BATCH):
                batch: list[Arg] = ["DEL", *keys[i : i + DEL_BATCH]]
                deleted += _integer(await self._roundtrip(node_id, batch))
            return deleted

        counts = await asyncio.gather(*(purge(n) for n in self._node_ids()))
        return sum(counts)

    async def flush_all(self) -> None:
        await self._each_shard(["FLUSHDB"])

    async def dbsize(self) -> int:
        return sum((await self.dbsize_per_shard()).values())

    async def dbsize_per_shard(self) -> dict[str, int]:
        replies = await self._each_shard(["DBSIZE"])
        return {node_id: _integer(reply) for node_id, reply in replies.items()}

    async def close(self) -> None:
        self._closed = True
        for pool in self._pools.values():
            while pool.idle:
                await self._close_writer(pool.idle.pop().writer)

    def _node_ids(self) -> list[str]:
        return [self._node_id(host, port) for host, port in self.shards]

    async def _each_shard(self, parts: list[Arg]) -> dict[str, object]:
        node_ids = self._node_ids()
        replies = await asyncio.gather(*(self._roundtrip(n, parts) for n in node_ids))
        return dict(zip(node_ids, replies))

    async def _execute(self, key: Arg, parts: list[Arg]) -> object:
        return await self._roundtrip(self.node_for(key), parts)

    async def _roundtrip(self, node_id: str, parts: Sequence[Arg]) -> object:
        async def op(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> object:
            writer.write(encode(parts))
            await writer.drain()
            return await read_reply(reader)

        reply = await self._run(node_id, op)
        if isinstance(reply, Error):
            raise ClusterError(f"{node_id}: {reply}")
        return reply

    async def _execute_many(
        self, commands: list[list[Arg]], raise_on_error: bool
    ) -> list[object]:
        groups: dict[str, list[int]] = {}
        for i, parts in enumerate(commands):
            groups.setdefault(self.node_for(parts[1]), []).append(i)
        results: list[object] = [None] * len(commands)

        async def run_batch(node_id: str, indexes: list[int]) -> None:
            batch = [commands[i] for i in indexes]

            async def op(
                reader: asyncio.StreamReader, writer: asyncio.StreamWriter
            ) -> list[object]:
                writer.write(b"".join(encode(parts) for parts in batch))
                await writer.drain()
                return [await read_reply(reader) for _ in batch]

            for i, reply in zip(indexes, await self._run(node_id, op)):
                results[i] = reply

        await asyncio.gather(*(run_batch(n, ix) for n, ix in groups.items()))
        if raise_on_error:
            for reply in results:
                if isinstance(reply, Error):
                    raise ClusterError(str(reply))
        return results

    async def _run(
        self,
        node_id: str,
        op: Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[T]],
    ) -> T:
        pool = self._pools.get(node_id)
        if pool is None:
            pool = self._pools[node_id] = _Pool(self.pool_size)
        while True:
            await pool.slots.acquire()
            conn: _Conn | None = None
            reused = False
            ok = False
            try:
                if pool.idle:
                    conn = pool.idle.pop()
                    reused = True
                else:
                    conn = await self._connect(node_id)
                result = await asyncio.wait_for(
                    op(conn.reader, conn.writer), self.read_timeout
                )
                ok = True
                return result
            except ClusterError:
                raise
            except TimeoutError:
                raise ClusterTimeoutError(
                    f"{node_id}: no reply within {self.read_timeout}s"
                ) from None
            except (OSError, ProtocolError, asyncio.IncompleteReadError):
                if not reused:
                    raise
            finally:
                if conn is not None:
                    if ok and not self._closed:
                        pool.idle.append(conn)
                    else:
                        conn.writer.close()
                pool.slots.release()

    async def _connect(self, node_id: str) -> _Conn:
        host, port_str = node_id.rsplit(":", 1)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, int(port_str)), self.connect_timeout
            )
        except TimeoutError:
            raise ClusterTimeoutError(
                f"{node_id}: connect timed out after {self.connect_timeout}s"
            ) from None
        if self.password is not None:
            try:
                reply = await asyncio.wait_for(
                    self._auth(reader, writer), self.read_timeout
                )
            except BaseException:
                writer.close()
                raise
            if isinstance(reply, Error):
                writer.close()
                raise ClusterError(f"{node_id}: {reply}")
        return _Conn(reader, writer)

    async def _auth(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> object:
        writer.write(encode(["AUTH", self.password]))
        await writer.drain()
        return await read_reply(reader)

    @staticmethod
    async def _close_writer(writer: asyncio.StreamWriter) -> None:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    @staticmethod
    def _node_id(host: str, port: int) -> str:
        return f"{host}:{port}"
