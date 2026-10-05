from __future__ import annotations

import asyncio
import bisect
import hashlib
from types import TracebackType

from mini_cache.protocol import Error, ProtocolError, encode, read_reply

DEFAULT_VIRTUAL_NODES = 150
DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_READ_TIMEOUT = 5.0


class ClusterError(Exception):
    """Raised when a shard answers a command with an error reply."""


class ClusterTimeoutError(ClusterError, TimeoutError):
    """Raised when a shard does not accept a connection or answer in time."""


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

    def get_node(self, key: str) -> str:
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
    def _hash(value: str) -> int:
        digest = hashlib.md5(value.encode("utf-8"), usedforsecurity=False)
        return int(digest.hexdigest(), 16)


class ClusterClient:
    def __init__(
        self,
        shards: list[tuple[str, int]],
        virtual_nodes: int = DEFAULT_VIRTUAL_NODES,
        *,
        connect_timeout: float | None = DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float | None = DEFAULT_READ_TIMEOUT,
        password: str | None = None,
    ) -> None:
        self.shards = shards
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.password = password
        self.ring = ConsistentHashRing(
            [self._node_id(h, p) for h, p in shards], virtual_nodes
        )
        self._connections: dict[
            str, tuple[asyncio.StreamReader, asyncio.StreamWriter]
        ] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def __aenter__(self) -> ClusterClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    def node_for(self, key: str) -> str:
        return self.ring.get_node(key)

    async def set(
        self,
        key: str,
        value: str,
        ttl: float | None = None,
        *,
        nx: bool = False,
        xx: bool = False,
    ) -> object:
        parts = ["SET", key, value]
        if ttl is not None:
            parts += ["EX", str(ttl)]
        if nx:
            parts.append("NX")
        if xx:
            parts.append("XX")
        return await self._execute(key, parts)

    async def get(self, key: str) -> object:
        return await self._execute(key, ["GET", key])

    async def delete(self, key: str) -> object:
        return await self._execute(key, ["DEL", key])

    async def exists(self, key: str) -> object:
        return await self._execute(key, ["EXISTS", key])

    async def incr(self, key: str, amount: int = 1) -> object:
        return await self._execute(key, ["INCRBY", key, str(amount)])

    async def expire(self, key: str, seconds: float) -> object:
        return await self._execute(key, ["EXPIRE", key, str(seconds)])

    async def ttl(self, key: str) -> object:
        return await self._execute(key, ["TTL", key])

    async def dbsize_per_shard(self) -> dict[str, object]:
        results: dict[str, object] = {}
        for host, port in self.shards:
            node_id = self._node_id(host, port)
            results[node_id] = await self._roundtrip(node_id, ["DBSIZE"])
        return results

    async def close(self) -> None:
        for node_id in list(self._connections):
            await self._drop_connection(node_id)

    async def _execute(self, key: str, parts: list[str]) -> object:
        return await self._roundtrip(self.node_for(key), parts)

    async def _roundtrip(self, node_id: str, parts: list[str]) -> object:
        lock = self._locks.setdefault(node_id, asyncio.Lock())
        async with lock:
            for attempt in (0, 1):
                try:
                    reader, writer = await self._connection_for(node_id)
                    reply = await asyncio.wait_for(
                        self._exchange(reader, writer, parts), self.read_timeout
                    )
                except ClusterError:
                    self._discard_connection(node_id)
                    raise
                except TimeoutError:
                    self._discard_connection(node_id)
                    raise ClusterTimeoutError(
                        f"{node_id}: no reply within {self.read_timeout}s"
                    ) from None
                except asyncio.CancelledError:
                    self._discard_connection(node_id)
                    raise
                except (OSError, ProtocolError, asyncio.IncompleteReadError):
                    self._discard_connection(node_id)
                    if attempt == 1:
                        raise
                    continue
                if isinstance(reply, Error):
                    raise ClusterError(f"{node_id}: {reply}")
                return reply
        raise AssertionError("unreachable")

    @staticmethod
    async def _exchange(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        parts: list[str],
    ) -> object:
        writer.write(encode(parts))
        await writer.drain()
        return await read_reply(reader)

    async def _connection_for(
        self, node_id: str
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        existing = self._connections.get(node_id)
        if existing is not None:
            return existing
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
                    self._exchange(reader, writer, ["AUTH", self.password]),
                    self.read_timeout,
                )
            except BaseException:
                writer.close()
                raise
            if isinstance(reply, Error):
                writer.close()
                raise ClusterError(f"{node_id}: {reply}")
        self._connections[node_id] = (reader, writer)
        return reader, writer

    def _discard_connection(self, node_id: str) -> None:
        connection = self._connections.pop(node_id, None)
        if connection is not None:
            connection[1].close()

    async def _drop_connection(self, node_id: str) -> None:
        connection = self._connections.pop(node_id, None)
        if connection is None:
            return
        writer = connection[1]
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    @staticmethod
    def _node_id(host: str, port: int) -> str:
        return f"{host}:{port}"
