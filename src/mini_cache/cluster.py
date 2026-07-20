from __future__ import annotations

import asyncio
import bisect
import hashlib

from mini_cache.protocol import encode, read_reply

DEFAULT_VIRTUAL_NODES = 150


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
        return int(hashlib.md5(value.encode("utf-8")).hexdigest(), 16)


class ClusterClient:
    def __init__(
        self, shards: list[tuple[str, int]], virtual_nodes: int = DEFAULT_VIRTUAL_NODES
    ) -> None:
        self.shards = shards
        self.ring = ConsistentHashRing(
            [self._node_id(h, p) for h, p in shards], virtual_nodes
        )
        self._connections: dict[
            str, tuple[asyncio.StreamReader, asyncio.StreamWriter]
        ] = {}

    def node_for(self, key: str) -> str:
        return self.ring.get_node(key)

    async def set(self, key: str, value: str, ttl: float | None = None) -> object:
        parts = ["SET", key, value]
        if ttl is not None:
            parts += ["EX", str(ttl)]
        return await self._execute(key, parts)

    async def get(self, key: str) -> object:
        return await self._execute(key, ["GET", key])

    async def delete(self, key: str) -> object:
        return await self._execute(key, ["DEL", key])

    async def exists(self, key: str) -> object:
        return await self._execute(key, ["EXISTS", key])

    async def expire(self, key: str, seconds: float) -> object:
        return await self._execute(key, ["EXPIRE", key, str(seconds)])

    async def ttl(self, key: str) -> object:
        return await self._execute(key, ["TTL", key])

    async def dbsize_per_shard(self) -> dict[str, object]:
        results: dict[str, object] = {}
        for host, port in self.shards:
            node_id = self._node_id(host, port)
            reader, writer = await self._connection_for(node_id)
            writer.write(encode(["DBSIZE"]))
            await writer.drain()
            results[node_id] = await read_reply(reader)
        return results

    async def close(self) -> None:
        for reader, writer in self._connections.values():
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
        self._connections.clear()

    async def _execute(self, key: str, parts: list[str]) -> object:
        node_id = self.node_for(key)
        reader, writer = await self._connection_for(node_id)
        writer.write(encode(parts))
        await writer.drain()
        return await read_reply(reader)

    async def _connection_for(
        self, node_id: str
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if node_id not in self._connections:
            host, port_str = node_id.rsplit(":", 1)
            reader, writer = await asyncio.open_connection(host, int(port_str))
            self._connections[node_id] = (reader, writer)
        return self._connections[node_id]

    @staticmethod
    def _node_id(host: str, port: int) -> str:
        return f"{host}:{port}"
