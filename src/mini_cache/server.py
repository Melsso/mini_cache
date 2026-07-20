from __future__ import annotations

import asyncio
import logging

from mini_cache.aof import AOFLog, is_write_command
from mini_cache.commands import dispatch
from mini_cache.protocol import Error, ProtocolError, encode, read_command
from mini_cache.replication import (
    HEARTBEAT_INTERVAL,
    LINK_TIMEOUT,
    ReplicaClient,
    build_snapshot_commands,
)
from mini_cache.store import Store

logger = logging.getLogger("mini_cache.server")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6380
EXPIRY_SWEEP_INTERVAL = 0.1


class Server:
    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        aof_path: str | None = None,
        replica_of: tuple[str, int] | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.store = Store()
        self.aof: AOFLog | None = None
        self._aof_path = aof_path
        self.replica_of = replica_of
        self.replica_client: ReplicaClient | None = None
        self._replica_task: asyncio.Task | None = None
        self._replicas: set[asyncio.StreamWriter] = set()
        self._replica_heartbeat_task: asyncio.Task | None = None
        self._asyncio_server: asyncio.base_events.Server | None = None
        self._sweep_task: asyncio.Task | None = None
        self._client_count = 0

    async def start(self) -> None:
        if self._aof_path is not None:
            self.aof = AOFLog(self._aof_path)
            replayed = await self.aof.replay(self.store)
            logger.info("replayed %d command(s) from %s", replayed, self._aof_path)

        if self.replica_of is not None:
            primary_host, primary_port = self.replica_of
            self.replica_client = ReplicaClient(primary_host, primary_port, self.store)
            self._replica_task = asyncio.create_task(self.replica_client.run())

        self._asyncio_server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        self._sweep_task = asyncio.create_task(self._sweep_loop())
        self._replica_heartbeat_task = asyncio.create_task(
            self._replica_heartbeat_loop()
        )
        addr = self._asyncio_server.sockets[0].getsockname()
        logger.info("mini_cache listening on %s:%s", addr[0], addr[1])

    async def serve_forever(self) -> None:
        await self.start()
        assert self._asyncio_server is not None
        async with self._asyncio_server:
            await self._asyncio_server.serve_forever()

    async def stop(self) -> None:
        if self._sweep_task is not None:
            self._sweep_task.cancel()
        if self._replica_task is not None:
            self._replica_task.cancel()
        if self._replica_heartbeat_task is not None:
            self._replica_heartbeat_task.cancel()
        if self._asyncio_server is not None:
            self._asyncio_server.close()
            await self._asyncio_server.wait_closed()
        if self.aof is not None:
            self.aof.close()
        for writer in list(self._replicas):
            writer.close()
        self._replicas.clear()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        self._client_count += 1
        logger.info("client connected: %s (active=%d)", peer, self._client_count)

        try:
            while True:
                try:
                    command = await read_command(reader)
                except ProtocolError as exc:
                    logger.warning("protocol error from %s: %s", peer, exc)
                    writer.write(encode(_protocol_error_reply(exc)))
                    await writer.drain()
                    break

                if command is None:
                    break
                if not command:
                    continue

                if command[0].upper() == "SYNC":
                    await self._serve_replica(reader, writer)
                    return

                reply: object
                if self.replica_of is not None and is_write_command(command[0]):
                    reply = Error("READONLY You can't write against a replica.")
                    writer.write(encode(reply))
                    await writer.drain()
                    continue

                reply = dispatch(self.store, command)
                if not isinstance(reply, Error) and is_write_command(command[0]):
                    if self.aof is not None:
                        self.aof.append(command)
                    await self._propagate_to_replicas(command)
                writer.write(encode(reply))
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            self._client_count -= 1
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.info("client disconnected: %s (active=%d)", peer, self._client_count)

    async def _serve_replica(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        logger.info("replica connected: %s", peer)
        self._replicas.add(writer)
        try:
            for snapshot_command in build_snapshot_commands(self.store):
                writer.write(encode(snapshot_command))
            await writer.drain()

            while True:
                try:
                    command = await asyncio.wait_for(
                        read_command(reader), timeout=LINK_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    logger.warning("replica %s timed out, dropping", peer)
                    break
                if command is None:
                    break
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            self._replicas.discard(writer)
            logger.info("replica disconnected: %s", peer)

    async def _propagate_to_replicas(self, command: list[str]) -> None:
        dead: list[asyncio.StreamWriter] = []
        for writer in self._replicas:
            try:
                writer.write(encode(command))
                await writer.drain()
            except (ConnectionResetError, BrokenPipeError):
                dead.append(writer)
        for writer in dead:
            self._replicas.discard(writer)

    async def _replica_heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                if self._replicas:
                    await self._propagate_to_replicas(["PING"])
        except asyncio.CancelledError:
            pass

    async def _sweep_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(EXPIRY_SWEEP_INTERVAL)
                removed = self.store.sweep_expired()
                if removed:
                    logger.debug("active expiry swept %d key(s)", removed)
        except asyncio.CancelledError:
            pass


def _protocol_error_reply(exc: ProtocolError):
    from mini_cache.protocol import Error

    return Error(f"ERR Protocol error: {exc}")
