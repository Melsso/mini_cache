from __future__ import annotations

import asyncio
import logging

from mini_cache.commands import dispatch
from mini_cache.protocol import encode, read_command
from mini_cache.store import Store

logger = logging.getLogger("mini_cache.replication")

RECONNECT_DELAY = 1.0
HEARTBEAT_INTERVAL = 1.0
LINK_TIMEOUT = 5.0


def build_snapshot_commands(store: Store) -> list[list[str]]:
    commands: list[list[str]] = []
    for key in store.keys():
        value = store.get(key)
        if value is None:
            continue
        commands.append(["SET", key, value])
        ttl = store.ttl(key)
        if ttl is not None and ttl >= 0:
            commands.append(["EXPIRE", key, str(int(ttl) + 1)])
    return commands


class ReplicaClient:
    def __init__(self, primary_host: str, primary_port: int, store: Store) -> None:
        self.primary_host = primary_host
        self.primary_port = primary_port
        self.store = store
        self.connected = asyncio.Event()

    async def run(self) -> None:
        while True:
            try:
                await self._connect_and_stream()
            except asyncio.CancelledError:
                raise
            except (ConnectionError, OSError) as exc:
                logger.warning(
                    "replication link to %s:%s lost: %s",
                    self.primary_host,
                    self.primary_port,
                    exc,
                )
            self.connected.clear()
            await asyncio.sleep(RECONNECT_DELAY)

    async def _connect_and_stream(self) -> None:
        reader, writer = await asyncio.open_connection(
            self.primary_host, self.primary_port
        )
        writer.write(encode(["SYNC"]))
        await writer.drain()
        logger.info(
            "connected to primary %s:%s, syncing", self.primary_host, self.primary_port
        )
        self.connected.set()

        heartbeat_task = asyncio.create_task(self._send_heartbeats(writer))
        try:
            while True:
                try:
                    command = await asyncio.wait_for(
                        read_command(reader), timeout=LINK_TIMEOUT
                    )
                except asyncio.TimeoutError as exc:
                    raise ConnectionError("replication link timed out") from exc
                if command is None:
                    break
                if not command:
                    continue
                if command[0].upper() == "PING":
                    continue
                dispatch(self.store, command)
        finally:
            heartbeat_task.cancel()
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _send_heartbeats(self, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                writer.write(encode(["PING"]))
                await writer.drain()
        except asyncio.CancelledError:
            pass
        except (ConnectionResetError, BrokenPipeError):
            pass
