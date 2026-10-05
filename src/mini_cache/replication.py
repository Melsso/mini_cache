from __future__ import annotations

import asyncio
import logging

from mini_cache.commands import dispatch, parse_set
from mini_cache.protocol import Error, encode, read_command, read_reply
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


def to_replication_command(command: list[str]) -> list[str]:
    if command[0].upper() == "SET":
        try:
            opts = parse_set(command[1:])
        except ValueError:
            return command
        out = ["SET", opts.key, opts.value]
        if opts.ttl is not None:
            out += ["PX", str(max(1, int(round(opts.ttl * 1000))))]
        return out
    return command


class ReplicaClient:
    def __init__(
        self,
        primary_host: str,
        primary_port: int,
        store: Store,
        password: str | None = None,
    ) -> None:
        self.primary_host = primary_host
        self.primary_port = primary_port
        self.store = store
        self.password = password
        self.connected = asyncio.Event()

    async def run(self) -> None:
        while True:
            try:
                await self._connect_and_stream()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "replication link to %s:%s lost: %s: %s",
                    self.primary_host,
                    self.primary_port,
                    type(exc).__name__,
                    exc,
                )
            self.connected.clear()
            await asyncio.sleep(RECONNECT_DELAY)

    async def _connect_and_stream(self) -> None:
        reader, writer = await asyncio.open_connection(
            self.primary_host, self.primary_port
        )
        heartbeat_task: asyncio.Task | None = None
        try:
            if self.password is not None:
                writer.write(encode(["AUTH", self.password]))
                await writer.drain()
                reply = await asyncio.wait_for(read_reply(reader), timeout=LINK_TIMEOUT)
                if isinstance(reply, Error):
                    raise ConnectionError(f"primary rejected AUTH: {reply}")
            writer.write(encode(["SYNC"]))
            await writer.drain()
            logger.info(
                "connected to primary %s:%s, syncing",
                self.primary_host,
                self.primary_port,
            )
            self.connected.set()

            heartbeat_task = asyncio.create_task(self._send_heartbeats(writer))
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
                if command[0].startswith("-"):
                    raise ConnectionError(
                        "primary refused replication: " + " ".join(command)[1:]
                    )
                if command[0].upper() == "PING":
                    continue
                dispatch(self.store, command)
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
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
