from __future__ import annotations

import asyncio
import hmac
import ipaddress
import logging
import time
from collections.abc import Callable, Sequence

from mini_cache.aof import (
    DEFAULT_FSYNC,
    FSYNC_POLICIES,
    AOFLog,
    is_write_command,
    to_aof_commands,
)
from mini_cache.commands import dispatch
from mini_cache.protocol import (
    Arg,
    Error,
    ProtocolError,
    SimpleString,
    encode,
    read_command,
)
from mini_cache.replication import (
    HEARTBEAT_INTERVAL,
    LINK_TIMEOUT,
    ReplicaClient,
    build_snapshot_commands,
    to_replication_command,
)
from mini_cache.store import Store

logger = logging.getLogger("mini_cache.server")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6380
DEFAULT_MAXCLIENTS = 10_000
DEFAULT_OUTPUT_LIMIT = 64 * 1024 * 1024
DEFAULT_AOF_REWRITE_MIN_SIZE = 64 * 1024 * 1024
DEFAULT_AOF_REWRITE_PERCENTAGE = 100
EXPIRY_SWEEP_INTERVAL = 0.1
AOF_SYNC_INTERVAL = 1.0
REPLICA_BACKLOG_LIMIT = 64 * 1024 * 1024
CLIENT_WRITE_TIMEOUT = 30.0
DENYOOM = {"SET", "MSET", "INCR", "DECR", "INCRBY", "DECRBY"}


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class _ClientOverflow(Exception):
    """A client's pending output exceeded the configured limit."""


class _ReplicaLink:
    def __init__(
        self,
        writer: asyncio.StreamWriter,
        limit: int,
        on_close: Callable[[_ReplicaLink], object],
    ) -> None:
        self.writer = writer
        self.closed = False
        self._limit = limit
        self._on_close = on_close
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._pending = 0
        self._task = asyncio.create_task(self._pump())

    def send(self, payload: bytes, *, force: bool = False) -> None:
        if self.closed:
            return
        self._pending += len(payload)
        if self._pending > self._limit and not force:
            logger.warning(
                "replica %s is %d bytes behind (limit %d), dropping it",
                self.writer.get_extra_info("peername"),
                self._pending,
                self._limit,
            )
            self.close()
            return
        self._queue.put_nowait(payload)

    async def _pump(self) -> None:
        try:
            while True:
                payload = await self._queue.get()
                self.writer.write(payload)
                await self.writer.drain()
                self._pending -= len(payload)
        except (OSError, asyncio.CancelledError):
            pass
        finally:
            self.close()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.writer.close()
        self._on_close(self)
        if asyncio.current_task() is not self._task:
            self._task.cancel()


class Server:
    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        aof_path: str | None = None,
        replica_of: tuple[str, int] | None = None,
        aof_fsync: str = DEFAULT_FSYNC,
        requirepass: str | None = None,
        masterauth: str | None = None,
        maxmemory: int = 0,
        maxmemory_policy: str = "allkeys-lru",
        maxclients: int = DEFAULT_MAXCLIENTS,
        client_timeout: float = 0.0,
        client_output_limit: int = DEFAULT_OUTPUT_LIMIT,
        aof_rewrite_min_size: int = DEFAULT_AOF_REWRITE_MIN_SIZE,
        aof_rewrite_percentage: int = DEFAULT_AOF_REWRITE_PERCENTAGE,
    ) -> None:
        if aof_fsync not in FSYNC_POLICIES:
            raise ValueError(
                f"aof_fsync must be one of {FSYNC_POLICIES}, got {aof_fsync!r}"
            )
        self.host = host
        self.port = port
        self._maxmemory_ignored = replica_of is not None and maxmemory > 0
        self.store = Store(
            maxmemory=0 if replica_of is not None else maxmemory,
            policy=maxmemory_policy,
        )
        self.aof: AOFLog | None = None
        self._aof_path = aof_path
        self._aof_fsync = aof_fsync
        self.aof_rewrite_min_size = aof_rewrite_min_size
        self.aof_rewrite_percentage = aof_rewrite_percentage
        self._aof_base_size = 0
        self.requirepass = requirepass or None
        self.masterauth = masterauth or None
        self.maxclients = maxclients
        self.client_timeout = client_timeout
        self.client_output_limit = client_output_limit
        self.replica_of = replica_of
        self.replica_client: ReplicaClient | None = None
        self._replica_task: asyncio.Task | None = None
        self._replicas: set[_ReplicaLink] = set()
        self._replica_heartbeat_task: asyncio.Task | None = None
        self._asyncio_server: asyncio.base_events.Server | None = None
        self._sweep_task: asyncio.Task | None = None
        self._aof_task: asyncio.Task | None = None
        self._rewrite_task: asyncio.Task | None = None
        self._client_tasks: set[asyncio.Task] = set()
        self._client_count = 0
        self._connections_received = 0
        self._rejected_connections = 0
        self._commands_processed = 0
        self._start_time: float | None = None

    @property
    def bound_port(self) -> int:
        if self._asyncio_server is None or not self._asyncio_server.sockets:
            raise RuntimeError("server is not running")
        return int(self._asyncio_server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        self._start_time = time.monotonic()
        self._warn_if_exposed()
        if self._maxmemory_ignored:
            logger.warning(
                "--maxmemory is ignored on replicas; they follow the primary"
            )
        if self._aof_path is not None:
            if self.replica_of is not None:
                logger.warning(
                    "AOF is ignored on replicas (replicated writes are not logged); "
                    "starting without persistence"
                )
            else:
                self.aof = AOFLog(self._aof_path, fsync=self._aof_fsync)
                replayed = await self.aof.replay(self.store)
                self._aof_base_size = self.aof.size
                logger.info("replayed %d command(s) from %s", replayed, self._aof_path)

        if self.replica_of is not None:
            primary_host, primary_port = self.replica_of
            self.replica_client = ReplicaClient(
                primary_host,
                primary_port,
                self.store,
                password=self.masterauth or self.requirepass,
            )
            self._replica_task = asyncio.create_task(self.replica_client.run())

        self._asyncio_server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        self._sweep_task = asyncio.create_task(self._sweep_loop())
        self._replica_heartbeat_task = asyncio.create_task(
            self._replica_heartbeat_loop()
        )
        if self.aof is not None:
            self._aof_task = asyncio.create_task(self._aof_loop())
        logger.info("mini_cache listening on %s:%s", self.host, self.bound_port)

    def _warn_if_exposed(self) -> None:
        if _is_loopback(self.host):
            return
        if self.requirepass is None:
            logger.warning(
                "listening on %s with NO password: anyone who can reach this port "
                "can read, write and flush the cache (use --requirepass)",
                self.host,
            )
        else:
            logger.warning(
                "listening on non-loopback %s: there is no TLS, so the password "
                "and data cross the network unencrypted",
                self.host,
            )

    async def serve_forever(self) -> None:
        await self.start()
        assert self._asyncio_server is not None
        try:
            await self._asyncio_server.serve_forever()
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._asyncio_server is not None:
            self._asyncio_server.close()

        await self._cancel_task(self._sweep_task)
        await self._cancel_task(self._replica_task)
        await self._cancel_task(self._replica_heartbeat_task)
        await self._cancel_task(self._rewrite_task)
        await self._cancel_task(self._aof_task)

        for task in list(self._client_tasks):
            task.cancel()
        if self._client_tasks:
            await asyncio.gather(*self._client_tasks, return_exceptions=True)

        for link in list(self._replicas):
            link.close()

        if self._asyncio_server is not None:
            await self._asyncio_server.wait_closed()

        if self.aof is not None:
            self.aof.close()
        self._replicas.clear()

    @staticmethod
    async def _cancel_task(task: asyncio.Task | None) -> None:
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("background task raised during shutdown")

    def _check_auth(self, args: Sequence[bytes]) -> Error | SimpleString:
        if self.requirepass is None:
            return Error("ERR AUTH called without any password configured")
        if len(args) not in (1, 2):
            return Error("ERR wrong number of arguments for 'AUTH' command")
        if len(args) == 2 and args[0] != b"default":
            return Error("WRONGPASS invalid username-password pair")
        if hmac.compare_digest(args[-1], self.requirepass.encode()):
            return SimpleString("OK")
        return Error("WRONGPASS invalid username-password pair")

    async def _send(self, writer: asyncio.StreamWriter, payload: bytes) -> None:
        writer.write(payload)
        buffered = writer.transport.get_write_buffer_size()
        if buffered:
            if self.client_output_limit and buffered > self.client_output_limit:
                raise _ClientOverflow(buffered)
            await asyncio.wait_for(writer.drain(), CLIENT_WRITE_TIMEOUT)

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        peer = writer.get_extra_info("peername")
        self._connections_received += 1

        if self.maxclients and self._client_count >= self.maxclients:
            self._rejected_connections += 1
            logger.warning(
                "rejecting %s: maxclients (%d) reached", peer, self.maxclients
            )
            try:
                writer.write(encode(Error("ERR max number of clients reached")))
                await writer.drain()
            except OSError:
                pass
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return

        if task is not None:
            self._client_tasks.add(task)
        self._client_count += 1
        logger.info("client connected: %s (active=%d)", peer, self._client_count)

        authed = self.requirepass is None

        try:
            while True:
                try:
                    read = read_command(reader)
                    if self.client_timeout > 0:
                        command = await asyncio.wait_for(read, self.client_timeout)
                    else:
                        command = await read
                except asyncio.TimeoutError:
                    logger.info("closing idle client %s", peer)
                    break
                except ProtocolError as exc:
                    logger.warning("protocol error from %s: %s", peer, exc)
                    await self._send(writer, encode(_protocol_error_reply(exc)))
                    break
                except asyncio.IncompleteReadError:
                    break

                if command is None:
                    break
                if not command:
                    continue

                self._commands_processed += 1
                name = command[0].decode("ascii", errors="replace").upper()

                if name == "QUIT":
                    await self._send(writer, encode(SimpleString("OK")))
                    break

                if name == "AUTH":
                    auth_reply = self._check_auth(command[1:])
                    if self.requirepass is not None:
                        authed = isinstance(auth_reply, SimpleString)
                    await self._send(writer, encode(auth_reply))
                    continue

                if not authed:
                    await self._send(
                        writer, encode(Error("NOAUTH Authentication required."))
                    )
                    continue

                if name == "SYNC":
                    if self.replica_of is not None:
                        await self._send(
                            writer,
                            encode(Error("ERR chained replication is not supported")),
                        )
                        continue
                    await self._serve_replica(reader, writer)
                    return

                if name == "INFO":
                    await self._send(writer, encode(self._build_info()))
                    continue

                if name == "BGREWRITEAOF":
                    await self._send(writer, encode(self._start_rewrite()))
                    continue

                if self.replica_of is not None and is_write_command(name):
                    await self._send(
                        writer,
                        encode(Error("READONLY You can't write against a replica.")),
                    )
                    continue

                if (
                    name in DENYOOM
                    and self.store.policy == "noeviction"
                    and self.store.over_limit()
                ):
                    await self._send(
                        writer,
                        encode(
                            Error(
                                "OOM command not allowed when used memory > 'maxmemory'."
                            )
                        ),
                    )
                    continue

                reply: object
                before = self.store.writes
                reply = dispatch(self.store, command)
                if not isinstance(reply, Error) and is_write_command(name):
                    if name != "SET" or self.store.writes != before:
                        if self.aof is not None:
                            for aof_command in to_aof_commands(command):
                                self.aof.append(aof_command)
                        self._propagate(to_replication_command(command))
                    for evicted_key in self.store.drain_evictions():
                        eviction: list[Arg] = ["DEL", evicted_key]
                        if self.aof is not None:
                            self.aof.append(eviction)
                        self._propagate(eviction)
                    if self.aof is not None:
                        await self.aof.commit()
                await self._send(writer, encode(reply))
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass
        except _ClientOverflow as exc:
            logger.warning(
                "client %s exceeded the output buffer limit (%s > %d bytes), closing",
                peer,
                exc,
                self.client_output_limit,
            )
        except asyncio.TimeoutError:
            logger.warning("client %s is not reading its replies, closing", peer)
        except Exception:
            logger.exception("unexpected error while serving %s", peer)
        finally:
            self._client_count -= 1
            if task is not None:
                self._client_tasks.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.info("client disconnected: %s (active=%d)", peer, self._client_count)

    def _build_info(self) -> str:
        uptime = int(time.monotonic() - self._start_time) if self._start_time else 0
        store = self.store
        lines = [
            "# Server",
            f"tcp_port:{self.bound_port}",
            f"uptime_in_seconds:{uptime}",
            "# Clients",
            f"connected_clients:{self._client_count}",
            f"maxclients:{self.maxclients}",
            "# Memory",
            f"used_memory:{store.used_memory}",
            f"maxmemory:{store.maxmemory}",
            f"maxmemory_policy:{store.policy}",
            "# Stats",
            f"total_connections_received:{self._connections_received}",
            f"rejected_connections:{self._rejected_connections}",
            f"total_commands_processed:{self._commands_processed}",
            f"keyspace_hits:{store.hits}",
            f"keyspace_misses:{store.misses}",
            f"expired_keys:{store.expired_keys}",
            f"evicted_keys:{store.evicted_keys}",
            "# Replication",
            f"role:{'replica' if self.replica_of else 'master'}",
            f"connected_replicas:{len(self._replicas)}",
        ]
        if self.replica_of is not None:
            master_host, master_port = self.replica_of
            link_up = bool(
                self.replica_client and self.replica_client.connected.is_set()
            )
            lines.append(f"master_host:{master_host}")
            lines.append(f"master_port:{master_port}")
            lines.append(f"master_link_status:{'up' if link_up else 'down'}")
        lines += [
            "# Persistence",
            f"aof_enabled:{1 if self.aof is not None else 0}",
        ]
        if self.aof is not None:
            lines += [
                f"aof_current_size:{self.aof.size}",
                f"aof_rewrite_in_progress:{1 if self._rewrite_running() else 0}",
                f"aof_rewrites_completed:{self.aof.rewrites}",
            ]
        lines += [
            "# Keyspace",
            f"db0:keys={len(store)}",
        ]
        return "\n".join(lines) + "\n"

    def _rewrite_running(self) -> bool:
        return self._rewrite_task is not None and not self._rewrite_task.done()

    def _start_rewrite(self) -> Error | SimpleString:
        if self.aof is None:
            return Error("ERR AOF is not enabled")
        if self._rewrite_running():
            return Error(
                "ERR Background append only file rewriting already in progress"
            )
        self._rewrite_task = asyncio.create_task(self._rewrite_aof())
        return SimpleString("Background append only file rewriting started")

    async def _rewrite_aof(self) -> None:
        assert self.aof is not None
        before = self.aof.size
        try:
            count = await self.aof.rewrite(self.store)
        except Exception:
            logger.exception("AOF rewrite failed")
            return
        self._aof_base_size = self.aof.size
        logger.info(
            "AOF rewritten: %d command(s), %d -> %d bytes",
            count,
            before,
            self._aof_base_size,
        )

    def _should_auto_rewrite(self) -> bool:
        if (
            self.aof is None
            or self.aof_rewrite_min_size <= 0
            or self._rewrite_running()
        ):
            return False
        size = self.aof.size
        if size < self.aof_rewrite_min_size:
            return False
        base = max(self._aof_base_size, 1)
        return size * 100 >= base * (100 + self.aof_rewrite_percentage)

    async def _aof_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(AOF_SYNC_INTERVAL)
                aof = self.aof
                if aof is None:
                    continue
                if self._aof_fsync == "everysec":
                    await aof.sync_async()
                if self._should_auto_rewrite():
                    self._start_rewrite()
        except asyncio.CancelledError:
            pass

    async def _serve_replica(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        logger.info("replica connected: %s", peer)
        if self.aof is None:
            logger.warning(
                "replica %s is syncing from a primary WITHOUT AOF: if this primary "
                "restarts empty, a full resync will reset the replica to empty too",
                peer,
            )
        link = _ReplicaLink(writer, REPLICA_BACKLOG_LIMIT, self._replicas.discard)
        link.send(encode(["FLUSHDB"]), force=True)
        for snapshot_command in build_snapshot_commands(self.store):
            link.send(encode(snapshot_command), force=True)
        self._replicas.add(link)
        try:
            while not link.closed:
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
            link.close()
            logger.info("replica disconnected: %s", peer)

    def _propagate(self, command: Sequence[Arg]) -> None:
        if not self._replicas:
            return
        payload = encode(command)
        for link in list(self._replicas):
            link.send(payload)

    async def _replica_heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                self._propagate(["PING"])
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


def _protocol_error_reply(exc: ProtocolError) -> Error:
    return Error(f"ERR Protocol error: {exc}")
