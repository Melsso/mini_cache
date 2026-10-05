from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import signal

from mini_cache import __version__
from mini_cache.aof import DEFAULT_FSYNC, FSYNC_POLICIES
from mini_cache.server import (
    DEFAULT_AOF_REWRITE_MIN_SIZE,
    DEFAULT_AOF_REWRITE_PERCENTAGE,
    DEFAULT_HOST,
    DEFAULT_MAXCLIENTS,
    DEFAULT_OUTPUT_LIMIT,
    DEFAULT_PORT,
    Server,
)
from mini_cache.store import POLICIES

_UNITS = {
    "": 1,
    "b": 1,
    "k": 1024,
    "kb": 1024,
    "m": 1024**2,
    "mb": 1024**2,
    "g": 1024**3,
    "gb": 1024**3,
}


def parse_size(text: str) -> int:
    match = re.fullmatch(r"\s*(\d+)\s*([a-zA-Z]*)\s*", text)
    if not match or match.group(2).lower() not in _UNITS:
        raise argparse.ArgumentTypeError(
            f"invalid size {text!r} (examples: 1048576, 64mb, 2gb)"
        )
    return int(match.group(1)) * _UNITS[match.group(2).lower()]


async def _run(server: Server) -> None:
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    if task is not None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, task.cancel)
            except (NotImplementedError, RuntimeError):
                pass
    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="mini_cache", description="A tiny Redis-like cache server."
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    parser.add_argument(
        "--host", default=DEFAULT_HOST, help=f"bind host (default: {DEFAULT_HOST})"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"bind port (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--aof-path",
        default="mini_cache.aof",
        help="AOF file path, or empty string to disable persistence",
    )
    parser.add_argument(
        "--aof-fsync",
        choices=FSYNC_POLICIES,
        default=DEFAULT_FSYNC,
        help="when to fsync the AOF: every write (safest), about once per second (default), or never",
    )
    parser.add_argument(
        "--aof-rewrite-min-size",
        type=parse_size,
        default=DEFAULT_AOF_REWRITE_MIN_SIZE,
        help="auto-rewrite the AOF once it is at least this big (0 disables auto-rewrite; default 64mb)",
    )
    parser.add_argument(
        "--aof-rewrite-percentage",
        type=int,
        default=DEFAULT_AOF_REWRITE_PERCENTAGE,
        help="...and has grown by this percentage since the last rewrite (default 100)",
    )
    parser.add_argument(
        "--maxmemory",
        type=parse_size,
        default=0,
        help="memory cap, e.g. 256mb (0 = unlimited; an estimate of payload + overhead, not RSS)",
    )
    parser.add_argument(
        "--maxmemory-policy",
        choices=POLICIES,
        default="allkeys-lru",
        help="what to do at the cap: evict least-recently-used keys, or reject writes",
    )
    parser.add_argument(
        "--maxclients",
        type=int,
        default=DEFAULT_MAXCLIENTS,
        help=f"maximum simultaneous connections (default {DEFAULT_MAXCLIENTS}, 0 = unlimited)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=0.0,
        help="close clients idle for this many seconds (default 0 = never)",
    )
    parser.add_argument(
        "--client-output-limit",
        type=parse_size,
        default=DEFAULT_OUTPUT_LIMIT,
        help="drop a client whose pending replies exceed this size (default 64mb, 0 = no limit)",
    )
    parser.add_argument(
        "--requirepass",
        default=os.environ.get("MINI_CACHE_PASSWORD") or None,
        help="require clients to AUTH with this password (default: $MINI_CACHE_PASSWORD; "
        "prefer the env var, command-line arguments are visible in `ps`)",
    )
    parser.add_argument(
        "--masterauth",
        default=os.environ.get("MINI_CACHE_MASTERAUTH") or None,
        help="password to AUTH with against the primary (default: the --requirepass value)",
    )
    parser.add_argument(
        "--replica-of",
        default=None,
        metavar="HOST:PORT",
        help="run as a replica of the given primary",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="enable debug logging"
    )
    args = parser.parse_args()

    replica_of: tuple[str, int] | None = None
    if args.replica_of:
        host, _, port = args.replica_of.rpartition(":")
        if not host or not port.isdigit():
            parser.error("--replica-of must look like HOST:PORT")
        replica_of = (host, int(port))

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    server = Server(
        host=args.host,
        port=args.port,
        aof_path=args.aof_path or None,
        replica_of=replica_of,
        aof_fsync=args.aof_fsync,
        requirepass=args.requirepass,
        masterauth=args.masterauth,
        maxmemory=args.maxmemory,
        maxmemory_policy=args.maxmemory_policy,
        maxclients=args.maxclients,
        client_timeout=args.timeout,
        client_output_limit=args.client_output_limit,
        aof_rewrite_min_size=args.aof_rewrite_min_size,
        aof_rewrite_percentage=args.aof_rewrite_percentage,
    )
    try:
        asyncio.run(_run(server))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
