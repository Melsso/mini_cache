from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from mini_cache import __version__
from mini_cache.aof import FSYNC_POLICIES
from mini_cache.server import DEFAULT_HOST, DEFAULT_PORT, Server


async def _run(server: Server) -> None:
    """Serve until SIGINT/SIGTERM, then shut down cleanly (closing clients and the AOF)."""
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
        default="always",
        help="when to fsync the AOF: every write (safest), about once per second, or never",
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
    )
    try:
        asyncio.run(_run(server))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
