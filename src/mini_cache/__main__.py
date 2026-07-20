"""
CLI entry point. `poetry run mini_cache` starts a server listening on
127.0.0.1:6380 by default (overridable with --host/--port).
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from mini_cache.server import DEFAULT_HOST, DEFAULT_PORT, Server


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="mini_cache", description="A tiny Redis-like cache server."
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
    )
    try:
        asyncio.run(server.serve_forever())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
