from __future__ import annotations

import argparse
import asyncio
import os
import random
import tempfile
import time

from mini_cache.protocol import encode, read_reply
from mini_cache.server import Server


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, int(len(sorted_values) * pct / 100))
    return sorted_values[idx]


async def run_client(
    host: str,
    port: int,
    make_command,
    count: int,
    pipeline: int,
    latencies: list[float],
) -> None:
    reader, writer = await asyncio.open_connection(host, port)
    done = 0
    while done < count:
        batch = min(pipeline, count - done)
        payload = b"".join(encode(make_command()) for _ in range(batch))
        start = time.perf_counter()
        writer.write(payload)
        await writer.drain()
        for _ in range(batch):
            await read_reply(reader)
        latencies.append(time.perf_counter() - start)
        done += batch
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass


async def run_scenario(
    name: str,
    host: str,
    port: int,
    make_command_factory,
    args: argparse.Namespace,
) -> dict[str, float | str]:
    per_client = max(1, args.requests // args.clients)
    total = per_client * args.clients
    latencies: list[float] = []

    start = time.perf_counter()
    await asyncio.gather(
        *(
            run_client(
                host,
                port,
                make_command_factory(random.Random(args.seed + i)),
                per_client,
                args.pipeline,
                latencies,
            )
            for i in range(args.clients)
        )
    )
    elapsed = time.perf_counter() - start

    latencies.sort()
    return {
        "scenario": name,
        "ops_per_sec": total / elapsed,
        "p50_ms": percentile(latencies, 50) * 1000,
        "p95_ms": percentile(latencies, 95) * 1000,
        "p99_ms": percentile(latencies, 99) * 1000,
        "max_ms": (latencies[-1] * 1000) if latencies else 0.0,
    }


async def preload(host: str, port: int, args: argparse.Namespace) -> None:
    reader, writer = await asyncio.open_connection(host, port)
    value = "x" * args.value_size
    chunk = 500
    for start in range(0, args.keyspace, chunk):
        end = min(args.keyspace, start + chunk)
        writer.write(
            b"".join(encode(["SET", f"key:{i}", value]) for i in range(start, end))
        )
        await writer.drain()
        for _ in range(start, end):
            await read_reply(reader)
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass


async def run_suite(host: str, port: int, label: str, args: argparse.Namespace) -> None:
    value = "x" * args.value_size
    keyspace = args.keyspace

    def set_factory(rng: random.Random):
        return lambda: ["SET", f"key:{rng.randrange(keyspace)}", value]

    def get_factory(rng: random.Random):
        return lambda: ["GET", f"key:{rng.randrange(keyspace)}"]

    def set_ex_factory(rng: random.Random):
        return lambda: ["SET", f"key:{rng.randrange(keyspace)}", value, "EX", "60"]

    def mixed_factory(rng: random.Random):
        def make():
            key = f"key:{rng.randrange(keyspace)}"
            if rng.random() < 0.8:
                return ["GET", key]
            return ["SET", key, value]

        return make

    await preload(host, port, args)

    results = []
    for name, factory in (
        ("GET", get_factory),
        ("SET", set_factory),
        ("SET EX 60", set_ex_factory),
        ("80% GET / 20% SET", mixed_factory),
    ):
        results.append(await run_scenario(name, host, port, factory, args))

    print(f"\n=== {label} ===")
    print(
        f"clients={args.clients} requests={args.requests} pipeline={args.pipeline} "
        f"value_size={args.value_size}B keyspace={args.keyspace}"
    )
    header = f"{'scenario':<20}{'ops/s':>12}{'p50 ms':>10}{'p95 ms':>10}{'p99 ms':>10}{'max ms':>10}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['scenario']:<20}{r['ops_per_sec']:>12,.0f}"
            f"{r['p50_ms']:>10.2f}{r['p95_ms']:>10.2f}{r['p99_ms']:>10.2f}{r['max_ms']:>10.2f}"
        )


async def run_in_process(
    label: str,
    aof_path: str | None,
    args: argparse.Namespace,
    aof_fsync: str = "always",
) -> None:
    server = Server(host="127.0.0.1", port=0, aof_path=aof_path, aof_fsync=aof_fsync)
    await server.start()
    port = server.bound_port
    try:
        await run_suite("127.0.0.1", port, label, args)
    finally:
        await server.stop()


async def main_async(args: argparse.Namespace) -> None:
    if args.port is not None:
        await run_suite(
            args.host, args.port, f"external server {args.host}:{args.port}", args
        )
        return

    await run_in_process("in-process, no persistence", None, args)
    with tempfile.TemporaryDirectory() as tmp:
        for policy in ("everysec", "always"):
            await run_in_process(
                f"in-process, AOF enabled (fsync: {policy})",
                os.path.join(tmp, f"bench-{policy}.aof"),
                args,
                aof_fsync=policy,
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark mini_cache.")
    parser.add_argument(
        "--host", default="127.0.0.1", help="host of an external server"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="benchmark an already running server on this port (default: start in-process servers)",
    )
    parser.add_argument(
        "--clients", type=int, default=50, help="concurrent connections"
    )
    parser.add_argument(
        "--requests", type=int, default=20000, help="total requests per scenario"
    )
    parser.add_argument(
        "--pipeline", type=int, default=1, help="commands per round trip"
    )
    parser.add_argument(
        "--value-size", type=int, default=64, help="value size in bytes"
    )
    parser.add_argument(
        "--keyspace", type=int, default=10000, help="number of distinct keys"
    )
    parser.add_argument("--seed", type=int, default=1234)
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
