# mini_cache

A small, from-scratch distributed cache server, wire-compatible with the RESP protocol used by Redis — real `redis-cli` and Redis client libraries can talk to it directly.

Built incrementally as a learning project: single-node cache → persistence → replication → failover → sharding.

## Features

- **RESP2 wire protocol** — works with `redis-cli` and standard Redis clients
- **Commands** — `GET`, `SET` (with `EX` TTL), `DEL`, `EXISTS`, `EXPIRE`, `PEXPIREAT`, `TTL`, `PERSIST`, `KEYS [pattern]`, `DBSIZE`, `FLUSHDB`, `PING`, `ECHO`, `INFO`, `QUIT`
- **TTLs** — lazy expiry on access plus a heap-driven background sweep (cost proportional to the number of expired keys)
- **AOF persistence** — every write logged to disk and replayed on restart; TTLs are stored as absolute deadlines, a torn tail from a crash is repaired automatically, `fsync` policy is configurable
- **Primary/replica replication** — live write propagation, full snapshot sync on connect (a reconnecting replica starts from a clean keyspace)
- **Failover-aware replication link** — heartbeats detect a dead primary/replica within seconds, automatic reconnect with backoff
- **Client-side sharding** — consistent-hashing router (`ClusterClient`) spreads keys across multiple independent primaries; safe for concurrent use and reconnects on its own
- **Read-only replicas** — writes against a replica are rejected with `READONLY`

## Installation

Requires Python 3.11 or 3.12. There are no runtime dependencies.

```bash
pip install "git+https://github.com/Melsso/mini_cache.git@v0.1.0"
```

With Poetry:

```toml
mini-cache = { git = "https://github.com/Melsso/mini_cache.git", tag = "v0.1.0" }
```

Wheels and sdists are also attached to each [GitHub release](https://github.com/Melsso/mini_cache/releases).

## Architecture

```
mini-cache/
├── src/mini_cache/
│   ├── protocol.py      # RESP encode/decode — no networking, no command logic
│   ├── store.py         # in-memory key-value engine with TTL (lazy + heap-driven active expiry)
│   ├── commands.py      # command name -> Store operation dispatch table
│   ├── server.py        # asyncio TCP server: connections, replication, AOF, INFO
│   ├── aof.py           # append-only file: log writes, replay on startup, crash repair
│   ├── replication.py   # replica client: SYNC, snapshot + live stream, heartbeats
│   ├── cluster.py       # consistent hash ring + multi-shard router client
│   └── __main__.py      # CLI entry point
├── benchmarks/bench.py  # throughput / latency benchmark
└── tests/               # unit + integration tests (real sockets)
```

Each module has exactly one job and no knowledge of the others' internals — `protocol.py` never touches a socket, `store.py` never touches RESP, `server.py` is the only place that wires networking to storage.

## Quickstart

```bash
poetry install
poetry run mini_cache                 # single node, 127.0.0.1:6380

# in another terminal
redis-cli -p 6380 SET foo bar
redis-cli -p 6380 GET foo
redis-cli -p 6380 KEYS 'f*'
redis-cli -p 6380 INFO
```

### Use as a library

```python
import asyncio
from mini_cache import Server, ClusterClient

async def main():
    server = Server(host="127.0.0.1", port=6380, aof_path="cache.aof")
    await server.start()

    async with ClusterClient([("127.0.0.1", 6380)]) as client:
        await client.set("user:1", "alice", ttl=60)
        print(await client.get("user:1"))   # alice

    await server.stop()

asyncio.run(main())
```

`ClusterClient` raises `ClusterError` when a shard answers with an error.

### Replication

```bash
poetry run mini_cache --port 6380                              # primary
poetry run mini_cache --port 6381 --replica-of 127.0.0.1:6380  # replica

redis-cli -p 6380 SET foo bar
redis-cli -p 6381 GET foo        # -> "bar", replicated live
redis-cli -p 6381 SET foo baz    # -> (error) READONLY
```

### Sharded cluster (client-side)

```python
from mini_cache import ClusterClient
import asyncio

async def main():
    async with ClusterClient([("127.0.0.1", 6400), ("127.0.0.1", 6401), ("127.0.0.1", 6402)]) as client:
        await client.set("user:1", "alice")
        print(await client.get("user:1"))

asyncio.run(main())
```

### Docker

```bash
docker compose up --build
python verify_cluster.py     # end-to-end checks against the running compose stack
```

## CLI options

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | bind address |
| `--port` | `6380` | bind port |
| `--aof-path` | `mini_cache.aof` | AOF file path; empty string disables persistence (ignored on replicas) |
| `--aof-fsync` | `always` | `always` (fsync every write), `everysec` (background, ~1s), `no` (leave it to the OS) |
| `--replica-of` | none | `host:port` of a primary to replicate from |
| `-v` / `--verbose` | off | debug logging |
| `--version` | | print the version |

The server shuts down cleanly on `SIGINT`/`SIGTERM` (clients are disconnected and the AOF is flushed and closed), so `docker stop` does not have to wait for a forced kill.

## Testing

```bash
poetry run pytest
poetry run mypy .
poetry run ruff check .
```

Integration tests spin up real `Server` instances on ephemeral ports and drive them over actual TCP sockets — not mocks — including primary/replica failover, crash recovery of a torn AOF and multi-shard key routing.

## Benchmark

```bash
poetry run python benchmarks/bench.py                      # in-process servers: no AOF, AOF everysec, AOF always
poetry run python benchmarks/bench.py --port 6379          # compare with a real Redis
poetry run python benchmarks/bench.py --clients 100 --requests 200000 --pipeline 16
```

Reports ops/s and p50/p95/p99 latency per round trip for GET, SET, SET with TTL and an 80/20 mix. The load generator is Python/asyncio and shares a CPU with the server, so treat the numbers as relative, not absolute. `--aof-fsync always` is by far the dominant cost for writes.

## Known limitations

- **RESP2 only** — no RESP3, no pub/sub, no transactions (`MULTI`/`EXEC`)
- **Small command set** — no `INCR`, `MGET`/`MSET`, `SELECT`, `SET NX/XX/PX`, `PEXPIRE`; `KEYS` patterns use Python `fnmatch` syntax, which is close to but not identical to Redis globbing
- **Values are UTF-8 text** — binary-unsafe; invalid bytes are replaced on decode
- **No authentication or TLS** — do not expose it to untrusted networks (the Docker image binds `0.0.0.0`)
- **Single-threaded per process** — `asyncio`, not a real thread/process pool; fine for a learning project, not for production throughput
- **No automatic failover** — if a primary dies, a replica does *not* automatically promote itself; you restart a primary manually (or point a replica config at a new one)
- **Replication is simple** — a slow replica slows writes on the primary, replicas cannot be chained, replicated TTLs drift by the network delay, and replicas ignore `--aof-path`
- **Sharding has no rebalancing tool** — adding/removing a shard remaps keys on the hash ring, but nothing physically migrates the data that moved
- **AOF has no compaction** — it grows forever; real Redis periodically rewrites it compactly, this doesn't
- **No cluster gossip** — shard membership is static, passed to `ClusterClient` by the caller, not auto-discovered

## Design notes

- **Why RESP instead of a custom protocol?** Wire-compatibility with `redis-cli` and existing client libraries, for barely more implementation cost than inventing a new format.
- **Why AOF instead of RDB snapshots?** Simpler to implement correctly and reason about — it's literally the same command log used for replication snapshots, reused as the persistence format.
- **Why absolute TTL deadlines in the AOF?** Replaying `SET k v EX 10` after a restart would hand the key a fresh 10 seconds, and would bring back keys that expired while the server was down. The log therefore stores `PEXPIREAT` wall-clock deadlines. Replication keeps relative TTLs on purpose, so it does not depend on synchronized clocks between machines.
- **Why client-side sharding instead of a cluster protocol?** Keeps each node's code identical and simple; the routing complexity lives in one place (`ClusterClient`) instead of being smeared across every server instance via gossip.

## License

MIT — see [LICENSE](LICENSE).
