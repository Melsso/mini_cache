# mini_cache

A small, from-scratch distributed cache server, wire-compatible with the RESP protocol used by Redis — real `redis-cli` and Redis client libraries can talk to it directly.

Built incrementally as a learning project: single-node cache → persistence → replication → failover → sharding.

## Features

- **RESP2 wire protocol** — works with `redis-cli` and standard Redis clients
- **Commands** — `GET`, `SET` (with `EX`, `PX`, `NX`, `XX`, `GET`), `MGET`, `MSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PERSIST`, `KEYS [pattern]`, `DBSIZE`, `FLUSHDB`, `PING`, `ECHO`, `INFO`, `AUTH`, `QUIT`
- **Authentication** — optional password (`--requirepass` / `AUTH`), enforced on clients and on replication links
- **TTLs** — lazy expiry on access plus a heap-driven background sweep (cost proportional to the number of expired keys)
- **AOF persistence** — every write logged to disk and replayed on restart; TTLs are stored as absolute deadlines, a torn tail from a crash is repaired automatically, `fsync` policy is configurable (group commit, off the event loop, for `always`)
- **Primary/replica replication** — live write propagation, full snapshot sync on connect (a reconnecting replica starts from a clean keyspace)
- **Failover-aware replication link** — heartbeats detect a dead primary/replica within seconds, automatic reconnect with backoff
- **Client-side sharding** — consistent-hashing router (`ClusterClient`) spreads keys across multiple independent primaries; safe for concurrent use, reconnects on its own, and has connect/read timeouts
- **Read-only replicas** — writes against a replica are rejected with `READONLY`

## Installation

Requires Python 3.11 or newer. There are no runtime dependencies.

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
│   ├── server.py        # asyncio TCP server: connections, auth, replication, AOF, INFO
│   ├── aof.py           # append-only file: log writes, replay on startup, crash repair, group commit
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
redis-cli -p 6380 SET lock owner-1 NX PX 5000   # atomic "claim this key"
redis-cli -p 6380 INCR hits
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
        print(await client.get("user:1"))             # alice
        print(await client.set("lock", "me", nx=True))  # OK, or None if already claimed
        print(await client.incr("hits"))              # 1

    await server.stop()

asyncio.run(main())
```

`ClusterClient` raises `ClusterError` when a shard answers with an error, and `ClusterTimeoutError` (a `ClusterError` and a `TimeoutError`) when a shard does not accept a connection or answer in time. Timeouts are configurable and a request that times out or is cancelled drops that shard's connection, so a late reply is never mistaken for the answer to the next request:

```python
ClusterClient(shards, connect_timeout=2.0, read_timeout=1.0, password="s3cret")
```

### Authentication

```bash
export MINI_CACHE_PASSWORD='s3cret'      # preferred: keeps it out of `ps`
poetry run mini_cache --host 0.0.0.0     # or: --requirepass s3cret

redis-cli -p 6380 -a s3cret PING
```

Without a password, every command is open to anyone who can reach the port. The server logs a warning when it binds a non-loopback address, with or without a password. There is **no TLS**, so the password and all data cross the network in clear text. Do not expose the port to untrusted networks.

### Replication

```bash
poetry run mini_cache --port 6380                              # primary
poetry run mini_cache --port 6381 --replica-of 127.0.0.1:6380  # replica

redis-cli -p 6380 SET foo bar
redis-cli -p 6381 GET foo        # -> "bar", replicated live
redis-cli -p 6381 SET foo baz    # -> (error) READONLY
```

If the primary requires a password, the replica authenticates with `--masterauth` (it defaults to the replica's own `--requirepass`).

> ⚠️ **A primary without AOF that restarts empty will wipe its replicas.**
> On reconnect a replica receives a full resync, which begins with `FLUSHDB`, so it converges to the primary's (empty) state. If replicas are your safety net, run the primary with `--aof-path` set, or stop the replicas before restarting an empty primary. A primary running without AOF logs a warning whenever a replica connects.

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

The compose stack publishes its ports on `127.0.0.1` only. To require a password, set it before starting (the same variable is read by `verify_cluster.py`):

```bash
export MINI_CACHE_PASSWORD='s3cret'
docker compose up --build
python verify_cluster.py
```

The image binds `0.0.0.0` inside the container so other containers can reach it. If you publish its port beyond loopback, set a password and put it behind a network you trust.

## CLI options

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | bind address (a warning is logged for non-loopback addresses) |
| `--port` | `6380` | bind port |
| `--aof-path` | `mini_cache.aof` | AOF file path; empty string disables persistence (ignored on replicas) |
| `--aof-fsync` | `everysec` | `always` (fsync every write, group-committed off the event loop), `everysec` (background, ~1s), `no` (leave it to the OS) |
| `--requirepass` | `$MINI_CACHE_PASSWORD` | require clients to `AUTH` with this password; prefer the env var, command-line arguments are visible in `ps` |
| `--masterauth` | `$MINI_CACHE_MASTERAUTH`, else the `--requirepass` value | password a replica uses to authenticate against its primary |
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

Integration tests spin up real `Server` instances on ephemeral ports and drive them over actual TCP sockets — not mocks — including primary/replica failover, crash recovery of a torn AOF, multi-shard key routing, authentication, and `ClusterClient` timeout/cancellation behaviour.

## Benchmark

```bash
poetry run python benchmarks/bench.py                      # in-process servers: no AOF, AOF everysec, AOF always
poetry run python benchmarks/bench.py --port 6379          # compare with a real Redis
poetry run python benchmarks/bench.py --clients 100 --requests 200000 --pipeline 16
```

Reports ops/s and p50/p95/p99 latency per round trip for GET, SET, SET with TTL and an 80/20 mix. The load generator is Python/asyncio and shares a CPU with the server, so treat the numbers as relative, not absolute. `--aof-fsync always` is still the dominant cost for writes, although concurrent writers now share fsyncs instead of blocking the event loop one at a time.

## Known limitations

- **RESP2 only** — no RESP3, no pub/sub, no transactions (`MULTI`/`EXEC`)
- **Small command set** — no `SELECT`, `SET KEEPTTL`/`EXAT`/`PXAT`; `KEYS` patterns use Python `fnmatch` syntax, which is close to but not identical to Redis globbing, and `KEYS` scans the whole keyspace, so it blocks the server on large datasets
- **Values are UTF-8 text** — binary-unsafe; invalid bytes are replaced on decode
- **No TLS** — `AUTH` passwords and data travel in clear text; do not expose it to untrusted networks (the Docker image binds `0.0.0.0`)
- **Single-threaded per process** — `asyncio`, not a real thread/process pool; fine for a learning project, not for production throughput
- **No automatic failover** — if a primary dies, a replica does *not* automatically promote itself; you restart a primary manually (or point a replica config at a new one)
- **Replication is simple** — a slow replica slows writes on the primary, replicas cannot be chained, replicated TTLs drift by the network delay, and replicas ignore `--aof-path`
- **An empty primary restart resets its replicas** — see the warning under [Replication](#replication)
- **Sharding has no rebalancing tool** — adding/removing a shard remaps keys on the hash ring, but nothing physically migrates the data that moved
- **AOF has no compaction** — it grows forever; real Redis periodically rewrites it compactly, this doesn't
- **No cluster gossip** — shard membership is static, passed to `ClusterClient` by the caller, not auto-discovered
- **No memory limit** — keys without a TTL live forever and nothing evicts them

## Design notes

- **Why RESP instead of a custom protocol?** Wire-compatibility with `redis-cli` and existing client libraries, for barely more implementation cost than inventing a new format.
- **Why AOF instead of RDB snapshots?** Simpler to implement correctly and reason about — it's literally the same command log used for replication snapshots, reused as the persistence format.
- **Why absolute TTL deadlines in the AOF?** Replaying `SET k v EX 10` after a restart would hand the key a fresh 10 seconds, and would bring back keys that expired while the server was down. The log therefore stores `PEXPIREAT` wall-clock deadlines. Replication keeps relative TTLs on purpose (`SET ... PX`), so it does not depend on synchronized clocks between machines.
- **Why log normalised commands?** A `SET ... NX`/`XX` that did nothing is not written to the AOF or sent to replicas, and the ones that did apply are logged as plain `SET`, so replay and replicas reproduce exactly what the primary did.
- **Why is `fsync` off the event loop?** A blocking `fsync` per write stalls every connected client. `always` now runs it in a worker thread and lets writers that arrive during a sync share the next one (group commit); the reply is still only sent once the write is on disk.
- **Why client-side sharding instead of a cluster protocol?** Keeps each node's code identical and simple; the routing complexity lives in one place (`ClusterClient`) instead of being smeared across every server instance via gossip.

## License

MIT — see [LICENSE](LICENSE).