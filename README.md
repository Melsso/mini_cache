# mini_cache

A small, from-scratch distributed cache server, wire-compatible with the RESP protocol used by Redis — real `redis-cli` and Redis client libraries can talk to it directly.

Built incrementally as a learning project: single-node cache → persistence → replication → failure detection → sharding.

## Features

- **RESP2 wire protocol** — works with `redis-cli` and standard Redis clients; keys and values are binary-safe bytes
- **Commands** — `GET`, `SET` (with `EX`, `PX`, `NX`, `XX`, `GET`), `MGET`, `MSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PERSIST`, `KEYS [pattern]`, `DBSIZE`, `FLUSHDB`, `PING`, `ECHO`, `INFO`, `AUTH`, `BGREWRITEAOF`, `QUIT`
- **Memory cap and eviction** — `--maxmemory` with `allkeys-lru` eviction (or `noeviction`, which rejects writes with `OOM`)
- **Authentication** — optional password (`--requirepass` / `AUTH`), enforced on clients and on replication links
- **Client limits** — maximum connections, idle timeout, and a per-client output limit; a client that stops reading is dropped
- **TTLs** — lazy expiry on access plus a heap-driven background sweep (cost proportional to the number of expired keys); `TTL` reports whole seconds, rounded up. `EX`/`EXPIRE` take integers (use `PX`/`PEXPIRE` for sub-second expiry)
- **AOF persistence** — every write logged to disk and replayed on restart; TTLs are stored as absolute deadlines, a torn tail from a crash is repaired automatically, `fsync` policy is configurable (group commit, off the event loop, for `always`), and the log is compacted by `BGREWRITEAOF` or automatically
- **Primary/replica replication** — live write propagation through a per-replica queue (a slow replica never slows the primary; one that falls too far behind is dropped and resyncs), full snapshot sync on connect
- **Failure-detecting replication link** — heartbeats detect a dead primary/replica within seconds, automatic reconnect with backoff (detection only: there is no automatic promotion)
- **Client-side sharding** — consistent-hashing router (`ClusterClient`) with a connection pool per shard, typed return values, `mget`/`mset`, pipelining, cross-shard `keys`/`delete_pattern`/`flush_all`, timeouts, and automatic reconnects
- **`INFO` stats** — connections, commands, hits/misses, expired and evicted keys, memory, AOF size
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
│   ├── protocol.py      # RESP encode/decode (bytes) — no networking, no command logic
│   ├── store.py         # in-memory key-value engine: TTL, LRU eviction, memory accounting
│   ├── commands.py      # command name -> Store operation dispatch table
│   ├── server.py        # asyncio TCP server: connections, limits, auth, replication, AOF, INFO
│   ├── aof.py           # append-only file: log writes, replay, crash repair, group commit, rewrite
│   ├── replication.py   # replica client: SYNC, snapshot + live stream, heartbeats
│   ├── cluster.py       # consistent hash ring + pooled multi-shard router client
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
    server = Server(host="127.0.0.1", port=0, aof_path="cache.aof", maxmemory=256 * 1024 * 1024)
    await server.start()

    async with ClusterClient([("127.0.0.1", server.bound_port)]) as client:
        await client.set("user:1", "alice", ttl=60)
        print(await client.get("user:1"))                 # b"alice"
        print(await client.set("lock", "me", nx=True))    # True, or False if already claimed
        print(await client.incr("hits"))                  # 1
        print(await client.mget(["user:1", "nope"]))      # [b"alice", None]

        pipe = client.pipeline()
        pipe.set("a", "1").incr("n").get("a")
        print(await pipe.execute())                       # ["OK", 1, b"1"]

        print(await client.delete_pattern("user:*"))      # across every shard

    await server.stop()

asyncio.run(main())
```

`ClusterClient` methods return typed values (`get` → `bytes | None`, `set` → `bool`, `incr`/`delete`/`ttl` → `int`, `mget` → `list[bytes | None]`). Keys and values may be `bytes` or `str`. It raises `ClusterError` when a shard answers with an error, and `ClusterTimeoutError` (a `ClusterError` and a `TimeoutError`) when a shard does not accept a connection or answer in time. Each shard gets a pool of up to `pool_size` connections; a request that times out or is cancelled drops its connection, so a late reply is never mistaken for the answer to the next request:

```python
ClusterClient(shards, connect_timeout=2.0, read_timeout=1.0, pool_size=8, password="s3cret")
```

`keys` and `delete_pattern` run `KEYS` on every shard, which is O(keyspace) and blocks each server while it runs. `mset` is atomic per shard, not across shards.

### Authentication

```bash
export MINI_CACHE_PASSWORD='s3cret'      # preferred: keeps it out of `ps`
poetry run mini_cache --host 0.0.0.0     # or: --requirepass s3cret

redis-cli -p 6380 -a s3cret PING
```

Without a password, every command is open to anyone who can reach the port. The server logs a warning when it binds a non-loopback address, with or without a password. There is **no TLS**, so the password and all data cross the network in clear text. Do not expose the port to untrusted networks.

### Memory limit

```bash
poetry run mini_cache --maxmemory 256mb                              # evict least-recently-used keys
poetry run mini_cache --maxmemory 256mb --maxmemory-policy noeviction  # reject SET/MSET/INCR* with OOM
```

`used_memory` is an estimate (key and value sizes plus a fixed per-entry overhead), not the process RSS, so leave headroom. Evictions are written to the AOF and sent to replicas as `DEL`. Replicas never evict on their own: give them the same capacity as the primary.

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

### Persistence and AOF rewrite

The AOF grows with every write. `BGREWRITEAOF` (or the automatic trigger: the file is at least `--aof-rewrite-min-size` and has grown by `--aof-rewrite-percentage` since the last rewrite) replaces it with the smallest log that rebuilds the current dataset. Writes that arrive during a rewrite are kept. Building the snapshot happens on the event loop, so a rewrite briefly pauses the server in proportion to the size of the dataset.

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
| `--aof-rewrite-min-size` | `64mb` | auto-rewrite the AOF once it is at least this big (`0` disables auto-rewrite) |
| `--aof-rewrite-percentage` | `100` | ...and has grown by this percentage since the last rewrite |
| `--maxmemory` | `0` | memory cap, e.g. `256mb` (`0` = unlimited); ignored on replicas |
| `--maxmemory-policy` | `allkeys-lru` | `allkeys-lru` evicts the least-recently-used keys, `noeviction` rejects writes with `OOM` |
| `--maxclients` | `10000` | maximum simultaneous connections (`0` = unlimited) |
| `--timeout` | `0` | close clients idle for this many seconds (`0` = never) |
| `--client-output-limit` | `64mb` | drop a client whose pending replies exceed this size (`0` = no limit) |
| `--requirepass` | `$MINI_CACHE_PASSWORD` | require clients to `AUTH` with this password; prefer the env var, command-line arguments are visible in `ps` |
| `--masterauth` | `$MINI_CACHE_MASTERAUTH`, else the `--requirepass` value | password a replica uses to authenticate against its primary |
| `--replica-of` | none | `host:port` of a primary to replicate from |
| `-v` / `--verbose` | off | debug logging |
| `--version` | | print the version |

Sizes accept a unit suffix (`64mb`, `2gb`). The server shuts down cleanly on `SIGINT`/`SIGTERM` (clients are disconnected and the AOF is flushed and closed), so `docker stop` does not have to wait for a forced kill.

## Testing

```bash
poetry run pytest
poetry run mypy .
poetry run ruff check .
```

Integration tests spin up real `Server` instances on ephemeral ports (`Server.bound_port`) and drive them over actual TCP sockets — not mocks — including primary/replica failover, crash recovery of a torn AOF, AOF rewrite, eviction, multi-shard routing, authentication, and `ClusterClient` timeout/cancellation behaviour.

## Benchmark

```bash
poetry run python benchmarks/bench.py                      # in-process servers: no AOF, AOF everysec, AOF always
poetry run python benchmarks/bench.py --port 6379          # compare with a real Redis
poetry run python benchmarks/bench.py --clients 100 --requests 200000 --pipeline 16
```

Reports ops/s and p50/p95/p99 latency per round trip for GET, SET, SET with TTL and an 80/20 mix. The load generator is Python/asyncio and shares a CPU with the server, so treat the numbers as relative, not absolute. `--aof-fsync always` is still the dominant cost for writes, although concurrent writers now share fsyncs instead of blocking the event loop one at a time.

## Known limitations

- **RESP2 only** — no RESP3, no pub/sub, no transactions (`MULTI`/`EXEC`)
- **Small command set** — no `SELECT`, `SET KEEPTTL`/`EXAT`/`PXAT`, no `SCAN`; `KEYS` patterns use Python `fnmatch` syntax, which is close to but not identical to Redis globbing, and `KEYS` scans the whole keyspace, so it blocks the server on large datasets
- **Memory accounting is an estimate** — `--maxmemory` caps key and value sizes plus a fixed per-entry overhead, not the process RSS; replicas ignore it
- **No TLS** — `AUTH` passwords and data travel in clear text; do not expose it to untrusted networks (the Docker image binds `0.0.0.0`)
- **Single-threaded per process** — `asyncio`, not a real thread/process pool; fine for a learning project, not for production throughput
- **No automatic failover** — if a primary dies, a replica does *not* automatically promote itself; you restart a primary manually (or point a replica config at a new one)
- **Replication is simple** — replicas cannot be chained, replicated TTLs drift by the network delay, replicas ignore `--aof-path`, and a replica more than 64 MiB behind is dropped and has to full-resync
- **An empty primary restart resets its replicas** — see the warning under [Replication](#replication)
- **AOF rewrite pauses the event loop briefly** — the snapshot is built in memory on the loop, so the pause and the extra memory both grow with the dataset
- **Sharding has no rebalancing tool** — adding/removing a shard remaps keys on the hash ring, but nothing physically migrates the data that moved
- **No cluster gossip** — shard membership is static, passed to `ClusterClient` by the caller, not auto-discovered

## Design notes

- **Why RESP instead of a custom protocol?** Wire-compatibility with `redis-cli` and existing client libraries, for barely more implementation cost than inventing a new format.
- **Why bytes?** Values are arbitrary binary in Redis; decoding them as UTF-8 silently corrupted anything that was not text. Keys and values now travel as bytes end to end, and the `str` API is a convenience that is encoded on the way in.
- **Why AOF instead of RDB snapshots?** Simpler to implement correctly and reason about — it's literally the same command log used for replication snapshots, reused as the persistence format. The rewrite is the same snapshot, written as a fresh log.
- **Why absolute TTL deadlines in the AOF?** Replaying `SET k v EX 10` after a restart would hand the key a fresh 10 seconds, and would bring back keys that expired while the server was down. The log therefore stores `PEXPIREAT` wall-clock deadlines. Replication keeps relative TTLs on purpose (`SET ... PX`), so it does not depend on synchronized clocks between machines.
- **Why log normalised commands?** A `SET ... NX`/`XX` that did nothing is not written to the AOF or sent to replicas, and the ones that did apply are logged as plain `SET`, so replay and replicas reproduce exactly what the primary did. Evictions are logged as `DEL` for the same reason.
- **Why is `fsync` off the event loop?** A blocking `fsync` per write stalls every connected client. `always` now runs it in a worker thread and lets writers that arrive during a sync share the next one (group commit); the reply is still only sent once the write is on disk.
- **Why a queue per replica?** Writing to replicas inside the write path made the slowest replica set the primary's latency. Each replica now has its own queue and writer task, so the primary never waits; a replica that falls too far behind is dropped rather than buffered without bound.
- **Why client-side sharding instead of a cluster protocol?** Keeps each node's code identical and simple; the routing complexity lives in one place (`ClusterClient`) instead of being smeared across every server instance via gossip.

## License

MIT — see [LICENSE](LICENSE).