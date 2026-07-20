# mini_cache

A small, from-scratch distributed cache server, wire-compatible with the RESP protocol used by Redis — real `redis-cli` and Redis client libraries can talk to it directly.

Built incrementally as a learning project: single-node cache → persistence → replication → failover → sharding.

## Features

- **RESP2 wire protocol** — works with `redis-cli` and standard Redis clients
- **Core commands** — `GET`, `SET` (with `EX` TTL), `DEL`, `EXISTS`, `EXPIRE`, `TTL`, `PERSIST`, `KEYS`, `DBSIZE`, `FLUSHDB`, `PING`, `ECHO`, `INFO`
- **AOF persistence** — every write logged to disk, replayed on restart
- **Primary/replica replication** — live write propagation, full snapshot sync on connect
- **Failover-aware replication link** — heartbeats detect a dead primary/replica within seconds, automatic reconnect with backoff
- **Client-side sharding** — consistent-hashing router (`ClusterClient`) spreads keys across multiple independent primaries
- **Read-only replicas** — writes against a replica are rejected with `READONLY`

## Architecture

```
minicache/
├── src/mini_cache/
│   ├── protocol.py      # RESP encode/decode — no networking, no command logic
│   ├── store.py         # in-memory key-value engine with TTL (lazy + active expiry)
│   ├── commands.py      # command name -> Store operation dispatch table
│   ├── server.py         # asyncio TCP server: connections, replication, AOF, INFO
│   ├── aof.py            # append-only file: log writes, replay on startup
│   ├── replication.py    # replica client: SYNC, snapshot + live stream, heartbeats
│   ├── cluster.py        # consistent hash ring + multi-shard router client
│   └── __main__.py       # CLI entry point
└── tests/                 # unit + integration tests (real sockets, real processes)
```

Each module has exactly one job and no knowledge of the others' internals — `protocol.py` never touches a socket, `store.py` never touches RESP, `server.py` is the only place that wires networking to storage.

## Quickstart

```bash
poetry install
poetry run mini_cache                 # single node, 127.0.0.1:6380

# in another terminal
redis-cli -p 6380 SET foo bar
redis-cli -p 6380 GET foo
redis-cli -p 6380 INFO
```

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
from mini_cache.cluster import ClusterClient
import asyncio

async def main():
    client = ClusterClient([("127.0.0.1", 6400), ("127.0.0.1", 6401), ("127.0.0.1", 6402)])
    await client.set("user:1", "alice")
    print(await client.get("user:1"))
    await client.close()

asyncio.run(main())
```

### Docker

```bash
docker compose up --build
```

## CLI options

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | bind address |
| `--port` | `6380` | bind port |
| `--aof-path` | `mini_cache.aof` | AOF file path; empty string disables persistence |
| `--replica-of` | none | `host:port` of a primary to replicate from |
| `-v` / `--verbose` | off | debug logging |

## Testing

```bash
poetry run pytest
python -m mypy .
```

Integration tests spin up real `Server` instances on ephemeral ports and drive them over actual TCP sockets — not mocks — including multi-process-style scenarios like primary/replica failover and multi-shard key routing.

## Known limitations

- **RESP2 only** — no RESP3, no pub/sub, no transactions (`MULTI`/`EXEC`)
- **Single-threaded per process** — `asyncio`, not a real thread/process pool; fine for a learning project, not for production throughput
- **No automatic failover** — if a primary dies, a replica does *not* automatically promote itself; you restart a primary manually (or point a replica config at a new one)
- **Sharding has no rebalancing tool** — adding/removing a shard remaps keys on the hash ring, but nothing physically migrates the data that moved
- **AOF has no compaction** — it grows forever; real Redis periodically rewrites it compactly, this doesn't
- **No cluster gossip** — shard membership is static, passed to `ClusterClient` by the caller, not auto-discovered

## Design notes

- **Why RESP instead of a custom protocol?** Wire-compatibility with `redis-cli` and existing client libraries, for barely more implementation cost than inventing a new format.
- **Why AOF instead of RDB snapshots?** Simpler to implement correctly and reason about — it's literally the same command log used for replication snapshots, reused as the persistence format.
- **Why client-side sharding instead of a cluster protocol?** Keeps each node's code identical and simple; the routing complexity lives in one place (`ClusterClient`) instead of being smeared across every server instance via gossip.