# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.2.0] - 2026-10-04

### Added
- `--maxmemory` / `--maxmemory-policy` (`allkeys-lru`, `noeviction`); evictions are logged to the AOF and replicated as `DEL`.
- `--maxclients`, `--timeout` (idle) and `--client-output-limit`; clients that stop reading are dropped.
- `BGREWRITEAOF`, automatic AOF rewrite (`--aof-rewrite-min-size`, `--aof-rewrite-percentage`).
- `INFO` memory, stats and AOF sections (hits, misses, expired/evicted keys, commands, connections).
- `Server.bound_port`.
- `ClusterClient`: connection pool (`pool_size`), `mget`, `mset`, `pipeline()`, `keys`, `delete_pattern`, `flush_all`, `dbsize`.
- `AUTH` / `--requirepass` (or `MINI_CACHE_PASSWORD`), `--masterauth` for replicas, and `password=` on `ClusterClient`.
- Warning at startup when binding a non-loopback address (no password / no TLS).
- Commands: `INCR`, `DECR`, `INCRBY`, `DECRBY`, `MGET`, `MSET`, `PEXPIRE`; `SET` options `NX`, `XX`, `PX`, `GET`.
- `ClusterClient(connect_timeout=, read_timeout=)`, `ClusterTimeoutError`, `ClusterClient.set(nx=, xx=)`, `ClusterClient.incr()`.
- Python 3.13 support (CI matrix and classifiers).
- Release workflow: pushing a `v*` tag builds the package and attaches the wheel and sdist to a GitHub release.

### Changed
- **Breaking:** keys and values are bytes end to end (previously UTF-8 text with invalid bytes replaced, which silently corrupted binary values). `ClusterClient` returns typed values: `get` -> `bytes | None`, `set` -> `bool`, `incr`/`delete`/`ttl` -> `int`.
- `ClusterClient` replaces its single connection per shard with a pool.
- Replication writes go through a per-replica queue: a slow replica no longer slows the primary, and one more than 64 MiB behind is dropped and resyncs.
- Replication stream order is now identical for every replica.
- README: "Failover-aware" reworded to "failure-detecting" (there is detection and reconnect, no promotion).
- **Default `--aof-fsync` is now `everysec`** (was `always`). `always` now uses group commit and no longer blocks the event loop.
- `requires-python` is now `>=3.11` (was `>=3.11,<3.13`).
- Replication now forwards `SET ... EX` as `SET ... PX` (relative, still clock-independent).
- A `SET NX/XX` that does not take effect is no longer written to the AOF or sent to replicas.
- `EX`/`PX` values that are `nan`/`inf` are rejected.
- **`TTL` rounds up** to whole seconds, so a key that still exists never reports `0`.
- **`SET ... EX` and `EXPIRE` now require integers**, like Redis (previously floats were accepted). Use `PX` / `PEXPIRE` for sub-second expiry. `ClusterClient.set(ttl=)` and `expire()` still accept floats and send `PX`/`PEXPIRE` for fractional values; `set` raises `ValueError` for a ttl that is zero, negative or non-finite.
- License metadata uses the SPDX expression `MIT` (`license-files = ["LICENSE"]`); build requires poetry-core >= 2.1.
- Version 0.2.0.

### Fixed
- `ClusterClient` no longer returns a stale reply to the next request after a task is cancelled mid-request (the connection is dropped).
- A hung shard can no longer hold its lock forever.
- A replica connected to a primary that requires a password now fails loudly instead of silently never syncing.

### Security / Operations
- Documented: a primary running **without AOF** that restarts empty will reset its replicas on reconnect (full resync starts with `FLUSHDB`). The primary now logs a warning when a replica connects to it without AOF.
- `docker-compose.yml` publishes ports on `127.0.0.1` only.

### Documented
- `KEYS` is O(keyspace) and blocks the server while it runs.

## [0.1.0] - 2026-10-04

Initial release: RESP2 server, TTLs, AOF persistence, replication, client-side sharding.