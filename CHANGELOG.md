# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- `AUTH` / `--requirepass` (or `MINI_CACHE_PASSWORD`), `--masterauth` for replicas, and `password=` on `ClusterClient`.
- Warning at startup when binding a non-loopback address (no password / no TLS).
- Commands: `INCR`, `DECR`, `INCRBY`, `DECRBY`, `MGET`, `MSET`, `PEXPIRE`; `SET` options `NX`, `XX`, `PX`, `GET`.
- `ClusterClient(connect_timeout=, read_timeout=)`, `ClusterTimeoutError`, `ClusterClient.set(nx=, xx=)`, `ClusterClient.incr()`.
- Python 3.13 support (CI matrix and classifiers).

### Changed
- **Default `--aof-fsync` is now `everysec`** (was `always`). `always` now uses group commit and no longer blocks the event loop.
- `requires-python` is now `>=3.11` (was `>=3.11,<3.13`).
- Replication now forwards `SET ... EX` as `SET ... PX` (relative, still clock-independent).
- A `SET NX/XX` that does not take effect is no longer written to the AOF or sent to replicas.
- `EX`/`PX` values that are `nan`/`inf` are rejected.

### Fixed
- `ClusterClient` no longer returns a stale reply to the next request after a task is cancelled mid-request (the connection is dropped).
- A hung shard can no longer hold its lock forever.
- A replica connected to a primary that requires a password now fails loudly instead of silently never syncing.

### Security / Operations
- Documented: a primary running **without AOF** that restarts empty will reset its replicas on reconnect (full resync starts with `FLUSHDB`). The primary now logs a warning when a replica connects to it without AOF.
- `docker-compose.yml` publishes ports on `127.0.0.1` only.

## [0.1.0] - 2026-10-04

Initial release: RESP2 server, TTLs, AOF persistence, replication, client-side sharding.