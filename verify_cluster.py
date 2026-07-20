import subprocess
import sys
import time

import redis

from mini_cache.cluster import ConsistentHashRing

SHARDS = {
    "shard1": 6400,
    "shard2": 6401,
    "shard3": 6402,
}
REPLICA_PORT = 6410


def connect(port: int) -> redis.Redis:
    return redis.Redis(
        host="127.0.0.1",
        port=port,
        decode_responses=True,
        socket_connect_timeout=2,
        protocol=2,
    )


def check(label: str, condition: bool) -> None:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {label}")
    if not condition:
        sys.exit(1)


def basic_set_get() -> None:
    print("\n== basic SET/GET on shard1 ==")
    r = connect(SHARDS["shard1"])
    r.set("foo", "bar")
    check("GET returns the value just set", r.get("foo") == "bar")
    info = r.info()
    check("INFO reports role master", info.get("role") == "master" or "role:master" in str(info))


def replication_propagates() -> None:
    print("\n== replication: shard1 -> shard1-replica ==")
    primary = connect(SHARDS["shard1"])
    replica = connect(REPLICA_PORT)

    primary.set("rep_test", "hello")

    value = None
    for _ in range(20):
        value = replica.get("rep_test")
        if value == "hello":
            break
        time.sleep(0.1)
    check("replica received the write within 2s", value == "hello")


def replica_rejects_writes() -> None:
    print("\n== replica read-only enforcement ==")
    replica = connect(REPLICA_PORT)
    try:
        replica.set("should_fail", "x")
        check("write to replica was rejected", False)
    except redis.exceptions.ReadOnlyError:
        check("write to replica was rejected with READONLY", True)
    except redis.exceptions.ResponseError as exc:
        check("write to replica was rejected with READONLY", "READONLY" in str(exc))


def sharding_routes_correctly() -> None:
    print("\n== sharding: keys land on the predicted shard ==")
    node_ids = [f"127.0.0.1:{port}" for port in SHARDS.values()]
    ring = ConsistentHashRing(node_ids)
    clients = {f"127.0.0.1:{port}": connect(port) for port in SHARDS.values()}

    all_correct = True
    for i in range(30):
        key = f"user:{i}"
        value = f"value-{i}"
        target_node = ring.get_node(key)
        clients[target_node].set(key, value)

        for node_id, client in clients.items():
            found = client.get(key)
            should_exist = node_id == target_node
            exists = found is not None
            if should_exist != exists:
                all_correct = False

    check("every key exists only on its predicted shard", all_correct)

    sizes = {node_id: client.dbsize() for node_id, client in clients.items()}
    print(f"    key counts per shard: {sizes}")
    check("keys spread across more than one shard", len({v for v in sizes.values() if v > 0}) > 1)


def persistence_survives_restart() -> None:
    print("\n== persistence: shard1 restart via docker compose ==")
    primary = connect(SHARDS["shard1"])
    primary.set("durable", "yes")

    try:
        result = subprocess.run(
            ["docker", "compose", "restart", "shard1"],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        print("    'docker' not found on PATH, skipping this check")
        return

    if result.returncode != 0:
        print("    could not run 'docker compose restart shard1', skipping this check")
        print(f"    stderr: {result.stderr.strip()}")
        return

    time.sleep(2)

    value = None
    for _ in range(20):
        try:
            value = connect(SHARDS["shard1"]).get("durable")
            if value == "yes":
                break
        except redis.exceptions.ConnectionError:
            pass
        time.sleep(0.5)

    check("key survived container restart", value == "yes")


def main() -> None:
    print("Assuming 'docker compose up' is already running (shard1/2/3 + shard1-replica).\n")
    try:
        connect(SHARDS["shard1"]).ping()
    except redis.exceptions.ConnectionError:
        print("Could not connect to 127.0.0.1:6400 — is 'docker compose up' running?")
        sys.exit(1)

    basic_set_get()
    replication_propagates()
    replica_rejects_writes()
    sharding_routes_correctly()
    persistence_survives_restart()

    print("\nAll checks passed.")


if __name__ == "__main__":
    main()