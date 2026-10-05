import time

import pytest

from mini_cache.aof import AOFLog, is_write_command, to_aof_commands
from mini_cache.store import Store


def test_pexpireat_is_a_write_command():
    assert is_write_command("PEXPIREAT") is True


def test_set_with_ex_becomes_set_plus_absolute_deadline():
    assert to_aof_commands(["SET", "k", "v", "EX", "10"], now=1000.0) == [
        ["SET", "k", "v"],
        ["PEXPIREAT", "k", "1010000"],
    ]


def test_fractional_ttl_is_converted():
    assert to_aof_commands(["set", "k", "v", "ex", "1.5"], now=1000.0)[1] == [
        "PEXPIREAT",
        "k",
        "1001500",
    ]


def test_expire_becomes_absolute_deadline():
    assert to_aof_commands(["EXPIRE", "k", "30"], now=1000.0) == [
        ["PEXPIREAT", "k", "1030000"]
    ]


def test_other_commands_are_logged_unchanged():
    assert to_aof_commands(["SET", "k", "v"]) == [["SET", "k", "v"]]
    assert to_aof_commands(["DEL", "k"]) == [["DEL", "k"]]
    assert to_aof_commands(["FLUSHDB"]) == [["FLUSHDB"]]


def test_invalid_fsync_policy_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        AOFLog(tmp_path / "x.aof", fsync="sometimes")


@pytest.mark.parametrize("policy", ["always", "everysec", "no"])
async def test_every_fsync_policy_round_trips(tmp_path, policy):
    path = tmp_path / f"{policy}.aof"
    log = AOFLog(path, fsync=policy)
    log.append(["SET", "a", "1"])
    log.sync()
    log.close()
    log.close()

    replay_log = AOFLog(path)
    store = Store()
    assert await replay_log.replay(store) == 1
    replay_log.close()
    assert store.get("a") == "1"


async def test_replay_cuts_off_a_command_torn_in_the_middle(tmp_path):
    path = tmp_path / "torn.aof"
    log = AOFLog(path)
    log.append(["SET", "a", "1"])
    log.append(["SET", "b", "2"])
    log.close()
    good_size = path.stat().st_size
    with open(path, "ab") as f:
        f.write(b"*3\r\n$3\r\nSET\r\n$3\r\nfo")

    log = AOFLog(path)
    store = Store()
    assert await log.replay(store) == 2
    assert store.get("a") == "1" and store.get("b") == "2"
    assert path.stat().st_size == good_size
    assert (
        tmp_path / "torn.aof.corrupt"
    ).read_bytes() == b"*3\r\n$3\r\nSET\r\n$3\r\nfo"

    log.append(["SET", "c", "3"])
    log.close()
    replay_log = AOFLog(path)
    store2 = Store()
    assert await replay_log.replay(store2) == 3
    replay_log.close()
    assert store2.get("c") == "3"


async def test_replay_cuts_off_a_torn_line(tmp_path):
    path = tmp_path / "torn-line.aof"
    log = AOFLog(path)
    log.append(["SET", "a", "1"])
    log.close()
    with open(path, "ab") as f:
        f.write(b"*3")

    log = AOFLog(path)
    store = Store()
    assert await log.replay(store) == 1
    log.close()
    assert store.get("a") == "1"


async def test_replay_honours_absolute_deadlines(tmp_path):
    path = tmp_path / "deadlines.aof"
    log = AOFLog(path)
    now_ms = int(time.time() * 1000)
    log.append(["SET", "gone", "x"])
    log.append(["PEXPIREAT", "gone", str(now_ms - 5_000)])
    log.append(["SET", "kept", "y"])
    log.append(["PEXPIREAT", "kept", str(now_ms + 100_000)])
    log.close()

    replay_log = AOFLog(path)
    store = Store()
    await replay_log.replay(store)
    replay_log.close()

    assert store.get("gone") is None
    assert store.get("kept") == "y"
    remaining = store.ttl("kept")
    assert remaining is not None and 98 <= remaining <= 100
