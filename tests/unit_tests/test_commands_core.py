import pytest
import time

from mini_cache.commands import dispatch
from mini_cache.protocol import Error, SimpleString
from mini_cache.store import Store


@pytest.fixture
def store():
    return Store()


def test_set_nx_only_sets_when_missing(store):
    assert dispatch(store, ["SET", "k", "1", "NX"]) == SimpleString("OK")
    assert dispatch(store, ["SET", "k", "2", "NX"]) is None
    assert dispatch(store, ["GET", "k"]) == "1"


def test_set_xx_only_sets_when_present(store):
    assert dispatch(store, ["SET", "k", "1", "XX"]) is None
    dispatch(store, ["SET", "k", "1"])
    assert dispatch(store, ["SET", "k", "2", "XX"]) == SimpleString("OK")
    assert dispatch(store, ["GET", "k"]) == "2"


def test_set_get_returns_previous_value(store):
    assert dispatch(store, ["SET", "k", "1", "GET"]) is None
    assert dispatch(store, ["SET", "k", "2", "GET"]) == "1"
    assert dispatch(store, ["GET", "k"]) == "2"


def test_set_px_sets_millisecond_ttl(store):
    dispatch(store, ["SET", "k", "v", "PX", "5000"])
    assert 4 <= dispatch(store, ["TTL", "k"]) <= 5


@pytest.mark.parametrize(
    "args",
    [
        ["SET", "k", "v", "NX", "XX"],
        ["SET", "k", "v", "EX", "1", "PX", "1"],
        ["SET", "k", "v", "PX", "1.5"],
        ["SET", "k", "v", "EX", "nan"],
        ["SET", "k", "v", "EX"],
        ["SET", "k", "v", "BOGUS"],
    ],
)
def test_set_rejects_bad_options(store, args):
    assert isinstance(dispatch(store, args), Error)


def test_incr_family(store):
    assert dispatch(store, ["INCR", "n"]) == 1
    assert dispatch(store, ["INCRBY", "n", "9"]) == 10
    assert dispatch(store, ["DECR", "n"]) == 9
    assert dispatch(store, ["DECRBY", "n", "20"]) == -11
    assert dispatch(store, ["GET", "n"]) == "-11"


def test_incr_keeps_ttl(store):
    dispatch(store, ["SET", "n", "1", "EX", "100"])
    dispatch(store, ["INCR", "n"])
    assert 98 <= dispatch(store, ["TTL", "n"]) <= 100


@pytest.mark.parametrize("value", ["abc", "1.5", "1_0", " 1", "+1"])
def test_incr_rejects_non_integers(store, value):
    dispatch(store, ["SET", "n", value])
    assert isinstance(dispatch(store, ["INCR", "n"]), Error)
    assert dispatch(store, ["GET", "n"]) == value


def test_incr_overflow_is_an_error(store):
    dispatch(store, ["SET", "n", str(2**63 - 1)])
    assert isinstance(dispatch(store, ["INCR", "n"]), Error)


def test_mset_and_mget(store):
    assert dispatch(store, ["MSET", "a", "1", "b", "2"]) == SimpleString("OK")
    assert dispatch(store, ["MGET", "a", "nope", "b"]) == ["1", None, "2"]
    assert isinstance(dispatch(store, ["MSET", "a", "1", "b"]), Error)


def test_pexpire(store):
    dispatch(store, ["SET", "k", "v"])
    assert dispatch(store, ["PEXPIRE", "k", "10000"]) is True
    assert 8 <= dispatch(store, ["TTL", "k"]) <= 10
    assert dispatch(store, ["PEXPIRE", "nope", "10"]) is False


@pytest.mark.parametrize(
    "args",
    [
        ["SET", "k", "v", "EX", "1.5"],
        ["SET", "k", "v", "EX", "1e3"],
        ["SET", "k", "v", "EX", "0"],
        ["EXPIRE", "k", "1.5"],
        ["EXPIRE", "k", "soon"],
        ["PEXPIRE", "k", "1.5"],
    ],
)
def test_expiry_arguments_must_be_integers(store, args):
    dispatch(store, ["SET", "k", "v"])
    assert isinstance(dispatch(store, args), Error)


def test_ttl_rounds_up_so_a_live_key_never_reports_zero(store, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    dispatch(store, ["SET", "k", "v", "PX", "500"])
    assert dispatch(store, ["TTL", "k"]) == 1
    now[0] += 0.4  # 0.1s left
    assert dispatch(store, ["TTL", "k"]) == 1
    now[0] += 0.2  # expired
    assert dispatch(store, ["TTL", "k"]) == -2
