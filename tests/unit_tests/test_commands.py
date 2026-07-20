import pytest

from mini_cache.commands import dispatch
from mini_cache.protocol import Error, SimpleString
from mini_cache.store import Store


@pytest.fixture
def store():
    return Store()


def test_ping_no_args(store):
    assert dispatch(store, ["PING"]) == SimpleString("PONG")


def test_ping_with_message_echoes_it(store):
    assert dispatch(store, ["PING", "hello"]) == "hello"


def test_ping_lowercase_command_name(store):
    assert dispatch(store, ["ping"]) == SimpleString("PONG")


def test_echo(store):
    assert dispatch(store, ["ECHO", "hi"]) == "hi"


def test_echo_wrong_arity(store):
    result = dispatch(store, ["ECHO"])
    assert isinstance(result, Error)


def test_set_then_get(store):
    assert dispatch(store, ["SET", "foo", "bar"]) == SimpleString("OK")
    assert dispatch(store, ["GET", "foo"]) == "bar"


def test_get_missing_key_returns_none(store):
    assert dispatch(store, ["GET", "nope"]) is None


def test_set_wrong_arity(store):
    result = dispatch(store, ["SET", "foo"])
    assert isinstance(result, Error)


def test_set_with_ex_ttl(store):
    assert dispatch(store, ["SET", "foo", "bar", "EX", "10"]) == SimpleString("OK")
    ttl = dispatch(store, ["TTL", "foo"])
    assert 0 <= ttl <= 10


def test_set_with_bad_ex_syntax_returns_error(store):
    result = dispatch(store, ["SET", "foo", "bar", "WRONG", "10"])
    assert isinstance(result, Error)


def test_set_with_non_numeric_ttl_returns_error(store):
    result = dispatch(store, ["SET", "foo", "bar", "EX", "notanumber"])
    assert isinstance(result, Error)


def test_set_with_negative_ttl_returns_error(store):
    result = dispatch(store, ["SET", "foo", "bar", "EX", "-5"])
    assert isinstance(result, Error)


def test_del_existing_key(store):
    dispatch(store, ["SET", "foo", "bar"])
    assert dispatch(store, ["DEL", "foo"]) == 1
    assert dispatch(store, ["GET", "foo"]) is None


def test_del_multiple_keys_counts_only_existing(store):
    dispatch(store, ["SET", "a", "1"])
    dispatch(store, ["SET", "b", "2"])
    assert dispatch(store, ["DEL", "a", "b", "c"]) == 2


def test_exists_counts_existing_keys(store):
    dispatch(store, ["SET", "a", "1"])
    assert dispatch(store, ["EXISTS", "a", "b"]) == 1


def test_expire_on_existing_key(store):
    dispatch(store, ["SET", "foo", "bar"])
    assert dispatch(store, ["EXPIRE", "foo", "100"]) is True


def test_expire_on_missing_key(store):
    assert dispatch(store, ["EXPIRE", "nope", "100"]) is False


def test_ttl_missing_key_returns_minus_two(store):
    assert dispatch(store, ["TTL", "nope"]) == -2


def test_ttl_key_without_expiry_returns_minus_one(store):
    dispatch(store, ["SET", "foo", "bar"])
    assert dispatch(store, ["TTL", "foo"]) == -1


def test_persist_removes_ttl(store):
    dispatch(store, ["SET", "foo", "bar", "EX", "100"])
    assert dispatch(store, ["PERSIST", "foo"]) is True
    assert dispatch(store, ["TTL", "foo"]) == -1


def test_keys_lists_all_keys(store):
    dispatch(store, ["SET", "a", "1"])
    dispatch(store, ["SET", "b", "2"])
    assert set(dispatch(store, ["KEYS"])) == {"a", "b"}


def test_dbsize(store):
    dispatch(store, ["SET", "a", "1"])
    dispatch(store, ["SET", "b", "2"])
    assert dispatch(store, ["DBSIZE"]) == 2


def test_flushdb_clears_everything(store):
    dispatch(store, ["SET", "a", "1"])
    assert dispatch(store, ["FLUSHDB"]) == SimpleString("OK")
    assert dispatch(store, ["DBSIZE"]) == 0


def test_unknown_command_returns_error(store):
    result = dispatch(store, ["FOOBAR"])
    assert isinstance(result, Error)


def test_empty_command_returns_error(store):
    result = dispatch(store, [])
    assert isinstance(result, Error)
