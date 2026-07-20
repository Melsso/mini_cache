from __future__ import annotations

from typing import Callable

from mini_cache.protocol import Error, SimpleString
from mini_cache.store import Store

Handler = Callable[[Store, list[str]], object]


def dispatch(store: Store, command: list[str]) -> object:
    if not command:
        return Error("ERR empty command")

    name = command[0].upper()
    args = command[1:]

    handler = _COMMANDS.get(name)
    if handler is None:
        return Error(f"ERR unknown command '{command[0]}'")

    try:
        return handler(store, args)
    except _WrongArity:
        return Error(f"ERR wrong number of arguments for '{name}' command")
    except _BadArgument as exc:
        return Error(f"ERR {exc}")


class _WrongArity(Exception):
    pass


class _BadArgument(Exception):
    pass


def _require(args: list[str], count: int) -> None:
    if len(args) != count:
        raise _WrongArity


def _require_min(args: list[str], minimum: int) -> None:
    if len(args) < minimum:
        raise _WrongArity


def _cmd_ping(store: Store, args: list[str]) -> object:
    if len(args) == 0:
        return SimpleString("PONG")
    if len(args) == 1:
        return args[0]
    raise _WrongArity


def _cmd_echo(store: Store, args: list[str]) -> object:
    _require(args, 1)
    return args[0]


def _cmd_set(store: Store, args: list[str]) -> object:
    _require_min(args, 2)
    key, value = args[0], args[1]
    ttl: float | None = None

    rest = args[2:]
    if rest:
        if len(rest) != 2 or rest[0].upper() != "EX":
            raise _BadArgument("syntax error")
        try:
            ttl = float(rest[1])
        except ValueError as exc:
            raise _BadArgument("value is not an integer or out of range") from exc
        if ttl <= 0:
            raise _BadArgument("invalid expire time")

    store.set(key, value, ttl=ttl)
    return SimpleString("OK")


def _cmd_get(store: Store, args: list[str]) -> object:
    _require(args, 1)
    return store.get(args[0])


def _cmd_del(store: Store, args: list[str]) -> object:
    _require_min(args, 1)
    return sum(1 for key in args if store.delete(key))


def _cmd_exists(store: Store, args: list[str]) -> object:
    _require_min(args, 1)
    return sum(1 for key in args if store.exists(key))


def _cmd_expire(store: Store, args: list[str]) -> object:
    _require(args, 2)
    try:
        seconds = float(args[1])
    except ValueError as exc:
        raise _BadArgument("value is not an integer or out of range") from exc
    return store.expire(args[0], seconds)


def _cmd_ttl(store: Store, args: list[str]) -> object:
    _require(args, 1)
    result = store.ttl(args[0])
    if result is None:
        return -2
    return int(result) if result != -1.0 else -1


def _cmd_persist(store: Store, args: list[str]) -> object:
    _require(args, 1)
    return store.persist(args[0])


def _cmd_keys(store: Store, args: list[str]) -> object:
    _require(args, 0)
    return store.keys()


def _cmd_flushdb(store: Store, args: list[str]) -> object:
    _require(args, 0)
    store.flush()
    return SimpleString("OK")


def _cmd_dbsize(store: Store, args: list[str]) -> object:
    _require(args, 0)
    return len(store)


_COMMANDS: dict[str, Handler] = {
    "PING": _cmd_ping,
    "ECHO": _cmd_echo,
    "SET": _cmd_set,
    "GET": _cmd_get,
    "DEL": _cmd_del,
    "EXISTS": _cmd_exists,
    "EXPIRE": _cmd_expire,
    "TTL": _cmd_ttl,
    "PERSIST": _cmd_persist,
    "KEYS": _cmd_keys,
    "FLUSHDB": _cmd_flushdb,
    "DBSIZE": _cmd_dbsize,
}
