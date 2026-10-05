from __future__ import annotations

import fnmatch
import math
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from mini_cache.protocol import Arg, Error, SimpleString, to_text
from mini_cache.store import Store

Handler = Callable[[Store, Sequence[Arg]], object]

_INT_RE = re.compile(r"-?[0-9]+")
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def dispatch(store: Store, command: Sequence[Arg]) -> object:
    if not command:
        return Error("ERR empty command")

    name = to_text(command[0]).upper()
    args = command[1:]

    handler = _COMMANDS.get(name)
    if handler is None:
        return Error(f"ERR unknown command '{to_text(command[0])}'")

    try:
        return handler(store, args)
    except _WrongArity:
        return Error(f"ERR wrong number of arguments for '{name}' command")
    except _BadArgument as exc:
        return Error(f"ERR {exc}")


class _WrongArity(Exception):
    pass


class _BadArgument(ValueError):
    pass


def _require(args: Sequence[Arg], count: int) -> None:
    if len(args) != count:
        raise _WrongArity


def _require_min(args: Sequence[Arg], minimum: int) -> None:
    if len(args) < minimum:
        raise _WrongArity


def _parse_int(arg: Arg) -> int:
    text = to_text(arg)
    if not _INT_RE.fullmatch(text):
        raise _BadArgument("value is not an integer or out of range")
    number = int(text)
    if not _INT64_MIN <= number <= _INT64_MAX:
        raise _BadArgument("value is not an integer or out of range")
    return number


def _like(sample: Arg, text: Arg) -> Arg:
    if isinstance(sample, (bytes, bytearray)):
        return text if isinstance(text, bytes) else text.encode("utf-8")
    return to_text(text)


def _lookup(store: Store, key: Arg) -> object:
    value = store.get(key)
    if value is None:
        store.misses += 1
    else:
        store.hits += 1
    return value


@dataclass
class SetOptions:
    key: Arg
    value: Arg
    nx: bool = False
    xx: bool = False
    get: bool = False
    ttl: float | None = None


def _parse_ttl(unit: str, arg: Arg) -> float:
    number = _parse_int(arg)
    if number <= 0:
        raise _BadArgument("invalid expire time in 'set' command")
    return float(number) if unit == "EX" else number / 1000


def parse_set(args: Sequence[Arg]) -> SetOptions:
    if len(args) < 2:
        raise _BadArgument("syntax error")
    opts = SetOptions(args[0], args[1])
    i = 2
    while i < len(args):
        option = to_text(args[i]).upper()
        if option == "NX":
            opts.nx = True
        elif option == "XX":
            opts.xx = True
        elif option == "GET":
            opts.get = True
        elif option in ("EX", "PX"):
            if opts.ttl is not None or i + 1 >= len(args):
                raise _BadArgument("syntax error")
            i += 1
            opts.ttl = _parse_ttl(option, args[i])
        else:
            raise _BadArgument("syntax error")
        i += 1
    if opts.nx and opts.xx:
        raise _BadArgument("syntax error")
    return opts


def _cmd_ping(store: Store, args: Sequence[Arg]) -> object:
    if len(args) == 0:
        return SimpleString("PONG")
    if len(args) == 1:
        return args[0]
    raise _WrongArity


def _cmd_echo(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 1)
    return args[0]


def _cmd_set(store: Store, args: Sequence[Arg]) -> object:
    _require_min(args, 2)
    opts = parse_set(args)
    exists = store.exists(opts.key)
    previous = store.get(opts.key) if opts.get else None
    if (opts.nx and exists) or (opts.xx and not exists):
        return previous
    store.set(opts.key, opts.value, ttl=opts.ttl)
    return previous if opts.get else SimpleString("OK")


def _cmd_get(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 1)
    return _lookup(store, args[0])


def _cmd_mset(store: Store, args: Sequence[Arg]) -> object:
    _require_min(args, 2)
    if len(args) % 2:
        raise _WrongArity
    for i in range(0, len(args), 2):
        store.set(args[i], args[i + 1])
    return SimpleString("OK")


def _cmd_mget(store: Store, args: Sequence[Arg]) -> object:
    _require_min(args, 1)
    return [_lookup(store, key) for key in args]


def _incr(store: Store, key: Arg, delta: int) -> object:
    try:
        return store.incr(key, delta)
    except ValueError as exc:
        raise _BadArgument(str(exc)) from exc


def _cmd_incr(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 1)
    return _incr(store, args[0], 1)


def _cmd_decr(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 1)
    return _incr(store, args[0], -1)


def _cmd_incrby(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 2)
    return _incr(store, args[0], _parse_int(args[1]))


def _cmd_decrby(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 2)
    return _incr(store, args[0], -_parse_int(args[1]))


def _cmd_del(store: Store, args: Sequence[Arg]) -> object:
    _require_min(args, 1)
    return sum(1 for key in args if store.delete(key))


def _cmd_exists(store: Store, args: Sequence[Arg]) -> object:
    _require_min(args, 1)
    return sum(1 for key in args if _lookup(store, key) is not None)


def _cmd_expire(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 2)
    return store.expire(args[0], _parse_int(args[1]))


def _cmd_pexpire(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 2)
    return store.expire(args[0], _parse_int(args[1]) / 1000)


def _cmd_pexpireat(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 2)
    try:
        unix_ms = int(to_text(args[1]))
    except ValueError as exc:
        raise _BadArgument("value is not an integer or out of range") from exc
    return store.expire_at_unix(args[0], unix_ms / 1000)


def _cmd_ttl(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 1)
    result = store.ttl(args[0])
    if result is None:
        return -2
    if result == -1.0:
        return -1
    return math.ceil(result)


def _cmd_persist(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 1)
    return store.persist(args[0])


def _cmd_keys(store: Store, args: Sequence[Arg]) -> object:
    if len(args) > 1:
        raise _WrongArity
    keys = store.keys()
    if not args:
        return keys
    pattern = args[0]
    return [key for key in keys if fnmatch.fnmatchcase(key, _like(key, pattern))]


def _cmd_flushdb(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 0)
    store.flush()
    return SimpleString("OK")


def _cmd_dbsize(store: Store, args: Sequence[Arg]) -> object:
    _require(args, 0)
    return len(store)


_COMMANDS: dict[str, Handler] = {
    "PING": _cmd_ping,
    "ECHO": _cmd_echo,
    "SET": _cmd_set,
    "GET": _cmd_get,
    "MSET": _cmd_mset,
    "MGET": _cmd_mget,
    "INCR": _cmd_incr,
    "DECR": _cmd_decr,
    "INCRBY": _cmd_incrby,
    "DECRBY": _cmd_decrby,
    "DEL": _cmd_del,
    "EXISTS": _cmd_exists,
    "EXPIRE": _cmd_expire,
    "PEXPIRE": _cmd_pexpire,
    "PEXPIREAT": _cmd_pexpireat,
    "TTL": _cmd_ttl,
    "PERSIST": _cmd_persist,
    "KEYS": _cmd_keys,
    "FLUSHDB": _cmd_flushdb,
    "DBSIZE": _cmd_dbsize,
}
