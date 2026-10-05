from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from mini_cache.cluster import ClusterClient, ClusterError, ConsistentHashRing
from mini_cache.server import Server
from mini_cache.store import Store

try:
    __version__ = version("mini-cache")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

__all__ = [
    "ClusterClient",
    "ClusterError",
    "ConsistentHashRing",
    "Server",
    "Store",
    "__version__",
]
