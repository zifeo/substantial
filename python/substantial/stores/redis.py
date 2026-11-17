"""Redis store URLs; the adapter is in `redis_store`, which needs the `redis` extra."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal
from urllib.parse import parse_qsl

from substantial.errors import ConfigError

Topology = Literal["standalone", "sentinel", "cluster"]
CONNECT_TIMEOUT = 5.0
ATTEMPT_TIMEOUT = 60.0
# How long a create waits for its AOF fsync on the primary and its replicas.
WAITAOF_MS = 5000
# How often `replicas=all` counts the replicas online again, so that one joining is waited for.
RECOUNT_EVERY = 10.0
MAX_WAIT_SECONDS = 20
SCHEMES: dict[str, tuple[Topology, bool]] = {
    "redis": ("standalone", False),
    "rediss": ("standalone", True),
    "redis+sentinel": ("sentinel", False),
    "rediss+sentinel": ("sentinel", True),
    "redis+cluster": ("cluster", False),
    "rediss+cluster": ("cluster", True),
}
# The namespace becomes the hash tag `{<ns>}` of every key, so it holds no brace.
_NAMESPACE = re.compile(r"[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*")
# ASCII only: `str.isdecimal` and `int` take every script's digits.
_DIGITS = re.compile(r"[0-9]+")
_PARAMETERS: dict[Topology, set[str]] = {
    "standalone": {"db", "replicas"},
    "sentinel": {"master", "db", "replicas"},
    "cluster": {"replicas"},
}


@dataclass(frozen=True)
class RedisLocation:
    """The parts of a Redis URL, every one required (I-16): the server, the sentinels or the
    cluster nodes to start from, the namespace, and `replicas`, the replicas that fsync a create
    before it is acknowledged, `None` for every replica connected to the primary."""

    topology: Topology
    tls: bool
    nodes: tuple[tuple[str, int], ...]
    namespace: str
    db: int
    replicas: int | None
    master: str | None

    @staticmethod
    def parse(url: str) -> RedisLocation:
        def invalid(why: str) -> ConfigError:
            return ConfigError(f"{url!r}: {why}")

        scheme, sep, rest = url.partition("://")
        if not sep or scheme not in SCHEMES:
            raise invalid(f"not a Redis URL; the schemes are {', '.join(SCHEMES)}")
        topology, tls = SCHEMES[scheme]
        if "#" in rest:
            raise invalid("a store URL takes no fragment")
        rest, _, query_text = rest.partition("?")
        netloc, _, namespace = rest.partition("/")
        if "@" in netloc:
            raise invalid("credentials go in `credentials`, not in the URL")
        nodes = tuple(_node(part, invalid) for part in netloc.split(","))
        if topology == "standalone" and len(nodes) != 1:
            raise invalid("names one server, host:port")
        if not _NAMESPACE.fullmatch(namespace):
            raise invalid("names no namespace, or one outside [A-Za-z0-9._-] and /")
        query: dict[str, str] = {}
        for name, value in parse_qsl(query_text, keep_blank_values=True):
            if name in query:
                raise invalid(f"{name} is given twice")
            if name not in _PARAMETERS[topology]:
                raise invalid(f"unknown query parameter {name}")
            query[name] = value
        for name in sorted(_PARAMETERS[topology] - set(query)):
            raise invalid(f"the query parameter {name} is required")
        replicas = _replicas(query["replicas"], topology, invalid)
        db = 0
        if "db" in query:
            if not _DIGITS.fullmatch(query["db"]):
                raise invalid(f"db is a database number, not {query['db']!r}")
            db = int(query["db"])
        master = query.get("master")
        if master == "":
            raise invalid("master names the primary the sentinels monitor")
        return RedisLocation(topology, tls, nodes, namespace, db, replicas, master)


def _node(part: str, invalid) -> tuple[str, int]:
    host, sep, port = part.rpartition(":")
    if not sep or not host or not _DIGITS.fullmatch(port) or not 0 < int(port) < 65536:
        raise invalid(f"{part!r} is not host:port")
    if host.startswith("["):
        if not host.endswith("]"):
            raise invalid(f"{part!r} is not host:port")
        host = host[1:-1]
    return host, int(port)


def _replicas(value: str, topology: Topology, invalid) -> int | None:
    if value == "all":
        return None
    # Sentinel and Cluster promote a replica on failover: a create one of them misses is lost.
    if topology != "standalone":
        raise invalid("replicas is all on Sentinel and Cluster, so a failover keeps every create")
    if not _DIGITS.fullmatch(value):
        raise invalid(f"replicas is a number or all, not {value!r}")
    return int(value)
