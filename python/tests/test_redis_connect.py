"""The checks of a Redis connection at open, without a server: a primary demoted mid-check."""

from __future__ import annotations

from typing import Any

import pytest
from redis.exceptions import ResponseError

from substantial.errors import ConfigError
from substantial.stores import redis_connect
from substantial.stores.redis import RedisLocation
from substantial.stores.redis_connect import CAS, CREATE, DELETE, Connector
from substantial.stores.retry import Transient

PRIMARY = {
    "aof_enabled": 1,
    "maxmemory_policy": "noeviction",
    "cluster_enabled": 1,
    "role": "master",
}
REPLICA = {**PRIMARY, "role": "slave"}
DEMOTED = "WAITAOF cannot be used with replica instances."


class Node:
    """Answers INFO from a list of states, one per call, and refuses WAITAOF with `refusal`."""

    def __init__(self, states: list[dict[str, Any]], refusal: str) -> None:
        self.states = states
        self.refusal = refusal

    async def execute_command(self, *args: Any) -> Any:
        if args[0] == "INFO":
            return self.states.pop(0)
        raise ResponseError(self.refusal)

    async def aclose(self) -> None:
        pass


def connector(node: Node, monkeypatch: pytest.MonkeyPatch) -> Connector:
    c = Connector(RedisLocation.parse("redis+cluster://127.0.0.1:39531/ns?replicas=all"), None, 0)

    async def address() -> str:
        return "127.0.0.1:39531"

    monkeypatch.setattr(c, "_address", address)
    monkeypatch.setattr(c, "client", lambda where, **_: node)
    return c


async def test_a_primary_demoted_between_info_and_waitaof_is_a_failover_not_a_config_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = Node([PRIMARY, REPLICA], DEMOTED)
    with pytest.raises(Transient, match="no longer the primary"):
        await connector(node, monkeypatch).connect((CREATE, CAS, DELETE))


async def test_a_primary_without_waitaof_is_still_a_config_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = Node([PRIMARY, PRIMARY], "unknown command 'WAITAOF'")
    with pytest.raises(ConfigError, match="has no WAITAOF"):
        await connector(node, monkeypatch).connect((CREATE, CAS, DELETE))


def test_the_checks_read_every_section_they_need() -> None:
    assert redis_connect.INFO == ("INFO", "persistence", "memory", "replication", "cluster")
