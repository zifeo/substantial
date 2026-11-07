"""Connections of the Redis adapter: its scripts, its password, where the primary of a namespace
is, and the checks at open."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.credentials import CredentialProvider
from redis.exceptions import AuthenticationError, NoPermissionError, RedisError, ResponseError

from substantial.errors import ConfigError, ModuleError
from substantial.stores.credentials import (
    REFRESH_BEFORE,
    CredentialsArg,
    Password,
    Provider,
    provide,
    resolve_password,
)
from substantial.stores.redis import ATTEMPT_TIMEOUT, CONNECT_TIMEOUT, WAITAOF_MS, RedisLocation
from substantial.stores.retry import Permanent, Transient


@dataclass(frozen=True)
class Script:
    text: bytes
    sha: str

    @staticmethod
    def shipped(name: str) -> Script:
        """A script of `redis/`, as the installed distribution ships it."""
        try:
            dist = importlib.metadata.distribution("substantial")
            text = dist.locate_file(f"substantial/redis/{name}").read_bytes()
        except (importlib.metadata.PackageNotFoundError, OSError) as e:
            raise ModuleError(
                f"the Redis script {name} is missing from the distribution: {e}"
            ) from e
        return Script(text, hashlib.sha1(text).hexdigest())


CREATE = Script.shipped("create.lua")
DELETE = Script.shipped("delete.lua")


class ProviderFailed(Exception):
    pass


class _Provided(CredentialProvider):
    """A provider's password, asked for at the first connection and again by any connection made
    within `REFRESH_BEFORE` of its `expires_at`; connections already open keep theirs."""

    def __init__(self, provider: Provider) -> None:
        self._provider = provider
        self._password: Password | None = None
        self._lock = asyncio.Lock()

    def get_credentials(self) -> tuple[str, ...]:
        raise NotImplementedError("the Redis store authenticates asynchronously")

    async def get_credentials_async(self) -> tuple[str, ...]:
        async with self._lock:
            p = self._password
            if p is None or (
                p.expires_at is not None and datetime.now(UTC) >= p.expires_at - REFRESH_BEFORE
            ):
                try:
                    p = await provide(self._provider, Password)
                except Exception as e:
                    raise ProviderFailed(f"credentials provider: {type(e).__name__}: {e}") from e
                self._password = p
        return (p.password,) if p.username is None else (p.username, p.password)


@dataclass
class Primary:
    """A connection pool to the primary holding the namespace, and the replicas a create waits
    for there."""

    client: Redis
    where: str
    replicas: int
    counted_at: float


class Connector:
    """Finds the primary of a namespace and opens a checked connection pool to it."""

    def __init__(self, location: RedisLocation, credentials: CredentialsArg, slot: int) -> None:
        password = resolve_password(credentials)
        self._auth: dict[str, Any] = {}
        if isinstance(password, Password):
            self._auth = {"username": password.username, "password": password.password}
        elif password is not None:
            self._auth = {"credential_provider": _Provided(password)}
        self._location = location
        self._slot = slot

    def client(self, where: str, *, db: int | None = None, auth: bool = True) -> Redis:
        host, _, port = where.rpartition(":")
        return Redis(
            host=host.strip("[]"),
            port=int(port),
            db=self._location.db if db is None else db,
            ssl=self._location.tls,
            socket_connect_timeout=CONNECT_TIMEOUT,
            socket_timeout=ATTEMPT_TIMEOUT,
            retry=Retry(NoBackoff(), 0),
            protocol=2,
            **(self._auth if auth else {}),
        )

    async def _address(self) -> str:
        """Where the primary is: the server, or what the sentinels or the cluster nodes say."""
        loc = self._location
        nodes = [f"[{h}]:{p}" if ":" in h else f"{h}:{p}" for h, p in loc.nodes]
        if loc.topology == "standalone":
            return nodes[0]
        failures: list[str] = []
        for node in nodes:
            # Sentinels take no password here: only the servers holding data do.
            seed = self.client(node, db=0, auth=loc.topology == "cluster")
            try:
                async with asyncio.timeout(CONNECT_TIMEOUT):
                    if loc.topology == "sentinel":
                        answer = await seed.execute_command(
                            "SENTINEL", "GET-MASTER-ADDR-BY-NAME", loc.master
                        )
                        if answer:
                            return f"{answer[0].decode()}:{int(answer[1])}"
                        failures.append(f"{node}: monitors no {loc.master}")
                        continue
                    for start, end, primary, *_ in await seed.execute_command("CLUSTER", "SLOTS"):
                        if start <= self._slot <= end:
                            return f"{primary[0].decode()}:{int(primary[1])}"
                    failures.append(f"{node}: no node serves slot {self._slot}")
            except (AuthenticationError, NoPermissionError) as e:
                raise Permanent(f"{node}: {type(e).__name__}: {e}") from e
            except (RedisError, OSError, TimeoutError) as e:
                failures.append(f"{node}: {type(e).__name__}: {e}")
            finally:
                await seed.aclose()
        raise Transient(f"no {loc.topology} node named the primary: {'; '.join(failures)}")

    async def connect(self, scripts: tuple[Script, ...]) -> Primary:
        where = await self._address()
        client = self.client(where)
        try:
            info = await client.execute_command(
                "INFO", "persistence", "memory", "replication", "cluster"
            )
            self._check(where, info)
            try:
                await client.execute_command("WAITAOF", 1, 0, WAITAOF_MS)
            except (AuthenticationError, NoPermissionError):
                raise
            except ResponseError as e:
                raise ConfigError(f"{where} has no WAITAOF, which needs Redis 7.2: {e}") from e
            for script in scripts:
                if (loaded := await client.script_load(script.text)) != script.sha:
                    raise Permanent(f"{where} loaded a script as {loaded}, not {script.sha}")
        except BaseException:
            await client.aclose()
            raise
        return Primary(client, where, self.replicas(info), asyncio.get_running_loop().time())

    def _check(self, where: str, info: dict[str, Any]) -> None:
        """The checks at open (I-26): a server that loses or evicts keys is not a store."""
        topology = self._location.topology
        if info.get("aof_enabled") != 1:
            raise ConfigError(f"{where} has no AOF: set appendonly yes and appendfsync always")
        policy = info.get("maxmemory_policy")
        if policy != "noeviction":
            raise ConfigError(f"{where} may evict keys, its maxmemory-policy is {policy}")
        if (info.get("cluster_enabled") == 1) != (topology == "cluster"):
            raise ConfigError(f"{where} is {'' if topology != 'cluster' else 'not '}a cluster node")
        if info.get("role") != "master":
            if topology == "standalone":
                raise ConfigError(f"{where} is a replica: name its primary")
            raise Transient(f"{where} is no longer the primary")

    def replicas(self, info: dict[str, Any]) -> int:
        """The replicas a create waits for: those of the URL, or every one online."""
        if self._location.replicas is not None:
            return self._location.replicas
        return sum(
            1
            for name, value in info.items()
            if name.startswith("slave") and isinstance(value, dict) and value["state"] == "online"
        )
