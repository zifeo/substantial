from __future__ import annotations

import asyncio
import inspect
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal

from substantial.errors import ConfigError

# A provider's keys are fetched again by the first request made this close to their expiry.
REFRESH_BEFORE = timedelta(minutes=5)


@dataclass(frozen=True)
class Credentials:
    """S3 credentials; `session_token` for temporary ones, and `expires_at`, an aware datetime,
    when a provider returns them."""

    access_key_id: str
    secret_access_key: str = field(repr=False)
    session_token: str | None = field(default=None, repr=False)
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        keys = (self.access_key_id, self.secret_access_key)
        if not all(isinstance(k, str) and k for k in keys):
            raise ConfigError("Credentials need an access key id and a secret access key")
        if self.session_token is not None and not isinstance(self.session_token, str):
            raise ConfigError("a session token is a string")
        at = self.expires_at
        if at is not None and (not isinstance(at, datetime) or at.utcoffset() is None):
            raise ConfigError(f"expires_at is an aware datetime, not {at!r}")


Provider = Callable[[], Credentials | Awaitable[Credentials]]
CredentialsArg = Credentials | Literal["env"] | Provider | None


def refuse(url: str, credentials: CredentialsArg) -> None:
    if credentials is not None:
        raise ConfigError(f"{url!r} takes no credentials")


def _static(credentials: CredentialsArg) -> Credentials:
    """The keys given once: explicit, or read from the standard variables."""
    if credentials is None:
        raise ConfigError('an S3 store needs credentials, "env", Credentials(...) or a provider')
    if isinstance(credentials, Credentials):
        if credentials.expires_at is not None:
            raise ConfigError("expires_at is for the credentials a provider returns")
        return credentials
    if credentials != "env":
        raise ConfigError(
            f'credentials are "env", Credentials(...) or a provider, not {credentials!r}'
        )

    def required(name: str) -> str:
        value = os.environ.get(name)
        if not value:
            raise ConfigError(f"{name} is not set")
        return value

    return Credentials(
        access_key_id=required("AWS_ACCESS_KEY_ID"),
        secret_access_key=required("AWS_SECRET_ACCESS_KEY"),
        session_token=os.environ.get("AWS_SESSION_TOKEN") or None,
    )


class Keys:
    """The keys an S3 store signs with: given once, or a provider's, fetched at the first request
    and again before any request made within `REFRESH_BEFORE` of their `expires_at`."""

    def __init__(self, credentials: CredentialsArg) -> None:
        if callable(credentials) and not isinstance(credentials, Credentials):
            self._provider: Provider = credentials
        else:
            given = _static(credentials)
            self._provider = lambda: given
        self._current: Credentials | None = None
        self._lock = asyncio.Lock()

    def _usable(self) -> Credentials | None:
        c = self._current
        if c is None or c.expires_at is None:
            return c
        return c if datetime.now(UTC) < c.expires_at - REFRESH_BEFORE else None

    async def get(self) -> Credentials:
        """The keys to sign with; what the provider raises propagates, and the next call asks it
        again."""
        async with self._lock:
            current = self._usable()
            if current is not None:
                return current
            # A synchronous provider may block, as a metadata endpoint does: a thread keeps the
            # worker going.
            result = await asyncio.to_thread(self._provider)
            if inspect.isawaitable(result):
                result = await result
            if not isinstance(result, Credentials):
                raise ConfigError(f"a credentials provider returns Credentials, not {result!r}")
            self._current = result
            return result
