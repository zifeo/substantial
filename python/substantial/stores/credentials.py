from __future__ import annotations

import asyncio
import inspect
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
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
        _check_expiry(self.expires_at)


@dataclass(frozen=True)
class Password:
    """A Redis password, for the ACL user `username` or for the default user without one; and
    `expires_at`, an aware datetime, when a provider returns it."""

    password: str = field(repr=False)
    username: str | None = None
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.password, str) or not self.password:
            raise ConfigError("a Password needs a password")
        if self.username is not None and (not isinstance(self.username, str) or not self.username):
            raise ConfigError(f"a username is a non-empty string, not {self.username!r}")
        _check_expiry(self.expires_at)


def _check_expiry(at: datetime | None) -> None:
    if at is not None and (not isinstance(at, datetime) or at.utcoffset() is None):
        raise ConfigError(f"expires_at is an aware datetime, not {at!r}")


Provider = Callable[[], Credentials | Password | Awaitable[Credentials | Password]]
CredentialsArg = Credentials | Password | Literal["env"] | Provider | None


def refuse(url: str, credentials: CredentialsArg) -> None:
    if credentials is not None:
        raise ConfigError(f"{url!r} takes no credentials")


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(f"{name} is not set")
    return value


def resolve(credentials: CredentialsArg) -> Credentials | Provider:
    """The keys given once, explicit or read from the standard variables, or the provider to ask
    for them."""
    if callable(credentials) and not isinstance(credentials, Credentials | Password):
        return credentials
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
    return Credentials(
        access_key_id=_required("AWS_ACCESS_KEY_ID"),
        secret_access_key=_required("AWS_SECRET_ACCESS_KEY"),
        session_token=os.environ.get("AWS_SESSION_TOKEN") or None,
    )


def resolve_password(credentials: CredentialsArg) -> Password | Provider | None:
    """A Redis store's password given once, explicit or read from `REDIS_PASSWORD` and
    `REDIS_USERNAME`, the provider to ask for it, or `None` for a server without AUTH."""
    if credentials is None:
        return None
    if callable(credentials) and not isinstance(credentials, Credentials | Password):
        return credentials
    if isinstance(credentials, Password):
        if credentials.expires_at is not None:
            raise ConfigError("expires_at is for the password a provider returns")
        return credentials
    if credentials != "env":
        raise ConfigError(
            f'a Redis store takes "env", Password(...), a provider or None, not {credentials!r}'
        )
    return Password(_required("REDIS_PASSWORD"), os.environ.get("REDIS_USERNAME") or None)


async def provide[T: (Credentials, Password)](provider: Provider, kind: type[T]) -> T:
    """What a provider returns, of the kind its store takes."""
    # A synchronous provider may block, as a metadata endpoint does: a thread keeps the worker
    # going.
    result = await asyncio.to_thread(provider)
    if inspect.isawaitable(result):
        result = await result
    if not isinstance(result, kind):
        raise ConfigError(f"a credentials provider returns {kind.__name__}, not {result!r}")
    return result
