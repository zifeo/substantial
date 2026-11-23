"""What runs cost (I-27, I-28): the store's prices, the chain's cost as `c.cost()` returns it, and
budgets."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from substantial._proto import layout, run
from substantial.errors import ConfigError, InvalidArgument, ProtocolError

# The largest amount or limit, as the core counts them.
# § one cvnf47vy8.a@1ktze
MAX = (1 << 63) - 1

_KINDS = ("create", "cas", "get", "list", "delete")
_REQUESTS = (*_KINDS, "requests")

_TABLES: dict[str, tuple[str, int, int, int, int, int]] = {
    # Source: https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json,
    # price list version 20261109154310 (published 2026-11-09), read 2026-11-15: S3 Standard, US
    # East (N. Virginia), "$0.005 per 1,000 PUT, COPY, POST, or LIST requests", "$0.004 per 10,000
    # GET and all other requests"; DELETE is free (https://aws.amazon.com/s3/pricing/, "DELETE
    # and CANCEL requests are free"). A compare-and-set is a conditional PUT.
    "aws-s3-standard-us-east-1-2026-11-09": ("USD", 5_000_000, 5_000_000, 400_000, 5_000_000, 0),
}


def is_currency(code: object) -> bool:
    return (
        isinstance(code, str)
        and len(code) == 3
        and code.isascii()
        and code.isalpha()
        and code.isupper()
    )


def _is_amount(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= MAX


@dataclass(frozen=True)
class Amount:
    """An integer amount of nano-units of a currency."""

    currency: str
    nano: int


@dataclass(frozen=True, init=False)
class Prices:
    """What the store charges: integer nano-units of `currency` per 1,000 requests of each kind
    (`create` and `cas` are S3's PUT class, `get` its GET class, `list` its LIST class). A request
    costs its share rounded up.

    `Prices("aws-s3-standard-us-east-1-2026-11-09")` is a shipped table, whose fields the keyword
    arguments override; without a name, every field is required."""

    currency: str
    create: int
    cas: int
    get: int
    list: int
    delete: int

    def __init__(
        self,
        name: str | None = None,
        *,
        currency: str | None = None,
        create: int | None = None,
        cas: int | None = None,
        get: int | None = None,
        list: int | None = None,
        delete: int | None = None,
    ) -> None:
        given: dict[str, object] = {
            "currency": currency,
            "create": create,
            "cas": cas,
            "get": get,
            "list": list,
            "delete": delete,
        }
        if name is not None:
            table = _TABLES.get(name) if isinstance(name, str) else None
            if table is None:
                raise ConfigError(f"no price table named {name!r}")
            fields = dict(zip(given, table, strict=True))
            fields.update({k: v for k, v in given.items() if v is not None})
        else:
            missing = [k for k, v in given.items() if v is None]
            if missing:
                raise ConfigError(f"prices without a table name need {', '.join(missing)}")
            fields = given
        if not is_currency(fields["currency"]):
            raise ConfigError(
                f"prices: the currency {fields['currency']!r} is not three uppercase ASCII letters"
            )
        for kind in _KINDS:
            if not _is_amount(fields[kind]):
                raise ConfigError(f"prices: {kind} is an int from 0 to {MAX}, not {fields[kind]!r}")
        for k, v in fields.items():
            object.__setattr__(self, k, v)

    def _of(self, op: str, attempts: int) -> Amount:
        """What `attempts` requests of the store operation `op` cost."""
        # § vg9yp9xsf@98gfz
        per_1000: int = getattr(self, op)
        return Amount(self.currency, -(-per_1000 // 1000) * attempts)

    def proto(self) -> run.Prices:
        return run.Prices(
            currency=self.currency,
            create=self.create,
            cas=self.cas,
            get=self.get,
            list=self.list,
            delete=self.delete,
        )


@dataclass(frozen=True)
class Meters:
    """Store requests by kind, their total, and amounts by currency in nano-units."""

    create: int
    cas: int
    get: int
    list: int
    delete: int
    requests: int
    amounts: dict[str, int]


@dataclass(frozen=True)
class Cost:
    """The chain's cost so far: its own store requests (`store`), its steps' charges by step name
    (`steps`) and its children's totals (`children`); the top-level fields are the total of the
    three."""

    create: int
    cas: int
    get: int
    list: int
    delete: int
    requests: int
    amounts: dict[str, int]
    store: Meters
    steps: dict[str, dict[str, int]]
    children: Meters


def _amounts(value: object, what: str) -> dict[str, int]:
    """Amounts as the core renders them, decimal strings, as they may pass 2^53."""
    if not isinstance(value, dict):
        raise ProtocolError(f"{what} {value!r}")
    out: dict[str, int] = {}
    # § hzh3dr6yh@v7kz2
    for currency, nano in value.items():
        if not isinstance(nano, str) or not (nano.isascii() and nano.isdigit()):
            raise ProtocolError(f"{what} {value!r}")
        out[currency] = int(nano)
    return out


def _meters(value: object, what: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProtocolError(f"{what} {value!r}")
    fields: dict[str, Any] = {}
    for k in _REQUESTS:
        n = value.get(k)
        if not isinstance(n, int) or isinstance(n, bool) or n < 0:
            raise ProtocolError(f"{what} {value!r}")
        fields[k] = n
    fields["amounts"] = _amounts(value.get("amounts"), f"{what} amounts")
    return fields


def cost_of(value: object) -> Cost:
    """A `Cost` from the JSON the core resolves `c.cost()` with."""
    total = _meters(value, "cost")
    assert isinstance(value, dict)
    steps = value.get("steps")
    if not isinstance(steps, dict):
        raise ProtocolError(f"cost steps {steps!r}")
    return Cost(
        **total,
        store=Meters(**_meters(value.get("store"), "cost store")),
        steps={step: _amounts(a, f"cost of step {step}") for step, a in steps.items()},
        children=Meters(**_meters(value.get("children"), "cost children")),
    )


def budget_proto(budget: object) -> layout.Budget:
    """A budget as the core takes it, its limits sorted by meter; `InvalidArgument` for a
    malformed meter or limit."""
    if not isinstance(budget, Mapping):
        raise InvalidArgument(f"a budget is a mapping of meters to limits, not {budget!r}")
    limits = []
    for meter, limit in budget.items():
        if meter not in _REQUESTS and not is_currency(meter):
            raise InvalidArgument(
                f"the budget meter {meter!r} is none of create, cas, get, list, delete, "
                "requests or a currency of three uppercase letters"
            )
        if not _is_amount(limit):
            raise InvalidArgument(
                f"the budget limit of {meter} is an int from 0 to {MAX}, not {limit!r}"
            )
        limits.append(layout.Limit(meter=meter, limit=limit))
    return layout.Budget(limits=sorted(limits, key=lambda limit: limit.meter))
