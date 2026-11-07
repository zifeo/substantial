"""The naming rules of I-17, checked by the host so that a refusal states the rule."""

from __future__ import annotations

import re

from substantial.errors import InvalidArgument

_KEY = re.compile(r"[a-z0-9._-]{1,100}")
_NAME = re.compile(r"[A-Za-z0-9._-]{1,128}")


def check_key(what: str, value: object) -> str:
    """Run and message ids, workflow names and published keys, which go into store keys."""
    if (
        isinstance(value, str)
        and _KEY.fullmatch(value) is not None
        and value not in (".", "..")
        and not value.startswith("substantial.")
    ):
        return value
    raise InvalidArgument(
        f"{what} {value!r} must match [a-z0-9._-]{{1,100}}, . and .. excepted, and not start "
        "with substantial."
    )


def check_name(what: str, value: object) -> str:
    """Step and message names."""
    if isinstance(value, str) and _NAME.fullmatch(value) is not None:
        return value
    raise InvalidArgument(f"{what} {value!r} must match [A-Za-z0-9._-]{{1,128}}")


def check_str(what: str, value: object) -> str:
    """A free text, such as a reason."""
    if isinstance(value, str):
        return value
    raise InvalidArgument(f"{what} is a string, not {value!r}")
