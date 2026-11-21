"""The naming rules of I-17, checked by the host so that a refusal states the rule."""

from __future__ import annotations

import re

from substantial.errors import InvalidArgument

# § px8dv58y7@w01ma
_KEY = re.compile(r"[a-z0-9._-]{1,100}")
_NAME = re.compile(r"[A-Za-z0-9._-]{1,128}")
# § end px8dv58y7


def check_key(what: str, value: object) -> str:
    """Run and message ids, workflow names and published keys, which go into store keys."""
    if (
        isinstance(value, str)
        and _KEY.fullmatch(value) is not None
        # § px8dv58y7@3rreg
        and value not in (".", "..")
        # § end px8dv58y7
        # § px8dv58y7.b@gcak8
        and not value.startswith("substantial.")
        # § end px8dv58y7.b
    ):
        return value
    # § one px8dv58y7.a@a9844
    raise InvalidArgument(
        f"{what} {value!r} must match [a-z0-9._-]{{1,100}}, . and .. excepted, and not start "
        "with substantial."
    )


def check_name(what: str, value: object) -> str:
    """Step and message names."""
    if isinstance(value, str) and _NAME.fullmatch(value) is not None:
        return value
    # § px8dv58y7.a@abpcj
    raise InvalidArgument(f"{what} {value!r} must match [A-Za-z0-9._-]{{1,128}}")
    # § end px8dv58y7.a


def check_sent_name(value: object) -> str:
    """The name of a message sent, by a client or a workflow."""
    name = check_name("message name", value)
    # § one j69rwh8x9@rfgd6
    if name.startswith("substantial."):
        raise InvalidArgument(f"message name {name!r} starts with substantial., which is reserved")
    return name


# The inline step `c.schedule()` records its fires in.
RESERVED_STEP = "schedule.next"


def check_step_name(value: object) -> str:
    """A step name of the code: `schedule.next` is `c.schedule()`'s."""
    # § one vnykycs34.b@0zq9r
    if value == RESERVED_STEP:
        raise InvalidArgument(f"the step name {RESERVED_STEP!r} is reserved for c.schedule()")
    name = check_name("step name", value)
    # § one px8dv58y7.c@9qqd4
    if name.startswith("substantial."):
        raise InvalidArgument(f"step name {name!r} starts with substantial., which is reserved")
    return name


def check_str(what: str, value: object) -> str:
    """A free text, such as a reason."""
    if isinstance(value, str):
        return value
    raise InvalidArgument(f"{what} is a string, not {value!r}")
