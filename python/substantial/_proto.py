"""Short names for the code generated from `proto/substantial`."""

import json

from substantial.boundary.v1 import agent_pb2 as agent
from substantial.boundary.v1 import boundary_pb2 as boundary
from substantial.boundary.v1 import plan_pb2 as plan
from substantial.boundary.v1 import run_pb2 as run
from substantial.errors import ValueTooLarge
from substantial.layout.v1 import layout_pb2 as layout

__all__ = ["agent", "boundary", "layout", "plan", "run"]

# Far below what traps the core: a value of about 1.5 GB exhausts its 4 GiB of wasm32 memory.
# § one h8wg39cct@w68bs
MAX_VALUE = 64 * 1024 * 1024


def checked(data: bytes) -> bytes:
    """`data`, a JSON encoding, refused past `MAX_VALUE` bytes."""
    # § h8wg39cct@75etm
    if len(data) > MAX_VALUE:
        raise ValueTooLarge(f"a value of {len(data)} bytes is past the limit of {MAX_VALUE} bytes")
    return data


def dumps(value: object) -> bytes:
    """The JSON of a value of the code. `json` would write a NaN as `NaN`, a key that is not a
    string as one, `{1: x}` as `{"1": x}`, a tuple as a list, and an integer past 2^53 - 1 that a
    JavaScript reader rounds: all four raise `ValueError` instead, and an object it cannot encode
    `TypeError`."""
    # § z6zyx22ss@1p9p0
    check_json(value)
    # § one e1dc4dar4@3g3gs
    return json.dumps(value, allow_nan=False).encode()


# The largest integer a JSON number read as a double holds exactly.
SAFE = 2**53 - 1


def check_json(value: object) -> None:
    """Raises `ValueError` for what `dumps` refuses that `json`, or `rfc8785` for an input,
    would write as another value."""
    stack = [value]
    seen: set[int] = set()
    while stack:
        v = stack.pop()
        # A cycle is left to `json`, which refuses it.
        if id(v) in seen:
            continue
        if isinstance(v, (dict, list)):
            seen.add(id(v))
        if isinstance(v, dict):
            for k, item in v.items():
                # § one z6zyx22ss.a@t2tm0
                if not isinstance(k, str):
                    raise ValueError(f"the key {k!r} is not a string")
                stack.append(item)
        elif isinstance(v, list):
            stack.extend(v)
        elif isinstance(v, tuple):
            # § z6zyx22ss.a@s5f60
            raise ValueError(f"the tuple {v!r} is not JSON, which reads it back as a list")
        elif isinstance(v, int) and not isinstance(v, bool) and abs(v) > SAFE:
            # § z6zyx22ss.e@n9v0k
            raise ValueError(
                f"{v} is past 2^53 - 1, the largest integer a JSON number holds exactly"
            )


def json_value(data: bytes) -> layout.Value:
    return layout.Value(json=checked(data))


def time(ms: int) -> layout.Time:
    return layout.Time(ms=ms)
