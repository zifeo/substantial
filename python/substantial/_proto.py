"""Short names for the code generated from `proto/substantial`."""

from substantial.boundary.v1 import agent_pb2 as agent
from substantial.boundary.v1 import boundary_pb2 as boundary
from substantial.boundary.v1 import plan_pb2 as plan
from substantial.boundary.v1 import run_pb2 as run
from substantial.errors import ValueTooLarge
from substantial.layout.v1 import layout_pb2 as layout

__all__ = ["agent", "boundary", "layout", "plan", "run"]

# Far below what traps the core: a value of about 1.5 GB exhausts its 4 GiB of wasm32 memory.
MAX_VALUE = 64 * 1024 * 1024


def checked(data: bytes) -> bytes:
    """`data`, a JSON encoding, refused past `MAX_VALUE` bytes."""
    if len(data) > MAX_VALUE:
        raise ValueTooLarge(f"a value of {len(data)} bytes is past the limit of {MAX_VALUE} bytes")
    return data


def json_value(data: bytes) -> layout.Value:
    return layout.Value(json=checked(data))


def time(ms: int) -> layout.Time:
    return layout.Time(ms=ms)
