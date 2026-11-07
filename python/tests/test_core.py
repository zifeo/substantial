"""The module, its checksum and the boundary ABI (I-25)."""

import pytest

from substantial._proto import agent, boundary, layout, run
from substantial.core import WASM_NAME, Core, CoreModule, Kind, shipped_files
from substantial.errors import CoreError, LayoutError, ModuleError, NonDeterminism
from substantial.machines import AgentMachine, core_error


def next_request() -> bytes:
    return agent.AgentRequest(next=layout.Time(ms=0)).SerializeToString()


def agent_error(core: Core, machine: int, request: bytes) -> agent.AgentResponse:
    response = agent.AgentResponse()
    response.ParseFromString(core.call(machine, request))
    assert response.WhichOneof("response") == "error", response
    return response.error


def test_the_shipped_module_matches_its_checksum() -> None:
    wasm, sums = shipped_files()
    CoreModule.shipped()
    with pytest.raises(ModuleError, match="sha256"):
        CoreModule(wasm, f"{'0' * 64} {WASM_NAME}\n")
    with pytest.raises(ModuleError, match=r"no substantial\.wasm line"):
        CoreModule(wasm, "abc other.wasm\n")
    tampered = bytearray(wasm)
    tampered[100] ^= 1
    with pytest.raises(ModuleError):
        CoreModule(bytes(tampered), sums)


def test_invalid_machines_answer_every_call_with_their_error() -> None:
    core = Core.new()
    for kind, data in [(9, b""), (Kind.AGENT, b"\xff\xff"), (Kind.AGENT, b"")]:
        machine = core.new_machine(kind, data)
        assert machine != 0, "sub_new never returns 0"
        first = agent_error(core, machine, next_request())
        assert agent_error(core, machine, next_request()) == first
        core.drop_machine(machine)


def config() -> agent.AgentConfig:
    return agent.AgentConfig(
        partitions=1,
        worker="w",
        workflows=[agent.Served(name="w", unversioned=True)],
        concurrency=1,
        claim_ttl_ms=30_000,
        claim_skew_ms=5_000,
        max_idle_ms=1_000,
        due_page=100,
        due_pages=10,
        snapshot_every=64,
        op_retries=5,
        wait_seconds=20,
        seed=bytes(32),
        settings=run.RunSettings(continue_as_new_after_segments=1000),
    )


def test_undecodable_requests_and_unknown_machines_are_errors() -> None:
    core = Core.new()
    machine = core.new_machine(Kind.AGENT, config().SerializeToString())
    invalid = boundary.ERROR_KIND_INVALID
    assert agent_error(core, machine, b"\xff\xff\xff").kind == invalid
    response = agent.AgentResponse()
    response.ParseFromString(core.call(machine, next_request()))
    assert response.WhichOneof("response") == "command", "left as it was"
    assert agent_error(core, 123_456, next_request()).kind == invalid
    core.drop_machine(machine)
    assert agent_error(core, machine, next_request()).kind == invalid


def test_core_errors_map_to_host_errors() -> None:
    machine = AgentMachine(Core.new(), agent.AgentConfig())
    with pytest.raises(CoreError) as raised:
        machine.next(layout.Time(ms=0))
    assert raised.value.kind == "INVALID"
    machine.close()
    machine.close()
    kinds = {
        boundary.ERROR_KIND_NON_DETERMINISM: NonDeterminism,
        boundary.ERROR_KIND_LAYOUT: LayoutError,
        boundary.ERROR_KIND_CORRUPT: CoreError,
    }
    for kind, error in kinds.items():
        assert type(core_error(boundary.CoreError(kind=kind, message="m"))) is error


def test_ulids_are_monotonic_in_the_process() -> None:
    from substantial.plan import ulid

    first = ulid(1_000_000)
    same = ulid(1_000_000)
    earlier = ulid(999_000)
    later = ulid(2_000_000)
    assert first < same < earlier < later
    digits = "0123456789abcdefghjkmnpqrstvwxyz"

    def value(u: str) -> int:
        return sum(digits.index(ch) << (5 * i) for i, ch in enumerate(reversed(u)))

    assert value(same) - value(first) == 1, "the same millisecond adds one"
    assert value(earlier) - value(same) == 1, "so does an earlier one"
    assert earlier[:10] == first[:10], "a clock gone back keeps the last time"
    assert len(later) == 26 and later == later.lower()
