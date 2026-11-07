"""Replays of hand-built `RunInput`s, the test playing the agent machine's part in memory."""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import timedelta
from typing import Any

import pytest

from substantial import (
    Context,
    NotInWorkflow,
    Permanent,
    Retry,
    StepFailed,
    Workflow,
    seconds,
    step,
    step_key,
    workflow,
)
from substantial._proto import agent, boundary, json_value, layout, run
from substantial.clock import Clock
from substantial.core import Core
from substantial.replay import Channel, Replay, replay
from substantial.stores.memory import MemoryStore


class Replayed:
    def __init__(self, commits: list[run.Commit], end: agent.ReplayEnd) -> None:
        self.commits = commits
        self.end = end

    def events(self) -> list[str]:
        return [e.WhichOneof("kind") for c in self.commits for e in c.segment.events]

    def kinds(self) -> list[int]:
        return [c.kind for c in self.commits]

    def next_wake(self) -> int | None:
        assert self.end.WhichOneof("end") == "idle", "dropped"
        return self.end.idle.next_wake.ms if self.end.idle.HasField("next_wake") else None

    def _event(self, kind: str) -> Any:
        found = [
            getattr(e, kind) for c in self.commits for e in c.segment.events if e.HasField(kind)
        ]
        return found[0] if found else None

    def completed(self) -> Any:
        c = self._event("completed")
        return None if c is None else json.loads(c.value.json)

    def failed(self) -> layout.Failure | None:
        f = self._event("failed")
        return None if f is None else f.error


class Harness:
    """An in-memory stand-in for the agent machine: it wins or loses every commit and keeps the
    history and inbox a store would hold."""

    def __init__(self, w: Workflow, step_timeout: timedelta = timedelta(minutes=5)) -> None:
        self.core = Core.new()
        self.store = MemoryStore(f"harness-{id(self)}")
        self.step_timeout = step_timeout
        self.workflow = w
        self.input = b"{}"
        self.history: list[layout.Event] = []
        self.next_n = 0
        self.inbox: list[run.InboxMessage] = []
        self.now = 0

    def message(self, key: str, name: str, value: str) -> None:
        body = layout.Message(
            id=f"id-{key}", name=name, value=json_value(value.encode()), sent_at=layout.Time(ms=0)
        )
        self.inbox.append(run.InboxMessage(key=key, body=body))

    def run_input(self) -> run.RunInput:
        start = layout.Start(
            workflow=self.workflow.name,
            input=json_value(self.input),
            chain="run-1",
            created_at=layout.Time(ms=0),
        )
        settings = run.RunSettings(
            continue_as_new_after_segments=1000,
            continue_as_new_after_bytes=8 << 20,
            dedup_window_ms=86_400_000,
            retention=run.Retention(forever=boundary.Empty()),
        )
        return run.RunInput(
            run="run-1",
            start=start,
            history=self.history,
            next_n=self.next_n,
            inbox=self.inbox,
            now=layout.Time(ms=self.now),
            seed=bytes(32),
            partitions=1,
            worker="test",
            settings=settings,
        )

    async def replay(self, now: int, won: bool = True) -> Replayed:
        self.now = now
        events: asyncio.Queue[tuple[Any, ...]] = asyncio.Queue()
        channel = Channel()
        r = Replay(self.core, self.store, _Fixed(self.now), events.put_nowait, self.step_timeout)
        task = asyncio.create_task(replay(r, self.workflow, self.run_input(), channel))
        commits = []
        while True:
            event = await events.get()
            if event[0] == "ended":
                await task
                return Replayed(commits, event[2])
            commit = event[2]
            channel.committed.put_nowait(agent.AgentCommand.Committed(n=commit.segment.n, won=won))
            if won:
                self.history.extend(commit.segment.events)
                self.next_n = commit.segment.n + 1
                self.inbox = [m for m in self.inbox if m.key not in commit.consumed]
                commits.append(commit)


class _Fixed(Clock):
    """The real clock's waits at a fixed time."""

    def __init__(self, ms: int) -> None:
        self.ms = ms

    def now(self) -> layout.Time:
        return layout.Time(ms=self.ms)


def counting(name: str, value: Any) -> tuple[list[int], Any]:
    runs: list[int] = []

    @step(name=name)
    async def counted() -> Any:
        runs.append(1)
        return value

    return runs, counted


async def test_a_step_then_the_end() -> None:
    runs, five = counting("five", 5)

    @workflow
    async def double(c: Context) -> int:
        return await five() * 2

    replayed = await Harness(double).replay(1_000)
    # The code ran on to its end before the next `next`: one commit records both.
    assert replayed.kinds() == [run.COMMIT_KIND_END]
    assert replayed.events() == ["step", "completed"]
    assert replayed.completed() == 10
    assert replayed.next_wake() is None
    assert len(runs) == 1


async def test_a_replay_returns_recorded_steps_and_fires_sleeps() -> None:
    a_runs, a = counting("a", 1)
    b_runs, b = counting("b", 2)

    @workflow
    async def sleepy(c: Context) -> int:
        x = await a()
        await c.sleep(seconds(10))
        return x + await b()

    h = Harness(sleepy)
    first = await h.replay(1_000)
    assert first.events() == ["step", "timer"]
    assert first.next_wake() == 11_000
    assert (await h.replay(5_000)).commits == [], "nothing to record before the timer"
    assert (await h.replay(11_000)).completed() == 3
    assert (len(a_runs), len(b_runs)) == (1, 1)


async def test_receive_takes_the_next_message_of_its_name() -> None:
    @workflow
    async def receiver(c: Context) -> int:
        return await c.receive("by") * await c.receive("by")

    h = Harness(receiver)
    h.message("k1", "other", "0")
    h.message("k2", "by", "6")
    first = await h.replay(1_000)
    assert first.next_wake() is None
    assert first.completed() is None
    h.message("k3", "by", "7")
    assert (await h.replay(2_000)).completed() == 42
    assert h.inbox == [], "every message consumed"


RETRY = Retry(max_attempts=3, initial=seconds(1), max=seconds(4), multiplier=2)


def flaky(fail_first: int, permanent: bool, retry: Retry | None) -> tuple[list[int], Any]:
    runs: list[int] = []

    @step(name="flaky", retry=retry)
    def attempt() -> str:
        runs.append(1)
        n = len(runs)
        if n <= fail_first:
            raise Permanent("gone") if permanent else RuntimeError(f"attempt {n}")
        return f"ok after {n}"

    return runs, attempt


async def test_a_failed_attempt_backs_off_then_retries() -> None:
    runs, f = flaky(1, False, RETRY)

    @workflow
    async def retrying(c: Context) -> str:
        return await f()

    h = Harness(retrying)
    first = await h.replay(1_000)
    assert first.events() == ["step_retry"]
    wake = first.next_wake()
    assert wake is not None and wake > 1_000
    assert (await h.replay(wake)).completed() == "ok after 2"
    assert len(runs) == 2


@pytest.mark.parametrize(("permanent", "retry"), [(False, None), (True, RETRY)])
async def test_steps_without_retry_or_with_a_permanent_error_fail_at_once(
    permanent: bool, retry: Retry | None
) -> None:
    runs, f = flaky(9, permanent, retry)

    @workflow
    async def failing(c: Context) -> str:
        return await f()

    failure = (await Harness(failing).replay(1_000)).failed()
    assert failure is not None and failure.type == "StepFailed", failure
    assert len(runs) == 1


async def test_a_late_attempt_times_out() -> None:
    @step(timeout=seconds(0.02))
    async def slow() -> int:
        await asyncio.sleep(5)
        return 1

    @workflow
    async def late(c: Context) -> str:
        try:
            await slow()
        except StepFailed as e:
            return str(e)
        raise AssertionError("the attempt did not time out")

    assert "attempt past" in (await Harness(late).replay(1_000)).completed()


async def test_parallel_steps_replay_the_same() -> None:
    a_runs, a = counting("a", 1)
    b_runs, b = counting("b", 2)

    @workflow
    async def parallel(c: Context) -> int:
        x, y = await asyncio.gather(a(), b())
        await c.sleep(seconds(1))
        return x + y

    h = Harness(parallel)
    assert (await h.replay(1_000)).next_wake() == 2_000
    assert (await h.replay(2_000)).completed() == 3
    assert len(a_runs) + len(b_runs) == 2


async def test_other_calls_than_the_history_fail_the_run() -> None:
    _, renamed = counting("renamed", 1)

    @workflow
    async def changed(c: Context) -> int:
        return await renamed()

    h = Harness(changed)
    recorded = layout.Step(name="original", occurrence=0, attempt=1, value=json_value(b"1"))
    h.history.append(layout.Event(step=recorded))
    h.next_n = 1
    failure = (await h.replay(1_000)).failed()
    assert failure is not None and failure.type == "NonDeterminism", failure


async def test_a_lost_commit_drops_the_replay() -> None:
    _, a = counting("a", 1)

    @workflow
    async def loser(c: Context) -> int:
        return await a()

    h = Harness(loser)
    replayed = await h.replay(1_000, won=False)
    assert replayed.end.WhichOneof("end") == "dropped"
    assert h.history == []


async def test_inputs_and_errors_of_the_code() -> None:
    class Negative(Exception):
        pass

    @workflow
    async def typed(c: Context, x: int) -> int:
        if x < 0:
            raise Negative("x < 0")
        return x + 1

    for data, failure in [
        (b'{"x": 41}', None),
        (b'{"x": -1}', "Negative"),
        (b'{"y": 1}', "TypeError"),
    ]:
        h = Harness(typed)
        h.input = data
        replayed = await h.replay(1_000)
        if failure is None:
            assert replayed.completed() == 42
        else:
            assert replayed.failed().type == failure


async def test_synchronous_steps_run_in_a_thread_with_their_key() -> None:
    main = threading.get_ident()

    def where(tag: str) -> list[Any]:
        return [threading.get_ident() != main, step_key(), tag]

    @workflow
    async def threaded(c: Context) -> list[Any]:
        first = await c.step("where", where, "a")
        second = await c.step("key", lambda: step_key())
        return [first, second]

    assert (await Harness(threaded).replay(1_000)).completed() == [
        [True, "run-1/where/0", "a"],
        "run-1/key/0",
    ]


async def test_steps_outside_a_workflow_and_steps_without_a_name_are_refused() -> None:
    _, a = counting("a", 1)
    with pytest.raises(NotInWorkflow):
        a()
    with pytest.raises(NotInWorkflow):
        step_key()

    @workflow
    async def unnamed(c: Context) -> int:
        return await c.step(abs, -1)  # type: ignore[arg-type]

    assert (await Harness(unnamed).replay(1_000)).failed().type == "InvalidArgument"
