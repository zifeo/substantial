"""Runs the workflows of dev/cross between every pair of hosts: a worker in one language serves
the runs a client in another starts with a budget, lists while they wait on messages, messages,
awaits and reads the history of, through one shared store. The run's step charges an amount and
the run reads its cost back, within the budget one host encoded and another's worker decides. A
second run, of `over`, charges past its budget: the worker fails it with `BudgetExceeded`, and the
client reads that type and the meter, limit and amount used from its result and history. Then
the client configures the schedule of a run of `ticks` the worker serves, waits for the config it
publishes, and checks that the same call does nothing and an older revision is refused. Then a run
of the third host's `watch-<language>`, served by that host's watcher, awaits the end of a run the
client started and the worker serves. Then each host starts the same id, without a budget, with
the same input written its own way, keys in
another order or `200.0` for `200`, and must get the same run, the input being canonical JSON.
Uses a `file:` store, the S3 one `SUBSTANTIAL_S3_URL` names and the Redis one
`SUBSTANTIAL_REDIS_URL` names, those of dev/compose.yaml at the URLs of dev/stores.env unless the
environment sets them.

    docker compose -f dev/compose.yaml up -d --wait
    uv run dev/cross/run.py

Needs cargo, uv and node 24 on the PATH, and `npm ci` done. Exits 1 on any mismatch, and at once
when a store does not answer.
"""

import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import permutations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STORES = ROOT / "dev" / "stores.env"
UP = "docker compose -f dev/compose.yaml up -d --wait"
CLIENT_TIMEOUT_S = 90
SHUTDOWN_TIMEOUT_S = 30


def output(*cmd: str) -> str:
    return subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True).stdout


def commands() -> dict[str, list[str]]:
    subprocess.run(
        ["cargo", "build", "-q", "-p", "substantial", "--example", "cross", "--features", "redis"],
        cwd=ROOT,
        check=True,
    )
    target = json.loads(output("cargo", "metadata", "--format-version", "1", "--no-deps"))[
        "target_directory"
    ]
    python = output(
        "uv", "run", "--locked", "python", "-c", "import sys; print(sys.executable)"
    ).strip()
    return {
        "rust": [str(Path(target) / "debug" / "examples" / "cross")],
        "python": [python, str(ROOT / "dev" / "cross" / "cross.py")],
        "typescript": [
            "node",
            "--conditions=substantial-source",
            str(ROOT / "dev" / "cross" / "cross.ts"),
        ],
    }


def load_stores() -> None:
    """Sets each variable of dev/stores.env the environment does not, for this script and the
    hosts it runs."""
    for n, line in enumerate(STORES.read_text().splitlines(), 1):
        if not line or line.startswith("#"):
            continue
        name, sep, value = line.partition("=")
        if not sep:
            sys.exit(f"{STORES}:{n} is not NAME=value: {line!r}")
        os.environ.setdefault(name, value)


def reached(variable: str) -> str:
    """The URL `variable` names, once each of its hosts accepts a connection."""
    url = os.environ.get(variable)
    if not url:
        sys.exit(f"{variable} is not set: see dev/stores.env and run `{UP}`")
    for address in url.partition("://")[2].partition("/")[0].split(","):
        host, _, port = address.rpartition(":")
        if not host or not port.isdigit():
            sys.exit(f"{variable} names {address!r}, not a host:port")
        try:
            socket.create_connection((host, int(port)), timeout=2).close()
        except OSError as e:
            sys.exit(f"{variable} names {address}, which does not answer ({e}): run `{UP}`")
    return url


def fresh_url(variable: str) -> str:
    """A fresh namespace under the one `variable` names."""
    url = reached(variable)
    path, _, query = url.partition("?")
    if not query:
        sys.exit(f"{variable} has no query: {url!r}")
    return f"{path}/cross-{secrets.token_hex(8)}?{query}"


def pair(cmds: dict[str, list[str]], client: str, worker: str, url: str, logs: Path) -> str | None:
    """Runs one pair, with the third host's watcher; the reason it failed, or None."""
    [third] = [host for host in cmds if host not in (client, worker)]
    log = logs / f"{worker}-worker-{secrets.token_hex(4)}.log"
    watch_log = logs / f"{third}-watcher-{secrets.token_hex(4)}.log"
    with log.open("w") as out, watch_log.open("w") as watch_out:
        served = subprocess.Popen(
            [*cmds[worker], "worker", url], cwd=ROOT, stdout=out, stderr=subprocess.STDOUT
        )
        watching = subprocess.Popen(
            [*cmds[third], "watcher", url], cwd=ROOT, stdout=watch_out, stderr=subprocess.STDOUT
        )
    try:
        ran = subprocess.run(
            [*cmds[client], "client", url, third],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=CLIENT_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as e:
        ran = None
        failure = f"the client did not end within {CLIENT_TIMEOUT_S} s\n{e.stderr!r}"
    codes = []
    for process in (served, watching):
        process.send_signal(signal.SIGTERM)
    for process in (served, watching):
        try:
            codes.append(process.wait(SHUTDOWN_TIMEOUT_S))
        except subprocess.TimeoutExpired:
            process.kill()
            codes.append(None)
    worker_log = (
        f"\n--- {worker} worker ---\n{log.read_text()}"
        f"\n--- {third} watcher ---\n{watch_log.read_text()}"
    )
    if ran is None:
        return failure + worker_log
    if ran.returncode != 0:
        return f"the client exited with {ran.returncode}\n{ran.stdout}{ran.stderr}{worker_log}"
    # § sqbjc1adw@wq5pb
    expected = {
        "result": {
            "greeting": "hello ada",
            "suffix": "!!!",
            "worker": worker,
            "charged": "1500000",
            "counted": True,
        },
        "status": "completed",
        "history": ["step", "timer", "fired", "message", "step", "step", "completed"],
        "over": {"type": "BudgetExceeded", "meter": "USD", "limit": "1000000", "used": "1500000"},
        "schedule": {
            "revision": 2,
            "cron": "0 3 * * *",
            "tz": "Europe/Zurich",
            "backfill": 3,
            "last": None,
            "recent": [],
            "stale": "ConfigError",
        },
        "watched": {"watcher": third, "result": {"worker": worker, "go": "now"}},
        "client": client,
    }
    try:
        got = json.loads(ran.stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        got = None
    if not isinstance(got, dict):
        return f"expected {json.dumps(expected)}\n     got {ran.stdout}{ran.stderr}{worker_log}"

    def differs(field: str, want: object, have: object) -> str:
        return (
            f"{field}: expected {json.dumps(want)}\n     got {json.dumps(have)}\n"
            f"{ran.stdout}{ran.stderr}{worker_log}"
        )

    history, over = got.get("history") or [], got.get("over") or {}
    # § one ap5tfnrja.d@pzabt
    if got.get("result") != expected["result"]:
        return differs("result", expected["result"], got.get("result"))
    # § one eqpe8dchq@b4hb2
    if history[:3] != ["step", "timer", "fired"]:
        return differs("the sleep", ["step", "timer", "fired"], history[:3])
    # § one bq8tt3bh7@bwxh7
    if history != expected["history"]:
        return differs("history", expected["history"], history)
    # § one j2xzy4akb@v2kc5
    if over.get("type") != "BudgetExceeded":
        return differs("the budget's failure", "BudgetExceeded", over.get("type"))
    # § one fkgsx3gvk.b@df3gw
    if over != expected["over"]:
        return differs("the budget's details", expected["over"], over)
    # § one gzga227zz@qb3vv
    if got.get("watched") != expected["watched"]:
        return differs("watched", expected["watched"], got.get("watched"))
    if got != expected:
        return f"expected {json.dumps(expected)}\n     got {ran.stdout}{ran.stderr}{worker_log}"
    # § end sqbjc1adw
    if codes != [0, 0]:
        return (
            f"the worker or the watcher did not shut down cleanly on SIGTERM: {codes}{worker_log}"
        )
    return None


def same_input(cmds: dict[str, list[str]], url: str) -> str | None:
    """Starts `same-input` from every host in turn; the reason it failed, or None."""
    for host, cmd in cmds.items():
        ran = subprocess.run(
            [*cmd, "start", url], cwd=ROOT, capture_output=True, text=True, timeout=CLIENT_TIMEOUT_S
        )
        expected = {"chain": "same-input", "status": "pending"}
        try:
            got = json.loads(ran.stdout.strip().splitlines()[-1])
        except (IndexError, json.JSONDecodeError):
            got = None
        # § jbkv4v0we.d@h89fv
        if ran.returncode != 0 or got != expected:
            return f"{host}: expected {json.dumps(expected)}\n     got {ran.stdout}{ran.stderr}"
    return None


def main() -> None:
    load_stores()
    reached("SUBSTANTIAL_S3_URL")
    reached("SUBSTANTIAL_REDIS_URL")
    cmds = commands()
    with tempfile.TemporaryDirectory(prefix="substantial-cross-") as tmp:
        logs = Path(tmp)
        pairs = list(permutations(cmds, 2))
        cases = [("file", c, w, f"file:{logs / f'{c}-{w}'}") for c, w in pairs]
        cases += [("s3", c, w, fresh_url("SUBSTANTIAL_S3_URL")) for c, w in pairs]
        cases += [("redis", c, w, fresh_url("SUBSTANTIAL_REDIS_URL")) for c, w in pairs]
        inputs = [
            ("file", f"file:{logs / 'same-input'}"),
            ("s3", fresh_url("SUBSTANTIAL_S3_URL")),
            ("redis", fresh_url("SUBSTANTIAL_REDIS_URL")),
        ]
        started = time.monotonic()
        with ThreadPoolExecutor(len(cases) + len(inputs)) as pool:
            pairs_done = pool.map(lambda case: pair(cmds, *case[1:], logs), cases)
            inputs_done = pool.map(lambda case: same_input(cmds, case[1]), inputs)
            failures = list(pairs_done)
            input_failures = list(inputs_done)
        for (store, client, worker, _), failure in zip(cases, failures, strict=True):
            verdict = "ok" if failure is None else "FAILED"
            print(f"{store:5} {client:>10} client, {worker:<10} worker: {verdict}")
            if failure is not None:
                print(failure)
        for (store, _), failure in zip(inputs, input_failures, strict=True):
            verdict = "ok" if failure is None else "FAILED"
            print(f"{store:5} the same input written by each host: {verdict}")
            if failure is not None:
                print(failure)
        print(f"{len(cases)} pairs in {time.monotonic() - started:.1f} s")
        if any(failures) or any(input_failures):
            sys.exit(1)


if __name__ == "__main__":
    main()
