"""Runs the workflow of dev/cross between every pair of hosts: a worker in one language serves
the runs a client in another starts, messages and awaits, through one shared store. Uses a
`file:` store, and an S3 one too when `SUBSTANTIAL_S3_URL` is set (see dev/rustfs.compose.yaml).

    uv run dev/cross/run.py

Needs cargo, uv and node 24 on the PATH, and `npm ci` done. Exits 1 on any mismatch.
"""

import json
import os
import secrets
import signal
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import permutations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CLIENT_TIMEOUT_S = 90
SHUTDOWN_TIMEOUT_S = 30


def output(*cmd: str) -> str:
    return subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True).stdout


def commands() -> dict[str, list[str]]:
    subprocess.run(
        ["cargo", "build", "-q", "-p", "substantial", "--example", "cross"], cwd=ROOT, check=True
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


def s3_url(url: str) -> str:
    """A fresh namespace under the one `url` names."""
    path, _, query = url.partition("?")
    if not query:
        sys.exit(f"SUBSTANTIAL_S3_URL has no query: {url!r}")
    return f"{path}/cross-{secrets.token_hex(8)}?{query}"


def pair(cmds: dict[str, list[str]], client: str, worker: str, url: str, logs: Path) -> str | None:
    """Runs one pair; the reason it failed, or None."""
    log = logs / f"{worker}-worker-{secrets.token_hex(4)}.log"
    with log.open("w") as out:
        served = subprocess.Popen(
            [*cmds[worker], "worker", url], cwd=ROOT, stdout=out, stderr=subprocess.STDOUT
        )
    try:
        ran = subprocess.run(
            [*cmds[client], "client", url],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=CLIENT_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as e:
        ran = None
        failure = f"the client did not end within {CLIENT_TIMEOUT_S} s\n{e.stderr!r}"
    served.send_signal(signal.SIGTERM)
    try:
        code = served.wait(SHUTDOWN_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        served.kill()
        code = None
    worker_log = f"\n--- {worker} worker ---\n{log.read_text()}"
    if ran is None:
        return failure + worker_log
    if ran.returncode != 0:
        return f"the client exited with {ran.returncode}\n{ran.stdout}{ran.stderr}{worker_log}"
    expected = {
        "result": {"greeting": "hello ada", "suffix": "!!!", "worker": worker},
        "status": "completed",
        "client": client,
    }
    try:
        got = json.loads(ran.stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        got = None
    if got != expected:
        return f"expected {json.dumps(expected)}\n     got {ran.stdout}{ran.stderr}{worker_log}"
    if code != 0:
        return f"the worker did not shut down cleanly on SIGTERM: {code}{worker_log}"
    return None


def main() -> None:
    cmds = commands()
    with tempfile.TemporaryDirectory(prefix="substantial-cross-") as tmp:
        logs = Path(tmp)
        pairs = list(permutations(cmds, 2))
        cases = [("file", c, w, f"file:{logs / f'{c}-{w}'}") for c, w in pairs]
        s3 = os.environ.get("SUBSTANTIAL_S3_URL")
        if s3 is None:
            print("skipping the S3 store: SUBSTANTIAL_S3_URL is not set")
        else:
            cases += [("s3", c, w, s3_url(s3)) for c, w in pairs]
        started = time.monotonic()
        with ThreadPoolExecutor(len(cases)) as pool:
            failures = list(pool.map(lambda case: pair(cmds, *case[1:], logs), cases))
        for (store, client, worker, _), failure in zip(cases, failures, strict=True):
            verdict = "ok" if failure is None else "FAILED"
            print(f"{store:5} {client:>10} client, {worker:<10} worker: {verdict}")
            if failure is not None:
                print(failure)
        print(f"{len(cases)} pairs in {time.monotonic() - started:.1f} s")
        if any(failures):
            sys.exit(1)


if __name__ == "__main__":
    main()
