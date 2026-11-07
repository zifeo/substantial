"""The backup of I-4, its step bodies stubbed, on a memory or file store:

    uv run python/examples/backup.py memory://backup
    uv run python/examples/backup.py file:/tmp/substantial-backup-py

Two approvals arrive at once, so the wait ends at the first replay that binds the handler; without
them, a day later, the rollback deletes the upload then the archive.
"""

import asyncio
import sys

import substantial as s


async def delete_archive(archive: str, files: list[str], tag: str) -> None:
    print(f"undo: delete {archive}")


async def delete_upload(url: str, archive: str, target: str) -> None:
    print(f"undo: delete {url}")


@s.step
async def list_files(source: str) -> list[str]:
    return [f"{source}/a.txt", f"{source}/b.txt"]


@s.step(timeout=s.minutes(10), undo=delete_archive)
async def make_archive(files: list[str], tag: str) -> str:
    return f"/tmp/backup-{tag}.tar"


@s.step(
    retry=s.Retry(max_attempts=5, initial=s.seconds(1), max=s.minutes(1), multiplier=2),
    undo=delete_upload,
)
async def upload(archive: str, target: str) -> str:
    return f"{target}/{archive.rsplit('/', 1)[-1]}"


@s.step
async def check_archive(archive: str) -> bool:
    return True


@s.workflow
async def verify(c: s.Context, archive: str) -> bool:
    return await check_archive(archive)


@s.workflow
async def backup(c: s.Context, source: str, target: str) -> dict:
    tag = await c.uuid()
    files = await list_files(source)
    archive = await make_archive(files, str(tag))
    url = await upload(archive, target)
    c.log.info("archived %d files", len(files))

    approvers: list[str] = []

    def approve(name: str) -> None:
        if name not in approvers:
            approvers.append(name)

    c.handle("approve", approve)
    if not await c.wait_until(lambda: len(approvers) >= 2, timeout=s.days(1)):
        await c.rollback("not approved within a day")

    check = await c.start(verify, {"archive": archive})
    return {"url": url, "approved_by": approvers, "check": await check.result()}


async def main(url: str) -> dict:
    async with (
        s.Substantial.create(url, partitions=1) as sub,
        sub.worker([backup, verify], preset="script"),
    ):
        run = await sub.start(
            backup, {"source": "/data", "target": "s3://archive"}, id="backup-2026-10-31"
        )
        await run.send("approve", "alice", id="approve-alice")
        await run.send("approve", "bob", id="approve-bob")
        return await run.result(timeout=s.minutes(1))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: backup.py <memory://ns | file:path>")
    print(asyncio.run(main(sys.argv[1])))
