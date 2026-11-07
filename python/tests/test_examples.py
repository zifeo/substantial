"""The examples of `python/examples` run to their end, the first one again once ended."""

from __future__ import annotations

import importlib.util
import secrets
from pathlib import Path
from types import ModuleType

EXAMPLES = Path(__file__).parents[1] / "examples"


def load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"example_{name}", EXAMPLES / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_the_backup_of_i_4(tmp_path: Path) -> None:
    backup = load("backup")
    for url in [f"memory://backup-{secrets.token_hex(4)}", f"file:{tmp_path / 'backup'}"]:
        result = await backup.main(url)
        assert result["approved_by"] == ["alice", "bob"]
        assert result["check"] is True
        assert result["url"].startswith("s3://archive/backup-")


async def test_the_first_workflow_runs_again_once_ended(tmp_path: Path, capsys) -> None:
    first = load("first_workflow")
    for _ in range(2):
        await first.main(str(tmp_path / "demo"))
        assert "Final output 100" in capsys.readouterr().out
