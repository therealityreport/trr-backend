from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _tracked_files() -> list[str]:
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files"],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def test_repo_does_not_track_sensitive_artifacts() -> None:
    tracked = _tracked_files()

    disallowed = [
        path for path in tracked if path == "data/tiktok_cookies.json" or path.startswith("docs/ai/evidence/")
    ]

    assert disallowed == []
