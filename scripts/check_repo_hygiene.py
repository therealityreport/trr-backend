#!/usr/bin/env python3
"""Fail when sensitive local-only artifacts are tracked in git."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DISALLOWED_TRACKED_PATHS = {"data/tiktok_cookies.json"}
DISALLOWED_TRACKED_PREFIXES = ("docs/ai/evidence/",)


def tracked_files(repo_root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files"],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def disallowed_tracked_files(paths: list[str]) -> list[str]:
    return sorted(
        path for path in paths if path in DISALLOWED_TRACKED_PATHS or path.startswith(DISALLOWED_TRACKED_PREFIXES)
    )


def main() -> int:
    disallowed = disallowed_tracked_files(tracked_files(REPO_ROOT))
    if not disallowed:
        return 0

    print("Disallowed tracked artifacts detected:", file=sys.stderr)
    for path in disallowed:
        print(f" - {path}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
