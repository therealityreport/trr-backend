#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

DESTRUCTIVE_SQL_RE = re.compile(r"\b(?:DROP|CREATE)\s+INDEX(?:\s+CONCURRENTLY)?\b", re.IGNORECASE)
DEFAULT_REVIEW_FILES = {
    "docs/workspace/unused-index-decision-matrix-2026-04-28.md",
    "docs/workspace/unused-index-keep-report-2026-04-28.md",
    "docs/workspace/unused-index-replacement-candidates-2026-04-28.md",
    "docs/workspace/unused-index-phase3-proposed-batches-2026-04-28.md",
    "docs/workspace/social-hashtag-leaderboard-architecture-2026-04-28.md",
    "docs/workspace/unused-index-owner-review-2026-04-28/README.md",
    "docs/workspace/unused-index-owner-review-2026-04-28/pipeline-owner-review.md",
    "docs/workspace/unused-index-owner-review-2026-04-28/admin-tooling-owner-review.md",
    "docs/workspace/unused-index-owner-review-2026-04-28/survey-public-app-owner-review.md",
    "docs/workspace/unused-index-owner-review-2026-04-28/screenalytics-ml-owner-review.md",
    "docs/workspace/unused-index-owner-review-2026-04-28/catalog-media-owner-review.md",
    "docs/workspace/unused-index-owner-review-2026-04-28/social-data-backfill-owner-review.md",
}


def _is_included(path: Path, root: Path) -> bool:
    rel = path.relative_to(root).as_posix()
    return rel in DEFAULT_REVIEW_FILES


def _is_allowed(path: Path, root: Path, allowed: set[str]) -> bool:
    rel = path.relative_to(root).as_posix()
    return rel in allowed


def _proposed_only(text: str) -> bool:
    lowered = text.lower()
    return "proposed" in lowered and "no sql is executed" in lowered


def _sql_context_matches(text: str) -> list[tuple[int, str]]:
    matches: list[tuple[int, str]] = []
    in_fence = False
    sql_fence = False
    for line_no, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("```"):
            if not in_fence:
                in_fence = True
                sql_fence = stripped.lower() in {"```sql", "```postgresql", "```psql"}
            else:
                in_fence = False
                sql_fence = False
            continue
        if stripped.startswith("|"):
            continue
        if in_fence and not sql_fence:
            continue
        candidate = stripped if in_fence else stripped.rstrip(";")
        if DESTRUCTIVE_SQL_RE.search(candidate) and (in_fence or DESTRUCTIVE_SQL_RE.match(candidate)):
            matches.append((line_no, DESTRUCTIVE_SQL_RE.search(candidate).group(0)))
    return matches


def scan(root: Path, allowed: set[str]) -> list[str]:
    errors: list[str] = []
    for rel_path in sorted(DEFAULT_REVIEW_FILES):
        path = root / rel_path
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        matches = _sql_context_matches(text)
        if not matches:
            continue
        if _is_allowed(path, root, allowed) and _proposed_only(text):
            continue
        rel = path.relative_to(root).as_posix()
        for line_no, sql in matches[:5]:
            errors.append(f"{rel}:{line_no}: found runnable-looking `{sql}`")
    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan unused-index review artifacts for destructive index SQL.")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--allow-proposed", action="append", default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    allowed = {Path(item).as_posix().lstrip("./") for item in args.allow_proposed}
    errors = scan(root, allowed)
    if errors:
        for error in errors:
            print(f"[destructive-sql-scan] ERROR: {error}", file=sys.stderr)
        return 1
    print("[destructive-sql-scan] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
