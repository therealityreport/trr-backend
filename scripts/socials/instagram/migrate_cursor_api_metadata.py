#!/usr/bin/env python3
"""Rename legacy cursor_api strategy metadata in social scrape rows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env


CONFIRM_APPLY = "MIGRATE CURSOR API METADATA"
OLD_VALUE = "cursor_api"
NEW_VALUE = "instagram_comments_endpoint_cursor"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run or migrate legacy Instagram comments cursor_api metadata.",
    )
    parser.add_argument("--apply", action="store_true", help="Update matching social.scrape_runs and scrape_jobs rows.")
    parser.add_argument("--confirm-apply", help=f"Required with --apply. Exact value: {CONFIRM_APPLY!r}.")
    parser.add_argument("--json", action="store_true", help="Emit JSON only.")
    parser.add_argument("--no-env", action="store_true", help="Skip loading .env.")
    return parser.parse_args(argv)


def fetch_counts() -> dict[str, int]:
    rows = pg.fetch_all(
        """
        select 'scrape_runs' as table_name, count(*)::bigint as row_count
        from social.scrape_runs
        where config::text like %s or coalesce(summary::text, '') like %s
        union all
        select 'scrape_jobs', count(*)::bigint
        from social.scrape_jobs
        where config::text like %s or coalesce(metadata::text, '') like %s
        """,
        [f"%{OLD_VALUE}%", f"%{OLD_VALUE}%", f"%{OLD_VALUE}%", f"%{OLD_VALUE}%"],
    )
    return {str(row["table_name"]): int(row["row_count"] or 0) for row in rows}


def apply_migration() -> None:
    pg.execute(
        """
        update social.scrape_runs
        set
          config = replace(config::text, %s, %s)::jsonb,
          summary = case
            when summary is null then null
            else replace(summary::text, %s, %s)::jsonb
          end
        where config::text like %s or coalesce(summary::text, '') like %s
        """,
        [OLD_VALUE, NEW_VALUE, OLD_VALUE, NEW_VALUE, f"%{OLD_VALUE}%", f"%{OLD_VALUE}%"],
    )
    pg.execute(
        """
        update social.scrape_jobs
        set
          config = replace(config::text, %s, %s)::jsonb,
          metadata = case
            when metadata is null then null
            else replace(metadata::text, %s, %s)::jsonb
          end
        where config::text like %s or coalesce(metadata::text, '') like %s
        """,
        [OLD_VALUE, NEW_VALUE, OLD_VALUE, NEW_VALUE, f"%{OLD_VALUE}%", f"%{OLD_VALUE}%"],
    )


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    before = fetch_counts()
    if args.apply and args.confirm_apply != CONFIRM_APPLY:
        return {
            "ok": False,
            "dry_run": False,
            "failure_reason": "confirm_apply_required",
            "confirm_required": CONFIRM_APPLY,
            "before": before,
        }
    if not args.apply:
        return {"ok": True, "dry_run": True, "before": before, "after": None}
    apply_migration()
    return {"ok": True, "dry_run": False, "before": before, "after": fetch_counts()}


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if not args.no_env:
        load_env()
    payload = build_payload(args)
    print(json.dumps(payload, indent=2 if args.json else None, sort_keys=True))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
