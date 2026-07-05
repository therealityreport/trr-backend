#!/usr/bin/env python3
"""Repair escaped Reddit media mirror URLs and normalize hosted statuses."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._workspace_runtime_env import apply_workspace_runtime_env  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--period-key", help="Repair media rows attached to this Reddit period key.")
    scope.add_argument("--run-id", help="Repair media rows attached to this Reddit refresh run id.")
    parser.add_argument("--run", action="store_true", help="Apply changes. Default is dry-run.")
    parser.add_argument("--retry", action="store_true", help="Retry mirrors for repaired source URLs.")
    parser.add_argument(
        "--normalize-hosted-status",
        action="store_true",
        help="Mark rows with hosted_url and hosted_key as mirrored.",
    )
    parser.add_argument("--limit", type=int, default=500, help="Maximum rows to inspect.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser


def _scope_clause(args: argparse.Namespace) -> tuple[str, list[Any]]:
    if args.period_key:
        return "m.period_key = %s", [str(args.period_key).strip()]
    return "m.run_id = %s::uuid", [str(args.run_id).strip()]


def _row_actions(row: dict[str, Any], *, clean_url: str, normalize_hosted_status: bool) -> list[str]:
    actions: list[str] = []
    if clean_url and clean_url != str(row.get("source_url") or ""):
        actions.append("clean_source_url")
    if normalize_hosted_status and row.get("hosted_url") and row.get("hosted_key") and row.get("status") != "mirrored":
        actions.append("normalize_hosted_status")
    return actions


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    load_dotenv(REPO_ROOT / ".env", override=False)
    apply_workspace_runtime_env(repo_root=REPO_ROOT)

    from trr_backend.db import pg
    from trr_backend.media.s3_mirror import mirror_url_to_s3
    from trr_backend.repositories import reddit_refresh

    scope_sql, scope_params = _scope_clause(args)
    limit = max(1, min(10_000, int(args.limit or 500)))
    rows = pg.fetch_all(
        f"""
        select
               mm.id::text,
               mm.reddit_post_id,
               mm.reddit_comment_id,
               mm.source_url,
               mm.media_type,
               mm.hosted_key,
               mm.hosted_url,
               mm.sha256,
               mm.size_bytes,
               mm.content_type,
               mm.status,
               mm.error_message,
               mm.created_at
        from social.reddit_media_mirrors mm
        join social.reddit_period_post_matches m
          on m.reddit_post_id = mm.reddit_post_id
        where {scope_sql}
          and (
            mm.source_url like '%%&amp;%%'
            or mm.source_url like '%%<%%'
            or mm.status = 'failed'
            or (%s and mm.hosted_url is not null and mm.hosted_key is not null and mm.status <> 'mirrored')
          )
        order by mm.status, mm.created_at
        limit %s
        """,
        [*scope_params, bool(args.normalize_hosted_status), limit],
    )

    items: list[dict[str, Any]] = []
    changed = 0
    retried = 0
    retry_succeeded = 0
    for row in rows:
        clean_url = reddit_refresh._sanitize_reddit_media_url(row.get("source_url"))  # noqa: SLF001
        actions = _row_actions(row, clean_url=clean_url, normalize_hosted_status=bool(args.normalize_hosted_status))
        retry_result = None

        if args.run and args.retry and clean_url and (
            clean_url != str(row.get("source_url") or "") or str(row.get("status") or "") == "failed"
        ):
            retried += 1
            result = mirror_url_to_s3(clean_url)
            retry_status = result.status if result.status in {"mirrored", "skipped"} else "failed"
            retry_succeeded += 1 if result.hosted_url and retry_status in {"mirrored", "skipped"} else 0
            retry_result = {
                "status": retry_status,
                "error_message": result.error,
                "hosted_url": result.hosted_url,
            }
            pg.fetch_one(
                """
                update social.reddit_media_mirrors
                set source_url = %s,
                    hosted_key = %s,
                    hosted_url = %s,
                    sha256 = %s,
                    size_bytes = %s,
                    content_type = %s,
                    status = %s,
                    error_message = %s,
                    updated_at = now()
                where id = %s::uuid
                returning id
                """,
                [
                    clean_url,
                    result.hosted_key,
                    result.hosted_url,
                    result.sha256,
                    result.size_bytes,
                    result.content_type,
                    retry_status,
                    result.error,
                    row["id"],
                ],
            )
            changed += 1
        elif args.run and actions:
            next_status = "mirrored" if "normalize_hosted_status" in actions else row.get("status")
            next_error = None if "normalize_hosted_status" in actions else row.get("error_message")
            pg.fetch_one(
                """
                update social.reddit_media_mirrors
                set source_url = %s,
                    status = %s,
                    error_message = %s,
                    updated_at = now()
                where id = %s::uuid
                returning id
                """,
                [clean_url or row.get("source_url"), next_status, next_error, row["id"]],
            )
            changed += 1

        items.append(
            {
                "id": row.get("id"),
                "reddit_post_id": row.get("reddit_post_id"),
                "status": row.get("status"),
                "error_message": row.get("error_message"),
                "actions": actions,
                "retry_result": retry_result,
            }
        )

    payload = {
        "dry_run": not args.run,
        "inspected": len(rows),
        "changed": changed,
        "retried": retried,
        "retry_succeeded": retry_succeeded,
        "items": items,
    }
    print(json.dumps(payload, indent=2 if args.pretty else None, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
