#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_backend_modules() -> tuple[Any, Any, Any]:
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as social_repo
    from trr_backend.utils.env import load_env

    return pg, social_repo, load_env


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a RHOSLC-seasons-only Threads full refresh (posts + comments) and execute synchronously.",
    )
    parser.add_argument(
        "--season-number",
        dest="season_numbers",
        action="append",
        type=int,
        help="Optional season number filter. Repeat for multiple values.",
    )
    parser.add_argument(
        "--season-id",
        dest="season_ids",
        action="append",
        help="Optional season UUID filter. Repeat for multiple values.",
    )
    parser.add_argument(
        "--source-scope",
        default="bravo",
        choices=["bravo", "creator", "community"],
        help="Target source scope (default: bravo).",
    )
    parser.add_argument(
        "--account",
        dest="accounts",
        action="append",
        help="Optional Threads account handle filter (repeatable, e.g. bravotv).",
    )
    parser.add_argument(
        "--max-comments-per-post",
        type=int,
        default=100000,
        help="Max comments/replies+quotes fetched per post (default: 100000).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve and print target RHOSLC seasons without enqueueing ingest runs.",
    )
    return parser.parse_args()


def _load_rhoslc_seasons(
    *,
    pg: Any,
    season_ids: set[str],
    season_numbers: set[int],
) -> list[dict[str, Any]]:
    clauses = [
        (
            "(\n"
            "  lower(coalesce(to_jsonb(sh) ->> 'slug', to_jsonb(sh) ->> 'canonical_slug', '')) = 'rhoslc'\n"
            "  or lower(coalesce(sh.name, '')) like '%%salt lake city%%'\n"
            ")"
        ),
    ]
    params: list[Any] = []
    if season_ids:
        clauses.append("s.id::text = any(%s)")
        params.append(sorted(season_ids))
    if season_numbers:
        clauses.append("s.season_number = any(%s)")
        params.append(sorted(season_numbers))

    where_clause = " and ".join(clauses)
    return pg.fetch_all(
        f"""
        select
          s.id::text as season_id,
          s.season_number,
          sh.id::text as show_id,
          sh.name as show_name,
          coalesce(to_jsonb(sh) ->> 'slug', to_jsonb(sh) ->> 'canonical_slug') as show_slug
        from core.seasons s
        join core.shows sh on sh.id = s.show_id
        where {where_clause}
        order by s.season_number asc, s.id asc
        """,
        params,
    )


def _normalize_account(value: Any) -> str:
    return str(value or "").strip().lower().lstrip("@")


def _threads_target_accounts_for_season(*, social_repo: Any, season_id: str, source_scope: str) -> list[str]:
    targets_payload = social_repo.get_targets(season_id, source_scope=source_scope)
    accounts: list[str] = []
    for target in targets_payload.get("targets", []):
        if not target.get("is_active", True):
            continue
        if str(target.get("platform") or "").strip().lower() != "threads":
            continue
        accounts.extend([_normalize_account(item) for item in (target.get("accounts") or [])])
    return sorted({account for account in accounts if account})


def _cancel_non_matching_account_jobs(*, pg: Any, run_id: str, allowed_accounts: set[str]) -> int:
    if not run_id or not allowed_accounts:
        return 0
    placeholders = ",".join(["%s"] * len(allowed_accounts))
    rows = pg.fetch_all(
        f"""
        update social.scrape_jobs j
        set
          status = 'cancelled',
          started_at = coalesce(j.started_at, now()),
          completed_at = now(),
          metadata = coalesce(j.metadata, '{{}}'::jsonb) || jsonb_build_object(
            'cancel_reason',
            'account_filter',
            'cancelled_by',
            'script:run_rhoslc_threads_full_refresh'
          )
        where j.run_id = %s::uuid
          and lower(coalesce(j.platform, '')) = 'threads'
          and lower(coalesce(j.status, '')) in ('queued', 'pending')
          and ltrim(lower(coalesce(j.account, '')), '@') <> all(array[{placeholders}]::text[])
        returning j.id::text as id
        """,
        [run_id, *sorted(allowed_accounts)],
    )
    return len(rows)


def main() -> int:
    pg, social_repo, load_env = _load_backend_modules()
    load_env()
    args = _parse_args()

    season_ids = {str(item).strip() for item in (args.season_ids or []) if str(item).strip()}
    season_numbers = {int(item) for item in (args.season_numbers or []) if isinstance(item, int)}
    account_filters = {_normalize_account(item) for item in (args.accounts or []) if _normalize_account(item)}

    seasons = _load_rhoslc_seasons(pg=pg, season_ids=season_ids, season_numbers=season_numbers)
    if not seasons:
        print(
            json.dumps(
                {
                    "source_scope": args.source_scope,
                    "account_filters": sorted(account_filters),
                    "dry_run": bool(args.dry_run),
                    "seasons": [],
                    "runs": [],
                    "message": "No RHOSLC seasons matched filters.",
                }
            )
        )
        return 0

    summary: dict[str, Any] = {
        "source_scope": args.source_scope,
        "account_filters": sorted(account_filters),
        "max_comments_per_post": int(max(0, args.max_comments_per_post)),
        "dry_run": bool(args.dry_run),
        "seasons": [
            {
                "season_id": row.get("season_id"),
                "season_number": row.get("season_number"),
                "show_id": row.get("show_id"),
                "show_slug": row.get("show_slug"),
                "show_name": row.get("show_name"),
            }
            for row in seasons
        ],
        "runs": [],
        "skipped": [],
    }

    if args.dry_run:
        print(json.dumps(summary))
        return 0

    for row in seasons:
        season_id = str(row.get("season_id") or "").strip()
        if not season_id:
            continue
        target_accounts = _threads_target_accounts_for_season(
            social_repo=social_repo,
            season_id=season_id,
            source_scope=args.source_scope,
        )
        selected_accounts = [
            account for account in target_accounts if not account_filters or account in account_filters
        ]
        if account_filters and not selected_accounts:
            summary["skipped"].append(
                {
                    "season_id": season_id,
                    "season_number": row.get("season_number"),
                    "reason": "no_matching_thread_targets_for_account_filters",
                    "target_accounts": target_accounts,
                }
            )
            continue
        ingest_result = social_repo.ingest_season(
            season_id,
            platforms=["threads"],
            source_scope=args.source_scope,
            sync_strategy="full_refresh",
            max_posts_per_target=0,
            max_comments_per_post=max(0, int(args.max_comments_per_post)),
            max_replies_per_post=max(0, int(args.max_comments_per_post)),
            fetch_replies=True,
            ingest_mode="posts_and_comments",
            date_start=None,
            date_end=None,
            comment_refresh_policy=social_repo.DEFAULT_COMMENT_REFRESH_POLICY,
            comment_anchor_source_ids=None,
            initiated_by="script:run_rhoslc_threads_full_refresh",
            inline_worker_id=None,
        )
        run_id = str(ingest_result.get("run_id") or "").strip()
        run_payload: dict[str, Any] = {
            "season_id": season_id,
            "season_number": row.get("season_number"),
            "target_accounts": target_accounts,
            "selected_accounts": selected_accounts,
            "run_id": run_id or None,
            "ingest": ingest_result,
        }
        if run_id:
            if account_filters:
                run_payload["cancelled_jobs_for_account_filter"] = _cancel_non_matching_account_jobs(
                    pg=pg,
                    run_id=run_id,
                    allowed_accounts=set(selected_accounts),
                )
            run_payload["executed"] = social_repo.execute_run(run_id, platform="threads")
        summary["runs"].append(run_payload)

    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
