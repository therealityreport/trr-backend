#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from typing import Any

from trr_backend.db import pg
from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.utils.env import load_env

TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill Instagram Reel views across full history using details_refresh per account."
    )
    parser.add_argument(
        "--season-id",
        action="append",
        default=[],
        help="Season UUID to backfill. Repeat to include multiple seasons.",
    )
    parser.add_argument(
        "--source-scope",
        default="bravo",
        choices=["bravo", "creator", "community"],
        help="Target source scope (default: bravo).",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Poll queued runs until terminal and print diagnostics.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=5.0,
        help="Polling interval when --wait is enabled (default: 5).",
    )
    parser.add_argument(
        "--poll-timeout-seconds",
        type=int,
        default=3600,
        help="Per-run timeout when --wait is enabled (default: 3600).",
    )
    parser.add_argument(
        "--initiated-by",
        default="script:backfill_instagram_reel_views_full_history",
        help="initiated_by value recorded on runs/jobs.",
    )
    return parser.parse_args()


def _discover_active_seasons(*, source_scope: str) -> list[str]:
    rows = pg.fetch_all(
        """
        select distinct season_id::text as season_id
        from social.season_targets
        where source_scope = %s
          and platform = 'instagram'
          and is_active = true
        order by season_id::text
        """,
        [source_scope],
    )
    return [str(row.get("season_id") or "").strip() for row in rows if str(row.get("season_id") or "").strip()]


def _season_instagram_accounts(*, season_id: str, source_scope: str) -> list[str]:
    by_platform = social_repo._target_accounts_by_platform(  # noqa: SLF001
        season_id,
        source_scope=source_scope,
        context=None,
    )
    accounts = sorted(by_platform.get("instagram") or [])
    return [str(account or "").strip() for account in accounts if str(account or "").strip()]


def _run_status(*, season_id: str, run_id: str) -> str:
    runs = social_repo.list_runs(season_id, limit=1, run_id=run_id)
    if not runs:
        return "missing"
    return str((runs[0] or {}).get("status") or "").strip().lower() or "unknown"


def _collect_run_diagnostics(*, season_id: str, run_id: str) -> dict[str, Any]:
    jobs = social_repo.list_jobs(season_id, run_id=run_id, limit=250)
    diagnostics: dict[str, Any] = {
        "run_id": run_id,
        "job_count": len(jobs),
        "posts_scanned": 0,
        "views_updated": 0,
        "views_preserved_missing": 0,
        "details_refresh_errors": 0,
        "job_status_counts": {},
        "views_sources": {},
        "failures_by_reason": {},
    }
    status_counts: Counter[str] = Counter()
    views_sources: Counter[str] = Counter()
    failures_by_reason: Counter[str] = Counter()

    for job in jobs:
        status_counts[str(job.get("status") or "unknown").strip().lower() or "unknown"] += 1
        metadata = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
        retrieval_meta = metadata.get("retrieval_meta") if isinstance(metadata.get("retrieval_meta"), dict) else {}
        scrape_counters = (
            retrieval_meta.get("scrape_counters")
            if isinstance(retrieval_meta.get("scrape_counters"), dict)
            else {}
        )
        diagnostics["posts_scanned"] += int(scrape_counters.get("posts") or 0)
        diagnostics["views_updated"] += int(retrieval_meta.get("details_refresh_views_updated") or 0)
        diagnostics["views_preserved_missing"] += int(
            retrieval_meta.get("details_refresh_views_preserved_missing") or 0
        )
        diagnostics["details_refresh_errors"] += int(retrieval_meta.get("details_refresh_errors") or 0)
        details_sources = (
            retrieval_meta.get("details_refresh_views_sources")
            if isinstance(retrieval_meta.get("details_refresh_views_sources"), dict)
            else {}
        )
        for source, count in details_sources.items():
            source_key = str(source or "").strip() or "unknown"
            views_sources[source_key] += int(count or 0)
        fail_reasons = (
            retrieval_meta.get("comment_fetch_failures_by_reason")
            if isinstance(retrieval_meta.get("comment_fetch_failures_by_reason"), dict)
            else {}
        )
        for reason, count in fail_reasons.items():
            reason_key = str(reason or "").strip() or "unknown"
            failures_by_reason[reason_key] += int(count or 0)

    diagnostics["job_status_counts"] = dict(status_counts)
    diagnostics["views_sources"] = dict(views_sources)
    diagnostics["failures_by_reason"] = dict(failures_by_reason)
    return diagnostics


def main() -> int:
    load_env()
    args = _parse_args()
    season_ids = [str(item or "").strip() for item in (args.season_id or []) if str(item or "").strip()]
    if not season_ids:
        season_ids = _discover_active_seasons(source_scope=args.source_scope)
    if not season_ids:
        print("No eligible seasons found for instagram full-history reel view backfill.")
        return 0

    queue_enabled = social_repo.is_queue_enabled()
    if queue_enabled:
        social_repo.assert_worker_available_when_queue_enabled()

    launched_runs: list[dict[str, Any]] = []
    for season_id in season_ids:
        accounts = _season_instagram_accounts(season_id=season_id, source_scope=args.source_scope)
        if not accounts:
            print(f"Skipping season {season_id}: no active instagram accounts.")
            continue
        for account in accounts:
            payload = social_repo.ingest_season(
                season_id,
                platforms=["instagram"],
                accounts_override=[account],
                hashtags_override=[],
                keywords_override=[],
                source_scope=args.source_scope,
                max_posts_per_target=0,
                max_comments_per_post=0,
                max_replies_per_post=0,
                fetch_replies=False,
                ingest_mode="details_refresh",
                sync_strategy="full_refresh",
                comment_refresh_policy="balanced",
                comment_anchor_source_ids=None,
                date_start=None,
                date_end=None,
                initiated_by=args.initiated_by,
                inline_worker_id=None if queue_enabled else "script-instagram-reel-views-backfill",
            )
            run_id = str(payload.get("run_id") or "")
            execution_mode = str(payload.get("execution_mode") or "")
            if not queue_enabled and run_id:
                # Ensure non-queue environments actually execute and complete the run.
                social_repo.execute_run(run_id, worker_id="script-instagram-reel-views-backfill", platform="instagram")
                execution_mode = execution_mode or "inline"
            launched_runs.append(
                {
                    "season_id": season_id,
                    "account": account,
                    "run_id": run_id,
                    "queued_or_started_jobs": int(payload.get("queued_or_started_jobs") or 0),
                    "execution_mode": execution_mode,
                }
            )

    if not args.wait:
        print(json.dumps({"launched": launched_runs}, indent=2, sort_keys=True))
        return 0

    completed: list[dict[str, Any]] = []
    for launched in launched_runs:
        season_id = str(launched.get("season_id") or "")
        run_id = str(launched.get("run_id") or "")
        if not season_id or not run_id:
            continue
        started_at = time.monotonic()
        status = _run_status(season_id=season_id, run_id=run_id)
        timed_out = False
        while status not in TERMINAL_STATUSES:
            if time.monotonic() - started_at > max(1, int(args.poll_timeout_seconds)):
                timed_out = True
                break
            time.sleep(max(1.0, float(args.poll_interval_seconds)))
            status = _run_status(season_id=season_id, run_id=run_id)
        if timed_out and status not in TERMINAL_STATUSES:
            social_repo.cancel_run(season_id, run_id, cancelled_by=f"{args.initiated_by}:poll-timeout")
            status = _run_status(season_id=season_id, run_id=run_id)
        diagnostics = _collect_run_diagnostics(season_id=season_id, run_id=run_id)
        diagnostics["season_id"] = season_id
        diagnostics["account"] = str(launched.get("account") or "")
        diagnostics["execution_mode"] = str(launched.get("execution_mode") or "")
        diagnostics["run_status"] = status
        diagnostics["timed_out"] = timed_out
        completed.append(diagnostics)

    print(json.dumps({"completed_runs": completed}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
