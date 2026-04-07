#!/usr/bin/env python3
"""Cancel wrong-strategy Instagram backfill jobs and optionally requeue a clean canary."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PRESET_DB_POOL_MINCONN = (os.environ.get("TRR_DB_POOL_MINCONN") or "").strip()
PRESET_DB_POOL_MAXCONN = (os.environ.get("TRR_DB_POOL_MAXCONN") or "").strip()

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._workspace_runtime_env import apply_workspace_runtime_env

load_dotenv()


def _apply_cli_db_pool_defaults() -> None:
    """Keep one-off remediation commands from starving the shared local DB lane."""

    if not PRESET_DB_POOL_MINCONN:
        os.environ["TRR_DB_POOL_MINCONN"] = "1"
    if not PRESET_DB_POOL_MAXCONN:
        os.environ["TRR_DB_POOL_MAXCONN"] = "1"


_apply_cli_db_pool_defaults()

ACTIVE_JOB_STATUSES = ("queued", "pending", "retrying", "running")


def _social_repo():
    from trr_backend.repositories import social_season_analytics as social_repo

    return social_repo


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="remediate_instagram_backfill_strategy_drift",
        description=(
            "Find active Instagram catalog backfill jobs that drifted into newest_first_frontier, "
            "cancel their runs, and optionally requeue a clean full-history backfill canary."
        ),
    )
    parser.add_argument("--account", required=True, help="Instagram account handle, for example bravotv")
    parser.add_argument("--source-scope", default="bravo", help="Source scope for requeue, default: bravo")
    parser.add_argument("--run-id", help="Optional run UUID to scope the search to a single run")
    parser.add_argument("--limit", type=int, default=25, help="Maximum candidate jobs to inspect, default: 25")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply cancellations. Without this flag the script only reports what it would do.",
    )
    parser.add_argument(
        "--requeue-canary",
        action="store_true",
        help="After cancellation, queue a fresh full-history backfill canary for the same account.",
    )
    parser.add_argument(
        "--include-gap-analysis",
        action="store_true",
        help=(
            "Run the heavier catalog gap-analysis query as part of the report. "
            "Disabled by default to avoid starving local admin/backend reads."
        ),
    )
    parser.add_argument("--cancelled-by", help="Audit label for cancellation metadata")
    parser.add_argument("--initiated-by", help="Audit label for the optional requeued canary")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print the JSON output")
    return parser.parse_args(argv if argv is not None else sys.argv[1:])


def _query_strategy_drift_jobs(*, account_handle: str, run_id: str | None, limit: int) -> list[dict[str, Any]]:
    social_repo = _social_repo()
    safe_limit = max(1, min(int(limit), 100))
    normalized_account = social_repo._normalize_social_account_profile_handle(account_handle)
    return social_repo.pg.fetch_all(
        """
        select
          j.id::text as job_id,
          j.run_id::text as run_id,
          j.job_type,
          j.status,
          j.created_at,
          j.config,
          j.metadata,
          r.status as run_status,
          r.config as run_config
        from social.scrape_jobs j
        join social.scrape_runs r on r.id = j.run_id
        where j.platform = 'instagram'
          and lower(coalesce(j.config->>'account', j.metadata->>'account', '')) = %s
          and j.status = any(%s::text[])
          and (%s::uuid is null or j.run_id = %s::uuid)
          and coalesce(r.config->>'pipeline_ingest_mode', '') = %s
          and coalesce(
                nullif(lower(coalesce(j.config->>'catalog_action', r.config->>'catalog_action', '')), ''),
                'backfill'
              ) = 'backfill'
          and lower(coalesce(j.config->>'partition_strategy', j.config->>'runner_strategy', '')) = %s
        order by j.created_at desc
        limit %s
        """,
        [
            normalized_account,
            list(ACTIVE_JOB_STATUSES),
            run_id,
            run_id,
            social_repo.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            social_repo.CATALOG_FULL_HISTORY_FRONTIER_STRATEGY,
            safe_limit,
        ],
    )


def _summarize_candidate_runs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    social_repo = _social_repo()
    summaries: dict[str, dict[str, Any]] = {}
    for row in rows:
        run_id = str(row.get("run_id") or "").strip()
        if not run_id:
            continue
        config = social_repo._metadata_dict(row.get("config"))
        run_config = social_repo._metadata_dict(row.get("run_config"))
        summary = summaries.setdefault(
            run_id,
            {
                "run_id": run_id,
                "run_status": str(row.get("run_status") or "").strip().lower() or None,
                "job_ids": [],
                "job_statuses": [],
                "job_types": [],
                "runner_strategy": str(config.get("runner_strategy") or "").strip().lower() or None,
                "partition_strategy": str(config.get("partition_strategy") or "").strip().lower() or None,
                "catalog_action": (
                    str(config.get("catalog_action") or run_config.get("catalog_action") or "").strip().lower()
                    or "backfill"
                ),
            },
        )
        summary["job_ids"].append(str(row.get("job_id") or "").strip())
        summary["job_statuses"].append(str(row.get("status") or "").strip().lower())
        summary["job_types"].append(str(row.get("job_type") or "").strip().lower())
    return list(summaries.values())


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    apply_workspace_runtime_env(repo_root=REPO_ROOT)
    social_repo = _social_repo()

    platform = "instagram"
    account_handle = social_repo._normalize_social_account_profile_handle(args.account)
    candidates = _query_strategy_drift_jobs(
        account_handle=account_handle,
        run_id=str(args.run_id or "").strip() or None,
        limit=args.limit,
    )
    candidate_runs = _summarize_candidate_runs(candidates)
    gap_analysis = (
        social_repo.get_social_account_catalog_gap_analysis(platform, account_handle)
        if args.include_gap_analysis
        else None
    )

    cancelled_runs: list[dict[str, Any]] = []
    requeued_canary: dict[str, Any] | None = None

    if args.execute:
        for run_summary in candidate_runs:
            cancelled_runs.append(
                social_repo.cancel_social_account_catalog_run(
                    platform=platform,
                    account_handle=account_handle,
                    run_id=run_summary["run_id"],
                    cancelled_by=args.cancelled_by,
                )
            )
        if args.requeue_canary:
            requeued_canary = social_repo.start_social_account_catalog_backfill(
                platform,
                account_handle,
                source_scope=args.source_scope,
                initiated_by=args.initiated_by,
                catalog_action="backfill",
                catalog_action_scope="full_history",
            )

    payload = {
        "platform": platform,
        "account_handle": account_handle,
        "dry_run": not args.execute,
        "candidate_job_count": len(candidates),
        "candidate_runs": candidate_runs,
        "gap_analysis": gap_analysis,
        "cancelled_runs": cancelled_runs,
        "requeued_canary": requeued_canary,
    }
    if args.pretty:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps(payload, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
