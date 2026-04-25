#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

try:
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env


OPEN_JOB_STATUSES = {"queued", "pending", "retrying", "running", "cancelling"}


@dataclass(slots=True)
class CleanupPlan:
    run_id: str
    duplicate_open_job_ids: list[str]
    retry_job_ids: list[str]
    run_status: str
    active_jobs: int


def _normalize_run_id(run_id: str) -> str:
    normalized = str(run_id or "").strip()
    if not normalized:
        raise ValueError("run_id is required")
    return normalized


def _fetch_run(run_id: str) -> dict[str, object]:
    run = pg.fetch_one(
        """
        select
          id::text as id,
          status,
          total_jobs,
          active_jobs
        from social.scrape_runs
        where id::text = %s
        """,
        [run_id],
    )
    if not run:
        raise ValueError(f"social.scrape_runs row not found for run_id={run_id}")
    return run


def _fetch_open_jobs(run_id: str) -> list[dict[str, object]]:
    return pg.fetch_all(
        """
        select
          id::text as id,
          status,
          job_type,
          last_error_code
        from social.scrape_jobs
        where run_id::text = %s
          and status = any(%s)
        order by created_at asc, id asc
        """,
        [run_id, sorted(OPEN_JOB_STATUSES)],
    )


def _identify_duplicate_open_job_ids(open_jobs: list[dict[str, object]]) -> list[str]:
    seen_job_types: set[str] = set()
    duplicate_ids: list[str] = []
    for job in open_jobs:
        job_type = str(job.get("job_type") or "").strip()
        job_id = str(job.get("id") or "").strip()
        if not job_type or not job_id:
            continue
        if job_type in seen_job_types:
            duplicate_ids.append(job_id)
            continue
        seen_job_types.add(job_type)
    return duplicate_ids


def _identify_retry_job_ids(open_jobs: list[dict[str, object]]) -> list[str]:
    return [
        str(job.get("id") or "").strip()
        for job in open_jobs
        if str(job.get("status") or "").strip() == "retrying" and str(job.get("id") or "").strip()
    ]


def plan_run_cleanup(run_id: str) -> CleanupPlan:
    normalized_run_id = _normalize_run_id(run_id)
    run = _fetch_run(normalized_run_id)
    open_jobs = _fetch_open_jobs(normalized_run_id)
    duplicate_open_job_ids = _identify_duplicate_open_job_ids(open_jobs)
    retry_job_ids = _identify_retry_job_ids(open_jobs)
    active_jobs = max(0, len(open_jobs) - len(duplicate_open_job_ids))

    return CleanupPlan(
        run_id=str(run.get("id") or normalized_run_id),
        duplicate_open_job_ids=duplicate_open_job_ids,
        retry_job_ids=retry_job_ids,
        run_status=str(run.get("status") or ""),
        active_jobs=active_jobs,
    )


def execute_run_cleanup(run_id: str) -> CleanupPlan:
    plan = plan_run_cleanup(run_id)
    if plan.duplicate_open_job_ids:
        pg.execute(
            """
            update social.scrape_jobs
            set
              status = 'cancelled',
              error_message = 'duplicate_open_job_cancelled_by_stale_run_reconciler',
              last_error_code = 'duplicate_open_job_cancelled',
              last_error_class = 'DuplicateOpenJobCancelled',
              metadata = coalesce(metadata, '{}'::jsonb) || %s::jsonb
            where run_id::text = %s
              and id::text = any(%s)
              and status = any(%s)
            """,
            [
                json.dumps({"stale_run_reconciler": {"reason": "duplicate_open_job"}}),
                plan.run_id,
                plan.duplicate_open_job_ids,
                sorted(OPEN_JOB_STATUSES),
            ],
        )

    pg.execute(
        """
        update social.scrape_runs
        set
          active_jobs = (
            select count(*)::int
            from social.scrape_jobs
            where run_id::text = %s
              and status = any(%s)
          ),
          summary = coalesce(summary, '{}'::jsonb) || %s::jsonb,
          updated_at = now()
        where id::text = %s
        """,
        [
            plan.run_id,
            sorted(OPEN_JOB_STATUSES),
            json.dumps(
                {
                    "stale_run_reconciler": {
                        "duplicate_open_job_ids": plan.duplicate_open_job_ids,
                        "retry_job_ids": plan.retry_job_ids,
                    }
                }
            ),
            plan.run_id,
        ],
    )
    return plan


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="reconcile_stale_social_run",
        description="Dry-run or execute cleanup for a stale social scrape run.",
    )
    parser.add_argument("run_id", help="social.scrape_runs id to inspect")
    parser.add_argument("--execute", action="store_true", help="Cancel duplicate open jobs and update the run summary.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = _parse_args(argv or sys.argv[1:])
    plan = execute_run_cleanup(args.run_id) if args.execute else plan_run_cleanup(args.run_id)
    print(json.dumps({"dry_run": not args.execute, "cleanup": asdict(plan)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
