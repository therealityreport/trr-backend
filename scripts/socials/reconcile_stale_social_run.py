#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
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
ACTIVE_RUN_STATUSES = {"queued", "pending", "retrying", "running", "cancelling"}


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
          config,
          metadata,
          last_error_code
        from social.scrape_jobs
        where run_id::text = %s
          and status = any(%s)
        order by created_at asc, id asc
        """,
        [run_id, sorted(OPEN_JOB_STATUSES)],
    )


def _json_fingerprint(value: object) -> str:
    return json.dumps(
        {"type": type(value).__name__, "value": value},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _duplicate_group_key(job: dict[str, object]) -> tuple[str, str, str]:
    job_type = str(job.get("job_type") or "").strip()
    return job_type, _json_fingerprint(job.get("config")), _json_fingerprint(job.get("metadata"))


def _identify_duplicate_open_job_ids(open_jobs: list[dict[str, object]]) -> list[str]:
    seen_keys: set[tuple[str, str, str]] = set()
    duplicate_ids: list[str] = []
    for job in open_jobs:
        duplicate_key = _duplicate_group_key(job)
        job_id = str(job.get("id") or "").strip()
        if not duplicate_key[0] or not job_id:
            continue
        if duplicate_key in seen_keys:
            duplicate_ids.append(job_id)
            continue
        seen_keys.add(duplicate_key)
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


def _fetch_remaining_job_counts(run_id: str, *, conn: object | None = None) -> dict[str, int]:
    row = pg.fetch_one(
        """
        select
          count(*) filter (where status = any(%s))::int as open_jobs,
          count(*) filter (where status = 'failed')::int as failed_jobs
        from social.scrape_jobs
        where run_id::text = %s
        """,
        [sorted(OPEN_JOB_STATUSES), run_id],
        conn=conn,
    )
    if not row:
        return {"open_jobs": 0, "failed_jobs": 0}
    return {
        "open_jobs": _normalize_non_negative_int(row.get("open_jobs")),
        "failed_jobs": _normalize_non_negative_int(row.get("failed_jobs")),
    }


def _fetch_summary_counters(run_id: str, *, conn: object | None = None) -> dict[str, object]:
    row = pg.fetch_one(
        """
        with job_rows as (
          select
            status,
            coalesce(config->>'stage', metadata->>'stage', job_type, 'unknown') as stage
          from social.scrape_jobs
          where run_id::text = %s
        ),
        stage_rows as (
          select
            stage,
            jsonb_build_object(
              'total', count(*)::int,
              'completed', count(*) filter (where status = 'completed')::int,
              'failed', count(*) filter (where status = 'failed')::int,
              'active', count(*) filter (where status = any(%s))::int
            ) as counters
          from job_rows
          group by stage
        )
        select
          count(*) filter (where status = any(%s))::int as active_jobs,
          count(*) filter (where status = 'completed')::int as completed_jobs,
          count(*) filter (where status = 'failed')::int as failed_jobs,
          coalesce(
            (select jsonb_object_agg(stage, counters) from stage_rows),
            '{}'::jsonb
          ) as stage_counts
        from job_rows
        """,
        [run_id, sorted(OPEN_JOB_STATUSES), sorted(OPEN_JOB_STATUSES)],
        conn=conn,
    )
    if not row:
        return {"active_jobs": 0, "completed_jobs": 0, "failed_jobs": 0, "stage_counts": {}}
    return {
        "active_jobs": _normalize_non_negative_int(row.get("active_jobs")),
        "completed_jobs": _normalize_non_negative_int(row.get("completed_jobs")),
        "failed_jobs": _normalize_non_negative_int(row.get("failed_jobs")),
        "stage_counts": row.get("stage_counts") if isinstance(row.get("stage_counts"), dict) else {},
    }


def _fetch_current_run_status(run_id: str, *, conn: object | None = None) -> str:
    row = (
        pg.fetch_one(
            """
            select status
            from social.scrape_runs
            where id::text = %s
            """,
            [run_id],
            conn=conn,
        )
        or {}
    )
    return str(row.get("status") or "").strip()


def _run_lock_key(run_id: str) -> int:
    return int(hashlib.md5(run_id.encode()).hexdigest()[:15], 16) % (2**31)


def _normalize_non_negative_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def execute_run_cleanup(run_id: str) -> CleanupPlan:
    plan = plan_run_cleanup(run_id)
    with pg.advisory_session_lock(_run_lock_key(plan.run_id), label="stale-social-run-reconciler") as lock_conn:
        if plan.duplicate_open_job_ids:
            pg.execute(
                """
                update social.scrape_jobs
                set
                  status = 'cancelled',
                  error_message = 'duplicate_open_job_cancelled_by_stale_run_reconciler',
                  last_error_code = 'duplicate_open_job_cancelled',
                  last_error_class = 'DuplicateOpenJobCancelled',
                  completed_at = coalesce(completed_at, now()),
                  heartbeat_at = now(),
                  worker_id = null,
                  claimed_at = null,
                  metadata = (
                    case when jsonb_typeof(metadata) = 'object' then metadata else '{}'::jsonb end
                  ) || %s::jsonb
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
                conn=lock_conn,
            )

        remaining_counts = _fetch_remaining_job_counts(plan.run_id, conn=lock_conn)
        remaining_open_jobs = remaining_counts["open_jobs"]
        remaining_failed_jobs = remaining_counts["failed_jobs"]
        summary_counters = _fetch_summary_counters(plan.run_id, conn=lock_conn)
        current_run_status = _fetch_current_run_status(plan.run_id, conn=lock_conn)
        cleanup_metadata = {
            **summary_counters,
            "stale_run_reconciler": {
                "duplicate_open_job_ids": plan.duplicate_open_job_ids,
                "retry_job_ids": plan.retry_job_ids,
                "remaining_open_jobs": remaining_open_jobs,
                "remaining_failed_jobs": remaining_failed_jobs,
                "terminalized": current_run_status in ACTIVE_RUN_STATUSES and remaining_open_jobs == 0,
            },
        }
        pg.execute(
            """
            update social.scrape_runs
            set
              active_jobs = %s,
              completed_jobs = %s,
              failed_jobs = %s,
              stage_counts = %s::jsonb,
              status = case
                when %s = any(%s) and %s = 0 and %s > 0 then 'failed'
                when %s = any(%s) and %s = 0 then 'completed'
                else status
              end,
              completed_at = case
                when %s = any(%s) and %s = 0 then coalesce(completed_at, now())
                else completed_at
              end,
              summary = coalesce(summary, '{}'::jsonb) || %s::jsonb,
              updated_at = now()
            where id::text = %s
            """,
            [
                _normalize_non_negative_int(summary_counters.get("active_jobs")),
                _normalize_non_negative_int(summary_counters.get("completed_jobs")),
                _normalize_non_negative_int(summary_counters.get("failed_jobs")),
                json.dumps(summary_counters.get("stage_counts") or {}),
                current_run_status,
                sorted(ACTIVE_RUN_STATUSES),
                remaining_open_jobs,
                remaining_failed_jobs,
                current_run_status,
                sorted(ACTIVE_RUN_STATUSES),
                remaining_open_jobs,
                current_run_status,
                sorted(ACTIVE_RUN_STATUSES),
                remaining_open_jobs,
                json.dumps(cleanup_metadata),
                plan.run_id,
            ],
            conn=lock_conn,
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
