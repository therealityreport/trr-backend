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
ACTIVE_RUN_STATUSES = {"queued", "pending", "retrying", "running"}
PARTITION_KEY_FIELDS = (
    "partition_id",
    "partition_key",
    "partition",
    "partition_index",
    "shard",
    "shard_id",
    "shard_index",
    "runner_lane",
    "required_worker_lane",
    "account_id",
    "account_handle",
    "shared_account_id",
    "source_account_id",
    "handle",
    "username",
)


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


def _metadata_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _lookup_nested_value(payload: dict[str, object], field: str) -> object:
    if field in payload:
        return payload[field]
    for value in payload.values():
        if isinstance(value, dict):
            nested_value = _lookup_nested_value(value, field)
            if nested_value not in (None, ""):
                return nested_value
    return None


def _stable_text(value: object) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(value or "").strip()


def _duplicate_group_key(job: dict[str, object]) -> tuple[str, tuple[tuple[str, str], ...]]:
    job_type = str(job.get("job_type") or "").strip()
    config = _metadata_dict(job.get("config"))
    metadata = _metadata_dict(job.get("metadata"))
    partition_values: list[tuple[str, str]] = []
    for field in PARTITION_KEY_FIELDS:
        value = _lookup_nested_value(config, field)
        if value in (None, ""):
            value = _lookup_nested_value(metadata, field)
        stable_value = _stable_text(value)
        if stable_value:
            partition_values.append((field, stable_value))
    return job_type, tuple(partition_values)


def _identify_duplicate_open_job_ids(open_jobs: list[dict[str, object]]) -> list[str]:
    seen_keys: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
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


def _fetch_remaining_job_counts(run_id: str) -> dict[str, int]:
    row = pg.fetch_one(
        """
        select
          count(*) filter (where status = any(%s))::int as open_jobs,
          count(*) filter (where status = 'failed')::int as failed_jobs
        from social.scrape_jobs
        where run_id::text = %s
        """,
        [sorted(OPEN_JOB_STATUSES), run_id],
    )
    if not row:
        return {"open_jobs": 0, "failed_jobs": 0}
    return {
        "open_jobs": _normalize_non_negative_int(row.get("open_jobs")),
        "failed_jobs": _normalize_non_negative_int(row.get("failed_jobs")),
    }


def _normalize_non_negative_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


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

    remaining_counts = _fetch_remaining_job_counts(plan.run_id)
    remaining_open_jobs = remaining_counts["open_jobs"]
    remaining_failed_jobs = remaining_counts["failed_jobs"]
    cleanup_metadata = json.dumps(
        {
            "stale_run_reconciler": {
                "duplicate_open_job_ids": plan.duplicate_open_job_ids,
                "retry_job_ids": plan.retry_job_ids,
                "remaining_open_jobs": remaining_open_jobs,
                "remaining_failed_jobs": remaining_failed_jobs,
                "terminalized": plan.run_status in ACTIVE_RUN_STATUSES and remaining_open_jobs == 0,
            }
        }
    )
    pg.execute(
        """
        update social.scrape_runs
        set
          active_jobs = %s,
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
            remaining_open_jobs,
            plan.run_status,
            sorted(ACTIVE_RUN_STATUSES),
            remaining_open_jobs,
            remaining_failed_jobs,
            plan.run_status,
            sorted(ACTIVE_RUN_STATUSES),
            remaining_open_jobs,
            plan.run_status,
            sorted(ACTIVE_RUN_STATUSES),
            remaining_open_jobs,
            cleanup_metadata,
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
