"""Read-only operator summary for one social catalog/backfill run.

The report intentionally prints only sanitized runtime metadata. It does not
dump raw run/job config because those payloads may contain operational details
that are not needed for acceptance.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any

from trr_backend.db import pg

POOL_ERROR_RE = re.compile(
    r"(poolerror|pool_capacity|connection pool exhausted|database service unavailable)",
    re.IGNORECASE,
)


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower().lstrip("@")


def _json_default(value: Any) -> str:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _fetch_run(run_id: str) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        select
          id::text as run_id,
          status,
          source_scope,
          config,
          summary,
          created_at,
          started_at,
          completed_at
        from social.scrape_runs
        where id = %s::uuid
        limit 1
        """,
        [run_id],
    )


def _fetch_jobs(run_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        select
          id::text as job_id,
          run_id::text as run_id,
          platform,
          job_type,
          status,
          coalesce(config->>'stage', metadata->>'stage', job_type) as stage,
          config,
          metadata,
          items_found,
          started_at,
          completed_at,
          error_message,
          last_error_code
        from social.scrape_jobs
        where run_id = %s::uuid
        order by created_at asc, id asc
        """,
        [run_id],
    )


def _job_account(job: dict[str, Any]) -> str | None:
    config = _metadata_dict(job.get("config"))
    metadata = _metadata_dict(job.get("metadata"))
    for key in ("account", "account_handle", "source_account", "username"):
        normalized = _normalize_text(config.get(key) or metadata.get(key))
        if normalized:
            return normalized
    return None


def _runtime_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    return (
        _metadata_dict(metadata.get("fetcher_runtime")) or _metadata_dict(metadata.get("runtime_metadata")) or metadata
    )


def _pool_error(job: dict[str, Any]) -> str | None:
    metadata = _metadata_dict(job.get("metadata"))
    fields = [
        job.get("last_error_code"),
        job.get("error_message"),
        metadata.get("last_error_code"),
        metadata.get("error_code"),
        metadata.get("failure_reason"),
        _metadata_dict(metadata.get("activity")).get("failure_reason"),
    ]
    text = " ".join(str(value or "") for value in fields)
    return text if POOL_ERROR_RE.search(text) else None


def build_summary(*, run_id: str, platform: str | None, account: str | None) -> tuple[dict[str, Any], list[str]]:
    run = _fetch_run(run_id)
    if not run:
        return {"run_id": run_id, "found": False}, [f"Run not found: {run_id}"]

    jobs = _fetch_jobs(run_id)
    run_config = _metadata_dict(run.get("config"))
    normalized_platform = _normalize_text(platform)
    normalized_account = _normalize_text(account)
    run_platform = _normalize_text(run_config.get("platform"))
    run_account = _normalize_text(run_config.get("account") or run_config.get("account_handle"))

    failures: list[str] = []
    if normalized_platform and run_platform and normalized_platform != run_platform:
        failures.append(f"Run platform mismatch: expected {normalized_platform}, saw {run_platform}")
    if normalized_account and run_account and normalized_account != run_account:
        failures.append(f"Run account mismatch: expected {normalized_account}, saw {run_account}")

    status_counts: Counter[str] = Counter()
    stage_counts: dict[str, Counter[str]] = defaultdict(Counter)
    stage_timings: dict[str, dict[str, Any]] = {}
    proxy_fingerprints: Counter[str] = Counter()
    proxy_session_modes: Counter[str] = Counter()
    proxy_session_keys: Counter[str] = Counter()
    detail_counters: Counter[str] = Counter()
    failed_jobs: list[dict[str, Any]] = []
    pool_errors: list[dict[str, Any]] = []

    for job in jobs:
        job_platform = _normalize_text(job.get("platform"))
        job_account = _job_account(job)
        if normalized_platform and job_platform and normalized_platform != job_platform:
            continue
        if normalized_account and job_account and normalized_account != job_account:
            continue

        status = _normalize_text(job.get("status")) or "unknown"
        stage = _normalize_text(job.get("stage")) or "unknown"
        metadata = _metadata_dict(job.get("metadata"))
        runtime = _runtime_metadata(metadata)
        activity = _metadata_dict(metadata.get("activity"))

        status_counts[status] += 1
        stage_counts[stage][status] += 1
        timing = stage_timings.setdefault(stage, {"started_at": None, "completed_at": None})
        started_at = job.get("started_at")
        completed_at = job.get("completed_at")
        if started_at and (timing["started_at"] is None or started_at < timing["started_at"]):
            timing["started_at"] = started_at
        if completed_at and (timing["completed_at"] is None or completed_at > timing["completed_at"]):
            timing["completed_at"] = completed_at

        fingerprint = str(runtime.get("selected_proxy_fingerprint") or runtime.get("proxy_fingerprint") or "").strip()
        if fingerprint:
            proxy_fingerprints[fingerprint] += 1
        session_mode = str(runtime.get("proxy_session_mode") or "").strip()
        if session_mode:
            proxy_session_modes[session_mode] += 1
        session_key = str(runtime.get("proxy_session_key") or "").strip()
        if session_key:
            proxy_session_keys[session_key] += 1

        for key in (
            "details_refresh_rows_seen",
            "details_refresh_detail_fetch_attempts",
            "details_refresh_fetch_attempts",
            "details_refresh_fetch_avoided",
            "details_refresh_rows_satisfied_from_gallery",
            "details_refresh_rows_satisfied_from_existing",
            "details_refresh_saved_posts",
        ):
            detail_counters[key] += _coerce_int(metadata.get(key) or activity.get(key))

        if status == "failed":
            failed_jobs.append(
                {
                    "job_id": job.get("job_id"),
                    "stage": stage,
                    "last_error_code": job.get("last_error_code"),
                    "error_message": job.get("error_message"),
                }
            )
        pool_error = _pool_error(job)
        if pool_error:
            pool_errors.append({"job_id": job.get("job_id"), "stage": stage, "error": pool_error[:240]})

    if failed_jobs:
        failures.append(f"{len(failed_jobs)} matching job(s) failed")
    if pool_errors:
        failures.append(f"{len(pool_errors)} matching job(s) reported pool-capacity errors")

    matched_jobs = sum(status_counts.values())
    if (normalized_platform or normalized_account) and jobs and matched_jobs == 0:
        failures.append("No jobs matched the requested platform/account filter")

    summary = {
        "run_id": run_id,
        "found": True,
        "run": {
            "status": run.get("status"),
            "platform": run_platform or None,
            "account": run_account or None,
            "source_scope": run.get("source_scope"),
            "created_at": run.get("created_at"),
            "started_at": run.get("started_at"),
            "completed_at": run.get("completed_at"),
        },
        "jobs": {
            "matched": matched_jobs,
            "status_counts": dict(sorted(status_counts.items())),
            "stage_counts": {stage: dict(sorted(counts.items())) for stage, counts in sorted(stage_counts.items())},
            "failed": failed_jobs,
        },
        "stage_timings": stage_timings,
        "details_refresh_counters": dict(sorted(detail_counters.items())),
        "proxy": {
            "fingerprints": dict(sorted(proxy_fingerprints.items())),
            "session_modes": dict(sorted(proxy_session_modes.items())),
            "session_keys": dict(sorted(proxy_session_keys.items())),
        },
        "pool_errors": pool_errors,
        "acceptance_failures": failures,
    }
    return summary, failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, help="social.scrape_runs id to summarize")
    parser.add_argument("--platform", default=None, help="Optional platform filter, e.g. instagram")
    parser.add_argument("--account", default=None, help="Optional account filter, e.g. thetraitorsus")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary, failures = build_summary(
        run_id=str(args.run_id).strip(),
        platform=args.platform,
        account=args.account,
    )
    print(json.dumps(summary, indent=2, sort_keys=True, default=_json_default))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
