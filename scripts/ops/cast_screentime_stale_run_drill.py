#!/usr/bin/env python3
"""Force a cast-screentime run into a stale-running state and validate reconciliation."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

import psycopg2
import requests


def _normalize_api_base_url(raw: str) -> str:
    clean = raw.strip().rstrip("/")
    if not clean:
        raise ValueError("TRR_API_URL is required")
    return clean if clean.endswith("/api/v1") else f"{clean}/api/v1"


def _database_url() -> str:
    for key in ("SUPABASE_DB_URL", "DATABASE_URL", "TRR_DB_URL"):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    raise ValueError("SUPABASE_DB_URL, DATABASE_URL, or TRR_DB_URL is required")


def _service_headers() -> dict[str, str]:
    service_role_key = (os.getenv("TRR_CORE_SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not service_role_key:
        raise ValueError("TRR_CORE_SUPABASE_SERVICE_ROLE_KEY is required")
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
    }
    internal_secret = (os.getenv("TRR_INTERNAL_ADMIN_SHARED_SECRET") or "").strip()
    if internal_secret:
        headers["X-TRR-Internal-Admin-Secret"] = internal_secret
    return headers


def _admin_request(session: requests.Session, *, api_base: str, method: str, path: str) -> dict[str, Any]:
    response = session.request(
        method=method,
        url=f"{api_base}{path}",
        headers=_service_headers(),
        timeout=(10, 60),
    )
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, dict):
        raise RuntimeError(f"Unexpected response body for {path}")
    return body


def _get_run_counts(run_id: str) -> dict[str, int]:
    sql = """
    SELECT
      (SELECT COUNT(*)::int FROM screenalytics.cast_screentime_segments WHERE run_id = %s::uuid) AS segment_count,
      (SELECT COUNT(*)::int FROM screenalytics.cast_screentime_evidence WHERE run_id = %s::uuid) AS evidence_count,
      (SELECT COUNT(*)::int FROM screenalytics.cast_screentime_excluded_sections WHERE run_id = %s::uuid) AS excluded_section_count,
      (SELECT COUNT(*)::int FROM screenalytics.run_artifacts WHERE run_id = %s::uuid) AS artifact_count,
      (SELECT COUNT(*)::int FROM screenalytics.run_person_metrics WHERE run_id = %s::uuid) AS metric_count
    """
    with psycopg2.connect(_database_url()) as conn, conn.cursor() as cursor:
        cursor.execute(sql, [run_id, run_id, run_id, run_id, run_id])
        row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"Unable to snapshot persisted counts for run {run_id}")
    return {
        "segment_count": int(row[0]),
        "evidence_count": int(row[1]),
        "excluded_section_count": int(row[2]),
        "artifact_count": int(row[3]),
        "metric_count": int(row[4]),
    }


def _force_stale_running_state(run_id: str, *, stale_after_seconds: int) -> dict[str, Any]:
    sql = """
    UPDATE screenalytics.runs_v2
    SET
      status = 'running',
      error_message = NULL,
      started_at = COALESCE(started_at, NOW() - (%s::int * INTERVAL '1 second')),
      worker_heartbeat_at = NOW() - (%s::int * INTERVAL '1 second')
    WHERE id = %s::uuid
    RETURNING
      id::text,
      status,
      error_message,
      worker_heartbeat_at,
      started_at
    """
    forced_seconds = max(stale_after_seconds + 60, stale_after_seconds * 2)
    with psycopg2.connect(_database_url()) as conn, conn.cursor() as cursor:
        cursor.execute(sql, [forced_seconds, forced_seconds, run_id])
        row = cursor.fetchone()
        conn.commit()
    if row is None:
        raise RuntimeError(f"Run not found: {run_id}")
    return {
        "id": row[0],
        "status": row[1],
        "error_message": row[2],
        "worker_heartbeat_at": row[3].isoformat() if row[3] else None,
        "started_at": row[4].isoformat() if row[4] else None,
        "forced_stale_seconds": forced_seconds,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-base-url", default=os.getenv("TRR_API_URL", ""), help="TRR API base URL. Defaults to TRR_API_URL.")
    parser.add_argument("--run-id", required=True, help="Existing cast-screentime run id to force stale.")
    parser.add_argument("--show-id", help="Optional show id filter for the reconcile endpoint.")
    parser.add_argument("--stale-after-seconds", type=int, default=1800, help="Threshold passed to the reconcile endpoint.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_base = _normalize_api_base_url(args.api_base_url)
    summary: dict[str, Any] = {
        "run_id": args.run_id,
        "stale_after_seconds": args.stale_after_seconds,
    }
    with requests.Session() as session:
        summary["counts_before"] = _get_run_counts(args.run_id)
        summary["forced_state"] = _force_stale_running_state(args.run_id, stale_after_seconds=args.stale_after_seconds)
        show_filter = f"&show_id={args.show_id}" if args.show_id else ""
        reconcile = _admin_request(
            session,
            api_base=api_base,
            method="POST",
            path=f"/admin/cast-screentime/runs/reconcile-stale?stale_after_seconds={args.stale_after_seconds}{show_filter}",
        )
        summary["reconcile_result"] = reconcile
        summary["run_after"] = _admin_request(
            session,
            api_base=api_base,
            method="GET",
            path=f"/admin/cast-screentime/runs/{args.run_id}",
        )
        summary["counts_after"] = _get_run_counts(args.run_id)

    counts_changed = summary["counts_before"] != summary["counts_after"]
    summary["counts_unchanged"] = not counts_changed
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if not counts_changed else 1


if __name__ == "__main__":
    raise SystemExit(main())
