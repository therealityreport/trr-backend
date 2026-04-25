#!/usr/bin/env python3
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
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env

try:
    from scripts.db.cleanup_stale_social_advisory_locks import find_stale_advisory_sessions
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from scripts.db.cleanup_stale_social_advisory_locks import find_stale_advisory_sessions


OPEN_JOB_STATUSES = {"queued", "pending", "retrying", "running", "cancelling"}


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    return str(value)


def _fetch_db_activity() -> dict[str, Any]:
    rows = pg.fetch_all(
        """
        select
          coalesce(state, 'unknown') as state,
          count(*)::int as count,
          count(*) filter (where wait_event is not null)::int as waiting_count,
          count(*) filter (where query ilike '%%pg_try_advisory_lock(%%')::int as advisory_lock_query_count,
          max(extract(epoch from now() - coalesce(state_change, query_start, backend_start)))::int as max_age_seconds
        from pg_stat_activity
        group by coalesce(state, 'unknown')
        order by coalesce(state, 'unknown')
        """,
        [],
    )
    by_state = {str(row.get("state") or "unknown"): _json_safe(row) for row in rows}
    return {
        "total_sessions": sum(int(row.get("count") or 0) for row in rows),
        "waiting_sessions": sum(int(row.get("waiting_count") or 0) for row in rows),
        "advisory_lock_query_sessions": sum(int(row.get("advisory_lock_query_count") or 0) for row in rows),
        "by_state": by_state,
    }


def _fetch_open_social_jobs() -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        select
          id::text as id,
          run_id::text as run_id,
          status,
          job_type,
          worker_id,
          claimed_at,
          heartbeat_at,
          created_at,
          updated_at,
          last_error_code,
          error_message
        from social.scrape_jobs
        where status = any(%s)
        order by created_at asc, id asc
        limit 200
        """,
        [sorted(OPEN_JOB_STATUSES)],
    )


def build_pressure_snapshot(
    *,
    min_age_minutes: int = 15,
    allowed_lock_keys: list[int] | tuple[int, ...] | set[int] | None = None,
) -> dict[str, Any]:
    stale_sessions = (
        find_stale_advisory_sessions(min_age_minutes, allowed_lock_keys)
        if allowed_lock_keys
        else []
    )
    social_jobs = _fetch_open_social_jobs()
    return {
        "db_activity": _fetch_db_activity(),
        "social_jobs": [_json_safe(job) for job in social_jobs],
        "stale_advisory_sessions": [_json_safe(session) for session in stale_sessions],
        "stale_advisory_session_count": len(stale_sessions),
    }


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="social_control_plane_pressure_snapshot",
        description="Write a JSON snapshot of DB activity, open social jobs, and allowed stale advisory-lock sessions.",
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--min-age-minutes", type=int, default=15)
    parser.add_argument("--lock-key", action="append", type=int, default=[])
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = _parse_args(argv or sys.argv[1:])
    snapshot = build_pressure_snapshot(
        min_age_minutes=args.min_age_minutes,
        allowed_lock_keys=args.lock_key,
    )
    Path(args.output).write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
