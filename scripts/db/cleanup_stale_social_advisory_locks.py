#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
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


ADVISORY_LOCK_QUERY_RE = re.compile(r"pg_try_advisory_lock\s*\(\s*(-?\d+)\s*\)", re.IGNORECASE)


class AdvisoryCleanupRefused(RuntimeError):  # noqa: N818 - task contract names this exception.
    """Raised when cleanup would run without a safe operator-provided allowlist."""


def extract_advisory_lock_key(query: object) -> int | None:
    match = ADVISORY_LOCK_QUERY_RE.search(str(query or ""))
    if not match:
        return None
    return int(match.group(1))


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    return str(value)


def _normalize_allowed_lock_keys(allowed_lock_keys: list[int] | tuple[int, ...] | set[int]) -> set[int]:
    normalized = {int(key) for key in allowed_lock_keys}
    if not normalized:
        raise AdvisoryCleanupRefused("Refusing advisory-lock cleanup without at least one --lock-key allowlist entry.")
    return normalized


def find_stale_advisory_sessions(
    min_age_minutes: int,
    allowed_lock_keys: list[int] | tuple[int, ...] | set[int],
) -> list[dict[str, Any]]:
    allowed_keys = _normalize_allowed_lock_keys(allowed_lock_keys)
    min_age = max(0, int(min_age_minutes))
    rows = pg.fetch_all(
        """
        select
          pid,
          usename,
          application_name,
          client_addr::text as client_addr,
          state,
          query,
          backend_start,
          xact_start,
          query_start,
          state_change,
          extract(epoch from now() - coalesce(state_change, query_start, backend_start))::int as idle_age_seconds
        from pg_stat_activity
        where state in ('idle', 'idle in transaction')
          and query ilike '%%pg_try_advisory_lock(%%'
          and coalesce(state_change, query_start, backend_start) <= now() - (%s * interval '1 minute')
        order by coalesce(state_change, query_start, backend_start) asc, pid asc
        """,
        [min_age],
    )

    stale_sessions: list[dict[str, Any]] = []
    for row in rows:
        lock_key = extract_advisory_lock_key(row.get("query"))
        if lock_key is None or lock_key not in allowed_keys:
            continue
        stale_sessions.append({**row, "lock_key": lock_key})
    return stale_sessions


def cleanup_stale_advisory_sessions(
    min_age_minutes: int,
    allowed_lock_keys: list[int] | tuple[int, ...] | set[int],
    *,
    execute: bool = False,
) -> dict[str, Any]:
    stale_sessions = find_stale_advisory_sessions(min_age_minutes, allowed_lock_keys)
    terminated_pids: list[int] = []
    if execute:
        for session in stale_sessions:
            pid = int(session["pid"])
            pg.execute("select pg_terminate_backend(%s)", [pid])
            terminated_pids.append(pid)

    return {
        "dry_run": not execute,
        "min_age_minutes": max(0, int(min_age_minutes)),
        "allowed_lock_keys": sorted(_normalize_allowed_lock_keys(allowed_lock_keys)),
        "terminated_pids": terminated_pids,
        "stale_advisory_sessions": [_json_safe(session) for session in stale_sessions],
    }


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cleanup_stale_social_advisory_locks",
        description="Find or terminate stale social advisory-lock sessions from an explicit lock-key allowlist.",
    )
    parser.add_argument("--min-age-minutes", type=int, default=15)
    parser.add_argument("--lock-key", action="append", type=int, default=[])
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = _parse_args(argv or sys.argv[1:])
    result = cleanup_stale_advisory_sessions(
        args.min_age_minutes,
        args.lock_key,
        execute=args.execute,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
