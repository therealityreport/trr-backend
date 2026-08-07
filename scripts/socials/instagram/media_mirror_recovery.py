#!/usr/bin/env python3
"""Dry-run or recover stale Instagram media mirror queue work."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    from trr_backend.db import pg
    from trr_backend.socials import social_season_analytics_impl as social_repo
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.socials import social_season_analytics_impl as social_repo
    from trr_backend.utils.env import load_env


CONFIRM_APPLY = "RECOVER MEDIA MIRROR JOBS"
MEDIA_STAGES = ("media_mirror", "comment_media_mirror")


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    return str(value)


def _normalize_account(value: Any) -> str | None:
    normalized = str(value or "").strip().lower().lstrip("@")
    return normalized or None


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="media_mirror_recovery",
        description="Inspect and optionally recover stale Instagram media mirror jobs for one scrape run.",
    )
    parser.add_argument("--run-id", required=True, help="social.scrape_runs id")
    parser.add_argument("--stage", choices=MEDIA_STAGES, default="media_mirror")
    parser.add_argument("--account", help="Optional Instagram account handle filter.")
    parser.add_argument("--stale-after-seconds", type=int, default=900)
    parser.add_argument("--recover-limit", type=int, default=5)
    parser.add_argument("--dispatch-limit", type=int, default=8)
    parser.add_argument("--skip-recover", action="store_true")
    parser.add_argument("--skip-dispatch", action="store_true")
    parser.add_argument("--apply", action="store_true", help="Mutate stale jobs and dispatch due jobs.")
    parser.add_argument("--confirm-apply", help=f"Required with --apply. Exact value: {CONFIRM_APPLY!r}.")
    parser.add_argument("--json", action="store_true", help="Emit JSON only.")
    return parser.parse_args(argv)


def _account_filter_sql(account: str | None) -> tuple[str, list[Any]]:
    if not account:
        return "", []
    return (
        """
        and ltrim(lower(coalesce(
          config->>'account',
          metadata->>'account',
          config->>'account_handle',
          metadata->>'account_handle',
          config->>'owner_username',
          metadata->>'owner_username',
          ''
        )), '@') = %s
        """,
        [account],
    )


def fetch_status_snapshot(*, run_id: str, stage: str, account: str | None, stale_after_seconds: int) -> dict[str, Any]:
    account_sql, account_params = _account_filter_sql(account)
    params: list[Any] = [run_id, stage, *account_params]
    rows = pg.fetch_all(
        f"""
        select
          status,
          count(*) as jobs,
          min(created_at) as oldest_created_at,
          max(created_at) as newest_created_at,
          max(started_at) as newest_started_at,
          max(heartbeat_at) as newest_heartbeat_at,
          array_remove(array_agg(distinct worker_id), null) as worker_ids
        from social.scrape_jobs
        where platform = 'instagram'
          and run_id = %s::uuid
          and coalesce(config->>'stage', metadata->>'stage', job_type) = %s
          {account_sql}
        group by status
        order by status
        """,
        params,
    )
    stale_rows = fetch_stale_running_jobs(
        run_id=run_id,
        stage=stage,
        account=account,
        stale_after_seconds=stale_after_seconds,
        limit=25,
    )
    return {
        "run_id": run_id,
        "stage": stage,
        "account": account,
        "status_counts": [
            {
                **row,
                "worker_ids": list(row.get("worker_ids") or [])[:8],
            }
            for row in rows
        ],
        "stale_running_jobs": stale_rows,
        "stale_running_count": len(stale_rows),
    }


def fetch_stale_running_jobs(
    *,
    run_id: str,
    stage: str,
    account: str | None,
    stale_after_seconds: int,
    limit: int,
) -> list[dict[str, Any]]:
    account_sql, account_params = _account_filter_sql(account)
    safe_limit = max(1, min(int(limit or 1), 250))
    return pg.fetch_all(
        f"""
        select
          id::text,
          status,
          worker_id,
          created_at,
          started_at,
          claimed_at,
          heartbeat_at,
          coalesce(config->>'stage', metadata->>'stage', job_type) as stage,
          coalesce(
            config->>'account',
            metadata->>'account',
            config->>'account_handle',
            metadata->>'account_handle'
          ) as account
        from social.scrape_jobs
        where platform = 'instagram'
          and run_id = %s::uuid
          and coalesce(config->>'stage', metadata->>'stage', job_type) = %s
          and status = 'running'
          and coalesce(heartbeat_at, started_at, claimed_at, created_at)
            < now() - (%s * interval '1 second')
          {account_sql}
        order by coalesce(heartbeat_at, started_at, claimed_at, created_at) asc
        limit %s
        """,
        [run_id, stage, max(30, int(stale_after_seconds or 900)), *account_params, safe_limit],
    )


def recover_and_dispatch(args: argparse.Namespace, account: str | None) -> dict[str, Any]:
    if args.apply and args.confirm_apply != CONFIRM_APPLY:
        return {
            "ok": False,
            "failure_reason": "confirm_apply_required",
            "apply": True,
            "confirm_required": CONFIRM_APPLY,
        }

    before = fetch_status_snapshot(
        run_id=args.run_id,
        stage=args.stage,
        account=account,
        stale_after_seconds=max(30, int(args.stale_after_seconds or 900)),
    )
    recovered: list[dict[str, Any]] = []
    dispatch: dict[str, Any] = {"dispatched_job_ids": [], "dispatch_attempts": 0, "skipped": True}

    if args.apply and not args.skip_recover:
        recovered = social_repo.recover_stale_running_jobs(
            run_id=args.run_id,
            stage=args.stage,
            platform="instagram",
            stale_after_seconds=max(30, int(args.stale_after_seconds or 900)),
            limit=max(1, int(args.recover_limit or 1)),
        )
    if args.apply and not args.skip_dispatch:
        dispatch = social_repo.dispatch_due_social_jobs(
            run_id=args.run_id,
            limit=max(1, int(args.dispatch_limit or 1)),
        )

    after = None
    if args.apply:
        after = fetch_status_snapshot(
            run_id=args.run_id,
            stage=args.stage,
            account=account,
            stale_after_seconds=max(30, int(args.stale_after_seconds or 900)),
        )

    return {
        "ok": True,
        "apply": bool(args.apply),
        "dry_run": not bool(args.apply),
        "run_id": args.run_id,
        "stage": args.stage,
        "account": account,
        "stale_after_seconds": max(30, int(args.stale_after_seconds or 900)),
        "recover_limit": max(1, int(args.recover_limit or 1)),
        "dispatch_limit": max(1, int(args.dispatch_limit or 1)),
        "before": before,
        "recovered_job_ids": [
            str(row.get("id") or "").strip() for row in recovered if str(row.get("id") or "").strip()
        ],
        "dispatch": dispatch,
        "after": after,
    }


def _print_compact(payload: dict[str, Any]) -> None:
    before = payload.get("before") or {}
    print(
        (
            "run_id={run_id} stage={stage} dry_run={dry_run} stale_running={stale} "
            "recovered={recovered} dispatched={dispatched}"
        ).format(
            run_id=payload.get("run_id"),
            stage=payload.get("stage"),
            dry_run=payload.get("dry_run"),
            stale=before.get("stale_running_count"),
            recovered=len(payload.get("recovered_job_ids") or []),
            dispatched=len((payload.get("dispatch") or {}).get("dispatched_job_ids") or []),
        )
    )


def main(argv: list[str] | None = None) -> int:
    load_env()
    os.environ.setdefault("TRR_DB_POOL_CLOSE_AFTER_RETURN", "true")
    args = _parse_args(argv)
    try:
        payload = recover_and_dispatch(args, _normalize_account(args.account))
        if args.json:
            print(json.dumps(_json_safe(payload), indent=2, sort_keys=True))
        else:
            _print_compact(payload)
            print(json.dumps(_json_safe(payload), indent=2, sort_keys=True))
        return 0 if payload.get("ok") else 1
    finally:
        pg.close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
