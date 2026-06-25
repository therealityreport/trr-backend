#!/usr/bin/env python3
"""Run one Instagram post media mirror job locally or through Modal."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    from trr_backend.db import pg
    from trr_backend.socials.control_plane import claim_and_process_social_job
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.socials.control_plane import claim_and_process_social_job
    from trr_backend.utils.env import load_env


MEDIA_STAGE = "media_mirror"
CLAIMABLE_STATUSES = ("queued", "pending", "retrying")


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


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="one_post_media_mirror",
        description="Claim and process one Instagram post media mirror job by job, post, or shortcode.",
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--job-id", help="Existing social.scrape_jobs id to process.")
    target.add_argument("--post-id", help="social.instagram_posts id to resolve to an active media job.")
    target.add_argument("--source-id", "--shortcode", dest="source_id", help="Instagram shortcode/source id.")
    parser.add_argument("--account", default="bravotv", help="Optional account/source-account filter.")
    parser.add_argument(
        "--mode",
        choices=("local", "modal"),
        default="local",
        help="Run locally with the current checkout or invoke deployed Modal.",
    )
    parser.add_argument("--modal-app", default="trr-backend-jobs", help="Modal app name.")
    parser.add_argument("--modal-function", default="run_social_media_job", help="Modal function name.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve and print the job without claiming it.")
    parser.add_argument("--json", action="store_true", help="Emit JSON only.")
    return parser.parse_args(argv)


def _account_filter_sql(account: str) -> tuple[str, list[Any]]:
    normalized = account.strip().lower().lstrip("@")
    if not normalized:
        return "", []
    return (
        """
        and ltrim(lower(coalesce(
          p.source_account,
          p.username,
          j.config->>'account',
          j.metadata->>'account',
          ''
        )), '@') = %s
        """,
        [normalized],
    )


def resolve_media_job(args: argparse.Namespace) -> dict[str, Any] | None:
    if _normalize_text(args.job_id):
        return pg.fetch_one(
            """
            select
              j.id::text as job_id,
              j.run_id::text as run_id,
              j.status as job_status,
              j.attempt_count,
              j.worker_id,
              j.created_at,
              j.started_at,
              j.completed_at,
              j.config->>'post_id' as post_id,
              j.config->>'source_id' as source_id,
              coalesce(p.shortcode, j.config->>'source_id') as shortcode,
              coalesce(p.source_account, p.username, j.config->>'account') as account,
              p.media_mirror_status,
              jsonb_array_length(coalesce(p.hosted_media_urls, '[]'::jsonb)) as hosted_media_count
            from social.scrape_jobs j
            left join social.instagram_posts p on p.id::text = j.config->>'post_id'
            where j.id = %s::uuid
              and j.platform = 'instagram'
              and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
            """,
            [_normalize_text(args.job_id), MEDIA_STAGE],
        )

    account_sql, account_params = _account_filter_sql(_normalize_text(args.account))
    target_sql = "p.id = %s::uuid" if _normalize_text(args.post_id) else "p.shortcode = %s"
    target_value = _normalize_text(args.post_id) or _normalize_text(args.source_id)
    return pg.fetch_one(
        f"""
        select
          j.id::text as job_id,
          j.run_id::text as run_id,
          j.status as job_status,
          j.attempt_count,
          j.worker_id,
          j.created_at,
          j.started_at,
          j.completed_at,
          p.id::text as post_id,
          p.shortcode as source_id,
          p.shortcode,
          coalesce(p.source_account, p.username, j.config->>'account') as account,
          p.media_mirror_status,
          jsonb_array_length(coalesce(p.hosted_media_urls, '[]'::jsonb)) as hosted_media_count
        from social.instagram_posts p
        join social.scrape_jobs j on j.config->>'post_id' = p.id::text
        where {target_sql}
          and j.platform = 'instagram'
          and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
          and j.status = any(%s::text[])
          {account_sql}
        order by
          case j.status when 'queued' then 0 when 'pending' then 1 when 'retrying' then 2 else 3 end,
          j.created_at asc
        limit 1
        """,
        [target_value, MEDIA_STAGE, list(CLAIMABLE_STATUSES), *account_params],
    )


def _run_local(job_id: str) -> dict[str, Any]:
    worker_id = f"modal:social-media-local:operator:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    return claim_and_process_social_job(job_id=job_id, worker_id=worker_id)


def _run_modal(args: argparse.Namespace, job_id: str) -> dict[str, Any]:
    import modal

    function = modal.Function.from_name(args.modal_app, args.modal_function)
    return function.remote(job_id)


def _print_compact(payload: dict[str, Any]) -> None:
    job = payload.get("job") if isinstance(payload.get("job"), dict) else {}
    print(
        "job_id={job_id} mode={mode} dry_run={dry_run} claimed={claimed} status={status} shortcode={shortcode}".format(
            job_id=payload.get("job_id"),
            mode=payload.get("mode"),
            dry_run=payload.get("dry_run"),
            claimed=payload.get("claimed"),
            status=job.get("status") or payload.get("job_status"),
            shortcode=payload.get("shortcode") or payload.get("source_id"),
        )
    )


def main(argv: list[str] | None = None) -> int:
    load_env()
    os.environ.setdefault("TRR_DB_POOL_CLOSE_AFTER_RETURN", "true")
    args = _parse_args(argv)
    try:
        resolved = resolve_media_job(args)
        if not resolved:
            payload = {
                "ok": False,
                "failure_reason": "media_mirror_job_not_found",
                "dry_run": bool(args.dry_run),
                "mode": args.mode,
            }
            print(json.dumps(_json_safe(payload), indent=2, sort_keys=True))
            return 1
        job_id = str(resolved.get("job_id") or "").strip()
        if args.dry_run:
            payload = {"ok": True, "dry_run": True, "mode": args.mode, **resolved}
        elif str(resolved.get("job_status") or "").strip().lower() not in CLAIMABLE_STATUSES:
            payload = {
                "ok": False,
                "dry_run": False,
                "mode": args.mode,
                "failure_reason": "job_not_claimable",
                **resolved,
            }
        else:
            result = _run_modal(args, job_id) if args.mode == "modal" else _run_local(job_id)
            payload = {
                "ok": bool(result.get("claimed")),
                "dry_run": False,
                "mode": args.mode,
                **resolved,
                "claimed": bool(result.get("claimed")),
                "result": result,
            }
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
