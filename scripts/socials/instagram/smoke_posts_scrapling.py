"""Repeatable smoke test for the Instagram posts Scrapling lane.

Creates a real scrape_run + scrape_job row, runs the job, prints the result.

Usage:
    python scripts/socials/instagram/smoke_posts_scrapling.py --account <handle> [--max-pages 1] [--fast]

Requires:
- Database reachable (`TRR_DB_URL` set; `TRR_DB_FALLBACK_URL` optional as an explicit fallback)
- Valid Instagram cookies (SOCIAL_INSTAGRAM_COOKIES_JSON or SOCIAL_INSTAGRAM_COOKIES_FILE)
- Optional: DECODO_USERNAME/DECODO_PASSWORD for proxy (falls back to direct mode)
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any


def _metadata_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _print_operator_summary(summary: dict[str, Any]) -> None:
    print("Instagram posts smoke summary")
    print(f"  account_handle: {summary['account_handle']}")
    print(f"  run_id: {summary['run_id']}")
    print(f"  job_id: {summary['job_id']}")
    print(f"  status: {summary['status']}")
    print(f"  pages_fetched: {summary['pages_fetched']}")
    print(f"  posts_fetched: {summary['posts_fetched']}")
    print(f"  posts_upserted: {summary['posts_upserted']}")
    print(f"  stop_reason: {summary['stop_reason'] or 'none'}")
    print(f"  decodo_mode: {summary['decodo_mode'] or 'unknown'}")
    print(f"  cooldown_state: {summary['cooldown_state']}")
    if summary.get("error_message"):
        print(f"  error_message: {summary['error_message']}")
    print("  proxy_pacing:")
    proxy_pacing = _metadata_dict(summary.get("proxy_pacing"))
    for key in ("enabled", "mode_configured", "mode_last_used", "lock_wait_ms", "lock_held_ms", "scheduled_sleep_ms"):
        if key in proxy_pacing:
            print(f"    {key}: {proxy_pacing.get(key)}")


def _build_operator_summary(*, account: str, run_id: str, job_id: str, result: dict[str, Any]) -> dict[str, Any]:
    metadata = _metadata_dict(result.get("metadata"))
    stage_counters = _metadata_dict(metadata.get("stage_counters"))
    listing_progress = _metadata_dict(metadata.get("listing_progress"))
    persist_counters = _metadata_dict(
        metadata.get("persist_counters") or metadata.get("posts_scrapling_persist_diagnostics")
    )
    fetcher_runtime = _metadata_dict(metadata.get("fetcher_runtime") or metadata.get("runtime_metadata"))
    proxy_pacing = _metadata_dict(fetcher_runtime.get("proxy_pacing") or metadata.get("proxy_pacing"))
    proxy_identity = _metadata_dict(proxy_pacing.get("identity") or fetcher_runtime.get("proxy_identity"))
    auth_cooldown = _metadata_dict(metadata.get("auth_cooldown"))
    cooldown_active = bool(metadata.get("auth_cooldown_active") or auth_cooldown)
    cooldown_state = "active" if cooldown_active else "none"
    if auth_cooldown.get("blocker_kind"):
        cooldown_state = f"{cooldown_state}:{auth_cooldown.get('blocker_kind')}"
    return {
        "account_handle": account,
        "run_id": str(result.get("run_id") or run_id),
        "job_id": str(result.get("id") or job_id),
        "status": str(result.get("status") or "unknown"),
        "pages_fetched": int(
            stage_counters.get("pages")
            or listing_progress.get("page_index")
            or fetcher_runtime.get("pages_fetched")
            or 0
        ),
        "posts_fetched": int(
            stage_counters.get("posts")
            or listing_progress.get("posts_seen")
            or result.get("items_found")
            or 0
        ),
        "posts_upserted": int(
            persist_counters.get("posts_upserted")
            or listing_progress.get("posts_upserted")
            or 0
        ),
        "stop_reason": (
            str(listing_progress.get("stop_reason") or metadata.get("stop_reason") or "").strip() or None
        ),
        "proxy_pacing": proxy_pacing,
        "decodo_mode": (
            str(proxy_identity.get("provider") or fetcher_runtime.get("selected_proxy_fingerprint") or "").strip()
            or None
        ),
        "cooldown_state": cooldown_state,
        "cooldown": auth_cooldown or None,
        "error_message": str(result.get("error_message") or "").strip() or None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test: Instagram posts Scrapling lane")
    parser.add_argument("--account", required=True, help="Instagram handle (without @)")
    parser.add_argument("--max-pages", type=int, default=1, help="Max GraphQL pages to fetch (bounded to 1-10)")
    parser.add_argument("--fast", action="store_true", help="Enable fast mode")
    parser.add_argument("--json", action="store_true", help="Emit only the operator summary JSON")
    args = parser.parse_args()

    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling.job_runner import (
        run_instagram_posts_scrapling_job,
    )

    account = args.account.strip().lower().lstrip("@")
    max_pages = max(1, min(int(args.max_pages or 1), 10))
    run_config = {
        "platform": "instagram",
        "stage": repo.INSTAGRAM_POSTS_SCRAPLING_STAGE,
        "account": account,
    }
    # scrape_runs.status check constraint allows only:
    # queued | running | completed | failed | cancelled
    # For manual smoke tests we use 'queued' to seed the row, then the runner
    # updates it to 'running' when it claims work and 'completed'/'failed' at the end.
    run_id = repo._create_run(
        None,
        source_scope="network",
        initiated_by="manual_smoke",
        config=run_config,
        status="queued",
    )
    job_id = repo._create_job(
        None,
        run_id=run_id,
        platform="instagram",
        source_scope="network",
        job_type="posts",
        stage=repo.INSTAGRAM_POSTS_SCRAPLING_STAGE,
        config={
            **run_config,
            "max_pages": max_pages,
            "fast_mode": args.fast,
        },
        initiated_by="manual_smoke",
        status="queued",
    )
    job = pg.fetch_one("select * from social.scrape_jobs where id = %s::uuid", [job_id])
    if not job:
        print(f"ERROR: job {job_id} not found in social.scrape_jobs", file=sys.stderr)
        return 1

    if not args.json:
        print(f"Created run={run_id}, job={job_id}, account=@{account}, max_pages={max_pages}. Running...")
    result = run_instagram_posts_scrapling_job(job)
    status = result.get("status", "unknown")
    summary = _build_operator_summary(account=account, run_id=str(run_id), job_id=str(job_id), result=result)
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    else:
        _print_operator_summary(summary)
        print("")
        print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0 if status == "completed" else 2


if __name__ == "__main__":
    sys.exit(main())
