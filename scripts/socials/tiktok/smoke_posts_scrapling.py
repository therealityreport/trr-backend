"""Repeatable smoke test for the TikTok posts Scrapling lane.

Creates a real scrape_run + scrape_job row, runs the job, prints the result.

Usage:
    python scripts/socials/tiktok/smoke_posts_scrapling.py --account <handle> [--max-pages 1]

Requires:
- Database reachable (TRR_DB_URL or SUPABASE_DB_URL set)
- Valid TikTok cookies (SOCIAL_TIKTOK_COOKIES_JSON or SOCIAL_TIKTOK_COOKIES_FILE or data/tiktok_cookies.json)
- Optional: DECODO_USERNAME/DECODO_PASSWORD for proxy

WARNING: TikTok lane is experimental. May fail with `challenge_or_blocked` if
TikTok's API requires JS-signed params (X-Bogus/_signature).
"""

from __future__ import annotations

import argparse
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test: TikTok posts Scrapling lane")
    parser.add_argument("--account", required=True, help="TikTok handle (without @)")
    parser.add_argument("--max-pages", type=int, default=1, help="Max API pages to fetch")
    args = parser.parse_args()

    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.tiktok.posts_scrapling.job_runner import (
        run_tiktok_posts_scrapling_job,
    )

    account = args.account.strip().lower().lstrip("@")
    run_config = {
        "platform": "tiktok",
        "stage": repo.TIKTOK_POSTS_SCRAPLING_STAGE,
        "account": account,
    }
    # scrape_runs.status check constraint allows only:
    # queued | running | completed | failed | cancelled
    # For manual smoke tests we use 'queued' to seed the row, then the runner
    # updates it to 'running' when it claims work and 'completed'/'failed' at the end.
    run_id = repo._create_run(
        None,
        source_scope="bravo",
        initiated_by="manual_smoke",
        config=run_config,
        status="queued",
    )
    job_id = repo._create_job(
        None,
        run_id=run_id,
        platform="tiktok",
        source_scope="bravo",
        job_type="posts",
        stage=repo.TIKTOK_POSTS_SCRAPLING_STAGE,
        config={
            **run_config,
            "max_pages": args.max_pages,
        },
        initiated_by="manual_smoke",
        status="queued",
    )
    job = pg.fetch_one("select * from social.scrape_jobs where id = %s::uuid", [job_id])
    if not job:
        print(f"ERROR: job {job_id} not found in social.scrape_jobs", file=sys.stderr)
        return 1

    print(f"Created run={run_id}, job={job_id}. Running...")
    result = run_tiktok_posts_scrapling_job(job)
    status = result.get("status", "unknown")
    items = result.get("items_found")
    print(f"Status: {status}")
    print(f"Items found: {items}")
    if result.get("error_message"):
        print(f"Error: {result.get('error_message')}")
    return 0 if status == "completed" else 2


if __name__ == "__main__":
    sys.exit(main())
