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
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test: Instagram posts Scrapling lane")
    parser.add_argument("--account", required=True, help="Instagram handle (without @)")
    parser.add_argument("--max-pages", type=int, default=1, help="Max GraphQL pages to fetch")
    parser.add_argument("--fast", action="store_true", help="Enable fast mode")
    args = parser.parse_args()

    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling.job_runner import (
        run_instagram_posts_scrapling_job,
    )

    account = args.account.strip().lower().lstrip("@")
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
        source_scope="bravo",
        initiated_by="manual_smoke",
        config=run_config,
        status="queued",
    )
    job_id = repo._create_job(
        None,
        run_id=run_id,
        platform="instagram",
        source_scope="bravo",
        job_type="posts",
        stage=repo.INSTAGRAM_POSTS_SCRAPLING_STAGE,
        config={
            **run_config,
            "max_pages": args.max_pages,
            "fast_mode": args.fast,
        },
        initiated_by="manual_smoke",
        status="queued",
    )
    job = pg.fetch_one("select * from social.scrape_jobs where id = %s::uuid", [job_id])
    if not job:
        print(f"ERROR: job {job_id} not found in social.scrape_jobs", file=sys.stderr)
        return 1

    print(f"Created run={run_id}, job={job_id}. Running...")
    result = run_instagram_posts_scrapling_job(job)
    status = result.get("status", "unknown")
    items = result.get("items_found")
    print(f"Status: {status}")
    print(f"Items found: {items}")
    if result.get("error_message"):
        print(f"Error: {result.get('error_message')}")
    return 0 if status == "completed" else 2


if __name__ == "__main__":
    sys.exit(main())
