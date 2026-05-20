"""Repeatable smoke test for the TikTok posts Scrapling lane.

Creates a real scrape_run + scrape_job row, runs the job, prints the result.

Usage:
    python scripts/socials/tiktok/smoke_posts_scrapling.py --account <handle> [--max-pages 1]

Requires:
- Database reachable (`TRR_DB_URL` set; `TRR_DB_FALLBACK_URL` optional as an explicit fallback)
- Valid TikTok cookies (SOCIAL_TIKTOK_COOKIES_JSON or SOCIAL_TIKTOK_COOKIES_FILE or data/tiktok_cookies.json)
- Optional: DECODO_USERNAME/DECODO_PASSWORD for proxy

WARNING: TikTok lane is experimental. May fail with `challenge_or_blocked` if
TikTok's API requires JS-signed params (X-Bogus/_signature).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add project root to path (scripts/socials/tiktok -> project root)
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from trr_backend.socials.tiktok.ops import run_posts_scrapling_smoke
from trr_backend.utils.env import load_env


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test: TikTok posts Scrapling lane")
    parser.add_argument("--account", required=True, help="TikTok handle (without @)")
    parser.add_argument("--max-pages", type=int, default=1, help="Max API pages to fetch")
    args = parser.parse_args()

    load_env()
    result = run_posts_scrapling_smoke(account=args.account, max_pages=args.max_pages)
    run_id = result.get("run_id")
    job_id = result.get("job_id")
    if result.get("status") == "missing_job":
        print(f"ERROR: {result.get('error_message')}", file=sys.stderr)
        return 1

    print(f"Created run={run_id}, job={job_id}. Running...")
    status = result.get("status", "unknown")
    items = result.get("items_found")
    print(f"Status: {status}")
    print(f"Items found: {items}")
    if result.get("error_message"):
        print(f"Error: {result.get('error_message')}")
    return 0 if status == "completed" else 2


if __name__ == "__main__":
    sys.exit(main())
