#!/usr/bin/env python3
"""Direct Instagram catalog backfill — bypasses the slow discovery/frontier pipeline.

Scrapes any Instagram account page-by-page using the universal InstagramScraper,
parses posts via _parse_post_node, and batch-upserts every page to the catalog DB.

This is the fast path: no frontier records, no advisory locks, no discovery stage.
Browser fallback is enabled if HTTP GraphQL gets blocked (403).

Usage:
    # Backfill any account:
    python scripts/socials/instagram/direct_catalog_backfill.py --account bravotv
    python scripts/socials/instagram/direct_catalog_backfill.py --account andypcohen --max-pages 100

    # Dry run (scrape only, no DB writes):
    python scripts/socials/instagram/direct_catalog_backfill.py --account bravotv --dry-run

    # Resume from a specific cursor (printed when interrupted):
    python scripts/socials/instagram/direct_catalog_backfill.py --account bravotv --resume-cursor 'QVF...'
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env", override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
# Show scraper activity but quiet the DB pool
logging.getLogger("trr_backend.db.pg").setLevel(logging.WARNING)
logging.getLogger("trr_backend.socials.instagram.scraper").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger("direct_catalog_backfill")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--account", required=True, help="Instagram account handle to backfill (e.g. bravotv)")
    parser.add_argument("--max-pages", type=int, default=600, help="Max pages to scrape (default: 600)")
    parser.add_argument("--page-size", type=int, default=33, help="Posts per page (default: 33)")
    parser.add_argument("--delay", type=float, default=2.5, help="Delay between pages in seconds (default: 2.5)")
    parser.add_argument("--dry-run", action="store_true", help="Print progress without writing to DB")
    parser.add_argument("--resume-cursor", type=str, default=None, help="Resume from a specific GraphQL cursor")
    args = parser.parse_args()

    account_handle = args.account.strip().lstrip("@").lower()
    if not account_handle:
        logger.error("--account is required")
        return 1

    from trr_backend.socials.ops.instagram_direct_catalog import (
        DirectInstagramCatalogBackfillOptions,
        run_direct_instagram_catalog_backfill,
    )

    run_direct_instagram_catalog_backfill(
        DirectInstagramCatalogBackfillOptions(
            account=account_handle,
            max_pages=args.max_pages,
            page_size=args.page_size,
            delay=args.delay,
            dry_run=bool(args.dry_run),
            resume_cursor=args.resume_cursor,
            repo_root=REPO_ROOT,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
