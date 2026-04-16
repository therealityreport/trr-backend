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
import json
import logging
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

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

    from trr_backend.socials.instagram.scraper import InstagramScraper, ScrapeConfig

    if not args.dry_run:
        from trr_backend.socials.control_plane import (
            _batch_upsert_shared_catalog_instagram_posts,
        )

    scrape_config = ScrapeConfig(username=account_handle)

    cookie_file = REPO_ROOT / "data" / "instagram_cookies.json"
    cookies = {}
    if cookie_file.is_file():
        raw = json.loads(cookie_file.read_text())
        cookies = {k: v for k, v in raw.items() if not k.startswith("_")}
    logger.info("Loaded %d cookies", len(cookies))

    scraper = InstagramScraper(cookies=cookies)
    cursor = args.resume_cursor
    total_posts = 0
    total_saved = 0
    page = 0
    start_time = time.monotonic()

    mode = "DRY RUN" if args.dry_run else "LIVE (saving to DB)"
    logger.info(
        "Starting direct catalog backfill for @%s [%s] (max_pages=%d, page_size=%d, delay=%.1fs)",
        account_handle,
        mode,
        args.max_pages,
        args.page_size,
        args.delay,
    )
    if cursor:
        logger.info("Resuming from cursor: %s", cursor[:40])

    while page < args.max_pages:
        page += 1
        logger.info("--- Page %d (cursor=%s) ---", page, (cursor or "START")[:30])

        result = scraper.fetch_posts_graphql(
            account_handle,
            cursor=cursor,
            delay=args.delay,
            request_timeout=30,
            fast_mode=False,
            allow_browser_fallback=True,
            page_size=args.page_size,
        )

        transport = scraper.last_retrieval_meta.get("transport", "unknown")

        if result is None:
            logger.error("Page %d returned None — stopping. Meta: %s", page, scraper.last_retrieval_meta)
            if cursor:
                logger.info("To resume, run with: --resume-cursor '%s'", cursor)
            break

        # Parse posts using the scraper's own methods
        page_posts = []
        page_info = {}
        for node, pi in scraper._iter_posts_from_graphql(result):
            page_info = pi
            page_posts.append(scraper._parse_post_node(node, scrape_config))

        has_next = page_info.get("has_next_page", False)
        next_cursor = page_info.get("end_cursor")
        post_count = len(page_posts)
        total_posts += post_count

        # Preview first post
        first_preview = ""
        if page_posts:
            first_preview = (page_posts[0].caption or "")[:60]

        elapsed = time.monotonic() - start_time
        rate = total_posts / elapsed if elapsed > 0 else 0

        logger.info(
            "Page %d: %d posts (total=%d, %.1f posts/s), transport=%s, has_next=%s, preview='%s'",
            page,
            post_count,
            total_posts,
            rate,
            transport,
            has_next,
            first_preview,
        )

        # Save to database using batch upsert (single INSERT for the whole page)
        if page_posts and not args.dry_run:
            try:
                saved_rows = _batch_upsert_shared_catalog_instagram_posts(
                    run_id=None,
                    account_handle=account_handle,
                    posts=page_posts,
                )
                saved_count = len(saved_rows)
                total_saved += saved_count
                logger.info(
                    "Page %d: saved %d/%d posts to DB (total saved=%d)", page, saved_count, post_count, total_saved
                )
            except Exception:
                logger.exception("Page %d: DB upsert failed — continuing to next page", page)

        if not has_next or not next_cursor:
            logger.info("Reached end of feed at page %d", page)
            break

        cursor = next_cursor
        time.sleep(args.delay)

    elapsed = time.monotonic() - start_time
    logger.info(
        "=== DONE: %d pages, %d total posts scraped, %d saved to DB (%.0fs elapsed) ===",
        page,
        total_posts,
        total_saved,
        elapsed,
    )
    if cursor and page >= args.max_pages:
        logger.info("Hit max-pages limit. To continue: --resume-cursor '%s'", cursor)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
