#!/usr/bin/env python3
"""Test Instagram scraper browser fallback directly — bypasses catalog pipeline.

Usage:
    python scripts/socials/instagram/test_browser_fallback.py
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv
load_dotenv(REPO_ROOT / ".env", override=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
# Quiet the DB pool noise
logging.getLogger("trr_backend.db.pg").setLevel(logging.WARNING)

logger = logging.getLogger("test_browser_fallback")


def main() -> int:
    from trr_backend.socials.instagram.scraper import InstagramScraper

    cookie_file = REPO_ROOT / "data" / "instagram_cookies.json"
    cookies = {}
    if cookie_file.is_file():
        raw = json.loads(cookie_file.read_text())
        cookies = {k: v for k, v in raw.items() if not k.startswith("_")}
        logger.info("Loaded %d cookies from %s", len(cookies), cookie_file)

    scraper = InstagramScraper(cookies=cookies)

    logger.info("Testing fetch_posts_graphql for @bravotv with browser fallback ENABLED")
    logger.info("If HTTP gets 403, it should fall through to Playwright browser fallback")

    result = scraper.fetch_posts_graphql(
        "bravotv",
        cursor=None,  # Start from the beginning (page 1)
        delay=2.5,
        request_timeout=30,
        fast_mode=False,
        allow_browser_fallback=True,
        page_size=12,
    )

    if result is not None:
        posts = result.get("posts") or result.get("edges") or []
        cursor = result.get("end_cursor") or result.get("next_cursor")
        has_next = result.get("has_next_page")
        transport = scraper.last_retrieval_meta.get("transport", "unknown")
        logger.info(
            "SUCCESS: %d posts, has_next=%s, cursor=%s..., transport=%s",
            len(posts) if isinstance(posts, list) else 0,
            has_next,
            str(cursor or "")[:30],
            transport,
        )
        return 0
    else:
        logger.error("FAILED: fetch_posts_graphql returned None")
        logger.error("Last meta: %s", scraper.last_retrieval_meta)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
