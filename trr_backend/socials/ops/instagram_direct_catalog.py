"""Reusable direct Instagram catalog backfill operation."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger("direct_catalog_backfill")


@dataclass(frozen=True)
class DirectInstagramCatalogBackfillOptions:
    account: str
    max_pages: int = 600
    page_size: int = 33
    delay: float = 2.5
    dry_run: bool = False
    resume_cursor: str | None = None
    repo_root: Path | None = None


def _load_repo_cookie_file(repo_root: Path) -> dict[str, str]:
    cookie_file = repo_root / "data" / "instagram_cookies.json"
    if not cookie_file.is_file():
        return {}
    raw = json.loads(cookie_file.read_text())
    return {str(k): str(v) for k, v in raw.items() if not str(k).startswith("_") and v is not None}


def run_direct_instagram_catalog_backfill(options: DirectInstagramCatalogBackfillOptions) -> dict[str, object]:
    account_handle = options.account.strip().lstrip("@").lower()
    if not account_handle:
        raise ValueError("account_required")

    from trr_backend.socials.instagram.scraper import InstagramScraper, ScrapeConfig

    batch_upsert = None
    if not options.dry_run:
        from trr_backend.socials.instagram.persistence import _batch_upsert_shared_catalog_instagram_posts

        batch_upsert = _batch_upsert_shared_catalog_instagram_posts

    scrape_config = ScrapeConfig(username=account_handle)
    repo_root = options.repo_root or Path.cwd()
    cookies = _load_repo_cookie_file(repo_root)
    logger.info("Loaded %d cookies", len(cookies))

    scraper = InstagramScraper(cookies=cookies)
    cursor = options.resume_cursor
    total_posts = 0
    total_saved = 0
    page = 0
    start_time = time.monotonic()

    mode = "DRY RUN" if options.dry_run else "LIVE (saving to DB)"
    logger.info(
        "Starting direct catalog backfill for @%s [%s] (max_pages=%d, page_size=%d, delay=%.1fs)",
        account_handle,
        mode,
        options.max_pages,
        options.page_size,
        options.delay,
    )
    if cursor:
        logger.info("Resuming from cursor: %s", cursor[:40])

    while page < options.max_pages:
        page += 1
        logger.info("--- Page %d (cursor=%s) ---", page, (cursor or "START")[:30])

        result = scraper.fetch_posts_graphql(
            account_handle,
            cursor=cursor,
            delay=options.delay,
            request_timeout=30,
            fast_mode=False,
            allow_browser_fallback=True,
            page_size=options.page_size,
        )

        transport = scraper.last_retrieval_meta.get("transport", "unknown")
        if result is None:
            logger.error("Page %d returned None; stopping. Meta: %s", page, scraper.last_retrieval_meta)
            if cursor:
                logger.info("To resume, run with: --resume-cursor '%s'", cursor)
            break

        page_posts = []
        page_info = {}
        for node, page_info_candidate in scraper._iter_posts_from_graphql(result):
            page_info = page_info_candidate
            page_posts.append(scraper._parse_post_node(node, scrape_config))

        has_next = page_info.get("has_next_page", False)
        next_cursor = page_info.get("end_cursor")
        post_count = len(page_posts)
        total_posts += post_count

        first_preview = (page_posts[0].caption or "")[:60] if page_posts else ""
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

        if page_posts and batch_upsert is not None:
            try:
                saved_rows = batch_upsert(run_id=None, account_handle=account_handle, posts=page_posts)
                saved_count = len(saved_rows)
                total_saved += saved_count
                logger.info(
                    "Page %d: saved %d/%d posts to DB (total saved=%d)", page, saved_count, post_count, total_saved
                )
            except Exception:
                logger.exception("Page %d: DB upsert failed; continuing to next page", page)

        if not has_next or not next_cursor:
            logger.info("Reached end of feed at page %d", page)
            break

        cursor = next_cursor
        time.sleep(options.delay)

    elapsed = time.monotonic() - start_time
    logger.info(
        "=== DONE: %d pages, %d total posts scraped, %d saved to DB (%.0fs elapsed) ===",
        page,
        total_posts,
        total_saved,
        elapsed,
    )
    if cursor and page >= options.max_pages:
        logger.info("Hit max-pages limit. To continue: --resume-cursor '%s'", cursor)
    return {
        "account": account_handle,
        "pages": page,
        "total_posts": total_posts,
        "total_saved": total_saved,
        "elapsed_seconds": elapsed,
        "resume_cursor": cursor if cursor and page >= options.max_pages else None,
    }

