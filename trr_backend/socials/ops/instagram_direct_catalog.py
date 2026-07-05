"""Reusable direct Instagram catalog backfill operation."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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


def _make_instagram_scraper(*, cookies: dict[str, str]) -> Any:
    from trr_backend.socials.instagram.scraper import InstagramScraper

    return InstagramScraper(cookies=cookies)


def _make_scrape_config(*, username: str) -> Any:
    from trr_backend.socials.instagram.scraper import ScrapeConfig

    return ScrapeConfig(username=username)


def _load_batch_upsert() -> Any:
    from trr_backend.socials.instagram.persistence import _batch_upsert_shared_catalog_instagram_posts

    return _batch_upsert_shared_catalog_instagram_posts


_EMPTY_SOFT_BLOCK_MARKERS = (
    "soft_block",
    "soft-block",
    "blocked",
    "checkpoint",
    "challenge",
    "login",
    "auth_failed",
    "forbidden",
    "unauthorized",
    "rate_limited",
    "empty_response",
    "empty_payload",
    "graphql_empty_or_error",
    "temporarily_unavailable",
)


def _soft_block_empty_reason(meta: dict[str, Any]) -> str | None:
    for key in (
        "error_code",
        "request_error_code",
        "stop_reason",
        "fallback_reason",
        "session_block_reason",
        "error_message",
        "redirect_target",
    ):
        value = str(meta.get(key) or "").strip()
        normalized = value.lower()
        if value and any(marker in normalized for marker in _EMPTY_SOFT_BLOCK_MARKERS):
            return value
    return None


def _retryable_from_meta(meta: dict[str, Any], *, default: bool) -> bool:
    value = meta.get("retryable")
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def run_direct_instagram_catalog_backfill(options: DirectInstagramCatalogBackfillOptions) -> dict[str, object]:
    account_handle = options.account.strip().lstrip("@").lower()
    if not account_handle:
        raise ValueError("account_required")

    batch_upsert = None
    if not options.dry_run:
        batch_upsert = _load_batch_upsert()

    scrape_config = _make_scrape_config(username=account_handle)
    repo_root = options.repo_root or Path.cwd()
    cookies = _load_repo_cookie_file(repo_root)
    logger.info("Loaded %d cookies", len(cookies))

    scraper = _make_instagram_scraper(cookies=cookies)
    cursor = options.resume_cursor
    total_posts = 0
    total_saved = 0
    db_errors = 0
    error_code: str | None = None
    error_message: str | None = None
    retryable = False
    status = "completed"
    stop_reason = "completed"
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
            meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
            soft_block_reason = _soft_block_empty_reason(meta)
            if soft_block_reason:
                status = "blocked"
                stop_reason = "soft_block_empty_page"
                error_code = "instagram_direct_catalog_soft_block_empty_page"
                error_message = soft_block_reason
                retryable = _retryable_from_meta(meta, default=True)
                logger.error(
                    "Page %d returned soft-block empty result; stopping. Reason=%s Meta: %s",
                    page,
                    soft_block_reason,
                    meta,
                )
            else:
                status = "failed"
                stop_reason = "empty_result"
                error_code = "instagram_direct_catalog_empty_result"
                error_message = "Instagram GraphQL returned no page payload."
                retryable = True
                logger.error("Page %d returned None; stopping. Meta: %s", page, meta)
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
        soft_block_reason = _soft_block_empty_reason(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))

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

        if not page_posts and soft_block_reason:
            meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
            status = "blocked"
            stop_reason = "soft_block_empty_page"
            error_code = "instagram_direct_catalog_soft_block_empty_page"
            error_message = soft_block_reason
            retryable = _retryable_from_meta(meta, default=True)
            logger.error("Page %d was empty with soft-block signal '%s'; stopping", page, soft_block_reason)
            break

        if page_posts and batch_upsert is not None:
            try:
                saved_rows = batch_upsert(run_id=None, account_handle=account_handle, posts=page_posts)
                saved_count = len(saved_rows)
                total_saved += saved_count
                logger.info(
                    "Page %d: saved %d/%d posts to DB (total saved=%d)", page, saved_count, post_count, total_saved
                )
            except Exception:
                db_errors += 1
                status = "failed"
                stop_reason = "db_upsert_failed"
                error_code = "instagram_direct_catalog_db_upsert_failed"
                error_message = "DB upsert failed for the current page; stopped before advancing cursor."
                retryable = True
                logger.exception("Page %d: DB upsert failed; stopping before advancing cursor", page)
                break

        if not has_next or not next_cursor:
            logger.info("Reached end of feed at page %d", page)
            stop_reason = "end_of_feed"
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
        "status": status,
        "pages": page,
        "total_posts": total_posts,
        "total_saved": total_saved,
        "db_errors": db_errors,
        "error_code": error_code,
        "error_message": error_message,
        "retryable": retryable,
        "stop_reason": stop_reason,
        "elapsed_seconds": elapsed,
        "resume_cursor": cursor if cursor and (page >= options.max_pages or status != "completed") else None,
    }
