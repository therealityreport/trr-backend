"""Bounded anonymous profile listing through TikTok's own browser requests."""

from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import parse_qs, urlsplit

from trr_backend.socials.scrapling_transport import build_stealthy_fetcher
from trr_backend.socials.tiktok.models import TikTokPost, TikTokScrapeConfig


def scrape_public_profile_posts(
    *, account_handle: str, max_pages: int, page_size: int
) -> tuple[list[TikTokPost], dict[str, Any]]:
    """Capture native listing pages without attaching the saved login session.

    An explicit page budget is required. Responses are registered before the
    navigation. Prefetched responses beyond the budget are ignored; comments are
    blocked, and the browser context closes when the listing budget is reached.
    No direct API requests, custom signing, or third-party provider is used.
    """
    from trr_backend.socials.tiktok.scraper import TikTokScraper

    if not re.fullmatch(r"[A-Za-z0-9_.]{1,64}", account_handle) or max_pages < 1 or page_size < 1:
        raise ValueError("A valid account and positive public listing budget are required")
    cap = max_pages * page_size
    parser = TikTokScraper(cookies={})
    config = TikTokScrapeConfig(username=account_handle, max_pages=max_pages)
    posts: list[TikTokPost] = []
    seen_ids: set[str] = set()
    cursors: set[str] = set()
    pages = 0
    failures = 0
    rejected_owners = 0
    checked = 0
    exhausted = False
    error_type: str | None = None
    warmup_status: int | None = None

    def at_budget() -> bool:
        return pages >= max_pages or len(posts) >= cap or exhausted

    def setup(page: Any) -> None:
        def response_received(response: Any) -> None:
            nonlocal pages, failures, rejected_owners, checked, exhausted
            url = urlsplit(response.url)
            host = (url.hostname or "").lower()
            if (
                not (host == "tiktok.com" or host.endswith(".tiktok.com"))
                or url.path != "/api/post/item_list/"
                or at_budget()
            ):
                return
            try:
                payload = response.json() if response.ok else None
            except Exception:
                payload = None
            if not isinstance(payload, dict) or payload.get("statusCode") != 0:
                failures += 1
                return
            items = payload.get("itemList")
            if not isinstance(items, list):
                failures += 1
                return
            cursor = str((parse_qs(url.query).get("cursor") or ["0"])[0])
            if cursor in cursors:
                return
            cursors.add(cursor)
            pages += 1
            exhausted = not bool(payload.get("hasMore", True))
            for item in items:
                if len(posts) >= cap:
                    break
                if not isinstance(item, dict):
                    continue
                checked += 1
                author = item.get("author")
                owner = str(author.get("uniqueId") or "") if isinstance(author, dict) else ""
                if owner.casefold() != account_handle.casefold():
                    rejected_owners += 1
                    continue
                video_id = str(item.get("id") or "")
                if not video_id.isdecimal() or video_id in seen_ids:
                    continue
                try:
                    post = parser._parse_post_item(item, config)  # noqa: SLF001 - canonical native-item parser
                except (TypeError, ValueError, KeyError, OverflowError):
                    failures += 1
                    continue
                if post.create_time <= 0 or post.video_id != video_id:
                    continue
                seen_ids.add(video_id)
                posts.append(post)

        page.route("**/api/comment/**", lambda route: route.abort())
        page.on("response", response_received)

    def collect(page: Any) -> None:
        deadline = time.monotonic() + min(120, 30 + max_pages * 5)
        next_scroll = time.monotonic() + 3
        while not at_budget() and time.monotonic() < deadline:
            page.wait_for_timeout(250)
            if not at_budget() and time.monotonic() >= next_scroll:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                next_scroll = time.monotonic() + 2

    try:
        response = build_stealthy_fetcher().fetch(
            f"https://www.tiktok.com/@{account_handle}",
            headless=True,
            network_idle=False,
            load_dom=True,
            timeout=45_000,
            retries=1,
            cookies=[],
            page_setup=setup,
            page_action=collect,
        )
        warmup_status = int(response.status)
    except Exception as exc:
        # Neither browser exception strings nor response URLs belong in job metadata.
        error_type = type(exc).__name__
    stop_reason = (
        "completed"
        if exhausted
        else "max_posts_reached"
        if len(posts) >= cap and pages < max_pages
        else "max_pages"
        if at_budget()
        else "public_listing_incomplete"
    )
    # A timeout or browser error must not turn an unfinished traversal into success.
    if error_type or not at_budget():
        posts = []
    return posts, {
        "retrieval_mode": "public_browser",
        "auth_mode": "public_anonymous",
        "pages_scanned": pages,
        "posts_checked": checked,
        "posts_found": len(posts),
        "post_cap": cap,
        "page_budget": max_pages,
        "response_failures": failures,
        "owner_rejections": rejected_owners,
        "stop_reason": stop_reason,
        "error_type": error_type,
        "warmup_status_code": warmup_status,
        "fallback_chain": ["yt_dlp", "public_browser"],
    }
