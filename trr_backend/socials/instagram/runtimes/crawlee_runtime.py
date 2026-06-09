"""Runtime wrapper around the existing Crawlee/Playwright Instagram path.

This runtime does *not* change any existing behavior - it adapts the
current `InstagramScraper` surface to the `InstagramRuntime` Protocol so
it can participate in the dispatcher priority chain.

Once the three alternative runtimes (Scrapling, crawl4ai, browser-use)
are implemented, the dispatcher reads `INSTAGRAM_RUNTIME_ORDER` and picks
the first healthy runtime for each endpoint.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from trr_backend.socials.instagram.runtimes.protocol import (
    InstagramRuntime,
    Post,
    PostDetail,
    ProfileInfo,
    RuntimeHealth,
    RuntimeUnsupported,
)

logger = logging.getLogger(__name__)


class CrawleeRuntime:
    """Adapter from `InstagramScraper` to the `InstagramRuntime` Protocol.

    We construct a lazily-initialized scraper on first use so healthcheck
    stays free of network IO.
    """

    name = "crawlee"

    def __init__(self, cookies: dict[str, str] | None = None) -> None:
        self._cookies = dict(cookies or {})
        self._scraper = None  # type: ignore[assignment]

    # ---------- Protocol methods ----------

    def healthcheck(self) -> RuntimeHealth:
        try:
            # Import lazily to keep module-load cheap.
            from trr_backend.socials.instagram.scraper import InstagramScraper  # noqa: F401
        except ImportError as exc:
            return RuntimeHealth(healthy=False, reason=f"import_failed:{exc}")
        return RuntimeHealth(healthy=True)

    async def fetch_profile(self, username: str) -> ProfileInfo:
        scraper = self._ensure_scraper()
        payload = await asyncio.to_thread(scraper.fetch_profile_info, username)
        payload_data = payload.get("data") if isinstance(payload, dict) else {}
        user = payload_data.get("user") if isinstance(payload_data, dict) else {}
        user = user or {}
        if not user:
            raise RuntimeUnsupported(f"CrawleeRuntime returned empty profile payload for {username}")
        return ProfileInfo(
            username=str(user.get("username") or username),
            user_id=str(user.get("id") or ""),
            full_name=(user.get("full_name") or None),
            biography=(user.get("biography") or None),
            follower_count=_safe_int(user.get("edge_followed_by", {}).get("count")),
            following_count=_safe_int(user.get("edge_follow", {}).get("count")),
            post_count=_safe_int(user.get("edge_owner_to_timeline_media", {}).get("count")),
            is_private=bool(user.get("is_private")),
            is_verified=bool(user.get("is_verified")),
        )

    async def fetch_posts(self, username: str, *, limit: int) -> list[Post]:
        scraper = self._ensure_scraper()
        payload = await asyncio.to_thread(scraper.fetch_posts_graphql, username)
        payload_data = payload.get("data") if isinstance(payload, dict) else {}
        connection = (
            payload_data.get("xdt_api__v1__feed__user_timeline_graphql_connection", {})
            if isinstance(payload_data, dict)
            else {}
        )
        edges = (connection.get("edges") or [])[:limit]
        posts: list[Post] = []
        for edge in edges:
            node = edge.get("node") or {}
            posts.append(_node_to_post(node))
        return posts

    async def fetch_post_detail(self, shortcode: str) -> PostDetail:
        # The existing scraper has permalink/metadata paths but they are
        # interleaved with state. For now, raise RuntimeUnsupported so the
        # dispatcher falls through to a runtime that implements it cleanly.
        # TODO: wire `permalink_metadata.fetch_permalink_metadata`.
        raise RuntimeUnsupported("CrawleeRuntime.fetch_post_detail not yet wired")

    # ---------- internals ----------

    def _ensure_scraper(self):
        if self._scraper is None:
            from trr_backend.socials.instagram.scraper import InstagramScraper

            self._scraper = InstagramScraper(cookies=self._cookies)
        return self._scraper


def _safe_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _node_to_post(node: dict) -> Post:
    shortcode = str(node.get("shortcode") or node.get("code") or "")
    caption = None
    caption_edges = node.get("edge_media_to_caption", {}).get("edges") or []
    if caption_edges:
        caption = caption_edges[0].get("node", {}).get("text")
    posted_at: datetime | None = None
    taken_at = node.get("taken_at_timestamp") or node.get("taken_at")
    if taken_at:
        try:
            posted_at = datetime.fromtimestamp(int(taken_at), tz=UTC)
        except (TypeError, ValueError):
            posted_at = None
    media_type = "unknown"
    typename = str(node.get("__typename") or "").lower()
    if "video" in typename:
        media_type = "video"
    elif "sidecar" in typename or "carousel" in typename:
        media_type = "carousel"
    elif "image" in typename or "photo" in typename:
        media_type = "image"
    display_url = node.get("display_url")
    media_urls: tuple[str, ...] = (str(display_url),) if display_url else ()
    return Post(
        shortcode=shortcode,
        caption=caption,
        posted_at=posted_at,
        media_type=media_type,
        media_urls=media_urls,
        like_count=_safe_int(node.get("edge_liked_by", {}).get("count")),
        comment_count=_safe_int(node.get("edge_media_to_comment", {}).get("count")),
    )


# Module-level guard: confirm CrawleeRuntime satisfies the Protocol at import.
# Runs in production but only costs a set-membership check.
assert isinstance(CrawleeRuntime(), InstagramRuntime)  # type: ignore[misc]
