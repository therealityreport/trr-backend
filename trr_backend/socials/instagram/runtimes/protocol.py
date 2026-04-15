"""Runtime Protocol for pluggable Instagram scrapers.

The Protocol lets us swap backends without touching caller code. Every
runtime must satisfy the same shape; the dispatcher (in
`instagram/runtimes/dispatcher.py`) picks a runtime per endpoint and falls
through on `RuntimeUnsupported` or classified failures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol, runtime_checkable

# ---------- Shared DTOs ----------


@dataclass(frozen=True)
class ProfileInfo:
    username: str
    user_id: str
    full_name: str | None = None
    biography: str | None = None
    follower_count: int | None = None
    following_count: int | None = None
    post_count: int | None = None
    is_private: bool = False
    is_verified: bool = False


@dataclass(frozen=True)
class Post:
    shortcode: str
    caption: str | None = None
    posted_at: datetime | None = None
    media_type: str = "unknown"  # image | video | carousel | unknown
    media_urls: tuple[str, ...] = field(default_factory=tuple)
    like_count: int | None = None
    comment_count: int | None = None


@dataclass(frozen=True)
class PostDetail:
    post: Post
    hashtags: tuple[str, ...] = field(default_factory=tuple)
    mentions: tuple[str, ...] = field(default_factory=tuple)
    permalink: str | None = None


@dataclass(frozen=True)
class RuntimeHealth:
    healthy: bool
    reason: str | None = None


class RuntimeUnsupported(Exception):  # noqa: N818 - sentinel, not error
    """Raised when a runtime cannot service the requested endpoint.

    The dispatcher catches this and falls through to the next runtime in
    the priority chain. Distinct from a real fetch failure (which should
    raise an appropriate scraping exception).
    """


# ---------- Protocol ----------


@runtime_checkable
class InstagramRuntime(Protocol):
    """Contract every Instagram runtime must satisfy.

    Implementations:
        - runtimes.crawlee_runtime.CrawleeRuntime (wrapping existing code)
        - runtimes.scrapling_runtime.ScraplingRuntime (stub; see file TODO)
        - runtimes.crawl4ai_runtime.Crawl4aiRuntime (stub; see file TODO)
        - runtimes.browser_use_runtime.BrowserUseRuntime (stub; see file TODO)

    Runtimes may raise `RuntimeUnsupported` from any endpoint method; the
    dispatcher will try the next runtime. Other exceptions (particularly
    `InstagramRequestFailure`) propagate so retry/classification logic
    upstream can act on them.
    """

    name: str

    def healthcheck(self) -> RuntimeHealth:
        """Cheap sanity probe (e.g. deps importable, creds present).

        Must not perform network IO. Called before dispatch; unhealthy
        runtimes are skipped.
        """
        ...

    async def fetch_profile(self, username: str) -> ProfileInfo:
        """Return profile metadata for a public username."""
        ...

    async def fetch_posts(self, username: str, *, limit: int) -> list[Post]:
        """Return up to `limit` most-recent posts for `username`."""
        ...

    async def fetch_post_detail(self, shortcode: str) -> PostDetail:
        """Return the full post (caption, tags, media) for a shortcode."""
        ...
