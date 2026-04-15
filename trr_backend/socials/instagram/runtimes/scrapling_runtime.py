"""Scrapling-based Instagram runtime (SCAFFOLD - see TODO checklist).

Scrapling (https://github.com/D4Vinci/Scrapling) is a stealth fetcher with
auto-match selectors. For Instagram, it fits the GraphQL/`i/api/v1` JSON
endpoints best: fast, no browser, harder to fingerprint via canvas/WebGL.

STATUS: scaffold. The module structure, Protocol conformance, and error
mapping are in place. The actual fetch bodies raise NotImplementedError
because they depend on Scrapling APIs that change between versions - any
implementation written from memory would be wrong.

TODO before production use:
  1. Pin the installed version: `pip show scrapling`.
  2. Verify the StealthyFetcher / Fetcher import paths against that version.
     Current docs: https://github.com/D4Vinci/Scrapling/blob/main/README.md
  3. Implement `_fetch_json` using the current session/cookie API.
  4. Confirm Instagram's response shape hasn't drifted (the shape used in
     crawlee_runtime._node_to_post is a good starting reference).
  5. Add a contract test in tests/socials/instagram/runtimes/
     that runs against a recorded VCR cassette.
"""

from __future__ import annotations

import logging
from typing import Any

from trr_backend.socials.instagram.runtimes.protocol import (
    InstagramRuntime,
    Post,
    PostDetail,
    ProfileInfo,
    RuntimeHealth,
    RuntimeUnsupported,
)

logger = logging.getLogger(__name__)


class ScraplingRuntime:
    name = "scrapling"

    def __init__(self, cookies: dict[str, str] | None = None) -> None:
        self._cookies = dict(cookies or {})

    def healthcheck(self) -> RuntimeHealth:
        try:
            import scrapling  # noqa: F401
        except ImportError as exc:
            return RuntimeHealth(
                healthy=False,
                reason=f"scrapling_not_installed: pip install scrapling ({exc})",
            )
        return RuntimeHealth(healthy=True)

    async def fetch_profile(self, username: str) -> ProfileInfo:
        raise NotImplementedError(
            "ScraplingRuntime.fetch_profile: verify current Scrapling API "
            "and implement JSON fetch against "
            "https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
        )

    async def fetch_posts(self, username: str, *, limit: int) -> list[Post]:
        raise NotImplementedError(
            "ScraplingRuntime.fetch_posts: verify current Scrapling API "
            "and implement GraphQL pagination against the xdt_api__v1__feed "
            "connection."
        )

    async def fetch_post_detail(self, shortcode: str) -> PostDetail:
        # Post detail via permalink HTML is the fallback for when the
        # JSON endpoint 429s; Scrapling's auto-match selectors survive
        # DOM churn better than hand-authored CSS selectors.
        raise NotImplementedError(
            "ScraplingRuntime.fetch_post_detail: verify Scrapling selector "
            "API and implement permalink HTML parsing."
        )

    async def _fetch_json(self, url: str, *, params: dict[str, Any] | None = None) -> Any:
        """Helper hook. See module TODO."""
        raise NotImplementedError


# Protocol conformance check (runs at import; cheap).
assert isinstance(ScraplingRuntime(), InstagramRuntime)  # type: ignore[misc]

# Prevent RuntimeUnsupported from being flagged as unused (dispatcher imports it).
_ = RuntimeUnsupported
