"""Pluggable Instagram scraping runtimes.

Rationale (.claude/plans/fancy-beaming-dijkstra.md -> "Fallback Runtimes"):
the existing Crawlee/Playwright path is flaky on Instagram's anti-bot
axis. Rather than replace it, we route per-endpoint across multiple
runtimes and fall through on classified failures.

Primary routing order (read from the INSTAGRAM_RUNTIME_ORDER env var):
    crawlee,scrapling,crawl4ai,browser_use,apify

Endpoint -> preferred runtime matrix:

    Profile JSON          Scrapling   -> Crawlee   -> crawl4ai
    Post detail           Scrapling   -> crawl4ai  -> Crawlee
    Comments pagination   Crawlee     -> crawl4ai  -> Scrapling
    Login/challenge       browser-use -> manual
    Bulk backfill         Apify       -> Scrapling -> Crawlee

See individual runtime modules for implementation status. Runtimes that
wrap external packages are scaffolded with NotImplementedError until their
current-version APIs are verified via docs; do not call them in production
without completing the TODO checklist in each file.
"""

from __future__ import annotations

from trr_backend.socials.instagram.runtimes.protocol import (
    InstagramRuntime,
    Post,
    PostDetail,
    ProfileInfo,
    RuntimeHealth,
    RuntimeUnsupported,
)

__all__ = [
    "InstagramRuntime",
    "Post",
    "PostDetail",
    "ProfileInfo",
    "RuntimeHealth",
    "RuntimeUnsupported",
]
