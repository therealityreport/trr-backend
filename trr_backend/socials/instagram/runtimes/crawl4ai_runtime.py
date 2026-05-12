"""crawl4ai-based Instagram runtime (SCAFFOLD - see TODO checklist).

crawl4ai (https://github.com/unclecode/crawl4ai) is an async Playwright
crawler with markdown-first extraction and LLM-assisted structured output.
Best for permalink HTML when the JSON endpoint is rate-limited.

STATUS: scaffold. Implementation bodies raise NotImplementedError because
crawl4ai's `AsyncWebCrawler` and extraction strategies have shifted
signatures across minor versions - any implementation written from memory
would likely fail.

TODO before production use:
  1. Pin the version: `pip show crawl4ai`.
  2. Verify `AsyncWebCrawler` + `CrawlerRunConfig` signatures. Docs:
     https://docs.crawl4ai.com/api/arun/ and
     https://docs.crawl4ai.com/extraction/llm-strategies/
  3. Implement `_crawl_html` using current async context-manager pattern.
  4. Choose between LLMExtractionStrategy (if we want structured
     extraction) vs. parsing the HTML ourselves. LLM is resilient to DOM
     churn but costs per request - gate via env var.
  5. Add a contract test with a recorded HTML fixture.
"""

from __future__ import annotations

import logging

from trr_backend.socials.instagram.runtimes.protocol import (
    InstagramRuntime,
    Post,
    PostDetail,
    ProfileInfo,
    RuntimeHealth,
    RuntimeUnsupported,
)

logger = logging.getLogger(__name__)


class Crawl4aiRuntime:
    name = "crawl4ai"

    def __init__(self, cookies: dict[str, str] | None = None) -> None:
        self._cookies = dict(cookies or {})

    def healthcheck(self) -> RuntimeHealth:
        try:
            import crawl4ai  # noqa: F401
        except ImportError as exc:
            return RuntimeHealth(
                healthy=False,
                reason=f"crawl4ai_not_installed: pip install crawl4ai ({exc})",
            )
        return RuntimeHealth(healthy=True)

    async def fetch_profile(self, username: str) -> ProfileInfo:
        raise NotImplementedError(
            "Crawl4aiRuntime.fetch_profile: profile HTML fallback; verify AsyncWebCrawler API and implement."
        )

    async def fetch_posts(self, username: str, *, limit: int) -> list[Post]:
        # HTML-scraped post listings are brittle; prefer the JSON path via
        # ScraplingRuntime. Raise RuntimeUnsupported so dispatcher falls
        # through rather than spinning up a browser unnecessarily.
        raise RuntimeUnsupported(
            "Crawl4aiRuntime.fetch_posts intentionally unsupported; the JSON "
            "path (ScraplingRuntime or CrawleeRuntime) is preferred."
        )

    async def fetch_post_detail(self, shortcode: str) -> PostDetail:
        raise NotImplementedError(
            "Crawl4aiRuntime.fetch_post_detail: primary use case. Implement "
            "permalink HTML crawl with either CSS extraction or LLM strategy."
        )


assert isinstance(Crawl4aiRuntime(), InstagramRuntime)  # type: ignore[misc]
