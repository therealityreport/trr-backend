"""browser-use Instagram runtime - AUTH-RECOVERY ONLY (SCAFFOLD).

browser-use (https://github.com/browser-use/browser-use) lets an LLM drive
a Playwright browser via natural-language tasks. It's expensive and
non-deterministic - use ONLY for flows that dead-reckoning scrapers
cannot handle:
  - login-flow checkpoints ("confirm it's you")
  - consent-wall variants
  - A/B-tested challenge screens

Steady-state fetching (profile, posts, comments) must route through
Scrapling/Crawlee/crawl4ai. browser_use is a last-resort recovery path.

STATUS: scaffold. browser-use's `Agent` constructor, LLM provider
integration, and task specification format have changed multiple times.
Implementation bodies are NotImplementedError until a pinned version is
chosen and its API is confirmed.

TODO before production use:
  1. Pin version: `pip show browser-use`.
  2. Verify `Agent` signature. Docs:
     https://docs.browser-use.com/quickstart
  3. Choose LLM provider. Prefer Vercel AI Gateway if available; otherwise
     direct Anthropic via the `claude-opus-4-8` model (per CLAUDE.md
     default). Route via the Vercel AI Gateway if present.
  4. Implement `recover_from_checkpoint` - the ONLY exposed entry point.
     Do not expose fetch_profile/fetch_posts here; those should remain
     RuntimeUnsupported so the dispatcher never routes steady-state work
     through the LLM.
  5. Add per-call budget guard reading
     BROWSER_USE_MAX_COST_USD_PER_CALL env var.
  6. Log every run with full session trace to observability drain.
"""

from __future__ import annotations

import logging
import os

from trr_backend.socials.instagram.runtimes.protocol import (
    InstagramRuntime,
    Post,
    PostDetail,
    ProfileInfo,
    RuntimeHealth,
    RuntimeUnsupported,
)

logger = logging.getLogger(__name__)


class BrowserUseRuntime:
    name = "browser_use"

    def __init__(self, cookies: dict[str, str] | None = None) -> None:
        self._cookies = dict(cookies or {})

    def healthcheck(self) -> RuntimeHealth:
        try:
            import browser_use  # noqa: F401
        except ImportError as exc:
            return RuntimeHealth(
                healthy=False,
                reason=f"browser_use_not_installed: pip install browser-use ({exc})",
            )
        budget = os.getenv("BROWSER_USE_MAX_COST_USD_PER_CALL")
        if budget is None:
            return RuntimeHealth(
                healthy=False,
                reason="BROWSER_USE_MAX_COST_USD_PER_CALL not set - refusing to run without cost cap",
            )
        return RuntimeHealth(healthy=True)

    # Steady-state methods: intentionally unsupported.
    async def fetch_profile(self, username: str) -> ProfileInfo:
        raise RuntimeUnsupported(
            "BrowserUseRuntime is auth-recovery only; profile fetching must route through Scrapling/Crawlee/crawl4ai."
        )

    async def fetch_posts(self, username: str, *, limit: int) -> list[Post]:
        raise RuntimeUnsupported(
            "BrowserUseRuntime is auth-recovery only; post fetching must route through Scrapling/Crawlee/crawl4ai."
        )

    async def fetch_post_detail(self, shortcode: str) -> PostDetail:
        raise RuntimeUnsupported(
            "BrowserUseRuntime is auth-recovery only; post detail fetching "
            "must route through Scrapling/crawl4ai/Crawlee."
        )

    # Auth-recovery entry point (not in Protocol; called directly by
    # cookie_refresh.py when a checkpoint is detected).
    async def recover_from_checkpoint(self, checkpoint_url: str) -> dict[str, str]:
        """Return fresh cookies after solving an Instagram checkpoint.

        See module TODO #4 before implementing.
        """
        raise NotImplementedError(
            "BrowserUseRuntime.recover_from_checkpoint: implement with LLM "
            "agent driven at natural-language-task level. Guard with cost budget."
        )


assert isinstance(BrowserUseRuntime(), InstagramRuntime)  # type: ignore[misc]
