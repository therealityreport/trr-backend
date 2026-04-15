"""Runtime dispatcher: picks the first healthy runtime that supports the
requested endpoint, falling through on `RuntimeUnsupported`.

Order is read from `INSTAGRAM_RUNTIME_ORDER` (comma-separated), defaulting
to `"crawlee,scrapling,crawl4ai,browser_use,apify"`. Disabled runtimes
listed in `INSTAGRAM_RUNTIME_DISABLED` are skipped.

Other failures (network errors, rate limits) propagate up - retry and
identity rotation live in the runtimes themselves.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable

from trr_backend.socials.instagram.runtimes.protocol import (
    InstagramRuntime,
    Post,
    PostDetail,
    ProfileInfo,
    RuntimeUnsupported,
)

logger = logging.getLogger(__name__)

_DEFAULT_ORDER = "crawlee,scrapling,crawl4ai,browser_use"


def _parse_env_list(env_var: str) -> list[str]:
    value = os.getenv(env_var, "")
    return [part.strip() for part in value.split(",") if part.strip()]


class InstagramRuntimeDispatcher:
    def __init__(
        self,
        factories: dict[str, Callable[[], InstagramRuntime]],
        *,
        order: list[str] | None = None,
        disabled: set[str] | None = None,
    ) -> None:
        self._factories = factories
        self._order = order or _parse_env_list("INSTAGRAM_RUNTIME_ORDER") or _DEFAULT_ORDER.split(",")
        self._disabled = disabled or set(_parse_env_list("INSTAGRAM_RUNTIME_DISABLED"))
        self._cache: dict[str, InstagramRuntime] = {}

    def _iter_runtimes(self) -> list[InstagramRuntime]:
        result: list[InstagramRuntime] = []
        for name in self._order:
            if name in self._disabled:
                continue
            factory = self._factories.get(name)
            if factory is None:
                logger.debug("instagram runtime %s has no factory; skipping", name)
                continue
            if name not in self._cache:
                try:
                    self._cache[name] = factory()
                except Exception:  # noqa: BLE001 - construction best-effort
                    logger.warning("instagram runtime %s failed to construct", name,
                                   exc_info=True)
                    continue
            runtime = self._cache[name]
            health = runtime.healthcheck()
            if not health.healthy:
                logger.debug("instagram runtime %s unhealthy: %s", name, health.reason)
                continue
            result.append(runtime)
        return result

    async def fetch_profile(self, username: str) -> ProfileInfo:
        last_unsupported: RuntimeUnsupported | None = None
        for runtime in self._iter_runtimes():
            try:
                return await runtime.fetch_profile(username)
            except RuntimeUnsupported as exc:
                last_unsupported = exc
                continue
        raise RuntimeUnsupported(
            f"no runtime supports fetch_profile({username!r}): {last_unsupported}"
        )

    async def fetch_posts(self, username: str, *, limit: int) -> list[Post]:
        last_unsupported: RuntimeUnsupported | None = None
        for runtime in self._iter_runtimes():
            try:
                return await runtime.fetch_posts(username, limit=limit)
            except RuntimeUnsupported as exc:
                last_unsupported = exc
                continue
        raise RuntimeUnsupported(
            f"no runtime supports fetch_posts({username!r}, limit={limit}): {last_unsupported}"
        )

    async def fetch_post_detail(self, shortcode: str) -> PostDetail:
        last_unsupported: RuntimeUnsupported | None = None
        for runtime in self._iter_runtimes():
            try:
                return await runtime.fetch_post_detail(shortcode)
            except RuntimeUnsupported as exc:
                last_unsupported = exc
                continue
        raise RuntimeUnsupported(
            f"no runtime supports fetch_post_detail({shortcode!r}): {last_unsupported}"
        )
