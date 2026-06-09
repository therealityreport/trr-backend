"""Compatibility wrapper + identity-pool seam for the posts Scrapling session."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from trr_backend.socials.instagram.auth_resolver import resolve_instagram_auth_session
from trr_backend.socials.instagram.identity_pool import (
    InstagramIdentityPool,
    InstagramIdentityPoolExhausted,
    InstagramScraperIdentity,
)
from trr_backend.socials.instagram.scrapling_session import (
    InstagramScraplingSession,
    cookies_to_scrapling,
    resolve_scrapling_session,
)

logger = logging.getLogger("socials.instagram.posts_scrapling.session")

InstagramPostsScraplingSession = InstagramScraplingSession


def resolve_posts_scrapling_session(
    *,
    browser_account_id: str | None,
    caller_context: str,
) -> InstagramScraplingSession:
    return resolve_scrapling_session(
        browser_account_id=browser_account_id,
        caller_context=caller_context,
        resolver=resolve_instagram_auth_session,
    )


# ---------------------------------------------------------------------------
# A3: multi-account identity pool seam (no-op at 1 identity).
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class PostsRotatedIdentity:
    """Identity payload the fetcher consumes on rotate_session.

    Shaped for InstagramPostsScraplingFetcher.rotate_session, which reads
    ``raw_cookies`` (dict), ``cookies`` (Scrapling list form) and
    ``browser_account_id``.
    """

    raw_cookies: dict[str, str]
    cookies: list[dict[str, Any]] = field(default_factory=list)
    browser_account_id: str | None = None
    session_id: str | None = None


def _identity_pool_enabled() -> bool:
    return str(os.getenv("SOCIAL_INSTAGRAM_IDENTITY_POOL_ENABLED") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _posts_identity_pool_proxy_urls() -> list[str]:
    # Mirrors scraper._build_identity_pool: the legacy pool keys identities off
    # SOCIAL_INSTAGRAM_PROXY_URLS. In the posts lane the live residential IP is
    # handled by Decodo sticky-session rotation (A2); the pool's role here is to
    # supply distinct identities (cookie sets) once more than one exists.
    raw = str(os.getenv("SOCIAL_INSTAGRAM_PROXY_URLS") or "")
    return [value.strip() for value in raw.split(",") if value.strip()]


def _build_posts_identity_pool(base_cookies: dict[str, str]) -> InstagramIdentityPool:
    return InstagramIdentityPool(
        proxy_urls=_posts_identity_pool_proxy_urls(),
        base_cookies=dict(base_cookies),
        max_requests=int(os.getenv("SOCIAL_INSTAGRAM_SESSION_MAX_REQUESTS") or "40"),
        max_age_seconds=int(os.getenv("SOCIAL_INSTAGRAM_SESSION_MAX_AGE_SECONDS") or "900"),
        max_generations=int(os.getenv("SOCIAL_INSTAGRAM_SESSION_MAX_GENERATIONS") or "2"),
        probe_timeout_seconds=float(os.getenv("SOCIAL_INSTAGRAM_PROXY_PROBE_TIMEOUT_SECONDS") or "5"),
    )


def _identity_to_payload(
    identity: InstagramScraperIdentity,
    *,
    browser_account_id: str | None,
) -> PostsRotatedIdentity:
    raw_cookies = {str(key): str(value) for key, value in (identity.cookies or {}).items() if value is not None}
    return PostsRotatedIdentity(
        raw_cookies=raw_cookies,
        cookies=cookies_to_scrapling(raw_cookies),
        browser_account_id=browser_account_id,
        session_id=identity.session_id,
    )


def build_posts_identity_provider(
    session: InstagramScraplingSession,
) -> Callable[[], PostsRotatedIdentity | None] | None:
    """Acquire-from-pool callable for the posts fetcher, or None when disabled.

    The provider acquires the *next* identity from the pool on each call, retiring
    the previously handed-out identity so ``acquire`` round-robins forward.
    Returns ``None`` (no provider wired) when the pool is disabled, so the fetcher
    keeps its single resolved identity. With one identity the pool yields one
    entry → every call returns the same cookie set → ``rotate_session`` is a
    no-op, exactly as required.
    """
    if not _identity_pool_enabled():
        return None

    base_cookies = {
        str(key): str(value)
        for key, value in (getattr(session.auth_session, "cookies", {}) or {}).items()
        if value is not None
    }
    if not base_cookies:
        return None

    try:
        pool = _build_posts_identity_pool(base_cookies)
    except Exception:  # noqa: BLE001 - pool construction must not wedge the lane
        logger.warning("instagram_posts identity_pool_build_failed", exc_info=True)
        return None

    state: dict[str, str | None] = {"active_session_id": None}

    def _provider() -> PostsRotatedIdentity | None:
        previous_session_id = state.get("active_session_id")
        if previous_session_id:
            # Retire the prior identity so acquire() advances to the next one.
            try:
                pool.retire(previous_session_id, reason="rotate_on_block", block_reason="auth_block")
            except KeyError:
                pass
        try:
            identity = pool.acquire()
        except InstagramIdentityPoolExhausted:
            logger.warning("instagram_posts identity_pool_exhausted account=%s", session.browser_account_id)
            return None
        state["active_session_id"] = identity.session_id
        return _identity_to_payload(identity, browser_account_id=session.browser_account_id)

    return _provider
