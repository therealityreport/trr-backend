"""Public-mode policy for Instagram comment scraping.

The comments pipeline has three places that must agree before a job is truly
public: launch config, runner config, and proxy selection. Keep the shared
rules here so old cursor-api jobs cannot silently drift back into a
cookie/proxy lane when the account scrape mode is public-first.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

PUBLIC_COMMENTS_SCRAPE_MODE = "public_first"
PUBLIC_COMMENTS_LOAD_STRATEGY = "public_relay"
AUTHENTICATED_COMMENTS_CURSOR_LOAD_STRATEGY = "instagram_comments_endpoint_cursor"
INSTAGRAM_SCRAPE_MODE_ENV = "SOCIAL_INSTAGRAM_SCRAPE_MODE"
COMMENTS_PROXY_PROVIDER_ENV = "SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER"

_PUBLIC_MODE_ALIASES = frozenset(
    {
        "",
        "anonymous",
        "logged_out",
        "no-auth",
        "no_auth",
        "no-login",
        "no_login",
        "nologin",
        "public",
        "public-first",
        "public_first",
        "public_relay",
    }
)
_AUTHENTICATED_MODE_ALIASES = frozenset(
    {
        "auth",
        "authenticated",
        "cookie",
        "cookies",
        "login",
        "logged_in",
        "private",
    }
)
_NO_PROXY_PROVIDER_ALIASES = frozenset({"0", "direct", "disabled", "false", "none", "off", "no_proxy"})


def _normalize_mode(value: Any) -> str:
    return str(value or "").strip().lower()


def comments_public_mode_from_config(config: Mapping[str, Any] | None = None) -> bool:
    """Return True when a comments job must run without login cookies or proxies."""
    data = dict(config or {})
    load_strategy = _normalize_mode(data.get("comments_load_strategy"))
    if load_strategy == PUBLIC_COMMENTS_LOAD_STRATEGY:
        return True
    if load_strategy in {"cursor_api", AUTHENTICATED_COMMENTS_CURSOR_LOAD_STRATEGY, "single_session_load_all"}:
        return False

    explicit_mode = data.get("instagram_scrape_mode") or data.get("scrape_mode") or data.get("comments_scrape_mode")
    if explicit_mode is None:
        explicit_mode = os.getenv(INSTAGRAM_SCRAPE_MODE_ENV) or PUBLIC_COMMENTS_SCRAPE_MODE
    normalized_mode = _normalize_mode(explicit_mode)
    if normalized_mode in _AUTHENTICATED_MODE_ALIASES:
        return False
    return normalized_mode in _PUBLIC_MODE_ALIASES


def comments_load_strategy_for_mode(
    requested_strategy: Any,
    *,
    public_mode: bool,
) -> str:
    """Use the public relay fetcher whenever the comments job is public."""
    normalized = _normalize_mode(requested_strategy or AUTHENTICATED_COMMENTS_CURSOR_LOAD_STRATEGY)
    if normalized == "cursor_api":
        normalized = AUTHENTICATED_COMMENTS_CURSOR_LOAD_STRATEGY
    return PUBLIC_COMMENTS_LOAD_STRATEGY if public_mode else normalized


def comments_proxy_provider_disabled() -> bool:
    """Return True when the comments lane explicitly refuses proxy use."""
    provider = _normalize_mode(os.getenv(COMMENTS_PROXY_PROVIDER_ENV))
    return provider in _NO_PROXY_PROVIDER_ALIASES


def comments_proxy_provider_name() -> str:
    return _normalize_mode(os.getenv(COMMENTS_PROXY_PROVIDER_ENV))


class PublicCommentsModeViolation(RuntimeError):
    """Raised when a public comments job would touch a proxy, cookies, or auth.

    The ``public_relay`` comments lane must never use Decodo, login cookies, or
    an authenticated fallback. Today that isolation is purely structural (the
    job runner sets ``proxy_config=None`` and empty cookies for public mode),
    so a future refactor could silently re-enable a proxy/auth path with no
    failure. This makes the invariant explicit and enforced.

    It checks RESOLVED state, not env presence: ``DECODO_*`` env vars legitimately
    exist for the authenticated lanes, so the guard must not fail merely because
    Decodo credentials are configured for some other job.
    """

    def __init__(self, violations: list[str]) -> None:
        self.violations = list(violations)
        super().__init__(
            "Public comments isolation breached (Decodo/cookies/auth must not be used in the "
            "public_relay lane): " + "; ".join(self.violations)
        )


def assert_public_comments_isolation(
    *,
    proxy_config: Any,
    session: Any,
    account_handle: str | None = None,
) -> dict[str, Any]:
    """Hard guard: a public comments job must run with no proxy, cookies, or auth.

    Returns an ``instagram_access_proof`` dict on success; raises
    :class:`PublicCommentsModeViolation` if any proxy/cookie/auth state leaked
    into a public run. Call this at the point where the public session and proxy
    config have been resolved, immediately before constructing the fetcher.
    """
    violations: list[str] = []

    if proxy_config is not None:
        fingerprint = getattr(proxy_config, "fingerprint", None)
        violations.append(f"proxy_config is set (fingerprint={fingerprint!r})")
        if getattr(proxy_config, "api_proxy_url", None):
            violations.append("proxy_config.api_proxy_url is set")

    if getattr(session, "cookies", None):
        violations.append("session.cookies is non-empty")

    auth_session = getattr(session, "auth_session", None)
    if auth_session is not None:
        if getattr(auth_session, "cookies", None):
            violations.append("auth_session.cookies is non-empty")
        source = str(getattr(auth_session, "source", "") or "").strip().lower()
        if source and source != "public":
            violations.append(f"auth_session.source={source!r} (expected 'public')")

    if violations:
        raise PublicCommentsModeViolation(violations)

    return {
        "no_cookies": True,
        "no_proxy": True,
        "no_auth_fallback": True,
        "proxy_state": "none",
        "auth_state": "public",
        "proxy_provider_disabled": comments_proxy_provider_disabled(),
    }
