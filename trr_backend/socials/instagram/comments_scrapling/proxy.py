"""Proxy selection for the Instagram comments Scrapling lane.

Public API:
    select_comments_proxy(*, session_key: str | None = None) -> CommentsProxyConfig | None

Everything else is internal. Callers (job_runner, CLI) use only
select_comments_proxy(). ProxyRotator construction and URL building
are private helpers.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlparse

from trr_backend.socials._scrapling_http_utils import env_truthy, resolve_positive_int_env
from trr_backend.socials.instagram._proxy_sessions import apply_decodo_session_affinity


@dataclass(slots=True)
class CommentsProxyConfig:
    """One proxy per fetcher. Both fields point to the same upstream so
    browser warmup and httpx API calls share an IP.

    browser_proxy: dict for Decodo (bypasses Scrapling's construct_proxy_dict
                   URL-parsing bug), raw URL str for explicit PROXY_URLS.
    api_proxy_url: URL-encoded string for httpx (handles decoding internally).
    proxy_rotator: pre-built ProxyRotator for StealthyFetcher.async_fetch().
    fingerprint:   "{host}:{port}:{provider}" — no credentials, safe to log.
    """

    browser_proxy: str | dict[str, str] | None
    api_proxy_url: str | None
    proxy_rotator: Any | None
    fingerprint: str
    session_mode: str = "rotating"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _split_proxy_values(raw: str) -> list[str]:
    values: list[str] = []
    for chunk in str(raw or "").replace("\n", ",").split(","):
        value = chunk.strip()
        if value:
            values.append(value)
    return values


def _build_proxy_rotator(browser_proxy: str | dict[str, str] | None) -> Any | None:
    """Build a ProxyRotator for the browser warmup path. Accepts both dict
    (Decodo — bypasses URL-parsing bug) and str (explicit PROXY_URLS).
    """
    if browser_proxy is None:
        return None
    try:
        from scrapling.fetchers import ProxyRotator
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Scrapling proxy rotation is unavailable. Install scrapling[fetchers].") from exc
    return ProxyRotator([browser_proxy])


def _fingerprint_from_gateway(gateway: str, provider: str) -> str:
    """host:port:provider — never includes credentials."""
    return f"{gateway}:{provider}"


def _fingerprint_from_url(url: str) -> str:
    """Extract host:port from a proxy URL. No credentials."""
    parsed = urlparse(url)
    host = parsed.hostname or "unknown"
    port = parsed.port or 0
    return f"{host}:{port}:explicit"


def _explicit_proxy_url_for_session(proxy_urls: list[str], session_key: str | None) -> str:
    """Pick one explicit proxy URL deterministically for a session/shard key."""
    urls = [str(url or "").strip() for url in proxy_urls if str(url or "").strip()]
    if not urls:
        raise ValueError("proxy_urls must contain at least one URL")
    normalized_key = str(session_key or "").strip().lower()
    if not normalized_key or len(urls) == 1:
        return urls[0]
    digest = hashlib.sha256(normalized_key.encode("utf-8")).hexdigest()
    return urls[int(digest[:16], 16) % len(urls)]


# ---------------------------------------------------------------------------
# Internal env readers
# ---------------------------------------------------------------------------


def _load_proxy_urls_from_env() -> list[str]:
    """Read explicit proxy URLs from env. Returns list[str] only."""
    proxy_urls = _split_proxy_values(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS") or "")
    return proxy_urls


def _decodo_env() -> tuple[str, str, str] | None:
    """Read Decodo credentials from env. Returns (username, password, gateway) or None."""
    username = str(os.getenv("DECODO_USERNAME") or "").strip()
    password = str(os.getenv("DECODO_PASSWORD") or "").strip()
    gateway = str(os.getenv("DECODO_GATEWAY") or "gate.decodo.com:7000").strip()
    if not (username and password and gateway):
        return None
    return username, password, gateway


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def select_comments_proxy(*, session_key: str | None = None) -> CommentsProxyConfig | None:
    """Single entry point for all proxy needs. Returns None when no proxy
    is configured (local-dev cookies-only mode).

    For Decodo:
      browser_proxy = dict (raw password, bypasses construct_proxy_dict bug)
      api_proxy_url = URL-encoded string (httpx handles decoding)

    For explicit PROXY_URLS:
      browser_proxy = first URL string (ProxyRotator parses it)
      api_proxy_url = same URL string
    """
    # 1. Explicit proxy URLs take precedence.
    explicit_urls = _load_proxy_urls_from_env()
    if explicit_urls:
        selected_url = _explicit_proxy_url_for_session(explicit_urls, session_key)
        rotator = _build_proxy_rotator(selected_url)
        return CommentsProxyConfig(
            browser_proxy=selected_url,
            api_proxy_url=selected_url,
            proxy_rotator=rotator,
            fingerprint=_fingerprint_from_url(selected_url),
            session_mode="explicit_sharded" if len(explicit_urls) > 1 and session_key else "explicit",
        )

    # 2. Decodo provider (default when env has credentials).
    provider = str(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER") or "").strip().lower()
    if provider in {"", "decodo", "smartproxy"}:
        creds = _decodo_env()
        if creds:
            username, password, gateway = creds
            sticky_username, session_mode = apply_decodo_session_affinity(
                username,
                use_sticky_proxy=env_truthy("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", False),
                session_ttl_seconds=resolve_positive_int_env(
                    "SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS",
                    600,
                    minimum=60,
                    maximum=86_400,
                ),
                session_id=session_key,
            )
            browser_dict: dict[str, str] = {
                "server": f"http://{gateway}",
                "username": sticky_username,
                "password": password,
            }
            api_url = f"http://{quote(sticky_username, safe='')}:{quote(password, safe='')}@{gateway}"
            rotator = _build_proxy_rotator(browser_dict)
            return CommentsProxyConfig(
                browser_proxy=browser_dict,
                api_proxy_url=api_url,
                proxy_rotator=rotator,
                fingerprint=_fingerprint_from_gateway(gateway, "decodo"),
                session_mode=session_mode,
            )

    # 3. No proxy configured — local dev mode.
    return None
