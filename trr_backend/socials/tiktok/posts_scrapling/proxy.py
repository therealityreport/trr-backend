"""Proxy selection for the TikTok posts Scrapling lane.

Public API: select_tiktok_posts_proxy() -> TikTokPostsProxyConfig | None

Same DECODO pattern as Instagram, env var: SOCIAL_TIKTOK_POSTS_PROXY_URLS.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlparse

from trr_backend.socials._scrapling_http_utils import env_truthy, resolve_positive_int_env
from trr_backend.socials.scrapling_transport import apply_decodo_session_affinity, build_proxy_rotator


@dataclass(slots=True)
class TikTokPostsProxyConfig:
    """Proxy config for one fetcher instance.

    browser_proxy: dict for Decodo (bypasses Scrapling URL-parsing bug),
                   raw URL str for explicit PROXY_URLS.
    api_proxy_url: URL-encoded string for httpx.
    proxy_rotator: pre-built ProxyRotator for StealthyFetcher.
    fingerprint:   "{host}:{port}:{provider}" — no credentials, safe to log.
    """

    browser_proxy: str | dict[str, str] | None
    api_proxy_url: str | None
    proxy_rotator: Any | None
    fingerprint: str
    session_mode: str = "rotating"


def _decodo_env() -> tuple[str, str, str] | None:
    username = str(os.getenv("DECODO_USERNAME") or "").strip()
    password = str(os.getenv("DECODO_PASSWORD") or "").strip()
    gateway = str(os.getenv("DECODO_GATEWAY") or "gate.decodo.com:7000").strip()
    if not (username and password and gateway):
        return None
    return username, password, gateway


def select_tiktok_posts_proxy(*, session_key: str | None = None) -> TikTokPostsProxyConfig | None:
    """Single entry point. Returns None when no proxy is configured."""
    # 1. Explicit proxy URLs take precedence.
    raw = str(os.getenv("SOCIAL_TIKTOK_POSTS_PROXY_URLS") or "").strip()
    explicit_urls = [v.strip() for v in raw.replace("\n", ",").split(",") if v.strip()]
    if explicit_urls:
        first_url = explicit_urls[0]
        parsed = urlparse(first_url)
        return TikTokPostsProxyConfig(
            browser_proxy=first_url,
            api_proxy_url=first_url,
            proxy_rotator=build_proxy_rotator(first_url),
            fingerprint=f"{parsed.hostname or 'unknown'}:{parsed.port or 0}:explicit",
            session_mode="explicit",
        )

    # 2. DECODO credentials.
    provider = str(os.getenv("SOCIAL_TIKTOK_POSTS_PROXY_PROVIDER") or "").strip().lower()
    if provider in {"decodo", "smartproxy"}:
        creds = _decodo_env()
        if creds:
            username, password, gateway = creds
            sticky_username, session_mode = apply_decodo_session_affinity(
                username,
                use_sticky_proxy=env_truthy("SOCIAL_TIKTOK_POSTS_USE_STICKY_PROXY", False),
                session_ttl_seconds=resolve_positive_int_env(
                    "SOCIAL_TIKTOK_POSTS_PROXY_SESSION_TTL_SECONDS",
                    600,
                    minimum=60,
                    maximum=86_400,
                ),
                session_id=session_key or os.getenv("SOCIAL_TIKTOK_POSTS_PROXY_SESSION_ID"),
            )
            browser_dict = {
                "server": f"http://{gateway}",
                "username": sticky_username,
                "password": password,
            }
            api_url = f"http://{quote(sticky_username, safe='')}:{quote(password, safe='')}@{gateway}"
            return TikTokPostsProxyConfig(
                browser_proxy=browser_dict,
                api_proxy_url=api_url,
                proxy_rotator=build_proxy_rotator(browser_dict),
                fingerprint=f"{gateway}:decodo",
                session_mode=session_mode,
            )

    # 3. No proxy — local dev mode.
    return None
