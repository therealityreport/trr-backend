"""Proxy selection for the Instagram posts Scrapling lane.

Public API:
    select_posts_proxy() -> PostsProxyConfig | None

Mirrors comments_scrapling/proxy.py with posts-lane env vars.
SOCIAL_INSTAGRAM_POSTS_PROXY_URLS takes precedence over DECODO credentials.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlparse


@dataclass(slots=True)
class PostsProxyConfig:
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


def _build_proxy_rotator(browser_proxy: str | dict[str, str] | None) -> Any | None:
    if browser_proxy is None:
        return None
    try:
        from scrapling.fetchers import ProxyRotator
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Scrapling proxy rotation is unavailable. Install scrapling[fetchers].") from exc
    return ProxyRotator([browser_proxy])


def _decodo_env() -> tuple[str, str, str] | None:
    username = str(os.getenv("DECODO_USERNAME") or "").strip()
    password = str(os.getenv("DECODO_PASSWORD") or "").strip()
    gateway = str(os.getenv("DECODO_GATEWAY") or "gate.decodo.com:7000").strip()
    if not (username and password and gateway):
        return None
    return username, password, gateway


def select_posts_proxy() -> PostsProxyConfig | None:
    """Single entry point. Returns None when no proxy is configured."""
    # 1. Explicit proxy URLs take precedence.
    raw = str(os.getenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS") or "").strip()
    explicit_urls = [v.strip() for v in raw.replace("\n", ",").split(",") if v.strip()]
    if explicit_urls:
        first_url = explicit_urls[0]
        parsed = urlparse(first_url)
        return PostsProxyConfig(
            browser_proxy=first_url,
            api_proxy_url=first_url,
            proxy_rotator=_build_proxy_rotator(first_url),
            fingerprint=f"{parsed.hostname or 'unknown'}:{parsed.port or 0}:explicit",
        )

    # 2. DECODO credentials.
    provider = str(os.getenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER") or "").strip().lower()
    if provider in {"", "decodo", "smartproxy"}:
        creds = _decodo_env()
        if creds:
            username, password, gateway = creds
            browser_dict = {
                "server": f"http://{gateway}",
                "username": username,
                "password": password,
            }
            api_url = f"http://{quote(username, safe='')}:{quote(password, safe='')}@{gateway}"
            return PostsProxyConfig(
                browser_proxy=browser_dict,
                api_proxy_url=api_url,
                proxy_rotator=_build_proxy_rotator(browser_dict),
                fingerprint=f"{gateway}:decodo",
            )

    # 3. No proxy — local dev mode.
    return None
