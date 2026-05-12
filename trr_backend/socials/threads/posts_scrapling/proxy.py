from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlparse


@dataclass(slots=True)
class ThreadsPostsProxyConfig:
    browser_proxy: str | dict[str, str] | None
    api_proxy_url: str | None
    proxy_rotator: Any | None
    fingerprint: str


def _build_proxy_rotator(browser_proxy: str | dict[str, str] | None) -> Any | None:
    if browser_proxy is None:
        return None
    try:
        from scrapling.fetchers import ProxyRotator
    except Exception:
        return None
    return ProxyRotator([browser_proxy])


def _decodo_env() -> tuple[str, str, str] | None:
    username = str(os.getenv("DECODO_USERNAME") or "").strip()
    password = str(os.getenv("DECODO_PASSWORD") or "").strip()
    gateway = str(os.getenv("DECODO_GATEWAY") or "gate.decodo.com:7000").strip()
    if not (username and password and gateway):
        return None
    return username, password, gateway


def select_threads_posts_proxy() -> ThreadsPostsProxyConfig | None:
    raw = str(os.getenv("SOCIAL_THREADS_POSTS_PROXY_URLS") or "").strip()
    explicit_urls = [value.strip() for value in raw.replace("\n", ",").split(",") if value.strip()]
    if explicit_urls:
        first_url = explicit_urls[0]
        parsed = urlparse(first_url)
        return ThreadsPostsProxyConfig(
            browser_proxy=first_url,
            api_proxy_url=first_url,
            proxy_rotator=_build_proxy_rotator(first_url),
            fingerprint=f"{parsed.hostname or 'unknown'}:{parsed.port or 0}:explicit",
        )

    provider = str(os.getenv("SOCIAL_THREADS_POSTS_PROXY_PROVIDER") or "").strip().lower()
    if provider in {"decodo", "smartproxy"}:
        creds = _decodo_env()
        if creds:
            username, password, gateway = creds
            browser_dict = {
                "server": f"http://{gateway}",
                "username": username,
                "password": password,
            }
            api_url = f"http://{quote(username, safe='')}:{quote(password, safe='')}@{gateway}"
            return ThreadsPostsProxyConfig(
                browser_proxy=browser_dict,
                api_proxy_url=api_url,
                proxy_rotator=_build_proxy_rotator(browser_dict),
                fingerprint=f"{gateway}:decodo",
            )

    return None
