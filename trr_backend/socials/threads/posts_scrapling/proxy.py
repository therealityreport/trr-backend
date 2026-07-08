from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlparse

from trr_backend.socials._scrapling_http_utils import env_truthy, resolve_positive_int_env
from trr_backend.socials.scrapling_transport import apply_decodo_session_affinity, build_proxy_rotator


@dataclass(slots=True)
class ThreadsPostsProxyConfig:
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


def select_threads_posts_proxy(*, session_key: str | None = None) -> ThreadsPostsProxyConfig | None:
    raw = str(os.getenv("SOCIAL_THREADS_POSTS_PROXY_URLS") or "").strip()
    explicit_urls = [value.strip() for value in raw.replace("\n", ",").split(",") if value.strip()]
    if explicit_urls:
        first_url = explicit_urls[0]
        parsed = urlparse(first_url)
        return ThreadsPostsProxyConfig(
            browser_proxy=first_url,
            api_proxy_url=first_url,
            proxy_rotator=build_proxy_rotator(first_url),
            fingerprint=f"{parsed.hostname or 'unknown'}:{parsed.port or 0}:explicit",
            session_mode="explicit",
        )

    provider = str(os.getenv("SOCIAL_THREADS_POSTS_PROXY_PROVIDER") or "").strip().lower()
    if provider in {"0", "false", "none", "off", "disabled"}:
        return None
    if provider in {"decodo", "smartproxy"}:
        creds = _decodo_env()
        if creds:
            username, password, gateway = creds
            sticky_username, session_mode = apply_decodo_session_affinity(
                username,
                use_sticky_proxy=env_truthy("SOCIAL_THREADS_POSTS_USE_STICKY_PROXY", False),
                session_ttl_seconds=resolve_positive_int_env(
                    "SOCIAL_THREADS_POSTS_PROXY_SESSION_TTL_SECONDS",
                    600,
                    minimum=60,
                    maximum=86_400,
                ),
                session_id=session_key or os.getenv("SOCIAL_THREADS_POSTS_PROXY_SESSION_ID"),
            )
            browser_dict = {
                "server": f"http://{gateway}",
                "username": sticky_username,
                "password": password,
            }
            api_url = f"http://{quote(sticky_username, safe='')}:{quote(password, safe='')}@{gateway}"
            return ThreadsPostsProxyConfig(
                browser_proxy=browser_dict,
                api_proxy_url=api_url,
                proxy_rotator=build_proxy_rotator(browser_dict),
                fingerprint=f"{gateway}:decodo",
                session_mode=session_mode,
            )

    return None
