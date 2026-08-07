"""Proxy selection for the SocialBlade Scrapling lane."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import quote, urlparse

from trr_backend.socials._scrapling_http_utils import env_truthy, resolve_positive_int_env
from trr_backend.socials.instagram._proxy_sessions import apply_decodo_session_affinity


@dataclass(slots=True)
class SocialBladeProxyConfig:
    browser_proxy: str | dict[str, str] | None
    api_proxy_url: str | None
    proxy_rotator: Any | None
    fingerprint: str
    session_mode: str = "rotating"


def _split_proxy_values(raw: str) -> list[str]:
    values: list[str] = []
    for chunk in str(raw or "").replace("\n", ",").split(","):
        value = chunk.strip()
        if value:
            values.append(value)
    return values


def _build_proxy_rotator(browser_proxy: str | dict[str, str] | Sequence[str | dict[str, str]] | None) -> Any | None:
    if browser_proxy is None:
        return None
    try:
        from scrapling.fetchers import ProxyRotator
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Scrapling proxy rotation is unavailable. Install scrapling[fetchers].") from exc
    if isinstance(browser_proxy, list):
        normalized = [value for value in cast("Sequence[Any]", browser_proxy) if value]
        return ProxyRotator(normalized) if normalized else None
    return ProxyRotator(cast("list[Any]", [browser_proxy]))


def _fingerprint_from_gateway(gateway: str, provider: str) -> str:
    return f"{gateway}:{provider}"


def _fingerprint_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or "unknown"
    try:
        port = parsed.port or 0
    except ValueError:
        port = 0
    return f"{host}:{port}:explicit"


def _explicit_proxy_url_for_session(proxy_urls: list[str], session_key: str | None) -> str:
    urls = [str(url or "").strip() for url in proxy_urls if str(url or "").strip()]
    if not urls:
        raise ValueError("proxy_urls must contain at least one URL")
    normalized_key = str(session_key or "").strip().lower()
    if not normalized_key or len(urls) == 1:
        return urls[0]
    digest = hashlib.sha256(normalized_key.encode("utf-8")).hexdigest()
    return urls[int(digest[:16], 16) % len(urls)]


def _decodo_env() -> tuple[str, str, str] | None:
    username = str(os.getenv("DECODO_USERNAME") or "").strip()
    password = str(os.getenv("DECODO_PASSWORD") or "").strip()
    gateway = str(os.getenv("DECODO_GATEWAY") or "gate.decodo.com:7000").strip()
    if not (username and password and gateway):
        return None
    return username, password, gateway


def select_socialblade_proxy(*, session_key: str | None = None) -> SocialBladeProxyConfig | None:
    explicit_urls = _split_proxy_values(os.getenv("SOCIALBLADE_PROXY_URLS") or "")
    if explicit_urls:
        selected_url = _explicit_proxy_url_for_session(explicit_urls, session_key)
        return SocialBladeProxyConfig(
            browser_proxy=selected_url,
            api_proxy_url=selected_url,
            proxy_rotator=_build_proxy_rotator(explicit_urls if len(explicit_urls) > 1 else selected_url),
            fingerprint=_fingerprint_from_url(selected_url),
            session_mode="explicit_sharded" if len(explicit_urls) > 1 and session_key else "explicit",
        )

    provider = str(os.getenv("SOCIALBLADE_PROXY_PROVIDER") or "").strip().lower()
    if provider in {"decodo", "smartproxy"}:
        creds = _decodo_env()
        if creds:
            username, password, gateway = creds
            sticky_username, session_mode = apply_decodo_session_affinity(
                username,
                use_sticky_proxy=env_truthy("SOCIALBLADE_USE_STICKY_PROXY", False),
                session_ttl_seconds=resolve_positive_int_env(
                    "SOCIALBLADE_PROXY_SESSION_TTL_SECONDS",
                    600,
                    minimum=60,
                    maximum=86_400,
                ),
                session_id=session_key,
            )
            browser_proxy = {
                "server": f"http://{gateway}",
                "username": sticky_username,
                "password": password,
            }
            return SocialBladeProxyConfig(
                browser_proxy=browser_proxy,
                api_proxy_url=f"http://{quote(sticky_username, safe='')}:{quote(password, safe='')}@{gateway}",
                proxy_rotator=_build_proxy_rotator(browser_proxy),
                fingerprint=_fingerprint_from_gateway(gateway, "decodo"),
                session_mode=session_mode,
            )

    return None
