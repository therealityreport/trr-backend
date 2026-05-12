"""Proxy selection for the Instagram posts Scrapling lane.

Public API:
    select_posts_proxy(*, session_key: str | None = None) -> PostsProxyConfig | None

Mirrors comments_scrapling/proxy.py with posts-lane env vars.
SOCIAL_INSTAGRAM_POSTS_PROXY_URLS takes precedence over DECODO credentials.
Decodo is opt-in via SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER=decodo; credentials
alone must not silently move authenticated browser warmup onto a proxy.
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
    session_mode: str = "rotating"
    rotation_urls: tuple[str, ...] = ()
    rotation_index: int | None = None


@dataclass(slots=True, frozen=True)
class PostsProxyIdentity:
    """Redaction-safe proxy identity for diagnostics and future pacing keys."""

    configured_fingerprint: str
    observed_identity: str | None
    observed_fingerprint: str | None
    pacing_identity: str
    redacted_api_proxy_url: str | None
    redacted_browser_proxy: str | dict[str, str] | None
    session_mode: str

    def to_metadata(self) -> dict[str, Any]:
        return {
            "configured_fingerprint": self.configured_fingerprint,
            "observed_identity": self.observed_identity,
            "observed_fingerprint": self.observed_fingerprint,
            "pacing_identity": self.pacing_identity,
            "redacted_api_proxy_url": self.redacted_api_proxy_url,
            "redacted_browser_proxy": self.redacted_browser_proxy,
            "session_mode": self.session_mode,
        }


def _split_proxy_values(raw: str) -> list[str]:
    values: list[str] = []
    for chunk in str(raw or "").replace("\n", ",").split(","):
        value = chunk.strip()
        if value:
            values.append(value)
    return values


def _build_proxy_rotator(
    browser_proxy: str | dict[str, str] | list[str | dict[str, str]] | None,
) -> Any | None:
    if browser_proxy is None:
        return None
    try:
        from scrapling.fetchers import ProxyRotator
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Scrapling proxy rotation is unavailable. Install scrapling[fetchers].") from exc
    if isinstance(browser_proxy, list):
        normalized = [value for value in browser_proxy if value]
        if not normalized:
            return None
        return ProxyRotator(normalized)
    return ProxyRotator([browser_proxy])


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


def redact_proxy_url(url: str | None) -> str | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw
    host = parsed.hostname or "unknown"
    port = f":{parsed.port}" if parsed.port else ""
    auth = "***:***@" if parsed.username or parsed.password else ""
    return f"{parsed.scheme}://{auth}{host}{port}"


def redact_browser_proxy(proxy: str | dict[str, str] | None) -> str | dict[str, str] | None:
    if proxy is None:
        return None
    if isinstance(proxy, str):
        return redact_proxy_url(proxy)
    redacted: dict[str, str] = {}
    for key, value in proxy.items():
        normalized_key = str(key or "").strip().lower()
        if normalized_key in {"username", "password"}:
            redacted[str(key)] = "***"
        else:
            redacted[str(key)] = str(value)
    return redacted


def posts_proxy_feature_flags() -> dict[str, bool]:
    return {
        "per_ip_pacing_enabled": env_truthy("SOCIAL_INSTAGRAM_POSTS_PER_IP_PACING_ENABLED", False),
        "page_proxy_rotation_enabled": env_truthy("SOCIAL_INSTAGRAM_POSTS_PAGE_PROXY_ROTATION_ENABLED", False),
    }


def build_posts_proxy_identity(
    proxy_config: PostsProxyConfig | None,
    *,
    observed_identity: str | None = None,
    observed_fingerprint: str | None = None,
    per_ip_pacing_enabled: bool | None = None,
) -> PostsProxyIdentity:
    flags = posts_proxy_feature_flags()
    pacing_enabled = flags["per_ip_pacing_enabled"] if per_ip_pacing_enabled is None else bool(per_ip_pacing_enabled)
    configured_fingerprint = proxy_config.fingerprint if proxy_config else "none"
    observed_identity_value = str(observed_identity or "").strip() or None
    observed_fingerprint_value = str(observed_fingerprint or "").strip() or None
    observed_key = observed_fingerprint_value or observed_identity_value
    pacing_identity = observed_key or configured_fingerprint if pacing_enabled else "instagram:global"
    return PostsProxyIdentity(
        configured_fingerprint=configured_fingerprint,
        observed_identity=observed_identity_value,
        observed_fingerprint=observed_fingerprint_value,
        pacing_identity=pacing_identity,
        redacted_api_proxy_url=redact_proxy_url(proxy_config.api_proxy_url if proxy_config else None),
        redacted_browser_proxy=redact_browser_proxy(proxy_config.browser_proxy if proxy_config else None),
        session_mode=proxy_config.session_mode if proxy_config else "none",
    )


def _explicit_proxy_url_for_session(proxy_urls: list[str], session_key: str | None) -> str:
    urls = [str(url or "").strip() for url in proxy_urls if str(url or "").strip()]
    if not urls:
        raise ValueError("proxy_urls must contain at least one URL")
    normalized_key = str(session_key or "").strip().lower()
    if not normalized_key or len(urls) == 1:
        return urls[0]
    digest = hashlib.sha256(normalized_key.encode("utf-8")).hexdigest()
    return urls[int(digest[:16], 16) % len(urls)]


def _explicit_proxy_url_for_page(proxy_urls: list[str], page_index: int | None) -> tuple[str, int]:
    urls = [str(url or "").strip() for url in proxy_urls if str(url or "").strip()]
    if not urls:
        raise ValueError("proxy_urls must contain at least one URL")
    try:
        index = int(page_index or 0)
    except (TypeError, ValueError):
        index = 0
    selected_index = max(0, index) % len(urls)
    return urls[selected_index], selected_index


def _load_proxy_urls_from_env() -> list[str]:
    return _split_proxy_values(os.getenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS") or "")


def _decodo_env() -> tuple[str, str, str] | None:
    username = str(os.getenv("DECODO_USERNAME") or "").strip()
    password = str(os.getenv("DECODO_PASSWORD") or "").strip()
    gateway = str(os.getenv("DECODO_GATEWAY") or "gate.decodo.com:7000").strip()
    if not (username and password and gateway):
        return None
    return username, password, gateway


def select_posts_proxy(*, session_key: str | None = None, page_index: int | None = None) -> PostsProxyConfig | None:
    """Single entry point. Returns None when no proxy is configured."""
    # 1. Explicit proxy URLs take precedence.
    explicit_urls = _load_proxy_urls_from_env()
    if explicit_urls:
        flags = posts_proxy_feature_flags()
        rotation_index: int | None = None
        if flags["page_proxy_rotation_enabled"] and page_index is not None and len(explicit_urls) > 1:
            selected_url, rotation_index = _explicit_proxy_url_for_page(explicit_urls, page_index)
            session_mode = "explicit_page_rotation"
        else:
            selected_url = _explicit_proxy_url_for_session(explicit_urls, session_key)
            session_mode = "explicit_sharded" if len(explicit_urls) > 1 and session_key else "explicit"
        rotator = (
            _build_proxy_rotator(list(explicit_urls)) if len(explicit_urls) > 1 else _build_proxy_rotator(selected_url)
        )
        return PostsProxyConfig(
            browser_proxy=selected_url,
            api_proxy_url=selected_url,
            proxy_rotator=rotator,
            fingerprint=_fingerprint_from_url(selected_url),
            session_mode=session_mode,
            rotation_urls=tuple(explicit_urls),
            rotation_index=rotation_index,
        )

    # 2. Explicit Decodo provider. Credentials alone are not enough because a
    # stale residential proxy can make healthy auth cookies look blocked.
    provider = str(os.getenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER") or "").strip().lower()
    if provider in {"decodo", "smartproxy"}:
        creds = _decodo_env()
        if creds:
            username, password, gateway = creds
            sticky_username, session_mode = apply_decodo_session_affinity(
                username,
                use_sticky_proxy=env_truthy("SOCIAL_INSTAGRAM_POSTS_USE_STICKY_PROXY", False),
                session_ttl_seconds=resolve_positive_int_env(
                    "SOCIAL_INSTAGRAM_POSTS_PROXY_SESSION_TTL_SECONDS",
                    600,
                    minimum=60,
                    maximum=86_400,
                ),
                session_id=session_key,
            )
            browser_dict = {
                "server": f"http://{gateway}",
                "username": sticky_username,
                "password": password,
            }
            api_url = f"http://{quote(sticky_username, safe='')}:{quote(password, safe='')}@{gateway}"
            return PostsProxyConfig(
                browser_proxy=browser_dict,
                api_proxy_url=api_url,
                proxy_rotator=_build_proxy_rotator(browser_dict),
                fingerprint=_fingerprint_from_gateway(gateway, "decodo"),
                session_mode=session_mode,
            )

    # 3. No proxy — local dev mode.
    return None
