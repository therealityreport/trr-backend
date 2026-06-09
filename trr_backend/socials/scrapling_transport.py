"""Shared transport glue for TRR Scrapling fetchers.

This module intentionally stays below platform policy. It only owns fetcher
construction, cookie shape conversion/sync, proxy rotator construction, and
redaction-safe runtime metadata.
"""

from __future__ import annotations

import hashlib
import math
import re
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from trr_backend.socials._scrapling_http_utils import extract_response_cookies


DEFAULT_TIMEOUT_MS = 45_000
DEFAULT_MAX_TRANSIENT_RETRIES = 3
DEFAULT_BASE_BACKOFF_SECONDS = 1.0


@dataclass(frozen=True, slots=True)
class ScraplingTransportDefaults:
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    max_transient_retries: int = DEFAULT_MAX_TRANSIENT_RETRIES
    base_backoff_seconds: float = DEFAULT_BASE_BACKOFF_SECONDS


DEFAULT_TRANSPORT = ScraplingTransportDefaults()
SCRAPLING_PROXY_CONFLICT_REASON = "scrapling_proxy_conflict"
_DECODO_SESSION_PARAM_RE = re.compile(r"(?:^|-)session-[^-]+(?:-|$)")
_DECODO_SESSION_DURATION_RE = re.compile(r"(?:^|-)sessionduration-\d+(?:-|$)")


def scrapling_runtime_versions() -> dict[str, str | None]:
    """Return installed Scrapling transport package versions without importing Scrapling."""
    packages = ("scrapling", "patchright", "playwright")
    result: dict[str, str | None] = {}
    for package in packages:
        try:
            result[package] = version(package)
        except PackageNotFoundError:
            result[package] = None
    return result


def scrapling_runtime_metadata() -> dict[str, Any]:
    """Redaction-safe runtime metadata for Scrapling-backed jobs and canaries."""
    versions = scrapling_runtime_versions()
    return {
        "scrapling_version": versions["scrapling"],
        "patchright_version": versions["patchright"],
        "playwright_version": versions["playwright"],
    }


def apply_decodo_session_affinity(
    username: str,
    *,
    use_sticky_proxy: bool,
    session_ttl_seconds: int,
    session_id: str | None = None,
) -> tuple[str, str]:
    """Return a Decodo username with optional sticky-session parameters."""
    normalized = str(username or "").strip()
    if not normalized:
        return "", "rotating"

    has_session = bool(_DECODO_SESSION_PARAM_RE.search(normalized))
    has_duration = bool(_DECODO_SESSION_DURATION_RE.search(normalized))

    if has_session:
        return normalized, "sticky_preconfigured"
    if not use_sticky_proxy:
        return normalized, "rotating"

    normalized_session_id = str(session_id or "").strip().lower()
    if normalized_session_id:
        sticky_session_id = hashlib.sha256(normalized_session_id.encode("utf-8")).hexdigest()[:16]
    else:
        sticky_session_id = uuid.uuid4().hex[:12]
    ttl_minutes = max(1, min(1440, math.ceil(max(1, int(session_ttl_seconds)) / 60)))
    updated = f"{normalized}-session-{sticky_session_id}"
    if not has_duration:
        updated = f"{updated}-sessionduration-{ttl_minutes}"
    return updated, "sticky"


def assert_no_conflicting_scrapling_proxies(
    *,
    session_proxy: Any | None = None,
    request_proxy: Any | None = None,
    request_proxies: Any | None = None,
) -> None:
    """Fail early when Scrapling proxy modes would conflict.

    Scrapling 0.4.9 correctly applies session-level proxies and raises when
    callers mix them with per-request proxy overrides. TRR checks this at the
    shared boundary so social lanes fail with one stable reason string.
    """
    if session_proxy is None:
        return
    if request_proxy is not None or request_proxies is not None:
        raise ValueError(SCRAPLING_PROXY_CONFLICT_REASON)


def cookies_to_scrapling(
    cookies: Mapping[str, str] | None,
    domain: str,
    path: str = "/",
) -> list[dict[str, Any]]:
    """Convert a simple name/value cookie mapping into browser cookie records."""
    cookie_domain = str(domain or "").strip()
    cookie_path = str(path or "").strip() or "/"
    if not cookie_domain:
        raise ValueError("domain is required for Scrapling browser cookies")

    payload: list[dict[str, Any]] = []
    for name, value in (cookies or {}).items():
        cookie_name = str(name or "").strip()
        cookie_value = str(value or "").strip()
        if not (cookie_name and cookie_value):
            continue
        payload.append(
            {
                "name": cookie_name,
                "value": cookie_value,
                "domain": cookie_domain,
                "path": cookie_path,
            }
        )
    return payload


def _build_fetcher_class(class_name: str, **kwargs: Any) -> Any:
    assert_no_conflicting_scrapling_proxies(
        session_proxy=kwargs.get("proxy"),
        request_proxies=kwargs.get("proxies"),
    )
    try:
        import scrapling.fetchers as fetchers
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Scrapling fetchers are unavailable. Install scrapling[fetchers].") from exc

    try:
        fetcher_cls = getattr(fetchers, class_name)
    except AttributeError as exc:
        raise RuntimeError(f"Scrapling {class_name} is unavailable. Install scrapling[fetchers].") from exc
    return fetcher_cls(**kwargs)


def build_stealthy_fetcher(**kwargs: Any) -> Any:
    """Lazily construct Scrapling's browser-backed StealthyFetcher."""
    return _build_fetcher_class("StealthyFetcher", **kwargs)


def build_dynamic_fetcher(**kwargs: Any) -> Any:
    """Lazily construct Scrapling's DynamicFetcher."""
    return _build_fetcher_class("DynamicFetcher", **kwargs)


def build_fetcher(**kwargs: Any) -> Any:
    """Lazily construct Scrapling's basic Fetcher."""
    return _build_fetcher_class("Fetcher", **kwargs)


def build_proxy_rotator(
    proxies: Iterable[str | dict[str, str]] | str | dict[str, str] | None,
) -> Any | None:
    """Build a Scrapling ProxyRotator from one or more proxy definitions."""
    if proxies is None:
        return None
    if isinstance(proxies, str):
        normalized: list[str | dict[str, str]] = [proxies.strip()] if proxies.strip() else []
    elif isinstance(proxies, dict):
        normalized = [proxies] if proxies else []
    else:
        normalized = []
        for proxy in proxies:
            if isinstance(proxy, str):
                value = proxy.strip()
                if value:
                    normalized.append(value)
            elif isinstance(proxy, dict) and proxy:
                normalized.append(proxy)

    if not normalized:
        return None

    try:
        from scrapling.fetchers import ProxyRotator
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Scrapling proxy rotation is unavailable. Install scrapling[fetchers].") from exc
    return ProxyRotator(normalized)


def merge_response_cookies(raw_cookies: Mapping[str, str] | None, response: Any) -> dict[str, str]:
    """Merge response cookies into an existing raw cookie mapping."""
    merged = {str(name): str(value) for name, value in (raw_cookies or {}).items()}
    merged.update(extract_response_cookies(response))
    return merged


def _cookie_names(cookies: Any) -> list[str]:
    if isinstance(cookies, Mapping):
        values = cookies.keys()
    elif isinstance(cookies, Iterable) and not isinstance(cookies, (str, bytes)):
        names: list[str] = []
        for cookie in cookies:
            if isinstance(cookie, Mapping):
                name = str(cookie.get("name") or "").strip()
                if name:
                    names.append(name)
        return sorted(set(names))
    else:
        values = []
    return sorted({str(name).strip() for name in values if str(name or "").strip()})


def safe_cookie_metadata(
    seed_cookies: Any,
    warmup_delta: Any | None = None,
    *,
    prefix: str = "cookie",
) -> dict[str, Any]:
    """Return cookie diagnostics that expose names/counts but never values."""
    key_prefix = f"{prefix.strip()}_" if prefix.strip() else ""
    seed_names = _cookie_names(seed_cookies)
    warmup_names = _cookie_names(warmup_delta or {})
    return {
        f"{key_prefix}seed_cookie_names": seed_names,
        f"{key_prefix}seed_cookie_count": len(seed_names),
        f"{key_prefix}warmup_cookie_names": warmup_names,
        f"{key_prefix}warmup_cookie_count": len(warmup_names),
    }
