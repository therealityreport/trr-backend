from __future__ import annotations

import os
import time
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse

import requests

BRANDFETCH_API_BASE_URL = "https://api.brandfetch.io/v2"
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_READ_TIMEOUT_SECONDS = 20.0
DEFAULT_RETRY_ATTEMPTS = 2
DEFAULT_RETRY_BACKOFF_MS = 300


class BrandfetchError(RuntimeError):
    """Base error for Brandfetch lookups."""


class BrandfetchAuthError(BrandfetchError):
    """Raised when Brandfetch auth is unavailable or rejected."""


class BrandfetchNotFoundError(BrandfetchError):
    """Raised when Brandfetch has no record for a domain."""


class BrandfetchRequestError(BrandfetchError):
    """Raised for transient/network/HTTP request failures."""


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_domain(value: str | None) -> str | None:
    text = _normalize_text(value).lower()
    if not text:
        return None

    parsed = urlparse(text if "://" in text else f"https://{text}")
    host = _normalize_text(parsed.netloc or parsed.path).lower()
    if host.startswith("www."):
        host = host[4:]
    if not host or "." not in host:
        return None
    return host


def _resolve_api_key(api_key: str | None = None) -> str:
    value = _normalize_text(api_key or os.getenv("BRANDFETCH_API_KEY"))
    if not value:
        raise BrandfetchAuthError("brandfetch_auth_missing")
    return value


def _timeout_seconds(timeout_seconds: float | None = None) -> float:
    if isinstance(timeout_seconds, (int, float)) and timeout_seconds > 0:
        return float(timeout_seconds)
    raw = _normalize_text(os.getenv("BRANDFETCH_TIMEOUT_SEC"))
    if raw:
        try:
            parsed = float(raw)
            if parsed > 0:
                return parsed
        except ValueError:
            pass
    return DEFAULT_READ_TIMEOUT_SECONDS


def _timeout_tuple(timeout_seconds: float | None = None) -> tuple[float, float]:
    read_timeout = _timeout_seconds(timeout_seconds)
    connect_timeout = min(DEFAULT_CONNECT_TIMEOUT_SECONDS, read_timeout)
    return connect_timeout, read_timeout


def _retry_attempts() -> int:
    raw = _normalize_text(os.getenv("BRANDFETCH_RETRY_ATTEMPTS"))
    if raw:
        try:
            parsed = int(raw)
            if parsed > 0:
                return min(parsed, 5)
        except ValueError:
            pass
    return DEFAULT_RETRY_ATTEMPTS


def _retry_backoff_ms() -> int:
    raw = _normalize_text(os.getenv("BRANDFETCH_RETRY_BACKOFF_MS"))
    if raw:
        try:
            parsed = int(raw)
            if parsed >= 0:
                return min(parsed, 5_000)
        except ValueError:
            pass
    return DEFAULT_RETRY_BACKOFF_MS


def _is_retryable_status(status_code: int) -> bool:
    return status_code == 429 or status_code >= 500


def _format_rank(fmt: Mapping[str, Any]) -> tuple[int, int]:
    fmt_name = _normalize_text(fmt.get("format")).lower()
    src = _normalize_text(fmt.get("src") or fmt.get("url") or "").lower()
    width = int(fmt.get("width") or 0) if isinstance(fmt.get("width"), int) else 0

    # Prefer raster assets first because downstream mirroring normalizes to PNG and
    # some environments cannot rasterize SVG dependencies at runtime.
    score = 100
    if fmt_name == "png" or ".png" in src:
        score = 0
    elif fmt_name == "webp" or ".webp" in src:
        score = 5
    elif fmt_name == "svg" or ".svg" in src:
        score = 15
    elif fmt_name in {"jpeg", "jpg"} or ".jpg" in src or ".jpeg" in src:
        score = 30

    if "transparent" in src:
        score -= 2
    if "logo" in src:
        score -= 1

    # Higher width is usually better for raster formats.
    return score, -width


def fetch_brandfetch_logo_candidates(
    domain: str,
    *,
    api_key: str | None = None,
    timeout_seconds: float | None = None,
    session: requests.Session | None = None,
) -> list[str]:
    normalized_domain = normalize_domain(domain)
    if not normalized_domain:
        raise BrandfetchRequestError("brandfetch_invalid_domain")

    key = _resolve_api_key(api_key)
    timeout = _timeout_tuple(timeout_seconds)
    retry_attempts = _retry_attempts()
    retry_backoff_ms = _retry_backoff_ms()

    session = session or requests.Session()
    url = f"{BRANDFETCH_API_BASE_URL}/brands/{normalized_domain}"

    response: requests.Response | None = None
    last_error: BrandfetchRequestError | None = None
    for attempt in range(retry_attempts):
        try:
            response = session.get(
                url,
                headers={
                    "accept": "application/json",
                    "authorization": f"Bearer {key}",
                    "user-agent": "TRR-Backend/1.0",
                },
                timeout=timeout,
            )
        except (requests.ConnectTimeout, requests.ReadTimeout, requests.Timeout) as exc:
            last_error = BrandfetchRequestError("brandfetch_timeout")
            if attempt + 1 < retry_attempts:
                if retry_backoff_ms > 0:
                    time.sleep(retry_backoff_ms / 1000)
                continue
            raise last_error from exc
        except requests.RequestException as exc:
            last_error = BrandfetchRequestError("brandfetch_request_failed")
            if attempt + 1 < retry_attempts:
                if retry_backoff_ms > 0:
                    time.sleep(retry_backoff_ms / 1000)
                continue
            raise last_error from exc

        if _is_retryable_status(response.status_code) and attempt + 1 < retry_attempts:
            if retry_backoff_ms > 0:
                time.sleep(retry_backoff_ms / 1000)
            continue
        break

    if response is None:
        if last_error is not None:
            raise last_error
        raise BrandfetchRequestError("brandfetch_request_failed")

    if response.status_code in {401, 403}:
        raise BrandfetchAuthError("brandfetch_auth_missing")
    if response.status_code == 404:
        raise BrandfetchNotFoundError("brandfetch_not_found")
    if response.status_code >= 400:
        raise BrandfetchRequestError(f"brandfetch_http_{response.status_code}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise BrandfetchRequestError("brandfetch_invalid_json") from exc

    logos = payload.get("logos") if isinstance(payload, Mapping) else None
    if not isinstance(logos, list):
        raise BrandfetchNotFoundError("brandfetch_not_found")

    ranked: list[tuple[tuple[int, int], str]] = []
    seen: set[str] = set()
    for logo in logos:
        if not isinstance(logo, Mapping):
            continue
        formats = logo.get("formats")
        if not isinstance(formats, list):
            continue
        for fmt in formats:
            if not isinstance(fmt, Mapping):
                continue
            src = _normalize_text(fmt.get("src") or fmt.get("url"))
            if not src or src in seen:
                continue
            seen.add(src)
            ranked.append((_format_rank(fmt), src))

    if not ranked:
        raise BrandfetchNotFoundError("brandfetch_not_found")

    ranked.sort(key=lambda item: item[0])
    return [url for _, url in ranked]
