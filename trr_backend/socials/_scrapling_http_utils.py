"""Shared pure helpers for Scrapling-based fetchers.

Extracted from byte-identical copies in the Instagram and TikTok
posts_scrapling fetchers. The comments_scrapling fetcher still inlines
its own copies of these helpers; migrating it to import from this module
is tracked for Phase E.

These helpers handle the tiny response-shape differences between httpx
(the post-warmup transport) and Scrapling (the warmup transport).

Kept small and side-effect-free so they can be imported into any lane
without triggering scrapling/httpx module-level costs.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse


def env_truthy(name: str, default: bool) -> bool:
    """Parse an env var as a boolean, tolerating whitespace and common casings."""
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def resolve_positive_int_env(name: str, default: int, *, minimum: int, maximum: int) -> int:
    """Parse an env var as a bounded positive integer, falling back to default."""
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def resolve_positive_float_env(name: str, default: float, *, minimum: float, maximum: float) -> float:
    """Parse an env var as a bounded positive float, falling back to default."""
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def response_text(response: Any) -> str:
    """Extract body text from a response object. Handles both the str attribute
    that httpx uses and the zero-arg callable some test mocks return."""
    text = getattr(response, "text", "")
    if callable(text):
        try:
            return str(text() or "")
        except Exception:  # noqa: BLE001
            return ""
    if text:
        return str(text)

    body = getattr(response, "body", None)
    if isinstance(body, bytes):
        return body.decode("utf-8", errors="replace")
    if isinstance(body, str):
        return body
    return ""


def status_code(response: Any) -> int:
    """Extract integer status. Handles httpx (.status_code) and Scrapling (.status)."""
    raw = getattr(response, "status_code", getattr(response, "status", 0))
    try:
        return int(raw or 0)
    except (TypeError, ValueError):
        return 0


def safe_location(response: Any) -> str:
    """Extract sanitized Location path (no query string, no credentials, lowercase)."""
    headers = getattr(response, "headers", None) or {}
    try:
        raw = str(headers.get("location") or headers.get("Location") or "")
    except Exception:  # noqa: BLE001
        return ""
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        return str(parsed.path or "/").lower()
    except Exception:  # noqa: BLE001
        return raw.split("?")[0].lower()


def extract_response_cookies(response: Any) -> dict[str, str]:
    """Pull cookies from a response object. Supports dicts, .items() protocol,
    and httpx's .jar attribute."""
    cookies_attr = getattr(response, "cookies", None)
    if cookies_attr is None:
        return {}
    result: dict[str, str] = {}
    try:
        if isinstance(cookies_attr, dict):
            for k, v in cookies_attr.items():
                result[str(k)] = str(v)
        elif hasattr(cookies_attr, "items"):
            for k, v in cookies_attr.items():
                result[str(k)] = str(v)
        elif hasattr(cookies_attr, "jar"):
            for cookie in cookies_attr.jar:
                result[str(cookie.name)] = str(cookie.value)
    except Exception:  # noqa: BLE001
        pass
    return result
