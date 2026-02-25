"""Crawlee-specific error classification."""

from __future__ import annotations

from typing import Any

ERROR_TAXONOMY = ("blocked", "rate_limited", "auth", "network", "parse", "unknown")
RETRYABLE_ERROR_CODES = {"blocked", "rate_limited", "network"}


def classify_exception(exc: Exception) -> tuple[str, str, bool]:
    """Classify an exception using the v1 taxonomy and retry semantics."""
    message = str(exc or "").strip().lower()
    error_class = exc.__class__.__name__
    if not message:
        return "unknown", error_class, False

    rate_limit_markers = ("429", "rate limit", "throttle", "quota", "too many requests")
    auth_markers = ("auth", "unauthorized", "forbidden", "login", "cookie", "token", "credential", "challenge")
    blocked_markers = ("captcha", "blocked", "bot", "robot", "denied", "suspicious")
    network_markers = (
        "timeout",
        "timed out",
        "connection",
        "network",
        "dns",
        "ssl",
        "502",
        "503",
        "504",
        "gateway",
        "service unavailable",
        "remote disconnected",
    )
    parse_markers = ("json", "decode", "parse", "schema", "keyerror", "valueerror", "typeerror", "invalid literal")

    if any(marker in message for marker in rate_limit_markers):
        return "rate_limited", error_class, True
    if any(marker in message for marker in blocked_markers):
        return "blocked", error_class, True
    if any(marker in message for marker in auth_markers):
        return "auth", error_class, False
    if any(marker in message for marker in network_markers):
        return "network", error_class, True
    if any(marker in message for marker in parse_markers):
        return "parse", error_class, False
    return "unknown", error_class, False


def merge_error_payload(
    *,
    metadata: dict[str, Any] | None,
    error_code: str,
    error_class: str,
    retryable: bool,
) -> dict[str, Any]:
    payload = dict(metadata or {})
    payload.update(
        {
            "failure_reason_code": error_code,
            "error_code": error_code,
            "error_class": error_class,
            "retryable": retryable,
        }
    )
    return payload
