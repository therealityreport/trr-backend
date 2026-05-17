"""Crawlee-specific error classification."""

from __future__ import annotations

from typing import Any

ERROR_TAXONOMY = ("blocked", "rate_limited", "auth", "network", "parse", "unknown")
RETRYABLE_ERROR_CODES = {"blocked", "rate_limited", "network"}


def _structured_error_code(exc: Exception) -> str | None:
    for attr_name in ("error_code", "failure_reason_code", "reason_code"):
        value = getattr(exc, attr_name, None)
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return None


def _structured_retryable(exc: Exception) -> bool | None:
    value = getattr(exc, "retryable", None)
    if isinstance(value, bool):
        return value
    return None


def _looks_like_timeout(exc: Exception, message: str) -> bool:
    class_name = exc.__class__.__name__.lower()
    return isinstance(exc, TimeoutError) or "timeout" in class_name or "timed out" in message or "timeout" in message


def _retryable_for_code_or_message(error_code: str, message: str, exc: Exception) -> bool:
    normalized_code = error_code.strip().lower()
    if normalized_code in RETRYABLE_ERROR_CODES:
        return True
    if any(marker in normalized_code for marker in ("rate_limited", "rate-limit", "429")):
        return True
    if any(marker in normalized_code for marker in ("transport", "timeout", "network")):
        return True
    if _looks_like_timeout(exc, message):
        return True
    if "upstream" in message and any(marker in message for marker in ("json", "decode", "body", "unexpected")):
        return True
    if "response body" in message and any(marker in message for marker in ("decode", "unexpected", "truncated")):
        return True
    return False


def _specific_auth_code(message: str) -> str | None:
    if "instagram_graphql_checkpoint_required" in message:
        return "instagram_graphql_checkpoint_required"
    if "checkpoint_required" in message or "checkpoint" in message:
        return "checkpoint_required"
    if "feedback_required" in message or "feedback required" in message:
        return "feedback_required"
    if "redirect_login" in message or "/accounts/login" in message:
        return "redirect_login"
    if "challenge_required" in message or "challenge" in message:
        return "challenge_required"
    if "login_required" in message:
        return "login_required"
    return None


def classify_exception(exc: Exception) -> tuple[str, str, bool]:
    """Classify an exception using the v1 taxonomy and retry semantics."""
    message = str(exc or "").strip().lower()
    error_class = exc.__class__.__name__
    structured_code = _structured_error_code(exc)
    structured_retryable = _structured_retryable(exc)
    if structured_code:
        retryable = (
            structured_retryable
            if structured_retryable is not None
            else _retryable_for_code_or_message(structured_code, message, exc)
        )
        return structured_code, error_class, retryable
    if _looks_like_timeout(exc, message):
        return "network", error_class, True
    if not message:
        return "unknown", error_class, False

    rate_limit_markers = ("429", "rate limit", "throttle", "quota", "too many requests")
    auth_markers = (
        "auth",
        "unauthorized",
        "forbidden",
        "login",
        "cookie",
        "token",
        "credential",
        "challenge",
        "checkpoint",
        "feedback",
        "redirect_login",
        "/accounts/login",
    )
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
        return _specific_auth_code(message) or "auth", error_class, False
    if any(marker in message for marker in network_markers):
        return "network", error_class, True
    if any(marker in message for marker in parse_markers):
        return "parse", error_class, _retryable_for_code_or_message("parse", message, exc)
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
