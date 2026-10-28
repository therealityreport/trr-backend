"""One bounded public failure diagnostic per worker attempt; no source values."""

from __future__ import annotations

import json
import logging
from contextvars import ContextVar
from typing import Any

ERROR_CODES = frozenset(
    {
        "network_error",
        "unauthorized",
        "forbidden",
        "rate_limited",
        "reply_access_rejected",
        "redirect_login",
        "login_required",
        "challenge_required",
        "checkpoint_required",
        "feedback_required",
        "consent_required",
        "sentry_block",
        "endpoint_redirect",
        "request_failed",
        "parse_error",
        "public_access_backoff",
    }
)
_KEYS = ("event", "error_code", "status", "method", "endpoint_category")
_EVENT = "instagram_public_request_failed"
_ATTEMPT: ContextVar[dict | None] = ContextVar("instagram_public_diagnostic", default=None)
_COMMENT_BASIS: ContextVar[dict | None] = ContextVar("instagram_public_comment_failure_basis", default=None)
_LOGGER = logging.getLogger("trr_backend.ingestion.runtime")
_BASIS_VERSION = "instagram-public-comment-failure-basis-v1"
_BASIS_NAMES = (
    "explicit_rate_true",
    "malformed_truthy_rate",
    "explicit_feedback_true",
    "malformed_truthy_feedback",
    "embedded_http_429",
    "rate_message_marker",
    "please_wait_message_marker",
    "feedback_message_marker",
    "legacy_rate_code",
)
_BASIS_KEYS = frozenset(
    {"version", "bases", "error_items", "critical_error", "child_connection_present", "scan_truncated"}
)
_CHILD_CONNECTION = "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection"
_ENVELOPES = frozenset(
    {
        "items",
        "data",
        "graphql",
        "shortcode_media",
        "xdt_shortcode_media",
        "media",
        "comments",
        "child_comments",
        "replies",
        "page_info",
        "edge_media_to_parent_comment",
        "edge_media_to_comment",
        "xdt_api__v1__media__media_id__comments__connection",
        _CHILD_CONNECTION,
    }
)


def project_public_comment_failure_basis(payload: Any) -> dict:
    """Explain native rejection signals with fixed tokens, never a failure verdict.

    This deliberately does not call or replace the source classifier. Only its
    caller's existing failure decision can activate recording. Bounds may omit
    signals; scan_truncated makes that limitation explicit.
    """
    bases: set[str] = set()
    nodes, errors = 0, 0
    critical = child_connection = truncated = False

    def message(value: Any) -> None:
        nonlocal truncated
        if not isinstance(value, str):
            return
        if len(value) > 2048:
            truncated = True
        text = value[:2048].lower()
        if any(marker in text for marker in ("rate_limit", "rate limit", "too many requests")):
            bases.add("rate_message_marker")
        if "please wait" in text:
            bases.add("please_wait_message_marker")
        if "feedback_required" in text:
            bases.add("feedback_message_marker")

    def visit(value: Any, depth: int = 0, *, error_context: bool = False) -> None:
        nonlocal nodes, errors, critical, child_connection, truncated
        nodes += 1
        if nodes > 128 or depth > 8:
            truncated = True
            return
        if isinstance(value, list):
            if len(value) > 32:
                truncated = True
            for item in value[:32]:
                visit(item, depth + 1, error_context=error_context)
            return
        if not isinstance(value, dict):
            if error_context:
                message(value)
            return
        if any(value.get(key) for key in ("id", "pk", "comment_id", "text")):
            return  # Public comment content and author metadata are not error messages.
        for field, name in (("rate_limited", "rate"), ("feedback_required", "feedback")):
            flag = value.get(field)
            if flag is True:
                bases.add(f"explicit_{name}_true")
            elif type(flag) in (str, int, float, list, dict) and flag:
                bases.add(f"malformed_truthy_{name}")
        if any(type(value.get(key)) is int and value[key] == 429 for key in ("status_code", "http_status")):
            bases.add("embedded_http_429")
        for key in ("message", "error_message", "error_type", "status", "error"):
            message(value.get(key))
        if error_context:
            critical = critical or value.get("severity") == "CRITICAL"
            for key in ("description", "code", "error_code"):
                message(value.get(key))
        # Preserve only a boolean for the legacy recognized rate code. No
        # arbitrary native numeric code, source key, or source text is retained.
        if any(value.get(key) in (1675004, "1675004") for key in ("error", "code", "error_code")):
            bases.add("legacy_rate_code")
        child_connection = child_connection or isinstance(value.get(_CHILD_CONNECTION), dict)
        raw_errors = value.get("errors")
        if isinstance(raw_errors, list):
            errors = min(33, errors + len(raw_errors))
            visit(raw_errors, depth + 1, error_context=True)
        elif raw_errors is not None:
            errors = min(33, errors + 1)
            visit(raw_errors, depth + 1, error_context=True)
        if isinstance(value.get("error"), (dict, list)):
            visit(value["error"], depth + 1, error_context=True)
        if error_context and isinstance(value.get("extensions"), dict):
            visit(value["extensions"], depth + 1, error_context=True)
        for key in _ENVELOPES.intersection(value):
            visit(value[key], depth + 1)

    visit(payload)
    return {
        "version": _BASIS_VERSION,
        "bases": [name for name in _BASIS_NAMES if name in bases],
        "error_items": "truncated" if errors > 32 else "multiple" if errors > 1 else "one" if errors else "none",
        "critical_error": critical,
        "child_connection_present": child_connection,
        "scan_truncated": truncated,
    }


def _validate_comment_basis(value: Any) -> dict | None:
    if type(value) is not dict or set(value) != _BASIS_KEYS or value.get("version") != _BASIS_VERSION:
        return None
    bases = value.get("bases")
    if (
        type(bases) is not list
        or any(type(name) is not str or name not in _BASIS_NAMES for name in bases)
        or bases != [name for name in _BASIS_NAMES if name in bases]
        or value.get("error_items") not in {"none", "one", "multiple", "truncated"}
        or any(
            type(value.get(key)) is not bool for key in ("critical_error", "child_connection_present", "scan_truncated")
        )
    ):
        return None
    return {**value, "bases": list(bases)}


def record_public_comment_failure_basis(payload: Any, *, public_only: bool = False) -> None:
    if public_only is True and _COMMENT_BASIS.get() is None:
        _COMMENT_BASIS.set(project_public_comment_failure_basis(payload))


def current_public_comment_failure_basis() -> dict | None:
    """Read inside the adapter's async callback; asyncio.run isolates context writes."""
    return _validate_comment_basis(_COMMENT_BASIS.get())


def validate_public_diagnostic(value) -> dict | None:
    if type(value) is not dict or set(value) != set(_KEYS):
        return None
    if any(type(value[key]) is not str for key in _KEYS if key != "status"):
        return None
    if value["event"] != _EVENT or value["error_code"] not in ERROR_CODES:
        return None
    if type(value["status"]) is not int or not (value["status"] == 0 or 100 <= value["status"] <= 599):
        return None
    if value["method"] not in {"GET", "POST", "OTHER"}:
        return None
    if value["endpoint_category"] not in {"profile_bootstrap", "graphql", "permalink", "other"}:
        return None
    return {key: value[key] for key in _KEYS}


def reset_public_diagnostic() -> None:
    _ATTEMPT.set(None)
    _COMMENT_BASIS.set(None)


def record_public_diagnostic(value) -> None:
    safe = validate_public_diagnostic(value)
    if safe is not None and _ATTEMPT.get() is None:
        _ATTEMPT.set(safe)


def current_public_diagnostic() -> dict | None:
    return validate_public_diagnostic(_ATTEMPT.get())


def publish_public_diagnostic(value) -> dict | None:
    # This is called by the parent runtime on untrusted decoded child output.
    safe = validate_public_diagnostic(value)
    if safe is not None:
        try:
            _LOGGER.warning(json.dumps(safe, sort_keys=True))
        except Exception:
            pass  # Logging must not replace an already projected business result.
    return safe
