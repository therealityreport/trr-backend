"""One bounded public failure diagnostic per worker attempt; no source values."""

from __future__ import annotations

import json
import logging
from contextvars import ContextVar

ERROR_CODES = frozenset(
    {
        "network_error",
        "unauthorized",
        "forbidden",
        "rate_limited",
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
_LOGGER = logging.getLogger("trr_backend.ingestion.runtime")


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
