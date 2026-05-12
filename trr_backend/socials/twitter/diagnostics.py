"""Safe Twitter/X diagnostics and runtime metadata shaping."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

TWITTER_COMPLETE_STOP_REASONS = {
    "complete",
    "no_cursor",
    "no_tweet_entries",
    "older_than_window_repeated",
    "playwright_no_more_payloads",
    "playwright_no_tweet_entries",
}

SAFE_RETRIEVAL_META_KEYS = {
    "retrieval_mode",
    "search_query",
    "window_start",
    "window_end",
    "window_start_day",
    "window_end_day_inclusive",
    "window_end_day_exclusive",
    "window_contract",
    "from_query",
    "fast_mode",
    "graphql_404_count",
    "graphql_failed",
    "fallback_triggered",
    "fallback_attempts",
    "retryable",
    "error_code",
    "error_class",
    "twikit_failure_reason",
    "playwright_failure_reason",
    "pages_scanned",
    "posts_checked",
    "filtered_out_of_window",
    "stop_reason",
    "tweet_count",
    "twikit_checked",
    "syndication_checked",
    "playwright_checked",
    "playwright_page_budget",
    "playwright_payloads_captured",
    "playwright_scrolls_performed",
    "playwright_stop_reason",
    "complete",
}
SAFE_RUNTIME_META_KEYS = {"request_count", "transport", "fallback_chain", "stop_reason", "retryable", "complete"}
SAFE_QUOTE_META_KEYS = {"source_used", "failure_reason", "attempts"}
UNSAFE_KEY_MARKERS = (
    "authorization",
    "bearer",
    "cookie",
    "credential",
    "csrf",
    "ct0",
    "password",
    "proxy",
    "secret",
    "session",
    "token",
)
REDACTED = "[redacted]"


def classify_twitter_search_complete(
    *,
    stop_reason: str | None,
    retryable: bool = False,
    error_code: str | None = None,
) -> bool:
    if retryable or error_code:
        return False
    return str(stop_reason or "").strip() in TWITTER_COMPLETE_STOP_REASONS


def safe_retrieval_metadata(value: Any) -> dict[str, Any]:
    """Return route-visible retrieval metadata without auth/session internals."""
    meta = _metadata_dict(value)
    return _filter_safe_keys(meta, SAFE_RETRIEVAL_META_KEYS)


def safe_runtime_metadata(value: Any) -> dict[str, Any]:
    """Return safe runtime metadata from a scraper instance or mapping."""
    meta = _metadata_dict(getattr(value, "runtime_metadata", value))
    return _filter_safe_keys(meta, SAFE_RUNTIME_META_KEYS)


def safe_quote_fetch_metadata(value: Any) -> dict[str, Any]:
    """Return safe quote-fetch metadata from a scraper instance or mapping."""
    meta = _metadata_dict(getattr(value, "last_quote_fetch_meta", value))
    shaped = _filter_safe_keys(meta, SAFE_QUOTE_META_KEYS)
    failure_reason = str(getattr(value, "last_quote_fetch_reason", "") or "").strip()
    if failure_reason:
        shaped["failure_reason"] = failure_reason
    return shaped


def _filter_safe_keys(meta: Mapping[str, Any], allowed_keys: set[str]) -> dict[str, Any]:
    return {
        key: _redact_unsafe_values(value)
        for key, value in meta.items()
        if key in allowed_keys and not _is_unsafe_key(key)
    }


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _redact_unsafe_values(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): REDACTED if _is_unsafe_key(str(key)) else _redact_unsafe_values(nested)
            for key, nested in value.items()
        }
    if isinstance(value, list):
        return [_redact_unsafe_values(item) for item in value]
    if isinstance(value, tuple):
        return [_redact_unsafe_values(item) for item in value]
    return value


def _is_unsafe_key(key: str) -> bool:
    normalized = key.strip().lower()
    return any(marker in normalized for marker in UNSAFE_KEY_MARKERS)
