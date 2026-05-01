"""Hybrid fetcher for the Instagram comments Scrapling lane.

Architecture:
  warmup()  →  _fetch_page()  →  StealthyFetcher (Patchright browser)
  comments  →  _fetch_api()   →  httpx.AsyncClient (plain HTTP + XHR headers)

The browser handles session establishment and challenge solving. All JSON
API calls go through httpx with the cookies bridged from warmup.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import html as html_lib
import json
import logging
import os
import re
import tempfile
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

import httpx

from trr_backend.socials._scrapling_http_utils import (
    env_truthy as _env_truthy,
)
from trr_backend.socials._scrapling_http_utils import (
    extract_response_cookies as _extract_response_cookies,
)
from trr_backend.socials._scrapling_http_utils import (
    resolve_positive_float_env as _resolve_positive_float_env,
)
from trr_backend.socials._scrapling_http_utils import (
    resolve_positive_int_env as _resolve_positive_int_env,
)
from trr_backend.socials._scrapling_http_utils import (
    response_text as _response_text,
)
from trr_backend.socials._scrapling_http_utils import (
    safe_location as _safe_location,
)
from trr_backend.socials._scrapling_http_utils import (
    status_code as _status_code,
)
from trr_backend.socials._scrapling_http_utils import (
    transient_backoff_seconds as _transient_backoff_seconds,
)
from trr_backend.socials._scrapling_http_utils import (
    transport_failure_reason as _transport_failure_reason,
)
from trr_backend.socials.instagram.comments_scrapling.counts import (
    flattened_comment_count,
    merge_comment_replies,
    missing_reply_count,
    reply_count_observed,
)
from trr_backend.socials.instagram.comments_scrapling.proxy import CommentsProxyConfig
from trr_backend.socials.instagram.constants import COMMENT_REPLIES_URL, COMMENTS_URL, resolve_comment_sort_order
from trr_backend.socials.instagram.permalink_metadata import _shortcode_to_media_id
from trr_backend.socials.instagram.scraper import InstagramComment, InstagramScraper

logger = logging.getLogger("socials.instagram.comments_scrapling.fetcher")

_COMMENT_PAGINATION_MAX_PAGES_DEFAULT = 250
_REPLY_PAGINATION_MAX_PAGES_DEFAULT = 100
_COMMENT_PAGINATION_MAX_SECONDS_DEFAULT = 600.0
_REPLY_PAGINATION_MAX_SECONDS_DEFAULT = 180.0
_REPLY_TAIL_TOTAL_MAX_SECONDS_PER_POST_DEFAULT = 90.0
_COMMENT_REQUEST_DELAY_DEFAULT = 0.25
_COMMENT_RATE_LIMIT_COOLDOWN_MIN_SECONDS_DEFAULT = 15.0
_COMMENT_RATE_LIMIT_COOLDOWN_MULTIPLIER_DEFAULT = 2.0
_REPLY_CHECKPOINT_MAX_ITEMS_DEFAULT = 25
_REPLY_CHECKPOINT_STRING_MAX_LENGTH = 256
_BROWSER_API_FALLBACK_ENV = "SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK"
_BROWSER_API_FALLBACK_ON_429_ENV = "SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK_ON_429"
_BROWSER_API_FALLBACK_ON_429_ATTEMPT_ENV = "SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK_ON_429_ATTEMPT"
_REVEAL_HIDDEN_COMMENTS_ENV = "SOCIAL_INSTAGRAM_COMMENTS_REVEAL_HIDDEN"
_REVEAL_HIDDEN_COMMENTS_WITHOUT_EXPECTED_ENV = "SOCIAL_INSTAGRAM_COMMENTS_REVEAL_HIDDEN_WITHOUT_EXPECTED"
_HIDDEN_COMMENTS_CLICK_LIMIT_DEFAULT = 4
_HIDDEN_UNAVAILABLE_GAP_MAX_DEFAULT = 2
_HIDDEN_UNAVAILABLE_GAP_RATIO_DEFAULT = 0.02

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_API_HEADER_KEYS_TO_STRIP = frozenset(
    {
        "x-requested-with",
        "x-ig-app-id",
        "sec-fetch-dest",
        "sec-fetch-mode",
        "sec-fetch-site",
    }
)

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_SPAN_TEXT_RE = re.compile(r"<span\b[^>]*\bdir=[\"']auto[\"'][^>]*>(.*?)</span>", re.IGNORECASE | re.DOTALL)
_TIME_DATETIME_RE = re.compile(r"<time\b[^>]*\bdatetime=[\"']([^\"']+)[\"']", re.IGNORECASE)
_PROFILE_HREF_RE = re.compile(r"href=[\"']/([^/\"?#]+)/[\"']", re.IGNORECASE)
_LIKE_COUNT_RE = re.compile(r"\b(\d[\d,]*)\s+likes?\b", re.IGNORECASE)
_MEDIA_SRC_RE = re.compile(r"<(?:img|video)\b[^>]*\b(?:src|poster)=[\"']([^\"']+)[\"']", re.IGNORECASE)


class _PaginationDeadlineExceededError(Exception):
    """Raised when request pacing/backoff would overrun a post-level deadline."""


def _deadline_remaining_seconds(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    try:
        return max(0.0, float(deadline) - time.monotonic())
    except (TypeError, ValueError):
        return None


def _deadline_response(attempt_count: int) -> dict[str, Any]:
    return {
        "failed": True,
        "auth_failed": False,
        "reason": "pagination_deadline_exceeded",
        "retryable": True,
        "payload": None,
        "attempt_count": max(0, int(attempt_count or 0)),
    }


async def _sleep_before_deadline(seconds: float, deadline: float | None) -> bool:
    delay = max(0.0, float(seconds or 0.0))
    remaining = _deadline_remaining_seconds(deadline)
    if remaining is not None:
        if remaining <= 0:
            return False
        if delay > remaining:
            await asyncio.sleep(remaining)
            return False
    if delay > 0:
        await asyncio.sleep(delay)
    return _deadline_remaining_seconds(deadline) != 0.0


def _safe_non_negative_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _clean_html_text(fragment: str) -> str:
    cleaned = _HTML_TAG_RE.sub(" ", str(fragment or ""))
    cleaned = html_lib.unescape(cleaned).replace("\xa0", " ")
    return re.sub(r"\s+", " ", cleaned).strip()


def _rendered_text_is_comment_body(value: str, *, username: str | None = None) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    normalized = text.casefold()
    if username and normalized == username.casefold():
        return False
    if normalized in {"reply", "like", "view hidden comments", "view replies", "hide replies"}:
        return False
    if re.fullmatch(r"\d+\s*(?:s|m|h|d|w|y)", normalized):
        return False
    if re.fullmatch(r"\d[\d,]*\s+likes?", normalized):
        return False
    if normalized.startswith("view ") and "comment" in normalized:
        return False
    return True


_INSTAGRAM_RESERVED_PROFILE_PATHS = frozenset({"accounts", "explore", "p", "reel", "reels", "stories"})


def _normalize_rendered_ignored_username(value: str | None) -> str:
    return str(value or "").strip().strip("/").lower().lstrip("@")


def _extract_rendered_comment_username(
    before_permalink_html: str,
    *,
    ignored_usernames: Iterable[str] | None = None,
) -> str:
    ignored = set(_INSTAGRAM_RESERVED_PROFILE_PATHS)
    ignored.update(
        normalized
        for normalized in (_normalize_rendered_ignored_username(value) for value in (ignored_usernames or []))
        if normalized
    )
    for match in reversed(list(_PROFILE_HREF_RE.finditer(str(before_permalink_html or "")))):
        username = html_lib.unescape(match.group(1)).strip().strip("/")
        if username and username.lower() not in ignored:
            return username
    return ""


def _extract_rendered_comment_text(after_permalink_html: str, *, username: str) -> str:
    for match in _SPAN_TEXT_RE.finditer(str(after_permalink_html or "")):
        candidate = _clean_html_text(match.group(1))
        if _rendered_text_is_comment_body(candidate, username=username):
            return candidate
    return ""


def _extract_rendered_comment_created_at(after_permalink_html: str) -> int:
    match = _TIME_DATETIME_RE.search(str(after_permalink_html or ""))
    if not match:
        return 0
    raw_value = html_lib.unescape(match.group(1)).strip()
    if not raw_value:
        return 0
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return max(0, int(parsed.timestamp()))


def _extract_rendered_comment_like_count(after_permalink_html: str) -> int:
    match = _LIKE_COUNT_RE.search(_clean_html_text(after_permalink_html[:2500]))
    if not match:
        return 0
    return int(match.group(1).replace(",", "") or 0)


def _extract_rendered_comment_media_urls(after_permalink_html: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for match in _MEDIA_SRC_RE.finditer(str(after_permalink_html or "")[:5000]):
        url = html_lib.unescape(str(match.group(1) or "").strip())
        if not url:
            continue
        if url.startswith("//"):
            url = f"https:{url}"
        lowered = url.lower()
        if not lowered.startswith(("http://", "https://")):
            continue
        if not any(marker in lowered for marker in ("scontent", "cdninstagram", "fbcdn", "instagram")):
            continue
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
        if len(urls) >= 4:
            break
    return urls


def _extract_rendered_permalink_comments(
    html_text: str,
    *,
    shortcode: str,
    post_url: str,
    ignored_usernames: Iterable[str] | None = None,
) -> list[InstagramComment]:
    """Extract comments visible in the rendered post DOM.

    Instagram's JSON comments endpoint can omit comments hidden behind the
    rendered "View hidden comments" control. Once the browser clicks that
    control, those comments still expose stable `/p/{shortcode}/c/{id}/`
    permalinks, which gives us a deterministic anchor for parsing.
    """

    normalized_shortcode = str(shortcode or "").strip()
    if not normalized_shortcode:
        return []
    permalink_pattern = re.compile(
        rf"href=[\"'](?P<href>/p/{re.escape(normalized_shortcode)}/c/(?P<comment_id>\d+)/?[^\"']*)[\"']",
        re.IGNORECASE,
    )
    comments: list[InstagramComment] = []
    seen_comment_ids: set[str] = set()
    text = str(html_text or "")
    for match in permalink_pattern.finditer(text):
        comment_id = str(match.group("comment_id") or "").strip()
        if not comment_id or comment_id in seen_comment_ids:
            continue
        context_start = max(0, match.start() - 3000)
        context_end = min(len(text), match.end() + 5000)
        before_permalink = text[context_start : match.start()]
        after_permalink = text[match.end() : context_end]
        username = _extract_rendered_comment_username(before_permalink, ignored_usernames=ignored_usernames)
        comment_text = _extract_rendered_comment_text(after_permalink, username=username)
        media_urls = _extract_rendered_comment_media_urls(after_permalink)
        if not username or (not comment_text and not media_urls):
            continue
        created_at = _extract_rendered_comment_created_at(after_permalink)
        seen_comment_ids.add(comment_id)
        comments.append(
            InstagramComment(
                comment_id=comment_id,
                text=comment_text,
                username=username,
                user_id="",
                created_at=created_at,
                date_time=datetime.fromtimestamp(created_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
                if created_at
                else "",
                likes=_extract_rendered_comment_like_count(after_permalink),
                is_reply=False,
                parent_comment_id=None,
                reply_count=0,
                post_shortcode=normalized_shortcode,
                post_url=post_url,
                media_urls=media_urls,
                is_hidden_by_instagram=True,
                source_snapshot_type="rendered_hidden_comments",
            )
        )
    return comments


def _merge_unique_comments(
    comments: list[InstagramComment],
    extra_comments: list[InstagramComment],
    *,
    max_comments: int,
) -> int:
    seen = {str(comment.comment_id or "").strip() for comment in comments if str(comment.comment_id or "").strip()}
    appended = 0
    for comment in extra_comments:
        comment_id = str(comment.comment_id or "").strip()
        if not comment_id or comment_id in seen:
            continue
        comments.append(comment)
        seen.add(comment_id)
        appended += 1
        if max_comments > 0 and len(comments) >= max_comments:
            break
    return appended


def _hidden_unavailable_gap_is_tolerable(*, unresolved_gap: int, target_count: int) -> bool:
    gap = max(0, int(unresolved_gap or 0))
    target = max(0, int(target_count or 0))
    if gap <= 0:
        return True
    if target <= 0:
        return False
    max_absolute_gap = _resolve_positive_int_env(
        "SOCIAL_INSTAGRAM_COMMENTS_HIDDEN_UNAVAILABLE_GAP_MAX",
        _HIDDEN_UNAVAILABLE_GAP_MAX_DEFAULT,
        minimum=0,
        maximum=50,
    )
    max_ratio = _resolve_positive_float_env(
        "SOCIAL_INSTAGRAM_COMMENTS_HIDDEN_UNAVAILABLE_GAP_RATIO",
        _HIDDEN_UNAVAILABLE_GAP_RATIO_DEFAULT,
        minimum=0.0,
        maximum=0.25,
    )
    ratio_gap = int(target * max_ratio)
    if ratio_gap < target * max_ratio:
        ratio_gap += 1
    return gap <= max(max_absolute_gap, ratio_gap)


_TOP_LEVEL_PAGINATION_KEYS = frozenset(
    {
        "has_more_comments",
        "has_more_headload_comments",
        "next_min_id",
        "next_max_id",
    }
)
_REPLY_PAGINATION_KEYS = frozenset(
    {
        "has_more_tail_child_comments",
        "has_more_head_child_comments",
        "next_min_child_cursor",
        "next_max_child_cursor",
    }
)


def _pagination_only_row(row: Any, *, keys: frozenset[str]) -> bool:
    if not isinstance(row, dict):
        return False
    if not keys.intersection(row):
        return False
    return not any(row.get(key) for key in ("id", "pk", "text", "user", "owner"))


def _extract_page_metadata(
    payload: Any,
    response: dict[str, Any],
    *,
    keys: frozenset[str],
) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    sources: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        sources.append(payload)
    elif isinstance(payload, list):
        sources.extend(item for item in payload if isinstance(item, dict))
    sources.append(response)
    for source in sources:
        for key in keys:
            if key in source and source.get(key) is not None:
                metadata[key] = source.get(key)
    return metadata


def _extract_top_level_page(
    payload: Any,
    response: dict[str, Any],
) -> tuple[list[dict[str, Any]], bool, str | None, str | None]:
    rows: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        raw_rows = payload.get("comments") or []
        rows = [row for row in raw_rows if isinstance(row, dict)] if isinstance(raw_rows, list) else []
    elif isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict) or _pagination_only_row(item, keys=_TOP_LEVEL_PAGINATION_KEYS):
                continue
            nested_rows = item.get("comments")
            if isinstance(nested_rows, list):
                rows.extend(row for row in nested_rows if isinstance(row, dict))
            else:
                rows.append(item)

    metadata = _extract_page_metadata(payload, response, keys=_TOP_LEVEL_PAGINATION_KEYS)
    has_more_comments = bool(metadata.get("has_more_comments"))
    has_more_headload = bool(metadata.get("has_more_headload_comments"))
    next_max = metadata.get("next_max_id")
    next_min = metadata.get("next_min_id")
    if has_more_comments and next_max:
        return rows, True, str(next_max), "max_id"
    if has_more_headload and next_min:
        return rows, True, str(next_min), "min_id"
    if has_more_comments and next_min:
        return rows, True, str(next_min), "min_id"
    if has_more_headload and next_max:
        return rows, True, str(next_max), "max_id"
    return rows, has_more_comments or has_more_headload, None, None


def _extract_reply_page(
    payload: Any,
    response: dict[str, Any],
) -> tuple[list[dict[str, Any]], str | None, str | None]:
    rows: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        raw_rows = payload.get("child_comments")
        if not isinstance(raw_rows, list):
            raw_rows = payload.get("replies")
        rows = [row for row in raw_rows if isinstance(row, dict)] if isinstance(raw_rows, list) else []
    elif isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict) or _pagination_only_row(item, keys=_REPLY_PAGINATION_KEYS):
                continue
            nested_rows = item.get("child_comments")
            if not isinstance(nested_rows, list):
                nested_rows = item.get("replies")
            if isinstance(nested_rows, list):
                rows.extend(row for row in nested_rows if isinstance(row, dict))
            else:
                rows.append(item)

    metadata = _extract_page_metadata(payload, response, keys=_REPLY_PAGINATION_KEYS)
    has_more_tail = bool(metadata.get("has_more_tail_child_comments"))
    has_more_head = bool(metadata.get("has_more_head_child_comments"))
    next_min = metadata.get("next_min_child_cursor")
    next_max = metadata.get("next_max_child_cursor")
    if (has_more_tail or has_more_head) and next_min:
        return rows, str(next_min), "min_id"
    if (has_more_head or has_more_tail) and next_max:
        return rows, str(next_max), "max_id"
    return rows, None, None


def _expected_target_count(expected_comment_count: int | None, max_comments: int) -> int | None:
    if expected_comment_count is None:
        return None
    return min(expected_comment_count, max_comments) if max_comments > 0 else expected_comment_count


def _normalized_cursor_key(cursor_param_name: str | None, cursor: str | None) -> str | None:
    normalized_cursor = str(cursor or "").strip()
    if not normalized_cursor:
        return None
    normalized_param = str(cursor_param_name or "min_id").strip() or "min_id"
    return f"{normalized_param}:{normalized_cursor}"


def _has_expected_gap(
    *,
    expected_comment_count: int | None,
    max_comments: int,
    current_comment_count: int,
) -> bool:
    target_count = _expected_target_count(expected_comment_count, max_comments)
    return target_count is not None and current_comment_count < target_count


def _auth_failure_text(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    return any(token in normalized for token in ("login", "checkpoint", "challenge", "accounts/login"))


def _global_rate_limit_key(browser_account_id: str | None, proxy_fingerprint: str | None) -> str:
    account = str(browser_account_id or "").strip().lower().lstrip("@") or "instagram"
    proxy = str(proxy_fingerprint or "").strip().lower() or "no-proxy"
    digest = hashlib.sha256(f"{account}:{proxy}".encode()).hexdigest()[:24]
    return f"{account}-{digest}"


def _global_rate_limit_path(key: str) -> str:
    safe_key = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(key or "instagram"))
    directory = os.path.join(tempfile.gettempdir(), "trr-instagram-comments-rate")
    os.makedirs(directory, exist_ok=True)
    return os.path.join(directory, f"{safe_key}.lock")


def _global_rate_cooldown_path(key: str) -> str:
    safe_key = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(key or "instagram"))
    directory = os.path.join(tempfile.gettempdir(), "trr-instagram-comments-rate")
    os.makedirs(directory, exist_ok=True)
    return os.path.join(directory, f"{safe_key}.cooldown")


def _read_monotonic_timestamp(handle: Any) -> float:
    handle.seek(0)
    raw_value = handle.read().strip()
    try:
        timestamp = float(raw_value)
    except (TypeError, ValueError):
        return 0.0
    now = time.monotonic()
    if timestamp > now + 3_600:
        return 0.0
    return max(0.0, timestamp)


def _record_global_api_cooldown(*, key: str, delay_seconds: float) -> None:
    delay = max(0.0, float(delay_seconds or 0))
    if delay <= 0:
        return
    path = _global_rate_cooldown_path(key)
    cooldown_until = time.monotonic() + delay
    with open(path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            existing_until = _read_monotonic_timestamp(handle)
            handle.seek(0)
            handle.truncate()
            handle.write(f"{max(existing_until, cooldown_until):.6f}")
            handle.flush()
            os.fsync(handle.fileno())
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _pace_global_api_request(*, key: str, delay_seconds: float, deadline: float | None = None) -> bool:
    delay = max(0.0, float(delay_seconds or 0))
    cooldown_path = _global_rate_cooldown_path(key)
    with open(cooldown_path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            cooldown_until = _read_monotonic_timestamp(handle)
            now = time.monotonic()
            remaining = cooldown_until - now
            if remaining > 0:
                deadline_remaining = _deadline_remaining_seconds(deadline)
                if deadline_remaining is not None:
                    if deadline_remaining <= 0:
                        return False
                    if remaining > deadline_remaining:
                        time.sleep(deadline_remaining)
                        return False
                time.sleep(remaining)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    if delay <= 0:
        return _deadline_remaining_seconds(deadline) != 0.0

    path = _global_rate_limit_path(key)
    with open(path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            last_started_at = _read_monotonic_timestamp(handle)
            now = time.monotonic()
            remaining = (last_started_at + delay) - now
            if remaining > 0:
                deadline_remaining = _deadline_remaining_seconds(deadline)
                if deadline_remaining is not None:
                    if deadline_remaining <= 0:
                        return False
                    if remaining > deadline_remaining:
                        time.sleep(deadline_remaining)
                        return False
                time.sleep(remaining)
                now = time.monotonic()
            handle.seek(0)
            handle.truncate()
            handle.write(f"{now:.6f}")
            handle.flush()
            os.fsync(handle.fileno())
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return _deadline_remaining_seconds(deadline) != 0.0


def _cookies_to_scrapling(cookies: dict[str, str]) -> list[dict[str, Any]]:
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
                "domain": ".instagram.com",
                "path": "/",
            }
        )
    return payload


def _document_auth_failure_text(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    return any(
        token in normalized
        for token in (
            "accounts/login",
            "/challenge/",
            "/checkpoint/",
            "login_required",
            "challenge_required",
            "checkpoint_required",
        )
    )


_TRANSPORT_FAILURE_MARKERS = (
    "wrong_version_number",
    "wrong version number",
    "ssl:",
    "ssl connection",
    "record layer failure",
    "closed unexpectedly",
    "proxy error",
    "proxyerror",
    "net::err_http_response_code_failure",
    "http_response_code_failure",
    "connecterror",
    "readerror",
    "connection reset",
    "server disconnected",
    "network is unreachable",
    "temporarily unavailable",
)


def _transport_failure_message(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in _TRANSPORT_FAILURE_MARKERS)


def _warmup_transport_failure(exc: BaseException) -> bool:
    if isinstance(exc, TimeoutError | httpx.TimeoutException | httpx.TransportError | OSError):
        return True
    return _transport_failure_message(exc)


def _api_transport_failure(exc: BaseException) -> bool:
    if isinstance(exc, TimeoutError | httpx.TimeoutException | httpx.TransportError):
        return True
    return isinstance(exc, OSError) and _transport_failure_message(exc)


class InstagramCommentsWarmupError(RuntimeError):
    error_code: str
    retryable: bool

    def __init__(self, message: str, *, error_code: str, retryable: bool = False) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retryable = retryable


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


def _failed_comment_entries_from_checkpoints(
    *,
    shortcode: str,
    reply_checkpoints: list[dict[str, Any]],
    top_level_checkpoint: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Phase 1.7: derive per-comment failure attribution from checkpoint data.

    Each entry carries the parent comment id (or empty for top-level pagination
    failures), stage, last error code and message. This is operational metadata
    that the job runner persists into ``social.scrape_jobs.metadata.comment_failures``.
    No new comments-table column is required.
    """
    entries: list[dict[str, Any]] = []
    normalized_shortcode = str(shortcode or "").strip() or None
    for checkpoint in reply_checkpoints or []:
        if not isinstance(checkpoint, dict):
            continue
        parent_comment_id = str(checkpoint.get("parent_comment_id") or "").strip()
        stop_reason = str(checkpoint.get("stop_reason") or checkpoint.get("last_error_code") or "").strip()
        error_code = str(checkpoint.get("last_error_code") or stop_reason or "").strip() or None
        if not parent_comment_id and not error_code:
            continue
        entries.append(
            {
                "comment_id": parent_comment_id or None,
                "stage": "reply",
                "error_code": error_code,
                "error_message": stop_reason or None,
                "shortcode": normalized_shortcode,
            }
        )
    if isinstance(top_level_checkpoint, dict):
        stop_reason = str(
            top_level_checkpoint.get("stop_reason") or top_level_checkpoint.get("last_error_code") or ""
        ).strip()
        error_code = str(top_level_checkpoint.get("last_error_code") or stop_reason or "").strip() or None
        if error_code:
            entries.append(
                {
                    "comment_id": None,
                    "stage": "top_level",
                    "error_code": error_code,
                    "error_message": stop_reason or None,
                    "shortcode": normalized_shortcode,
                }
            )
    return entries


@dataclass(slots=True)
class InstagramCommentsFetchResult:
    comments: list[InstagramComment] = field(default_factory=list)
    fetch_failed: bool = False
    auth_failed: bool = False
    fetch_reason: str | None = None
    reported_comment_count: int | None = None
    request_count: int = 0
    retryable: bool = False
    reply_checkpoints: list[dict[str, Any]] = field(default_factory=list)
    top_level_checkpoint: dict[str, Any] | None = None
    # Phase 1.7: per-comment failure attribution. Each entry is
    # {"comment_id": str, "stage": "top_level" | "reply", "error_code": str,
    #  "error_message": str, "shortcode": str | None}. Persisted into
    # social.scrape_jobs.metadata.comment_failures by the job runner.
    # Operational metadata only; not promoted to a comments-table column.
    failed_comment_ids: list[dict[str, Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------


class InstagramCommentsScraplingFetcher:
    """Hybrid fetcher: Patchright for warmup, httpx for API calls."""

    # Retry policy for transient errors (429 / 5xx / transport timeout).
    _MAX_TRANSIENT_RETRIES: int = 5
    _BASE_BACKOFF_SECONDS: float = 1.0

    def __init__(
        self,
        *,
        cookies: list[dict[str, Any]],
        raw_cookies: dict[str, str],
        browser_account_id: str | None,
        proxy_config: CommentsProxyConfig | None = None,
        headless: bool | None = None,
        timeout_ms: int = 45_000,
    ) -> None:
        self._cookies = list(cookies or [])
        self._raw_cookies = raw_cookies if isinstance(raw_cookies, dict) else dict(raw_cookies or {})
        self._browser_account_id = str(browser_account_id or "").strip() or None
        self._proxy_config = proxy_config
        self._proxy_rotator = proxy_config.proxy_rotator if proxy_config else None
        self._api_proxy_url = proxy_config.api_proxy_url if proxy_config else None
        self._headless = headless if headless is not None else _env_truthy("SOCIAL_INSTAGRAM_COMMENTS_HEADLESS", True)
        self._timeout_ms = max(5_000, int(timeout_ms))
        self._parser = InstagramScraper(cookies=self._raw_cookies, browser_account_id=self._browser_account_id)
        self._request_count = 0
        self._warmup_cookie_delta: dict[str, str] = {}
        self._selected_proxy_fingerprint: str = proxy_config.fingerprint if proxy_config else "none"
        self._proxy_session_mode: str = proxy_config.session_mode if proxy_config else "none"
        self._api_delay_seconds = _resolve_positive_float_env(
            "SOCIAL_INSTAGRAM_COMMENT_DELAY_SEC",
            _COMMENT_REQUEST_DELAY_DEFAULT,
            minimum=0.0,
            maximum=30.0,
        )
        self._global_api_delay_seconds = (
            _resolve_positive_float_env(
                "SOCIAL_INSTAGRAM_COMMENT_GLOBAL_DELAY_SEC",
                self._api_delay_seconds,
                minimum=0.0,
                maximum=60.0,
            )
            if _env_truthy("SOCIAL_INSTAGRAM_COMMENT_GLOBAL_THROTTLE", True)
            else 0.0
        )
        self._comment_sort_order = resolve_comment_sort_order()
        self._rate_limit_cooldown_min_seconds = _resolve_positive_float_env(
            "SOCIAL_INSTAGRAM_COMMENT_429_COOLDOWN_MIN_SEC",
            _COMMENT_RATE_LIMIT_COOLDOWN_MIN_SECONDS_DEFAULT,
            minimum=0.0,
            maximum=300.0,
        )
        self._rate_limit_cooldown_multiplier = _resolve_positive_float_env(
            "SOCIAL_INSTAGRAM_COMMENT_429_COOLDOWN_MULTIPLIER",
            _COMMENT_RATE_LIMIT_COOLDOWN_MULTIPLIER_DEFAULT,
            minimum=1.0,
            maximum=10.0,
        )
        self._global_rate_limit_key = _global_rate_limit_key(
            self._browser_account_id,
            self._selected_proxy_fingerprint,
        )
        self._max_transient_retries = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_COMMENT_TRANSIENT_RETRIES",
            self._MAX_TRANSIENT_RETRIES,
            minimum=0,
            maximum=20,
        )
        self._reply_max_transient_retries = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_COMMENT_REPLY_TRANSIENT_RETRIES",
            self._max_transient_retries,
            minimum=0,
            maximum=20,
        )
        self._reply_tail_total_budget_seconds = _resolve_positive_float_env(
            "SOCIAL_INSTAGRAM_REPLY_TAIL_TOTAL_MAX_SECONDS_PER_POST",
            _REPLY_TAIL_TOTAL_MAX_SECONDS_PER_POST_DEFAULT,
            minimum=0.0,
            maximum=1_800.0,
        )
        self._last_api_request_started_at = 0.0
        self._retry_reason_counts: dict[str, int] = {}
        self._top_level_checkpoints: list[dict[str, Any]] = []
        self._top_level_checkpoint_total_count = 0
        self._top_level_checkpoint_dropped_count = 0
        self._reply_checkpoints: list[dict[str, Any]] = []
        self._reply_checkpoint_total_count = 0
        self._reply_checkpoint_dropped_count = 0
        self._hidden_comments_render_attempts = 0
        self._hidden_comments_rendered_comments = 0
        self._hidden_comments_merged = 0
        self._reply_checkpoint_max_items = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_REPLY_CHECKPOINT_MAX_ITEMS",
            _REPLY_CHECKPOINT_MAX_ITEMS_DEFAULT,
            minimum=0,
            maximum=500,
        )
        self._top_level_checkpoint_max_items = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_TOP_LEVEL_CHECKPOINT_MAX_ITEMS",
            _REPLY_CHECKPOINT_MAX_ITEMS_DEFAULT,
            minimum=0,
            maximum=500,
        )

        # Browser fetcher (for warmup only).
        try:
            from scrapling.fetchers import StealthyFetcher
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Scrapling StealthyFetcher is unavailable. Install scrapling[fetchers].") from exc
        self._fetcher = StealthyFetcher()

        # httpx client (for API calls). Created lazily after warmup bridges cookies.
        self._http_client: httpx.AsyncClient | None = None

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    @property
    def runtime_metadata(self) -> dict[str, Any]:
        """Postmortem data for job metadata. The only way job_runner should
        read internal fetcher state."""
        return {
            "warmup_cookie_names": sorted(self._warmup_cookie_delta.keys()),
            "warmup_cookie_count": len(self._warmup_cookie_delta),
            "selected_proxy_fingerprint": self._selected_proxy_fingerprint,
            "proxy_session_mode": self._proxy_session_mode,
            "api_delay_seconds": self._api_delay_seconds,
            "global_api_delay_seconds": self._global_api_delay_seconds,
            "comment_sort_order": self._comment_sort_order,
            "global_rate_limit_key": self._global_rate_limit_key,
            "rate_limit_cooldown_min_seconds": self._rate_limit_cooldown_min_seconds,
            "rate_limit_cooldown_multiplier": self._rate_limit_cooldown_multiplier,
            "max_transient_retries": self._max_transient_retries,
            "reply_max_transient_retries": self._reply_max_transient_retries,
            "reply_tail_total_budget_seconds": self._reply_tail_total_budget_seconds,
            "transport": "httpx_after_browser_warmup",
            "request_count": self._request_count,
            "retry_reason_counts": dict(sorted(self._retry_reason_counts.items())),
            "hidden_comments": {
                "render_attempts": self._hidden_comments_render_attempts,
                "rendered_comments": self._hidden_comments_rendered_comments,
                "merged_comments": self._hidden_comments_merged,
            },
            "top_level_checkpoint_metadata": {
                "items": list(self._top_level_checkpoints),
                "total_count": self._top_level_checkpoint_total_count,
                "max_items": self._top_level_checkpoint_max_items,
                "dropped_count": self._top_level_checkpoint_dropped_count,
                "truncated": self._top_level_checkpoint_dropped_count > 0,
            },
            "reply_checkpoint_metadata": {
                "items": list(self._reply_checkpoints),
                "total_count": self._reply_checkpoint_total_count,
                "max_items": self._reply_checkpoint_max_items,
                "dropped_count": self._reply_checkpoint_dropped_count,
                "truncated": self._reply_checkpoint_dropped_count > 0,
            },
        }

    async def warmup(self) -> None:
        """Navigate to instagram.com via Patchright to establish the session,
        solve challenges, and bridge cookies into the httpx client."""
        warmup_account = str(self._browser_account_id or "").strip().lower().lstrip("@")
        warmup_url = f"https://www.instagram.com/{warmup_account}/" if warmup_account else "https://www.instagram.com/"
        try:
            response = await self._fetch_page(
                warmup_url,
                referer=warmup_url,
            )
        except Exception as exc:  # noqa: BLE001
            if not _warmup_transport_failure(exc):
                raise
            self._record_retry_reason("warmup_transport_error")
            raise InstagramCommentsWarmupError(
                f"Instagram comments warmup failed on transport/proxy setup: {exc}",
                error_code="instagram_comments_warmup_transport_error",
                retryable=True,
            ) from exc
        text = _response_text(response)
        if _status_code(response) in {401, 403} or _document_auth_failure_text(text):
            raise InstagramCommentsWarmupError(
                "Instagram comments warmup failed because the session appears logged out or challenged.",
                error_code="instagram_comments_warmup_auth_failed",
                retryable=False,
            )
        self._merge_warmup_cookies(response)
        if not self._warmup_cookie_delta and not str(self._raw_cookies.get("sessionid") or "").strip():
            raise InstagramCommentsWarmupError(
                "Instagram comments warmup did not bridge any cookies.",
                error_code="instagram_comments_warmup_no_cookies",
                retryable=True,
            )
        await self._rebuild_http_client()

    async def fetch_comments_for_shortcode(
        self,
        shortcode: str,
        *,
        max_comments: int,
        fetch_replies: bool,
        expected_comment_count: int | None = None,
        top_level_cursor: str | None = None,
        top_level_cursor_param: str | None = None,
        reply_resume_cursors: dict[str, str] | None = None,
        reply_resume_cursor_params: dict[str, str] | None = None,
        persisted_replies_by_parent: dict[str, list[InstagramComment]] | None = None,
        persisted_top_level_comments: list[InstagramComment] | None = None,
        reply_only: bool = False,
    ) -> InstagramCommentsFetchResult:
        try:
            media_id = _shortcode_to_media_id(shortcode)
        except Exception as exc:  # noqa: BLE001
            return InstagramCommentsFetchResult(
                comments=[],
                fetch_failed=True,
                auth_failed=False,
                fetch_reason=f"invalid_shortcode:{exc.__class__.__name__}",
                reported_comment_count=_safe_non_negative_int(expected_comment_count),
                request_count=self._request_count,
            )

        expected_comments = _safe_non_negative_int(expected_comment_count)
        post_url = f"https://www.instagram.com/p/{shortcode}/"
        comments: list[InstagramComment] = []
        cursor: str | None = str(top_level_cursor or "").strip() or None
        cursor_param_name = str(top_level_cursor_param or "min_id").strip() or "min_id"
        if cursor_param_name not in {"min_id", "max_id"}:
            cursor_param_name = "min_id"
        comments_fetched = 0
        fetch_failed = False
        auth_failed = False
        fetch_reason: str | None = None
        retryable = False
        reply_checkpoints: list[dict[str, Any]] = []
        reply_resume_cursors_by_parent = {
            str(parent_id or "").strip(): str(cursor_value or "").strip()
            for parent_id, cursor_value in (reply_resume_cursors or {}).items()
            if str(parent_id or "").strip() and str(cursor_value or "").strip()
        }
        reply_resume_cursor_params_by_parent = {
            str(parent_id or "").strip(): str(cursor_param or "").strip()
            for parent_id, cursor_param in (reply_resume_cursor_params or {}).items()
            if str(parent_id or "").strip() and str(cursor_param or "").strip() in {"min_id", "max_id"}
        }
        persisted_replies_by_parent_id = {
            str(parent_id or "").strip(): list(replies or [])
            for parent_id, replies in (persisted_replies_by_parent or {}).items()
            if str(parent_id or "").strip()
        }
        top_level_checkpoint: dict[str, Any] | None = None
        pages_seen = 0
        seen_cursors: set[str] = set()
        seen_top_level_comment_ids: set[str] = set()
        api_top_level_complete = False
        api_top_level_reveal_candidate = False
        hidden_reveal_attempted = False
        post_deadline_reached = False
        deadline = time.monotonic() + _resolve_positive_float_env(
            "SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_SECONDS",
            _COMMENT_PAGINATION_MAX_SECONDS_DEFAULT,
            minimum=1.0,
            maximum=1_800.0,
        )
        reply_tail_deadline: float | None = None
        if fetch_replies:
            if self._reply_tail_total_budget_seconds > 0:
                reply_tail_deadline = min(deadline, time.monotonic() + self._reply_tail_total_budget_seconds)
        page_cap = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_PAGES",
            _COMMENT_PAGINATION_MAX_PAGES_DEFAULT,
            minimum=1,
            maximum=250,
        )

        if reply_only and persisted_top_level_comments:
            return await self._fetch_persisted_reply_tails(
                shortcode=shortcode,
                media_id=media_id,
                post_url=post_url,
                max_comments=max_comments,
                fetch_replies=fetch_replies,
                expected_comment_count=expected_comments,
                persisted_top_level_comments=list(persisted_top_level_comments),
                persisted_replies_by_parent_id=persisted_replies_by_parent_id,
                reply_resume_cursors_by_parent=reply_resume_cursors_by_parent,
                reply_resume_cursor_params_by_parent=reply_resume_cursor_params_by_parent,
                deadline=deadline,
                reply_tail_deadline=reply_tail_deadline,
            )

        while True:
            if time.monotonic() >= deadline:
                fetch_failed = True
                fetch_reason = "pagination_deadline_exceeded"
                retryable = True
                top_level_checkpoint = self._record_top_level_checkpoint(
                    shortcode=shortcode,
                    media_id=media_id,
                    stop_reason=fetch_reason,
                    last_error_code=fetch_reason,
                    last_top_level_cursor=cursor,
                    next_top_level_cursor=cursor,
                    last_top_level_cursor_param=cursor_param_name,
                    next_top_level_cursor_param=cursor_param_name,
                    observed_comment_count=flattened_comment_count(comments),
                    expected_comment_count=expected_comments,
                    pages_seen=pages_seen,
                )
                logger.warning("Instagram comments pagination deadline exceeded for shortcode=%s", shortcode)
                break
            response = await self._fetch_json_response(
                COMMENTS_URL.format(media_id=media_id),
                referer=post_url,
                params={
                    "can_support_threading": "true",
                    "permalink_enabled": "false",
                    **({"sort_order": self._comment_sort_order} if self._comment_sort_order else {}),
                    **({cursor_param_name: cursor} if cursor else {}),
                },
                deadline=deadline,
            )
            payload = response.get("payload")
            page_fetch_reason = response.get("reason")
            page_fetch_failed = bool(response.get("failed"))
            page_auth_failed = bool(response.get("auth_failed"))
            page_retryable = bool(response.get("retryable"))
            fetch_failed = fetch_failed or page_fetch_failed
            auth_failed = auth_failed or page_auth_failed
            retryable = retryable or page_retryable
            if page_fetch_reason and not fetch_reason:
                fetch_reason = page_fetch_reason
            if page_fetch_failed or not isinstance(payload, (dict, list)):
                if page_retryable:
                    top_level_checkpoint = self._record_top_level_checkpoint(
                        shortcode=shortcode,
                        media_id=media_id,
                        stop_reason=page_fetch_reason or "top_level_pagination_retryable_stop",
                        last_error_code=page_fetch_reason,
                        last_top_level_cursor=cursor,
                        next_top_level_cursor=cursor,
                        last_top_level_cursor_param=cursor_param_name,
                        next_top_level_cursor_param=cursor_param_name,
                        observed_comment_count=flattened_comment_count(comments),
                        expected_comment_count=expected_comments,
                        pages_seen=pages_seen,
                    )
                break
            pages_seen += 1

            comment_rows, has_more, next_cursor, next_cursor_param_name = _extract_top_level_page(payload, response)
            for comment_data in comment_rows:
                if time.monotonic() >= deadline:
                    fetch_failed = True
                    fetch_reason = fetch_reason or "pagination_deadline_exceeded"
                    retryable = True
                    post_deadline_reached = True
                    top_level_checkpoint = self._record_top_level_checkpoint(
                        shortcode=shortcode,
                        media_id=media_id,
                        stop_reason=fetch_reason,
                        last_error_code=fetch_reason,
                        last_top_level_cursor=cursor,
                        next_top_level_cursor=cursor,
                        last_top_level_cursor_param=cursor_param_name,
                        next_top_level_cursor_param=cursor_param_name,
                        observed_comment_count=flattened_comment_count(comments),
                        expected_comment_count=expected_comments,
                        pages_seen=pages_seen,
                    )
                    logger.warning("Instagram comments pagination deadline exceeded for shortcode=%s", shortcode)
                    break
                if not isinstance(comment_data, dict):
                    continue
                comment = self._parser.parse_comment(comment_data, shortcode, post_url)
                comment_id = str(comment.comment_id or "").strip()
                if comment_id:
                    if comment_id in seen_top_level_comment_ids:
                        continue
                    seen_top_level_comment_ids.add(comment_id)
                persisted_replies = persisted_replies_by_parent_id.get(comment_id)
                if persisted_replies:
                    comment.replies = merge_comment_replies(
                        comment.replies,
                        persisted_replies,
                        parent_comment_id=comment.comment_id,
                    )
                observed_replies = reply_count_observed(comment)
                if fetch_replies and comment.reply_count > observed_replies:
                    reply_fetch_deadline = deadline
                    if reply_tail_deadline is not None:
                        if _deadline_remaining_seconds(reply_tail_deadline) == 0.0:
                            checkpoint = self._record_reply_checkpoint(
                                shortcode=shortcode,
                                media_id=media_id,
                                parent_comment_id=comment.comment_id,
                                stop_reason="reply_tail_budget_exhausted",
                                attempt_count=None,
                                last_error_code="reply_tail_budget_exhausted",
                                last_reply_cursor=None,
                                next_reply_cursor=None,
                                saved_reply_count=observed_replies,
                                expected_reply_count=comment.reply_count,
                                pages_seen=0,
                            )
                            if checkpoint:
                                reply_checkpoints.append(checkpoint)
                            fetch_failed = True
                            retryable = True
                            if not fetch_reason:
                                fetch_reason = "reply_tail_budget_exhausted"
                            comments.append(comment)
                            comments_fetched += 1
                            if max_comments > 0 and comments_fetched >= max_comments:
                                break
                            continue
                        reply_fetch_deadline = min(deadline, reply_tail_deadline)
                    replies_result = await self._fetch_comment_replies(
                        media_id=media_id,
                        comment_id=comment.comment_id,
                        shortcode=shortcode,
                        post_url=post_url,
                        expected_reply_count=comment.reply_count,
                        existing_replies=comment.replies,
                        resume_cursor=reply_resume_cursors_by_parent.get(str(comment.comment_id or "").strip()),
                        resume_cursor_param=reply_resume_cursor_params_by_parent.get(
                            str(comment.comment_id or "").strip()
                        ),
                        deadline=reply_fetch_deadline,
                    )
                    if (
                        reply_tail_deadline is not None
                        and replies_result.fetch_reason == "pagination_deadline_exceeded"
                        and _deadline_remaining_seconds(reply_tail_deadline) == 0.0
                    ):
                        replies_result.fetch_reason = "reply_tail_budget_exhausted"
                    comment.replies = merge_comment_replies(
                        comment.replies,
                        replies_result.comments,
                        parent_comment_id=comment.comment_id,
                    )
                    reply_checkpoints.extend(replies_result.reply_checkpoints)
                    fetch_failed = fetch_failed or replies_result.fetch_failed
                    auth_failed = auth_failed or replies_result.auth_failed
                    retryable = retryable or replies_result.retryable
                    if replies_result.fetch_reason and not fetch_reason:
                        fetch_reason = replies_result.fetch_reason
                    if (
                        replies_result.fetch_failed
                        and replies_result.retryable
                        and not replies_result.reply_checkpoints
                    ):
                        checkpoint = self._record_reply_checkpoint(
                            shortcode=shortcode,
                            media_id=media_id,
                            parent_comment_id=comment.comment_id,
                            stop_reason=replies_result.fetch_reason or "reply_pagination_retryable_stop",
                            attempt_count=None,
                            last_error_code=replies_result.fetch_reason,
                            last_reply_cursor=None,
                            next_reply_cursor=None,
                            saved_reply_count=reply_count_observed(comment),
                            expected_reply_count=comment.reply_count,
                            pages_seen=0,
                        )
                        if checkpoint:
                            reply_checkpoints.append(checkpoint)
                        logger.warning(
                            "Instagram comments reply fetch stopped for shortcode=%s parent_comment_id=%s reason=%s",
                            shortcode,
                            comment.comment_id,
                            replies_result.fetch_reason,
                        )
                comments.append(comment)
                comments_fetched += 1
                if max_comments > 0 and comments_fetched >= max_comments:
                    break

            if post_deadline_reached:
                break
            if max_comments > 0 and comments_fetched >= max_comments:
                break
            if not isinstance(payload, dict):
                if has_more and next_cursor:
                    pass
                else:
                    api_top_level_complete = True
                    break
            if not has_more or not next_cursor:
                api_top_level_complete = True
                break
            next_cursor = str(next_cursor)
            next_cursor_param_name = str(next_cursor_param_name or "min_id").strip() or "min_id"
            if next_cursor_param_name not in {"min_id", "max_id"}:
                next_cursor_param_name = "min_id"
            next_cursor_key = _normalized_cursor_key(next_cursor_param_name, next_cursor)
            current_cursor_key = _normalized_cursor_key(cursor_param_name, cursor)
            if next_cursor_key == current_cursor_key or (next_cursor_key and next_cursor_key in seen_cursors):
                has_gap = _has_expected_gap(
                    expected_comment_count=expected_comments,
                    max_comments=max_comments,
                    current_comment_count=flattened_comment_count(comments),
                )
                if expected_comments is None:
                    has_gap = True
                fetch_failed = fetch_failed or has_gap
                fetch_reason = "pagination_repeated_cursor"
                retryable = retryable or has_gap
                api_top_level_reveal_candidate = api_top_level_reveal_candidate or has_gap
                if has_gap:
                    top_level_checkpoint = self._record_top_level_checkpoint(
                        shortcode=shortcode,
                        media_id=media_id,
                        stop_reason=fetch_reason,
                        last_error_code=fetch_reason,
                        last_top_level_cursor=cursor,
                        next_top_level_cursor=None,
                        last_top_level_cursor_param=cursor_param_name,
                        next_top_level_cursor_param=None,
                        observed_comment_count=flattened_comment_count(comments),
                        expected_comment_count=expected_comments,
                        pages_seen=pages_seen,
                    )
                logger.warning(
                    "Instagram comments pagination repeated cursor for shortcode=%s cursor=%s",
                    shortcode,
                    next_cursor,
                )
                break
            if pages_seen >= page_cap:
                has_gap = _has_expected_gap(
                    expected_comment_count=expected_comments,
                    max_comments=max_comments,
                    current_comment_count=flattened_comment_count(comments),
                )
                if expected_comments is None:
                    has_gap = True
                fetch_failed = fetch_failed or has_gap
                fetch_reason = "pagination_page_cap_reached"
                retryable = retryable or has_gap
                api_top_level_reveal_candidate = api_top_level_reveal_candidate or has_gap
                if has_gap:
                    top_level_checkpoint = self._record_top_level_checkpoint(
                        shortcode=shortcode,
                        media_id=media_id,
                        stop_reason=fetch_reason,
                        last_error_code=fetch_reason,
                        last_top_level_cursor=cursor,
                        next_top_level_cursor=next_cursor,
                        last_top_level_cursor_param=cursor_param_name,
                        next_top_level_cursor_param=next_cursor_param_name,
                        observed_comment_count=flattened_comment_count(comments),
                        expected_comment_count=expected_comments,
                        pages_seen=pages_seen,
                    )
                logger.warning(
                    "Instagram comments pagination page cap reached for shortcode=%s page_cap=%d",
                    shortcode,
                    page_cap,
                )
                break
            if next_cursor_key:
                seen_cursors.add(next_cursor_key)
            cursor = next_cursor
            cursor_param_name = next_cursor_param_name

        if self._should_reveal_hidden_comments(
            expected_comment_count=expected_comments,
            current_comment_count=flattened_comment_count(comments),
            missing_reply_count=missing_reply_count(comments),
            max_comments=max_comments,
            auth_failed=auth_failed,
            api_top_level_complete=api_top_level_complete or api_top_level_reveal_candidate,
        ):
            hidden_reveal_attempted = True
            hidden_comments = await self._fetch_rendered_comments_after_revealing_hidden(shortcode, post_url)
            merged_count = _merge_unique_comments(comments, hidden_comments, max_comments=max_comments)
            self._hidden_comments_merged += merged_count
            if merged_count:
                logger.info(
                    "Merged %d rendered hidden Instagram comment(s) for shortcode=%s",
                    merged_count,
                        shortcode,
                    )
            if api_top_level_reveal_candidate and fetch_reason in {
                "pagination_repeated_cursor",
                "pagination_page_cap_reached",
            }:
                target_count = _expected_target_count(expected_comments, max_comments)
                current_flattened_count = flattened_comment_count(comments)
                if target_count is not None and current_flattened_count >= target_count and not missing_reply_count(
                    comments
                ):
                    fetch_failed = False
                    retryable = False
                    fetch_reason = "hidden_comments_recovered"

        target_count = _expected_target_count(expected_comments, max_comments)
        if target_count is not None and (max_comments <= 0 or expected_comments <= max_comments):
            current_flattened_count = flattened_comment_count(comments)
            current_missing_reply_count = missing_reply_count(comments)
            if not auth_failed and current_flattened_count >= target_count and current_missing_reply_count == 0:
                fetch_failed = False
                retryable = False
                if fetch_reason in {
                    "hidden_comments_unresolved",
                    "pagination_deadline_exceeded",
                    "pagination_page_cap_reached",
                    "pagination_repeated_cursor",
                    "reply_tail_incomplete",
                }:
                    fetch_reason = "coverage_target_met"
            elif not auth_failed and current_flattened_count < target_count:
                unresolved_gap = target_count - current_flattened_count
                if (
                    hidden_reveal_attempted
                    and api_top_level_complete
                    and current_missing_reply_count == 0
                    and _hidden_unavailable_gap_is_tolerable(
                        unresolved_gap=unresolved_gap,
                        target_count=target_count,
                    )
                ):
                    fetch_failed = False
                    retryable = False
                    fetch_reason = "hidden_comments_unavailable_reconciled"
                else:
                    fetch_failed = True
                    retryable = True
                    if not fetch_reason:
                        fetch_reason = (
                            "reply_tail_incomplete"
                            if current_missing_reply_count > 0
                            else "hidden_comments_unresolved"
                        )

        # Phase 1.7: derive failed_comment_ids from accumulated checkpoints so
        # the job runner can persist per-comment failure attribution into
        # social.scrape_jobs.metadata.comment_failures without each call site
        # having to track failures separately.
        failed_comment_ids = _failed_comment_entries_from_checkpoints(
            shortcode=shortcode,
            reply_checkpoints=reply_checkpoints,
            top_level_checkpoint=top_level_checkpoint,
        )
        return InstagramCommentsFetchResult(
            comments=comments,
            fetch_failed=fetch_failed,
            auth_failed=auth_failed,
            fetch_reason=fetch_reason,
            reported_comment_count=expected_comments,
            request_count=self._request_count,
            retryable=retryable,
            reply_checkpoints=reply_checkpoints,
            top_level_checkpoint=top_level_checkpoint,
            failed_comment_ids=failed_comment_ids,
        )

    async def _fetch_persisted_reply_tails(
        self,
        *,
        shortcode: str,
        media_id: str,
        post_url: str,
        max_comments: int,
        fetch_replies: bool,
        expected_comment_count: int | None,
        persisted_top_level_comments: list[InstagramComment],
        persisted_replies_by_parent_id: dict[str, list[InstagramComment]],
        reply_resume_cursors_by_parent: dict[str, str],
        reply_resume_cursor_params_by_parent: dict[str, str],
        deadline: float,
        reply_tail_deadline: float | None,
    ) -> InstagramCommentsFetchResult:
        comments: list[InstagramComment] = []
        fetch_failed = False
        auth_failed = False
        fetch_reason: str | None = None
        retryable = False
        reply_checkpoints: list[dict[str, Any]] = []
        comments_fetched = 0

        for comment in persisted_top_level_comments:
            if time.monotonic() >= deadline:
                fetch_failed = True
                retryable = True
                fetch_reason = fetch_reason or "pagination_deadline_exceeded"
                break
            comment.post_shortcode = comment.post_shortcode or shortcode
            comment.post_url = comment.post_url or post_url
            comment_id = str(comment.comment_id or "").strip()
            persisted_replies = persisted_replies_by_parent_id.get(comment_id)
            if persisted_replies:
                comment.replies = merge_comment_replies(
                    comment.replies,
                    persisted_replies,
                    parent_comment_id=comment.comment_id,
                )
            observed_replies = reply_count_observed(comment)
            if fetch_replies and comment.reply_count > observed_replies:
                reply_fetch_deadline = deadline
                if reply_tail_deadline is not None:
                    if _deadline_remaining_seconds(reply_tail_deadline) == 0.0:
                        checkpoint = self._record_reply_checkpoint(
                            shortcode=shortcode,
                            media_id=media_id,
                            parent_comment_id=comment.comment_id,
                            stop_reason="reply_tail_budget_exhausted",
                            attempt_count=None,
                            last_error_code="reply_tail_budget_exhausted",
                            last_reply_cursor=None,
                            next_reply_cursor=None,
                            saved_reply_count=observed_replies,
                            expected_reply_count=comment.reply_count,
                            pages_seen=0,
                        )
                        if checkpoint:
                            reply_checkpoints.append(checkpoint)
                        fetch_failed = True
                        retryable = True
                        fetch_reason = fetch_reason or "reply_tail_budget_exhausted"
                        comments.append(comment)
                        comments_fetched += 1
                        if max_comments > 0 and comments_fetched >= max_comments:
                            break
                        continue
                    reply_fetch_deadline = min(deadline, reply_tail_deadline)
                replies_result = await self._fetch_comment_replies(
                    media_id=media_id,
                    comment_id=comment.comment_id,
                    shortcode=shortcode,
                    post_url=post_url,
                    expected_reply_count=comment.reply_count,
                    existing_replies=comment.replies,
                    resume_cursor=reply_resume_cursors_by_parent.get(comment_id),
                    resume_cursor_param=reply_resume_cursor_params_by_parent.get(comment_id),
                    deadline=reply_fetch_deadline,
                )
                if (
                    reply_tail_deadline is not None
                    and replies_result.fetch_reason == "pagination_deadline_exceeded"
                    and _deadline_remaining_seconds(reply_tail_deadline) == 0.0
                ):
                    replies_result.fetch_reason = "reply_tail_budget_exhausted"
                comment.replies = merge_comment_replies(
                    comment.replies,
                    replies_result.comments,
                    parent_comment_id=comment.comment_id,
                )
                reply_checkpoints.extend(replies_result.reply_checkpoints)
                fetch_failed = fetch_failed or replies_result.fetch_failed
                auth_failed = auth_failed or replies_result.auth_failed
                retryable = retryable or replies_result.retryable
                if replies_result.fetch_reason and not fetch_reason:
                    fetch_reason = replies_result.fetch_reason
                if (
                    replies_result.fetch_failed
                    and replies_result.retryable
                    and not replies_result.reply_checkpoints
                ):
                    checkpoint = self._record_reply_checkpoint(
                        shortcode=shortcode,
                        media_id=media_id,
                        parent_comment_id=comment.comment_id,
                        stop_reason=replies_result.fetch_reason or "reply_pagination_retryable_stop",
                        attempt_count=None,
                        last_error_code=replies_result.fetch_reason,
                        last_reply_cursor=None,
                        next_reply_cursor=None,
                        saved_reply_count=reply_count_observed(comment),
                        expected_reply_count=comment.reply_count,
                        pages_seen=0,
                    )
                    if checkpoint:
                        reply_checkpoints.append(checkpoint)
            comments.append(comment)
            comments_fetched += 1
            if max_comments > 0 and comments_fetched >= max_comments:
                break

        if missing_reply_count(comments) > 0:
            fetch_failed = True
            retryable = True
            fetch_reason = fetch_reason or "reply_tail_incomplete"
        elif fetch_reason in {
            "pagination_deadline_exceeded",
            "reply_tail_budget_exhausted",
            "reply_tail_incomplete",
        }:
            fetch_failed = False
            retryable = False
            fetch_reason = "reply_tail_coverage_complete"
        elif expected_comment_count is not None and flattened_comment_count(comments) < expected_comment_count:
            fetch_reason = "reply_tail_coverage_complete"

        return InstagramCommentsFetchResult(
            comments=comments,
            fetch_failed=fetch_failed,
            auth_failed=auth_failed,
            fetch_reason=fetch_reason,
            reported_comment_count=expected_comment_count,
            request_count=self._request_count,
            retryable=retryable,
            reply_checkpoints=reply_checkpoints,
            top_level_checkpoint=None,
        )

    def _should_reveal_hidden_comments(
        self,
        *,
        expected_comment_count: int | None,
        current_comment_count: int,
        missing_reply_count: int,
        max_comments: int,
        auth_failed: bool,
        api_top_level_complete: bool,
    ) -> bool:
        if auth_failed or not _env_truthy(_REVEAL_HIDDEN_COMMENTS_ENV, True):
            return False
        if not api_top_level_complete:
            return False
        if max_comments > 0 and current_comment_count >= max_comments:
            return False
        if expected_comment_count is not None:
            target_count = min(expected_comment_count, max_comments) if max_comments > 0 else expected_comment_count
            unresolved_gap = max(0, target_count - current_comment_count)
            if missing_reply_count > 0 and unresolved_gap <= missing_reply_count:
                return False
            return current_comment_count < target_count
        return _env_truthy(_REVEAL_HIDDEN_COMMENTS_WITHOUT_EXPECTED_ENV, False)

    async def _fetch_rendered_comments_after_revealing_hidden(
        self,
        shortcode: str,
        post_url: str,
    ) -> list[InstagramComment]:
        click_limit = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_COMMENTS_HIDDEN_CLICK_LIMIT",
            _HIDDEN_COMMENTS_CLICK_LIMIT_DEFAULT,
            minimum=0,
            maximum=25,
        )
        if click_limit <= 0:
            return []

        async def reveal_hidden_comments(page: Any) -> None:
            await page.evaluate(
                """
                async ({ maxClicks }) => {
                  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
                  const textFor = (element) => {
                    if (!element) return "";
                    return [
                      element.innerText || "",
                      element.textContent || "",
                      element.getAttribute?.("aria-label") || "",
                      element.querySelector?.("title")?.textContent || "",
                    ].join(" ").replace(/\\s+/g, " ").trim();
                  };
                  for (let index = 0; index < maxClicks; index += 1) {
                    const candidates = Array.from(
                      document.querySelectorAll('[role="button"], button, a, [tabindex="0"], svg, span, div')
                    );
                    const exactControl = candidates.find((element) => {
                      const text = textFor(element).toLowerCase();
                      return text === "view hidden comments";
                    });
                    const control = exactControl || candidates.find((element) => {
                      const text = textFor(element).toLowerCase();
                      return text.includes("view hidden comments") && text.length < 80;
                    });
                    if (!control) break;
                    const clickable = control.closest?.('[role="button"], button, a, [tabindex="0"]') || control;
                    clickable.click?.();
                    clickable.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, view: window }));
                    await sleep(700);
                  }
                }
                """,
                {"maxClicks": click_limit},
            )

        self._hidden_comments_render_attempts += 1
        self._request_count += 1
        all_headers = self._parser.get_headers(post_url)
        nav_headers = {k: v for k, v in all_headers.items() if k.lower() not in _API_HEADER_KEYS_TO_STRIP}
        try:
            response = await self._fetcher.async_fetch(
                post_url,
                headless=self._headless,
                network_idle=True,
                load_dom=True,
                cookies=_cookies_to_scrapling(self._raw_cookies),
                proxy_rotator=self._proxy_rotator,
                extra_headers=nav_headers,
                timeout=self._timeout_ms,
                retries=1,
                retry_delay=1.0,
                wait=1_000,
                page_action=reveal_hidden_comments,
            )
        except Exception as exc:  # noqa: BLE001
            self._record_retry_reason("hidden_comments_render_fetch_failed")
            logger.warning(
                "Rendered hidden comments fetch failed for shortcode=%s: %s",
                shortcode,
                exc,
                exc_info=True,
            )
            return []

        self._sync_response_cookies(response)
        html_text = _response_text(response)
        comments = _extract_rendered_permalink_comments(
            html_text,
            shortcode=shortcode,
            post_url=post_url,
            ignored_usernames=[self._browser_account_id or ""],
        )
        self._hidden_comments_rendered_comments += len(comments)
        if comments:
            logger.info(
                "Rendered Instagram post yielded %d permalink comment(s) after hidden-comment reveal for shortcode=%s",
                len(comments),
                shortcode,
            )
        return comments

    async def aclose(self) -> None:
        """Close the httpx client. Called by job_runner in finally."""
        if self._http_client is not None:
            try:
                await self._http_client.aclose()
            except Exception:  # noqa: BLE001
                pass
            self._http_client = None

    # -------------------------------------------------------------------
    # Reply fetching
    # -------------------------------------------------------------------

    async def _fetch_comment_replies(
        self,
        *,
        media_id: str,
        comment_id: str,
        shortcode: str,
        post_url: str,
        expected_reply_count: int | None = None,
        existing_replies: list[InstagramComment] | None = None,
        resume_cursor: str | None = None,
        resume_cursor_param: str | None = None,
        deadline: float | None = None,
    ) -> InstagramCommentsFetchResult:
        preview_replies = list(existing_replies or [])
        replies: list[InstagramComment] = []
        cursor: str | None = str(resume_cursor or "").strip() or None
        normalized_resume_cursor_param = str(resume_cursor_param or "").strip()
        if normalized_resume_cursor_param not in {"min_id", "max_id"}:
            normalized_resume_cursor_param = "min_id"
        cursor_param_name: str | None = normalized_resume_cursor_param if cursor else None
        fetch_failed = False
        auth_failed = False
        fetch_reason: str | None = None
        retryable = False
        pages_seen = 0
        seen_cursors: set[str] = set()
        last_attempt_count = 0
        last_reply_cursor: str | None = None
        last_reply_cursor_param: str | None = None
        next_reply_cursor: str | None = None
        next_reply_cursor_param: str | None = None
        reply_deadline = time.monotonic() + _resolve_positive_float_env(
            "SOCIAL_INSTAGRAM_REPLY_PAGINATION_MAX_SECONDS",
            _REPLY_PAGINATION_MAX_SECONDS_DEFAULT,
            minimum=1.0,
            maximum=1_800.0,
        )
        if deadline is not None:
            reply_deadline = min(reply_deadline, float(deadline))
        page_cap = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_REPLY_PAGINATION_MAX_PAGES",
            _REPLY_PAGINATION_MAX_PAGES_DEFAULT,
            minimum=1,
            maximum=250,
        )

        while True:
            if time.monotonic() >= reply_deadline:
                fetch_failed = True
                fetch_reason = "pagination_deadline_exceeded"
                retryable = True
                logger.warning("Instagram reply pagination deadline exceeded for comment_id=%s", comment_id)
                break
            response = await self._fetch_json_response(
                COMMENT_REPLIES_URL.format(media_id=media_id, comment_id=comment_id),
                referer=post_url,
                params={cursor_param_name or "min_id": cursor} if cursor else None,
                max_retries=self._reply_max_transient_retries,
                deadline=reply_deadline,
            )
            last_attempt_count = int(response.get("attempt_count") or 0)
            last_reply_cursor = cursor
            last_reply_cursor_param = cursor_param_name
            payload = response.get("payload")
            fetch_reason = response.get("reason")
            fetch_failed = bool(response.get("failed"))
            auth_failed = bool(response.get("auth_failed"))
            retryable = retryable or bool(response.get("retryable"))
            if fetch_failed or not isinstance(payload, (dict, list)):
                break
            pages_seen += 1

            reply_rows, next_cursor, next_cursor_param_name = _extract_reply_page(payload, response)
            for reply_data in reply_rows:
                if not isinstance(reply_data, dict):
                    continue
                parsed_reply = self._parser.parse_comment(
                    reply_data,
                    shortcode,
                    post_url,
                    is_reply=True,
                    parent_id=comment_id,
                )
                replies = merge_comment_replies(
                    replies,
                    [parsed_reply],
                    parent_comment_id=comment_id,
                )

            if not isinstance(payload, dict):
                if next_cursor and next_cursor_param_name:
                    pass
                else:
                    break
            if not next_cursor or not next_cursor_param_name:
                break
            next_reply_cursor = next_cursor
            next_reply_cursor_param = next_cursor_param_name
            next_cursor_key = f"{next_cursor_param_name}:{next_cursor}"
            current_cursor_key = f"{cursor_param_name}:{cursor}" if cursor and cursor_param_name else None
            if next_cursor_key == current_cursor_key or next_cursor_key in seen_cursors:
                observed_reply_total = len(
                    merge_comment_replies(
                        preview_replies,
                        replies,
                        parent_comment_id=comment_id,
                    )
                )
                has_gap = expected_reply_count is None or observed_reply_total < expected_reply_count
                fetch_failed = fetch_failed or has_gap
                fetch_reason = "pagination_repeated_cursor"
                retryable = retryable or has_gap
                logger.warning(
                    "Instagram reply pagination repeated cursor for comment_id=%s cursor=%s",
                    comment_id,
                    next_cursor,
                )
                break
            if pages_seen >= page_cap:
                observed_reply_total = len(
                    merge_comment_replies(
                        preview_replies,
                        replies,
                        parent_comment_id=comment_id,
                    )
                )
                has_gap = expected_reply_count is None or observed_reply_total < expected_reply_count
                fetch_failed = fetch_failed or has_gap
                fetch_reason = "pagination_page_cap_reached"
                retryable = retryable or has_gap
                logger.warning(
                    "Instagram reply pagination page cap reached for comment_id=%s page_cap=%d",
                    comment_id,
                    page_cap,
                )
                break
            seen_cursors.add(next_cursor_key)
            cursor = next_cursor
            cursor_param_name = next_cursor_param_name

        reply_checkpoints: list[dict[str, Any]] = []
        if fetch_failed and retryable:
            observed_reply_total = len(
                merge_comment_replies(
                    preview_replies,
                    replies,
                    parent_comment_id=comment_id,
                )
            )
            checkpoint = self._record_reply_checkpoint(
                shortcode=shortcode,
                media_id=media_id,
                parent_comment_id=comment_id,
                stop_reason=fetch_reason or "reply_pagination_retryable_stop",
                attempt_count=last_attempt_count,
                last_error_code=fetch_reason,
                last_reply_cursor=last_reply_cursor,
                next_reply_cursor=next_reply_cursor,
                last_reply_cursor_param=last_reply_cursor_param,
                next_reply_cursor_param=next_reply_cursor_param,
                saved_reply_count=observed_reply_total,
                expected_reply_count=expected_reply_count,
                pages_seen=pages_seen,
            )
            if checkpoint:
                reply_checkpoints.append(checkpoint)

        return InstagramCommentsFetchResult(
            comments=replies,
            fetch_failed=fetch_failed,
            auth_failed=auth_failed,
            fetch_reason=fetch_reason,
            request_count=self._request_count,
            retryable=retryable,
            reply_checkpoints=reply_checkpoints,
        )

    # -------------------------------------------------------------------
    # Cookie bridge
    # -------------------------------------------------------------------

    def _merge_warmup_cookies(self, response: Any) -> None:
        """Record warmup cookie delta and sync the live request state."""
        new_cookies = _extract_response_cookies(response)
        self._warmup_cookie_delta = dict(new_cookies)
        self._sync_response_cookies(response)

    def _sync_response_cookies(self, response: Any) -> None:
        """Keep the lightweight transport and parser headers in sync.

        httpx updates its own cookie jar automatically, but the parser keeps a
        separate mutable cookie dict that drives future request headers. Mirror
        response cookies into both so later API calls keep using the freshest
        `csrftoken` / session state.
        """
        new_cookies = _extract_response_cookies(response)
        for name, value in new_cookies.items():
            self._raw_cookies[name] = value
            if hasattr(self._parser, "cookies") and isinstance(self._parser.cookies, dict):
                self._parser.cookies[name] = value
            if hasattr(self._parser, "session") and hasattr(self._parser.session, "cookies"):
                try:
                    self._parser.session.cookies.set(name, value)
                except Exception:  # noqa: BLE001
                    pass

    async def _rebuild_http_client(self) -> None:
        """Create or recreate the httpx client with current cookies and proxy."""
        existing_client = self._http_client
        self._http_client = None
        if existing_client is not None:
            try:
                await existing_client.aclose()
            except Exception:  # noqa: BLE001
                pass
        self._http_client = httpx.AsyncClient(
            cookies=dict(self._raw_cookies),
            timeout=httpx.Timeout(self._timeout_ms / 1000),
            proxy=self._api_proxy_url,
            follow_redirects=False,
            trust_env=False,
        )

    # -------------------------------------------------------------------
    # Transport: browser (warmup only)
    # -------------------------------------------------------------------

    async def _fetch_page(
        self,
        url: str,
        *,
        referer: str,
    ) -> Any:
        """Full page navigation via Patchright. Used ONLY by warmup().
        Strips API-specific headers (x-ig-app-id, x-requested-with, sec-fetch-*)
        that don't belong on document navigation.
        """
        self._request_count += 1
        all_headers = self._parser.get_headers(referer)
        nav_headers = {k: v for k, v in all_headers.items() if k.lower() not in _API_HEADER_KEYS_TO_STRIP}
        return await self._fetcher.async_fetch(
            url,
            headless=self._headless,
            network_idle=False,
            load_dom=False,
            cookies=self._cookies,
            proxy_rotator=self._proxy_rotator,
            extra_headers=nav_headers,
            timeout=self._timeout_ms,
            retries=1,
            retry_delay=1.0,
        )

    # -------------------------------------------------------------------
    # Transport: httpx (API calls)
    # -------------------------------------------------------------------

    async def _fetch_api(
        self,
        url: str,
        *,
        referer: str,
        params: dict[str, Any] | None = None,
        deadline: float | None = None,
    ) -> httpx.Response:
        """Plain HTTP GET via httpx. Used for comments/replies JSON API calls."""
        if self._http_client is None:
            await self._rebuild_http_client()
        if not await self._pace_api_requests(deadline=deadline):
            raise _PaginationDeadlineExceededError
        self._request_count += 1
        headers = self._parser.get_headers(referer)
        clean_params = {k: v for k, v in (params or {}).items() if v is not None} or None
        response = await self._http_client.get(url, params=clean_params, headers=headers)  # type: ignore[union-attr]
        self._sync_response_cookies(response)
        return response

    async def _fetch_api_with_browser(
        self,
        url: str,
        *,
        referer: str,
        params: dict[str, Any] | None = None,
        deadline: float | None = None,
    ) -> Any:
        """Fetch a JSON API URL through the same browser transport as warmup.

        Instagram sometimes redirects the lightweight httpx transport from the
        comments API to `/` after a successful browser warmup. In that state the
        session is not necessarily logged out; the API call is being rejected on
        transport/browser-context signals. This fallback keeps the same cookie
        jar and proxy plane but asks Patchright/Scrapling to make the request.
        """
        clean_params = {key: value for key, value in (params or {}).items() if value is not None}
        request_url = url
        if clean_params:
            separator = "&" if "?" in request_url else "?"
            request_url = f"{request_url}{separator}{urlencode(clean_params, doseq=True)}"
        remaining = _deadline_remaining_seconds(deadline)
        if remaining is not None and remaining <= 0:
            raise _PaginationDeadlineExceededError
        self._request_count += 1
        fetch_timeout_ms = self._timeout_ms
        if remaining is not None:
            fetch_timeout_ms = max(1_000, min(fetch_timeout_ms, int(remaining * 1_000)))
        fetch_task = self._fetcher.async_fetch(
            request_url,
            headless=self._headless,
            network_idle=False,
            load_dom=False,
            cookies=_cookies_to_scrapling(self._raw_cookies),
            proxy_rotator=self._proxy_rotator,
            extra_headers=self._parser.get_headers(referer),
            timeout=fetch_timeout_ms,
            retries=1,
            retry_delay=1.0,
        )
        response = await asyncio.wait_for(fetch_task, timeout=remaining) if remaining is not None else await fetch_task
        self._sync_response_cookies(response)
        await self._rebuild_http_client()
        return response

    async def _pace_api_requests(self, *, deadline: float | None = None) -> bool:
        if self._global_api_delay_seconds > 0:
            paced = await asyncio.to_thread(
                _pace_global_api_request,
                key=self._global_rate_limit_key,
                delay_seconds=self._global_api_delay_seconds,
                deadline=deadline,
            )
            if not paced:
                return False
        if self._api_delay_seconds <= 0:
            return _deadline_remaining_seconds(deadline) != 0.0
        remaining = (self._last_api_request_started_at + self._api_delay_seconds) - time.monotonic()
        if remaining > 0:
            if not await _sleep_before_deadline(remaining, deadline):
                return False
        self._last_api_request_started_at = time.monotonic()
        return _deadline_remaining_seconds(deadline) != 0.0

    async def _recover_homepage_redirect(self, *, referer: str) -> bool:
        recovery_url = str(referer or "").strip() or "https://www.instagram.com/"
        self._record_retry_reason("homepage_redirect_recovery")
        try:
            recovery_response = await self._fetch_page(recovery_url, referer=recovery_url)
        except Exception:  # noqa: BLE001
            logger.warning("Instagram homepage redirect recovery warmup failed for %s", recovery_url, exc_info=True)
            return False
        status_code = _status_code(recovery_response)
        text = _response_text(recovery_response)
        if status_code >= 400 or 300 <= status_code < 400 or _document_auth_failure_text(text):
            return False
        self._merge_warmup_cookies(recovery_response)
        await self._rebuild_http_client()
        return True

    def _record_retry_reason(self, reason: str | None) -> None:
        normalized = str(reason or "").strip()
        if not normalized:
            return
        self._retry_reason_counts[normalized] = self._retry_reason_counts.get(normalized, 0) + 1

    def _decode_json_response_result(self, response: Any, *, attempt: int) -> dict[str, Any]:
        status_code = _status_code(response)
        text = _response_text(response)
        auth_failed = status_code in {401, 403} or _auth_failure_text(text)

        if 300 <= status_code < 400:
            location = _safe_location(response)
            reason = (
                "redirect_to_login"
                if "/accounts/login" in location
                else "redirect_to_checkpoint"
                if ("/challenge" in location or "/checkpoint" in location)
                else "redirect_to_homepage"
            )
            auth_redirect = any(token in location for token in ("login", "challenge", "checkpoint"))
            return {
                "failed": True,
                "auth_failed": auth_redirect or reason == "redirect_to_homepage",
                "reason": reason,
                "retryable": False,
                "payload": None,
                "attempt_count": attempt,
            }

        if self._is_transient_status(status_code):
            return {
                "failed": True,
                "auth_failed": False,
                "reason": f"http_{status_code}",
                "retryable": True,
                "payload": None,
                "attempt_count": attempt,
            }

        if status_code >= 400:
            return {
                "failed": True,
                "auth_failed": auth_failed,
                "reason": f"http_{status_code}",
                "retryable": False,
                "payload": None,
                "attempt_count": attempt,
            }

        if text and text.lstrip().startswith("<"):
            return {
                "failed": True,
                "auth_failed": auth_failed or _auth_failure_text(text),
                "reason": "html_challenge_or_auth_required",
                "retryable": False,
                "payload": None,
                "attempt_count": attempt,
            }

        try:
            payload = response.json()
        except Exception:  # noqa: BLE001
            try:
                payload = json.loads(text)
            except Exception:  # noqa: BLE001
                return {
                    "failed": True,
                    "auth_failed": auth_failed,
                    "reason": "non_json_response",
                    "retryable": False,
                    "payload": None,
                    "attempt_count": attempt,
                }

        if isinstance(payload, dict):
            status_value = str(payload.get("status") or "").strip().lower()
            message = str(payload.get("message") or payload.get("error_message") or "").strip().lower()
            if status_value and status_value != "ok":
                return {
                    "failed": True,
                    "auth_failed": auth_failed
                    or any(
                        token in f"{status_value} {message}"
                        for token in ("login", "checkpoint", "challenge", "unauthorized")
                    ),
                    "reason": status_value or "api_status_fail",
                    "retryable": False,
                    "payload": payload,
                    "attempt_count": attempt,
                }

        return {
            "failed": False,
            "auth_failed": auth_failed,
            "reason": None,
            "retryable": False,
            "payload": payload,
            "attempt_count": attempt,
        }

    @staticmethod
    def _compact_checkpoint_text(value: Any) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        if len(text) <= _REPLY_CHECKPOINT_STRING_MAX_LENGTH:
            return text
        return text[:_REPLY_CHECKPOINT_STRING_MAX_LENGTH]

    @staticmethod
    def _non_negative_int(value: Any) -> int | None:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed >= 0 else None

    def _record_reply_checkpoint(
        self,
        *,
        shortcode: str,
        media_id: str,
        parent_comment_id: str,
        stop_reason: str,
        attempt_count: int | None,
        last_error_code: str | None,
        last_reply_cursor: str | None,
        next_reply_cursor: str | None,
        last_reply_cursor_param: str | None = None,
        next_reply_cursor_param: str | None = None,
        saved_reply_count: int,
        expected_reply_count: int | None,
        pages_seen: int,
    ) -> dict[str, Any] | None:
        self._reply_checkpoint_total_count += 1
        if self._reply_checkpoint_max_items <= 0:
            self._reply_checkpoint_dropped_count += 1
            return None

        checkpoint = {
            "platform": "instagram",
            "target_shortcode": self._compact_checkpoint_text(shortcode),
            "source_id": self._compact_checkpoint_text(shortcode),
            "media_id": self._compact_checkpoint_text(media_id),
            "parent_comment_id": self._compact_checkpoint_text(parent_comment_id),
            "stop_reason": self._compact_checkpoint_text(stop_reason),
            "attempt_count": self._non_negative_int(attempt_count),
            "last_error_code": self._compact_checkpoint_text(last_error_code or stop_reason),
            "last_reply_cursor": self._compact_checkpoint_text(last_reply_cursor),
            "next_reply_cursor": self._compact_checkpoint_text(next_reply_cursor),
            "last_reply_cursor_param": self._compact_checkpoint_text(last_reply_cursor_param),
            "next_reply_cursor_param": self._compact_checkpoint_text(next_reply_cursor_param),
            "saved_reply_count_observed": self._non_negative_int(saved_reply_count),
            "expected_reply_count": self._non_negative_int(expected_reply_count),
            "pages_seen": self._non_negative_int(pages_seen),
            "retryable": True,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        compact_checkpoint = {key: value for key, value in checkpoint.items() if value is not None}
        while len(self._reply_checkpoints) >= self._reply_checkpoint_max_items:
            self._reply_checkpoints.pop(0)
            self._reply_checkpoint_dropped_count += 1
        self._reply_checkpoints.append(compact_checkpoint)
        return compact_checkpoint

    def _record_top_level_checkpoint(
        self,
        *,
        shortcode: str,
        media_id: str,
        stop_reason: str,
        last_error_code: str | None,
        last_top_level_cursor: str | None,
        next_top_level_cursor: str | None,
        last_top_level_cursor_param: str | None,
        next_top_level_cursor_param: str | None,
        observed_comment_count: int,
        expected_comment_count: int | None,
        pages_seen: int,
    ) -> dict[str, Any] | None:
        self._top_level_checkpoint_total_count += 1
        if self._top_level_checkpoint_max_items <= 0:
            self._top_level_checkpoint_dropped_count += 1
            return None

        checkpoint = {
            "platform": "instagram",
            "target_shortcode": self._compact_checkpoint_text(shortcode),
            "source_id": self._compact_checkpoint_text(shortcode),
            "media_id": self._compact_checkpoint_text(media_id),
            "stop_reason": self._compact_checkpoint_text(stop_reason),
            "last_error_code": self._compact_checkpoint_text(last_error_code or stop_reason),
            "last_top_level_cursor": self._compact_checkpoint_text(last_top_level_cursor),
            "next_top_level_cursor": self._compact_checkpoint_text(next_top_level_cursor),
            "last_top_level_cursor_param": self._compact_checkpoint_text(last_top_level_cursor_param),
            "next_top_level_cursor_param": self._compact_checkpoint_text(next_top_level_cursor_param),
            "observed_comment_count": self._non_negative_int(observed_comment_count),
            "expected_comment_count": self._non_negative_int(expected_comment_count),
            "pages_seen": self._non_negative_int(pages_seen),
            "retryable": True,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        compact_checkpoint = {key: value for key, value in checkpoint.items() if value is not None}
        while len(self._top_level_checkpoints) >= self._top_level_checkpoint_max_items:
            self._top_level_checkpoints.pop(0)
            self._top_level_checkpoint_dropped_count += 1
        self._top_level_checkpoints.append(compact_checkpoint)
        return compact_checkpoint

    # -------------------------------------------------------------------
    # JSON response handling with retry/backoff
    # -------------------------------------------------------------------

    @staticmethod
    def _is_transient_status(status_code: int) -> bool:
        return status_code == 429 or (500 <= status_code < 600)

    @staticmethod
    def _retry_after_seconds(response: Any) -> float | None:
        headers = getattr(response, "headers", None) or {}
        raw = None
        try:
            raw = headers.get("retry-after") if hasattr(headers, "get") else None
        except Exception:  # noqa: BLE001
            raw = None
        if raw is None:
            return None
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return None

    async def _fetch_json_response(
        self,
        url: str,
        *,
        referer: str,
        params: dict[str, Any] | None = None,
        max_retries: int | None = None,
        deadline: float | None = None,
    ) -> dict[str, Any]:
        """JSON fetch via httpx with bounded exponential backoff on transient
        failures (429 / 5xx / transport timeout).
        """
        attempt = 0
        retry_limit = self._max_transient_retries if max_retries is None else max(0, int(max_retries))
        homepage_redirect_recovery_attempted = False
        browser_api_fallback_attempted = False
        last_transient_reason: str | None = None
        while True:
            if _deadline_remaining_seconds(deadline) == 0.0:
                return _deadline_response(attempt)
            attempt += 1
            try:
                response = await self._fetch_api(url, referer=referer, params=params, deadline=deadline)
            except _PaginationDeadlineExceededError:
                return _deadline_response(attempt)
            except (TimeoutError, httpx.TimeoutException, httpx.TransportError, OSError) as exc:
                if not _api_transport_failure(exc):
                    raise
                last_transient_reason = _transport_failure_reason(exc)
                self._record_retry_reason(last_transient_reason)
                if attempt > retry_limit:
                    return {
                        "failed": True,
                        "auth_failed": False,
                        "reason": last_transient_reason,
                        "retryable": True,
                        "payload": None,
                        "attempt_count": attempt,
                    }
                try:
                    await self._rebuild_http_client()
                except Exception:  # noqa: BLE001
                    logger.debug(
                        "Failed to rebuild Instagram comments HTTP client after transport error", exc_info=True
                    )
                if not await _sleep_before_deadline(
                    _transient_backoff_seconds(attempt, self._BASE_BACKOFF_SECONDS),
                    deadline,
                ):
                    return _deadline_response(attempt)
                continue

            status_code = _status_code(response)
            text = _response_text(response)

            # 3xx: explicit redirect handling.
            if 300 <= status_code < 400:
                location = _safe_location(response)
                reason = (
                    "redirect_to_login"
                    if "/accounts/login" in location
                    else "redirect_to_checkpoint"
                    if ("/challenge" in location or "/checkpoint" in location)
                    else "redirect_to_homepage"
                )
                logger.warning(
                    "Instagram API redirected (%d) to %s — reason=%s",
                    status_code,
                    location,
                    reason,
                )
                auth_redirect = any(token in location for token in ("login", "challenge", "checkpoint"))
                if reason == "redirect_to_homepage":
                    if not homepage_redirect_recovery_attempted:
                        homepage_redirect_recovery_attempted = True
                        if await self._recover_homepage_redirect(referer=referer):
                            continue
                    if (
                        not browser_api_fallback_attempted
                        and _env_truthy(_BROWSER_API_FALLBACK_ENV, True)
                    ):
                        browser_api_fallback_attempted = True
                        self._record_retry_reason("browser_api_fallback")
                        try:
                            browser_response = await self._fetch_api_with_browser(
                                url,
                                referer=referer,
                                params=params,
                                deadline=deadline,
                            )
                        except _PaginationDeadlineExceededError:
                            return _deadline_response(attempt)
                        except TimeoutError:
                            return _deadline_response(attempt)
                        except Exception as exc:  # noqa: BLE001
                            if not _warmup_transport_failure(exc):
                                raise
                            reason = _transport_failure_reason(exc)
                            self._record_retry_reason(reason)
                            return {
                                "failed": True,
                                "auth_failed": False,
                                "reason": reason,
                                "retryable": True,
                                "payload": None,
                                "attempt_count": attempt,
                            }
                        browser_result = self._decode_json_response_result(browser_response, attempt=attempt)
                        if not (
                            browser_result.get("failed")
                            and browser_result.get("reason") == "redirect_to_homepage"
                        ):
                            return browser_result
                    auth_redirect = True
                return {
                    "failed": True,
                    "auth_failed": auth_redirect,
                    "reason": reason,
                    "retryable": False,
                    "payload": None,
                    "attempt_count": attempt,
                }

            # Transient 429 / 5xx: retry with backoff.
            if self._is_transient_status(status_code):
                last_transient_reason = f"http_{status_code}"
                self._record_retry_reason(last_transient_reason)
                if attempt > retry_limit:
                    return {
                        "failed": True,
                        "auth_failed": False,
                        "reason": last_transient_reason,
                        "retryable": True,
                        "payload": None,
                        "attempt_count": attempt,
                    }
                try:
                    await self._rebuild_http_client()
                except Exception:  # noqa: BLE001
                    logger.debug(
                        "Failed to rebuild Instagram comments HTTP client after transient HTTP status",
                        exc_info=True,
                    )
                fallback_attempt = _resolve_positive_int_env(
                    _BROWSER_API_FALLBACK_ON_429_ATTEMPT_ENV,
                    3,
                    minimum=1,
                    maximum=20,
                )
                if (
                    status_code == 429
                    and not browser_api_fallback_attempted
                    and attempt >= fallback_attempt
                    and _env_truthy(_BROWSER_API_FALLBACK_ON_429_ENV, True)
                    and _env_truthy(_BROWSER_API_FALLBACK_ENV, True)
                ):
                    browser_api_fallback_attempted = True
                    self._record_retry_reason("browser_api_fallback_after_429")
                    try:
                        browser_response = await self._fetch_api_with_browser(
                            url,
                            referer=referer,
                            params=params,
                            deadline=deadline,
                        )
                    except _PaginationDeadlineExceededError:
                        return _deadline_response(attempt)
                    except TimeoutError:
                        return _deadline_response(attempt)
                    except Exception as exc:  # noqa: BLE001
                        if not _warmup_transport_failure(exc):
                            raise
                        self._record_retry_reason(_transport_failure_reason(exc))
                    else:
                        browser_result = self._decode_json_response_result(browser_response, attempt=attempt)
                        if not (
                            browser_result.get("failed")
                            and browser_result.get("reason") == "http_429"
                        ):
                            return browser_result
                retry_after = self._retry_after_seconds(response)
                sleep_seconds = _transient_backoff_seconds(
                    attempt,
                    self._BASE_BACKOFF_SECONDS,
                    retry_after=retry_after,
                )
                if status_code == 429:
                    cooldown_seconds = max(
                        sleep_seconds * self._rate_limit_cooldown_multiplier,
                        self._rate_limit_cooldown_min_seconds,
                    )
                    _record_global_api_cooldown(
                        key=self._global_rate_limit_key,
                        delay_seconds=cooldown_seconds,
                    )
                if not await _sleep_before_deadline(sleep_seconds, deadline):
                    return _deadline_response(attempt)
                continue

            # Permanent 4xx.
            if status_code >= 400:
                return self._decode_json_response_result(response, attempt=attempt)

            # HTML response (challenge page, not JSON).
            if text and text.lstrip().startswith("<"):
                return self._decode_json_response_result(response, attempt=attempt)

            return self._decode_json_response_result(response, attempt=attempt)
