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
from collections import Counter
from collections.abc import Iterable, Mapping
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
    child_reply_count,
    flattened_comment_count,
    merge_comment_replies,
    missing_reply_count,
    parent_comment_count,
    parentless_reply_ids,
    reply_count_observed,
)
from trr_backend.socials.instagram.comments_scrapling.proxy import CommentsProxyConfig
from trr_backend.socials.instagram.constants import COMMENT_REPLIES_URL, COMMENTS_URL, resolve_comment_sort_order
from trr_backend.socials.instagram.permalink_metadata import _graphql_doc_ids, _shortcode_to_media_id
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
_COMMENTS_ENDPOINT_PROBE_TIMEOUT_SECONDS_DEFAULT = 20.0
_REVEAL_HIDDEN_COMMENTS_ENV = "SOCIAL_INSTAGRAM_COMMENTS_REVEAL_HIDDEN"
_REVEAL_HIDDEN_COMMENTS_WITHOUT_EXPECTED_ENV = "SOCIAL_INSTAGRAM_COMMENTS_REVEAL_HIDDEN_WITHOUT_EXPECTED"
_HIDDEN_COMMENTS_CLICK_LIMIT_DEFAULT = 4
_HIDDEN_UNAVAILABLE_GAP_MAX_DEFAULT = 1
_HIDDEN_UNAVAILABLE_GAP_RATIO_DEFAULT = 0.0
_COAUTHOR_STATUS_ONLY_CLICK_LIMIT_DEFAULT = 8
_COAUTHOR_STATUS_ONLY_SCROLL_LIMIT_DEFAULT = 8
_COAUTHOR_RENDERED_FALLBACK_VERSION = "2026-05-05.coauthor-rendered-dom-v2"
_STATUS_ONLY_METADATA_MAX_ITEMS = 20
_POST_ACTION_GRAPHQL_URL = "https://www.instagram.com/graphql/query/"
_POST_ACTION_GRAPHQL_FRIENDLY_NAME = "PolarisPostActionLoadPostQueryQuery"
_POST_COMMENTS_GRAPHQL_FRIENDLY_NAME = "PolarisPostCommentsPaginationQuery"
_POST_COMMENTS_GRAPHQL_HEADER_FRIENDLY_NAME = "PolarisPostCommentsPaginationQuery"
_POST_COMMENTS_GRAPHQL_DOC_IDS = ("25516980651312394", "26113520058347588")
_POST_COMMENTS_GRAPHQL_PAGE_SIZE = 12
_POST_COMMENTS_GRAPHQL_MAX_PAGES = 25
_POST_CHILD_COMMENTS_GRAPHQL_FRIENDLY_NAME = "PolarisPostChildCommentsQuery"
_POST_CHILD_COMMENTS_GRAPHQL_DOC_ATTEMPTS = (
    ("PolarisPostChildCommentsQuery", "34884685271179117"),
    ("PolarisPostCommentsChildrenPaginationtQuery", "36239935742272683"),
)
_POST_CHILD_COMMENTS_GRAPHQL_PAGE_SIZE = 12
_POST_CHILD_COMMENTS_GRAPHQL_MAX_PAGES = 10
_GRAPHQL_COAUTHOR_SOURCE_SNAPSHOT_TYPE = "graphql_coauthor_preview_comments"
_RELAY_COAUTHOR_SOURCE_SNAPSHOT_TYPE = "graphql_coauthor_relay_comments"
_TERMINAL_MISSING_CLASSIFIED_REASON = "coverage_terminal_missing_classified"
_TERMINAL_MISSING_REASON_INSTAGRAM_NOT_SERVED = "instagram_not_served_after_all_lanes"
_PARENTLESS_REPLY_ATTACH_FAILED_REASON = "parentless_reply_attach_failed"
_LOGGED_OUT_LSD_RE = re.compile(r'"LSD",\[\],\{"token":"([^"]+)"')
_LOGGED_OUT_MREQUEST_LSD_RE = re.compile(r'"lsd":"([^"]+)"')
_LOGGED_OUT_JAZOEST_RE = re.compile(r"jazoest=(\d+)")
_LOGGED_OUT_SPIN_RE = re.compile(r'"(?P<key>__spin_[rbt])":(?P<value>\d+|"[^"]+")')
_POST_COMMENTS_CONTAINER_QUERY_RE = re.compile(
    r'"queryID":"(?P<query_id>\d+)","variables":\{"media_id":"(?P<media_id>\d+)"[^}]*\},'
    r'"queryName":"PolarisPostCommentsContainerQuery"'
)

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
_PROFILE_HREF_RE = re.compile(
    r"href=[\"'](?:https?://(?:www\.)?instagram\.com)?/([^/\"?#]+)/?[\"']",
    re.IGNORECASE,
)
_RENDERED_COMMENTS_JSON_ID = "trr-rendered-comments-json"
_RENDERED_COMMENTS_JSON_RE = re.compile(
    rf"<script[^>]*\bid=[\"']{_RENDERED_COMMENTS_JSON_ID}[\"'][^>]*>(?P<payload>.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
_RENDERED_DOM_TIME_PREFIX_RE = re.compile(r"^(?:now|\d+\s*[smhdwy])\b\s*", re.IGNORECASE)
_RENDERED_DOM_REPLY_SUFFIX_RE = re.compile(
    r"\s*(?:(?P<likes>\d[\d,]*)\s+likes?\s*)?Reply(?:\s+Like(?:\s+Like)?)?\s*$",
    re.IGNORECASE,
)
_RENDERED_DOM_VIEW_REPLIES_SUFFIX_RE = re.compile(
    r"\s*View all\s+\d[\d,]*\s+repl(?:y|ies)\s*$",
    re.IGNORECASE,
)
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
    source_snapshot_type: str = "rendered_hidden_comments",
    is_hidden_by_instagram: bool = True,
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
        rf"href=[\"'](?P<href>(?:https?://(?:www\.)?instagram\.com)?"
        rf"/p/{re.escape(normalized_shortcode)}/c/(?P<comment_id>\d+)/?[^\"']*)[\"']",
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
                is_hidden_by_instagram=is_hidden_by_instagram,
                source_snapshot_type=source_snapshot_type,
            )
        )
    return comments


def _rendered_dom_synthetic_comment_id(
    *,
    shortcode: str,
    username: str,
    text: str,
    parent_comment_id: str | None = None,
) -> str:
    digest_source = "\n".join(
        [
            str(shortcode or "").strip(),
            _normalize_rendered_ignored_username(username),
            str(parent_comment_id or "").strip(),
            str(text or "").strip(),
        ]
    )
    digest = hashlib.sha1(digest_source.encode("utf-8")).hexdigest()
    return f"rendered_{digest[:24]}"


def _parse_rendered_dom_comment_text(row_text: str, *, username: str) -> tuple[str, int]:
    text = _clean_html_text(row_text)
    normalized_username = str(username or "").strip()
    if normalized_username and text.lower().startswith(normalized_username.lower()):
        text = text[len(normalized_username) :].strip()
    if not _RENDERED_DOM_TIME_PREFIX_RE.match(text):
        return "", 0
    text = _RENDERED_DOM_TIME_PREFIX_RE.sub("", text, count=1).strip()
    text = _RENDERED_DOM_VIEW_REPLIES_SUFFIX_RE.sub("", text).strip()
    likes = 0
    suffix_match = _RENDERED_DOM_REPLY_SUFFIX_RE.search(text)
    if suffix_match:
        like_value = suffix_match.group("likes")
        if like_value:
            likes = int(like_value.replace(",", "") or 0)
        text = text[: suffix_match.start()].strip()
    return text, likes


def _coerce_rendered_dom_row_left(row: Mapping[str, Any]) -> int | None:
    for key in ("left", "anchorLeft"):
        parsed = _safe_non_negative_int(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _rendered_dom_snapshot_payload(html_text: str) -> tuple[Any, str | None]:
    match = _RENDERED_COMMENTS_JSON_RE.search(str(html_text or ""))
    if not match:
        return None, "snapshot_script_missing"
    try:
        return json.loads(html_lib.unescape(match.group("payload") or "")), None
    except (TypeError, json.JSONDecodeError) as exc:
        return None, f"snapshot_json_invalid:{exc.__class__.__name__}"


def _compact_rendered_dom_snapshot_diagnostics(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    allowed_scalar_keys = {
        "version",
        "url",
        "title",
        "readyState",
        "userAgent",
        "bodyTextLength",
        "initialProfileAnchors",
        "initialCommentPermalinks",
        "initialReplyTextMatches",
        "candidateProfileAnchors",
        "reservedProfileAnchors",
        "rowTextMismatchAnchors",
        "longRowAnchors",
        "emptyRowAnchors",
        "clickedControls",
        "scrollSteps",
        "rowsCollected",
        "error",
        "errorStack",
    }
    compact: dict[str, Any] = {
        key: value.get(key)
        for key in allowed_scalar_keys
        if value.get(key) is not None
    }
    samples = value.get("rowTextSamples")
    if isinstance(samples, list):
        compact["rowTextSamples"] = [str(sample)[:180] for sample in samples[:3]]
    return compact


def _rendered_dom_snapshot_metadata(
    html_text: str,
    *,
    shortcode: str,
    dom_comments_count: int,
    permalink_comments_count: int,
    status_code: int | None = None,
    location: str | None = None,
) -> dict[str, Any]:
    text = str(html_text or "")
    normalized_shortcode = str(shortcode or "").strip()
    payload, error = _rendered_dom_snapshot_payload(text)
    rows: Any = None
    diagnostics: Any = None
    if isinstance(payload, Mapping):
        rows = payload.get("rows")
        diagnostics = payload.get("diagnostics")
    elif isinstance(payload, list):
        rows = payload
    return {
        "version": _COAUTHOR_RENDERED_FALLBACK_VERSION,
        "status_code": status_code,
        "location": location,
        "html_length": len(text),
        "snapshot_present": error != "snapshot_script_missing",
        "snapshot_error": error,
        "snapshot_rows": len(rows) if isinstance(rows, list) else 0,
        "dom_comments": max(0, int(dom_comments_count or 0)),
        "permalink_comments": max(0, int(permalink_comments_count or 0)),
        "markers": {
            "has_shortcode": bool(normalized_shortcode and normalized_shortcode in text),
            "has_add_comment": "Add a comment" in text,
            "has_reply": "Reply" in text,
            "has_comment_permalink": "/c/" in text,
            "has_login": "/accounts/login" in text or "Log in" in text,
            "has_challenge": "challenge" in text.lower() or "checkpoint" in text.lower(),
        },
        "snapshot_diagnostics": _compact_rendered_dom_snapshot_diagnostics(diagnostics),
    }


def _rendered_dom_should_retry_without_proxy(metadata: Mapping[str, Any]) -> bool:
    markers = metadata.get("markers")
    diagnostics = metadata.get("snapshot_diagnostics")
    title = ""
    body_samples: list[str] = []
    if isinstance(diagnostics, Mapping):
        title = str(diagnostics.get("title") or "").strip().lower()
        samples = diagnostics.get("rowTextSamples")
        if isinstance(samples, list):
            body_samples = [str(sample or "").strip().lower() for sample in samples[:3]]
    marker_map = markers if isinstance(markers, Mapping) else {}
    has_comment_surface = bool(marker_map.get("has_add_comment") or marker_map.get("has_comment_permalink"))
    body_text = " ".join(body_samples)
    page_load_failed = "couldn't load" in title or "could not be loaded" in body_text
    geo_restricted = "restricted post" in body_text or "not available in your country" in body_text
    snapshot_rows = _safe_non_negative_int(metadata.get("snapshot_rows")) or 0
    return (page_load_failed or geo_restricted) and not has_comment_surface and snapshot_rows <= 3


def _extract_rendered_dom_snapshot_comments(
    html_text: str,
    *,
    shortcode: str,
    post_url: str,
    ignored_usernames: Iterable[str] | None = None,
    source_snapshot_type: str = "rendered_coauthor_comments",
    is_hidden_by_instagram: bool = False,
) -> list[InstagramComment]:
    """Extract comments from the browser-injected rendered DOM snapshot.

    Coauthored Instagram posts can render comments in the browser while omitting
    both the public comments API payload and stable comment permalink anchors.
    The page action injects a compact JSON snapshot of visible rows so this lane
    can still persist those otherwise unreachable comments.
    """

    normalized_shortcode = str(shortcode or "").strip()
    if not normalized_shortcode:
        return []

    payload, error = _rendered_dom_snapshot_payload(str(html_text or ""))
    if error:
        return []
    rows = payload.get("rows") if isinstance(payload, Mapping) else payload
    if not isinstance(rows, list):
        return []

    ignored = set(_INSTAGRAM_RESERVED_PROFILE_PATHS)
    ignored.update(
        normalized
        for normalized in (_normalize_rendered_ignored_username(value) for value in (ignored_usernames or []))
        if normalized
    )

    valid_rows: list[Mapping[str, Any]] = [row for row in rows if isinstance(row, Mapping)]
    row_left_values = [
        left
        for left in (_coerce_rendered_dom_row_left(row) for row in valid_rows)
        if left is not None and left > 50
    ]
    base_left = min(row_left_values) if row_left_values else None
    reply_indent_threshold = 20

    comments: list[InstagramComment] = []
    current_parent: InstagramComment | None = None
    seen_keys: set[tuple[str, str, str | None]] = set()
    for row in valid_rows:
        username = _normalize_rendered_ignored_username(row.get("username"))
        if not username or username in ignored:
            continue
        text, likes = _parse_rendered_dom_comment_text(str(row.get("rowText") or ""), username=username)
        if not text:
            continue

        row_left = _coerce_rendered_dom_row_left(row)
        parent_comment_id: str | None = None
        is_reply = False
        if current_parent is not None and base_left is not None and row_left is not None:
            is_reply = row_left >= base_left + reply_indent_threshold
            if is_reply:
                parent_comment_id = current_parent.comment_id

        dedupe_key = (username, text, parent_comment_id)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        comment_id = str(row.get("commentId") or "").strip()
        if not comment_id:
            comment_id = _rendered_dom_synthetic_comment_id(
                shortcode=normalized_shortcode,
                username=username,
                text=text,
                parent_comment_id=parent_comment_id,
            )
        comment_href = str(row.get("commentHref") or "").strip()
        comment_url = None
        if comment_href:
            comment_url = (
                comment_href
                if comment_href.startswith(("http://", "https://"))
                else f"https://www.instagram.com{comment_href}"
            )
        comment = InstagramComment(
            comment_id=comment_id,
            text=text,
            username=username,
            user_id="",
            created_at=0,
            date_time="",
            likes=likes,
            is_reply=is_reply,
            parent_comment_id=parent_comment_id,
            reply_count=0,
            reply_depth=1 if is_reply else 0,
            post_shortcode=normalized_shortcode,
            post_url=post_url,
            comment_url=comment_url,
            source_snapshot_type=source_snapshot_type,
            is_hidden_by_instagram=is_hidden_by_instagram,
            owner_profile_pic_url=str(row.get("profilePicUrl") or "").strip() or None,
        )
        if is_reply and current_parent is not None:
            current_parent.replies.append(comment)
            current_parent.reply_count_observed = len(current_parent.replies)
            current_parent.reply_count = max(int(current_parent.reply_count or 0), len(current_parent.replies))
            continue
        comments.append(comment)
        current_parent = comment

    return comments


def _graphql_comment_owner(node: Mapping[str, Any]) -> Mapping[str, Any]:
    owner = node.get("owner")
    if isinstance(owner, Mapping):
        return owner
    user = node.get("user")
    return user if isinstance(user, Mapping) else {}


def _graphql_comment_id(node: Mapping[str, Any]) -> str:
    for key in ("id", "pk", "comment_id"):
        value = str(node.get(key) or "").strip()
        if value:
            return value
    return ""


def _graphql_comment_timestamp(node: Mapping[str, Any]) -> int:
    for key in ("created_at", "created_at_utc", "timestamp"):
        parsed = _safe_non_negative_int(node.get(key))
        if parsed is not None:
            return parsed
    return 0


def _graphql_comment_likes(node: Mapping[str, Any]) -> int:
    for key in ("comment_like_count", "like_count", "likes"):
        parsed = _safe_non_negative_int(node.get(key))
        if parsed is not None:
            return parsed
    edge_liked_by = node.get("edge_liked_by")
    if isinstance(edge_liked_by, Mapping):
        parsed = _safe_non_negative_int(edge_liked_by.get("count"))
        if parsed is not None:
            return parsed
    return 0


def _graphql_comment_reply_edge(node: Mapping[str, Any]) -> Mapping[str, Any]:
    edge = node.get("edge_threaded_comments")
    return edge if isinstance(edge, Mapping) else {}


def _graphql_comment_reply_edges(node: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    edge = _graphql_comment_reply_edge(node)
    rows = edge.get("edges")
    if not isinstance(rows, list):
        return []
    out: list[Mapping[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        child = row.get("node")
        if isinstance(child, Mapping):
            out.append(child)
    return out


def _graphql_comment_to_instagram_comment(
    node: Mapping[str, Any],
    *,
    shortcode: str,
    post_url: str,
    is_reply: bool = False,
    parent_comment_id: str | None = None,
    reply_depth: int = 0,
    phase: str | None = None,
    source_snapshot_type: str = _GRAPHQL_COAUTHOR_SOURCE_SNAPSHOT_TYPE,
) -> InstagramComment | None:
    comment_id = _graphql_comment_id(node)
    text = str(node.get("text") or "").strip()
    owner = _graphql_comment_owner(node)
    username = str(owner.get("username") or node.get("ownerUsername") or "").strip()
    user_id = str(owner.get("id") or owner.get("pk") or node.get("ownerId") or "").strip()

    def first_present(*keys: str) -> Any:
        for source in (node, owner):
            for key in keys:
                if key in source and source.get(key) is not None:
                    return source.get(key)
        return None

    def coerce_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "t", "1", "yes", "y", "on"}:
                return True
            if normalized in {"false", "f", "0", "no", "n", "off"}:
                return False
        return default

    def first_text(*keys: str) -> str | None:
        value = first_present(*keys)
        text_value = str(value or "").strip()
        return text_value or None

    is_covered = coerce_bool(first_present("is_covered", "isCovered", "covered"), False)
    if not comment_id or not username or (not text and not is_covered):
        return None

    created_at = _graphql_comment_timestamp(node)
    created_at_iso: str | None = None
    date_time = ""
    if created_at:
        try:
            created_at_datetime = datetime.fromtimestamp(created_at, tz=UTC)
            created_at_iso = created_at_datetime.isoformat(timespec="milliseconds").replace("+00:00", "Z")
            date_time = created_at_datetime.strftime("%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError, OSError, OverflowError):
            created_at_iso = None
            date_time = ""

    reply_edge = _graphql_comment_reply_edge(node)
    reply_count = _safe_non_negative_int(reply_edge.get("count")) if reply_edge else None
    if reply_count is None:
        reply_count = _safe_non_negative_int(node.get("child_comment_count"))
    reply_nodes = _graphql_comment_reply_edges(node)
    replies: list[InstagramComment] = []
    for reply_node in reply_nodes:
        reply = _graphql_comment_to_instagram_comment(
            reply_node,
            shortcode=shortcode,
            post_url=post_url,
            is_reply=True,
            parent_comment_id=comment_id,
            reply_depth=reply_depth + 1,
            phase="child",
            source_snapshot_type=source_snapshot_type,
        )
        if reply is not None:
            replies.append(reply)

    normalized_shortcode = str(shortcode or "").strip()
    normalized_phase = (
        "child"
        if is_reply or parent_comment_id
        else str(phase or first_present("phase", "comment_phase", "commentPhase", "source_phase") or "").strip()
        or None
    )
    child_comment_count = _safe_non_negative_int(
        first_present("child_comment_count", "childCommentCount", "repliesCount", "reply_count", "replies_count")
    )
    if child_comment_count is None and reply_count is not None:
        child_comment_count = reply_count
    cursor_payload = first_present("cursor_payload", "cursorPayload")
    if not isinstance(cursor_payload, Mapping):
        cursor_payload = {}
    return InstagramComment(
        comment_id=comment_id,
        text=text,
        username=username,
        user_id=user_id,
        created_at=created_at,
        date_time=date_time,
        likes=_graphql_comment_likes(node),
        is_reply=is_reply,
        parent_comment_id=parent_comment_id,
        reply_count=reply_count if reply_count is not None else len(replies),
        reply_depth=max(0, int(reply_depth or 0)),
        replies=replies,
        owner_full_name=str(owner.get("full_name") or owner.get("fullName") or "").strip() or None,
        owner_profile_pic_url=str(owner.get("profile_pic_url") or owner.get("profilePicUrl") or "").strip() or None,
        owner_profile_pic_url_hd=str(
            owner.get("profile_pic_url_hd") or owner.get("profilePicUrlHd") or ""
        ).strip()
        or None,
        owner_is_verified=bool(owner.get("is_verified")) if "is_verified" in owner else None,
        post_shortcode=normalized_shortcode,
        post_url=post_url,
        comment_url=f"https://www.instagram.com/p/{normalized_shortcode}/c/{comment_id}/"
        if normalized_shortcode
        else None,
        created_at_iso=created_at_iso,
        reply_count_observed=len(replies),
        source_snapshot_type=source_snapshot_type,
        is_covered=is_covered,
        is_ranked=coerce_bool(first_present("is_ranked", "isRanked", "ranked"), normalized_phase == "ranked"),
        comment_index=_safe_non_negative_int(
            first_present("comment_index", "commentIndex", "ranked_index", "rankedIndex")
        ),
        phase=normalized_phase,
        did_report_as_spam=coerce_bool(
            first_present("did_report_as_spam", "didReportAsSpam", "reported_as_spam", "reportedAsSpam"),
            False,
        ),
        status=first_text("status", "comment_status", "commentStatus") or "Active",
        is_edited=coerce_bool(first_present("is_edited", "isEdited", "edited"), False),
        is_pinned=coerce_bool(first_present("is_pinned", "isPinned", "pinned"), False),
        meta_ai_comment_type=first_text("meta_ai_comment_type", "metaAiCommentType") or "NONE",
        child_comment_count=child_comment_count or 0,
        liked_by_media_coauthors=coerce_bool(
            first_present(
                "liked_by_media_coauthors",
                "likedByMediaCoauthors",
                "liked_by_media_coauthor",
                "likedByMediaCoauthor",
            ),
            False,
        ),
        cursor_min_id=first_text("cursor_min_id", "cursorMinId", "min_id", "next_min_id", "nextMinId"),
        cursor_param=first_text("cursor_param", "cursorParam", "cursor_name", "cursorName", "cursor_param_name"),
        cursor_payload=dict(cursor_payload),
        comment_filter_param=first_text(
            "comment_filter_param",
            "commentFilterParam",
            "comment_filter",
            "commentFilter",
        ),
    )


def _extract_graphql_preview_comments(
    payload: Mapping[str, Any],
    *,
    shortcode: str,
    post_url: str,
    source_snapshot_type: str = _GRAPHQL_COAUTHOR_SOURCE_SNAPSHOT_TYPE,
) -> tuple[list[InstagramComment], dict[str, Any]]:
    data = payload.get("data")
    media = data.get("xdt_shortcode_media") if isinstance(data, Mapping) else None
    if not isinstance(media, Mapping):
        return [], {"payload_shape": _payload_shape(payload), "media_found": False}
    comments_edge = media.get("edge_media_to_parent_comment")
    if not isinstance(comments_edge, Mapping):
        return [], {
            "payload_shape": _payload_shape(payload),
            "media_found": True,
            "comments_edge_found": False,
        }
    rows = comments_edge.get("edges")
    if not isinstance(rows, list):
        rows = []

    comments: list[InstagramComment] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        node = row.get("node")
        if not isinstance(node, Mapping):
            continue
        comment = _graphql_comment_to_instagram_comment(
            node,
            shortcode=shortcode,
            post_url=post_url,
            source_snapshot_type=source_snapshot_type,
        )
        if comment is not None:
            comments.append(comment)

    page_info = comments_edge.get("page_info")
    return comments, {
        "payload_shape": _payload_shape(payload),
        "media_found": True,
        "comments_edge_found": True,
        "reported_comment_count": _safe_non_negative_int(comments_edge.get("count")),
        "top_level_preview_count": len(comments),
        "flattened_preview_count": flattened_comment_count(comments),
        "page_info": dict(page_info) if isinstance(page_info, Mapping) else None,
        "has_next_page": bool(page_info.get("has_next_page")) if isinstance(page_info, Mapping) else None,
    }


def _post_comments_graphql_doc_ids(*extra_doc_ids: str | None) -> list[str]:
    ids: list[str] = []
    override = str(os.getenv("INSTAGRAM_POST_COMMENTS_GRAPHQL_DOC_IDS") or "").strip()
    for source in [override, *extra_doc_ids, *_POST_COMMENTS_GRAPHQL_DOC_IDS]:
        for candidate in str(source or "").split(","):
            normalized = candidate.strip()
            if normalized and normalized not in ids:
                ids.append(normalized)
    return ids


def _post_child_comments_graphql_doc_attempts() -> list[tuple[str, str]]:
    attempts: list[tuple[str, str]] = []
    override_friendly_name = str(os.getenv("INSTAGRAM_POST_CHILD_COMMENTS_GRAPHQL_FRIENDLY_NAME") or "").strip()
    override_doc_ids = str(os.getenv("INSTAGRAM_POST_CHILD_COMMENTS_GRAPHQL_DOC_IDS") or "").strip()
    for candidate in override_doc_ids.split(","):
        doc_id = candidate.strip()
        if doc_id:
            attempts.append((override_friendly_name or _POST_CHILD_COMMENTS_GRAPHQL_FRIENDLY_NAME, doc_id))
    for friendly_name, doc_id in _POST_CHILD_COMMENTS_GRAPHQL_DOC_ATTEMPTS:
        normalized_doc_id = str(doc_id or "").strip()
        if normalized_doc_id and all(existing_doc_id != normalized_doc_id for _, existing_doc_id in attempts):
            attempts.append((str(friendly_name or _POST_CHILD_COMMENTS_GRAPHQL_FRIENDLY_NAME), normalized_doc_id))
    return attempts


def _extract_logged_out_graphql_context(html: str, *, media_id: str | None = None) -> dict[str, Any]:
    text = str(html or "")
    lsd_match = _LOGGED_OUT_LSD_RE.search(text) or _LOGGED_OUT_MREQUEST_LSD_RE.search(text)
    jazoest_match = _LOGGED_OUT_JAZOEST_RE.search(text)
    query_id: str | None = None
    for match in _POST_COMMENTS_CONTAINER_QUERY_RE.finditer(text):
        if media_id and match.group("media_id") != media_id:
            continue
        query_id = match.group("query_id")
        break
    spin: dict[str, str] = {}
    for match in _LOGGED_OUT_SPIN_RE.finditer(text):
        value = match.group("value").strip('"')
        spin[match.group("key")] = value
    return {
        "lsd": lsd_match.group(1) if lsd_match else None,
        "jazoest": jazoest_match.group(1) if jazoest_match else None,
        "container_query_id": query_id,
        "spin": spin,
    }


def _graphql_connection_page_info(connection: Mapping[str, Any]) -> Mapping[str, Any]:
    page_info = connection.get("page_info")
    return page_info if isinstance(page_info, Mapping) else {}


def _extract_graphql_connection_comments(
    payload: Mapping[str, Any],
    *,
    shortcode: str,
    post_url: str,
    source_snapshot_type: str = _RELAY_COAUTHOR_SOURCE_SNAPSHOT_TYPE,
) -> tuple[list[InstagramComment], dict[str, Any]]:
    data = payload.get("data")
    connection = (
        data.get("xdt_api__v1__media__media_id__comments__connection")
        if isinstance(data, Mapping)
        else None
    )
    if not isinstance(connection, Mapping):
        return [], {"payload_shape": _payload_shape(payload), "connection_found": False}

    rows = connection.get("edges")
    if not isinstance(rows, list):
        rows = []

    comments: list[InstagramComment] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        node = row.get("node")
        if not isinstance(node, Mapping):
            continue
        comment = _graphql_comment_to_instagram_comment(
            node,
            shortcode=shortcode,
            post_url=post_url,
            source_snapshot_type=source_snapshot_type,
        )
        if comment is not None:
            comments.append(comment)

    page_info = _graphql_connection_page_info(connection)
    return comments, {
        "payload_shape": _payload_shape(payload),
        "connection_found": True,
        "edge_count": len(rows),
        "top_level_count": len(comments),
        "flattened_count": flattened_comment_count(comments),
        "page_info": dict(page_info),
        "has_next_page": bool(page_info.get("has_next_page")),
        "end_cursor": str(page_info.get("end_cursor") or "").strip() or None,
    }


def _extract_graphql_child_connection_comments(
    payload: Mapping[str, Any],
    *,
    shortcode: str,
    post_url: str,
    parent_comment_id: str,
    source_snapshot_type: str = _RELAY_COAUTHOR_SOURCE_SNAPSHOT_TYPE,
) -> tuple[list[InstagramComment], dict[str, Any]]:
    data = payload.get("data")
    connection = (
        data.get("xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection")
        if isinstance(data, Mapping)
        else None
    )
    if not isinstance(connection, Mapping):
        return [], {"payload_shape": _payload_shape(payload), "connection_found": False}

    rows = connection.get("edges")
    if not isinstance(rows, list):
        rows = []

    comments: list[InstagramComment] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        node = row.get("node")
        if not isinstance(node, Mapping):
            continue
        comment = _graphql_comment_to_instagram_comment(
            node,
            shortcode=shortcode,
            post_url=post_url,
            is_reply=True,
            parent_comment_id=parent_comment_id,
            reply_depth=1,
            source_snapshot_type=source_snapshot_type,
        )
        if comment is not None:
            comments.append(comment)

    page_info = _graphql_connection_page_info(connection)
    return comments, {
        "payload_shape": _payload_shape(payload),
        "connection_found": True,
        "edge_count": len(rows),
        "reply_count": len(comments),
        "page_info": dict(page_info),
        "has_next_page": bool(page_info.get("has_next_page")),
        "end_cursor": str(page_info.get("end_cursor") or "").strip() or None,
    }


def _decode_graphql_json_payload(response: httpx.Response) -> Mapping[str, Any] | None:
    try:
        payload = response.json()
    except ValueError:
        text = _response_text(response).lstrip()
        if text.startswith("for (;;);"):
            text = text[len("for (;;);") :].lstrip()
        try:
            payload = json.loads(text)
        except ValueError:
            return None
    return payload if isinstance(payload, Mapping) else None


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


def _instagram_comment_phase_counts(comments: Iterable[InstagramComment]) -> dict[str, int]:
    counts: Counter[str] = Counter()

    def visit(comment: InstagramComment) -> None:
        phase = str(getattr(comment, "phase", "") or "").strip() or "unknown"
        counts[phase] += 1
        for reply in list(getattr(comment, "replies", []) or []):
            if isinstance(reply, InstagramComment):
                visit(reply)

    for item in comments:
        if isinstance(item, InstagramComment):
            visit(item)
    return dict(sorted(counts.items()))


def _ensure_child_reply_phase(comments: Iterable[InstagramComment]) -> None:
    def visit(comment: InstagramComment, *, nested: bool) -> None:
        if nested or bool(getattr(comment, "is_reply", False)) or getattr(comment, "parent_comment_id", None):
            comment.is_reply = True
            comment.phase = "child"
        for reply in list(getattr(comment, "replies", []) or []):
            if isinstance(reply, InstagramComment):
                if not getattr(reply, "parent_comment_id", None):
                    reply.parent_comment_id = str(getattr(comment, "comment_id", "") or "").strip() or None
                visit(reply, nested=True)

    for item in comments:
        if isinstance(item, InstagramComment):
            visit(item, nested=False)


def _hidden_unavailable_gap_is_tolerable(*, unresolved_gap: int, target_count: int) -> bool:
    gap = max(0, int(unresolved_gap or 0))
    target = max(0, int(target_count or 0))
    if gap <= 0:
        return True
    if target <= 0:
        return False
    if target <= 3 and gap <= 2:
        return True
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


_TOP_LEVEL_CURSOR_KEYS = frozenset(
    {
        "next_min_id",
        "next_max_id",
        "cached_comments_cursor",
        "bifilter_token",
        "tao_cursor",
    }
)
_TOP_LEVEL_PAGINATION_KEYS = frozenset(
    {
        "has_more_comments",
        "has_more_headload_comments",
        *_TOP_LEVEL_CURSOR_KEYS,
    }
)
_TOP_LEVEL_CURSOR_PARAM_BY_PAYLOAD_KEY = {
    "next_min_id": "min_id",
    "next_max_id": "max_id",
    "cached_comments_cursor": "cached_comments_cursor",
    "bifilter_token": "bifilter_token",
    "tao_cursor": "tao_cursor",
}
_TOP_LEVEL_CURSOR_PARAM_NAMES = frozenset(_TOP_LEVEL_CURSOR_PARAM_BY_PAYLOAD_KEY.values())
_COMMENT_FILTER_PARAM_KEYS = frozenset(
    {
        "comment_filter_param",
        "commentFilterParam",
        "comment_filter",
        "commentFilter",
        "filter_param",
        "filterParam",
    }
)
_FB_CROSSPOST_PAGINATION_KEYS = frozenset(
    {
        "has_more_headload_fb_comments",
    }
)
_COMMENT_COUNT_KEY_MARKERS = frozenset({"count", "total"})
_TARGET_METADATA_TEXT_KEYS = (
    "source_id",
    "post_id",
    "materialized_post_id",
    "profile_account",
    "selected_profile_account",
    "source_account",
    "account_handle",
    "caption_author",
    "caption_writer",
    "original_author",
    "owner_username",
    "owner",
    "username",
    "media_type",
    "product_type",
    "profile_source_surface",
    "profile_match_mode",
)
_TARGET_METADATA_COLLABORATOR_KEYS = (
    "collaborator_handles",
    "collaborators",
    "collaborators_detail",
    "coauthors",
    "coauthor_handles",
    "collab_handles",
)


@dataclass(frozen=True, slots=True)
class _TopLevelPageEnvelope:
    rows: list[dict[str, Any]]
    has_more: bool
    primary_cursor: str | None
    primary_cursor_param: str | None
    alt_cursor: str | None
    alt_cursor_param: str | None
    cursor_payload: dict[str, Any]
    cursor_shape_names: tuple[str, ...]
    phase_signal: str | None
    comment_filter_param: str | None


def _compact_metadata_text(value: Any, *, max_length: int = _REPLY_CHECKPOINT_STRING_MAX_LENGTH) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text[:max_length] if len(text) > max_length else text


def _normalize_instagram_handle(value: Any) -> str:
    return str(value or "").strip().strip("/").lower().lstrip("@")


def _payload_keys(payload: Any) -> list[str]:
    if isinstance(payload, dict):
        return sorted(str(key) for key in payload.keys())
    if isinstance(payload, list):
        keys: set[str] = set()
        for item in payload:
            if isinstance(item, dict):
                keys.update(str(key) for key in item.keys())
        return sorted(keys)
    return []


def _payload_shape(payload: Any) -> str:
    if isinstance(payload, dict):
        return "dict"
    if isinstance(payload, list):
        return "list"
    return type(payload).__name__


def _payload_has_comment_count_field(payload: Any) -> bool:
    sources: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        sources.append(payload)
    elif isinstance(payload, list):
        sources.extend(item for item in payload if isinstance(item, dict))
    for source in sources:
        for key, value in source.items():
            normalized_key = str(key or "").strip().lower()
            if not normalized_key or value in (None, ""):
                continue
            if any(marker in normalized_key for marker in _COMMENT_COUNT_KEY_MARKERS):
                return True
    return False


def _payload_has_useful_top_level_cursor(payload: Any, response: dict[str, Any]) -> bool:
    metadata = _extract_page_metadata(payload, response, keys=_TOP_LEVEL_PAGINATION_KEYS)
    for key in _TOP_LEVEL_PAGINATION_KEYS:
        value = metadata.get(key)
        if key.startswith("next_") and str(value or "").strip():
            return True
        if key in _TOP_LEVEL_CURSOR_KEYS and str(value or "").strip():
            return True
        if key.startswith("has_more") and bool(value):
            return True
    return False


def _top_level_payload_is_status_only(
    payload: Any,
    response: dict[str, Any],
    *,
    comment_rows: list[dict[str, Any]],
    expected_comment_count: int | None,
) -> bool:
    if not expected_comment_count or expected_comment_count <= 0:
        return False
    if comment_rows:
        return False
    if _payload_has_useful_top_level_cursor(payload, response):
        return False
    if _payload_has_comment_count_field(payload):
        return False
    return isinstance(payload, (dict, list))


def _target_metadata_list_values(value: Any) -> list[str]:
    if isinstance(value, str):
        candidates = re.split(r"[,;\s]+", value)
    elif isinstance(value, Mapping):
        candidates = [value]
    elif isinstance(value, Iterable) and not isinstance(value, (dict, bytes, bytearray)):
        candidates = list(value)
    else:
        candidates = []
    values: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        text: str | None = None
        if isinstance(candidate, Mapping):
            for key in ("username", "handle", "user_name", "collaborator_handle", "source_account"):
                text = _compact_metadata_text(candidate.get(key), max_length=96)
                if text:
                    break
            if not text:
                for key in ("user", "owner", "profile"):
                    nested = candidate.get(key)
                    if not isinstance(nested, Mapping):
                        continue
                    for nested_key in ("username", "handle", "user_name"):
                        text = _compact_metadata_text(nested.get(nested_key), max_length=96)
                        if text:
                            break
                    if text:
                        break
        else:
            text = _compact_metadata_text(candidate, max_length=96)
        if not text:
            continue
        normalized = _normalize_instagram_handle(text)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalized)
        if len(values) >= 12:
            break
    return values


def _target_metadata_context(target_metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(target_metadata, Mapping):
        return {}
    context: dict[str, Any] = {}
    for key in _TARGET_METADATA_TEXT_KEYS:
        if key not in target_metadata:
            continue
        text = _compact_metadata_text(target_metadata.get(key), max_length=128)
        if text is not None:
            context[key] = text
    collaborator_handles: list[str] = []
    seen_collaborators: set[str] = set()
    for key in _TARGET_METADATA_COLLABORATOR_KEYS:
        for handle in _target_metadata_list_values(target_metadata.get(key)):
            if handle in seen_collaborators:
                continue
            seen_collaborators.add(handle)
            collaborator_handles.append(handle)
    if collaborator_handles:
        context["collaborator_handles"] = collaborator_handles
    for key in ("is_coauthor", "is_collaborator", "is_collaborator_post", "has_collaborators"):
        if key in target_metadata:
            context[key] = bool(target_metadata.get(key))
    return context


def _target_metadata_indicates_coauthor(target_metadata: Mapping[str, Any] | None) -> bool:
    context = _target_metadata_context(target_metadata)
    if any(
        bool(context.get(key))
        for key in ("is_coauthor", "is_collaborator", "is_collaborator_post", "has_collaborators")
    ):
        return True
    selected_profile = _normalize_instagram_handle(
        context.get("selected_profile_account")
        or context.get("profile_account")
        or context.get("account_handle")
        or context.get("source_account")
    )
    source_account = _normalize_instagram_handle(context.get("source_account"))
    owner_username = _normalize_instagram_handle(
        context.get("caption_author")
        or context.get("caption_writer")
        or context.get("original_author")
        or context.get("owner_username")
        or context.get("owner")
    )
    post_username = _normalize_instagram_handle(context.get("username"))
    collaborator_handles = {
        _normalize_instagram_handle(handle)
        for handle in context.get("collaborator_handles", [])
        if _normalize_instagram_handle(handle)
    }
    profile_for_match = selected_profile or source_account
    if profile_for_match and owner_username and owner_username != profile_for_match:
        if not collaborator_handles or profile_for_match in collaborator_handles:
            return True
    if source_account and selected_profile and source_account != selected_profile:
        return True
    if source_account and owner_username and owner_username != source_account and source_account in collaborator_handles:
        return True
    if profile_for_match and post_username and post_username != profile_for_match and profile_for_match in collaborator_handles:
        return True
    return bool(profile_for_match and profile_for_match in collaborator_handles and (owner_username or post_username))


def _status_only_fetch_reason(target_metadata: Mapping[str, Any] | None) -> str:
    return (
        "coauthor_comments_endpoint_empty"
        if _target_metadata_indicates_coauthor(target_metadata)
        else "comments_endpoint_status_only"
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


def _top_level_metadata_sources(payload: Any, response: dict[str, Any]) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        sources.append(payload)
    elif isinstance(payload, list):
        sources.extend(item for item in payload if isinstance(item, dict))
    sources.append(response)
    return sources


def _compact_cursor_payload_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool | int | float):
        return value
    text = str(value or "").strip()
    return text or None


def _extract_top_level_cursor_payload(metadata: Mapping[str, Any]) -> dict[str, Any]:
    cursor_payload: dict[str, Any] = {}
    for key in sorted(_TOP_LEVEL_PAGINATION_KEYS):
        if key not in metadata:
            continue
        value = _compact_cursor_payload_value(metadata.get(key))
        if value is not None:
            cursor_payload[key] = value
    return cursor_payload


def _top_level_cursor_shape_names(payload: Any, response: dict[str, Any]) -> tuple[str, ...]:
    names: list[str] = []
    seen: set[str] = set()
    for source in _top_level_metadata_sources(payload, response):
        for raw_key, raw_value in source.items():
            key = str(raw_key or "").strip()
            if not key or raw_value in (None, ""):
                continue
            if key in _TOP_LEVEL_CURSOR_KEYS:
                shape = key
            elif ("cursor" in key.lower() or key.startswith("next_")) and key not in _TOP_LEVEL_PAGINATION_KEYS:
                shape = f"unknown:{key}"
            else:
                continue
            if shape not in seen:
                seen.add(shape)
                names.append(shape)
    return tuple(names)


def _normalize_top_level_cursor_param(value: Any, *, default: str = "min_id") -> str:
    text = str(value or "").strip()
    aliases = {
        "next_min_id": "min_id",
        "nextMinId": "min_id",
        "next_max_id": "max_id",
        "nextMaxId": "max_id",
    }
    normalized = aliases.get(text, text)
    return normalized if normalized in _TOP_LEVEL_CURSOR_PARAM_NAMES else default


def _first_cursor_candidate(
    metadata: Mapping[str, Any],
    keys: Iterable[str],
) -> tuple[str | None, str | None]:
    for key in keys:
        value = str(metadata.get(key) or "").strip()
        if not value:
            continue
        return value, _TOP_LEVEL_CURSOR_PARAM_BY_PAYLOAD_KEY.get(key)
    return None, None


def _top_level_phase_signal(metadata: Mapping[str, Any]) -> str | None:
    has_more_comments = bool(metadata.get("has_more_comments"))
    has_more_headload = bool(metadata.get("has_more_headload_comments"))
    if has_more_comments or str(metadata.get("cached_comments_cursor") or "").strip():
        return "ranked"
    if str(metadata.get("tao_cursor") or "").strip() and not has_more_headload:
        return "ranked"
    if has_more_headload or str(metadata.get("next_min_id") or "").strip():
        return "headload"
    if str(metadata.get("next_max_id") or "").strip():
        return "ranked"
    return None


def _extract_comment_filter_param(payload: Any, response: dict[str, Any]) -> str | None:
    for source in _top_level_metadata_sources(payload, response):
        for key in _COMMENT_FILTER_PARAM_KEYS:
            if key not in source:
                continue
            value = str(source.get(key) or "").strip()
            if value:
                return value
    return None


def _extract_fb_crosspost_comment_rows(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows = payload.get("fb_comments") or []
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _payload_has_more_fb_crosspost_comments(payload: Any, response: dict[str, Any] | None = None) -> bool:
    metadata = _extract_page_metadata(payload, response or {}, keys=_FB_CROSSPOST_PAGINATION_KEYS)
    return bool(metadata.get("has_more_headload_fb_comments"))


def _fb_crosspost_comment_to_instagram_comment(
    row: Mapping[str, Any],
    *,
    shortcode: str,
    post_url: str,
    cursor_payload: Mapping[str, Any],
    comment_filter_param: str | None,
) -> InstagramComment | None:
    raw_id = str(row.get("id") or row.get("pk") or row.get("comment_id") or "").strip()
    if not raw_id:
        return None
    author = row.get("from")
    if not isinstance(author, Mapping):
        author = row.get("user") if isinstance(row.get("user"), Mapping) else {}
    text = str(row.get("text") or row.get("message") or "").strip()
    if not text:
        return None
    created_at = _safe_non_negative_int(row.get("created_at") or row.get("created_time") or row.get("timestamp")) or 0
    date_time = ""
    created_at_iso: str | None = None
    if created_at:
        try:
            created_dt = datetime.fromtimestamp(created_at, tz=UTC)
            date_time = created_dt.strftime("%Y-%m-%d %H:%M:%S")
            created_at_iso = created_dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        except (TypeError, ValueError, OSError, OverflowError):
            date_time = ""
            created_at_iso = None
    normalized_shortcode = str(shortcode or "").strip()
    source_payload = dict(cursor_payload)
    source_payload["raw_fb_comment_id"] = raw_id
    return InstagramComment(
        comment_id=f"fb:{raw_id}",
        text=text,
        username=str(author.get("username") or author.get("name") or "facebook_user").strip() or "facebook_user",
        user_id=str(author.get("id") or author.get("pk") or "").strip(),
        created_at=created_at,
        date_time=date_time,
        likes=_safe_non_negative_int(row.get("like_count") or row.get("likes")) or 0,
        is_reply=False,
        parent_comment_id=None,
        reply_count=0,
        reply_depth=0,
        post_shortcode=normalized_shortcode,
        post_url=post_url,
        comment_url=None,
        created_at_iso=created_at_iso,
        source_snapshot_type="instagram_fb_crosspost_comments",
        phase="fb_crosspost",
        cursor_payload=source_payload,
        comment_filter_param=comment_filter_param,
    )


def _extract_top_level_page_envelope(payload: Any, response: dict[str, Any]) -> _TopLevelPageEnvelope:
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

    primary_cursor: str | None = None
    primary_param: str | None = None
    alt_cursor: str | None = None
    alt_param: str | None = None

    if has_more_comments:
        primary_cursor, primary_param = _first_cursor_candidate(
            metadata,
            ("cached_comments_cursor", "tao_cursor", "next_max_id", "next_min_id", "bifilter_token"),
        )
    elif has_more_headload:
        primary_cursor, primary_param = _first_cursor_candidate(
            metadata,
            ("next_min_id", "next_max_id", "cached_comments_cursor", "tao_cursor", "bifilter_token"),
        )
    else:
        primary_cursor, primary_param = _first_cursor_candidate(
            metadata,
            ("cached_comments_cursor", "tao_cursor", "bifilter_token", "next_min_id", "next_max_id"),
        )

    next_min = str(metadata.get("next_min_id") or "").strip() or None
    next_max = str(metadata.get("next_max_id") or "").strip() or None
    if primary_param == "max_id" and next_min:
        alt_cursor, alt_param = next_min, "min_id"
    elif primary_param == "min_id" and next_max:
        alt_cursor, alt_param = next_max, "max_id"

    has_more = bool(primary_cursor) or has_more_comments or has_more_headload
    return _TopLevelPageEnvelope(
        rows=rows,
        has_more=has_more,
        primary_cursor=primary_cursor,
        primary_cursor_param=primary_param,
        alt_cursor=alt_cursor,
        alt_cursor_param=alt_param,
        cursor_payload=_extract_top_level_cursor_payload(metadata),
        cursor_shape_names=_top_level_cursor_shape_names(payload, response),
        phase_signal=_top_level_phase_signal(metadata),
        comment_filter_param=_extract_comment_filter_param(payload, response),
    )


def _extract_top_level_page(
    payload: Any,
    response: dict[str, Any],
) -> tuple[list[dict[str, Any]], bool, str | None, str | None, str | None, str | None]:
    """Parse a top-level comments page.

    Returns ``(rows, has_more, primary_cursor, primary_cursor_param,
    alt_cursor, alt_cursor_param)``. The primary cursor is the one the loop
    should follow first. The alt cursor is the cross-direction value (when IG
    ships both ``next_min_id`` and ``next_max_id``) so the caller can swap
    directions when the primary loops on a repeated cursor.
    """
    envelope = _extract_top_level_page_envelope(payload, response)
    return (
        envelope.rows,
        envelope.has_more,
        envelope.primary_cursor,
        envelope.primary_cursor_param,
        envelope.alt_cursor,
        envelope.alt_cursor_param,
    )


def _extract_reply_page(
    payload: Any,
    response: dict[str, Any],
) -> tuple[list[dict[str, Any]], str | None, str | None, str | None, str | None]:
    """Parse a reply (child-comments) page.

    Returns ``(rows, primary_cursor, primary_cursor_param, alt_cursor,
    alt_cursor_param)``. The alt cursor is exposed so a stuck reply pagination
    can swap min_id <-> max_id once before declaring repeated_cursor terminal.
    """
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
    next_min_raw = metadata.get("next_min_child_cursor")
    next_max_raw = metadata.get("next_max_child_cursor")
    next_min = str(next_min_raw) if next_min_raw else None
    next_max = str(next_max_raw) if next_max_raw else None

    primary_cursor: str | None = None
    primary_param: str | None = None
    alt_cursor: str | None = None
    alt_param: str | None = None
    if (has_more_tail or has_more_head) and next_min:
        primary_cursor, primary_param = next_min, "min_id"
        if next_max:
            alt_cursor, alt_param = next_max, "max_id"
    elif (has_more_head or has_more_tail) and next_max:
        primary_cursor, primary_param = next_max, "max_id"
        if next_min:
            alt_cursor, alt_param = next_min, "min_id"
    return rows, primary_cursor, primary_param, alt_cursor, alt_param


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


def _relay_child_metadata(graphql_metadata: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not isinstance(graphql_metadata, Mapping):
        return {}
    relay_metadata = graphql_metadata.get("relay_comments")
    if not isinstance(relay_metadata, Mapping):
        return {}
    child_metadata = relay_metadata.get("child_comments")
    return child_metadata if isinstance(child_metadata, Mapping) else {}


def _status_only_child_lane_terminal(graphql_metadata: Mapping[str, Any] | None) -> bool:
    child_metadata = _relay_child_metadata(graphql_metadata)
    if not bool(child_metadata.get("attempted")):
        return False
    reason = str(child_metadata.get("reason") or "").strip()
    return reason in {"completed", "target_reached"}


def _status_only_missing_classification(
    *,
    comments: list[InstagramComment],
    target_count: int | None,
    graphql_metadata: Mapping[str, Any] | None,
    rendered_attempted: bool,
    rendered_terminal: bool = False,
) -> dict[str, Any] | None:
    if target_count is None or target_count <= 0:
        return None
    observed_count = flattened_comment_count(comments)
    residual = max(0, target_count - observed_count)
    if observed_count <= 0 or residual <= 0:
        return None
    if not rendered_attempted or not (rendered_terminal or _status_only_child_lane_terminal(graphql_metadata)):
        return None
    return {
        "classified_missing_comments": residual,
        "missing_reason_counts": {
            _TERMINAL_MISSING_REASON_INSTAGRAM_NOT_SERVED: residual,
        },
        "coverage_formula": {
            "parent_comments": parent_comment_count(comments),
            "child_replies": child_reply_count(comments),
            "facebook_comments": 0,
            "missing_comments": residual,
            "reported_comments": target_count,
        },
        "formula_label": (
            f"{parent_comment_count(comments)} parent comments + "
            f"{child_reply_count(comments)} child replies + 0 Facebook comments + "
            f"{residual} missing comments = {target_count} reported comments"
        ),
    }


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


def _wait_for_global_api_cooldown(*, key: str, deadline: float | None = None) -> bool:
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
    return _deadline_remaining_seconds(deadline) != 0.0


_RATE_LIMIT_ADVISORY_LOCK_NAMESPACE = 0x49_47_43_4D  # "IGCM" — IG comments lane.
_RATE_LIMIT_MODE_ENV = "SOCIAL_INSTAGRAM_COMMENTS_GLOBAL_RATE_LIMIT_MODE"
_RATE_LIMIT_MODE_DEFAULT = "advisory"
_RATE_LIMIT_VALID_MODES = frozenset({"advisory", "file_lock"})


def _resolve_rate_limit_mode(raw_value: str | None = None) -> str:
    """Phase 5.2: choose between Postgres-advisory-lock + file-lock fallback
    (default, cross-container) and pure file-lock (legacy, per-container)."""
    raw = raw_value if raw_value is not None else os.getenv(_RATE_LIMIT_MODE_ENV)
    value = str(raw or _RATE_LIMIT_MODE_DEFAULT).strip().lower()
    return value if value in _RATE_LIMIT_VALID_MODES else _RATE_LIMIT_MODE_DEFAULT


def _advisory_lock_keys_for(key: str) -> tuple[int, int]:
    """Derive a deterministic (namespace, key) pair from the rate-limit key.

    pg_advisory_lock takes two int4 args. The namespace is fixed so this
    lane never collides with other advisory-lock users; the key is a
    sha256-derived 32-bit slice so two containers using the same
    ``_global_rate_limit_key`` serialize.
    """
    digest = hashlib.sha256(str(key or "instagram").encode("utf-8")).digest()
    key_int = int.from_bytes(digest[:4], byteorder="big", signed=True)
    return _RATE_LIMIT_ADVISORY_LOCK_NAMESPACE, key_int


def _try_advisory_lock_pace(*, key: str, delay_seconds: float, deadline: float | None) -> dict[str, Any]:
    """Phase 5.2: try cross-container advisory-lock pacing first.

    Returns ``{"acquired": bool, "paced": bool, "wait_ms": int, "error": str|None}``.
    On any DB-side error, ``acquired`` is False and the caller falls back to
    the per-container file-lock path. Wall-clock waits are still respected
    inside the lock so the rate-limit semantics stay identical.
    """
    delay = max(0.0, float(delay_seconds or 0))
    started_at = time.monotonic()
    if not _wait_for_global_api_cooldown(key=key, deadline=deadline):
        return {
            "acquired": False,
            "paced": False,
            "wait_ms": int((time.monotonic() - started_at) * 1000),
            "error": None,
            "cooldown_blocked": True,
        }
    namespace, lock_key = _advisory_lock_keys_for(key)
    try:
        from trr_backend.db import pg
    except Exception as exc:  # noqa: BLE001
        return {"acquired": False, "paced": True, "wait_ms": 0, "error": f"pg_import_failed:{exc}"}
    try:
        with pg.db_connection(label="instagram-comments-rate-limit-advisory") as conn:
            with pg.db_cursor(conn=conn) as cur:
                cur.execute("select pg_advisory_lock(%s::int, %s::int)", (namespace, lock_key))
                wait_ms = int((time.monotonic() - started_at) * 1000)
                try:
                    if delay > 0:
                        deadline_remaining = _deadline_remaining_seconds(deadline)
                        if deadline_remaining is not None and deadline_remaining <= 0:
                            return {"acquired": True, "paced": False, "wait_ms": wait_ms, "error": None}
                        sleep_for = delay
                        if deadline_remaining is not None:
                            sleep_for = min(sleep_for, deadline_remaining)
                        if sleep_for > 0:
                            time.sleep(sleep_for)
                    return {"acquired": True, "paced": True, "wait_ms": wait_ms, "error": None}
                finally:
                    cur.execute("select pg_advisory_unlock(%s::int, %s::int)", (namespace, lock_key))
    except Exception as exc:  # noqa: BLE001
        return {"acquired": False, "paced": True, "wait_ms": 0, "error": str(exc)}


def _pace_global_api_request(*, key: str, delay_seconds: float, deadline: float | None = None) -> bool:
    delay = max(0.0, float(delay_seconds or 0))
    if not _wait_for_global_api_cooldown(key=key, deadline=deadline):
        return False

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
    "net::err_timed_out",
    "http_response_code_failure",
    "timed out",
    "timeout",
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
    diagnostic_metadata: dict[str, Any] = field(default_factory=dict)
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
        # Phase 5.2: cross-container Postgres-advisory-lock pacing with
        # per-container fcntl fallback. Operators can pin to ``file_lock`` via
        # SOCIAL_INSTAGRAM_COMMENTS_GLOBAL_RATE_LIMIT_MODE for environments
        # without DB connectivity. Counters surface in runtime_metadata so
        # silent slowdowns are visible in job metadata.
        self._global_rate_limit_mode_configured = _resolve_rate_limit_mode()
        self._global_rate_limit_mode_last: str | None = None
        self._global_rate_limit_advisory_attempts = 0
        self._global_rate_limit_advisory_acquires = 0
        self._global_rate_limit_advisory_fallback_count = 0
        self._global_rate_limit_advisory_total_wait_ms = 0
        self._global_rate_limit_advisory_last_error: str | None = None
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
        self._lane_diagnostics: dict[str, dict[str, Any]] = {}
        self._top_level_checkpoints: list[dict[str, Any]] = []
        self._top_level_checkpoint_total_count = 0
        self._top_level_checkpoint_dropped_count = 0
        self._reply_checkpoints: list[dict[str, Any]] = []
        self._reply_checkpoint_total_count = 0
        self._reply_checkpoint_dropped_count = 0
        self._comments_auth_validation: dict[str, Any] = {}
        # Phase A5 follow-up: cursor-direction swap telemetry. Non-zero values
        # indicate IG returned a repeated cursor and we recovered (or tried to)
        # by swapping min_id <-> max_id on the same page payload.
        self._top_level_cursor_direction_swaps = 0
        self._reply_cursor_direction_swaps = 0
        self._top_level_cursor_shape_counts: Counter[str] = Counter()
        self._hidden_comments_render_attempts = 0
        self._hidden_comments_rendered_comments = 0
        self._hidden_comments_merged = 0
        self._status_only_payload_count = 0
        self._status_only_payload_samples: list[dict[str, Any]] = []
        self._coauthor_status_only_fallback_attempts = 0
        self._coauthor_status_only_rendered_comments = 0
        self._coauthor_status_only_merged = 0
        self._last_coauthor_status_only_render_metadata: dict[str, Any] = {}
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
            "global_rate_limit": {
                "mode_configured": self._global_rate_limit_mode_configured,
                "mode_last_used": self._global_rate_limit_mode_last,
                "advisory_attempts": self._global_rate_limit_advisory_attempts,
                "advisory_acquires": self._global_rate_limit_advisory_acquires,
                "advisory_fallback_count": self._global_rate_limit_advisory_fallback_count,
                "advisory_total_wait_ms": self._global_rate_limit_advisory_total_wait_ms,
                "advisory_last_error": self._global_rate_limit_advisory_last_error,
            },
            "rate_limit_cooldown_min_seconds": self._rate_limit_cooldown_min_seconds,
            "rate_limit_cooldown_multiplier": self._rate_limit_cooldown_multiplier,
            "max_transient_retries": self._max_transient_retries,
            "reply_max_transient_retries": self._reply_max_transient_retries,
            "reply_tail_total_budget_seconds": self._reply_tail_total_budget_seconds,
            "transport": "httpx_after_browser_warmup",
            "comments_auth_validation": dict(self._comments_auth_validation),
            "request_count": self._request_count,
            "retry_reason_counts": dict(sorted(self._retry_reason_counts.items())),
            "lane_diagnostics": {lane: dict(metadata) for lane, metadata in sorted(self._lane_diagnostics.items())},
            "hidden_comments": {
                "render_attempts": self._hidden_comments_render_attempts,
                "rendered_comments": self._hidden_comments_rendered_comments,
                "merged_comments": self._hidden_comments_merged,
            },
            "status_only_comments": {
                "payload_count": self._status_only_payload_count,
                "samples": list(self._status_only_payload_samples),
                "coauthor_fallback_attempts": self._coauthor_status_only_fallback_attempts,
                "coauthor_fallback_rendered_comments": self._coauthor_status_only_rendered_comments,
                "coauthor_fallback_merged_comments": self._coauthor_status_only_merged,
                "last_coauthor_rendered_metadata": dict(self._last_coauthor_status_only_render_metadata),
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
            # Phase A5 follow-up: cursor-direction swap counters across the
            # whole shard. Non-zero indicates IG returned repeated cursors and
            # the fetcher recovered (or attempted to) by switching directions.
            "cursor_direction_swaps": {
                "top_level": self._top_level_cursor_direction_swaps,
                "reply": self._reply_cursor_direction_swaps,
            },
            "cursor_shape_counts": {
                "top_level": dict(sorted(self._top_level_cursor_shape_counts.items())),
            },
        }

    async def validate_comments_endpoint(
        self,
        shortcode: str,
        *,
        mode: str = "comments_endpoint",
    ) -> dict[str, Any]:
        """Probe the comments endpoint once after warmup and before target work."""
        normalized_shortcode = str(shortcode or "").strip()
        normalized_mode = str(mode or "comments_endpoint").strip().lower() or "comments_endpoint"
        started_at = time.monotonic()

        def finish(
            status: str,
            *,
            reason: str | None = None,
            retryable: bool = False,
            status_code: int | None = None,
            payload: Any | None = None,
            attempt_count: int | None = None,
            media_id: str | None = None,
        ) -> dict[str, Any]:
            metadata = {
                "mode": normalized_mode,
                "shortcode": normalized_shortcode or None,
                "media_id": media_id,
                "status": status,
                "result": status,
                "reason": reason,
                "retryable": bool(retryable),
                "status_code": status_code,
                "attempt_count": attempt_count,
                "proxy_fingerprint": self._selected_proxy_fingerprint,
                "proxy_session_mode": self._proxy_session_mode,
                "transport": "httpx_after_browser_warmup",
                "duration_ms": int((time.monotonic() - started_at) * 1000),
                "checked_at": datetime.now(UTC).isoformat(),
            }
            if isinstance(payload, dict):
                metadata["response_status"] = payload.get("status")
                metadata["response_message"] = payload.get("message") or payload.get("error_message")
            self._comments_auth_validation = {key: value for key, value in metadata.items() if value is not None}
            return dict(self._comments_auth_validation)

        try:
            media_id = _shortcode_to_media_id(normalized_shortcode)
        except Exception as exc:  # noqa: BLE001
            return finish(
                "transport_blocked",
                reason=f"invalid_shortcode:{exc.__class__.__name__}",
                retryable=True,
            )

        deadline = time.monotonic() + _resolve_positive_float_env(
            "SOCIAL_INSTAGRAM_COMMENTS_ENDPOINT_PROBE_TIMEOUT_SEC",
            _COMMENTS_ENDPOINT_PROBE_TIMEOUT_SECONDS_DEFAULT,
            minimum=1.0,
            maximum=120.0,
        )
        post_url = f"https://www.instagram.com/p/{normalized_shortcode}/"
        try:
            response = await self._fetch_api(
                COMMENTS_URL.format(media_id=media_id),
                referer=post_url,
                params={
                    "can_support_threading": "true",
                    "permalink_enabled": "false",
                    **({"sort_order": self._comment_sort_order} if self._comment_sort_order else {}),
                },
                deadline=deadline,
            )
        except _PaginationDeadlineExceededError:
            return finish(
                "transport_blocked",
                media_id=media_id,
                reason="comments_endpoint_probe_timeout",
                retryable=True,
            )
        except (TimeoutError, httpx.TimeoutException, httpx.TransportError, OSError) as exc:
            return finish(
                "transport_blocked",
                media_id=media_id,
                reason=_transport_failure_reason(exc),
                retryable=True,
            )

        decoded = self._decode_json_response_result(response, attempt=1)
        payload = decoded.get("payload")
        reason = str(decoded.get("reason") or "").strip() or None
        auth_blocked = bool(decoded.get("auth_failed")) or reason in {
            "redirect_to_login",
            "redirect_to_checkpoint",
            "redirect_to_homepage",
            "html_challenge_or_auth_required",
            "checkpoint_required",
            "challenge_required",
            "login_required",
            "auth_required",
            "unauthorized",
        }
        if isinstance(payload, dict):
            payload_blob = json.dumps(payload, sort_keys=True, default=str).lower()
            auth_blocked = auth_blocked or any(
                token in payload_blob
                for token in (
                    "auth_required",
                    "checkpoint_required",
                    "challenge_required",
                    "login_required",
                    "not_logged_in",
                    "accounts/login",
                )
            )
        if not decoded.get("failed") and isinstance(payload, (dict, list)):
            return finish(
                "valid",
                media_id=media_id,
                status_code=_status_code(response),
                payload=payload,
                attempt_count=int(decoded.get("attempt_count") or 1),
            )
        if auth_blocked:
            return finish(
                "auth_blocked",
                media_id=media_id,
                reason=reason or "comments_endpoint_auth_required",
                retryable=False,
                status_code=_status_code(response),
                payload=payload,
                attempt_count=int(decoded.get("attempt_count") or 1),
            )
        return finish(
            "transport_blocked",
            media_id=media_id,
            reason=reason or "comments_endpoint_probe_failed",
            retryable=True,
            status_code=_status_code(response),
            payload=payload,
            attempt_count=int(decoded.get("attempt_count") or 1),
        )

    async def warmup(self) -> None:
        """Navigate to instagram.com via Patchright to establish the session,
        solve challenges, and bridge cookies into the httpx client."""
        warmup_account = str(self._browser_account_id or "").strip().lower().lstrip("@")
        warmup_url = f"https://www.instagram.com/{warmup_account}/" if warmup_account else "https://www.instagram.com/"
        async def continue_with_existing_cookies() -> bool:
            if not _env_truthy("SOCIAL_INSTAGRAM_COMMENTS_WARMUP_COOKIE_ONLY_ON_TRANSPORT", True):
                return False
            if not str(self._raw_cookies.get("sessionid") or "").strip():
                return False
            self._record_retry_reason("warmup_cookie_only_after_transport_error")
            await self._rebuild_http_client()
            return True

        try:
            response = await self._fetch_page(
                warmup_url,
                referer=warmup_url,
            )
        except Exception as exc:  # noqa: BLE001
            if not _warmup_transport_failure(exc):
                raise
            if warmup_account and _env_truthy("SOCIAL_INSTAGRAM_COMMENTS_WARMUP_HOMEPAGE_FALLBACK", True):
                self._record_retry_reason("warmup_homepage_fallback")
                homepage_url = "https://www.instagram.com/"
                try:
                    response = await self._fetch_page(
                        homepage_url,
                        referer=homepage_url,
                    )
                except Exception as homepage_exc:  # noqa: BLE001
                    if not _warmup_transport_failure(homepage_exc):
                        raise
                    if await continue_with_existing_cookies():
                        return
                    self._record_retry_reason("warmup_transport_error")
                    raise InstagramCommentsWarmupError(
                        f"Instagram comments warmup failed on transport/proxy setup: {homepage_exc}",
                        error_code="instagram_comments_warmup_transport_error",
                        retryable=True,
                    ) from homepage_exc
            else:
                if await continue_with_existing_cookies():
                    return
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
        target_metadata: Mapping[str, Any] | None = None,
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
        cursor_param_name = _normalize_top_level_cursor_param(top_level_cursor_param)
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
        reply_lane_attempted_parent_ids: set[str] = set()
        api_top_level_complete = False
        api_top_level_reveal_candidate = False
        hidden_reveal_attempted = False
        status_only_endpoint_detected = False
        status_only_metadata: dict[str, Any] = {}
        post_deadline_reached = False
        current_top_level_phase: str | None = None
        fb_crosspost_pagination_incomplete = False
        cursor_shape_counts: Counter[str] = Counter()
        last_comment_filter_param: str | None = None
        # Phase A5 follow-up: track cursor-direction swaps so we can recover
        # from IG cursor-loops by switching min_id <-> max_id once before
        # falling back to terminal repeated_cursor.
        cursor_directions_attempted: set[str] = set()
        cursor_direction_swaps = 0
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
            self._record_lane_diagnostic(
                "parent",
                shortcode=shortcode,
                reason=str(page_fetch_reason or "page_fetched"),
                count=pages_seen + 1,
                metadata={"cursor_param": cursor_param_name, "has_cursor": bool(cursor)},
            )
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

            page_envelope = _extract_top_level_page_envelope(payload, response)
            comment_rows = page_envelope.rows
            has_more = page_envelope.has_more
            next_cursor = page_envelope.primary_cursor
            next_cursor_param_name = page_envelope.primary_cursor_param
            alt_next_cursor = page_envelope.alt_cursor
            alt_next_cursor_param_name = page_envelope.alt_cursor_param
            for shape_name in page_envelope.cursor_shape_names:
                cursor_shape_counts[shape_name] += 1
                self._top_level_cursor_shape_counts[shape_name] += 1
            if page_envelope.cursor_shape_names:
                self._record_lane_diagnostic(
                    "parent_cursor",
                    shortcode=shortcode,
                    reason="cursor_shapes_seen",
                    count=len(page_envelope.cursor_shape_names),
                    metadata={
                        "cursor_shapes": list(page_envelope.cursor_shape_names),
                        "phase_signal": page_envelope.phase_signal,
                    },
                )
            if page_envelope.phase_signal:
                current_top_level_phase = page_envelope.phase_signal
            page_phase = current_top_level_phase or "ranked"
            if page_envelope.comment_filter_param:
                last_comment_filter_param = page_envelope.comment_filter_param
            comment_cursor_payload = dict(page_envelope.cursor_payload)
            if cursor:
                comment_cursor_payload["request_cursor_param"] = cursor_param_name
                comment_cursor_payload["request_cursor"] = cursor
            if next_cursor and next_cursor_param_name:
                comment_cursor_payload["chosen_cursor_param"] = next_cursor_param_name
                comment_cursor_payload["chosen_cursor"] = next_cursor
            if (
                pages_seen == 1
                and not comments
                and cursor is None
                and _top_level_payload_is_status_only(
                    payload,
                    response,
                    comment_rows=comment_rows,
                    expected_comment_count=expected_comments,
                )
            ):
                status_only_endpoint_detected = True
                api_top_level_complete = True
                fetch_reason = _status_only_fetch_reason(target_metadata)
                fetch_failed = True
                retryable = True
                status_only_metadata = self._status_only_diagnostic_metadata(
                    shortcode=shortcode,
                    media_id=media_id,
                    payload=payload,
                    expected_comment_count=expected_comments,
                    target_metadata=target_metadata,
                    pages_seen=pages_seen,
                    fallback_attempted=False,
                    fallback_rendered_count=0,
                    fallback_merged_count=0,
                    comments_before_fallback=0,
                    comments_after_fallback=0,
                    reason=fetch_reason,
                )
                if fetch_reason == "coauthor_comments_endpoint_empty":
                    graphql_comments, graphql_metadata = await self._fetch_graphql_coauthor_comments_for_status_only(
                        shortcode,
                        post_url,
                        media_id=media_id,
                        expected_comment_count=expected_comments,
                        max_comments=max_comments,
                    )
                    graphql_merged_count = _merge_unique_comments(
                        comments,
                        graphql_comments,
                        max_comments=max_comments,
                    )
                    if graphql_merged_count:
                        logger.info(
                            "Merged %d GraphQL preview coauthor Instagram comment(s) "
                            "for status-only shortcode=%s",
                            graphql_merged_count,
                            shortcode,
                        )
                    target_count = _expected_target_count(expected_comments, max_comments)
                    rendered_comments: list[InstagramComment] = []
                    rendered_merged_count = 0
                    should_try_rendered = (
                        target_count is None or flattened_comment_count(comments) < target_count
                    )
                    if should_try_rendered:
                        rendered_comments = await self._fetch_rendered_coauthor_comments_for_status_only(
                            shortcode,
                            post_url,
                            target_metadata=target_metadata,
                        )
                        rendered_merged_count = _merge_unique_comments(
                            comments,
                            rendered_comments,
                            max_comments=max_comments,
                        )
                        self._coauthor_status_only_merged += rendered_merged_count
                        if rendered_merged_count:
                            logger.info(
                                "Merged %d rendered coauthor Instagram comment(s) for status-only shortcode=%s",
                                rendered_merged_count,
                                shortcode,
                            )
                    status_only_metadata.update(
                        {
                            "fallback_attempted": True,
                            "fallback_type": "coauthor_status_only_fallbacks",
                            "fallback_result_counts": {
                                "graphql_preview_comments": len(graphql_comments),
                                "graphql_merged_comments": graphql_merged_count,
                                "rendered_comments": len(rendered_comments),
                                "rendered_merged_comments": rendered_merged_count,
                                "merged_comments": graphql_merged_count + rendered_merged_count,
                                "comments_before_fallback": 0,
                                "comments_after_fallback": flattened_comment_count(comments),
                            },
                            "graphql_preview": graphql_metadata,
                            "rendered_fallback": dict(self._last_coauthor_status_only_render_metadata),
                        }
                    )
                    if target_count is not None and flattened_comment_count(comments) >= target_count:
                        fetch_failed = False
                        retryable = False
                        fetch_reason = "coauthor_comments_fallback_recovered"
                        status_only_metadata["reason"] = fetch_reason
                    else:
                        missing_classification = _status_only_missing_classification(
                            comments=comments,
                            target_count=target_count,
                            graphql_metadata=graphql_metadata,
                            rendered_attempted=should_try_rendered,
                            rendered_terminal=bool(rendered_comments),
                        )
                        if missing_classification:
                            fetch_failed = False
                            retryable = False
                            fetch_reason = _TERMINAL_MISSING_CLASSIFIED_REASON
                            status_only_metadata["reason"] = fetch_reason
                            status_only_metadata.update(missing_classification)
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
                    diagnostic_metadata=status_only_metadata,
                )
                self._record_status_only_metadata(status_only_metadata)
                logger.warning(
                    "Instagram comments endpoint returned status-only payload for shortcode=%s reason=%s "
                    "expected_comment_count=%s payload_keys=%s",
                    shortcode,
                    fetch_reason,
                    expected_comments,
                    status_only_metadata.get("payload_keys"),
                )
                break
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
                comment = self._parser.parse_comment(
                    comment_data,
                    shortcode,
                    post_url,
                    phase=page_phase,
                    cursor_param=next_cursor_param_name,
                    cursor_min_id=next_cursor,
                    cursor_payload=comment_cursor_payload,
                    comment_filter_param=page_envelope.comment_filter_param,
                )
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
                    reply_lane_attempted_parent_ids.add(str(comment.comment_id or "").strip())
                    self._record_lane_diagnostic(
                        "child",
                        shortcode=shortcode,
                        reason="reported_reply_count_gap",
                        count=observed_replies,
                        metadata={
                            "parent_comment_id": str(comment.comment_id or "").strip() or None,
                            "expected_reply_count": comment.reply_count,
                        },
                    )
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

            if max_comments <= 0 or comments_fetched < max_comments:
                fb_crosspost_rows = _extract_fb_crosspost_comment_rows(payload)
                for fb_row in fb_crosspost_rows:
                    fb_comment = _fb_crosspost_comment_to_instagram_comment(
                        fb_row,
                        shortcode=shortcode,
                        post_url=post_url,
                        cursor_payload=comment_cursor_payload,
                        comment_filter_param=page_envelope.comment_filter_param,
                    )
                    if fb_comment is None:
                        continue
                    fb_comment_id = str(fb_comment.comment_id or "").strip()
                    if fb_comment_id:
                        if fb_comment_id in seen_top_level_comment_ids:
                            continue
                        seen_top_level_comment_ids.add(fb_comment_id)
                    comments.append(fb_comment)
                    comments_fetched += 1
                    if max_comments > 0 and comments_fetched >= max_comments:
                        break
                if fb_crosspost_rows:
                    self._record_lane_diagnostic(
                        "fb_crosspost",
                        shortcode=shortcode,
                        reason="fb_comments_seen",
                        count=len(fb_crosspost_rows),
                    )
                if _payload_has_more_fb_crosspost_comments(payload, response):
                    fb_crosspost_pagination_incomplete = True
                    self._record_lane_diagnostic(
                        "fb_crosspost",
                        shortcode=shortcode,
                        reason="fb_crosspost_pagination_incomplete",
                        count=len(fb_crosspost_rows),
                        metadata={
                            "has_more_headload_fb_comments": True,
                            "fb_comments_seen": len(fb_crosspost_rows),
                        },
                    )
            elif max_comments > 0 and comments_fetched >= max_comments and _payload_has_more_fb_crosspost_comments(
                payload,
                response,
            ):
                fb_crosspost_pagination_incomplete = True
                self._record_lane_diagnostic(
                    "fb_crosspost",
                    shortcode=shortcode,
                    reason="fb_crosspost_pagination_incomplete",
                    count=0,
                    metadata={
                        "has_more_headload_fb_comments": True,
                        "fb_comments_skipped_by_max_comments": True,
                    },
                )

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
            next_cursor_param_name = _normalize_top_level_cursor_param(next_cursor_param_name)
            next_cursor_key = _normalized_cursor_key(next_cursor_param_name, next_cursor)
            current_cursor_key = _normalized_cursor_key(cursor_param_name, cursor)
            # Only record an "attempt" of a direction when we actually issued a
            # paginated request with that cursor — the initial seed page
            # (cursor is None) doesn't exercise either direction.
            if cursor is not None and cursor_param_name in {"min_id", "max_id"}:
                cursor_directions_attempted.add(cursor_param_name)
            if next_cursor_key == current_cursor_key or (next_cursor_key and next_cursor_key in seen_cursors):
                # Phase A5 follow-up: try the cross-direction cursor (min_id <-> max_id)
                # before declaring repeated_cursor terminal. IG sometimes ships both
                # next_min_id and next_max_id in the same response; switching
                # direction often unblocks a stuck cursor without re-fetching pages.
                alt_param = (
                    str(alt_next_cursor_param_name or "").strip()
                    if alt_next_cursor and alt_next_cursor_param_name
                    else None
                )
                if (
                    alt_param in {"min_id", "max_id"}
                    and alt_param not in cursor_directions_attempted
                    and alt_next_cursor
                ):
                    alt_cursor_key = _normalized_cursor_key(alt_param, alt_next_cursor)
                    if alt_cursor_key and alt_cursor_key not in seen_cursors:
                        logger.info(
                            "Instagram comments pagination swapping cursor direction "
                            "from %s to %s on shortcode=%s repeated_cursor=%s",
                            cursor_param_name,
                            alt_param,
                            shortcode,
                            next_cursor,
                        )
                        seen_cursors.add(alt_cursor_key)
                        cursor_direction_swaps += 1
                        cursor_directions_attempted.add(alt_param)
                        cursor = str(alt_next_cursor)
                        cursor_param_name = alt_param
                        self._record_retry_reason("pagination_repeated_cursor_swap_direction")
                        self._top_level_cursor_direction_swaps += 1
                        continue

                has_gap = _has_expected_gap(
                    expected_comment_count=expected_comments,
                    max_comments=max_comments,
                    current_comment_count=flattened_comment_count(comments),
                )
                if expected_comments is None:
                    has_gap = True
                fetch_failed = fetch_failed or has_gap
                fetch_reason = "pagination_repeated_cursor"
                # Phase A5 follow-up: once BOTH cursor directions have actually been
                # attempted (we swapped at least once and still hit a repeat), the
                # IG state is genuinely stuck — retrying the same shard re-loops on
                # the same payload. Mark non-retryable so the job-level Phase 1.4
                # raise stops firing on this stop reason. When only one direction
                # has been tried (alt unavailable), preserve the legacy retryable
                # behavior so the next attempt has a chance to see different cursors.
                both_directions_attempted = (
                    "min_id" in cursor_directions_attempted
                    and "max_id" in cursor_directions_attempted
                )
                if both_directions_attempted:
                    retryable = retryable and not has_gap
                else:
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
                    "Instagram comments pagination repeated cursor for shortcode=%s cursor=%s "
                    "directions_attempted=%s direction_swaps=%d",
                    shortcode,
                    next_cursor,
                    sorted(cursor_directions_attempted),
                    cursor_direction_swaps,
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

        if (
            fetch_replies
            and not status_only_endpoint_detected
            and not auth_failed
            and api_top_level_complete
            and _has_expected_gap(
                expected_comment_count=expected_comments,
                max_comments=max_comments,
                current_comment_count=flattened_comment_count(comments),
            )
        ):
            residual_child_metadata = await self._fetch_residual_child_reply_lanes(
                comments=comments,
                attempted_parent_ids=reply_lane_attempted_parent_ids,
                shortcode=shortcode,
                media_id=media_id,
                post_url=post_url,
                expected_comment_count=expected_comments,
                max_comments=max_comments,
                reply_resume_cursors_by_parent=reply_resume_cursors_by_parent,
                reply_resume_cursor_params_by_parent=reply_resume_cursor_params_by_parent,
                deadline=deadline,
                reply_tail_deadline=reply_tail_deadline,
            )
            reply_checkpoints.extend(
                item
                for item in residual_child_metadata.get("reply_checkpoints", [])
                if isinstance(item, dict)
            )
            fetch_failed = fetch_failed or bool(residual_child_metadata.get("fetch_failed"))
            auth_failed = auth_failed or bool(residual_child_metadata.get("auth_failed"))
            retryable = retryable or bool(residual_child_metadata.get("retryable"))
            if residual_child_metadata.get("fetch_reason") and not fetch_reason:
                fetch_reason = str(residual_child_metadata.get("fetch_reason"))

        if not status_only_endpoint_detected and self._should_reveal_hidden_comments(
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
            elif (
                not auth_failed
                and current_flattened_count < target_count
                and fetch_reason != _TERMINAL_MISSING_CLASSIFIED_REASON
            ):
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

        if fb_crosspost_pagination_incomplete:
            fetch_failed = True
            retryable = True
            if not fetch_reason or fetch_reason in {
                "coverage_target_met",
                "hidden_comments_recovered",
                "hidden_comments_unavailable_reconciled",
            }:
                fetch_reason = "fb_crosspost_pagination_incomplete"
            fb_metadata = {
                "reason": "fb_crosspost_pagination_incomplete",
                "has_more_headload_fb_comments": True,
            }
            if top_level_checkpoint is None:
                top_level_checkpoint = self._record_top_level_checkpoint(
                    shortcode=shortcode,
                    media_id=media_id,
                    stop_reason="fb_crosspost_pagination_incomplete",
                    last_error_code="fb_crosspost_pagination_incomplete",
                    last_top_level_cursor=cursor,
                    next_top_level_cursor=None,
                    last_top_level_cursor_param=cursor_param_name,
                    next_top_level_cursor_param=None,
                    observed_comment_count=flattened_comment_count(comments),
                    expected_comment_count=expected_comments,
                    pages_seen=pages_seen,
                    diagnostic_metadata=fb_metadata,
                )
            status_only_metadata.update(fb_metadata)

        parentless_ids = parentless_reply_ids(comments)
        if parentless_ids:
            fetch_failed = True
            retryable = True
            fetch_reason = _PARENTLESS_REPLY_ATTACH_FAILED_REASON
            parentless_metadata = {
                "reason": fetch_reason,
                "parentless_reply_ids": parentless_ids[:_STATUS_ONLY_METADATA_MAX_ITEMS],
                "parentless_reply_count": len(parentless_ids),
            }
            if status_only_metadata:
                status_only_metadata["parentless_replies"] = parentless_metadata
                status_only_metadata["reason"] = fetch_reason
            else:
                status_only_metadata = parentless_metadata
            if top_level_checkpoint is None:
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
                    diagnostic_metadata=status_only_metadata,
                )

        # Phase 1.7: derive failed_comment_ids from accumulated checkpoints so
        # the job runner can persist per-comment failure attribution into
        # social.scrape_jobs.metadata.comment_failures without each call site
        # having to track failures separately.
        _ensure_child_reply_phase(comments)
        failed_comment_ids = _failed_comment_entries_from_checkpoints(
            shortcode=shortcode,
            reply_checkpoints=reply_checkpoints,
            top_level_checkpoint=top_level_checkpoint,
        )
        result_diagnostic_metadata = dict(status_only_metadata)
        result_diagnostic_metadata["phase_counts"] = _instagram_comment_phase_counts(comments)
        if cursor_shape_counts:
            result_diagnostic_metadata["cursor_shape_counts"] = dict(sorted(cursor_shape_counts.items()))
        if last_comment_filter_param:
            result_diagnostic_metadata["comment_filter_param"] = last_comment_filter_param
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
            diagnostic_metadata=result_diagnostic_metadata,
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
        merged_reply_count = 0
        diagnostic_metadata: dict[str, Any] = {}

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
                before_reply_count = reply_count_observed(comment)
                comment.replies = merge_comment_replies(
                    comment.replies,
                    replies_result.comments,
                    parent_comment_id=comment.comment_id,
                )
                merged_reply_count += max(0, reply_count_observed(comment) - before_reply_count)
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

        unresolved_missing_replies = missing_reply_count(comments)
        if unresolved_missing_replies > 0:
            if not fetch_failed and not auth_failed and merged_reply_count == 0:
                for comment in comments:
                    observed_replies = reply_count_observed(comment)
                    if int(getattr(comment, "reply_count", 0) or 0) > observed_replies:
                        comment.reply_count = observed_replies
                fetch_failed = False
                retryable = False
                fetch_reason = _TERMINAL_MISSING_CLASSIFIED_REASON
                reported_count = expected_comment_count or (flattened_comment_count(comments) + unresolved_missing_replies)
                diagnostic_metadata = {
                    "classified_missing_comments": unresolved_missing_replies,
                    "missing_reason_counts": {
                        _TERMINAL_MISSING_REASON_INSTAGRAM_NOT_SERVED: unresolved_missing_replies,
                    },
                    "coverage_formula": {
                        "parent_comments": parent_comment_count(comments),
                        "child_replies": child_reply_count(comments),
                        "facebook_comments": 0,
                        "missing_comments": unresolved_missing_replies,
                        "reported_comments": reported_count,
                    },
                    "formula_label": (
                        f"{parent_comment_count(comments)} parent comments + "
                        f"{child_reply_count(comments)} child replies + 0 Facebook comments + "
                        f"{unresolved_missing_replies} missing comments = {reported_count} reported comments"
                    ),
                }
            else:
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
            diagnostic_metadata=diagnostic_metadata,
        )

    async def _fetch_residual_child_reply_lanes(
        self,
        *,
        comments: list[InstagramComment],
        attempted_parent_ids: set[str],
        shortcode: str,
        media_id: str,
        post_url: str,
        expected_comment_count: int | None,
        max_comments: int,
        reply_resume_cursors_by_parent: dict[str, str],
        reply_resume_cursor_params_by_parent: dict[str, str],
        deadline: float,
        reply_tail_deadline: float | None,
    ) -> dict[str, Any]:
        target_count = _expected_target_count(expected_comment_count, max_comments)
        metadata: dict[str, Any] = {
            "attempted": False,
            "reason": "no_residual_gap",
            "parent_attempts": 0,
            "merged_replies": 0,
            "reply_checkpoints": [],
            "fetch_failed": False,
            "auth_failed": False,
            "retryable": False,
            "fetch_reason": None,
        }
        if target_count is None or flattened_comment_count(comments) >= target_count:
            return metadata

        metadata["attempted"] = True
        metadata["reason"] = "residual_child_lane_complete"
        for comment in comments:
            if target_count is not None and flattened_comment_count(comments) >= target_count:
                metadata["reason"] = "target_reached"
                break
            parent_id = str(getattr(comment, "comment_id", "") or "").strip()
            if not parent_id or parent_id in attempted_parent_ids:
                continue
            if max_comments > 0 and flattened_comment_count(comments) >= max_comments:
                metadata["reason"] = "max_comments_reached"
                break
            reply_fetch_deadline = deadline
            observed_replies = reply_count_observed(comment)
            if reply_tail_deadline is not None:
                if _deadline_remaining_seconds(reply_tail_deadline) == 0.0:
                    checkpoint = self._record_reply_checkpoint(
                        shortcode=shortcode,
                        media_id=media_id,
                        parent_comment_id=parent_id,
                        stop_reason="reply_tail_budget_exhausted",
                        attempt_count=None,
                        last_error_code="reply_tail_budget_exhausted",
                        last_reply_cursor=None,
                        next_reply_cursor=None,
                        saved_reply_count=observed_replies,
                        expected_reply_count=None,
                        pages_seen=0,
                    )
                    if checkpoint:
                        metadata["reply_checkpoints"].append(checkpoint)
                    metadata["fetch_failed"] = True
                    metadata["retryable"] = True
                    metadata["fetch_reason"] = metadata.get("fetch_reason") or "reply_tail_budget_exhausted"
                    metadata["reason"] = "reply_tail_budget_exhausted"
                    break
                reply_fetch_deadline = min(deadline, reply_tail_deadline)

            attempted_parent_ids.add(parent_id)
            metadata["parent_attempts"] = int(metadata.get("parent_attempts") or 0) + 1
            self._record_lane_diagnostic(
                "child",
                shortcode=shortcode,
                reason="residual_reported_gap_probe",
                count=observed_replies,
                metadata={
                    "parent_comment_id": parent_id,
                    "expected_reply_count": None,
                    "reported_gap": max(0, target_count - flattened_comment_count(comments)),
                },
            )
            replies_result = await self._fetch_comment_replies(
                media_id=media_id,
                comment_id=parent_id,
                shortcode=shortcode,
                post_url=post_url,
                expected_reply_count=None,
                existing_replies=comment.replies,
                resume_cursor=reply_resume_cursors_by_parent.get(parent_id),
                resume_cursor_param=reply_resume_cursor_params_by_parent.get(parent_id),
                deadline=reply_fetch_deadline,
            )
            if (
                reply_tail_deadline is not None
                and replies_result.fetch_reason == "pagination_deadline_exceeded"
                and _deadline_remaining_seconds(reply_tail_deadline) == 0.0
            ):
                replies_result.fetch_reason = "reply_tail_budget_exhausted"
            before_count = reply_count_observed(comment)
            comment.replies = merge_comment_replies(
                comment.replies,
                replies_result.comments,
                parent_comment_id=parent_id,
            )
            after_count = reply_count_observed(comment)
            merged_count = max(0, after_count - before_count)
            if after_count > int(comment.reply_count or 0):
                comment.reply_count = after_count
            metadata["merged_replies"] = int(metadata.get("merged_replies") or 0) + merged_count
            metadata["reply_checkpoints"].extend(replies_result.reply_checkpoints)
            metadata["fetch_failed"] = bool(metadata.get("fetch_failed")) or replies_result.fetch_failed
            metadata["auth_failed"] = bool(metadata.get("auth_failed")) or replies_result.auth_failed
            metadata["retryable"] = bool(metadata.get("retryable")) or replies_result.retryable
            if replies_result.fetch_reason and not metadata.get("fetch_reason"):
                metadata["fetch_reason"] = replies_result.fetch_reason
            if (
                replies_result.fetch_failed
                and replies_result.retryable
                and not replies_result.reply_checkpoints
            ):
                checkpoint = self._record_reply_checkpoint(
                    shortcode=shortcode,
                    media_id=media_id,
                    parent_comment_id=parent_id,
                    stop_reason=replies_result.fetch_reason or "reply_pagination_retryable_stop",
                    attempt_count=None,
                    last_error_code=replies_result.fetch_reason,
                    last_reply_cursor=None,
                    next_reply_cursor=None,
                    saved_reply_count=reply_count_observed(comment),
                    expected_reply_count=None,
                    pages_seen=0,
                )
                if checkpoint:
                    metadata["reply_checkpoints"].append(checkpoint)
        return metadata

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

        self._record_lane_diagnostic("rendered", shortcode=shortcode, reason="hidden_comments_reveal")

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
        self._record_lane_diagnostic(
            "rendered",
            shortcode=shortcode,
            reason="hidden_comments_revealed",
            count=len(comments),
        )
        self._hidden_comments_rendered_comments += len(comments)
        if comments:
            logger.info(
                "Rendered Instagram post yielded %d permalink comment(s) after hidden-comment reveal for shortcode=%s",
                len(comments),
                shortcode,
            )
        return comments

    def _status_only_diagnostic_metadata(
        self,
        *,
        shortcode: str,
        media_id: str,
        payload: Any,
        expected_comment_count: int | None,
        target_metadata: Mapping[str, Any] | None,
        pages_seen: int,
        fallback_attempted: bool,
        fallback_rendered_count: int,
        fallback_merged_count: int,
        comments_before_fallback: int,
        comments_after_fallback: int,
        reason: str,
    ) -> dict[str, Any]:
        payload_status = payload.get("status") if isinstance(payload, dict) else None
        return {
            "reason": reason,
            "shortcode": str(shortcode or "").strip() or None,
            "media_id": str(media_id or "").strip() or None,
            "expected_comment_count": _safe_non_negative_int(expected_comment_count),
            "payload_keys": _payload_keys(payload),
            "payload_shape": _payload_shape(payload),
            "payload_status": payload_status,
            "pages_seen": _safe_non_negative_int(pages_seen),
            "owner_context": _target_metadata_context(target_metadata),
            "is_coauthor_context": _target_metadata_indicates_coauthor(target_metadata),
            "fallback_attempted": bool(fallback_attempted),
            "fallback_type": "rendered_coauthor_comments" if fallback_attempted else None,
            "fallback_result_counts": {
                "rendered_comments": _safe_non_negative_int(fallback_rendered_count) or 0,
                "merged_comments": _safe_non_negative_int(fallback_merged_count) or 0,
                "comments_before_fallback": _safe_non_negative_int(comments_before_fallback) or 0,
                "comments_after_fallback": _safe_non_negative_int(comments_after_fallback) or 0,
            },
        }

    def _record_status_only_metadata(self, metadata: dict[str, Any]) -> None:
        self._status_only_payload_count += 1
        if not metadata:
            return
        sample = dict(metadata)
        while len(self._status_only_payload_samples) >= _STATUS_ONLY_METADATA_MAX_ITEMS:
            self._status_only_payload_samples.pop(0)
        self._status_only_payload_samples.append(sample)

    async def _fetch_graphql_coauthor_comments_for_status_only(
        self,
        shortcode: str,
        post_url: str,
        *,
        media_id: str | None = None,
        expected_comment_count: int | None = None,
        max_comments: int = 0,
    ) -> tuple[list[InstagramComment], dict[str, Any]]:
        """Fetch GraphQL comments for coauthor status-only posts.

        Coauthored posts can return only ``{"status": "ok"}`` from the media
        comments endpoint even when Instagram's own Relay post page exposes a
        visible comments connection. Prefer that paginated Relay connection,
        then fall back to the post-action preview if the public Relay surface is
        unavailable.
        """

        self._record_lane_diagnostic("relay", shortcode=shortcode, reason="coauthor_status_only_relay")
        relay_comments, relay_metadata = await self._fetch_public_relay_coauthor_comments_for_status_only(
            shortcode,
            post_url,
            media_id=media_id,
            expected_comment_count=expected_comment_count,
            max_comments=max_comments,
        )
        if relay_comments:
            relay_fallback_source = (
                "authenticated_relay_comments"
                if relay_metadata.get("auth_mode") == "authenticated"
                else "public_relay_comments"
            )
            self._record_lane_diagnostic(
                "relay",
                shortcode=shortcode,
                reason=str(relay_metadata.get("reason") or "public_relay_comments"),
                count=flattened_comment_count(relay_comments),
            )
            return relay_comments, {
                "fallback_source": relay_fallback_source,
                "relay_comments": relay_metadata,
            }

        normalized_shortcode = str(shortcode or "").strip()
        if not normalized_shortcode:
            return [], {
                "reason": "missing_shortcode",
                "relay_comments": relay_metadata,
            }
        if self._http_client is None:
            await self._rebuild_http_client()
        headers = self._parser.get_headers(post_url)
        headers.update(
            {
                "content-type": "application/x-www-form-urlencoded",
                "x-fb-friendly-name": _POST_ACTION_GRAPHQL_FRIENDLY_NAME,
            }
        )
        base_body = {
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": _POST_ACTION_GRAPHQL_FRIENDLY_NAME,
            "variables": json.dumps({"shortcode": normalized_shortcode}, separators=(",", ":")),
        }
        attempts: list[dict[str, Any]] = []
        for doc_id in _graphql_doc_ids():
            normalized_doc_id = str(doc_id or "").strip()
            if not normalized_doc_id:
                continue
            if not await self._pace_api_requests(deadline=None):
                return [], {"reason": "graphql_preview_deadline_exceeded", "attempts": attempts}
            self._request_count += 1
            try:
                response = await self._http_client.post(  # type: ignore[union-attr]
                    _POST_ACTION_GRAPHQL_URL,
                    data={**base_body, "doc_id": normalized_doc_id},
                    headers=headers,
                )
            except (TimeoutError, httpx.TimeoutException, httpx.TransportError, OSError) as exc:
                reason = _transport_failure_reason(exc)
                self._record_retry_reason(reason)
                attempts.append(
                    {
                        "doc_id": normalized_doc_id,
                        "success": False,
                        "reason": reason,
                        "error_type": exc.__class__.__name__,
                    }
                )
                continue

            self._sync_response_cookies(response)
            status_code = _status_code(response)
            attempt: dict[str, Any] = {
                "doc_id": normalized_doc_id,
                "http_status": status_code,
                "success": False,
            }
            attempts.append(attempt)
            if status_code >= 400 or 300 <= status_code < 400:
                attempt["reason"] = (
                    "redirect_to_login"
                    if "/accounts/login" in _safe_location(response)
                    else "graphql_preview_http_error"
                )
                continue
            try:
                payload = response.json()
            except ValueError:
                attempt["reason"] = "graphql_preview_non_json_response"
                continue
            if not isinstance(payload, Mapping):
                attempt["reason"] = "graphql_preview_unexpected_payload"
                continue
            comments, metadata = _extract_graphql_preview_comments(
                payload,
                shortcode=normalized_shortcode,
                post_url=post_url,
            )
            attempt.update(
                {
                    "success": bool(comments),
                    "reason": None if comments else "graphql_preview_empty",
                    "comment_count": len(comments),
                    "flattened_comment_count": flattened_comment_count(comments),
                    "reported_comment_count": metadata.get("reported_comment_count"),
                    "has_next_page": metadata.get("has_next_page"),
                }
            )
            if comments:
                metadata["doc_id"] = normalized_doc_id
                metadata["attempts"] = attempts
                self._record_lane_diagnostic(
                    "parent",
                    shortcode=shortcode,
                    reason="post_action_preview_comments",
                    count=flattened_comment_count(comments),
                )
                return comments, {
                    "fallback_source": "post_action_preview",
                    "relay_comments": relay_metadata,
                    "post_action_preview": metadata,
                }
        return [], {
            "reason": "graphql_preview_unavailable",
            "relay_comments": relay_metadata,
            "post_action_preview": {"attempts": attempts},
        }

    async def _fetch_public_relay_coauthor_comments_for_status_only(
        self,
        shortcode: str,
        post_url: str,
        *,
        media_id: str | None = None,
        expected_comment_count: int | None = None,
        max_comments: int = 0,
    ) -> tuple[list[InstagramComment], dict[str, Any]]:
        normalized_shortcode = str(shortcode or "").strip()
        if not normalized_shortcode:
            return [], {"reason": "missing_shortcode"}
        normalized_media_id = str(media_id or "").strip()
        if not normalized_media_id:
            try:
                normalized_media_id = str(_shortcode_to_media_id(normalized_shortcode) or "").strip()
            except Exception:  # noqa: BLE001
                normalized_media_id = ""
        if not normalized_media_id:
            return [], {"reason": "missing_media_id"}

        target_count = _expected_target_count(expected_comment_count, max_comments)
        page_size = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_PAGE_SIZE",
            _POST_COMMENTS_GRAPHQL_PAGE_SIZE,
            minimum=1,
            maximum=50,
        )
        max_pages = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_MAX_PAGES",
            _POST_COMMENTS_GRAPHQL_MAX_PAGES,
            minimum=1,
            maximum=100,
        )
        timeout = httpx.Timeout(self._timeout_ms / 1000)
        metadata: dict[str, Any] = {
            "media_id": normalized_media_id,
            "target_count": target_count,
            "page_size": page_size,
            "max_pages": max_pages,
            "attempts": [],
            "pages": [],
            "mode_attempts": [],
        }

        async def run_relay_attempt(
            *,
            client: httpx.AsyncClient,
            auth_mode: str,
            page_headers: Mapping[str, str],
            viewer_id: str,
            relay_is_logged_in: bool,
        ) -> tuple[list[InstagramComment], dict[str, Any]]:
            attempt_metadata: dict[str, Any] = {
                "media_id": normalized_media_id,
                "target_count": target_count,
                "page_size": page_size,
                "max_pages": max_pages,
                "auth_mode": auth_mode,
                "relay_is_logged_in": relay_is_logged_in,
                "attempts": [],
                "pages": [],
            }
            if not await self._pace_api_requests(deadline=None):
                return [], {"reason": "graphql_relay_deadline_exceeded", **attempt_metadata}
            self._request_count += 1
            try:
                page_response = await client.get(post_url, headers=dict(page_headers))
            except (TimeoutError, httpx.TimeoutException, httpx.TransportError, OSError) as exc:
                reason = _transport_failure_reason(exc)
                self._record_retry_reason(reason)
                return [], {"reason": reason, **attempt_metadata}
            self._sync_response_cookies(page_response)

            status_code = _status_code(page_response)
            attempt_metadata["post_page_status"] = status_code
            if status_code >= 400 or 300 <= status_code < 400:
                attempt_metadata["post_page_location"] = _safe_location(page_response)
                return [], {"reason": "graphql_relay_post_page_unavailable", **attempt_metadata}

            context = _extract_logged_out_graphql_context(
                _response_text(page_response),
                media_id=normalized_media_id,
            )
            attempt_metadata["context"] = {
                "has_lsd": bool(context.get("lsd")),
                "has_jazoest": bool(context.get("jazoest")),
                "container_query_id": context.get("container_query_id"),
            }
            lsd = str(context.get("lsd") or self._raw_cookies.get("lsd") or "").strip()
            if not lsd:
                return [], {"reason": "graphql_relay_missing_lsd", **attempt_metadata}
            jazoest = str(context.get("jazoest") or "").strip()
            if not jazoest and hasattr(self._parser, "_jazoest_for_token"):
                try:
                    jazoest = str(self._parser._jazoest_for_token(lsd) or "").strip()  # noqa: SLF001
                except Exception:  # noqa: BLE001
                    jazoest = ""
            spin = context.get("spin") if isinstance(context.get("spin"), Mapping) else {}
            if relay_is_logged_in:
                graphql_headers = self._parser.get_headers(post_url)
                graphql_headers.update(
                    {
                        "accept": "*/*",
                        "content-type": "application/x-www-form-urlencoded",
                        "x-asbd-id": "359341",
                        "x-fb-friendly-name": _POST_COMMENTS_GRAPHQL_HEADER_FRIENDLY_NAME,
                        "x-fb-lsd": lsd,
                        "x-ig-app-id": "936619743392459",
                        "x-requested-with": "XMLHttpRequest",
                    }
                )
            else:
                graphql_headers = {
                    "accept": "*/*",
                    "content-type": "application/x-www-form-urlencoded",
                    "referer": post_url,
                    "user-agent": dict(page_headers).get("user-agent", "Mozilla/5.0"),
                    "x-asbd-id": "359341",
                    "x-fb-friendly-name": _POST_COMMENTS_GRAPHQL_HEADER_FRIENDLY_NAME,
                    "x-fb-lsd": lsd,
                    "x-ig-app-id": "936619743392459",
                    "x-requested-with": "XMLHttpRequest",
                }
            common_body: dict[str, Any] = {
                "av": viewer_id,
                "__a": "1",
                "__comet_req": "7",
                "__d": "www",
                "__req": "1",
                "__user": viewer_id,
                "fb_api_caller_class": "RelayModern",
                "fb_api_req_friendly_name": _POST_COMMENTS_GRAPHQL_FRIENDLY_NAME,
                "jazoest": jazoest,
                "lsd": lsd,
                "server_timestamps": "true",
            }
            for key in ("__spin_r", "__spin_b", "__spin_t"):
                if spin.get(key):
                    common_body[key] = spin[key]

            for doc_id in _post_comments_graphql_doc_ids(str(context.get("container_query_id") or "")):
                normalized_doc_id = str(doc_id or "").strip()
                if not normalized_doc_id:
                    continue
                comments: list[InstagramComment] = []
                cursor: str | None = None
                seen_cursors: set[str] = set()
                attempt: dict[str, Any] = {
                    "doc_id": normalized_doc_id,
                    "auth_mode": auth_mode,
                    "success": False,
                    "pages": 0,
                    "comments": 0,
                }
                attempt_metadata["attempts"].append(attempt)

                async def hydrate_child_comments(
                    terminal_reason: str,
                    *,
                    attempt_ref: dict[str, Any] = attempt,
                    comments_ref: list[InstagramComment] = comments,
                ) -> str:
                    child_metadata = await self._fetch_public_relay_child_comments_for_status_only(
                        public_client=client,
                        shortcode=normalized_shortcode,
                        post_url=post_url,
                        media_id=normalized_media_id,
                        comments=comments_ref,
                        graphql_headers=graphql_headers,
                        common_body=common_body,
                        relay_is_logged_in=relay_is_logged_in,
                        target_count=target_count,
                        max_comments=max_comments,
                    )
                    attempt_metadata["child_comments"] = child_metadata
                    attempt_ref["comments"] = flattened_comment_count(comments_ref)
                    attempt_ref["child_replies_merged"] = child_metadata.get("merged_replies")
                    if target_count is not None and flattened_comment_count(comments_ref) >= target_count:
                        attempt_ref["success"] = True
                        return "child_comments_target_reached"
                    if child_metadata.get("merged_replies"):
                        return "child_comments_partial" if target_count is not None else "pagination_complete"
                    return terminal_reason

                for page_index in range(1, max_pages + 1):
                    if not await self._pace_api_requests(deadline=None):
                        attempt["reason"] = "graphql_relay_deadline_exceeded"
                        return comments, {"reason": "graphql_relay_deadline_exceeded", **attempt_metadata}
                    variables: dict[str, Any] = {
                        "__relay_internal__pv__PolarisIsLoggedInrelayprovider": relay_is_logged_in,
                        "first": page_size,
                        "media_id": normalized_media_id,
                        "sort_order": self._comment_sort_order or "popular",
                    }
                    if cursor:
                        variables["after"] = cursor
                    self._request_count += 1
                    try:
                        response = await client.post(
                            _POST_ACTION_GRAPHQL_URL,
                            data={
                                **common_body,
                                "doc_id": normalized_doc_id,
                                "variables": json.dumps(variables, separators=(",", ":")),
                            },
                            headers=graphql_headers,
                        )
                    except (TimeoutError, httpx.TimeoutException, httpx.TransportError, OSError) as exc:
                        reason = _transport_failure_reason(exc)
                        self._record_retry_reason(reason)
                        attempt["reason"] = reason
                        break

                    status_code = _status_code(response)
                    if status_code >= 400 or 300 <= status_code < 400:
                        attempt["http_status"] = status_code
                        attempt["location"] = _safe_location(response)
                        attempt["reason"] = "graphql_relay_http_error"
                        break
                    payload = _decode_graphql_json_payload(response)
                    if payload is None:
                        attempt["reason"] = "graphql_relay_non_json_response"
                        break
                    if payload.get("error") or payload.get("errors"):
                        attempt["reason"] = "graphql_relay_error_payload"
                        attempt["error"] = payload.get("error") or payload.get("errors")
                        break

                    page_comments, page_metadata = _extract_graphql_connection_comments(
                        payload,
                        shortcode=normalized_shortcode,
                        post_url=post_url,
                    )
                    appended = _merge_unique_comments(
                        comments,
                        page_comments,
                        max_comments=max_comments,
                    )
                    next_cursor = str(page_metadata.get("end_cursor") or "").strip() or None
                    page_summary = {
                        "doc_id": normalized_doc_id,
                        "auth_mode": auth_mode,
                        "page": page_index,
                        "comments": len(page_comments),
                        "merged_comments": appended,
                        "has_next_page": page_metadata.get("has_next_page"),
                        "has_end_cursor": bool(next_cursor),
                    }
                    attempt_metadata["pages"].append(page_summary)
                    attempt["pages"] = page_index
                    attempt["comments"] = flattened_comment_count(comments)

                    if target_count is not None and flattened_comment_count(comments) >= target_count:
                        terminal_reason = await hydrate_child_comments("graphql_relay_target_reached")
                        attempt["success"] = True
                        attempt["reason"] = terminal_reason
                        return comments, {"reason": attempt["reason"], **attempt_metadata}
                    if not page_metadata.get("has_next_page") or not next_cursor:
                        terminal_reason = "pagination_complete" if comments else "graphql_relay_empty"
                        terminal_reason = await hydrate_child_comments(terminal_reason)
                        attempt["success"] = bool(comments)
                        attempt["reason"] = terminal_reason
                        return comments, {"reason": attempt["reason"], **attempt_metadata}
                    if next_cursor in seen_cursors or appended <= 0:
                        terminal_reason = "pagination_stalled"
                        terminal_reason = await hydrate_child_comments(terminal_reason)
                        attempt["success"] = bool(comments)
                        attempt["reason"] = terminal_reason
                        return comments, {"reason": attempt["reason"], **attempt_metadata}
                    seen_cursors.add(next_cursor)
                    cursor = next_cursor

                if comments:
                    terminal_reason = await hydrate_child_comments(attempt.get("reason") or "page_limit_reached")
                    attempt["success"] = True
                    attempt["reason"] = terminal_reason
                    return comments, {"reason": attempt["reason"], **attempt_metadata}

            return [], {"reason": "graphql_relay_unavailable", **attempt_metadata}

        def mode_attempt_summaries() -> list[dict[str, Any]]:
            return [
                {key: value for key, value in attempt.items() if key != "mode_attempts"}
                for attempt in mode_attempts
            ]

        mode_attempts: list[dict[str, Any]] = []
        sessionid = str(self._raw_cookies.get("sessionid") or "").strip()
        viewer_id = str(self._raw_cookies.get("ds_user_id") or "").strip()
        if sessionid and viewer_id:
            if self._http_client is None:
                await self._rebuild_http_client()
            if self._http_client is not None:
                all_headers = self._parser.get_headers(post_url)
                authenticated_page_headers = {
                    key: value for key, value in all_headers.items() if key.lower() not in _API_HEADER_KEYS_TO_STRIP
                }
                authenticated_page_headers.update(
                    {
                        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "referer": post_url,
                        "sec-fetch-dest": "document",
                        "sec-fetch-mode": "navigate",
                        "sec-fetch-site": "same-origin",
                    }
                )
                auth_comments, auth_metadata = await run_relay_attempt(
                    client=self._http_client,
                    auth_mode="authenticated",
                    page_headers=authenticated_page_headers,
                    viewer_id=viewer_id,
                    relay_is_logged_in=True,
                )
                mode_attempts.append(auth_metadata)
                if auth_comments:
                    auth_metadata["mode_attempts"] = mode_attempt_summaries()
                    auth_metadata["fallback_source"] = "authenticated_relay_comments"
                    return auth_comments, auth_metadata
        else:
            mode_attempts.append(
                {
                    "auth_mode": "authenticated",
                    "reason": "authenticated_relay_missing_cookies",
                    "has_sessionid": bool(sessionid),
                    "has_ds_user_id": bool(viewer_id),
                }
            )

        public_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            # Instagram's logged-out Relay SSR accepts a simple UA here; a full
            # desktop UA currently trips an anti-abuse `for (;;);` error for
            # the comment pagination persisted query.
            "user-agent": "Mozilla/5.0",
        }
        async with httpx.AsyncClient(
            timeout=timeout,
            proxy=self._api_proxy_url,
            follow_redirects=False,
            trust_env=False,
        ) as public_client:
            public_comments, public_metadata = await run_relay_attempt(
                client=public_client,
                auth_mode="public",
                page_headers=public_headers,
                viewer_id="0",
                relay_is_logged_in=False,
            )
            mode_attempts.append(public_metadata)
            public_metadata["mode_attempts"] = mode_attempt_summaries()
            if public_comments:
                public_metadata["fallback_source"] = "public_relay_comments"
                return public_comments, public_metadata

        last_metadata = mode_attempts[-1] if mode_attempts else metadata
        return [], {
            **metadata,
            **last_metadata,
            "reason": str(last_metadata.get("reason") or "graphql_relay_unavailable"),
            "mode_attempts": mode_attempt_summaries(),
        }

    async def _fetch_public_relay_child_comments_for_status_only(
        self,
        *,
        public_client: httpx.AsyncClient,
        shortcode: str,
        post_url: str,
        media_id: str,
        comments: list[InstagramComment],
        graphql_headers: Mapping[str, str],
        common_body: Mapping[str, Any],
        target_count: int | None,
        max_comments: int,
        relay_is_logged_in: bool = False,
    ) -> dict[str, Any]:
        if not comments:
            return {"attempted": False, "reason": "no_parent_comments"}

        page_size = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_CHILD_PAGE_SIZE",
            _POST_CHILD_COMMENTS_GRAPHQL_PAGE_SIZE,
            minimum=1,
            maximum=50,
        )
        max_pages = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_CHILD_MAX_PAGES",
            _POST_CHILD_COMMENTS_GRAPHQL_MAX_PAGES,
            minimum=1,
            maximum=100,
        )
        parent_limit = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_CHILD_PARENT_LIMIT",
            len(comments),
            minimum=1,
            maximum=500,
        )
        metadata: dict[str, Any] = {
            "attempted": True,
            "page_size": page_size,
            "max_pages": max_pages,
            "parent_limit": parent_limit,
            "parent_attempts": 0,
            "parent_attempt_ids": [],
            "parents_with_replies": 0,
            "fetched_replies": 0,
            "merged_replies": 0,
            "pages": [],
        }
        attempts = _post_child_comments_graphql_doc_attempts()
        if not attempts:
            return {**metadata, "reason": "missing_child_graphql_doc_ids"}

        for parent in comments[:parent_limit]:
            if max_comments > 0 and flattened_comment_count(comments) >= max_comments:
                metadata["reason"] = "max_comments_reached"
                break

            parent_comment_id = str(parent.comment_id or "").strip()
            if not parent_comment_id:
                continue
            metadata["parent_attempts"] += 1
            if len(metadata["parent_attempt_ids"]) < _STATUS_ONLY_METADATA_MAX_ITEMS:
                metadata["parent_attempt_ids"].append(parent_comment_id)
            self._record_lane_diagnostic(
                "child",
                shortcode=shortcode,
                reason="coauthor_status_only_child_relay",
                count=reply_count_observed(parent),
                metadata={
                    "parent_comment_id": parent_comment_id,
                    "reported_reply_count": getattr(parent, "reply_count", None),
                },
            )
            for friendly_name, doc_id in attempts:
                normalized_doc_id = str(doc_id or "").strip()
                normalized_friendly_name = str(friendly_name or _POST_CHILD_COMMENTS_GRAPHQL_FRIENDLY_NAME).strip()
                if not normalized_doc_id or not normalized_friendly_name:
                    continue

                child_comments: list[InstagramComment] = []
                cursor: str | None = None
                seen_cursors: set[str] = set()
                stop_reason: str | None = None

                for page_index in range(1, max_pages + 1):
                    if not await self._pace_api_requests(deadline=None):
                        metadata["reason"] = "graphql_child_relay_deadline_exceeded"
                        return metadata
                    variables: dict[str, Any] = {
                        "__relay_internal__pv__PolarisIsLoggedInrelayprovider": relay_is_logged_in,
                        "first": page_size,
                        "is_chronological": False,
                        "media_id": media_id,
                        "parent_comment_id": parent_comment_id,
                    }
                    if cursor:
                        variables["after"] = cursor

                    self._request_count += 1
                    try:
                        response = await public_client.post(
                            _POST_ACTION_GRAPHQL_URL,
                            data={
                                **common_body,
                                "fb_api_req_friendly_name": normalized_friendly_name,
                                "doc_id": normalized_doc_id,
                                "variables": json.dumps(variables, separators=(",", ":")),
                            },
                            headers={
                                **dict(graphql_headers),
                                "x-fb-friendly-name": normalized_friendly_name,
                            },
                        )
                    except (TimeoutError, httpx.TimeoutException, httpx.TransportError, OSError) as exc:
                        reason = _transport_failure_reason(exc)
                        self._record_retry_reason(reason)
                        stop_reason = reason
                        break

                    status_code = _status_code(response)
                    if status_code >= 400 or 300 <= status_code < 400:
                        stop_reason = "graphql_child_relay_http_error"
                        break
                    payload = _decode_graphql_json_payload(response)
                    if payload is None:
                        stop_reason = "graphql_child_relay_non_json_response"
                        break
                    if payload.get("error") or payload.get("errors"):
                        stop_reason = "graphql_child_relay_error_payload"
                        break

                    page_comments, page_metadata = _extract_graphql_child_connection_comments(
                        payload,
                        shortcode=shortcode,
                        post_url=post_url,
                        parent_comment_id=parent_comment_id,
                    )
                    _merge_unique_comments(child_comments, page_comments, max_comments=0)
                    next_cursor = str(page_metadata.get("end_cursor") or "").strip() or None
                    if len(metadata["pages"]) < _STATUS_ONLY_METADATA_MAX_ITEMS:
                        metadata["pages"].append(
                            {
                                "doc_id": normalized_doc_id,
                                "friendly_name": normalized_friendly_name,
                                "parent_comment_id": parent_comment_id,
                                "page": page_index,
                                "replies": len(page_comments),
                                "has_next_page": page_metadata.get("has_next_page"),
                                "has_end_cursor": bool(next_cursor),
                            }
                        )
                    if not page_metadata.get("has_next_page") or not next_cursor:
                        stop_reason = "pagination_complete"
                        break
                    if next_cursor in seen_cursors:
                        stop_reason = "pagination_stalled"
                        break
                    seen_cursors.add(next_cursor)
                    cursor = next_cursor

                if child_comments:
                    if max_comments > 0:
                        remaining = max(0, max_comments - flattened_comment_count(comments))
                        child_comments = child_comments[:remaining]
                    before_count = reply_count_observed(parent)
                    parent.replies = merge_comment_replies(
                        parent.replies,
                        child_comments,
                        parent_comment_id=parent_comment_id,
                    )
                    after_count = reply_count_observed(parent)
                    merged_count = max(0, after_count - before_count)
                    if after_count > int(parent.reply_count or 0):
                        parent.reply_count = after_count
                    metadata["fetched_replies"] += len(child_comments)
                    metadata["merged_replies"] += merged_count
                    metadata["parents_with_replies"] += 1
                    break

                if stop_reason == "pagination_complete":
                    break

        metadata.setdefault("reason", "completed")
        metadata["flattened_count_after_children"] = flattened_comment_count(comments)
        return metadata

    async def _fetch_rendered_coauthor_comments_for_status_only(
        self,
        shortcode: str,
        post_url: str,
        *,
        target_metadata: Mapping[str, Any] | None = None,
    ) -> list[InstagramComment]:
        click_limit = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_STATUS_ONLY_CLICK_LIMIT",
            _COAUTHOR_STATUS_ONLY_CLICK_LIMIT_DEFAULT,
            minimum=0,
            maximum=20,
        )
        scroll_limit = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_STATUS_ONLY_SCROLL_LIMIT",
            _COAUTHOR_STATUS_ONLY_SCROLL_LIMIT_DEFAULT,
            minimum=0,
            maximum=20,
        )
        self._record_lane_diagnostic("rendered", shortcode=shortcode, reason="coauthor_status_only_render")
        self._last_coauthor_status_only_render_metadata = {
            "version": _COAUTHOR_RENDERED_FALLBACK_VERSION,
            "shortcode": str(shortcode or "").strip() or None,
        }
        context = _target_metadata_context(target_metadata)
        ignored_usernames = [
            self._browser_account_id or "",
            context.get("profile_account") or "",
            context.get("selected_profile_account") or "",
            context.get("source_account") or "",
            context.get("account_handle") or "",
            context.get("caption_author") or "",
            context.get("caption_writer") or "",
            context.get("original_author") or "",
            context.get("owner_username") or "",
            context.get("owner") or "",
            context.get("username") or "",
            *(context.get("collaborator_handles") or []),
        ]

        async def load_visible_comments(page: Any) -> None:
            await page.evaluate(
                """
                async ({ maxClicks, maxScrolls, ignoredUsernames, snapshotElementId, version }) => {
                  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
                  const clean = (value) => String(value || "").replace(/\\s+/g, " ").trim();
                  const bodyText = clean(document.body?.innerText || document.body?.textContent || "");
                  const diagnostics = {
                    version,
                    url: String(window.location.href || ""),
                    title: String(document.title || ""),
                    readyState: String(document.readyState || ""),
                    userAgent: String(navigator.userAgent || ""),
                    bodyTextLength: bodyText.length,
                    initialProfileAnchors: 0,
                    initialCommentPermalinks: document.querySelectorAll('a[href*="/c/"]').length,
                    initialReplyTextMatches: (bodyText.match(/\\bReply\\b/g) || []).length,
                    candidateProfileAnchors: 0,
                    reservedProfileAnchors: 0,
                    rowTextMismatchAnchors: 0,
                    longRowAnchors: 0,
                    emptyRowAnchors: 0,
                    clickedControls: 0,
                    scrollSteps: 0,
                    rowsCollected: 0,
                    rowTextSamples: [],
                  };
                  let rows = [];
                  try {
                    const reserved = new Set([
                      "accounts",
                      "explore",
                      "p",
                      "reel",
                      "reels",
                      "stories",
                      ...ignoredUsernames.map((value) => String(value || "").trim().replace(/^@/, "").toLowerCase()),
                    ].filter(Boolean));
                    const profileHrefMatch = (href) =>
                      String(href || "").match(/^(?:https?:\\/\\/(?:www\\.)?instagram\\.com)?\\/([^/?#]+)\\/?$/);
                    const textFor = (element) => {
                      if (!element) return "";
                      return [
                        element.innerText || "",
                        element.textContent || "",
                        element.getAttribute?.("aria-label") || "",
                        element.querySelector?.("title")?.textContent || "",
                      ].join(" ").replace(/\\s+/g, " ").trim();
                    };
                    diagnostics.initialProfileAnchors = Array.from(document.querySelectorAll("a[href]"))
                      .filter((anchor) => Boolean(profileHrefMatch(anchor.getAttribute("href") || ""))).length;
                    const collectRows = () => {
                      const collected = [];
                      const seen = new Set();
                      for (const anchor of Array.from(document.querySelectorAll("a[href]"))) {
                        const href = anchor.getAttribute("href") || "";
                        const match = profileHrefMatch(href);
                        if (!match) continue;
                        const username = match[1].toLowerCase();
                        diagnostics.candidateProfileAnchors += 1;
                        if (reserved.has(username)) {
                          diagnostics.reservedProfileAnchors += 1;
                          continue;
                        }
                        let row = anchor;
                        for (let depth = 0; depth < 10 && row.parentElement; depth += 1) {
                          row = row.parentElement;
                          const candidateText = clean(row.innerText || row.textContent || "");
                          if (candidateText.includes("Reply") && candidateText.length < 1200) break;
                        }
                        const rowText = clean(row.innerText || row.textContent || "");
                        if (!rowText) {
                          diagnostics.emptyRowAnchors += 1;
                          continue;
                        }
                        if (rowText.length >= 1200) {
                          diagnostics.longRowAnchors += 1;
                          continue;
                        }
                        const rowTextLower = rowText.toLowerCase();
                        const anchorText = clean(
                          anchor.innerText || anchor.textContent || anchor.getAttribute("aria-label") || ""
                        );
                        const anchorTextLower = anchorText.toLowerCase();
                        const profileImage = Array.from(row.querySelectorAll("img"))
                          .find((image) => clean(image.getAttribute("alt") || "").toLowerCase().includes(username));
                        const usernameVisible =
                          anchorTextLower === username ||
                          anchorTextLower.includes(`@${username}`) ||
                          rowTextLower.startsWith(`${username} `) ||
                          rowTextLower.startsWith(`${username}\\n`) ||
                          Boolean(profileImage);
                        if (!usernameVisible) {
                          diagnostics.rowTextMismatchAnchors += 1;
                          continue;
                        }
                        const key = `${username}:${rowText}`;
                        if (seen.has(key)) continue;
                        seen.add(key);
                        const rowRect = row.getBoundingClientRect();
                        const anchorRect = anchor.getBoundingClientRect();
                        const commentLink = row.querySelector?.('a[href*="/c/"]');
                        const commentHref = commentLink?.getAttribute?.("href") || "";
                        const commentMatch = commentHref.match(/\\/c\\/(\\d+)/);
                        const sample = rowText.slice(0, 180);
                        if (diagnostics.rowTextSamples.length < 3) diagnostics.rowTextSamples.push(sample);
                        collected.push({
                          username,
                          rowText: rowText.slice(0, 1200),
                          href,
                          commentId: commentMatch?.[1] || null,
                          commentHref: commentHref || null,
                          left: Math.round(rowRect.left || 0),
                          top: Math.round(rowRect.top || 0),
                          anchorLeft: Math.round(anchorRect.left || 0),
                          profilePicUrl: profileImage?.getAttribute?.("src") || null,
                        });
                      }
                      return collected;
                    };
                    const getScrollTarget = () => {
                      const candidates = Array.from(document.querySelectorAll("*"))
                        .filter((element) => {
                          if (element.scrollHeight <= element.clientHeight + 40 || element.clientHeight <= 80) {
                            return false;
                          }
                          const elementText = clean(element.innerText || element.textContent || "");
                          return /Add a comment|Reply|likes/i.test(elementText);
                        })
                        .sort((left, right) => {
                          const leftGap = left.scrollHeight - left.clientHeight;
                          const rightGap = right.scrollHeight - right.clientHeight;
                          return rightGap - leftGap;
                        });
                      return candidates[0] || document.scrollingElement || document.documentElement;
                    };
                    const commentControl = (element) => {
                      const text = textFor(element).toLowerCase();
                      if (!text || text.length > 140) return false;
                      return (
                        (text.includes("view all") && text.includes("repl")) ||
                        text.includes("view all comments") ||
                        text.includes("view more comments") ||
                        text.includes("load more comments") ||
                        text.includes("view hidden comments") ||
                        text.includes("view replies") ||
                        text.includes("more comments")
                      );
                    };
                    await sleep(1200);
                    const steps = Math.max(maxClicks, maxScrolls);
                    for (let index = 0; index < steps; index += 1) {
                      let clicked = 0;
                      const candidates = Array.from(
                        document.querySelectorAll('[role="button"], button, a, [tabindex="0"], span, div')
                      ).filter(commentControl);
                      for (const control of candidates.slice(0, 8)) {
                        if (clicked >= maxClicks) break;
                        const rect = control.getBoundingClientRect();
                        if (rect.bottom < -50 || rect.top > (window.innerHeight || 900) + 50) continue;
                        const clickable = control.closest?.('[role="button"], button, a, [tabindex="0"]') || control;
                        clickable.click?.();
                        clickable.dispatchEvent(
                          new MouseEvent("click", { bubbles: true, cancelable: true, view: window })
                        );
                        clicked += 1;
                        diagnostics.clickedControls += 1;
                        await sleep(350);
                      }
                      const scrollTarget = getScrollTarget();
                      const scrollBy = Math.max(
                        450,
                        Math.round((scrollTarget.clientHeight || window.innerHeight || 700) * 0.8)
                      );
                      scrollTarget.scrollTop = (scrollTarget.scrollTop || 0) + scrollBy;
                      window.scrollBy(0, Math.min(600, scrollBy));
                      diagnostics.scrollSteps += 1;
                      await sleep(700);
                    }
                    rows = collectRows();
                    diagnostics.rowsCollected = rows.length;
                  } catch (error) {
                    diagnostics.error = String(error?.message || error);
                    diagnostics.errorStack = String(error?.stack || "").slice(0, 600);
                  }
                  const existingSnapshot = document.getElementById(snapshotElementId);
                  if (existingSnapshot) existingSnapshot.remove();
                  const snapshot = document.createElement("script");
                  snapshot.id = snapshotElementId;
                  snapshot.type = "application/json";
                  snapshot.textContent = JSON.stringify({ rows, diagnostics });
                  document.documentElement.appendChild(snapshot);
                }
                """,
                {
                    "maxClicks": click_limit,
                    "maxScrolls": scroll_limit,
                    "ignoredUsernames": ignored_usernames,
                    "snapshotElementId": _RENDERED_COMMENTS_JSON_ID,
                    "version": _COAUTHOR_RENDERED_FALLBACK_VERSION,
                },
            )

        self._coauthor_status_only_fallback_attempts += 1
        attempt_plan: list[tuple[str, Any]] = (
            [("configured_proxy", self._proxy_rotator), ("direct", None)]
            if self._proxy_rotator is not None
            else [("direct", None)]
        )
        attempt_metadata: list[dict[str, Any]] = []
        comments: list[InstagramComment] = []
        for attempt_label, attempt_proxy_rotator in attempt_plan:
            self._request_count += 1
            try:
                response = await self._fetcher.async_fetch(
                    post_url,
                    headless=self._headless,
                    network_idle=True,
                    load_dom=True,
                    cookies=self._cookies,
                    proxy_rotator=attempt_proxy_rotator,
                    timeout=self._timeout_ms,
                    retries=1,
                    retry_delay=1.0,
                    wait=1_000,
                    page_action=load_visible_comments,
                )
            except Exception as exc:  # noqa: BLE001
                self._record_retry_reason("coauthor_status_only_render_fetch_failed")
                failed_metadata = {
                    "transport": attempt_label,
                    "reason": "render_fetch_failed",
                    "error": f"{exc.__class__.__name__}: {exc}",
                }
                attempt_metadata.append(failed_metadata)
                self._last_coauthor_status_only_render_metadata = {
                    **self._last_coauthor_status_only_render_metadata,
                    **failed_metadata,
                    "attempts": list(attempt_metadata),
                }
                logger.warning(
                    "Rendered coauthor comments fallback failed for shortcode=%s transport=%s: %s",
                    shortcode,
                    attempt_label,
                    exc,
                    exc_info=True,
                )
                if attempt_label == "direct":
                    return []
                continue

            self._sync_response_cookies(response)
            html_text = _response_text(response)
            dom_comments = _extract_rendered_dom_snapshot_comments(
                html_text,
                shortcode=shortcode,
                post_url=post_url,
                ignored_usernames=ignored_usernames,
                source_snapshot_type="rendered_coauthor_comments",
                is_hidden_by_instagram=False,
            )
            permalink_comments = _extract_rendered_permalink_comments(
                html_text,
                shortcode=shortcode,
                post_url=post_url,
                ignored_usernames=ignored_usernames,
                source_snapshot_type="rendered_coauthor_comments",
                is_hidden_by_instagram=False,
            )
            comments = dom_comments or permalink_comments
            render_metadata = _rendered_dom_snapshot_metadata(
                html_text,
                shortcode=shortcode,
                dom_comments_count=len(dom_comments),
                permalink_comments_count=len(permalink_comments),
                status_code=_status_code(response),
                location=_safe_location(response),
            )
            render_metadata["transport"] = attempt_label
            render_metadata["reason"] = "rendered_comments_found" if comments else "rendered_comments_empty"
            attempt_metadata.append(render_metadata)
            self._last_coauthor_status_only_render_metadata = {
                **self._last_coauthor_status_only_render_metadata,
                **render_metadata,
                "attempts": list(attempt_metadata),
            }
            if comments:
                break
            if attempt_label == "direct" or not _rendered_dom_should_retry_without_proxy(render_metadata):
                break
        self._record_lane_diagnostic(
            "rendered",
            shortcode=shortcode,
            reason="coauthor_status_only_rendered",
            count=len(comments),
            metadata=self._last_coauthor_status_only_render_metadata,
        )
        self._coauthor_status_only_rendered_comments += len(comments)
        if comments:
            logger.info(
                "Rendered coauthor Instagram post yielded %d permalink comment(s) for shortcode=%s",
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
        self._record_lane_diagnostic(
            "child",
            shortcode=shortcode,
            reason="reply_api_request",
            metadata={"parent_comment_id": str(comment_id or "").strip() or None},
        )
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
        # Phase A5 follow-up: track which directions have been attempted on this
        # parent so reply pagination can swap min_id <-> max_id once before
        # declaring repeated_cursor terminal.
        cursor_directions_attempted: set[str] = set()
        cursor_direction_swaps = 0
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

            (
                reply_rows,
                next_cursor,
                next_cursor_param_name,
                alt_next_reply_cursor,
                alt_next_reply_cursor_param_name,
            ) = _extract_reply_page(payload, response)
            for reply_data in reply_rows:
                if not isinstance(reply_data, dict):
                    continue
                parsed_reply = self._parser.parse_comment(
                    reply_data,
                    shortcode,
                    post_url,
                    is_reply=True,
                    parent_id=comment_id,
                    phase="child",
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
            # Only record a "direction attempt" for an actually-paginated request;
            # the initial seed (cursor is None) doesn't exercise either direction.
            if cursor is not None and cursor_param_name in {"min_id", "max_id"}:
                cursor_directions_attempted.add(cursor_param_name)
            if next_cursor_key == current_cursor_key or next_cursor_key in seen_cursors:
                # Phase A5 follow-up: try the cross-direction reply cursor
                # (min_id <-> max_id) before declaring repeated_cursor terminal.
                alt_param = (
                    str(alt_next_reply_cursor_param_name or "").strip()
                    if alt_next_reply_cursor and alt_next_reply_cursor_param_name
                    else None
                )
                if (
                    alt_param in {"min_id", "max_id"}
                    and alt_param not in cursor_directions_attempted
                    and alt_next_reply_cursor
                ):
                    alt_cursor_key = f"{alt_param}:{alt_next_reply_cursor}"
                    if alt_cursor_key not in seen_cursors:
                        logger.info(
                            "Instagram reply pagination swapping cursor direction "
                            "from %s to %s on comment_id=%s repeated_cursor=%s",
                            cursor_param_name,
                            alt_param,
                            comment_id,
                            next_cursor,
                        )
                        seen_cursors.add(alt_cursor_key)
                        cursor_direction_swaps += 1
                        cursor_directions_attempted.add(alt_param)
                        cursor = str(alt_next_reply_cursor)
                        cursor_param_name = alt_param
                        self._record_retry_reason("pagination_repeated_cursor_swap_direction_reply")
                        self._reply_cursor_direction_swaps += 1
                        continue

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
                # Phase A5 follow-up: only mark non-retryable when BOTH cursor
                # directions have been actually attempted on this parent. When
                # alt was never available, preserve legacy retryable behavior
                # so the next attempt can see different IG state.
                both_directions_attempted = (
                    "min_id" in cursor_directions_attempted
                    and "max_id" in cursor_directions_attempted
                )
                if both_directions_attempted:
                    retryable = retryable and not has_gap
                else:
                    retryable = retryable or has_gap
                logger.warning(
                    "Instagram reply pagination repeated cursor for comment_id=%s cursor=%s "
                    "directions_attempted=%s direction_swaps=%d",
                    comment_id,
                    next_cursor,
                    sorted(cursor_directions_attempted),
                    cursor_direction_swaps,
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
            # Phase 5.2: try Postgres advisory lock for cross-container coordination
            # when configured; fall through to per-container file lock otherwise.
            paced: bool
            if self._global_rate_limit_mode_configured == "advisory":
                self._global_rate_limit_advisory_attempts += 1
                advisory_result = await asyncio.to_thread(
                    _try_advisory_lock_pace,
                    key=self._global_rate_limit_key,
                    delay_seconds=self._global_api_delay_seconds,
                    deadline=deadline,
                )
                self._global_rate_limit_advisory_total_wait_ms += int(advisory_result.get("wait_ms") or 0)
                if advisory_result.get("cooldown_blocked"):
                    self._global_rate_limit_mode_last = "advisory"
                    return False
                if advisory_result.get("acquired"):
                    self._global_rate_limit_advisory_acquires += 1
                    self._global_rate_limit_mode_last = "advisory"
                    paced = bool(advisory_result.get("paced", True))
                else:
                    self._global_rate_limit_advisory_fallback_count += 1
                    self._global_rate_limit_advisory_last_error = advisory_result.get("error")
                    self._global_rate_limit_mode_last = "file_lock_fallback"
                    paced = await asyncio.to_thread(
                        _pace_global_api_request,
                        key=self._global_rate_limit_key,
                        delay_seconds=self._global_api_delay_seconds,
                        deadline=deadline,
                    )
            else:
                self._global_rate_limit_mode_last = "file_lock"
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

    def _record_lane_diagnostic(
        self,
        lane: str,
        *,
        shortcode: str | None = None,
        reason: str | None = None,
        count: int | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        normalized_lane = str(lane or "").strip()
        if not normalized_lane:
            return
        lane_metadata = self._lane_diagnostics.setdefault(
            normalized_lane,
            {
                "attempted": False,
                "attempt_count": 0,
            },
        )
        lane_metadata["attempted"] = True
        lane_metadata["attempt_count"] = int(lane_metadata.get("attempt_count") or 0) + 1
        if shortcode:
            lane_metadata["last_shortcode"] = self._compact_checkpoint_text(shortcode)
        if reason:
            lane_metadata["last_reason"] = self._compact_checkpoint_text(reason)
        if count is not None:
            lane_metadata["last_count"] = self._non_negative_int(count)
        if metadata:
            lane_metadata["last_metadata"] = {
                str(key): value
                for key, value in dict(metadata).items()
                if value is not None
            }

    def _record_retry_reason(self, reason: str | None) -> None:
        normalized = str(reason or "").strip()
        if not normalized:
            return
        self._retry_reason_counts[normalized] = self._retry_reason_counts.get(normalized, 0) + 1
        self._record_lane_diagnostic("retry", reason=normalized)

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
        diagnostic_metadata: dict[str, Any] | None = None,
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
        if diagnostic_metadata:
            checkpoint["diagnostic_metadata"] = diagnostic_metadata
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
                if (
                    not browser_api_fallback_attempted
                    and _env_truthy(_BROWSER_API_FALLBACK_ENV, True)
                ):
                    browser_api_fallback_attempted = True
                    self._record_retry_reason("browser_api_fallback_after_html_challenge")
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
                        and browser_result.get("reason") == "html_challenge_or_auth_required"
                    ):
                        return browser_result
                return self._decode_json_response_result(response, attempt=attempt)

            return self._decode_json_response_result(response, attempt=attempt)
