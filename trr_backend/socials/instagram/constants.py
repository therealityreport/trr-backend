from __future__ import annotations

import os
from enum import StrEnum


class InstagramErrorCode(StrEnum):
    """Canonical error codes raised by the Instagram scraping stack.

    Before this enum, error classification used bare strings like
    ``"rate_limited"``, ``"unauthorized"``, and ``"validation_skipped"`` that
    flowed through ``InstagramRequestFailure`` and comparison sites. Typos in
    any branch would silently skip handling. Use these members at every
    raise-site and compare by identity (``code is InstagramErrorCode.RATE_LIMITED``)
    when the value is known to be an enum instance; string comparisons still
    work because ``StrEnum`` inherits from ``str``.
    """

    RATE_LIMITED = "rate_limited"
    UNAUTHORIZED = "unauthorized"
    FORBIDDEN = "forbidden"
    REDIRECT_LOGIN = "redirect_login"
    REQUEST_FAILED = "request_failed"
    VALIDATION_SKIPPED = "validation_skipped"
    STRUCTURAL_INVALID = "structural_invalid"
    CHECKPOINT_REQUIRED = "checkpoint_required"
    CHALLENGE_REQUIRED = "challenge_required"
    FEEDBACK_REQUIRED = "feedback_required"
    LOGIN_REQUIRED = "login_required"
    CONSENT_REQUIRED = "consent_required"
    SENTRY_BLOCK = "sentry_block"


GRAPHQL_URL = "https://www.instagram.com/graphql/query"
PROFILE_INFO_URL = "https://www.instagram.com/api/v1/users/web_profile_info/"
POST_INFO_URL = "https://www.instagram.com/api/v1/media/{media_id}/info/"
COMMENTS_URL = "https://www.instagram.com/api/v1/media/{media_id}/comments/"
COMMENT_REPLIES_URL = "https://www.instagram.com/api/v1/media/{media_id}/comments/{comment_id}/child_comments/"
PROFILE_PAGE_URL = "https://www.instagram.com/{username}/"
PERMALINK_URL = "https://www.instagram.com/p/{shortcode}/"
COMMENT_SORT_ORDER_ENV = "SOCIAL_INSTAGRAM_COMMENTS_SORT_ORDER"
DEFAULT_COMMENT_SORT_ORDER = "recent"
VALID_COMMENT_SORT_ORDERS = frozenset({"popular", "recent"})

PROFILE_POSTS_DOC_IDS_ENV = "SOCIAL_INSTAGRAM_PROFILE_POSTS_DOC_IDS"
PROFILE_POSTS_FRIENDLY_NAME = "PolarisProfilePostsQuery"
PROFILE_POSTS_ROOT_FIELD_NAME = "xdt_api__v1__feed__user_timeline_graphql_connection"
_PROFILE_POSTS_DOC_IDS_FALLBACK = (
    "26859136577041380",
    "25645538101792896",
    "26035927152742158",
    "33944389991841132",
)
PROFILE_PAGE_CONTENT_DOC_IDS_ENV = "SOCIAL_INSTAGRAM_PROFILE_PAGE_CONTENT_DOC_IDS"
PROFILE_PAGE_CONTENT_FRIENDLY_NAME = "PolarisProfilePageContentQuery"
PROFILE_PAGE_CONTENT_ROOT_FIELD_NAME = "fetch__XDTUserDict"
_PROFILE_PAGE_CONTENT_DOC_IDS_FALLBACK = ("35710877621861450",)


def resolve_profile_posts_doc_ids(raw_value: str | None = None) -> tuple[str, ...]:
    """Phase 4.1: env-driven hot rotation for IG profile-posts GraphQL doc IDs.

    Comma-separated values from ``SOCIAL_INSTAGRAM_PROFILE_POSTS_DOC_IDS`` are
    parsed, whitespace-stripped, deduplicated, and validated as digit-only
    strings. When the env var is unset, empty, or every entry is invalid, the
    hardcoded fallback tuple is returned instead. Operators can hot-rotate
    without a deploy when IG bumps doc IDs.
    """
    raw = raw_value if raw_value is not None else os.getenv(PROFILE_POSTS_DOC_IDS_ENV)
    if not raw:
        return _PROFILE_POSTS_DOC_IDS_FALLBACK
    seen: set[str] = set()
    chosen: list[str] = []
    for token in str(raw).split(","):
        candidate = token.strip()
        if not candidate or not candidate.isdigit():
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        chosen.append(candidate)
    return tuple(chosen) if chosen else _PROFILE_POSTS_DOC_IDS_FALLBACK


def resolve_profile_page_content_doc_ids(raw_value: str | None = None) -> tuple[str, ...]:
    """Resolve web profile-content GraphQL doc IDs captured from the Instagram profile page."""
    raw = raw_value if raw_value is not None else os.getenv(PROFILE_PAGE_CONTENT_DOC_IDS_ENV)
    if not raw:
        return _PROFILE_PAGE_CONTENT_DOC_IDS_FALLBACK
    seen: set[str] = set()
    chosen: list[str] = []
    for token in str(raw).split(","):
        candidate = token.strip()
        if not candidate or not candidate.isdigit():
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        chosen.append(candidate)
    return tuple(chosen) if chosen else _PROFILE_PAGE_CONTENT_DOC_IDS_FALLBACK


PROFILE_POSTS_DOC_IDS = resolve_profile_posts_doc_ids()
PROFILE_PAGE_CONTENT_DOC_IDS = resolve_profile_page_content_doc_ids()

WEB_X_ASBD_ID = "359341"
PROFILE_POSTS_PAGE_SIZE = int(os.getenv("SOCIAL_INSTAGRAM_PAGE_SIZE", "33"))
PROFILE_POSTS_FAST_PAGE_SIZE = 50

QUERY_TYPE_PROFILE_HTML = "profile_html"
QUERY_TYPE_PROFILE_INFO = "profile_info"
QUERY_TYPE_GRAPHQL_PROFILE_PAGE_CONTENT = "graphql_profile_page_content"
QUERY_TYPE_GRAPHQL_PROFILE_POSTS = "graphql_profile_posts"
QUERY_TYPE_PERMALINK_MEDIA = "permalink_media"
QUERY_TYPE_BROWSER_GRAPHQL_INTERCEPT = "browser_graphql_intercept"
QUERY_TYPE_LEGACY = "legacy"

AUTH_FATAL_MESSAGES = {
    "checkpoint_required",
    "challenge_required",
    "feedback_required",
    "login_required",
    "consent_required",
    "sentry_block",
}


def resolve_comment_sort_order(raw_value: str | None = None) -> str | None:
    """Resolve the Instagram comments ordering used for full pagination.

    Instagram's default ranked ordering can cycle through headload cursors on
    high-volume posts. The `recent` order is the safer default for coverage
    backfills; operators can set the env var to `popular` for one-off parity
    checks, or `none` to omit the parameter during upstream debugging.
    """
    raw = raw_value if raw_value is not None else os.getenv(COMMENT_SORT_ORDER_ENV)
    value = str(raw or DEFAULT_COMMENT_SORT_ORDER).strip().lower()
    if value in {"", "none", "default", "off", "false"}:
        return None
    if value in VALID_COMMENT_SORT_ORDERS:
        return value
    return DEFAULT_COMMENT_SORT_ORDER
