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

PROFILE_POSTS_DOC_IDS = (
    "25645538101792896",
    "26035927152742158",
    "33944389991841132",
)

WEB_X_ASBD_ID = "359341"
PROFILE_POSTS_PAGE_SIZE = int(os.getenv("SOCIAL_INSTAGRAM_PAGE_SIZE", "33"))
PROFILE_POSTS_FAST_PAGE_SIZE = 50

QUERY_TYPE_PROFILE_HTML = "profile_html"
QUERY_TYPE_PROFILE_INFO = "profile_info"
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

