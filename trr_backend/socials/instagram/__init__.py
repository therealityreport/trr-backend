"""
Instagram scraping module.

Provides tools for fetching and analyzing Instagram posts
with support for filtering by hashtags, accounts, and date ranges.
Includes comment and reply fetching with like counts.
"""

from .auth_resolver import (
    InstagramAuthSession,
    auth_session_log_payload,
    build_authenticated_instagram_scraper,
    clear_instagram_auth_runtime_state,
    get_current_instagram_auth_session,
    resolve_instagram_auth_session,
    resolve_instagram_comments_auth_session,
    resolve_instagram_comments_auth_validation_mode,
    set_current_instagram_auth_session,
    set_instagram_runtime_override,
)
from .cookie_refresh import refresh_instagram_cookies
from .crawlee_adapter import run_stage_with_crawlee
from .permalink_metadata import (
    InstagramFacebookCrosspostMetadata,
    InstagramMediaResolution,
    InstagramPermalinkMetadata,
    fetch_instagram_facebook_crosspost_metadata,
    fetch_permalink_media_item,
    fetch_permalink_metadata,
    parse_permalink_metadata,
    resolve_instagram_media,
)
from .scraper import (
    InstagramComment,
    InstagramPost,
    InstagramScraper,
    InstagramUserDetail,
    ScrapeConfig,
    load_cookies_from_file,
)

__all__ = [
    "InstagramScraper",
    "InstagramPost",
    "InstagramComment",
    "InstagramUserDetail",
    "InstagramAuthSession",
    "ScrapeConfig",
    "load_cookies_from_file",
    "resolve_instagram_auth_session",
    "resolve_instagram_comments_auth_session",
    "resolve_instagram_comments_auth_validation_mode",
    "build_authenticated_instagram_scraper",
    "get_current_instagram_auth_session",
    "set_current_instagram_auth_session",
    "set_instagram_runtime_override",
    "clear_instagram_auth_runtime_state",
    "auth_session_log_payload",
    "refresh_instagram_cookies",
    "InstagramPermalinkMetadata",
    "InstagramMediaResolution",
    "InstagramFacebookCrosspostMetadata",
    "fetch_instagram_facebook_crosspost_metadata",
    "fetch_permalink_media_item",
    "fetch_permalink_metadata",
    "parse_permalink_metadata",
    "resolve_instagram_media",
    "run_stage_with_crawlee",
]
