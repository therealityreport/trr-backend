"""Runtime and auth helpers for the social control plane."""

from __future__ import annotations

from trr_backend.repositories.social_season_analytics import (
    SocialIngestConflictError,
    SocialIngestValidationError,
    SocialWorkerUnavailableError,
    _adapt_payload_json_values,
    _load_facebook_cookies,
    _load_instagram_cookies,
    _load_threads_cookies,
    _load_tiktok_cookies,
    _load_twikit_credentials,
    _load_twitter_auth,
    _pg_upsert_many,
    check_platform_cookie_health,
    refresh_platform_cookies_interactive,
)

load_facebook_cookies = _load_facebook_cookies
load_instagram_cookies = _load_instagram_cookies
load_threads_cookies = _load_threads_cookies
load_tiktok_cookies = _load_tiktok_cookies
load_twikit_credentials = _load_twikit_credentials
load_twitter_auth = _load_twitter_auth
adapt_payload_json_values = _adapt_payload_json_values
pg_upsert_many = _pg_upsert_many

__all__ = [
    "SocialIngestConflictError",
    "SocialIngestValidationError",
    "SocialWorkerUnavailableError",
    "_adapt_payload_json_values",
    "_load_facebook_cookies",
    "_load_instagram_cookies",
    "_load_threads_cookies",
    "_load_tiktok_cookies",
    "_load_twikit_credentials",
    "_load_twitter_auth",
    "_pg_upsert_many",
    "adapt_payload_json_values",
    "check_platform_cookie_health",
    "load_facebook_cookies",
    "load_instagram_cookies",
    "load_threads_cookies",
    "load_tiktok_cookies",
    "load_twikit_credentials",
    "load_twitter_auth",
    "pg_upsert_many",
    "refresh_platform_cookies_interactive",
]
