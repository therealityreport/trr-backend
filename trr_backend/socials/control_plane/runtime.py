"""Runtime and auth helpers for the social control plane.

This module is the canonical control-plane runtime import surface. Non-Instagram
runtime helpers temporarily bridge to `social_season_analytics_impl` until their
platform-specific modules own the full implementation.
"""

from __future__ import annotations

from typing import Any

import trr_backend.socials.social_season_analytics_impl as _core
from trr_backend.socials.instagram.auth_runtime import _load_instagram_cookies

SocialIngestConflictError = _core.SocialIngestConflictError
SocialIngestValidationError = _core.SocialIngestValidationError
SocialWorkerUnavailableError = _core.SocialWorkerUnavailableError


def _load_facebook_cookies() -> dict[str, str]:
    return _core._load_facebook_cookies()


def _load_threads_cookies() -> dict[str, str]:
    return _core._load_threads_cookies()


def _load_tiktok_cookies() -> dict[str, str]:
    return _core._load_tiktok_cookies()


def _load_twikit_credentials() -> Any:
    return _core._load_twikit_credentials()


def _load_twitter_auth() -> Any:
    return _core._load_twitter_auth()


def _adapt_payload_json_values(value: Any) -> Any:
    return _core._adapt_payload_json_values(value)


def _pg_upsert_many(*args: Any, **kwargs: Any) -> Any:
    return _core._pg_upsert_many(*args, **kwargs)


def _resolve_runtime_version_stamp() -> str:
    return _core._resolve_runtime_version_stamp()


def check_platform_cookie_health(*args: Any, **kwargs: Any) -> Any:
    return _core.check_platform_cookie_health(*args, **kwargs)


def refresh_platform_cookies_interactive(*args: Any, **kwargs: Any) -> Any:
    return _core.refresh_platform_cookies_interactive(*args, **kwargs)

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
    "_resolve_runtime_version_stamp",
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
