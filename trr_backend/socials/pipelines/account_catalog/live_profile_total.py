"""Import-neutral Instagram live-profile-total probing for catalog freshness."""

from __future__ import annotations

import json
import logging
import os
import time as time_module
from pathlib import Path
from threading import Lock
from typing import Any

from trr_backend.socials.cookie_sources import (
    _default_platform_cookie_file_path,
    _platform_cookie_file_candidates,
    _select_preferred_cookie_candidate,
)

logger = logging.getLogger(__name__)

_LIVE_TOTAL_CACHE: dict[str, tuple[float, int | None]] = {}
_LIVE_TOTAL_CACHE_LOCK = Lock()


def _env_truthy(name: str) -> bool:
    return str(os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _live_total_cache_ttl_seconds() -> int:
    raw = os.getenv("SOCIAL_PROFILE_TOTAL_POSTS_CACHE_TTL_SEC") or "300"
    try:
        return max(30, int(raw))
    except (TypeError, ValueError):
        return 300


def _live_total_probe_deadline_seconds() -> float:
    raw = os.getenv("SOCIAL_LIVE_PROFILE_TOTAL_PROBE_DEADLINE_SEC") or "22.0"
    try:
        return max(4.0, float(raw))
    except (TypeError, ValueError):
        return 22.0


def _instagram_cookie_validation_username() -> str:
    explicit = (os.getenv("SOCIAL_INSTAGRAM_COOKIE_VALIDATION_USERNAME") or "").strip().lstrip("@")
    if explicit:
        return explicit.lower()
    fallback = (os.getenv("SOCIAL_INSTAGRAM_COOKIE_VALIDATION_FALLBACK_USERNAME") or "").strip().lstrip("@")
    return (fallback or "instagram").lower()


def _cookie_mapping(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): str(cookie_value)
        for key, cookie_value in value.items()
        if cookie_value is not None and not str(key).startswith("_")
    }


def _load_cookie_file(path: Path) -> dict[str, str]:
    try:
        return _cookie_mapping(json.loads(path.read_text(encoding="utf-8")))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to load Instagram cookies from %s: %s", path, exc)
        return {}


def _load_legacy_instagram_cookies() -> dict[str, str]:
    """Load the same configured cookie sources as the legacy compatibility path."""

    candidates: list[dict[str, str]] = []
    raw_json = (os.getenv("SOCIAL_INSTAGRAM_COOKIES_JSON") or "").strip() or (
        os.getenv("INSTAGRAM_COOKIES_JSON") or ""
    ).strip()
    if raw_json:
        try:
            cookies = _cookie_mapping(json.loads(raw_json))
        except json.JSONDecodeError:
            logger.warning("Invalid Instagram cookies JSON from env; falling back to file-based cookies")
        else:
            if cookies:
                candidates.append(cookies)

    default_path = _default_platform_cookie_file_path("instagram")
    for path in _platform_cookie_file_candidates(
        default_path,
        "SOCIAL_INSTAGRAM_COOKIES_FILE",
        "INSTAGRAM_COOKIES_FILE",
    ):
        if path.is_file() and (cookies := _load_cookie_file(path)):
            candidates.append(cookies)

    return _select_preferred_cookie_candidate(
        candidates,
        required_cookie_names_any=("sessionid",),
        required_cookie_names_all=("csrftoken", "ds_user_id"),
    )


def _load_instagram_auth_cookies() -> dict[str, str]:
    if not _env_truthy("INSTAGRAM_AUTH_RESOLVER_V2"):
        return _load_legacy_instagram_cookies()

    from trr_backend.socials.instagram.auth_resolver import resolve_instagram_auth_session

    auth_session = resolve_instagram_auth_session(
        browser_account_id=_instagram_cookie_validation_username(),
        caller_context="legacy_loader",
        require_validation=True,
    )
    return dict(auth_session.cookies)


def _instagram_scraper_type() -> type[Any]:
    from trr_backend.socials.instagram.scraper import InstagramScraper

    return InstagramScraper


def cached_instagram_live_profile_total_posts(account_handle: str) -> int | None:
    cache_key = account_handle
    now_monotonic = time_module.monotonic()
    with _LIVE_TOTAL_CACHE_LOCK:
        cached = _LIVE_TOTAL_CACHE.get(cache_key)
        if cached and cached[0] > now_monotonic:
            return cached[1]

    total_posts: int | None = None
    try:
        # Start the bounded request budget before auth loading, matching the
        # compatibility path and keeping cookie resolution inside the deadline.
        probe_deadline = time_module.monotonic() + _live_total_probe_deadline_seconds()
        auth_cookies = _load_instagram_auth_cookies()
        scraper_type = _instagram_scraper_type()
        scraper_candidates = [("public_profile_info", scraper_type(cookies={}, browser_account_id=account_handle))]
        if auth_cookies:
            scraper_candidates.append(
                (
                    "authenticated_profile_info",
                    scraper_type(cookies=auth_cookies, browser_account_id=account_handle),
                )
            )

        for source_label, scraper in scraper_candidates:
            if time_module.monotonic() >= probe_deadline:
                logger.debug(
                    "Instagram live profile total probe budget exhausted for instagram @%s before %s",
                    account_handle,
                    source_label,
                )
                break
            profile_payload = scraper.fetch_profile_info(account_handle, delay=0.0, request_timeout=(4, 7))
            if not isinstance(profile_payload, dict):
                continue
            total_posts = scraper._extract_profile_total_posts(profile_payload, source="profile_info")  # noqa: SLF001
            if total_posts is not None:
                break
    except Exception:  # noqa: BLE001
        logger.debug(
            "Failed resolving live profile total posts for instagram @%s",
            account_handle,
            exc_info=True,
        )

    with _LIVE_TOTAL_CACHE_LOCK:
        _LIVE_TOTAL_CACHE[cache_key] = (now_monotonic + _live_total_cache_ttl_seconds(), total_posts)
    return total_posts


def cached_instagram_live_profile_total_posts_cached_only(account_handle: str) -> int | None:
    with _LIVE_TOTAL_CACHE_LOCK:
        cached = _LIVE_TOTAL_CACHE.get(account_handle)
        if cached and cached[0] > time_module.monotonic():
            return cached[1]
    return None


__all__ = [
    "cached_instagram_live_profile_total_posts",
    "cached_instagram_live_profile_total_posts_cached_only",
]
