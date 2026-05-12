"""Cookie refresh operations shared by social auth CLI entrypoints."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import trr_backend.socials.social_season_analytics_impl as social_repo

PlatformCookieLoader = Callable[[], dict[str, str]]
PlatformCookieValidator = Callable[[dict[str, str]], tuple[bool, str | None]]
PlatformCookieRefresher = Callable[[str | None], dict[str, str]]
PlatformCookiePathResolver = Callable[[], Path]


@dataclass(frozen=True)
class PlatformHandlers:
    platform: str
    load: PlatformCookieLoader
    load_from_sources: PlatformCookieLoader
    validate: PlatformCookieValidator
    refresh: PlatformCookieRefresher
    cookie_file: PlatformCookiePathResolver
    headless_env: str


def _twitter_cookie_path() -> Path:
    return social_repo._platform_cookie_refresh_target_path(
        social_repo._default_twitter_cookie_file_path(),
        "SOCIAL_TWITTER_COOKIES_FILE",
        "TWITTER_COOKIES_FILE",
    )


def _tiktok_cookie_path() -> Path:
    return social_repo._platform_cookie_refresh_target_path(
        social_repo._default_tiktok_cookie_file_path(),
        "SOCIAL_TIKTOK_COOKIES_FILE",
        "TIKTOK_COOKIES_FILE",
    )


def _facebook_cookie_path() -> Path:
    return social_repo._platform_cookie_refresh_target_path(
        social_repo._default_facebook_cookie_file_path(),
        "SOCIAL_FACEBOOK_COOKIES_FILE",
        "FACEBOOK_COOKIES_FILE",
    )


def _threads_cookie_path() -> Path:
    return social_repo._platform_cookie_refresh_target_path(
        social_repo._default_threads_cookie_file_path(),
        "SOCIAL_THREADS_COOKIES_FILE",
        "THREADS_COOKIES_FILE",
    )


def _socialblade_cookie_path() -> Path:
    from trr_backend.socials.socialblade.auth import socialblade_cookie_file_path

    return socialblade_cookie_file_path()


PLATFORM_HANDLERS: dict[str, PlatformHandlers] = {
    "instagram": PlatformHandlers(
        platform="instagram",
        load=social_repo._load_instagram_cookies,
        load_from_sources=social_repo._load_instagram_cookies_from_sources,
        validate=social_repo._validate_instagram_cookie_health,
        refresh=social_repo._refresh_instagram_cookies,
        cookie_file=social_repo._instagram_cookie_refresh_target_path,
        headless_env="SOCIAL_INSTAGRAM_COOKIE_REFRESH_HEADLESS",
    ),
    "tiktok": PlatformHandlers(
        platform="tiktok",
        load=social_repo._load_tiktok_cookies,
        load_from_sources=social_repo._load_tiktok_cookies_from_sources,
        validate=social_repo._validate_tiktok_cookie_health,
        refresh=social_repo._refresh_tiktok_cookies,
        cookie_file=_tiktok_cookie_path,
        headless_env="SOCIAL_TIKTOK_COOKIE_REFRESH_HEADLESS",
    ),
    "twitter": PlatformHandlers(
        platform="twitter",
        load=lambda: social_repo._load_twitter_auth()[0],
        load_from_sources=lambda: social_repo._load_twitter_auth_from_sources()[0],
        validate=social_repo._validate_twitter_cookie_health,
        refresh=social_repo._refresh_twitter_cookies,
        cookie_file=_twitter_cookie_path,
        headless_env="SOCIAL_TWITTER_COOKIE_REFRESH_HEADLESS",
    ),
    "facebook": PlatformHandlers(
        platform="facebook",
        load=social_repo._load_facebook_cookies,
        load_from_sources=social_repo._load_facebook_cookies_from_sources,
        validate=social_repo._validate_facebook_cookie_health,
        refresh=social_repo._refresh_facebook_cookies,
        cookie_file=_facebook_cookie_path,
        headless_env="SOCIAL_FACEBOOK_COOKIE_REFRESH_HEADLESS",
    ),
    "threads": PlatformHandlers(
        platform="threads",
        load=social_repo._load_threads_cookies,
        load_from_sources=social_repo._load_threads_cookies_from_sources,
        validate=social_repo._validate_threads_cookie_health,
        refresh=social_repo._refresh_threads_cookies,
        cookie_file=_threads_cookie_path,
        headless_env="SOCIAL_THREADS_COOKIE_REFRESH_HEADLESS",
    ),
    "socialblade": PlatformHandlers(
        platform="socialblade",
        load=lambda: __import__(
            "trr_backend.socials.socialblade.auth",
            fromlist=["load_socialblade_cookies"],
        ).load_socialblade_cookies(),
        load_from_sources=lambda: __import__(
            "trr_backend.socials.socialblade.auth",
            fromlist=["load_socialblade_cookies_from_sources"],
        ).load_socialblade_cookies_from_sources(),
        validate=lambda cookies: __import__(
            "trr_backend.socials.socialblade.auth",
            fromlist=["validate_socialblade_cookie_health"],
        ).validate_socialblade_cookie_health(cookies),
        refresh=lambda reason: __import__(
            "trr_backend.socials.socialblade.auth",
            fromlist=["refresh_socialblade_cookies"],
        ).refresh_socialblade_cookies(reason),
        cookie_file=_socialblade_cookie_path,
        headless_env="SOCIALBLADE_COOKIE_REFRESH_HEADLESS",
    ),
}


def cookie_summary(cookies: dict[str, str]) -> dict[str, object]:
    return {
        "cookie_count": len(cookies),
        "cookie_names": sorted(cookies.keys()),
    }


def run_platform(
    handlers: PlatformHandlers,
    *,
    force: bool,
    validate_only: bool,
    headed: bool,
    validation_mode: str = "graphql_profile",
) -> tuple[int, dict[str, object]]:
    if headed:
        os.environ[handlers.headless_env] = "false"
    if handlers.platform == "instagram":
        os.environ["SOCIAL_INSTAGRAM_COMMENTS_AUTH_VALIDATION"] = validation_mode

    if force:
        cookies = handlers.refresh("forced_by_cli")
        action = "force_refresh"
    elif validate_only:
        cookies = handlers.load_from_sources()
        action = "validate_only"
    else:
        cookies = handlers.load()
        action = "load_or_refresh"

    valid, reason = handlers.validate(cookies) if cookies else (False, "no_cookies_loaded")
    result: dict[str, object] = {
        "platform": handlers.platform,
        "action": action,
        "headless": not headed,
        "validation_mode": validation_mode if handlers.platform == "instagram" else None,
        "cookie_file": str(handlers.cookie_file()),
        "validated": valid,
        "reason": reason,
        **cookie_summary(cookies),
    }
    return (0 if valid else 1), result


__all__ = [
    "PLATFORM_HANDLERS",
    "PlatformCookieLoader",
    "PlatformCookiePathResolver",
    "PlatformCookieRefresher",
    "PlatformCookieValidator",
    "PlatformHandlers",
    "cookie_summary",
    "run_platform",
]
