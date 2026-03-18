#!/usr/bin/env python3
"""Validate and refresh social auth cookies using the repo's canonical loaders."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

# Add project root to path (scripts/socials -> project root)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.utils.env import load_env

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
    return social_repo._platform_cookie_refresh_target_path(  # noqa: SLF001
        social_repo._default_twitter_cookie_file_path(),  # noqa: SLF001
        "SOCIAL_TWITTER_COOKIES_FILE",
        "TWITTER_COOKIES_FILE",
    )


def _tiktok_cookie_path() -> Path:
    return social_repo._platform_cookie_refresh_target_path(  # noqa: SLF001
        social_repo._default_tiktok_cookie_file_path(),  # noqa: SLF001
        "SOCIAL_TIKTOK_COOKIES_FILE",
        "TIKTOK_COOKIES_FILE",
    )


def _facebook_cookie_path() -> Path:
    return social_repo._platform_cookie_refresh_target_path(  # noqa: SLF001
        social_repo._default_facebook_cookie_file_path(),  # noqa: SLF001
        "SOCIAL_FACEBOOK_COOKIES_FILE",
        "FACEBOOK_COOKIES_FILE",
    )


def _threads_cookie_path() -> Path:
    return social_repo._platform_cookie_refresh_target_path(  # noqa: SLF001
        social_repo._default_threads_cookie_file_path(),  # noqa: SLF001
        "SOCIAL_THREADS_COOKIES_FILE",
        "THREADS_COOKIES_FILE",
    )


def _socialblade_cookie_path() -> Path:
    from trr_backend.socials.socialblade.auth import socialblade_cookie_file_path

    return socialblade_cookie_file_path()


PLATFORM_HANDLERS: dict[str, PlatformHandlers] = {
    "instagram": PlatformHandlers(
        platform="instagram",
        load=social_repo._load_instagram_cookies,  # noqa: SLF001
        load_from_sources=social_repo._load_instagram_cookies_from_sources,  # noqa: SLF001
        validate=social_repo._validate_instagram_cookie_health,  # noqa: SLF001
        refresh=social_repo._refresh_instagram_cookies,  # noqa: SLF001
        cookie_file=social_repo._instagram_cookie_refresh_target_path,  # noqa: SLF001
        headless_env="SOCIAL_INSTAGRAM_COOKIE_REFRESH_HEADLESS",
    ),
    "tiktok": PlatformHandlers(
        platform="tiktok",
        load=social_repo._load_tiktok_cookies,  # noqa: SLF001
        load_from_sources=social_repo._load_tiktok_cookies_from_sources,  # noqa: SLF001
        validate=social_repo._validate_tiktok_cookie_health,  # noqa: SLF001
        refresh=social_repo._refresh_tiktok_cookies,  # noqa: SLF001
        cookie_file=_tiktok_cookie_path,
        headless_env="SOCIAL_TIKTOK_COOKIE_REFRESH_HEADLESS",
    ),
    "twitter": PlatformHandlers(
        platform="twitter",
        load=lambda: social_repo._load_twitter_auth()[0],  # noqa: SLF001
        load_from_sources=lambda: social_repo._load_twitter_auth_from_sources()[0],  # noqa: SLF001
        validate=social_repo._validate_twitter_cookie_health,  # noqa: SLF001
        refresh=social_repo._refresh_twitter_cookies,  # noqa: SLF001
        cookie_file=_twitter_cookie_path,
        headless_env="SOCIAL_TWITTER_COOKIE_REFRESH_HEADLESS",
    ),
    "facebook": PlatformHandlers(
        platform="facebook",
        load=social_repo._load_facebook_cookies,  # noqa: SLF001
        load_from_sources=social_repo._load_facebook_cookies_from_sources,  # noqa: SLF001
        validate=social_repo._validate_facebook_cookie_health,  # noqa: SLF001
        refresh=social_repo._refresh_facebook_cookies,  # noqa: SLF001
        cookie_file=_facebook_cookie_path,
        headless_env="SOCIAL_FACEBOOK_COOKIE_REFRESH_HEADLESS",
    ),
    "threads": PlatformHandlers(
        platform="threads",
        load=social_repo._load_threads_cookies,  # noqa: SLF001
        load_from_sources=social_repo._load_threads_cookies_from_sources,  # noqa: SLF001
        validate=social_repo._validate_threads_cookie_health,  # noqa: SLF001
        refresh=social_repo._refresh_threads_cookies,  # noqa: SLF001
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


def _cookie_summary(cookies: dict[str, str]) -> dict[str, object]:
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
) -> tuple[int, dict[str, object]]:
    if headed:
        os.environ[handlers.headless_env] = "false"

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
        "cookie_file": str(handlers.cookie_file()),
        "validated": valid,
        "reason": reason,
        **_cookie_summary(cookies),
    }
    return (0 if valid else 1), result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate or refresh social auth cookies using canonical repo loaders",
    )
    parser.add_argument(
        "--platform",
        choices=[*PLATFORM_HANDLERS.keys(), "all"],
        default="all",
        help="Target platform (default: all)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force cookie regeneration instead of only loading/validating current cookies",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate stored cookies without triggering auto-refresh loaders",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run cookie refresh in headed browser mode for platforms that need interactive auth",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env()

    platform_names = list(PLATFORM_HANDLERS) if args.platform == "all" else [args.platform]
    exit_code = 0
    for platform_name in platform_names:
        handlers = PLATFORM_HANDLERS[platform_name]
        rc, result = run_platform(
            handlers,
            force=bool(args.force),
            validate_only=bool(args.validate_only),
            headed=bool(args.headed),
        )
        exit_code = max(exit_code, rc)
        print(json.dumps(result, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
