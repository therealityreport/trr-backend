"""Environment-driven config for Crawlee-backed social scraping."""

from __future__ import annotations

import os
from dataclasses import dataclass

from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS

CRAWLEE_SUPPORTED_PLATFORMS = SOCIAL_SUPPORTED_PLATFORMS

CREDENTIAL_ACCOUNT_REGISTRY = {
    "instagram": "@codexhuli",
    "threads": "@codexhuli",
    "facebook": "https://www.facebook.com/Bravo",
    "twitter": "@CodexHuli",
    "youtube": "https://www.youtube.com/@CodexHuli",
    "reddit": "u/SuccotashHorror5266",
    "tiktok": "configured_in_env",
}

_MAX_CONCURRENCY_DEFAULT = 2
_MAX_RETRIES_DEFAULT = 3
_MAX_CONCURRENCY_BY_PLATFORM_DEFAULT = {
    "instagram": 4,
    "tiktok": 4,
    "twitter": 4,
    "youtube": 3,
}


@dataclass(frozen=True)
class CrawleeRuntimeConfig:
    """Effective Crawlee runtime config for a platform."""

    enabled: bool
    platform: str
    max_concurrency: int
    max_retries: int
    auth_strict: bool
    enabled_platforms: tuple[str, ...]
    force_legacy_platforms: tuple[str, ...]


def _env_truthy(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_positive_int_env(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def _parse_platform_set(raw: str | None, *, default_all: bool) -> tuple[str, ...]:
    if raw is None:
        return CRAWLEE_SUPPORTED_PLATFORMS if default_all else ()
    normalized = [token.strip().lower() for token in raw.split(",") if token.strip()]
    if not normalized:
        return CRAWLEE_SUPPORTED_PLATFORMS if default_all else ()
    allowed = [platform for platform in normalized if platform in CRAWLEE_SUPPORTED_PLATFORMS]
    return tuple(dict.fromkeys(allowed))


def should_use_crawlee(platform: str) -> bool:
    """Return whether Crawlee runtime should be used for this platform."""
    normalized_platform = (platform or "").strip().lower()
    if normalized_platform not in CRAWLEE_SUPPORTED_PLATFORMS:
        return False
    if not _env_truthy("SOCIAL_CRAWLEE_ENABLED", default=False):
        return False
    enabled_platforms = _parse_platform_set(os.getenv("SOCIAL_CRAWLEE_PLATFORMS"), default_all=True)
    if normalized_platform not in enabled_platforms:
        return False
    force_legacy_platforms = _parse_platform_set(os.getenv("SOCIAL_CRAWLEE_FORCE_LEGACY_PLATFORMS"), default_all=False)
    return normalized_platform not in force_legacy_platforms


def is_auth_strict_for_platform(platform: str) -> bool:
    """Return whether Crawlee auth preflight should fail-fast for the platform."""
    normalized_platform = (platform or "").strip().lower()
    if normalized_platform == "instagram":
        return _env_truthy("SOCIAL_CRAWLEE_AUTH_STRICT_INSTAGRAM", default=False)
    if normalized_platform == "facebook":
        return _env_truthy(
            "SOCIAL_CRAWLEE_AUTH_STRICT_FACEBOOK",
            default=_env_truthy("SOCIAL_CRAWLEE_AUTH_STRICT_INSTAGRAM", default=False),
        )
    if normalized_platform == "threads":
        return _env_truthy(
            "SOCIAL_CRAWLEE_AUTH_STRICT_THREADS",
            default=_env_truthy("SOCIAL_CRAWLEE_AUTH_STRICT_INSTAGRAM", default=False),
        )
    return True


def build_runtime_config(platform: str) -> CrawleeRuntimeConfig:
    """Build effective runtime config for a platform."""
    normalized_platform = (platform or "").strip().lower()
    enabled = should_use_crawlee(normalized_platform)
    enabled_platforms = _parse_platform_set(os.getenv("SOCIAL_CRAWLEE_PLATFORMS"), default_all=True)
    force_legacy_platforms = _parse_platform_set(os.getenv("SOCIAL_CRAWLEE_FORCE_LEGACY_PLATFORMS"), default_all=False)

    platform_suffix = normalized_platform.upper()
    platform_concurrency_default = _MAX_CONCURRENCY_BY_PLATFORM_DEFAULT.get(
        normalized_platform,
        _MAX_CONCURRENCY_DEFAULT,
    )
    max_concurrency = _resolve_positive_int_env(
        f"SOCIAL_CRAWLEE_MAX_CONCURRENCY_{platform_suffix}",
        _resolve_positive_int_env(
            "SOCIAL_CRAWLEE_MAX_CONCURRENCY",
            platform_concurrency_default,
            minimum=1,
            maximum=32,
        ),
        minimum=1,
        maximum=32,
    )
    max_retries = _resolve_positive_int_env(
        f"SOCIAL_CRAWLEE_MAX_RETRIES_{platform_suffix}",
        _resolve_positive_int_env("SOCIAL_CRAWLEE_MAX_RETRIES", _MAX_RETRIES_DEFAULT, minimum=1, maximum=10),
        minimum=1,
        maximum=10,
    )

    return CrawleeRuntimeConfig(
        enabled=enabled,
        platform=normalized_platform,
        max_concurrency=max_concurrency,
        max_retries=max_retries,
        auth_strict=is_auth_strict_for_platform(normalized_platform),
        enabled_platforms=enabled_platforms,
        force_legacy_platforms=force_legacy_platforms,
    )
