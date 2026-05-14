"""Shared social platform constants and helpers."""

from __future__ import annotations

from urllib.parse import urlparse

SOCIAL_SUPPORTED_PLATFORMS: tuple[str, ...] = (
    "instagram",
    "tiktok",
    "twitter",
    "youtube",
    "facebook",
    "threads",
    "reddit",
)

SOCIAL_SUPPORTED_PLATFORMS_SET: set[str] = set(SOCIAL_SUPPORTED_PLATFORMS)

SOCIALBLADE_SUPPORTED_PLATFORMS: tuple[str, ...] = (
    "instagram",
    "facebook",
    "tiktok",
    "youtube",
)

SOCIALBLADE_SUPPORTED_PLATFORMS_SET: set[str] = set(SOCIALBLADE_SUPPORTED_PLATFORMS)

SOCIAL_SOURCE_SCOPES: tuple[str, ...] = (
    "network",
    "creator",
    "community",
    "news",
)

SOCIAL_SOURCE_SCOPES_SET: set[str] = set(SOCIAL_SOURCE_SCOPES)
LEGACY_SOURCE_SCOPE_ALIASES: dict[str, str] = {
    "bravo": "network",
}

SOCIAL_PLATFORM_DEFAULT_ORDER: tuple[str, ...] = (
    "instagram",
    "youtube",
    "tiktok",
    "twitter",
    "facebook",
    "threads",
    "reddit",
)

# Explicit URL->platform mapping used by auto-discovery and adapter selection.
URL_PLATFORM_MAPPING: tuple[tuple[str, str], ...] = (
    ("instagram.com", "instagram"),
    ("tiktok.com", "tiktok"),
    ("twitter.com", "twitter"),
    ("x.com", "twitter"),
    ("youtube.com", "youtube"),
    ("youtu.be", "youtube"),
    ("reddit.com", "reddit"),
    ("imdb.com", "imdb"),
    ("facebook.com", "facebook"),
    ("threads.net", "threads"),
)


def normalize_platform(value: str | None) -> str:
    return str(value or "").strip().lower()


def is_supported_platform(value: str | None) -> bool:
    return normalize_platform(value) in SOCIAL_SUPPORTED_PLATFORMS_SET


def is_socialblade_supported_platform(value: str | None) -> bool:
    return normalize_platform(value) in SOCIALBLADE_SUPPORTED_PLATFORMS_SET


def normalize_source_scope(value: str | None, *, default: str = "network") -> str:
    normalized = str(value or default).strip().lower() or default
    canonical = LEGACY_SOURCE_SCOPE_ALIASES.get(normalized, normalized)
    if canonical not in SOCIAL_SOURCE_SCOPES_SET:
        raise ValueError(f"Unsupported source scope: {value}")
    return canonical


def infer_platform_from_url(url: str | None, *, fallback: str = "generic") -> str:
    text = str(url or "").strip()
    if not text:
        return fallback
    host = urlparse(text).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    for suffix, platform in URL_PLATFORM_MAPPING:
        if host == suffix or host.endswith(f".{suffix}"):
            return platform
    return fallback
