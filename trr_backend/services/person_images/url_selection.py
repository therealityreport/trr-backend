"""URL selection helpers for person image workflows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlparse


def looks_like_getty_media_url(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    cleaned = value.strip()
    if not cleaned:
        return False
    try:
        hostname = (urlparse(cleaned).hostname or "").strip().lower()
    except Exception:
        return False
    return hostname == "media.gettyimages.com" or hostname.endswith(".gettyimages.com")


def get_mirrored_from_url(metadata: Any) -> str:
    if not isinstance(metadata, dict):
        return ""
    value = metadata.get("mirrored_from")
    return str(value or "").strip()


def should_reset_getty_hosted_state(
    *,
    desired_original_url: str | None,
    current_source_url: Any,
    hosted_url: Any,
    hosted_key: Any,
    metadata: Any,
) -> bool:
    desired = str(desired_original_url or "").strip()
    current_source = str(current_source_url or "").strip()
    current_hosted_url = str(hosted_url or "").strip()
    current_hosted_key = str(hosted_key or "").strip()
    mirrored_from = get_mirrored_from_url(metadata)

    if current_hosted_url and looks_like_getty_media_url(current_hosted_url):
        return True
    if desired and current_source and current_source != desired:
        return True
    if desired and mirrored_from and mirrored_from != desired:
        return True
    if desired and current_hosted_url and not current_hosted_key:
        return True
    return False


def is_http_url(value: str | None) -> bool:
    if not isinstance(value, str):
        return False
    trimmed = value.strip().lower()
    return trimmed.startswith("http://") or trimmed.startswith("https://")


def is_wikia_static_url(value: str | None) -> bool:
    if not isinstance(value, str) or not is_http_url(value):
        return False
    return "static.wikia.nocookie.net" in value.lower()


def iter_unique_urls(candidates: Sequence[str | None]) -> list[str]:
    seen: set[str] = set()
    urls: list[str] = []
    for value in candidates:
        if not is_http_url(value):
            continue
        normalized = str(value).strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls


def pick_autocount_urls(row: Mapping[str, Any]) -> list[str]:
    from trr_backend.media.s3_mirror import normalize_fandom_file_url

    source = str(row.get("source") or "").lower()
    image_url = row.get("image_url") or row.get("url")
    raw_url = row.get("url")
    thumb_url = row.get("thumb_url")
    hosted_url = row.get("hosted_url")
    referer = row.get("source_page_url") if isinstance(row.get("source_page_url"), str) else None

    if source == "tmdb":
        return iter_unique_urls([image_url, raw_url, hosted_url, thumb_url])

    if source in ("fandom", "fandom-gallery"):
        normalized = [
            normalize_fandom_file_url(str(value), referer=referer) if isinstance(value, str) else None
            for value in (image_url, raw_url, thumb_url)
        ]
        return iter_unique_urls([hosted_url, *normalized, image_url, raw_url, thumb_url])

    return iter_unique_urls([hosted_url, image_url, raw_url, thumb_url])


def pick_autocount_url(row: Mapping[str, Any]) -> str | None:
    urls = pick_autocount_urls(row)
    return urls[0] if urls else None


def build_media_link_autocount_urls(row: Mapping[str, Any]) -> list[str]:
    from trr_backend.media.s3_mirror import normalize_fandom_file_url

    source = str(row.get("source") or "").lower()
    hosted_url = row.get("hosted_url")
    source_url = row.get("source_url")
    raw_metadata = row.get("metadata")
    metadata: dict[str, Any] = raw_metadata if isinstance(raw_metadata, dict) else {}
    page_url_val = metadata.get("page_url")
    source_page_url_val = metadata.get("source_page_url")
    source_page_url = (
        page_url_val
        if isinstance(page_url_val, str)
        else source_page_url_val
        if isinstance(source_page_url_val, str)
        else None
    )

    if source in {"fandom", "fandom-gallery"} and isinstance(source_url, str):
        normalized = normalize_fandom_file_url(source_url, referer=source_page_url)
        return iter_unique_urls([hosted_url, normalized, source_url])
    return iter_unique_urls([hosted_url, source_url])
