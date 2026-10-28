# ruff: noqa: F821, UP037
"""TikTok sound (music) parsing: sound id normalization, sound fields from post payloads,
and the public sound-page snapshot fetch.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import requests

from trr_backend.socials.control_plane.run_lifecycle import _legacy_module as _published_legacy_module
from trr_backend.socials.provider_registry import (
    LateNamespaceProvider,
    LateProviderProxy,
    adopt_published,
    publish_module_slot,
)
from trr_backend.socials.tiktok.constants import TIKTOK_USER_AGENT

if TYPE_CHECKING:
    # Repository names resolved through the published provider namespace.
    def _as_string_mapping(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_non_negative_int(*args: Any, **kwargs: Any) -> Any: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_normalize_tiktok_sound_id",
    "_normalize_tiktok_sound_ids",
    "_extract_tiktok_sound_fields",
    "_build_tiktok_sound_url",
    "_extract_tiktok_sound_snapshot_from_html",
    "_fetch_tiktok_sound_snapshot",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_SOUNDS_PROVIDER",
    room_names=_LOCAL_ROOM_NAMES,
    imported_names=_IMPORTED_CORE_NAMES,
    room_wrappers=_CORE_ROOM_WRAPPERS,
    commit=publish_module_slot(globals(), "_core"),
)
_core: Any = LateProviderProxy(_PROVIDER)
_require_provider_ready = _PROVIDER.require
_configure_legacy_provider = _PROVIDER.configure
_sync_core_overrides = _PROVIDER.sync
_room_callable = _PROVIDER.room_callable
adopt_published(_PROVIDER, _published_legacy_module)
del _published_legacy_module


def _normalize_tiktok_sound_id(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        return raw
    if "tiktok.com/music/" in raw:
        match = re.search(r"-([0-9]{8,})", raw)
        if match:
            return str(match.group(1))
    match = re.search(r"\b([0-9]{8,})\b", raw)
    if match:
        return str(match.group(1))
    return None


def _normalize_tiktok_sound_ids(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        sound_id = _room_callable("_normalize_tiktok_sound_id", _normalize_tiktok_sound_id)(value)
        if not sound_id or sound_id in seen:
            continue
        seen.add(sound_id)
        normalized.append(sound_id)
    return normalized


def _extract_tiktok_sound_fields(raw_data: dict[str, Any], post: Any) -> dict[str, Any]:
    _sync_core_overrides()
    music = _as_string_mapping(raw_data.get("music"))
    music_meta = _as_string_mapping(raw_data.get("musicMeta"))
    sound_id = _room_callable("_normalize_tiktok_sound_id", _normalize_tiktok_sound_id)(
        music.get("id")
        or music.get("musicId")
        or music.get("mid")
        or music_meta.get("musicId")
        or music_meta.get("id")
        or raw_data.get("music_id")
        or raw_data.get("sound_id")
    )
    sound_title = (
        str(
            music.get("title")
            or music.get("song")
            or music_meta.get("musicName")
            or music_meta.get("title")
            or getattr(post, "music_title", "")
            or ""
        ).strip()
        or None
    )
    sound_author = (
        str(
            music.get("authorName")
            or music.get("author_name")
            or music_meta.get("musicAuthor")
            or music_meta.get("authorName")
            or getattr(post, "music_author", "")
            or ""
        ).strip()
        or None
    )
    usage_count = _normalize_non_negative_int(
        music.get("videoCount")
        or music.get("video_count")
        or music_meta.get("videoCount")
        or music_meta.get("video_count")
        or raw_data.get("music_usage_count")
        or raw_data.get("sound_usage_count")
    )
    return {
        "sound_id": sound_id,
        "sound_title": sound_title,
        "sound_author": sound_author,
        "sound_usage_count": usage_count,
    }


def _build_tiktok_sound_url(sound_ref: str) -> str:
    normalized = str(sound_ref or "").strip()
    if normalized.startswith("http://") or normalized.startswith("https://"):
        return normalized
    return f"https://www.tiktok.com/music/-{normalized}"


def _extract_tiktok_sound_snapshot_from_html(html: str) -> dict[str, Any]:
    text = str(html or "")
    if not text:
        return {}

    title: str | None = None
    artist_name: str | None = None
    usage_count = 0

    title_match = re.search(r'"musicName"\s*:\s*"([^"]+)"', text)
    if title_match:
        title = str(title_match.group(1) or "").strip() or None
    artist_match = re.search(r'"authorName"\s*:\s*"([^"]+)"', text)
    if artist_match:
        artist_name = str(artist_match.group(1) or "").strip() or None

    for pattern in (r'"videoCount"\s*:\s*([0-9]+)', r'"video_count"\s*:\s*([0-9]+)', r'"usage_count"\s*:\s*([0-9]+)'):
        match = re.search(pattern, text)
        if match:
            usage_count = max(usage_count, int(match.group(1)))

    post_matches = re.findall(r"(?:https?://www\\.tiktok\\.com)?/@([A-Za-z0-9._-]{1,64})/video/([0-9]{8,32})", text)
    posts: list[dict[str, Any]] = []
    seen_video_ids: set[str] = set()
    for author, video_id in post_matches:
        source_id = str(video_id or "").strip()
        if not source_id or source_id in seen_video_ids:
            continue
        seen_video_ids.add(source_id)
        posts.append(
            {
                "platform_post_id": source_id,
                "creator_handle": str(author or "").strip() or None,
            }
        )

    return {
        "title": title,
        "artist_name": artist_name,
        "usage_count": usage_count,
        "posts": posts,
    }


def _fetch_tiktok_sound_snapshot(sound_ref: str) -> dict[str, Any]:
    sound_url = _room_callable("_build_tiktok_sound_url", _build_tiktok_sound_url)(sound_ref)
    try:
        response = requests.get(
            sound_url,
            timeout=(8, 25),
            headers={
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "accept-language": "en-US,en;q=0.9",
                "user-agent": TIKTOK_USER_AGENT,
            },
        )
    except Exception:  # noqa: BLE001
        return {
            "source_url": sound_url,
            "title": None,
            "artist_name": None,
            "usage_count": 0,
            "posts": [],
        }

    if not response.ok:
        return {
            "source_url": sound_url,
            "title": None,
            "artist_name": None,
            "usage_count": 0,
            "posts": [],
        }
    parsed = _room_callable("_extract_tiktok_sound_snapshot_from_html", _extract_tiktok_sound_snapshot_from_html)(
        response.text
    )
    parsed["source_url"] = sound_url
    return parsed


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_build_tiktok_sound_url",
    "_extract_tiktok_sound_fields",
    "_extract_tiktok_sound_snapshot_from_html",
    "_fetch_tiktok_sound_snapshot",
    "_normalize_tiktok_sound_id",
    "_normalize_tiktok_sound_ids",
]
