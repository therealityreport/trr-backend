"""Resolve direct TikTok media URLs for backend mirroring jobs."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from typing import Any

import requests

from trr_backend.socials.media_url_safety import (
    MediaUrlSafetyPolicy,
    UnsafeMediaUrlError,
    allowed_hosts_for_platform,
    safe_requests_get,
    validate_media_url,
)

_REHYDRATION_RE = re.compile(
    r'<script\s+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>',
    re.DOTALL,
)
_SIGI_STATE_RE = re.compile(
    r'<script\s+id="SIGI_STATE"[^>]*>(.*?)</script>',
    re.DOTALL,
)
_OG_VIDEO_RE = re.compile(
    r'<meta[^>]+(?:property|name)=["\']og:video(?::url)?["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_OG_IMAGE_RE = re.compile(
    r'<meta[^>]+(?:property|name)=["\']og:image(?::url)?["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    ),
}
_PROBE_HEADERS = {
    "user-agent": _DEFAULT_HEADERS["user-agent"],
    "referer": "https://www.tiktok.com/",
    "range": "bytes=0-1",
}
_VALID_MEDIA_CONTENT_TYPE_PREFIXES = ("image/", "video/")
_TIKTOK_MEDIA_URL_POLICY = MediaUrlSafetyPolicy(allowed_hosts_for_platform("tiktok"))


@dataclass
class TikTokMediaResolution:
    source: str | None = None
    media_urls: list[str] = field(default_factory=list)
    thumbnail_url: str | None = None
    author_avatar_url: str | None = None
    media_asset_meta: dict[str, Any] = field(default_factory=dict)
    attempts: list[dict[str, Any]] = field(default_factory=list)


def _build_attempt(
    *,
    source: str,
    success: bool,
    reason_code: str | None = None,
    http_status: int | None = None,
    content_type: str | None = None,
    probe_evidence: dict[str, Any] | None = None,
    selected_url_count: int = 0,
    error: Exception | None = None,
) -> dict[str, Any]:
    attempt = {
        "source": source,
        "success": bool(success),
        "reason_code": str(reason_code or "") or None,
        "http_status": int(http_status) if isinstance(http_status, int) else None,
        "content_type": str(content_type or "").strip() or None,
        "selected_url_count": max(0, int(selected_url_count or 0)),
        "error_type": error.__class__.__name__ if error else None,
        "error_message": (str(error)[:240] if error else None),
    }
    if probe_evidence:
        attempt["probe_evidence"] = {key: value for key, value in probe_evidence.items() if value is not None}
    return attempt


def _dedupe_urls(urls: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        normalized = str(url or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _normalize_content_type(value: Any) -> str | None:
    content_type = str(value or "").split(";", 1)[0].strip().lower()
    return content_type or None


def _is_media_content_type(content_type: str | None) -> bool:
    normalized = _normalize_content_type(content_type)
    return bool(normalized and normalized.startswith(_VALID_MEDIA_CONTENT_TYPE_PREFIXES))


def _probe_failure_reason(evidence: dict[str, Any]) -> str:
    status_code = int(evidence.get("http_status") or 0)
    if status_code in {200, 206} and not _is_media_content_type(str(evidence.get("content_type") or "")):
        return "download_bad_content_type"
    return "download_failed"


def _attach_probe_evidence(
    meta: dict[str, Any],
    *,
    media_url: str,
    probe_evidence: dict[str, Any] | None,
) -> dict[str, Any]:
    if not probe_evidence:
        return meta
    probe = {key: value for key, value in probe_evidence.items() if value is not None}
    probe["url"] = media_url
    enriched = dict(meta or {})
    existing_probes = list(enriched.get("probe_evidence") or [])
    existing_probes.append(probe)
    enriched["probe_evidence"] = existing_probes
    source_assets: list[dict[str, Any]] = []
    for asset in enriched.get("source_assets") or []:
        if not isinstance(asset, dict):
            continue
        normalized_asset = dict(asset)
        if str(normalized_asset.get("url") or "").strip() == media_url:
            normalized_asset["probe"] = probe
        source_assets.append(normalized_asset)
    if source_assets:
        enriched["source_assets"] = source_assets
    return enriched


def _build_video_asset_meta(
    media_url: str,
    thumbnail_url: str | None,
    *,
    probe_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    meta = {
        "selection_policy": "best_per_asset",
        "source_assets": [
            {
                "url": media_url,
                "type": "video",
                "width": None,
                "height": None,
                "resolution": None,
                "fps": None,
                "bitrate": None,
                "duration_seconds": None,
            }
        ],
        "thumbnail_source": (
            {"url": thumbnail_url, "type": "thumbnail", "width": None, "height": None, "resolution": None}
            if thumbnail_url
            else None
        ),
    }
    return _attach_probe_evidence(meta, media_url=media_url, probe_evidence=probe_evidence)


def _first_url(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        for item in value:
            candidate = _first_url(item)
            if candidate:
                return candidate
        return ""
    if isinstance(value, dict):
        for key in ("url", "playAddr", "downloadAddr", "cover", "dynamicCover"):
            candidate = _first_url(value.get(key))
            if candidate:
                return candidate
        for key in ("url_list", "UrlList"):
            candidate = _first_url(value.get(key))
            if candidate:
                return candidate
    return ""


def _find_ytdlp_cookie_file() -> str | None:
    import os
    from pathlib import Path

    path = (os.getenv("TIKTOK_COOKIES_NETSCAPE_FILE") or "").strip()
    if path:
        candidate = Path(path).expanduser()
        if candidate.is_file():
            return str(candidate)

    for env_key in ("TIKTOK_COOKIES_FILE", "SOCIAL_TIKTOK_COOKIES_FILE"):
        json_path = (os.getenv(env_key) or "").strip()
        if not json_path:
            continue
        candidate = Path(json_path).with_name(Path(json_path).stem + "_netscape.txt")
        if candidate.is_file():
            return str(candidate)

    default = Path("data/tiktok_cookies_netscape.txt")
    if default.is_file():
        return str(default)
    return None


def _parse_ytdlp_payload(payload: dict[str, Any]) -> tuple[list[str], str | None]:
    urls: list[str] = []
    primary_url = str(payload.get("url") or "").strip()
    if primary_url:
        urls.append(primary_url)

    best_requested_url = ""
    best_requested_score = (-1, -1)
    requested_formats = payload.get("requested_formats")
    if isinstance(requested_formats, list):
        for fmt in requested_formats:
            if not isinstance(fmt, dict):
                continue
            url = str(fmt.get("url") or "").strip()
            if not url:
                continue
            has_video = str(fmt.get("vcodec") or "").lower() not in {"", "none"}
            has_audio = str(fmt.get("acodec") or "").lower() not in {"", "none"}
            height = int(fmt.get("height") or 0)
            score = (1 if (has_video and has_audio) else (0 if has_video else -1), height)
            if score > best_requested_score:
                best_requested_score = score
                best_requested_url = url
    if best_requested_url:
        urls.append(best_requested_url)

    best_format_url = ""
    best_format_score = (-1, -1, -1)
    formats = payload.get("formats")
    if isinstance(formats, list):
        for fmt in formats:
            if not isinstance(fmt, dict):
                continue
            url = str(fmt.get("url") or "").strip()
            if not url:
                continue
            has_video = str(fmt.get("vcodec") or "").lower() not in {"", "none"}
            if not has_video:
                continue
            has_audio = str(fmt.get("acodec") or "").lower() not in {"", "none"}
            height = int(fmt.get("height") or 0)
            tbr = int(fmt.get("tbr") or 0)
            score = (1 if has_audio else 0, height, tbr)
            if score > best_format_score:
                best_format_score = score
                best_format_url = url
    if best_format_url:
        urls.append(best_format_url)

    thumbnail = str(payload.get("thumbnail") or "").strip() or None
    if not thumbnail:
        thumbs = payload.get("thumbnails")
        if isinstance(thumbs, list):
            best_thumb = ""
            best_width = -1
            for item in thumbs:
                if not isinstance(item, dict):
                    continue
                url = str(item.get("url") or "").strip()
                if not url:
                    continue
                width = int(item.get("width") or 0)
                if width >= best_width:
                    best_width = width
                    best_thumb = url
            thumbnail = best_thumb or None

    return _dedupe_urls(urls)[:1], thumbnail


def _resolve_with_ytdlp(video_url: str) -> tuple[list[str], str | None, dict[str, Any], dict[str, Any]]:
    try:
        video_url = validate_media_url(video_url, policy=_TIKTOK_MEDIA_URL_POLICY)
    except UnsafeMediaUrlError as exc:
        return (
            [],
            None,
            _build_attempt(
                source="yt_dlp_manifest",
                success=False,
                reason_code="tiktok_unsafe_video_url",
                selected_url_count=0,
                error=exc,
            ),
            {},
        )
    if not shutil.which("yt-dlp"):
        return (
            [],
            None,
            _build_attempt(
                source="yt_dlp_manifest",
                success=False,
                reason_code="tiktok_ytdlp_unavailable",
                selected_url_count=0,
            ),
            {},
        )

    cmd = [
        "yt-dlp",
        "--dump-single-json",
        "--no-playlist",
        "--skip-download",
        "--format",
        "best[ext=mp4]/best",
    ]
    cookie_file = _find_ytdlp_cookie_file()
    if cookie_file:
        cmd.extend(["--cookies", cookie_file])
    cmd.append(video_url)

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except Exception as exc:  # noqa: BLE001
        return (
            [],
            None,
            _build_attempt(
                source="yt_dlp_manifest",
                success=False,
                reason_code="tiktok_ytdlp_failed",
                selected_url_count=0,
                error=exc,
            ),
            {},
        )

    if proc.returncode != 0:
        message = (proc.stderr or proc.stdout or "").strip()
        error = RuntimeError(message[:240] if message else "yt-dlp exited non-zero")
        return (
            [],
            None,
            _build_attempt(
                source="yt_dlp_manifest",
                success=False,
                reason_code="tiktok_ytdlp_failed",
                selected_url_count=0,
                error=error,
            ),
            {},
        )

    try:
        payload = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError as exc:
        return (
            [],
            None,
            _build_attempt(
                source="yt_dlp_manifest",
                success=False,
                reason_code="tiktok_ytdlp_parse_failed",
                selected_url_count=0,
                error=exc,
            ),
            {},
        )

    media_urls, thumbnail_url = _parse_ytdlp_payload(payload)
    # Extract author avatar from yt-dlp metadata (TikTok exposes uploader_avatar).
    author_avatar_url = (
        str(
            payload.get("uploader_avatar")
            or payload.get("channel_thumbnail")
            or payload.get("channelAvatarUrl")
            or payload.get("author_avatar_url")
            or ""
        ).strip()
        or None
    )
    width = int(payload.get("width") or 0)
    height = int(payload.get("height") or 0)
    fps = int(payload.get("fps") or 0)
    bitrate = int(payload.get("tbr") or 0)
    duration_seconds = int(payload.get("duration") or 0) or None
    source_assets = []
    if media_urls:
        source_assets.append(
            {
                "url": media_urls[0],
                "type": "video",
                "width": width or None,
                "height": height or None,
                "resolution": f"{width}x{height}" if width > 0 and height > 0 else None,
                "fps": fps or None,
                "bitrate": bitrate or None,
                "duration_seconds": duration_seconds,
            }
        )
    thumbnail_meta = (
        {"url": thumbnail_url, "type": "thumbnail", "width": None, "height": None, "resolution": None}
        if thumbnail_url
        else None
    )
    return (
        media_urls,
        thumbnail_url,
        _build_attempt(
            source="yt_dlp_manifest",
            success=bool(media_urls),
            reason_code=(None if media_urls else "tiktok_media_not_found"),
            selected_url_count=len(media_urls),
        ),
        {
            "selection_policy": "best_per_asset",
            "source_assets": source_assets,
            "thumbnail_source": thumbnail_meta,
            "author_avatar_url": author_avatar_url,
        },
    )


def _extract_candidate_item(payload: Any, *, video_id: str) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        item_info = payload.get("itemInfo")
        if isinstance(item_info, dict):
            item_struct = item_info.get("itemStruct")
            if isinstance(item_struct, dict):
                candidate_id = str(item_struct.get("id") or item_struct.get("aweme_id") or "")
                # Accept when: no target id specified, OR candidate id matches.
                # Previously accepted `not candidate_id` which let malformed
                # payloads (missing id) masquerade as matches.
                if not video_id or (candidate_id and candidate_id == video_id):
                    return item_struct
        item_module = payload.get("ItemModule")
        if isinstance(item_module, dict):
            if video_id and isinstance(item_module.get(video_id), dict):
                return item_module.get(video_id)
            for value in item_module.values():
                if isinstance(value, dict) and "video" in value:
                    return value
        for value in payload.values():
            candidate = _extract_candidate_item(value, video_id=video_id)
            if candidate:
                return candidate
    if isinstance(payload, list):
        for value in payload:
            candidate = _extract_candidate_item(value, video_id=video_id)
            if candidate:
                return candidate
    return None


def _extract_urls_from_video_item(item: dict[str, Any]) -> tuple[list[str], str | None]:
    if not isinstance(item, dict):
        return [], None
    video = item.get("video")
    if not isinstance(video, dict):
        return [], None

    bitrate_rows: list[tuple[tuple[int, int, int], str]] = []
    for entry in video.get("bitrateInfo") or []:
        if not isinstance(entry, dict):
            continue
        play_addr = entry.get("PlayAddr")
        if not isinstance(play_addr, dict):
            continue
        url = _first_url(play_addr.get("UrlList"))
        if not url:
            continue
        height = int(play_addr.get("Height") or 0)
        width = int(play_addr.get("Width") or 0)
        bitrate = int(entry.get("Bitrate") or 0)
        bitrate_rows.append(((max(height, width), bitrate, play_addr.get("DataSize") or 0), url))
    bitrate_rows.sort(reverse=True)

    media_urls: list[str] = []
    if bitrate_rows:
        media_urls.append(bitrate_rows[0][1])
    for key in ("playAddr", "downloadAddr"):
        url = _first_url(video.get(key))
        if url:
            media_urls.append(url)
    media_urls = _dedupe_urls(media_urls)[:1]

    thumbnail_url = _first_url(video.get("cover")) or _first_url(video.get("dynamicCover")) or None
    return media_urls, thumbnail_url


def _parse_og_tags(html: str) -> tuple[str | None, str | None]:
    og_video_match = _OG_VIDEO_RE.search(html or "")
    og_image_match = _OG_IMAGE_RE.search(html or "")
    og_video = str(og_video_match.group(1)).strip() if og_video_match else None
    og_image = str(og_image_match.group(1)).strip() if og_image_match else None
    return og_video or None, og_image or None


def _probe_media_url(
    media_url: str,
    *,
    timeout: tuple[int, int],
) -> tuple[bool, dict[str, Any], Exception | None]:
    evidence: dict[str, Any] = {"url": media_url}
    try:
        response = safe_requests_get(
            requests,
            media_url,
            policy=_TIKTOK_MEDIA_URL_POLICY,
            headers=_PROBE_HEADERS,
            timeout=timeout,
            stream=True,
            allow_redirects=True,
        )
        status_code = int(response.status_code)
        content_type = _normalize_content_type((getattr(response, "headers", None) or {}).get("content-type"))
        evidence.update(
            {
                "http_status": status_code,
                "content_type": content_type,
                "content_type_valid": _is_media_content_type(content_type),
                "final_url": str(getattr(response, "url", "") or "").strip() or None,
            }
        )
        response.close()
        if status_code in {200, 206} and _is_media_content_type(content_type):
            return True, evidence, None
        return False, evidence, None
    except Exception as exc:  # noqa: BLE001
        status_code = int(getattr(getattr(exc, "response", None), "status_code", 0) or 0) or None
        evidence.update(
            {"http_status": status_code, "error_type": exc.__class__.__name__, "error_message": str(exc)[:240]}
        )
        return False, evidence, exc


def _resolve_with_unofficial_api(
    *,
    video_url: str,
    timeout: tuple[int, int],
) -> tuple[list[str], str | None, dict[str, Any]]:
    try:
        video_url = validate_media_url(video_url, policy=_TIKTOK_MEDIA_URL_POLICY)
        response = safe_requests_get(
            requests,
            "https://www.tikwm.com/api/",
            policy=_TIKTOK_MEDIA_URL_POLICY,
            params={"url": video_url, "hd": "1"},
            headers={"accept": "application/json", "user-agent": _DEFAULT_HEADERS["user-agent"]},
            timeout=timeout,
        )
        status_code = int(response.status_code)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # noqa: BLE001
        return (
            [],
            None,
            _build_attempt(
                source="unofficial_api",
                success=False,
                reason_code="tiktok_unofficial_failed",
                selected_url_count=0,
                error=exc,
            ),
        )

    data = payload.get("data") if isinstance(payload, dict) else None
    media_urls = _dedupe_urls(
        [
            _first_url((data or {}).get("hdplay")),
            _first_url((data or {}).get("play")),
            _first_url((data or {}).get("wmplay")),
        ]
    )
    thumbnail_url = (
        _first_url((data or {}).get("origin_cover"))
        or _first_url((data or {}).get("ai_dynamic_cover"))
        or _first_url((data or {}).get("cover"))
        or None
    )
    return (
        media_urls[:1],
        thumbnail_url,
        _build_attempt(
            source="unofficial_api",
            success=bool(media_urls),
            reason_code=(None if media_urls else "tiktok_media_not_found"),
            http_status=status_code,
            selected_url_count=len(media_urls[:1]),
        ),
    )


def _resolve_thumbnail_via_oembed(
    video_url: str,
    *,
    timeout: tuple[int, int] = (5, 10),
) -> tuple[str | None, dict[str, Any]]:
    """Call TikTok oEmbed API to get thumbnail URL.

    Returns (thumbnail_url, attempt_dict).  oEmbed is a lightweight official
    endpoint that reliably returns thumbnail URLs even when watch-page parsing
    or the unofficial API fails.
    """
    oembed_endpoint = "https://www.tiktok.com/oembed"
    try:
        video_url = validate_media_url(video_url, policy=_TIKTOK_MEDIA_URL_POLICY)
        resp = safe_requests_get(
            requests,
            oembed_endpoint,
            policy=_TIKTOK_MEDIA_URL_POLICY,
            params={"url": video_url},
            headers={"accept": "application/json", "user-agent": _DEFAULT_HEADERS["user-agent"]},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        thumb = str(data.get("thumbnail_url") or "").strip() or None
        return (
            thumb,
            _build_attempt(
                source="oembed",
                success=bool(thumb),
                reason_code=(None if thumb else "oembed_no_thumbnail"),
                http_status=int(resp.status_code),
                selected_url_count=(1 if thumb else 0),
            ),
        )
    except Exception as exc:  # noqa: BLE001
        return (
            None,
            _build_attempt(
                source="oembed",
                success=False,
                reason_code="oembed_failed",
                http_status=int(getattr(getattr(exc, "response", None), "status_code", 0) or 0) or None,
                selected_url_count=0,
                error=exc,
            ),
        )


def _extract_author_avatar_from_item(item: dict[str, Any] | None) -> str | None:
    """Pull the author avatar URL from a TikTok watch-page JSON item dict."""
    if not isinstance(item, dict):
        return None
    author = item.get("author") or {}
    if not isinstance(author, dict):
        author = {}
    author_meta = item.get("authorMeta") or {}
    if not isinstance(author_meta, dict):
        author_meta = {}
    url = (
        str(
            author.get("avatarUrl")
            or author.get("avatar")
            or author.get("originalAvatarUrl")
            or author.get("avatarLarger")
            or author.get("avatar_larger")
            or author.get("avatar_thumb")
            or author_meta.get("avatarUrl")
            or author_meta.get("avatar")
            or author_meta.get("avatarThumb")
            or author_meta.get("avatar_thumb")
            or item.get("avatarUrl")
            or item.get("avatar")
            or ""
        ).strip()
        or None
    )
    return url if url and url.startswith("http") else None


def resolve_tiktok_media(
    video_id: str,
    *,
    canonical_url: str | None = None,
    session: requests.Session | None = None,
    timeout: tuple[int, int] = (10, 45),
    allow_ytdlp: bool = True,
    validate_download_url: bool = False,
) -> TikTokMediaResolution:
    cleaned_video_id = str(video_id or "").strip()
    candidate_url = str(canonical_url or "").strip()
    if not candidate_url and cleaned_video_id:
        candidate_url = f"https://www.tiktok.com/@_/video/{cleaned_video_id}"

    resolution = TikTokMediaResolution(source=None, media_urls=[], thumbnail_url=None, media_asset_meta={}, attempts=[])
    if not candidate_url:
        resolution.attempts.append(
            _build_attempt(
                source="watch_page_json",
                success=False,
                reason_code="tiktok_media_not_found",
                selected_url_count=0,
            )
        )
        return resolution
    try:
        candidate_url = validate_media_url(candidate_url, policy=_TIKTOK_MEDIA_URL_POLICY)
    except UnsafeMediaUrlError as exc:
        resolution.attempts.append(
            _build_attempt(
                source="watch_page_json",
                success=False,
                reason_code="tiktok_unsafe_video_url",
                selected_url_count=0,
                error=exc,
            )
        )
        return resolution

    if allow_ytdlp:
        ytdlp_urls, ytdlp_thumb, ytdlp_attempt, ytdlp_meta = _resolve_with_ytdlp(candidate_url)
        resolution.attempts.append(ytdlp_attempt)
        if ytdlp_urls:
            if not validate_download_url:
                resolution.source = "yt_dlp_manifest"
                resolution.media_urls = ytdlp_urls
                resolution.thumbnail_url = ytdlp_thumb
                resolution.author_avatar_url = str(ytdlp_meta.get("author_avatar_url") or "").strip() or None
                resolution.media_asset_meta = ytdlp_meta
                return resolution
            ytdlp_ok, ytdlp_probe, ytdlp_error = _probe_media_url(ytdlp_urls[0], timeout=(5, 15))
            if ytdlp_ok:
                resolution.source = "yt_dlp_manifest"
                resolution.media_urls = ytdlp_urls
                resolution.thumbnail_url = ytdlp_thumb
                resolution.author_avatar_url = str(ytdlp_meta.get("author_avatar_url") or "").strip() or None
                resolution.media_asset_meta = _attach_probe_evidence(
                    ytdlp_meta,
                    media_url=ytdlp_urls[0],
                    probe_evidence=ytdlp_probe,
                )
                return resolution
            resolution.attempts.append(
                _build_attempt(
                    source="yt_dlp_manifest_probe",
                    success=False,
                    reason_code=_probe_failure_reason(ytdlp_probe),
                    http_status=int(ytdlp_probe.get("http_status") or 0) or None,
                    content_type=str(ytdlp_probe.get("content_type") or "") or None,
                    probe_evidence=ytdlp_probe,
                    selected_url_count=0,
                    error=ytdlp_error,
                )
            )
    else:
        resolution.attempts.append(
            _build_attempt(
                source="yt_dlp_manifest",
                success=False,
                reason_code="tiktok_ytdlp_skipped",
                selected_url_count=0,
            )
        )

    client = session or requests.Session()
    try:
        response = safe_requests_get(
            client,
            candidate_url,
            policy=_TIKTOK_MEDIA_URL_POLICY,
            headers=_DEFAULT_HEADERS,
            timeout=timeout,
        )
        status_code = int(response.status_code)
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        resolution.attempts.append(
            _build_attempt(
                source="watch_page_json",
                success=False,
                reason_code="tiktok_watch_page_failed",
                http_status=int(getattr(getattr(exc, "response", None), "status_code", 0) or 0) or None,
                selected_url_count=0,
                error=exc,
            )
        )
        unofficial_urls, unofficial_thumb, unofficial_attempt = _resolve_with_unofficial_api(
            video_url=candidate_url,
            timeout=timeout,
        )
        resolution.attempts.append(unofficial_attempt)
        if unofficial_urls:
            resolution.source = "unofficial_api"
            resolution.media_urls = unofficial_urls
            resolution.thumbnail_url = unofficial_thumb
            resolution.media_asset_meta = _build_video_asset_meta(unofficial_urls[0], unofficial_thumb)
            return resolution
        # Watch page and unofficial API both failed; try oEmbed for thumbnail.
        if not resolution.thumbnail_url and candidate_url:
            oembed_thumb, oembed_attempt = _resolve_thumbnail_via_oembed(candidate_url)
            resolution.attempts.append(oembed_attempt)
            if oembed_thumb:
                resolution.thumbnail_url = oembed_thumb
        return resolution

    html = str(response.text or "")
    item = None
    for pattern in (_REHYDRATION_RE, _SIGI_STATE_RE):
        match = pattern.search(html)
        if not match:
            continue
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        item = _extract_candidate_item(payload, video_id=cleaned_video_id)
        if item:
            break

    if item:
        # Capture author avatar from watch-page JSON if available.
        if not resolution.author_avatar_url:
            resolution.author_avatar_url = _extract_author_avatar_from_item(item)
        media_urls, thumbnail_url = _extract_urls_from_video_item(item)
        resolution.attempts.append(
            _build_attempt(
                source="watch_page_json",
                success=bool(media_urls),
                reason_code=(None if media_urls else "tiktok_media_not_found"),
                http_status=status_code,
                selected_url_count=len(media_urls),
            )
        )
        if media_urls:
            if not validate_download_url:
                resolution.source = "watch_page_json"
                resolution.media_urls = media_urls
                resolution.thumbnail_url = thumbnail_url
                resolution.media_asset_meta = _build_video_asset_meta(media_urls[0], thumbnail_url)
                return resolution
            watch_ok, watch_probe, watch_error = _probe_media_url(media_urls[0], timeout=(5, 15))
            if watch_ok:
                resolution.source = "watch_page_json"
                resolution.media_urls = media_urls
                resolution.thumbnail_url = thumbnail_url
                resolution.media_asset_meta = _build_video_asset_meta(
                    media_urls[0],
                    thumbnail_url,
                    probe_evidence=watch_probe,
                )
                return resolution
            resolution.attempts.append(
                _build_attempt(
                    source="watch_page_json_probe",
                    success=False,
                    reason_code=_probe_failure_reason(watch_probe),
                    http_status=int(watch_probe.get("http_status") or 0) or None,
                    content_type=str(watch_probe.get("content_type") or "") or None,
                    probe_evidence=watch_probe,
                    selected_url_count=0,
                    error=watch_error,
                )
            )
    else:
        resolution.attempts.append(
            _build_attempt(
                source="watch_page_json",
                success=False,
                reason_code="tiktok_watch_parse_failed",
                http_status=status_code,
                selected_url_count=0,
            )
        )

    unofficial_urls, unofficial_thumb, unofficial_attempt = _resolve_with_unofficial_api(
        video_url=candidate_url,
        timeout=timeout,
    )
    resolution.attempts.append(unofficial_attempt)
    if unofficial_urls:
        if not validate_download_url:
            resolution.source = "unofficial_api"
            resolution.media_urls = unofficial_urls
            resolution.thumbnail_url = unofficial_thumb
            resolution.media_asset_meta = _build_video_asset_meta(unofficial_urls[0], unofficial_thumb)
            return resolution
        unofficial_ok, unofficial_probe, unofficial_error = _probe_media_url(unofficial_urls[0], timeout=(5, 15))
        if unofficial_ok:
            resolution.source = "unofficial_api"
            resolution.media_urls = unofficial_urls
            resolution.thumbnail_url = unofficial_thumb
            resolution.media_asset_meta = _build_video_asset_meta(
                unofficial_urls[0],
                unofficial_thumb,
                probe_evidence=unofficial_probe,
            )
            return resolution
        resolution.attempts.append(
            _build_attempt(
                source="unofficial_api_probe",
                success=False,
                reason_code=_probe_failure_reason(unofficial_probe),
                http_status=int(unofficial_probe.get("http_status") or 0) or None,
                content_type=str(unofficial_probe.get("content_type") or "") or None,
                probe_evidence=unofficial_probe,
                selected_url_count=0,
                error=unofficial_error,
            )
        )

    og_video, og_image = _parse_og_tags(html)
    if og_video:
        resolution.source = "og_fallback"
        resolution.media_urls = [og_video]
        resolution.thumbnail_url = og_image
        resolution.media_asset_meta = {
            "selection_policy": "best_per_asset",
            "source_assets": [
                {
                    "url": og_video,
                    "type": "video",
                    "width": None,
                    "height": None,
                    "resolution": None,
                    "fps": None,
                    "bitrate": None,
                    "duration_seconds": None,
                }
            ],
            "thumbnail_source": (
                {"url": og_image, "type": "thumbnail", "width": None, "height": None, "resolution": None}
                if og_image
                else None
            ),
        }
        resolution.attempts.append(
            _build_attempt(
                source="og_fallback",
                success=True,
                reason_code=None,
                http_status=status_code,
                selected_url_count=1,
            )
        )
        return resolution

    resolution.thumbnail_url = og_image
    resolution.attempts.append(
        _build_attempt(
            source="og_fallback",
            success=False,
            reason_code="tiktok_media_not_found",
            http_status=status_code,
            selected_url_count=0,
        )
    )

    # Last resort: try oEmbed for a thumbnail when everything else failed.
    if not resolution.thumbnail_url and candidate_url:
        oembed_thumb, oembed_attempt = _resolve_thumbnail_via_oembed(candidate_url)
        resolution.attempts.append(oembed_attempt)
        if oembed_thumb:
            resolution.thumbnail_url = oembed_thumb

    return resolution
