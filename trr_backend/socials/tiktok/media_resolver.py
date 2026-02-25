"""Resolve direct TikTok media URLs for backend mirroring jobs."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from typing import Any

import requests

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


@dataclass
class TikTokMediaResolution:
    source: str | None = None
    media_urls: list[str] = field(default_factory=list)
    thumbnail_url: str | None = None
    attempts: list[dict[str, Any]] = field(default_factory=list)


def _build_attempt(
    *,
    source: str,
    success: bool,
    reason_code: str | None = None,
    http_status: int | None = None,
    selected_url_count: int = 0,
    error: Exception | None = None,
) -> dict[str, Any]:
    return {
        "source": source,
        "success": bool(success),
        "reason_code": str(reason_code or "") or None,
        "http_status": int(http_status) if isinstance(http_status, int) else None,
        "selected_url_count": max(0, int(selected_url_count or 0)),
        "error_type": error.__class__.__name__ if error else None,
        "error_message": (str(error)[:240] if error else None),
    }


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


def _resolve_with_ytdlp(video_url: str) -> tuple[list[str], str | None, dict[str, Any]]:
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
        )

    media_urls, thumbnail_url = _parse_ytdlp_payload(payload)
    return (
        media_urls,
        thumbnail_url,
        _build_attempt(
            source="yt_dlp_manifest",
            success=bool(media_urls),
            reason_code=(None if media_urls else "tiktok_media_not_found"),
            selected_url_count=len(media_urls),
        ),
    )


def _extract_candidate_item(payload: Any, *, video_id: str) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        item_info = payload.get("itemInfo")
        if isinstance(item_info, dict):
            item_struct = item_info.get("itemStruct")
            if isinstance(item_struct, dict):
                candidate_id = str(item_struct.get("id") or item_struct.get("aweme_id") or "")
                if not video_id or not candidate_id or candidate_id == video_id:
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
) -> tuple[bool, int | None, Exception | None]:
    try:
        response = requests.get(
            media_url,
            headers=_PROBE_HEADERS,
            timeout=timeout,
            stream=True,
            allow_redirects=True,
        )
        status_code = int(response.status_code)
        response.close()
        if status_code in {200, 206}:
            return True, status_code, None
        return False, status_code, None
    except Exception as exc:  # noqa: BLE001
        status_code = int(getattr(getattr(exc, "response", None), "status_code", 0) or 0) or None
        return False, status_code, exc


def _resolve_with_unofficial_api(
    *,
    video_url: str,
    timeout: tuple[int, int],
) -> tuple[list[str], str | None, dict[str, Any]]:
    try:
        response = requests.get(
            "https://www.tikwm.com/api/",
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

    resolution = TikTokMediaResolution(source=None, media_urls=[], thumbnail_url=None, attempts=[])
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

    if allow_ytdlp:
        ytdlp_urls, ytdlp_thumb, ytdlp_attempt = _resolve_with_ytdlp(candidate_url)
        resolution.attempts.append(ytdlp_attempt)
        if ytdlp_urls:
            if not validate_download_url:
                resolution.source = "yt_dlp_manifest"
                resolution.media_urls = ytdlp_urls
                resolution.thumbnail_url = ytdlp_thumb
                return resolution
            ytdlp_ok, ytdlp_status, ytdlp_error = _probe_media_url(ytdlp_urls[0], timeout=(5, 15))
            if ytdlp_ok:
                resolution.source = "yt_dlp_manifest"
                resolution.media_urls = ytdlp_urls
                resolution.thumbnail_url = ytdlp_thumb
                return resolution
            resolution.attempts.append(
                _build_attempt(
                    source="yt_dlp_manifest_probe",
                    success=False,
                    reason_code="download_failed",
                    http_status=ytdlp_status,
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
        response = client.get(candidate_url, headers=_DEFAULT_HEADERS, timeout=timeout)
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
            return resolution
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
                return resolution
            watch_ok, watch_status, watch_error = _probe_media_url(media_urls[0], timeout=(5, 15))
            if watch_ok:
                resolution.source = "watch_page_json"
                resolution.media_urls = media_urls
                resolution.thumbnail_url = thumbnail_url
                return resolution
            resolution.attempts.append(
                _build_attempt(
                    source="watch_page_json_probe",
                    success=False,
                    reason_code="download_failed",
                    http_status=watch_status,
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
            return resolution
        unofficial_ok, unofficial_status, unofficial_error = _probe_media_url(unofficial_urls[0], timeout=(5, 15))
        if unofficial_ok:
            resolution.source = "unofficial_api"
            resolution.media_urls = unofficial_urls
            resolution.thumbnail_url = unofficial_thumb
            return resolution
        resolution.attempts.append(
            _build_attempt(
                source="unofficial_api_probe",
                success=False,
                reason_code="download_failed",
                http_status=unofficial_status,
                selected_url_count=0,
                error=unofficial_error,
            )
        )

    og_video, og_image = _parse_og_tags(html)
    if og_video:
        resolution.source = "og_fallback"
        resolution.media_urls = [og_video]
        resolution.thumbnail_url = og_image
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
    return resolution
