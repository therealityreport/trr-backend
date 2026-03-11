"""Resolve direct YouTube media URLs for backend mirroring jobs."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs

import requests

_PLAYER_RESPONSE_MARKERS = (
    "ytInitialPlayerResponse =",
    "var ytInitialPlayerResponse =",
    "window['ytInitialPlayerResponse'] =",
    'window["ytInitialPlayerResponse"] =',
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


@dataclass
class YouTubeMediaResolution:
    source: str | None = None
    media_urls: list[str] = field(default_factory=list)
    thumbnail_url: str | None = None
    media_asset_meta: dict[str, Any] = field(default_factory=dict)
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


def _extract_json_object_after_marker(text: str, marker: str) -> str | None:
    marker_idx = text.find(marker)
    if marker_idx < 0:
        return None
    start = text.find("{", marker_idx + len(marker))
    if start < 0:
        return None

    depth = 0
    in_string = False
    escaped = False
    for idx in range(start, len(text)):
        char = text[idx]
        if in_string:
            if escaped:
                escaped = False
                continue
            if char == "\\":
                escaped = True
                continue
            if char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            continue
        if char == "{":
            depth += 1
            continue
        if char == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
    return None


def _extract_player_response(text: str) -> dict[str, Any] | None:
    for marker in _PLAYER_RESPONSE_MARKERS:
        payload = _extract_json_object_after_marker(text, marker)
        if not payload:
            continue
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            return data
    return None


def _url_from_cipher(cipher: str | None) -> str:
    if not cipher:
        return ""
    try:
        parsed = parse_qs(cipher, keep_blank_values=True)
        value = (parsed.get("url") or [""])[0]
        return str(value or "").strip()
    except Exception:
        return ""


def _stream_has_audio(fmt: dict[str, Any], mime_type: str) -> bool:
    if fmt.get("audioQuality"):
        return True
    codecs = str(fmt.get("codecs") or "")
    if not codecs:
        mime_match = re.search(r'codecs="([^"]+)"', mime_type)
        codecs = mime_match.group(1) if mime_match else ""
    codecs_value = codecs.lower()
    return any(token in codecs_value for token in ("mp4a", "opus", "vorbis", "aac"))


def _stream_has_video(fmt: dict[str, Any], mime_type: str) -> bool:
    if fmt.get("height") or fmt.get("width"):
        return True
    return mime_type.lower().startswith("video/")


def _pick_best_stream_url(player_response: dict[str, Any]) -> tuple[list[str], str | None, dict[str, Any]]:
    streaming_data = player_response.get("streamingData")
    if not isinstance(streaming_data, dict):
        return [], None, {}

    formats: list[dict[str, Any]] = []
    for key in ("formats", "adaptiveFormats"):
        value = streaming_data.get(key)
        if isinstance(value, list):
            formats.extend(item for item in value if isinstance(item, dict))

    ranked: list[tuple[tuple[int, int, int, int, int], str, dict[str, Any]]] = []
    for fmt in formats:
        url = str(fmt.get("url") or "").strip() or _url_from_cipher(
            str(fmt.get("signatureCipher") or fmt.get("cipher") or "")
        )
        if not url:
            continue
        mime_type = str(fmt.get("mimeType") or "")
        has_video = _stream_has_video(fmt, mime_type)
        if not has_video:
            continue
        has_audio = _stream_has_audio(fmt, mime_type)
        height = int(fmt.get("height") or 0)
        width = int(fmt.get("width") or 0)
        fps = int(fmt.get("fps") or 0)
        bitrate = int(fmt.get("bitrate") or fmt.get("averageBitrate") or 0)
        ranked.append(
            (
                (1 if has_audio else 0, height, width, fps, bitrate),
                url,
                {
                    "url": url,
                    "type": "video",
                    "width": width or None,
                    "height": height or None,
                    "resolution": f"{width}x{height}" if width > 0 and height > 0 else None,
                    "fps": fps or None,
                    "bitrate": bitrate or None,
                    "duration_seconds": int(player_response.get("videoDetails", {}).get("lengthSeconds") or 0) or None,
                    "has_audio": bool(has_audio),
                },
            )
        )

    ranked.sort(reverse=True)
    media_urls: list[str] = []
    source_assets: list[dict[str, Any]] = []
    if ranked:
        media_urls = [ranked[0][1]]
        source_assets = [ranked[0][2]]

    thumbnail_url = None
    thumbnail_meta: dict[str, Any] | None = None
    video_details = player_response.get("videoDetails")
    if isinstance(video_details, dict):
        thumb = video_details.get("thumbnail")
        if isinstance(thumb, dict):
            thumbs = thumb.get("thumbnails")
            if isinstance(thumbs, list):
                best = None
                best_width = -1
                best_height = 0
                for item in thumbs:
                    if not isinstance(item, dict):
                        continue
                    width = int(item.get("width") or 0)
                    if width >= best_width:
                        best_width = width
                        best_height = int(item.get("height") or 0)
                        best = str(item.get("url") or "").strip() or None
                thumbnail_url = best
                if best:
                    thumbnail_meta = {
                        "url": best,
                        "type": "thumbnail",
                        "width": best_width or None,
                        "height": best_height or None,
                        "resolution": f"{best_width}x{best_height}" if best_width > 0 and best_height > 0 else None,
                    }

    meta = {
        "selection_policy": "best_per_asset",
        "source_assets": source_assets,
        "thumbnail_source": thumbnail_meta,
    }
    return media_urls, thumbnail_url, meta


def _parse_og_tags(html: str) -> tuple[str | None, str | None]:
    og_video_match = _OG_VIDEO_RE.search(html or "")
    og_image_match = _OG_IMAGE_RE.search(html or "")
    og_video = str(og_video_match.group(1)).strip() if og_video_match else None
    og_image = str(og_image_match.group(1)).strip() if og_image_match else None
    return og_video or None, og_image or None


def _resolve_with_ytdlp(video_url: str) -> tuple[list[str], str | None, dict[str, Any], dict[str, Any]]:
    if not shutil.which("yt-dlp"):
        return (
            [],
            None,
            _build_attempt(
                source="yt_dlp_manifest",
                success=False,
                reason_code="youtube_ytdlp_unavailable",
                selected_url_count=0,
            ),
            {},
        )

    try:
        proc = subprocess.run(
            [
                "yt-dlp",
                "--dump-single-json",
                "--no-playlist",
                "--skip-download",
                "--format",
                "best[acodec!=none][vcodec!=none]/best",
                video_url,
            ],
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
                reason_code="youtube_ytdlp_failed",
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
                reason_code="youtube_ytdlp_failed",
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
                reason_code="youtube_ytdlp_parse_failed",
                selected_url_count=0,
                error=exc,
            ),
            {},
        )

    media_url = str(payload.get("url") or "").strip()
    media_urls = [media_url] if media_url else []
    selected_asset_meta: dict[str, Any] | None = None
    duration_seconds = int(payload.get("duration") or 0) or None
    thumbnail = str(payload.get("thumbnail") or "").strip() or None

    if not media_urls:
        requested_formats = payload.get("requested_formats")
        if isinstance(requested_formats, list):
            best_url = ""
            best_score = (-1, -1)
            best_meta: dict[str, Any] | None = None
            for item in requested_formats:
                if not isinstance(item, dict):
                    continue
                url = str(item.get("url") or "").strip()
                if not url:
                    continue
                has_video = str(item.get("vcodec") or "").lower() not in {"none", ""}
                has_audio = str(item.get("acodec") or "").lower() not in {"none", ""}
                height = int(item.get("height") or 0)
                score = (1 if (has_video and has_audio) else (0 if has_video else -1), height)
                if score > best_score:
                    best_score = score
                    best_url = url
                    best_meta = {
                        "url": best_url,
                        "type": "video",
                        "width": int(item.get("width") or 0) or None,
                        "height": height or None,
                        "resolution": (
                            f"{int(item.get('width') or 0)}x{height}"
                            if int(item.get("width") or 0) > 0 and height > 0
                            else None
                        ),
                        "fps": int(item.get("fps") or 0) or None,
                        "bitrate": int(item.get("tbr") or 0) or None,
                        "duration_seconds": duration_seconds,
                        "has_audio": bool(has_audio),
                    }
            if best_url:
                media_urls = [best_url]
                selected_asset_meta = best_meta

    if media_urls and selected_asset_meta is None:
        selected_asset_meta = {
            "url": media_urls[0],
            "type": "video",
            "width": int(payload.get("width") or 0) or None,
            "height": int(payload.get("height") or 0) or None,
            "resolution": (
                f"{int(payload.get('width') or 0)}x{int(payload.get('height') or 0)}"
                if int(payload.get("width") or 0) > 0 and int(payload.get("height") or 0) > 0
                else None
            ),
            "fps": int(payload.get("fps") or 0) or None,
            "bitrate": int(payload.get("tbr") or 0) or None,
            "duration_seconds": duration_seconds,
            "has_audio": str(payload.get("acodec") or "").lower() not in {"none", ""},
        }

    thumbnail_meta = (
        {
            "url": thumbnail,
            "type": "thumbnail",
            "width": int(payload.get("thumbnail_width") or 0) or None,
            "height": int(payload.get("thumbnail_height") or 0) or None,
            "resolution": (
                f"{int(payload.get('thumbnail_width') or 0)}x{int(payload.get('thumbnail_height') or 0)}"
                if int(payload.get("thumbnail_width") or 0) > 0 and int(payload.get("thumbnail_height") or 0) > 0
                else None
            ),
        }
        if thumbnail
        else None
    )

    return (
        media_urls,
        thumbnail,
        _build_attempt(
            source="yt_dlp_manifest",
            success=bool(media_urls),
            reason_code=(None if media_urls else "youtube_media_not_found"),
            selected_url_count=len(media_urls),
        ),
        {
            "selection_policy": "best_per_asset",
            "source_assets": [selected_asset_meta] if selected_asset_meta else [],
            "thumbnail_source": thumbnail_meta,
        },
    )


def resolve_youtube_media(
    video_id: str,
    *,
    session: requests.Session | None = None,
    timeout: tuple[int, int] = (10, 45),
) -> YouTubeMediaResolution:
    cleaned_video_id = str(video_id or "").strip()
    if not cleaned_video_id:
        return YouTubeMediaResolution(
            source=None,
            media_urls=[],
            thumbnail_url=None,
            media_asset_meta={},
            attempts=[
                _build_attempt(
                    source="watch_page_streaming_data",
                    success=False,
                    reason_code="youtube_media_not_found",
                    selected_url_count=0,
                )
            ],
        )

    watch_url = f"https://www.youtube.com/watch?v={cleaned_video_id}"
    resolution = YouTubeMediaResolution(
        source=None, media_urls=[], thumbnail_url=None, media_asset_meta={}, attempts=[]
    )

    ytdlp_urls, ytdlp_thumbnail, ytdlp_attempt, ytdlp_meta = _resolve_with_ytdlp(watch_url)
    resolution.attempts.append(ytdlp_attempt)
    if ytdlp_urls:
        resolution.source = "yt_dlp_manifest"
        resolution.media_urls = ytdlp_urls
        resolution.thumbnail_url = ytdlp_thumbnail
        resolution.media_asset_meta = ytdlp_meta
        return resolution

    client = session or requests.Session()
    try:
        response = client.get(watch_url, headers=_DEFAULT_HEADERS, timeout=timeout)
        status_code = int(response.status_code)
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        resolution.attempts.append(
            _build_attempt(
                source="watch_page_streaming_data",
                success=False,
                reason_code="youtube_watch_page_failed",
                http_status=(int(getattr(getattr(exc, "response", None), "status_code", 0) or 0) or None),
                selected_url_count=0,
                error=exc,
            )
        )
        resolution.attempts.append(
            _build_attempt(
                source="og_fallback",
                success=False,
                reason_code="youtube_media_not_found",
                selected_url_count=0,
            )
        )
        return resolution

    html = str(response.text or "")
    player_response = _extract_player_response(html)
    if player_response:
        media_urls, thumbnail_url, source_meta = _pick_best_stream_url(player_response)
        resolution.attempts.append(
            _build_attempt(
                source="watch_page_streaming_data",
                success=bool(media_urls),
                reason_code=(None if media_urls else "youtube_media_not_found"),
                http_status=status_code,
                selected_url_count=len(media_urls),
            )
        )
        if media_urls:
            resolution.source = "watch_page_streaming_data"
            resolution.media_urls = media_urls
            resolution.thumbnail_url = thumbnail_url
            resolution.media_asset_meta = source_meta
            return resolution
    else:
        resolution.attempts.append(
            _build_attempt(
                source="watch_page_streaming_data",
                success=False,
                reason_code="youtube_watch_parse_failed",
                http_status=status_code,
                selected_url_count=0,
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
                    "has_audio": None,
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
            reason_code="youtube_media_not_found",
            http_status=status_code,
            selected_url_count=0,
        )
    )
    return resolution
