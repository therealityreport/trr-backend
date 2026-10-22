"""yt-dlp payload parsing shared by the TikTok scraper transport and the media resolver."""

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trr_backend.socials.tiktok.constants import (
    ENV_SOCIAL_TIKTOK_COOKIES_FILE,
    ENV_TIKTOK_COOKIES_FILE,
    ENV_TIKTOK_COOKIES_NETSCAPE_FILE,
    TIKTOK_DEFAULT_NETSCAPE_COOKIE_PATH,
)
from trr_backend.socials.tiktok.models import TikTokPost, TikTokScrapeConfig


def find_ytdlp_cookie_file() -> str | None:
    """Find a Netscape-format cookie file for yt-dlp."""
    # Check env var first
    path = (os.getenv(ENV_TIKTOK_COOKIES_NETSCAPE_FILE) or "").strip()
    if path:
        p = Path(path).expanduser()
        if p.is_file():
            return str(p)

    # Auto-detect alongside the JSON cookie file
    for env_key in (ENV_TIKTOK_COOKIES_FILE, ENV_SOCIAL_TIKTOK_COOKIES_FILE):
        json_path = (os.getenv(env_key) or "").strip()
        if json_path:
            netscape = Path(json_path).with_name(Path(json_path).stem + "_netscape.txt")
            if netscape.is_file():
                return str(netscape)

    # Default location
    default = Path(TIKTOK_DEFAULT_NETSCAPE_COOKIE_PATH)
    if default.is_file():
        return str(default)

    return None


def extract_ytdlp_video_urls(data: dict) -> list[str]:
    urls: list[str] = []

    primary_url = str(data.get("url") or "").strip()
    if primary_url:
        urls.append(primary_url)

    best_requested_url = ""
    best_requested_score = (-1, -1)
    requested_formats = data.get("requested_formats")
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
    formats = data.get("formats")
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

    deduped: list[str] = []
    for url in urls:
        if url and url not in deduped:
            deduped.append(url)
    return deduped[:1]


def extract_ytdlp_thumbnail_url(data: dict) -> str | None:
    thumbnail = str(data.get("thumbnail") or "").strip()
    if thumbnail:
        return thumbnail
    thumbnails = data.get("thumbnails")
    if not isinstance(thumbnails, list):
        return None
    best_url = ""
    best_width = -1
    for entry in thumbnails:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url") or "").strip()
        if not url:
            continue
        width = int(entry.get("width") or 0)
        if width >= best_width:
            best_width = width
            best_url = url
    return best_url or None


def parse_ytdlp_payload(payload: dict[str, Any]) -> tuple[list[str], str | None]:
    """Return ``(media_urls, thumbnail_url)`` from a yt-dlp ``--dump-json`` payload."""
    return extract_ytdlp_video_urls(payload), extract_ytdlp_thumbnail_url(payload)


class YtDlpParsingMixin:
    """Convert yt-dlp metadata into ``TikTokPost`` records."""

    def _parse_ytdlp_metadata(self, data: dict, config: TikTokScrapeConfig) -> TikTokPost:
        """Convert yt-dlp JSON metadata into a TikTokPost."""
        description = data.get("description", "") or data.get("title", "")
        create_time = data.get("timestamp", 0)
        username = data.get("uploader", config.username)
        video_id = data.get("id", "")
        media_urls = self._extract_ytdlp_video_urls(data)
        thumbnail_url = self._extract_ytdlp_thumbnail_url(data)
        user_avatar_url = self._pick_best_avatar_url(
            data.get("uploader_avatar"),
            data.get("channel_thumbnail"),
            data.get("channelAvatarUrl"),
            data.get("author_avatar_url"),
        )

        return TikTokPost(
            video_id=video_id,
            date_time=(
                datetime.fromtimestamp(create_time, tz=UTC).strftime("%Y-%m-%d %H:%M:%S") if create_time else ""
            ),
            create_time=create_time,
            description=description,
            hashtags=self._extract_hashtags(description),
            mentions=self._extract_mentions(description),
            likes=self._safe_int_metric(data.get("like_count", 0) or 0),
            comments=self._safe_int_metric(data.get("comment_count", 0) or 0),
            shares=self._safe_int_metric(data.get("repost_count", 0) or 0),
            saves=self._safe_int_metric(data.get("collect_count", 0) or data.get("save_count", 0) or 0),
            views=self._safe_int_metric(data.get("view_count", 0) or 0),
            url=f"https://www.tiktok.com/@{username}/video/{video_id}" if video_id else "",
            username=username,
            author_nickname=data.get("channel", ""),
            duration=data.get("duration", 0) or 0,
            music_title=data.get("track", ""),
            music_author=data.get("artist", ""),
            user_avatar_url=user_avatar_url,
            media_urls=media_urls,
            thumbnail_url=thumbnail_url or (media_urls[0] if media_urls else None),
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    @staticmethod
    def _extract_ytdlp_video_urls(data: dict) -> list[str]:
        return extract_ytdlp_video_urls(data)

    @staticmethod
    def _extract_ytdlp_thumbnail_url(data: dict) -> str | None:
        return extract_ytdlp_thumbnail_url(data)


__all__ = [
    "YtDlpParsingMixin",
    "extract_ytdlp_thumbnail_url",
    "extract_ytdlp_video_urls",
    "find_ytdlp_cookie_file",
    "parse_ytdlp_payload",
]
