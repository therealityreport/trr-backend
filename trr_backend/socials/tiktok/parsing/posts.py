"""TikTok post-item parsing: metrics, media URLs, hashtags, mentions, avatars, and embedded HTML data."""

import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from trr_backend.socials.tiktok.models import TikTokPost, TikTokScrapeConfig, _dict_or_empty

# Patterns for extracting embedded JSON from TikTok HTML pages.
REHYDRATION_RE = re.compile(
    r'<script\s+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>',
    re.DOTALL,
)
SIGI_STATE_RE = re.compile(
    r'<script\s+id="SIGI_STATE"[^>]*>(.*?)</script>',
    re.DOTALL,
)


class PostParsingMixin:
    """Parse TikTok post items into ``TikTokPost`` records."""

    # Pattern for extracting embedded JSON from TikTok HTML pages.
    _REHYDRATION_RE = REHYDRATION_RE
    _SIGI_STATE_RE = SIGI_STATE_RE

    @staticmethod
    def _coerce_timestamp(value: Any) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return 0
            if raw.isdigit():
                return int(raw)
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                return 0
            return int(parsed.timestamp())
        return 0

    @staticmethod
    def _safe_int_metric(value: Any) -> int:
        if isinstance(value, bool):
            return 0
        if isinstance(value, (int, float)):
            return max(0, int(float(value)))
        raw = str(value or "").strip()
        if not raw:
            return 0
        compact = raw.replace(",", "").replace("_", "").strip()
        if not compact:
            return 0
        suffix = compact[-1].upper()
        numeric_portion = compact
        multiplier = 1.0
        if suffix in {"K", "M", "B"}:
            numeric_portion = compact[:-1].strip()
            multiplier = {"K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0}[suffix]
        try:
            return max(0, int(float(numeric_portion) * multiplier))
        except ValueError:
            return 0

    @staticmethod
    def _dedupe_preserve_order(values: list[str]) -> list[str]:
        output: list[str] = []
        seen: set[str] = set()
        for value in values:
            token = str(value or "").strip()
            if not token:
                continue
            key = token.lower()
            if key in seen:
                continue
            seen.add(key)
            output.append(token)
        return output

    @staticmethod
    def _extract_video_id_from_url(url: str | None) -> str:
        value = str(url or "").strip()
        if not value:
            return ""
        path = urlparse(value).path
        match = re.search(r"/video/(\d+)", path)
        return match.group(1) if match else ""

    @staticmethod
    def _normalize_hashtag_token(value: Any) -> str | None:
        token = str(value or "").strip().lstrip("#")
        if not token:
            return None
        match = re.search(r"[A-Za-z0-9_]+", token)
        return match.group(0) if match else None

    @staticmethod
    def _normalize_mention_token(value: Any) -> str | None:
        token = str(value or "").strip().lstrip("@")
        if not token:
            return None
        match = re.search(r"[A-Za-z0-9._]+", token)
        if not match:
            return None
        normalized = match.group(0).rstrip(".,:;!?)]}>'\"")
        return normalized if normalized else None

    def _extract_posts_from_html_data(self, data: dict, username: str) -> tuple[list[dict], str | None]:
        """Extract post items from HTML-embedded JSON data."""
        items: list[dict] = []
        sec_uid = None

        # __UNIVERSAL_DATA_FOR_REHYDRATION__ format
        default_scope = data.get("__DEFAULT_SCOPE__", {})
        webapp_detail = default_scope.get("webapp.user-detail", {})
        user_info = webapp_detail.get("userInfo", {})
        if user_info:
            user = user_info.get("user", {})
            sec_uid = user.get("secUid")
            # itemList lives directly under userInfo (not under a "post" sub-key)
            item_list = user_info.get("itemList", [])
            if item_list:
                items = item_list

        # Fallback: SIGI_STATE / ItemModule format
        if not items:
            item_module = data.get("ItemModule", {})
            if item_module and isinstance(item_module, dict):
                items = list(item_module.values())

        if not sec_uid:
            user_module = data.get("UserModule", {}).get("users", {})
            user_data = user_module.get(username, {})
            sec_uid = user_data.get("secUid")

        return items, sec_uid

    def _extract_hashtags(self, text: str) -> list[str]:
        """Extract hashtags from text."""
        values: list[str] = []
        for token in re.findall(r"#([A-Za-z0-9_]+)", text or ""):
            normalized = self._normalize_hashtag_token(token)
            if normalized:
                values.append(normalized)
        return self._dedupe_preserve_order(values)

    def _extract_mentions(self, text: str) -> list[str]:
        """Extract @mentions from text."""
        values: list[str] = []
        for token in re.findall(r"@([A-Za-z0-9._]+)", text or ""):
            normalized = self._normalize_mention_token(token)
            if normalized:
                values.append(normalized)
        return self._dedupe_preserve_order(values)

    def _extract_structured_hashtags(self, item: dict) -> list[str]:
        hashtags: list[str] = []
        for row in item.get("hashtags") or []:
            value = row
            if isinstance(row, dict):
                value = row.get("name") or row.get("hashtagName")
            normalized = self._normalize_hashtag_token(value)
            if normalized:
                hashtags.append(normalized)
        return self._dedupe_preserve_order(hashtags)

    def _extract_structured_mentions(self, item: dict) -> list[str]:
        mentions: list[str] = []
        for key in ("mentions", "detailedMentions"):
            for row in item.get(key) or []:
                value = row
                if isinstance(row, dict):
                    value = row.get("name") or row.get("username") or row.get("uniqueId")
                normalized = self._normalize_mention_token(value)
                if normalized:
                    mentions.append(normalized)
        return self._dedupe_preserve_order(mentions)

    def _parse_post_item(self, item: dict, config: TikTokScrapeConfig) -> TikTokPost:
        """Parse a post item into TikTokPost."""
        video_id = str(
            item.get("id")
            or item.get("aweme_id")
            or item.get("videoId")
            or self._extract_video_id_from_url(item.get("webVideoUrl") or item.get("url"))
            or ""
        )
        create_time = self._coerce_timestamp(item.get("createTime") or item.get("createTimeISO"))
        description = str(item.get("desc") or item.get("text") or "")
        media_urls = self._extract_media_urls(item)
        thumbnail_url = self._extract_thumbnail_url(item)

        # Author info
        author = _dict_or_empty(item.get("author"))
        author_meta = _dict_or_empty(item.get("authorMeta"))
        username = str(
            author.get("uniqueId")
            or author.get("unique_id")
            or author_meta.get("name")
            or author_meta.get("uniqueId")
            or config.username
        )
        nickname = str(author.get("nickname") or author_meta.get("nickName") or author_meta.get("nickname") or "")
        user_avatar_url = self._pick_best_avatar_url(
            author.get("avatarLarger"),
            author.get("avatar_larger"),
            author.get("originalAvatarUrl"),
            author.get("avatarUrl"),
            author.get("avatar"),
            author.get("avatar_thumb"),
            author.get("avatar_thumb_url"),
            author_meta.get("avatarLarger"),
            author_meta.get("avatar_larger"),
            author_meta.get("avatarUrl"),
            author_meta.get("avatar"),
            author_meta.get("avatarThumb"),
            author_meta.get("avatar_thumb"),
            item.get("avatarUrl"),
            item.get("avatar"),
        )

        # Stats
        stats = _dict_or_empty(item.get("stats"))
        stats_v2 = _dict_or_empty(item.get("statsV2"))

        # Music info
        music = _dict_or_empty(item.get("music"))
        music_meta = _dict_or_empty(item.get("musicMeta"))

        # Video info
        video = _dict_or_empty(item.get("video"))
        video_meta = _dict_or_empty(item.get("videoMeta"))
        duration = int(video.get("duration") or video_meta.get("duration") or 0)
        hashtags = self._dedupe_preserve_order(
            [*self._extract_structured_hashtags(item), *self._extract_hashtags(description)]
        )
        mentions = self._dedupe_preserve_order(
            [*self._extract_structured_mentions(item), *self._extract_mentions(description)]
        )
        canonical_url = str(item.get("webVideoUrl") or item.get("url") or "").strip() or (
            f"https://www.tiktok.com/@{username}/video/{video_id}" if video_id else ""
        )

        return TikTokPost(
            video_id=video_id,
            date_time=datetime.fromtimestamp(create_time, tz=UTC).strftime("%Y-%m-%d %H:%M:%S") if create_time else "",
            create_time=create_time,
            description=description,
            hashtags=hashtags,
            mentions=mentions,
            likes=self._safe_int_metric(
                stats.get("diggCount")
                or stats.get("digg_count")
                or stats_v2.get("diggCount")
                or stats_v2.get("digg_count")
                or item.get("diggCount")
                or item.get("digg_count")
                or 0
            ),
            comments=self._safe_int_metric(
                stats.get("commentCount")
                or stats.get("comment_count")
                or stats_v2.get("commentCount")
                or stats_v2.get("comment_count")
                or item.get("commentCount")
                or item.get("comment_count")
                or 0
            ),
            shares=self._safe_int_metric(
                stats.get("shareCount")
                or stats.get("share_count")
                or stats_v2.get("shareCount")
                or stats_v2.get("share_count")
                or item.get("shareCount")
                or item.get("share_count")
                or 0
            ),
            saves=self._safe_int_metric(
                stats.get("collectCount")
                or stats.get("collect_count")
                or stats.get("favoriteCount")
                or stats.get("favorite_count")
                or stats_v2.get("collectCount")
                or stats_v2.get("collect_count")
                or stats_v2.get("favoriteCount")
                or stats_v2.get("favorite_count")
                or item.get("collectCount")
                or item.get("collect_count")
                or item.get("favoriteCount")
                or item.get("favorite_count")
                or 0
            ),
            views=self._safe_int_metric(
                stats.get("playCount")
                or stats.get("play_count")
                or stats_v2.get("playCount")
                or stats_v2.get("play_count")
                or item.get("playCount")
                or item.get("play_count")
                or 0
            ),
            url=canonical_url,
            username=username,
            author_nickname=nickname,
            duration=duration,
            music_title=str(music.get("title") or music_meta.get("musicName") or music_meta.get("title") or ""),
            music_author=str(
                music.get("authorName")
                or music.get("author_name")
                or music_meta.get("musicAuthor")
                or music_meta.get("authorName")
                or ""
            ),
            user_avatar_url=user_avatar_url,
            media_urls=media_urls,
            thumbnail_url=thumbnail_url or (media_urls[0] if media_urls else None),
            show_id=config.show_id,
            season_number=config.season_number,
            person_id=config.person_id,
        )

    def _extract_media_urls(self, item: dict) -> list[str]:
        """Extract playable video URLs from a post."""
        urls: list[str] = []

        video = item.get("video") if isinstance(item.get("video"), dict) else {}
        video_meta = item.get("videoMeta") if isinstance(item.get("videoMeta"), dict) else {}

        for payload in (video, video_meta):
            if not payload:
                continue
            best_bitrate_url = self._extract_best_bitrate_video_url(payload)
            if best_bitrate_url:
                urls.append(best_bitrate_url)
            for key in ("playAddr", "downloadAddr", "playUrl", "url"):
                candidate = self._extract_url_value(payload.get(key))
                if candidate and candidate not in urls:
                    urls.append(candidate)

        for row in item.get("mediaUrls") or []:
            candidate = self._extract_url_value(row)
            if candidate and candidate not in urls:
                urls.append(candidate)

        deduped: list[str] = []
        for url in urls:
            normalized = str(url or "").strip()
            if normalized and normalized not in deduped:
                deduped.append(normalized)
        return deduped[:1]

    @staticmethod
    def _avatar_url_quality_score(url: str) -> tuple[int, int]:
        normalized = str(url or "").strip().lower()
        if not normalized:
            return (-1, -1)
        score = 0
        if any(
            token in normalized
            for token in ("avatarlarger", "avatar_larger", "originalavatarurl", "cropcenter:1080", "profile_pic")
        ):
            score += 40
        if any(token in normalized for token in ("avatarthumbnail", "avatar_thumb", "thumbnail", "small", "tiny")):
            score -= 25
        max_dim = 0
        for match in re.finditer(r"(?:s)?(\d{2,4})x(\d{2,4})", normalized):
            max_dim = max(max_dim, int(match.group(1)), int(match.group(2)))
        for match in re.finditer(r"cropcenter:(\d{2,4}):(\d{2,4})", normalized):
            max_dim = max(max_dim, int(match.group(1)), int(match.group(2)))
        return (score + min(max_dim, 4096), len(normalized))

    def _pick_best_avatar_url(self, *candidates: Any) -> str | None:
        best_url: str | None = None
        best_score: tuple[int, int] = (-1, -1)
        for candidate in candidates:
            value = str(candidate or "").strip()
            if not value.startswith(("http://", "https://")):
                continue
            score = self._avatar_url_quality_score(value)
            if score > best_score:
                best_score = score
                best_url = value
        return best_url

    @staticmethod
    def _extract_best_bitrate_video_url(video: dict[str, Any]) -> str | None:
        rows: list[tuple[tuple[int, int, int], str]] = []
        for entry in video.get("bitrateInfo") or []:
            if not isinstance(entry, dict):
                continue
            play_addr = entry.get("PlayAddr")
            if not isinstance(play_addr, dict):
                continue
            url_list = play_addr.get("UrlList")
            if not isinstance(url_list, list) or not url_list:
                continue
            url = str(url_list[0] or "").strip()
            if not url:
                continue
            height = int(play_addr.get("Height") or 0)
            width = int(play_addr.get("Width") or 0)
            bitrate = int(entry.get("Bitrate") or 0)
            rows.append(((max(height, width), bitrate, int(play_addr.get("DataSize") or 0)), url))
        rows.sort(reverse=True)
        return rows[0][1] if rows else None

    @staticmethod
    def _extract_url_value(value: Any) -> str:
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            for item in value:
                candidate = PostParsingMixin._extract_url_value(item)
                if candidate:
                    return candidate
            return ""
        if isinstance(value, dict):
            for key in ("url", "playAddr", "downloadAddr"):
                candidate = PostParsingMixin._extract_url_value(value.get(key))
                if candidate:
                    return candidate
            for key in ("url_list", "UrlList"):
                candidate = PostParsingMixin._extract_url_value(value.get(key))
                if candidate:
                    return candidate
        return ""

    @staticmethod
    def _extract_thumbnail_url(item: dict[str, Any]) -> str | None:
        video = item.get("video") if isinstance(item.get("video"), dict) else {}
        video_meta = item.get("videoMeta") if isinstance(item.get("videoMeta"), dict) else {}

        for payload, keys in (
            (video, ("cover", "dynamicCover")),
            (video_meta, ("coverUrl", "originalCoverUrl", "cover", "dynamicCover")),
        ):
            if not payload:
                continue
            for key in keys:
                candidate = PostParsingMixin._extract_url_value(payload.get(key))
                if candidate:
                    return candidate

        for key in ("coverUrl", "originalCoverUrl", "thumbnail", "thumbnailUrl"):
            candidate = PostParsingMixin._extract_url_value(item.get(key))
            if candidate:
                return candidate
        return None


__all__ = [
    "PostParsingMixin",
    "REHYDRATION_RE",
    "SIGI_STATE_RE",
]
