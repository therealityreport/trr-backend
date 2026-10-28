"""TikTok comment parsing: comment records and comment media URL extraction."""

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from trr_backend.socials.tiktok.constants import URL_RE
from trr_backend.socials.tiktok.models import TikTokComment, _dict_or_empty


class CommentParsingMixin:
    """Parse TikTok comment payloads into ``TikTokComment`` records."""

    if TYPE_CHECKING:
        # Sibling-mixin helpers supplied by ``TikTokScraper``'s other parsing mixins.
        @staticmethod
        def _coerce_timestamp(value: Any) -> int: ...
        @staticmethod
        def _extract_video_id_from_url(url: str | None) -> str: ...
        def _pick_best_avatar_url(self, *candidates: Any) -> str | None: ...

    def _parse_comment(
        self,
        data: dict,
        video_id: str,
        post_url: str,
        is_reply: bool = False,
        parent_id: str | None = None,
    ) -> TikTokComment:
        """Parse comment data into TikTokComment object."""
        created_at = self._coerce_timestamp(
            data.get("create_time") or data.get("createTime") or data.get("createTimeISO") or data.get("timestamp")
        )
        user = _dict_or_empty(data.get("user"))
        resolved_post_url = str(post_url or data.get("videoWebUrl") or "").strip()
        resolved_video_id = str(
            video_id or data.get("aweme_id") or data.get("awemeId") or data.get("item_id") or data.get("itemId") or ""
        ).strip()
        if not resolved_video_id:
            resolved_video_id = self._extract_video_id_from_url(resolved_post_url)
        comment_id = str(data.get("cid") or data.get("id") or "").strip()
        parent_source_comment_id = (
            str(data.get("parentId") or data.get("parent_id") or data.get("parentCid") or parent_id or "").strip()
            or None
        )
        aweme_id = str(data.get("awemeId") or data.get("aweme_id") or resolved_video_id or "").strip() or None
        comment_language = str(data.get("commentLanguage") or data.get("comment_language") or "").strip() or None
        is_author_liked_raw = data.get("isAuthorLiked")
        is_author_liked = bool(is_author_liked_raw) if isinstance(is_author_liked_raw, (bool, int)) else None
        media_urls = self._extract_comment_media_urls(data, user=user)

        parsed_replies: list[TikTokComment] = []
        for row in data.get("replies") or []:
            if not isinstance(row, dict):
                continue
            parsed_replies.append(
                self._parse_comment(
                    row,
                    resolved_video_id,
                    resolved_post_url,
                    is_reply=True,
                    parent_id=comment_id or parent_id,
                )
            )

        reply_count = int(
            data.get("reply_comment_total")
            or data.get("replyCommentTotal")
            or data.get("replyCount")
            or data.get("repliesCount")
            or 0
        )
        if parsed_replies and reply_count <= 0:
            reply_count = len(parsed_replies)

        profile_username = (
            str(
                user.get("unique_id")
                or user.get("uniqueId")
                or data.get("uniqueId")
                or data.get("ownerUsername")
                or user.get("username")
                or ""
            )
            .strip()
            .lstrip("@")
        )

        return TikTokComment(
            comment_id=comment_id,
            text=str(data.get("text") or ""),
            username=profile_username,
            user_id=str(user.get("uid") or data.get("uid") or user.get("id") or ""),
            nickname=str(user.get("nickname") or user.get("nickName") or data.get("nickname") or ""),
            created_at=created_at,
            date_time=(datetime.fromtimestamp(created_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S") if created_at else ""),
            likes=int(data.get("digg_count") or data.get("diggCount") or data.get("likesCount") or 0),
            is_reply=is_reply,
            parent_comment_id=parent_id,
            reply_count=reply_count,
            replies=parsed_replies,
            avatar_thumbnail_url=(
                str(
                    data.get("avatarThumbnail")
                    or user.get("avatar_thumb")
                    or user.get("avatar_thumb_url")
                    or user.get("avatarLarger")
                    or user.get("avatar_larger")
                    or ""
                ).strip()
                or None
            ),
            comment_language=comment_language,
            is_author_liked=is_author_liked,
            aweme_id=aweme_id,
            parent_source_comment_id=parent_source_comment_id,
            user_url=(
                str(user.get("url") or user.get("profileUrl") or "").strip()
                or (f"https://www.tiktok.com/@{profile_username}" if profile_username else None)
            ),
            user_bio=(str(user.get("bio") or user.get("signature") or "").strip() or None),
            user_avatar_url=self._pick_best_avatar_url(
                user.get("avatarLarger"),
                user.get("avatar_larger"),
                user.get("originalAvatarUrl"),
                user.get("avatarThumb"),
                user.get("avatar_thumb"),
                user.get("avatar_thumb_url"),
                user.get("avatarUrl"),
                user.get("avatar"),
                data.get("avatarThumbnail"),
                data.get("avatarUrl"),
                data.get("avatar"),
            ),
            user_region=(str(user.get("region") or "").strip() or None),
            user_language=(str(user.get("language") or user.get("lang") or "").strip() or None),
            media_urls=media_urls,
            video_id=resolved_video_id,
            post_url=resolved_post_url,
        )

    def _extract_comment_media_urls(self, item: dict[str, Any], *, user: dict[str, Any] | None = None) -> list[str]:
        if not isinstance(item, dict):
            return []

        avatar_candidates = {
            str(value or "").strip()
            for value in (
                item.get("avatarThumbnail"),
                (user or {}).get("avatarThumbnail"),
                (user or {}).get("avatar_thumb"),
                (user or {}).get("avatar_thumb_url"),
                (user or {}).get("avatarUrl"),
                (user or {}).get("avatar"),
                (user or {}).get("originalAvatarUrl"),
                (user or {}).get("avatarLarger"),
                (user or {}).get("avatar_larger"),
            )
            if str(value or "").strip()
        }

        candidates: list[str] = []
        media_keys = (
            "media",
            "medias",
            "mediaUrls",
            "media_urls",
            "images",
            "image",
            "videos",
            "video",
            "gif",
            "gifs",
            "attachments",
            "attachment",
            "stickers",
            "sticker",
            "resource",
            "resources",
        )
        for key in media_keys:
            self._collect_candidate_urls(item.get(key), candidates)

        text = str(item.get("text") or "")
        for matched in URL_RE.findall(text):
            cleaned = matched.rstrip(".,!?;:)]}\"'")
            if cleaned:
                candidates.append(cleaned)

        deduped: list[str] = []
        for candidate in candidates:
            normalized = str(candidate or "").strip()
            if not normalized or normalized in deduped:
                continue
            if normalized in avatar_candidates or self._looks_like_avatar_url(normalized):
                continue
            if not self._is_allowed_comment_media_url(normalized):
                continue
            deduped.append(normalized)
        return deduped

    def _collect_candidate_urls(self, value: Any, collector: list[str]) -> None:
        if value is None:
            return
        if isinstance(value, str):
            normalized = value.strip()
            if normalized.startswith("http://") or normalized.startswith("https://"):
                collector.append(normalized)
            return
        if isinstance(value, list):
            for item in value:
                self._collect_candidate_urls(item, collector)
            return
        if isinstance(value, dict):
            for key in (
                "url",
                "src",
                "uri",
                "image",
                "imageUrl",
                "image_url",
                "gifUrl",
                "gif_url",
                "videoUrl",
                "video_url",
                "playAddr",
                "downloadAddr",
                "play_url",
                "download_url",
                "url_list",
                "UrlList",
            ):
                self._collect_candidate_urls(value.get(key), collector)
            return

    @staticmethod
    def _is_allowed_comment_media_url(url: str) -> bool:
        parsed = urlparse(url)
        host = str(parsed.netloc or "").lower()
        path = str(parsed.path or "").lower()
        if "tiktokcdn" not in host:
            return False
        if any(path.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".mov", ".webm")):
            return True
        return any(token in path for token in ("image-origin", "image", "gif", "video"))

    @staticmethod
    def _looks_like_avatar_url(url: str) -> bool:
        parsed = urlparse(url)
        path = str(parsed.path or "").lower()
        return any(token in path for token in ("avt-", "/avatar", "profile_pic", "profile"))


__all__ = [
    "CommentParsingMixin",
]
