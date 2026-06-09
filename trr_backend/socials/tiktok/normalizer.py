"""Route-facing TikTok response normalizers.

Direct preview/scrape payloads stay separate from ``posts_scrapling`` claimed
jobs. TikTok comments remain outside the persisted backend comments contract.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def post_to_route_response(post: Any) -> dict[str, Any]:
    """Convert a TikTokPost-like object to the current admin route response shape."""

    return {
        "video_id": post.video_id,
        "date_time": post.date_time,
        "description": post.description,
        "hashtags": post.hashtags,
        "mentions": post.mentions,
        "likes": post.likes,
        "comments": post.comments,
        "shares": post.shares,
        "views": post.views,
        "url": post.url,
        "username": post.username,
        "author_nickname": post.author_nickname,
        "duration": post.duration,
        "music_title": post.music_title,
        "music_author": post.music_author,
    }


def profile_preview_to_route_response(
    user_data: Mapping[str, Any],
    stats: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the current TikTok profile preview response shape."""

    return {
        "username": user_data.get("uniqueId"),
        "nickname": user_data.get("nickname"),
        "bio": user_data.get("signature"),
        "is_verified": user_data.get("verified", False),
        "is_private": user_data.get("privateAccount", False),
        "followers": stats.get("followerCount", 0),
        "following": stats.get("followingCount", 0),
        "likes": stats.get("heartCount", stats.get("heart", 0)),
        "video_count": stats.get("videoCount", 0),
        "profile_pic_url": user_data.get("avatarLarger") or user_data.get("avatarMedium"),
    }
