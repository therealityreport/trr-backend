"""Nullable public observations. Never infer a measurement from an absent field."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any


@dataclass
class PublicPost:
    tweet_id: str
    text: str
    username: str
    url: str
    date_time: str | None = None
    created_at: int | None = None
    display_name: str | None = None
    user_id: str | None = None
    user_profile_url: str | None = None
    user_verified: bool | None = None
    likes: int | None = None
    retweets: int | None = None
    replies: int | None = None
    quotes: int | None = None
    views: int | None = None
    bookmarks: int | None = None
    shares: int | None = None
    is_reply: bool | None = None
    is_retweet: bool | None = None
    is_quote: bool | None = None
    reply_to_tweet_id: str | None = None
    quoted_tweet_id: str | None = None
    thread_root_tweet_id: str | None = None
    thread_position: int | None = None
    is_thread_part: bool | None = None
    twitter_context_role: str | None = None
    long_text: bool | None = None
    language: str | None = None
    quote: dict | None = None
    reposted_by: Any = None
    hashtags: list[str] = field(default_factory=list)
    mentions: list[str] = field(default_factory=list)
    media_urls: list[str] = field(default_factory=list)
    hosted_media_urls: list[str] = field(default_factory=list)
    media: list[dict] = field(default_factory=list)
    evidence: dict = field(default_factory=dict)

    def __post_init__(self):
        for name in ("tweet_id", "reply_to_tweet_id", "quoted_tweet_id", "thread_root_tweet_id", "user_id"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not re.fullmatch(r"[0-9]{2,20}", value)):
                raise ValueError("Invalid public ID")
        if (
            not isinstance(self.text, str)
            or not isinstance(self.username, str)
            or not re.fullmatch(r"[A-Za-z0-9_]{1,15}", self.username)
        ):
            raise ValueError("Invalid public text or author")
        for name in (
            "likes",
            "retweets",
            "replies",
            "quotes",
            "views",
            "bookmarks",
            "shares",
            "created_at",
            "thread_position",
        ):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("Invalid public numeric field")
        for name in ("user_verified", "is_reply", "is_quote", "is_retweet", "is_thread_part", "long_text"):
            if getattr(self, name) is not None and type(getattr(self, name)) is not bool:
                raise ValueError("Invalid public boolean field")
        for name in ("display_name", "language", "date_time", "twitter_context_role"):
            if getattr(self, name) is not None and not isinstance(getattr(self, name), str):
                raise ValueError("Invalid public optional text")
        timestamp(self.date_time)
        for name in ("hashtags", "mentions", "media_urls", "hosted_media_urls"):
            value = getattr(self, name)
            if not isinstance(value, list) or any(not isinstance(x, str) for x in value):
                raise ValueError("Invalid public array")
        if (
            not isinstance(self.media, list)
            or any(not isinstance(x, dict) for x in self.media)
            or not isinstance(self.evidence, dict)
        ):
            raise ValueError("Invalid public media/evidence")

    def to_dict(self) -> dict:
        return asdict(self)


def timestamp(value: str | None) -> datetime | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("Invalid public timestamp")
    try:
        date = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        date = parsedate_to_datetime(value)
    if date.tzinfo is None:
        raise ValueError("Public timestamp requires timezone")
    return date.astimezone(UTC)


def normalize(
    post: Any, *, root_id: str | None = None, role: str | None = None, evidence: dict | None = None
) -> PublicPost:
    if not isinstance(post, dict):
        raise ValueError("Invalid mirror post")
    author = post.get("author")
    if (
        not isinstance(post.get("id"), str)
        or not re.fullmatch(r"[0-9]{2,20}", post["id"])
        or not isinstance(post.get("text"), str)
        or not isinstance(author, dict)
        or not isinstance(author.get("screen_name"), str)
        or not re.fullmatch(r"[A-Za-z0-9_]{1,15}", author["screen_name"])
    ):
        raise ValueError("Invalid mirror post identity or text")
    parent = post.get("replying_to")
    if parent is not None and not isinstance(parent, dict):
        raise ValueError("Invalid mirror parent")
    parent_id = parent.get("status") if parent else None
    if parent_id is not None and (not isinstance(parent_id, str) or not re.fullmatch(r"[0-9]{2,20}", parent_id)):
        raise ValueError("Invalid mirror parent ID")
    date = timestamp(post.get("created_at"))

    def count(key: str) -> int | None:
        value = post.get(key)
        return value if type(value) is int and value >= 0 else None

    quote = post.get("quote")
    if quote is not None and not isinstance(quote, dict):
        raise ValueError("Invalid mirror quote")
    media = []
    raw_media = post.get("media") or {}
    if not isinstance(raw_media, dict):
        raise ValueError("Invalid mirror media")
    for key in ("photos", "videos"):
        values = raw_media.get(key) or []
        if not isinstance(values, list):
            raise ValueError("Invalid mirror media")
        for item in values:
            if not isinstance(item, dict):
                raise ValueError("Invalid mirror media")
            if isinstance(item.get("url"), str):
                media.append({"url": item["url"], "role": "image" if key == "photos" else "video-source"})
            if key == "videos":
                if item.get("thumbnail_url"):
                    media.append({"url": item["thumbnail_url"], "role": "video-poster"})
                for variant in item.get("variants") or []:
                    if isinstance(variant, dict) and isinstance(variant.get("url"), str):
                        media.append({"url": variant["url"], "role": "video-source"})
    username = author["screen_name"]
    return PublicPost(
        tweet_id=post["id"],
        text=post["text"],
        username=username,
        url=f"https://x.com/{username}/status/{post['id']}",
        date_time=date.isoformat() if date else None,
        created_at=int(date.timestamp()) if date else None,
        display_name=author.get("name") if isinstance(author.get("name"), str) else None,
        user_id=str(author["id"]) if author.get("id") else None,
        user_profile_url=f"https://x.com/{username}",
        user_verified=author.get("verified") if type(author.get("verified")) is bool else None,
        likes=count("likes"),
        retweets=count("reposts"),
        replies=count("replies"),
        quotes=count("quotes"),
        views=count("views"),
        bookmarks=count("bookmarks"),
        shares=None,
        is_reply=True if parent_id else None,
        is_retweet=True if post.get("reposted_by") else None,
        is_quote=True if quote else None,
        reply_to_tweet_id=parent_id,
        quoted_tweet_id=quote.get("id") if quote and isinstance(quote.get("id"), str) else None,
        quote=quote,
        reposted_by=post.get("reposted_by"),
        thread_root_tweet_id=root_id,
        is_thread_part=True if role == "thread" else None,
        twitter_context_role=role,
        long_text=post.get("is_note_tweet") if type(post.get("is_note_tweet")) is bool else None,
        language=post.get("lang") if isinstance(post.get("lang"), str) else None,
        hashtags=re.findall(r"#(\w+)", post["text"]),
        mentions=re.findall(r"@([A-Za-z0-9_]+)", post["text"]),
        media=media,
        media_urls=list(dict.fromkeys(m["url"] for m in media if m["role"] != "video-poster")),
        evidence={"provider": "fxtwitter", **(evidence or {})},
    )


def conversation_posts(
    data: dict, *, root_id: str, capability: str = "conversation", evidence: dict | None = None
) -> list[PublicPost]:
    """Shared root/author-thread/reply parser; roles never imply parent edges."""
    if not isinstance(data.get("status"), dict) or data["status"].get("id") != root_id:
        raise ValueError("Mismatched public post ID")
    posts = [
        normalize(
            data["status"],
            root_id=root_id if capability != "status" else None,
            role="root" if capability != "status" else None,
            evidence=evidence,
        )
    ]
    for key in () if capability == "status" else ("thread", "replies") if capability == "conversation" else ("thread",):
        if not isinstance(data.get(key), list):
            raise ValueError("Malformed conversation branch")
        posts.extend(
            normalize(row, root_id=root_id, role="reply" if key == "replies" else "thread", evidence=evidence)
            for row in data[key]
        )
    # First occurrence wins: root, then author thread, then audience replies.
    unique = {}
    for post in posts:
        unique.setdefault(post.tweet_id, post)
    return list(unique.values())
