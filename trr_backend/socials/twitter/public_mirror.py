"""Opt-in anonymous conversation reads from the external FxTwitter mirror.

No TRR credentials are loaded or sent. The mirror's own upstream authentication
is unknown. Exhausting its cursor does not establish complete X thread coverage.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import requests

if TYPE_CHECKING:
    from trr_backend.socials.twitter.scraper import Tweet

_ENDPOINT = "https://api.fxtwitter.com/2/conversation"
_MAX_RESPONSE_BYTES = 2_000_000


@dataclass
class PublicMirrorConversation:
    tweets: list[Tweet] = field(default_factory=list)
    reply_ids: set[str] = field(default_factory=set)
    pages: list[dict[str, Any]] = field(default_factory=list)
    stop_reason: str = "max_pages"
    error: str | None = None
    next_cursor: str | None = None

    def coverage(self) -> dict[str, Any]:
        return {
            "provider": "fxtwitter",
            "retrieval_mode": "public_conversation_mirror",
            "pages_scanned": len(self.pages),
            "posts_checked": len(self.tweets),
            "stop_reason": self.stop_reason,
            "pagination_exhausted": self.stop_reason == "no_cursor",
            "thread_completeness": "unknown",
            "complete": False,
            "upstream_authentication": "unknown; provider may use accounts",
            "authentication_provided": False,
            "external_dependency": True,
            "paid_services": False,
            "parentage": "provider-reported; not independently verified",
            "optional_fields": "provider-reported; absent metrics default to zero",
            "pages": self.pages,
        }


def _fetch_page(url: str) -> bytes:
    # Ignore ambient .netrc credentials and proxy configuration. Never redirect
    # a conversation request to a login or another provider.
    with requests.Session() as session:
        session.trust_env = False
        with session.get(
            url,
            headers={"Accept": "application/json", "User-Agent": "TRR-public-conversation/1.0"},
            timeout=(10, 20),
            allow_redirects=False,
            stream=True,
        ) as response:
            if response.status_code != 200:
                raise ValueError(f"Mirror HTTP {response.status_code}")
            body = bytearray()
            for chunk in response.iter_content(chunk_size=65536):
                body.extend(chunk)
                if len(body) > _MAX_RESPONSE_BYTES:
                    raise ValueError("Mirror response exceeds size limit")
            return bytes(body)


def _to_tweet(post: Any, *, root_id: str, role: str) -> Tweet:
    from trr_backend.socials.twitter.scraper import Tweet

    if not isinstance(post, dict):
        raise ValueError("Invalid mirror post")
    post_id, text, author = post.get("id"), post.get("text"), post.get("author")
    if (
        not isinstance(post_id, str)
        or not re.fullmatch(r"[0-9]{2,20}", post_id)
        or not isinstance(text, str)
        or not isinstance(author, dict)
        or not isinstance(author.get("screen_name"), str)
        or not re.fullmatch(r"[A-Za-z0-9_]{1,15}", author["screen_name"])
    ):
        raise ValueError("Invalid mirror post identity or text")
    username = author["screen_name"]
    parent = post.get("replying_to")
    if parent is not None and not isinstance(parent, dict):
        raise ValueError("Invalid mirror parent")
    parent_id = parent.get("status") if isinstance(parent, dict) else None
    if parent_id is not None and (not isinstance(parent_id, str) or not re.fullmatch(r"[0-9]{2,20}", parent_id)):
        raise ValueError("Invalid mirror parent ID")
    date_time = post.get("created_at") or ""
    if not isinstance(date_time, str):
        raise ValueError("Invalid mirror post date")
    try:
        created_at = int(parsedate_to_datetime(date_time).timestamp()) if date_time else 0
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("Invalid mirror post date") from exc

    def count(key: str) -> int:
        value = post.get(key)
        return max(0, value) if isinstance(value, int) and not isinstance(value, bool) else 0

    return Tweet(
        tweet_id=post_id,
        date_time=date_time,
        created_at=created_at,
        text=text,
        hashtags=re.findall(r"#(\w+)", text),
        mentions=re.findall(r"@([A-Za-z0-9_]+)", text),
        likes=count("likes"),
        retweets=count("retweets"),
        replies=count("replies"),
        quotes=count("quote_count"),
        views=count("views"),
        url=f"https://x.com/{username}/status/{post_id}",
        username=username,
        display_name=author.get("name") if isinstance(author.get("name"), str) else username,
        user_verified=author.get("verified") is True,
        is_reply=parent_id is not None,
        is_retweet=False,
        is_quote=isinstance(post.get("quote"), dict),
        reply_to_tweet_id=parent_id,
        user_id=str(author["id"]) if author.get("id") else None,
        user_profile_url=f"https://x.com/{username}",
        thread_root_tweet_id=root_id,
        is_thread_part=role == "thread",
        twitter_context_role=role,
    )


def fetch_public_conversation(tweet_id: str, *, max_pages: int = 3, max_items: int = 100) -> PublicMirrorConversation:
    """Read bounded status/thread/replies pages; retain partial results on error.

    Limits count all unique posts, including the requested status. There are no
    retries, search fallback, credentials, persistence, or media downloads.
    """
    if not isinstance(tweet_id, str) or not re.fullmatch(r"[0-9]{2,20}", tweet_id):
        raise ValueError("tweet_id must contain 2-20 digits")
    if type(max_pages) is not int or not 1 <= max_pages <= 10:
        raise ValueError("max_pages must be 1-10")
    if type(max_items) is not int or not 1 <= max_items <= 1000:
        raise ValueError("max_items must be 1-1000")
    result = PublicMirrorConversation()
    seen_ids: set[str] = set()
    seen_cursors: set[str] = set()
    for _ in range(max_pages):
        url = f"{_ENDPOINT}/{tweet_id}"
        if result.next_cursor:
            url += "?" + urlencode({"cursor": result.next_cursor})
        try:
            body = _fetch_page(url)
            data = json.loads(body)
            if (
                not isinstance(data, dict)
                or data.get("code") != 200
                or not isinstance(data.get("status"), dict)
                or not isinstance(data.get("thread"), list)
                or not isinstance(data.get("replies"), list)
            ):
                raise ValueError("Invalid mirror conversation payload")
            if data["status"].get("id") != tweet_id:
                raise ValueError("Mirror returned a different status")
            cursor = data.get("cursor")
            if cursor is not None and not isinstance(cursor, dict):
                raise ValueError("Invalid mirror cursor")
            next_cursor = cursor.get("bottom") if cursor else None
            if next_cursor is not None and (not isinstance(next_cursor, str) or len(next_cursor) > 8192):
                raise ValueError("Invalid mirror cursor")
            # Validate the whole page before accepting any of its rows.
            tweets = [_to_tweet(data["status"], root_id=tweet_id, role="root")]
            for role in ("thread", "replies"):
                tweets.extend(
                    _to_tweet(post, root_id=tweet_id, role="reply" if role == "replies" else role)
                    for post in data[role]
                )
            added = 0
            truncated = False
            for tweet in tweets:
                if tweet.tweet_id in seen_ids:
                    continue
                if len(result.tweets) >= max_items:
                    truncated = True
                    break
                seen_ids.add(tweet.tweet_id)
                result.tweets.append(tweet)
                added += 1
            result.pages.append(
                {
                    "url": url,
                    "retrieved_at": datetime.now(UTC).isoformat(),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "added": added,
                }
            )
            result.next_cursor = next_cursor or None
            result.reply_ids.update(post["id"] for post in data["replies"] if post["id"] in seen_ids)
            if truncated:
                result.stop_reason = "max_items"
                break
            if not next_cursor:
                result.stop_reason = "no_cursor"
                break
            if next_cursor in seen_cursors:
                result.stop_reason = "repeated_cursor"
                break
            if not added:
                result.stop_reason = "no_progress"
                break
            seen_cursors.add(next_cursor)
            if len(result.tweets) >= max_items:
                result.stop_reason = "max_items"
                break
        except (requests.RequestException, ValueError) as exc:
            result.stop_reason = "error"
            # Avoid copying provider bodies, network URLs, or ambient secrets.
            result.error = str(exc) if isinstance(exc, ValueError) else "Mirror request failed"
            break
    return result
