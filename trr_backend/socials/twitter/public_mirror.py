"""Opt-in anonymous conversation reads from the external FxTwitter mirror.

No TRR credentials are loaded or sent. The mirror's own upstream authentication
is unknown. Exhausting its cursor does not establish complete X thread coverage.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import requests

if TYPE_CHECKING:
    from trr_backend.socials.twitter.public_models import PublicPost

_ENDPOINT = "https://api.fxtwitter.com/2/conversation"
_MAX_RESPONSE_BYTES = 2_000_000


@dataclass
class PublicMirrorConversation:
    tweets: list[PublicPost] = field(default_factory=list)
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
            "optional_fields": "provider-reported; absent metrics remain null",
            "pages": self.pages,
        }


def _fetch_page(url: str, *, deadline: float) -> bytes:
    from trr_backend.socials.twitter.public_transport import fetch

    return fetch(url, deadline=deadline)


def _to_tweet(post: Any, *, root_id: str, role: str):
    from trr_backend.socials.twitter.public_models import normalize

    return normalize(post, root_id=root_id, role=role)


def fetch_public_conversation(
    tweet_id: str, *, max_pages: int = 3, max_items: int = 100, deadline: float | None = None
) -> PublicMirrorConversation:
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
    deadline = min(deadline, time.monotonic() + 120) if deadline is not None else time.monotonic() + 120
    result = PublicMirrorConversation()
    seen_ids: set[str] = set()
    seen_cursors: set[str] = set()
    for _ in range(max_pages):
        url = f"{_ENDPOINT}/{tweet_id}"
        if result.next_cursor:
            url += "?" + urlencode({"cursor": result.next_cursor})
        try:
            body = _fetch_page(url, deadline=deadline)
            if time.monotonic() > deadline:
                raise ValueError("Public provider deadline")
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
            from trr_backend.socials.twitter.public_models import conversation_posts

            tweets = conversation_posts(data, root_id=tweet_id)
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
            result.stop_reason = "deadline" if "deadline" in str(exc) else "error"
            # Avoid copying provider bodies, network URLs, or ambient secrets.
            result.error = str(exc) if isinstance(exc, ValueError) else "Mirror request failed"
            break
    return result
