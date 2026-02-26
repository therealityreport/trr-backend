from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from .scraper import TwitterScraper

_TWEET_ID_RE = re.compile(r"\b(\d{5,25})\b")
_STATUS_PATH_RE = re.compile(r"/status/(\d{5,25})")
_I_STATUS_PATH_RE = re.compile(r"/i/status/(\d{5,25})")


def normalize_tweet_id(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.isdigit():
        return raw

    parsed = urlparse(raw)
    path = parsed.path or ""
    for pattern in (_STATUS_PATH_RE, _I_STATUS_PATH_RE):
        match = pattern.search(path)
        if match:
            return match.group(1)

    fallback = _TWEET_ID_RE.search(raw)
    return fallback.group(1) if fallback else ""


def canonical_tweet_url(tweet_id: str, username: str | None = None) -> str:
    normalized_id = normalize_tweet_id(tweet_id)
    if not normalized_id:
        return ""
    handle = str(username or "").strip().lstrip("@")
    if handle:
        return f"https://x.com/{handle}/status/{normalized_id}"
    return f"https://x.com/i/status/{normalized_id}"


def resolve_twitter_media(
    *,
    tweet_id_or_url: str,
    canonical_url: str | None = None,
    username: str | None = None,
    cookies: dict[str, Any] | None = None,
    bearer_token: str | None = None,
    twikit_credentials: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tweet_id = normalize_tweet_id(tweet_id_or_url) or normalize_tweet_id(canonical_url)
    normalized_username = str(username or "").strip().lstrip("@")

    if not tweet_id:
        return {
            "tweet_id": "",
            "canonical_url": canonical_tweet_url(tweet_id_or_url, normalized_username),
            "source": "tweet_detail",
            "media_urls": [],
            "thumbnail_url": None,
            "attempts": [
                {
                    "source": "tweet_detail",
                    "success": False,
                    "reason_code": "missing_tweet_id",
                    "selected_url_count": 0,
                }
            ],
        }

    scraper = TwitterScraper(
        cookies=cookies,
        bearer_token=bearer_token,
        twikit_credentials=twikit_credentials,
    )
    tweet = scraper.fetch_tweet_detail(tweet_id, delay=0.0)
    media_urls = []
    if tweet:
        media_urls = [str(url).strip() for url in (tweet.media_urls or []) if str(url).strip()]

    media_urls = list(dict.fromkeys(media_urls))
    fallback_username = normalized_username or (tweet.username if tweet else None)
    result_url = canonical_url or canonical_tweet_url(tweet_id, fallback_username)
    if media_urls:
        return {
            "tweet_id": tweet_id,
            "canonical_url": result_url,
            "source": "tweet_detail",
            "media_urls": media_urls,
            "thumbnail_url": media_urls[0],
            "attempts": [
                {
                    "source": "tweet_detail",
                    "success": True,
                    "reason_code": None,
                    "selected_url_count": len(media_urls),
                }
            ],
        }

    return {
        "tweet_id": tweet_id,
        "canonical_url": result_url,
        "source": "tweet_detail",
        "media_urls": [],
        "thumbnail_url": None,
        "attempts": [
            {
                "source": "tweet_detail",
                "success": False,
                "reason_code": "twitter_media_not_found",
                "selected_url_count": 0,
            }
        ],
    }
