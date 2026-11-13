"""Convert explicit public research results to the protected search contract."""

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import Any

from trr_backend.socials.twitter.public_search import search_public_posts


def search_anonymous_twitter(request: Any) -> dict[str, Any]:
    """No credential loader, database, scheduler, or media service is used here."""
    if isinstance(request, dict):
        request = SimpleNamespace(**request)
    # Keep these guards for direct Python callers as well as validated API input.
    if getattr(request, "persist", False) or getattr(request, "mirror_to_s3", False):
        raise ValueError("Anonymous research does not persist results or upload media")
    result = search_public_posts(
        request.query,
        provider=request.provider,
        sort=getattr(request, "sort", "latest"),
        max_pages=getattr(request, "max_pages", None) or 1,
        max_items=getattr(request, "max_items", 20),
    )
    tweets = [
        {
            "tweet_id": post["tweet_id"],
            "date_time": post["date_time"],
            "text": post["text"],
            "url": post["url"],
            "username": post["username"],
            "display_name": post["username"],
            "hashtags": re.findall(r"#(\w+)", post["text"]),
            "mentions": re.findall(r"@([A-Za-z0-9_]+)", post["text"]),
            **dict.fromkeys(
                (
                    "likes",
                    "retweets",
                    "replies",
                    "quotes",
                    "views",
                    "bookmarks",
                    "shares",
                    "user_verified",
                    "is_reply",
                    "is_retweet",
                    "is_quote",
                ),
                None,
            ),
            "media_urls": [],
            "hosted_media_urls": [],
        }
        for post in result["posts"]
    ]
    metadata = {
        key: result[key]
        for key in (
            "provider",
            "status",
            "coverage",
            "upstream_authentication",
            "authentication_provided",
            "external_dependency",
            "usage",
            "stop_reason",
            "pages",
        )
    }
    metadata.update(
        retrieval_mode="anonymous_research",
        complete=False,
        pages_scanned=len(result["pages"]),
        max_pages=getattr(request, "max_pages", None) or 1,
        max_items=getattr(request, "max_items", 20),
        text_scope="mirror display text; may omit media links or truncate long posts",
        fields_unavailable=["engagement", "verification", "reply_relationships", "media"],
    )
    return {
        "success": bool(tweets),
        "query": request.query,
        "tweets_found": len(tweets),
        "tweets": tweets,
        "search_query_used": request.query,
        "filters_applied": {"mode": "anonymous_research", "query": request.query, "sort": result["sort"]},
        "retrieval_meta": metadata,
        "complete": False,
        "persist_summary": None,
        "scrape_run_id": None,
        "error": result["error"],
    }
