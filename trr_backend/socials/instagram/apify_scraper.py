"""
Apify-based Instagram scraper using the apify/instagram-scraper actor.

This is an alternative to the built-in GraphQL/browser-based scraper that
delegates the actual scraping to Apify's managed infrastructure, which handles
proxy rotation, anti-bot bypass, and Instagram API changes automatically.

The output is normalized to match the existing catalog post schema so it can
be ingested by the same downstream pipeline.
"""

import logging
import os
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Apify actor configuration
# ---------------------------------------------------------------------------
APIFY_ACTOR_ID = "apify/instagram-post-scraper"

# Maps Apify post output fields → internal catalog schema
_POST_TYPE_MAP = {
    "Image": "image",
    "Video": "video",
    "Sidecar": "carousel",
}


def _get_apify_client():
    """Lazy-import and instantiate the Apify client."""
    from apify_client import ApifyClient

    token = os.environ.get("APIFY_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            "APIFY_API_TOKEN is not set. Add it to .env to use the Apify backfill."
        )
    return ApifyClient(token)


def run_apify_instagram_scrape(
    *,
    username: str,
    results_limit: int = 100,
    date_start: datetime | None = None,
    data_detail_level: str = "detailedData",
    skip_pinned_posts: bool = False,
) -> dict[str, Any]:
    """
    Run the apify/instagram-scraper actor and return raw results.

    Returns a dict with:
      - "posts": list of raw Apify post dicts
      - "run_id": Apify run ID for debugging
      - "dataset_id": Apify dataset ID
      - "post_count": number of posts returned
      - "actor": actor ID used
    """
    client = _get_apify_client()

    run_input: dict[str, Any] = {
        "username": [username],
        "resultsLimit": results_limit,
        "dataDetailLevel": data_detail_level,
        "skipPinnedPosts": skip_pinned_posts,
    }

    if date_start is not None:
        run_input["onlyPostsNewerThan"] = date_start.strftime("%Y-%m-%d")

    logger.info(
        "Starting Apify instagram-scraper run: username=%s limit=%d date_start=%s",
        username,
        results_limit,
        run_input.get("onlyPostsNewerThan"),
    )

    run = client.actor(APIFY_ACTOR_ID).call(run_input=run_input)
    run_id = run.get("id", "unknown")
    dataset_id = run.get("defaultDatasetId", "")

    logger.info(
        "Apify run completed: run_id=%s dataset_id=%s",
        run_id,
        dataset_id,
    )

    items = list(client.dataset(dataset_id).iterate_items())

    logger.info(
        "Apify run returned %d items for @%s",
        len(items),
        username,
    )

    return {
        "posts": items,
        "run_id": run_id,
        "dataset_id": dataset_id,
        "post_count": len(items),
        "actor": APIFY_ACTOR_ID,
    }


def normalize_apify_post(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize a single Apify post dict into the shape expected by the
    catalog ingest pipeline.

    The output mirrors the fields used by the built-in scraper so that
    downstream code (DB upsert, classification, etc.) can handle both
    sources uniformly.
    """
    post_type = _POST_TYPE_MAP.get(raw.get("type", ""), "unknown")
    timestamp_str = raw.get("timestamp", "")
    posted_at = None
    if timestamp_str:
        try:
            posted_at = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            # Apify occasionally changes timestamp shape; log so operators
            # can diagnose drift instead of silently nulling posted_at.
            logger.debug(
                "apify instagram timestamp parse failed: %r",
                timestamp_str,
                exc_info=True,
            )
            posted_at = None

    # Extract hashtags from caption if not provided
    caption = raw.get("caption") or ""
    hashtags = raw.get("hashtags") or []
    mentions = raw.get("mentions") or []

    # Child posts for carousels
    child_posts = raw.get("childPosts") or []
    child_media_urls = []
    if child_posts:
        child_media_urls = [
            cp.get("displayUrl") or cp.get("videoUrl", "")
            for cp in child_posts
            if cp.get("displayUrl") or cp.get("videoUrl")
        ]
    # Also include displayResourceUrls if present (post-detail format)
    display_resource_urls = raw.get("displayResourceUrls") or []
    if display_resource_urls and not child_media_urls:
        child_media_urls = display_resource_urls

    # Latest comments
    latest_comments = []
    for comment in raw.get("latestComments") or []:
        latest_comments.append({
            "id": comment.get("id"),
            "text": comment.get("text", ""),
            "username": comment.get("ownerUsername", ""),
            "timestamp": comment.get("timestamp"),
            "likes_count": comment.get("likesCount", 0),
            "replies_count": comment.get("repliesCount", 0),
        })

    # Tagged users
    tagged_users = []
    for tagged in raw.get("taggedUsers") or []:
        tagged_users.append({
            "username": tagged.get("username", ""),
            "full_name": tagged.get("full_name", ""),
            "is_verified": tagged.get("is_verified", False),
        })

    # Co-author producers
    coauthors = []
    for coauthor in raw.get("coauthorProducers") or []:
        coauthors.append({
            "username": coauthor.get("username", ""),
            "is_verified": coauthor.get("is_verified", False),
        })

    return {
        "source": "apify",
        "source_id": str(raw.get("id", "")),
        "shortcode": raw.get("shortCode", ""),
        "post_type": post_type,
        "caption": caption,
        "hashtags": hashtags,
        "mentions": mentions,
        "posted_at": posted_at.isoformat() if posted_at else None,
        "likes_count": raw.get("likesCount", 0),
        "comments_count": raw.get("commentsCount", 0),
        "video_view_count": raw.get("videoViewCount"),
        "video_play_count": raw.get("videoPlayCount"),
        "video_duration": raw.get("videoDuration"),
        "display_url": raw.get("displayUrl", ""),
        "video_url": raw.get("videoUrl"),
        "alt_text": raw.get("alt"),
        "dimensions_width": raw.get("dimensionsWidth"),
        "dimensions_height": raw.get("dimensionsHeight"),
        "owner_username": raw.get("ownerUsername", ""),
        "owner_full_name": raw.get("ownerFullName", ""),
        "owner_id": raw.get("ownerId", ""),
        "permalink": raw.get("url", ""),
        "first_comment": raw.get("firstComment"),
        "latest_comments": latest_comments,
        "tagged_users": tagged_users,
        "coauthors": coauthors,
        "child_media_urls": child_media_urls,
        "is_comments_disabled": raw.get("isCommentsDisabled", False),
        "location_name": raw.get("locationName"),
        "location_id": raw.get("locationId"),
        "music_info": raw.get("musicInfo"),
        "product_type": raw.get("productType"),
        "is_advertisement": raw.get("isAdvertisement", False),
    }


def run_and_normalize(
    *,
    username: str,
    results_limit: int = 100,
    date_start: datetime | None = None,
    data_detail_level: str = "detailedData",
    skip_pinned_posts: bool = False,
) -> dict[str, Any]:
    """
    Run the Apify scraper and return normalized posts ready for catalog ingest.

    Returns a dict with:
      - "posts": list of normalized post dicts
      - "raw_posts": list of original Apify output (for debugging)
      - "run_id", "dataset_id", "post_count", "actor"
    """
    result = run_apify_instagram_scrape(
        username=username,
        results_limit=results_limit,
        date_start=date_start,
        data_detail_level=data_detail_level,
        skip_pinned_posts=skip_pinned_posts,
    )

    raw_posts = result["posts"]
    normalized = [normalize_apify_post(p) for p in raw_posts]

    return {
        "posts": normalized,
        "raw_posts": raw_posts,
        "run_id": result["run_id"],
        "dataset_id": result["dataset_id"],
        "post_count": result["post_count"],
        "actor": result["actor"],
    }
