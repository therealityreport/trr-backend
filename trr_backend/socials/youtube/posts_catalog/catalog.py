"""Posts-only YouTube shared catalog orchestration.

The module is intentionally wired through callbacks for persistence and shared
helpers so it can be imported by the monolith compatibility wrapper without the
YouTube package importing repository surfaces back.
"""

from __future__ import annotations

import logging
import re
import shutil
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from trr_backend.socials.youtube.api_client import YouTubeDataApiClient
from trr_backend.socials.youtube.scraper import YouTubeScrapeConfig, YouTubeScraper

logger = logging.getLogger(__name__)

SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE = "shared_account_catalog_backfill"

ProgressCallback = Callable[[dict[str, Any]], None]


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _normalize_account_handle(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    candidate = raw
    if "://" in raw or raw.lower().startswith("www."):
        url_value = raw if "://" in raw else f"https://{raw}"
        parsed = urlparse(url_value)
        path_parts = [segment for segment in str(parsed.path or "").split("/") if segment]
        candidate = path_parts[0] if path_parts else str(parsed.netloc or "")

    candidate = candidate.strip().lstrip("@")
    candidate = candidate.split("?")[0].split("#")[0].split("/")[0].strip().lower()
    return candidate


def _platform_profile_url_for_handle(platform: str, handle: Any) -> str | None:
    normalized_handle = _normalize_account_handle(handle)
    if not normalized_handle:
        return None
    if str(platform or "").strip().lower() == "youtube":
        return f"https://www.youtube.com/@{normalized_handle}"
    return None


def _profile_avatar_quality_score(value: str) -> tuple[int, int]:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return (-1, -1)
    max_dim = 0
    for match in re.finditer(r"(?:s)?(\d{2,4})x(\d{2,4})", normalized):
        max_dim = max(max_dim, int(match.group(1)), int(match.group(2)))
    for match in re.finditer(r"=s(\d{2,4})", normalized):
        max_dim = max(max_dim, int(match.group(1)))
    profile_score = 50 if "yt3.googleusercontent.com" in normalized or "avatar" in normalized else 0
    thumb_penalty = -30 if "thumb" in normalized or "thumbnail" in normalized else 0
    return (profile_score + thumb_penalty + min(max_dim, 4096), len(normalized))


def _upgrade_youtube_avatar_url(value: Any) -> str | None:
    candidate = str(value or "").strip()
    if not candidate.startswith(("http://", "https://")):
        return None
    if "yt3.googleusercontent.com" in candidate:
        return re.sub(r"=s\d+(-[^?]*)?", r"=s1024\1", candidate)
    return candidate


def _best_profile_avatar_url(candidates: list[Any]) -> str | None:
    best_url: str | None = None
    best_score: tuple[int, int] = (-1, -1)
    seen: set[str] = set()
    for candidate in candidates:
        for variant in (candidate, _upgrade_youtube_avatar_url(candidate)):
            normalized = str(variant or "").strip()
            if not normalized or not normalized.startswith(("http://", "https://")):
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            score = _profile_avatar_quality_score(normalized)
            if score > best_score:
                best_score = score
                best_url = normalized
    return best_url


def _merge_social_profile_snapshots(*snapshots: Mapping[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for snapshot in snapshots:
        metadata = _metadata_dict(snapshot)
        if not metadata:
            continue
        for key in ("username", "display_name", "bio", "avatar_url", "profile_url", "channel_id"):
            value = str(metadata.get(key) or "").strip()
            if value and not str(merged.get(key) or "").strip():
                merged[key] = value
        for key in ("follower_count", "following_count", "total_posts"):
            if metadata.get(key) is None:
                continue
            merged[key] = max(
                _normalize_non_negative_int(merged.get(key)),
                _normalize_non_negative_int(metadata.get(key)),
            )
        if bool(metadata.get("is_verified")):
            merged["is_verified"] = True
    return merged


def _youtube_profile_snapshot_from_api_identity(
    identity: Mapping[str, Any] | None,
    *,
    account_handle: str,
) -> dict[str, Any]:
    metadata = _metadata_dict(identity)
    if not metadata:
        return {}
    payload = _metadata_dict(metadata.get("payload"))
    snippet = _metadata_dict(payload.get("snippet"))
    statistics = _metadata_dict(payload.get("statistics"))
    thumbnails = _metadata_dict(snippet.get("thumbnails"))
    avatar_url = _best_profile_avatar_url(
        [
            _metadata_dict(thumbnails.get("high")).get("url"),
            _metadata_dict(thumbnails.get("medium")).get("url"),
            _metadata_dict(thumbnails.get("default")).get("url"),
            snippet.get("thumbnailUrl"),
        ]
    )
    canonical_handle = _normalize_account_handle(metadata.get("canonical_handle") or account_handle) or account_handle
    return {
        "username": canonical_handle,
        "display_name": str(metadata.get("title") or snippet.get("title") or "").strip() or None,
        "bio": str(snippet.get("description") or "").strip() or None,
        "avatar_url": avatar_url,
        "profile_url": _platform_profile_url_for_handle("youtube", canonical_handle),
        "follower_count": _normalize_non_negative_int(statistics.get("subscriberCount")) or None,
        "total_posts": _normalize_non_negative_int(statistics.get("videoCount")) or None,
        "channel_id": str(metadata.get("channel_id") or "").strip() or None,
    }


def _shared_catalog_mode(config: Mapping[str, Any] | None) -> bool:
    return (
        str((config or {}).get("pipeline_ingest_mode") or "").strip().lower()
        == SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE
    )


def _shared_stage_post_limit(config: Mapping[str, Any] | None, *, default: int = 100) -> int | None:
    raw_limit = None if config is None else config.get("max_posts_per_target")
    if raw_limit is None:
        return default
    try:
        parsed = int(raw_limit)
    except (TypeError, ValueError):
        return default
    if parsed <= 0:
        return None
    return parsed


def _coerce_dt(value: Any) -> Any:
    return value


def _ytdlp_available() -> bool:
    return bool(shutil.which("yt-dlp"))


@dataclass(slots=True)
class YouTubePostsCatalogDependencies:
    scraper_factory: Callable[[], Any] = YouTubeScraper
    api_client_factory: Callable[[], Any] = YouTubeDataApiClient
    scrape_config_factory: Callable[..., Any] = YouTubeScrapeConfig
    coerce_dt: Callable[[Any], Any] = _coerce_dt
    shared_stage_post_limit: Callable[[Mapping[str, Any] | None], int | None] = _shared_stage_post_limit
    shared_catalog_mode: Callable[[Mapping[str, Any] | None], bool] = _shared_catalog_mode
    metadata_dict: Callable[[Any], dict[str, Any]] = _metadata_dict
    normalize_account_handle: Callable[[Any], str] = _normalize_account_handle
    merge_social_profile_snapshots: Callable[..., dict[str, Any]] = _merge_social_profile_snapshots
    youtube_profile_snapshot_from_api_identity: Callable[..., dict[str, Any]] = (
        _youtube_profile_snapshot_from_api_identity
    )
    best_profile_avatar_url: Callable[[list[Any]], str | None] = _best_profile_avatar_url
    platform_profile_url_for_handle: Callable[[str, Any], str | None] = _platform_profile_url_for_handle
    persist_shared_catalog_posts_with_progress: Callable[..., list[dict[str, Any]]] | None = None
    upsert_shared_catalog_post: Callable[..., dict[str, Any] | None] | None = None
    upsert_youtube_video: Callable[..., dict[str, Any] | None] | None = None
    ytdlp_available: Callable[[], bool] = _ytdlp_available


def _api_client_enabled(api_client: Any) -> bool:
    enabled = getattr(api_client, "enabled", None)
    if callable(enabled):
        return bool(enabled())
    return bool(enabled)


def _resolve_channel_identity(api_client: Any, account_handle: str) -> Mapping[str, Any] | None:
    if not _api_client_enabled(api_client):
        return None
    resolve_channel = getattr(api_client, "resolve_channel", None)
    if not callable(resolve_channel):
        return None
    identity = resolve_channel(account_handle)
    return identity if isinstance(identity, Mapping) else None


def _first_non_empty_attr(items: Sequence[Any], attr: str) -> str | None:
    for item in items:
        value = str(getattr(item, attr, "") or "").strip()
        if value:
            return value
    return None


def _persist_shared_catalog_posts(
    *,
    deps: YouTubePostsCatalogDependencies,
    run_id: str | None,
    canonical_handle: str,
    posts: Sequence[Any],
    retrieval_meta: MutableMapping[str, Any],
    progress_cb: ProgressCallback | None,
) -> list[dict[str, Any]]:
    if deps.persist_shared_catalog_posts_with_progress is None:
        raise RuntimeError("persist_shared_catalog_posts_with_progress dependency is required in shared catalog mode")
    if deps.upsert_shared_catalog_post is None:
        raise RuntimeError("upsert_shared_catalog_post dependency is required in shared catalog mode")

    return deps.persist_shared_catalog_posts_with_progress(
        platform="youtube",
        run_id=run_id,
        account_handle=canonical_handle,
        items=posts,
        retrieval_meta=retrieval_meta,
        progress_cb=progress_cb,
        upsert_item=lambda video: deps.upsert_shared_catalog_post(
            platform="youtube",
            run_id=run_id,
            account_handle=canonical_handle,
            post=video,
        ),
    )


def _persist_youtube_videos(
    *,
    deps: YouTubePostsCatalogDependencies,
    job_id: str,
    canonical_handle: str,
    posts: Sequence[Any],
) -> list[dict[str, Any]]:
    if deps.upsert_youtube_video is None:
        raise RuntimeError("upsert_youtube_video dependency is required outside shared catalog mode")

    rows: list[dict[str, Any]] = []
    for video in posts:
        row = deps.upsert_youtube_video(None, job_id=job_id, account=canonical_handle, video=video)
        if row:
            rows.append(row)
    return rows


def scrape_shared_youtube_posts(
    *,
    run_id: str | None,
    account_handle: str,
    config: Mapping[str, Any],
    job_id: str,
    progress_cb: ProgressCallback | None = None,
    dependencies: YouTubePostsCatalogDependencies | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    deps = dependencies or YouTubePostsCatalogDependencies()
    scraper = deps.scraper_factory()
    youtube_api = deps.api_client_factory()
    scrape_config = deps.scrape_config_factory(
        channel_handle=account_handle,
        keywords=[],
        date_start=deps.coerce_dt(config.get("date_start")),
        date_end=deps.coerce_dt(config.get("date_end")),
        delay_seconds=0.35,
        max_results=deps.shared_stage_post_limit(config),
        enforce_keyword_filter=False,
    )
    api_identity = _resolve_channel_identity(youtube_api, account_handle)
    posts = list(scraper.scrape(scrape_config, progress_cb=progress_cb))

    ytdlp_available = bool(deps.ytdlp_available())
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
    retrieval_meta["ytdlp_available"] = ytdlp_available
    if not ytdlp_available:
        logger.warning(
            "yt-dlp not available in runtime - YouTube video metrics "
            "(likes, comments, tags, duration) will be incomplete for @%s",
            account_handle,
        )

    canonical_handle = (
        deps.normalize_account_handle(
            retrieval_meta.get("canonical_handle")
            or deps.metadata_dict(api_identity).get("canonical_handle")
            or account_handle
        )
        or account_handle
    )
    profile_snapshot = deps.merge_social_profile_snapshots(
        deps.youtube_profile_snapshot_from_api_identity(api_identity, account_handle=canonical_handle),
        {
            "username": canonical_handle,
            "display_name": str(
                retrieval_meta.get("resolved_channel_title")
                or _first_non_empty_attr(posts, "channel_title")
                or ""
            ).strip()
            or None,
            "avatar_url": deps.best_profile_avatar_url(
                [
                    retrieval_meta.get("resolved_channel_avatar_url"),
                    _first_non_empty_attr(posts, "user_avatar_url"),
                ]
            ),
            "profile_url": deps.platform_profile_url_for_handle("youtube", canonical_handle),
            "channel_id": str(
                retrieval_meta.get("canonical_channel_id") or _first_non_empty_attr(posts, "channel_id") or ""
            ).strip()
            or None,
        },
    )

    if deps.shared_catalog_mode(config):
        rows = _persist_shared_catalog_posts(
            deps=deps,
            run_id=run_id,
            canonical_handle=canonical_handle,
            posts=posts,
            retrieval_meta=retrieval_meta,
            progress_cb=progress_cb,
        )
    else:
        rows = _persist_youtube_videos(
            deps=deps,
            job_id=job_id,
            canonical_handle=canonical_handle,
            posts=posts,
        )

    retrieval_meta["profile_snapshot"] = profile_snapshot
    retrieval_meta["total_posts"] = max(
        _normalize_non_negative_int(retrieval_meta.get("total_posts")),
        _normalize_non_negative_int(profile_snapshot.get("total_posts")),
        len(rows),
    )

    if not rows and not retrieval_meta.get("error_code"):
        first_page_counts = retrieval_meta.get("first_page_counts") or {}
        if not first_page_counts.get("videos") and not first_page_counts.get("shorts"):
            retrieval_meta["error_code"] = "youtube_empty_channel_page"
            retrieval_meta["retryable"] = True
            retrieval_meta["error_class"] = "YouTubeEmptyChannelPage"
            logger.warning(
                "YouTube scrape for @%s returned 0 posts with no continuation error - "
                "marking as youtube_empty_channel_page (first_page_counts=%s)",
                account_handle,
                first_page_counts,
            )

    return rows, retrieval_meta
