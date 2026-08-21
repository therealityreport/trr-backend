# ruff: noqa: F821, UP037
"""Instagram shared-account catalog ingest orchestration and persistence helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from trr_backend.socials.control_plane.run_lifecycle import _legacy_module as _published_legacy_module
from trr_backend.socials.instagram import payload_sidecars as _payload_sidecars
from trr_backend.socials.instagram.post_normalizer import _REPOST_COUNT_ALIASES, _extract_repost_count
from trr_backend.socials.provider_registry import (
    LateNamespaceProvider,
    LateProviderProxy,
    adopt_published,
    publish_module_slot,
)

if TYPE_CHECKING:
    import hashlib
    import logging
    import os
    import time as time_module
    from collections import Counter
    from contextlib import nullcontext
    from datetime import UTC, datetime

    from trr_backend.db import pg
    from trr_backend.socials.model_types import SeasonContext

    CATALOG_FULL_HISTORY_CURSOR_PARTITION_STRATEGY: str
    PLATFORM_CATALOG_POST_TABLES: dict[str, str]
    SHARED_ACCOUNT_EXECUTION_LOCK_UNAVAILABLE_ERROR_CODE: str
    logger: logging.Logger

    class SharedStageRuntimeError(RuntimeError):
        def __init__(
            self,
            message: str,
            *,
            error_code: str,
            retryable: bool = False,
            error_class: str | None = None,
            runtime_metadata: Mapping[str, Any] | None = None,
        ) -> None: ...

    class SharedAccountCursorPartition:
        partition_key: str
        shard_index: int
        shard_total: int
        runner_lane: str
        cursor_start: str | None
        cursor_end: str | None
        boundary_start_at: datetime | None
        boundary_end_at: datetime | None
        metadata: dict[str, Any]

        def __init__(
            self,
            partition_key: str,
            shard_index: int,
            shard_total: int,
            runner_lane: str,
            cursor_start: str | None,
            cursor_end: str | None,
            boundary_start_at: datetime | None,
            boundary_end_at: datetime | None,
            metadata: dict[str, Any],
        ) -> None: ...

    def _apply_assignment_payload(payload: dict[str, Any], context: SeasonContext | None) -> None: ...

    def _normalize_cursor_partition_token(value: Any) -> str | None: ...

    def _catalog_backfill_run_scheduler_lanes(runner_count: int) -> list[str]: ...

    def _catalog_full_history_posts_per_shard(platform: str | None) -> int: ...

    def _now_utc() -> datetime: ...

    def _env_truthy(name: str, *, default: bool = False) -> bool: ...

    def _catalog_backfill_has_bounded_window(*, date_start: datetime | None, date_end: datetime | None) -> bool: ...

    def _shared_account_expected_total_posts_from_config(
        config: Mapping[str, Any] | None, *, platform: Any, account_handle: Any
    ) -> int: ...

    def _metadata_dict(value: Any) -> dict[str, Any]: ...

    def _coerce_dt(value: Any) -> datetime | None: ...

    def _iso(dt: datetime | None) -> str | None: ...

    def _instagram_posts_has_column(column: str, *, conn: Any | None = None) -> bool: ...

    def _load_instagram_cookies_from_sources(*args: Any, **kwargs: Any) -> Any: ...

    def _validate_instagram_cookie_health(*args: Any, **kwargs: Any) -> Any: ...

    def _normalize_non_negative_int(value: Any) -> int: ...

    def _instagram_reported_comment_count_from_payload(payload: Any) -> int: ...

    def _as_text_list(value: Any, *, prefix: str = "", strip_prefix: str | None = None) -> list[str]: ...

    def _first_non_empty_str(*values: Any) -> str | None: ...

    def _mark_instagram_metadata_attempt(
        *, post: Any, now_utc: datetime, success: bool, error_code: str | None = None
    ) -> None: ...

    def _enrich_instagram_post_from_permalink(*, post: Any, scraper: Any, now_utc: datetime) -> None: ...

    def _normalize_instagram_mirror_attempts(value: Any) -> list[dict[str, Any]]: ...

    def _load_existing_social_account_posts(
        platform: str,
        account: str,
        date_start: datetime | None,
        date_end: datetime | None,
        source_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]: ...

    def _pg_upsert(
        table: str,
        payload: dict[str, Any],
        *,
        conflict_col: str | Sequence[str],
        conn: Any | None = None,
        include_inserted_flag: bool = False,
    ) -> dict[str, Any] | None: ...

    def _pg_upsert_many(
        table: str,
        payloads: list[dict[str, Any]],
        *,
        conflict_col: str | Sequence[str],
        conn: Any | None = None,
        include_inserted_flag: bool = False,
        coalesce_preserve_cols: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]: ...

    def _instagram_post_owner_id(post: Any, raw_data: Mapping[str, Any]) -> str | None: ...

    def _sync_instagram_canonical_post(
        *, legacy_row: Mapping[str, Any] | None, payload: Mapping[str, Any], post: Any, conn: Any | None
    ) -> dict[str, Any] | None: ...

    def _platform_post_needs_media_mirror(
        platform: str,
        post_row: dict[str, Any],
        *,
        conn: Any | None = None,
        include_auxiliary_repairs: bool = True,
    ) -> bool: ...

    def _enqueue_instagram_media_mirror_job(*args: Any, **kwargs: Any) -> Any: ...

    def _extract_instagram_post_detail_node(payload: dict[str, Any] | None) -> dict[str, Any] | None: ...

    def _refresh_instagram_post_metrics_only(
        *,
        post_db_id: str,
        likes: int,
        comments_count: int,
        views: int | None,
        views_source: str | None = None,
        views_raw_candidates: list[dict[str, Any]] | None = None,
        conn: Any,
    ) -> None: ...

    def _shared_catalog_mode(config: Mapping[str, Any] | None) -> bool: ...

    def _shared_catalog_post_url(
        platform: str,
        *,
        account_handle: str,
        source_id: str,
        explicit: str | None,
        post_format: object | None = None,
        media_type: object | None = None,
        raw_data: Mapping[str, object] | None = None,
    ) -> str | None: ...

    def _shared_catalog_payload_base(
        *,
        source_id: str,
        account_handle: str,
        posted_at: datetime | None,
        permalink: str | None,
        title: str | None = None,
        caption: str | None = None,
        description: str | None = None,
        text: str | None = None,
        media_type: str | None = None,
        media_urls: list[str] | None = None,
        thumbnail_url: str | None = None,
        hashtags: list[str] | None = None,
        mentions: list[str] | None = None,
        collaborators: list[str] | None = None,
        profile_tags: list[str] | None = None,
        likes: int | None = None,
        comments_count: int | None = None,
        views: int | None = None,
        shares: int | None = None,
        retweets: int | None = None,
        replies_count: int | None = None,
        quotes: int | None = None,
        raw_data: dict[str, Any] | None = None,
        run_id: str | None = None,
        music_info: dict[str, Any] | None = None,
        audio_url: str | None = None,
        paid_partnership: bool = False,
        child_posts_data: list[dict[str, Any]] | None = None,
        owner_username: str | None = None,
        video_play_count: int | None = None,
        video_duration: float | None = None,
    ) -> dict[str, Any]: ...

    def _sync_instagram_catalog_post_collaborators(
        catalog_post_row: Mapping[str, Any], *, conn: Any | None = None
    ) -> None: ...

    def _shared_account_partition_key(
        *,
        run_id: str,
        platform: str,
        account_handle: str,
        shard_index: int,
        cursor_start: str | None,
        cursor_end: str | None,
    ) -> str: ...

    def _shared_catalog_progress_interval_posts() -> int: ...

    def _sanitize_instagram_browser_account_id(browser_account_id: str | None) -> str | None: ...

    def _persist_shared_catalog_posts_batch(
        *,
        platform: str,
        run_id: str | None,
        account_handle: str,
        posts: Sequence[Any],
        job_id: str | None = None,
        source_scope: str = "network",
        enable_media_followups: bool = False,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> tuple[list[dict[str, Any]], list[str], dict[str, int]]: ...

    def _normalize_shared_catalog_posts_batch_result(
        value: Any,
    ) -> tuple[list[dict[str, Any]], list[str], dict[str, int]]: ...

    def _parse_instagram_time(value: Any) -> datetime | None: ...


_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="INSTAGRAM_CATALOG_INGEST_PROVIDER",
    room_names=_LOCAL_ROOM_NAMES,
    imported_names=_IMPORTED_CORE_NAMES,
    room_wrappers=_CORE_ROOM_WRAPPERS,
    commit=publish_module_slot(globals(), "_core"),
)
_core: Any = LateProviderProxy(_PROVIDER)
_require_provider_ready = _PROVIDER.require
_configure_legacy_provider = _PROVIDER.configure
_sync_core_overrides = _PROVIDER.sync
_room_callable = _PROVIDER.room_callable
adopt_published(_PROVIDER, _published_legacy_module)
del _published_legacy_module


_INSTAGRAM_COMMENTS_HEADER_KEYS = {
    "has_more_comments",
    "has_more_headload_comments",
    "fb_comments",
    "comment_threading_enabled",
}
_INSTAGRAM_RICH_POST_KEYS = {
    "image_versions2",
    "video_versions",
    "original_width",
    "original_height",
    "dimensions",
    "usertags",
    "coauthor_producers",
    "location",
    "media_notes",
    "floating_context_items",
    "accessibility_caption",
    "like_and_view_counts_disabled",
    "comments_disabled",
    "commenting_disabled_for_viewer",
    "is_paid_partnership",
    "can_viewer_reshare",
    "has_audio",
    "crosspost_metadata",
    "social_context",
} | set(_REPOST_COUNT_ALIASES)
_INSTAGRAM_THIN_PRESERVE_OPTIONAL_KEYS = {
    "original_width",
    "original_height",
    "like_and_view_counts_disabled",
    "comments_disabled",
    "commenting_disabled_for_viewer",
    "can_viewer_reshare",
    "has_audio",
    "video_play_count",
}
_INSTAGRAM_POST_BATCH_SIZE = 100
_MISSING_INSTAGRAM_METRIC = object()
_INSTAGRAM_LIKE_HIDDEN_KEYS = {
    "like_and_view_counts_disabled",
    "hide_like_and_view_counts",
    "like_count_hidden",
    "likes_hidden",
}
_INSTAGRAM_VIEW_HIDDEN_KEYS = _INSTAGRAM_LIKE_HIDDEN_KEYS | {
    "view_count_hidden",
    "views_hidden",
}
_INSTAGRAM_COMMENT_HIDDEN_KEYS = {
    "comment_count_hidden",
    "comments_count_hidden",
}


def _context_manager_from_callable(callable_obj: Any, /, *args: Any, **kwargs: Any) -> Any:
    context_or_generator = callable_obj(*args, **kwargs)
    if hasattr(context_or_generator, "__enter__") and hasattr(context_or_generator, "__exit__"):
        return context_or_generator

    @contextmanager
    def _generated_context():
        yield from context_or_generator

    return _generated_context()


def _instagram_repost_count_from_post(post: Any, raw_data: dict[str, Any] | None = None) -> int | None:
    node = dict(raw_data) if isinstance(raw_data, dict) else {}
    for key in _REPOST_COUNT_ALIASES:
        if key not in node:
            value = getattr(post, key, None)
            if value is not None:
                node[key] = value
    return _extract_repost_count(node)


def _instagram_raw_data_is_thin_comments_header(raw_data: Any) -> bool:
    if not isinstance(raw_data, dict) or not raw_data:
        return False
    has_comments_shape = any(key in raw_data for key in _INSTAGRAM_COMMENTS_HEADER_KEYS)
    has_rich_post_shape = any(key in raw_data for key in _INSTAGRAM_RICH_POST_KEYS)
    return has_comments_shape and not has_rich_post_shape


def _instagram_catalog_posted_at(value: Any) -> Any | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if value <= 0:
            return None
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            if float(stripped) <= 0:
                return None
        except ValueError:
            pass
    return _parse_instagram_time(value)


def _instagram_truthy_metric_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    return False


def _instagram_metric_hidden(raw_data: Mapping[str, Any], post: Any, keys: set[str]) -> bool:
    for key in keys:
        if _instagram_truthy_metric_flag(raw_data.get(key)):
            return True
        if _instagram_truthy_metric_flag(getattr(post, key, None)):
            return True
    return False


def _instagram_known_metric_value(value: Any) -> int | None:
    if value is None or value is _MISSING_INSTAGRAM_METRIC or isinstance(value, bool):
        return None
    if isinstance(value, Mapping):
        for key in ("count", "total_count", "value"):
            result = _instagram_known_metric_value(value.get(key))
            if result is not None:
                return result
        return None
    if isinstance(value, (int, float)):
        return _normalize_non_negative_int(value)
    if isinstance(value, str):
        stripped = value.strip().replace(",", "")
        if not stripped:
            return None
        try:
            return _normalize_non_negative_int(stripped)
        except (TypeError, ValueError):
            return None
    return None


def _instagram_first_known_metric_from_raw(raw_data: Mapping[str, Any], keys: Sequence[str]) -> int | None:
    for key in keys:
        if key not in raw_data:
            continue
        result = _instagram_known_metric_value(raw_data.get(key))
        if result is not None:
            return result
    return None


def _instagram_catalog_likes_metric(post: Any, raw_data: Mapping[str, Any]) -> int | None:
    if _instagram_metric_hidden(raw_data, post, _INSTAGRAM_LIKE_HIDDEN_KEYS):
        return None
    raw_value = _instagram_first_known_metric_from_raw(
        raw_data,
        (
            "like_count",
            "likesCount",
            "likes_count",
            "edge_liked_by",
            "edge_media_preview_like",
        ),
    )
    if raw_value is not None:
        return raw_value
    return _instagram_known_metric_value(getattr(post, "likes", _MISSING_INSTAGRAM_METRIC))


def _instagram_catalog_comments_metric(post: Any, raw_data: Mapping[str, Any]) -> int | None:
    if _instagram_metric_hidden(raw_data, post, _INSTAGRAM_COMMENT_HIDDEN_KEYS):
        return None
    raw_value = _instagram_first_known_metric_from_raw(
        raw_data,
        (
            "comment_count",
            "commentsCount",
            "comments_count",
            "edge_media_to_comment",
            "edge_media_to_parent_comment",
        ),
    )
    if raw_value is not None:
        return raw_value
    return _instagram_known_metric_value(getattr(post, "comments", _MISSING_INSTAGRAM_METRIC))


def _instagram_catalog_views_metric(post: Any, raw_data: Mapping[str, Any]) -> int | None:
    if _instagram_metric_hidden(raw_data, post, _INSTAGRAM_VIEW_HIDDEN_KEYS):
        return None
    observed = getattr(post, "video_views_observed", _MISSING_INSTAGRAM_METRIC)
    observed_value = _instagram_known_metric_value(observed)
    if observed is not _MISSING_INSTAGRAM_METRIC:
        return observed_value
    raw_value = _instagram_first_known_metric_from_raw(
        raw_data,
        (
            "video_view_count",
            "videoViewCount",
            "view_count",
            "views_count",
            "play_count",
            "video_play_count",
            "videoPlayCount",
            "playCount",
            "viewCount",
        ),
    )
    if raw_value is not None:
        return raw_value
    return _instagram_known_metric_value(getattr(post, "video_views", _MISSING_INSTAGRAM_METRIC))


def _apply_instagram_catalog_metric_semantics(
    payload: dict[str, Any],
    *,
    post: Any,
    raw_data: Mapping[str, Any],
) -> dict[str, Any]:
    """Drop unknown metric columns so conflict updates preserve stored values."""
    metric_values = {
        "likes": _instagram_catalog_likes_metric(post, raw_data),
        "comments_count": _instagram_catalog_comments_metric(post, raw_data),
        "views": _instagram_catalog_views_metric(post, raw_data),
    }
    for key, value in metric_values.items():
        if value is None:
            payload.pop(key, None)
        else:
            payload[key] = value
    return payload


def _shared_instagram_catalog_graphql_page_size() -> int:
    """Catalog-only GraphQL page size override (default 50, bounded 33..50)."""
    raw = (os.getenv("SOCIAL_INSTAGRAM_CATALOG_GRAPHQL_PAGE_SIZE") or "").strip()
    if raw:
        try:
            val = int(raw)
        except ValueError:
            return 50
        return max(33, min(50, val))
    return 50


def _shared_instagram_catalog_graphql_request_timeout() -> tuple[int, int]:
    def _read_timeout_env(name: str, *, default: int, minimum: int, maximum: int) -> int:
        raw = (os.getenv(name) or "").strip()
        if raw:
            try:
                value = int(float(raw))
            except ValueError:
                value = default
        else:
            value = default
        return max(minimum, min(maximum, value))

    connect_timeout = _read_timeout_env(
        "SOCIAL_INSTAGRAM_CATALOG_GRAPHQL_CONNECT_TIMEOUT_SECONDS",
        default=5,
        minimum=2,
        maximum=15,
    )
    read_timeout = _read_timeout_env(
        "SOCIAL_INSTAGRAM_CATALOG_GRAPHQL_READ_TIMEOUT_SECONDS",
        default=12,
        minimum=5,
        maximum=30,
    )
    return connect_timeout, read_timeout


def _shared_instagram_browser_session_lock_timeout_seconds() -> float:
    raw = (
        os.getenv("SOCIAL_INSTAGRAM_CATALOG_BROWSER_SESSION_LOCK_TIMEOUT_SECONDS")
        or os.getenv("SOCIAL_BROWSER_SESSION_LOCK_TIMEOUT_SECONDS")
        or ""
    ).strip()
    if raw:
        try:
            value = float(raw)
        except ValueError:
            value = 30.0
    else:
        value = 30.0
    return max(5.0, min(300.0, value))


def _shared_instagram_catalog_delay_seconds(
    *,
    base_delay: float,
    success_streak: int,
    discovery: bool = False,
) -> float:
    """Catalog-specific adaptive delay tiers. Derives from SOCIAL_INSTAGRAM_DELAY_SEC base.

    Discovery mode uses shorter delays since it only reads cursors, not persisting posts.
    """
    if discovery:
        if success_streak >= 10:
            return round(base_delay, 4)
        if success_streak >= 3:
            return round(base_delay * 2, 4)
        return round(base_delay * 3, 4)
    if success_streak >= 20:
        return round(base_delay, 4)
    if success_streak >= 5:
        return round(base_delay * 2, 4)
    return round(base_delay * 3, 4)


def _instagram_post_payload(
    context: SeasonContext | None,
    *,
    job_id: str | None,
    account: str,
    post: Any,
    conn: Any | None = None,
    existing_row: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Build the legacy social.instagram_posts payload for one post."""
    _sync_core_overrides()
    shortcode = str(getattr(post, "shortcode", "") or "").strip()
    posted_at = _instagram_catalog_posted_at(getattr(post, "taken_at", None))
    media_urls = [str(url).strip() for url in (getattr(post, "media_urls", []) or []) if str(url).strip()]
    thumbnail_url = str(getattr(post, "thumbnail_url", "") or "").strip() or (media_urls[0] if media_urls else None)
    profile_tags = _as_text_list(getattr(post, "profile_tags", []))
    collaborators = _as_text_list(getattr(post, "collaborators", []))
    hashtags = _as_text_list(getattr(post, "hashtags", []), strip_prefix="#")
    mentions = _as_text_list(getattr(post, "mentions", []), prefix="@", strip_prefix="@")
    hosted_media_urls = [str(url).strip() for url in (getattr(post, "hosted_media_urls", []) or []) if str(url).strip()]
    hosted_thumbnail_url = str(getattr(post, "hosted_thumbnail_url", "") or "").strip() or None
    media_mirror_status = getattr(post, "media_mirror_status", None)
    media_mirror_error = getattr(post, "media_mirror_error", None)
    media_mirror_attempt_count = getattr(post, "media_mirror_attempt_count", None)
    media_mirror_last_attempt_at = _coerce_dt(getattr(post, "media_mirror_last_attempt_at", None))
    media_mirror_last_job_id = str(getattr(post, "media_mirror_last_job_id", "") or "").strip() or None
    metadata_scraped_at = _coerce_dt(getattr(post, "metadata_scraped_at", None))
    duration_seconds = getattr(post, "duration_seconds", None)
    try:
        duration_seconds = int(duration_seconds) if duration_seconds is not None else None
    except (TypeError, ValueError):
        duration_seconds = None
    raw_data_raw = post.to_dict() if hasattr(post, "to_dict") else {}
    raw_data = dict(raw_data_raw) if isinstance(raw_data_raw, dict) else {}
    incoming_raw_data_is_thin = _instagram_raw_data_is_thin_comments_header(raw_data)
    existing_row = dict(existing_row or {})
    existing_views = 0
    if shortcode and not existing_row:
        with pg.db_cursor(conn=conn) as cur:
            existing_row = (
                pg.fetch_one_with_cursor(
                    cur,
                    (
                        "select coalesce(p.views, 0)::bigint as views, "
                        "coalesce(s.raw_data, p.raw_data, '{}'::jsonb) as raw_data, "
                        "coalesce(s.child_posts_data, p.child_posts_data, '[]'::jsonb) as child_posts_data, "
                        "coalesce(s.asset_manifest, p.asset_manifest, '{}'::jsonb) as asset_manifest, "
                        "coalesce(p.media_urls, '[]'::jsonb) as media_urls, "
                        "nullif(p.thumbnail_url, '') as thumbnail_url "
                        "from social.instagram_posts p "
                        "left join social.instagram_post_payloads s on s.post_id = p.id "
                        "where p.shortcode = %s limit 1"
                    ),
                    [shortcode],
                )
                or {}
            )
    existing_views = _normalize_non_negative_int(existing_row.get("views"))
    if incoming_raw_data_is_thin:
        existing_raw_data = existing_row.get("raw_data")
        if isinstance(existing_raw_data, dict) and existing_raw_data:
            raw_data = dict(existing_raw_data)
        existing_media_urls = _as_text_list(existing_row.get("media_urls"))
        if existing_media_urls and not media_urls:
            media_urls = existing_media_urls
        if not thumbnail_url:
            existing_thumbnail_url = str(existing_row.get("thumbnail_url") or "").strip()
            thumbnail_url = existing_thumbnail_url or (media_urls[0] if media_urls else None)
    incoming_views_missing = object()
    incoming_views = getattr(post, "video_views_observed", incoming_views_missing)
    if incoming_views is incoming_views_missing:
        incoming_views = getattr(post, "video_views", None)
    normalized_incoming_views: int | None
    if incoming_views is None:
        normalized_incoming_views = None
    else:
        normalized_incoming_views = _normalize_non_negative_int(incoming_views)
    resolved_views = (
        existing_views if normalized_incoming_views is None else max(existing_views, normalized_incoming_views)
    )
    resolved_repost_count = _instagram_repost_count_from_post(post, raw_data)
    view_metrics_source = str(getattr(post, "video_views_source", "") or "").strip() or None
    view_metrics_observed_at = _coerce_dt(getattr(post, "metadata_scraped_at", None)) or _now_utc()
    raw_candidates = getattr(post, "video_views_raw_candidates", None)
    view_metrics_payload: dict[str, Any] = {
        "observed_at": view_metrics_observed_at.isoformat(),
    }
    if view_metrics_source:
        view_metrics_payload["source"] = view_metrics_source
    if normalized_incoming_views is not None:
        view_metrics_payload["observed_count"] = normalized_incoming_views
    if isinstance(raw_candidates, list) and raw_candidates:
        compact_candidates: list[dict[str, Any]] = []
        for item in raw_candidates[:10]:
            if not isinstance(item, dict):
                continue
            compact_candidates.append(
                {
                    "source": str(item.get("source") or "").strip() or None,
                    "raw": item.get("raw"),
                    "parsed": (
                        _normalize_non_negative_int(item.get("parsed")) if item.get("parsed") is not None else None
                    ),
                }
            )
        if compact_candidates:
            view_metrics_payload["raw_candidates"] = compact_candidates
    if len(view_metrics_payload) > 1:
        raw_data["view_metrics"] = view_metrics_payload
    media_retrieval_meta = getattr(post, "media_retrieval_meta", None)
    if isinstance(media_retrieval_meta, dict):
        raw_data["media_retrieval_meta"] = {
            "selected_source": str(media_retrieval_meta.get("selected_source") or "") or None,
            "attempts": _normalize_instagram_mirror_attempts(media_retrieval_meta.get("attempts")),
        }

    facebook_crosspost_raw = getattr(post, "facebook_crosspost", None)
    if not isinstance(facebook_crosspost_raw, dict):
        raw_facebook_crosspost = raw_data.get("facebook_crosspost")
        facebook_crosspost_raw = raw_facebook_crosspost if isinstance(raw_facebook_crosspost, dict) else {}
    facebook_crosspost_payload: dict[str, Any] = dict(facebook_crosspost_raw or {})
    crosspost_observed_at = _coerce_dt(
        getattr(post, "facebook_crosspost_observed_at", None) or facebook_crosspost_payload.get("observed_at")
    )
    crosspost_source = (
        str(getattr(post, "facebook_crosspost_source", None) or facebook_crosspost_payload.get("source") or "").strip()
        or None
    )
    fb_comment_count = getattr(post, "fb_comment_count", None)
    if fb_comment_count is None:
        fb_comment_count = facebook_crosspost_payload.get("comments_count")
    fb_like_count = getattr(post, "fb_like_count", None)
    if fb_like_count is None:
        fb_like_count = facebook_crosspost_payload.get("likes_count")
    is_shared_to_fb = getattr(post, "is_shared_to_fb", None)
    if is_shared_to_fb is None:
        is_shared_to_fb = facebook_crosspost_payload.get("is_shared_to_fb")
    crosspost_metadata = getattr(post, "crosspost_metadata", None)
    if not isinstance(crosspost_metadata, dict):
        crosspost_metadata = facebook_crosspost_payload.get("metadata")
    if not isinstance(crosspost_metadata, dict):
        crosspost_metadata = {}
    social_context = getattr(post, "social_context", None)
    if not isinstance(social_context, dict):
        social_context = facebook_crosspost_payload.get("social_context")
    if not isinstance(social_context, dict):
        social_context = {}
    facebook_post_id = (
        str(getattr(post, "facebook_post_id", None) or facebook_crosspost_payload.get("post_id") or "").strip() or None
    )
    facebook_post_url = (
        str(getattr(post, "facebook_post_url", None) or facebook_crosspost_payload.get("post_url") or "").strip()
        or None
    )
    has_facebook_crosspost = any(
        value is not None and value != {} and value != ""
        for value in (
            fb_comment_count,
            fb_like_count,
            is_shared_to_fb,
            crosspost_metadata,
            social_context,
            facebook_post_id,
            facebook_post_url,
            crosspost_observed_at,
            crosspost_source,
        )
    )
    if has_facebook_crosspost:
        if fb_comment_count is not None:
            fb_comment_count = _normalize_non_negative_int(fb_comment_count)
        if fb_like_count is not None:
            fb_like_count = _normalize_non_negative_int(fb_like_count)
        facebook_crosspost_payload = {
            "comments_count": fb_comment_count,
            "likes_count": fb_like_count,
            "is_shared_to_fb": is_shared_to_fb if isinstance(is_shared_to_fb, bool) else None,
            "post_id": facebook_post_id,
            "post_url": facebook_post_url,
            "metadata": crosspost_metadata,
            "social_context": social_context,
            "observed_at": crosspost_observed_at.isoformat() if crosspost_observed_at is not None else None,
            "source": crosspost_source,
        }
        raw_data["facebook_crosspost"] = facebook_crosspost_payload

    reported_comments = max(
        _normalize_non_negative_int(getattr(post, "comments", 0)),
        _instagram_reported_comment_count_from_payload(raw_data),
    )
    payload = {
        "shortcode": shortcode,
        "media_id": getattr(post, "pk", None),
        "username": getattr(post, "username", account),
        "user_id": None,
        "caption": getattr(post, "caption", None),
        "media_type": getattr(post, "post_type", None),
        "media_urls": media_urls,
        "thumbnail_url": thumbnail_url,
        "likes": int(getattr(post, "likes", 0) or 0),
        "comments_count": reported_comments,
        "views": resolved_views,
        "posted_at": posted_at,
        "scraped_at": _now_utc(),
        "raw_data": raw_data,
        "source_account": account,
    }
    _apply_assignment_payload(payload, context)
    if job_id:
        payload["job_id"] = job_id

    # Serialize rich user detail objects
    tagged_users_detail_raw = getattr(post, "tagged_users_detail", []) or []
    tagged_users_detail = [u.to_dict() if hasattr(u, "to_dict") else u for u in tagged_users_detail_raw]
    collaborators_detail_raw = getattr(post, "collaborators_detail", []) or []
    collaborators_detail = [u.to_dict() if hasattr(u, "to_dict") else u for u in collaborators_detail_raw]
    owner_detail = getattr(post, "owner_detail", None)
    child_posts_data = getattr(post, "child_posts_data", []) or []
    asset_manifest = getattr(post, "asset_manifest", None)
    if incoming_raw_data_is_thin:
        if not child_posts_data:
            existing_child_posts_data = existing_row.get("child_posts_data")
            if isinstance(existing_child_posts_data, list):
                child_posts_data = existing_child_posts_data
        if not asset_manifest:
            existing_asset_manifest = existing_row.get("asset_manifest")
            if isinstance(existing_asset_manifest, dict):
                asset_manifest = existing_asset_manifest

    # Coerce video_duration
    video_duration_raw = getattr(post, "video_duration", None)
    try:
        video_duration = float(video_duration_raw) if video_duration_raw is not None else None
    except (TypeError, ValueError):
        video_duration = None
    caption_raw = _metadata_dict(raw_data.get("caption"))
    caption_id = getattr(post, "caption_id", None) or caption_raw.get("pk")
    caption_has_translation = getattr(post, "caption_has_translation", None)
    if caption_has_translation is None:
        caption_has_translation = caption_raw.get("has_translation")

    def _post_attr_or_raw(attr_name: str, raw_key: str) -> Any:
        value = getattr(post, attr_name, None)
        if value is not None:
            return value
        return raw_data.get(raw_key)

    optional_payload = {
        "post_format": getattr(post, "post_format", None),
        "profile_tags": profile_tags,
        "collaborators": collaborators,
        "hashtags": hashtags,
        "mentions": mentions,
        "duration_seconds": duration_seconds,
        "metadata_source": getattr(post, "metadata_source", None),
        "metadata_scraped_at": metadata_scraped_at,
        "metadata_last_attempted_at": _coerce_dt(getattr(post, "metadata_last_attempted_at", None)),
        "metadata_last_failed_at": _coerce_dt(getattr(post, "metadata_last_failed_at", None)),
        "metadata_consecutive_failures": _normalize_non_negative_int(getattr(post, "metadata_consecutive_failures", 0)),
        "metadata_error": getattr(post, "metadata_error", None),
        # Enhanced metadata (migration 0147)
        "tagged_users_detail": tagged_users_detail,
        "collaborators_detail": collaborators_detail,
        "owner_profile_pic_url": (
            owner_detail.profile_pic_url
            if owner_detail and hasattr(owner_detail, "profile_pic_url")
            else getattr(post, "owner_profile_pic_url", None)
        ),
        "owner_full_name": (
            owner_detail.full_name
            if owner_detail and hasattr(owner_detail, "full_name")
            else getattr(post, "owner_full_name", None)
        ),
        "owner_is_verified": (
            owner_detail.is_verified
            if owner_detail and hasattr(owner_detail, "is_verified")
            else getattr(post, "owner_is_verified", None)
        ),
        "product_type": getattr(post, "product_type", None),
        "video_play_count": getattr(post, "video_play_count", None),
        "alt_text": getattr(post, "alt_text", None),
        "width": getattr(post, "width", None),
        "height": getattr(post, "height", None),
        "is_comments_disabled": getattr(post, "is_comments_disabled", None),
        "source_input_url": _post_attr_or_raw("input_url", "inputUrl"),
        "source_post_id": _post_attr_or_raw("source_post_id", "pk") or raw_data.get("id") or getattr(post, "pk", None),
        "permalink": _post_attr_or_raw("url", "url"),
        "caption_id": caption_id,
        "caption_is_edited": _post_attr_or_raw("caption_is_edited", "caption_is_edited"),
        "caption_has_translation": caption_has_translation,
        "owner_user_id": _instagram_post_owner_id(post, raw_data),
        "owner_username": getattr(post, "owner_username", None) or getattr(post, "username", None) or account,
        "owner_profile_pic_url_hd": getattr(post, "owner_profile_pic_url_hd", None)
        or (owner_detail.profile_pic_url_hd if owner_detail and hasattr(owner_detail, "profile_pic_url_hd") else None),
        "location_id": _post_attr_or_raw("location_id", "locationId"),
        "location_name": _post_attr_or_raw("location_name", "locationName"),
        "location_raw": getattr(post, "location_raw", None) or raw_data.get("location") or {},
        "original_width": _post_attr_or_raw("original_width", "original_width"),
        "original_height": _post_attr_or_raw("original_height", "original_height"),
        "like_and_view_counts_disabled": _post_attr_or_raw(
            "like_and_view_counts_disabled", "like_and_view_counts_disabled"
        ),
        "comments_disabled": _post_attr_or_raw("comments_disabled", "comments_disabled"),
        "commenting_disabled_for_viewer": _post_attr_or_raw(
            "commenting_disabled_for_viewer", "commenting_disabled_for_viewer"
        ),
        "is_paid_partnership": _post_attr_or_raw("is_paid_partnership", "is_paid_partnership"),
        "is_advertisement": _post_attr_or_raw("is_advertisement", "isAdvertisement"),
        "can_viewer_reshare": _post_attr_or_raw("can_viewer_reshare", "can_viewer_reshare"),
        "has_audio": _post_attr_or_raw("has_audio", "has_audio"),
        "audio_url": getattr(post, "audio_url", None) or raw_data.get("audioUrl") or raw_data.get("audio_url"),
        "music_info": getattr(post, "music_info", None),
        "video_duration": video_duration,
        "child_posts_data": child_posts_data if child_posts_data else [],
    }
    if isinstance(asset_manifest, dict) and _instagram_posts_has_column("asset_manifest", conn=conn):
        optional_payload["asset_manifest"] = asset_manifest
    for key, value in optional_payload.items():
        if incoming_raw_data_is_thin and key in _INSTAGRAM_THIN_PRESERVE_OPTIONAL_KEYS and value is None:
            continue
        if _instagram_posts_has_column(key, conn=conn):
            payload[key] = value
    if resolved_repost_count is not None and _instagram_posts_has_column("media_repost_count", conn=conn):
        payload["media_repost_count"] = resolved_repost_count
    if has_facebook_crosspost:
        facebook_crosspost_columns = {
            "fb_comment_count": fb_comment_count,
            "fb_like_count": fb_like_count,
            "is_shared_to_fb": is_shared_to_fb if isinstance(is_shared_to_fb, bool) else None,
            "crosspost_metadata": crosspost_metadata,
            "social_context": social_context,
            "facebook_post_id": facebook_post_id,
            "facebook_post_url": facebook_post_url,
            "facebook_crosspost_observed_at": crosspost_observed_at,
            "facebook_crosspost_source": crosspost_source,
        }
        for key, value in facebook_crosspost_columns.items():
            if _instagram_posts_has_column(key, conn=conn):
                payload[key] = value

    # Preserve hosted URLs unless new hosted values are explicitly supplied.
    if hosted_thumbnail_url and _instagram_posts_has_column("hosted_thumbnail_url", conn=conn):
        payload["hosted_thumbnail_url"] = hosted_thumbnail_url
    if hosted_media_urls and _instagram_posts_has_column("hosted_media_urls", conn=conn):
        payload["hosted_media_urls"] = hosted_media_urls
    if _instagram_posts_has_column("media_mirror_status", conn=conn):
        payload["media_mirror_status"] = media_mirror_status
    if _instagram_posts_has_column("media_mirror_error", conn=conn):
        payload["media_mirror_error"] = media_mirror_error
    if media_mirror_attempt_count is not None and _instagram_posts_has_column("media_mirror_attempt_count", conn=conn):
        payload["media_mirror_attempt_count"] = max(0, int(media_mirror_attempt_count))
    if media_mirror_last_attempt_at is not None and _instagram_posts_has_column(
        "media_mirror_last_attempt_at", conn=conn
    ):
        payload["media_mirror_last_attempt_at"] = media_mirror_last_attempt_at
    if media_mirror_last_job_id and _instagram_posts_has_column("media_mirror_last_job_id", conn=conn):
        payload["media_mirror_last_job_id"] = media_mirror_last_job_id

    # Profile picture mirror fields (migration 0147)
    hosted_owner_pic = getattr(post, "hosted_owner_profile_pic_url", None)
    if hosted_owner_pic and _instagram_posts_has_column("hosted_owner_profile_pic_url", conn=conn):
        payload["hosted_owner_profile_pic_url"] = hosted_owner_pic
    hosted_tagged_pics = getattr(post, "hosted_tagged_profile_pics", None)
    if hosted_tagged_pics and _instagram_posts_has_column("hosted_tagged_profile_pics", conn=conn):
        payload["hosted_tagged_profile_pics"] = hosted_tagged_pics
    if _instagram_posts_has_column("profile_pic_mirror_status", conn=conn):
        payload["profile_pic_mirror_status"] = getattr(post, "profile_pic_mirror_status", None)
    if _instagram_posts_has_column("profile_pic_mirror_error", conn=conn):
        payload["profile_pic_mirror_error"] = getattr(post, "profile_pic_mirror_error", None)

    return payload


def _upsert_instagram_post(
    context: SeasonContext | None,
    *,
    job_id: str | None,
    account: str,
    post: Any,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    _sync_core_overrides()
    with _payload_sidecars.payload_write_transaction(
        conn,
        label="instagram_post_payload_dual_write",
    ) as tx_conn:
        payload = _instagram_post_payload(
            context,
            job_id=job_id,
            account=account,
            post=post,
            conn=tx_conn,
        )
        if payload is None:
            return None
        row = _pg_upsert("instagram_posts", payload, conflict_col="shortcode", conn=tx_conn)
        sidecar = _payload_sidecars.post_sidecar_payload(legacy_row=row or {}, payload=payload)
        if sidecar is not None:
            _payload_sidecars.upsert_post_payloads([sidecar], conn=tx_conn)
        _sync_instagram_canonical_post(legacy_row=row, payload=payload, post=post, conn=tx_conn)
        return row


def _batch_upsert_instagram_posts(
    context: SeasonContext | None,
    *,
    job_id: str | None,
    account: str,
    posts: list[Any],
    conn: Any | None = None,
) -> list[dict[str, Any]]:
    """Batch upsert legacy social.instagram_posts rows and sync canonical rows."""
    _sync_core_overrides()
    payload_builder = _room_callable(
        "_instagram_post_payload",
        _instagram_post_payload,
    )
    with _payload_sidecars.payload_write_transaction(conn, label="instagram_post_payload_batch_dual_write") as tx_conn:
        existing_by_shortcode: dict[str, dict[str, Any]] = {}
        local_payload_builder = _LOCAL_ROOM_FUNCTIONS.get("_instagram_post_payload", _instagram_post_payload)
        if payload_builder is local_payload_builder:
            existing_by_shortcode = _payload_sidecars.fetch_post_preservation_rows(
                [str(getattr(post, "shortcode", "") or "") for post in posts],
                conn=tx_conn,
            )
        records: list[tuple[dict[str, Any], Any]] = []
        for post in posts:
            builder_kwargs: dict[str, Any] = {
                "job_id": job_id,
                "account": account,
                "post": post,
                "conn": tx_conn,
            }
            if payload_builder is local_payload_builder:
                shortcode = str(getattr(post, "shortcode", "") or "").strip()
                builder_kwargs["existing_row"] = existing_by_shortcode.get(shortcode, {})
            payload = payload_builder(context, **builder_kwargs)
            if payload is not None:
                records.append((payload, post))
        if not records:
            return []

        rows: list[dict[str, Any]] = []
        for index in range(0, len(records), _INSTAGRAM_POST_BATCH_SIZE):
            chunk = records[index : index + _INSTAGRAM_POST_BATCH_SIZE]
            records_by_columns: dict[tuple[str, ...], list[tuple[dict[str, Any], Any]]] = {}
            for payload, post in chunk:
                records_by_columns.setdefault(tuple(payload.keys()), []).append((payload, post))

            for grouped_records in records_by_columns.values():
                payloads = [payload for payload, _post in grouped_records]
                grouped_rows = _pg_upsert_many("instagram_posts", payloads, conflict_col="shortcode", conn=tx_conn)
                records_by_shortcode = {
                    str(payload.get("shortcode") or "").strip(): (payload, post) for payload, post in grouped_records
                }
                sidecars: list[dict[str, Any]] = []
                for row in grouped_rows:
                    shortcode = str((row or {}).get("shortcode") or "").strip()
                    record = records_by_shortcode.get(shortcode)
                    if record is not None:
                        payload, post = record
                        sidecar = _payload_sidecars.post_sidecar_payload(legacy_row=row or {}, payload=payload)
                        if sidecar is not None:
                            sidecars.append(sidecar)
                        _sync_instagram_canonical_post(legacy_row=row, payload=payload, post=post, conn=tx_conn)
                    rows.append(row)
                _payload_sidecars.upsert_post_payloads(sidecars, conn=tx_conn)
        return rows


def _upsert_shared_catalog_instagram_post(
    *,
    run_id: str | None,
    account_handle: str,
    post: Any,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    _sync_core_overrides()
    with _payload_sidecars.payload_write_transaction(
        conn,
        label="instagram_catalog_payload_dual_write",
    ) as tx_conn:
        shortcode = str(getattr(post, "shortcode", "") or "").strip()
        existing_row = None
        if shortcode and tx_conn is not None:
            existing_row = _payload_sidecars.fetch_catalog_preservation_rows([shortcode], conn=tx_conn).get(shortcode)
        payload = _shared_catalog_instagram_post_payload(
            run_id=run_id,
            account_handle=account_handle,
            post=post,
            existing_row=existing_row,
        )
        if payload is None:
            return None
        row = _pg_upsert(
            PLATFORM_CATALOG_POST_TABLES["instagram"],
            payload,
            conflict_col="source_id",
            conn=tx_conn,
        )
        sidecar = _payload_sidecars.catalog_sidecar_payload(legacy_row=row or {}, payload=payload)
        if sidecar is not None:
            _payload_sidecars.upsert_catalog_payloads([sidecar], conn=tx_conn)
        if row:
            _sync_instagram_catalog_post_collaborators(row, conn=tx_conn)
        return row


def _shared_catalog_instagram_post_payload(
    *,
    run_id: str | None,
    account_handle: str,
    post: Any,
    existing_row: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Build the upsert payload dict for one Instagram catalog post (no DB call)."""
    _sync_core_overrides()
    shortcode = str(getattr(post, "shortcode", "") or "").strip()
    if not shortcode:
        return None
    media_urls = [str(url).strip() for url in (getattr(post, "media_urls", []) or []) if str(url).strip()]
    raw_data = post.to_dict() if hasattr(post, "to_dict") else {}
    raw_data = raw_data if isinstance(raw_data, dict) else {}
    child_posts_data = getattr(post, "child_posts_data", None)
    if _instagram_raw_data_is_thin_comments_header(raw_data) and existing_row:
        existing_raw_data = existing_row.get("raw_data")
        if isinstance(existing_raw_data, dict) and existing_raw_data:
            raw_data = dict(existing_raw_data)
        if not child_posts_data:
            existing_children = existing_row.get("child_posts_data")
            if isinstance(existing_children, list) and existing_children:
                child_posts_data = list(existing_children)
    resolved_repost_count = _instagram_repost_count_from_post(post, raw_data)
    payload = _shared_catalog_payload_base(
        source_id=shortcode,
        account_handle=account_handle,
        posted_at=_instagram_catalog_posted_at(getattr(post, "taken_at", None)),
        permalink=_shared_catalog_post_url(
            "instagram",
            account_handle=account_handle,
            source_id=shortcode,
            explicit=_first_non_empty_str(
                getattr(post, "post_url", None),
                getattr(post, "permalink_url", None),
            ),
            post_format=_first_non_empty_str(getattr(post, "post_format", None), getattr(post, "post_type", None)),
            media_type=getattr(post, "media_type", None),
            raw_data=raw_data,
        ),
        caption=str(getattr(post, "caption", "") or "") or None,
        media_type=str(getattr(post, "post_type", "") or "").strip() or None,
        media_urls=media_urls,
        thumbnail_url=(
            str(getattr(post, "thumbnail_url", "") or "").strip() or (media_urls[0] if media_urls else None)
        ),
        hashtags=_as_text_list(getattr(post, "hashtags", []), strip_prefix="#"),
        mentions=_as_text_list(getattr(post, "mentions", []), prefix="@", strip_prefix="@"),
        collaborators=_as_text_list(getattr(post, "collaborators", []), prefix="@", strip_prefix="@"),
        profile_tags=_as_text_list(getattr(post, "profile_tags", []), prefix="@", strip_prefix="@"),
        likes=getattr(post, "likes", None),
        comments_count=getattr(post, "comments", None),
        views=getattr(post, "video_views_observed", None),
        shares=resolved_repost_count,
        raw_data=raw_data,
        run_id=run_id,
        music_info=getattr(post, "music_info", None),
        audio_url=str(getattr(post, "audio_url", "") or "").strip() or None,
        paid_partnership=bool(getattr(post, "sponsored", False)),
        child_posts_data=child_posts_data,
        owner_username=str(getattr(post, "username", "") or "").strip() or None,
        video_play_count=getattr(post, "video_play_count", None),
        video_duration=getattr(post, "video_duration", None),
    )
    return _apply_instagram_catalog_metric_semantics(payload, post=post, raw_data=raw_data)


def _batch_upsert_shared_catalog_instagram_posts(
    *,
    run_id: str | None,
    account_handle: str,
    posts: list[Any],
    conn: Any | None = None,
) -> list[dict[str, Any]]:
    """Batch upsert catalog posts using _pg_upsert_many (execute_values)."""
    _sync_core_overrides()
    payload_builder = _room_callable(
        "_shared_catalog_instagram_post_payload",
        _shared_catalog_instagram_post_payload,
    )
    production_upsert = getattr(_pg_upsert_many, "__module__", "") == _core.__name__
    transaction = (
        _payload_sidecars.payload_write_transaction(conn, label="instagram_catalog_payload_batch_dual_write")
        if conn is not None or production_upsert
        else nullcontext(conn)
    )
    with transaction as tx_conn:
        existing_by_source_id: dict[str, dict[str, Any]] = {}
        local_payload_builder = _LOCAL_ROOM_FUNCTIONS.get(
            "_shared_catalog_instagram_post_payload", _shared_catalog_instagram_post_payload
        )
        if payload_builder is local_payload_builder and tx_conn is not None:
            existing_by_source_id = _payload_sidecars.fetch_catalog_preservation_rows(
                [str(getattr(post, "shortcode", "") or "") for post in posts],
                conn=tx_conn,
            )
        payloads: list[dict[str, Any]] = []
        for post in posts:
            builder_kwargs: dict[str, Any] = {
                "run_id": run_id,
                "account_handle": account_handle,
                "post": post,
            }
            if payload_builder is local_payload_builder:
                source_id = str(getattr(post, "shortcode", "") or "").strip()
                builder_kwargs["existing_row"] = existing_by_source_id.get(source_id)
            payload = payload_builder(**builder_kwargs)
            if payload is not None:
                payloads.append(payload)
        if not payloads:
            return []
        rows: list[dict[str, Any]] = []
        for index in range(0, len(payloads), _INSTAGRAM_POST_BATCH_SIZE):
            chunk = payloads[index : index + _INSTAGRAM_POST_BATCH_SIZE]
            payloads_by_columns: dict[tuple[str, ...], list[dict[str, Any]]] = {}
            for payload in chunk:
                payloads_by_columns.setdefault(tuple(payload.keys()), []).append(payload)
            for grouped_payloads in payloads_by_columns.values():
                grouped_rows = _pg_upsert_many(
                    PLATFORM_CATALOG_POST_TABLES["instagram"],
                    grouped_payloads,
                    conflict_col="source_id",
                    conn=tx_conn,
                )
                payload_by_source_id = {
                    str(payload.get("source_id") or "").strip(): payload for payload in grouped_payloads
                }
                sidecars = [
                    sidecar
                    for row in grouped_rows
                    if (
                        sidecar := _payload_sidecars.catalog_sidecar_payload(
                            legacy_row=row,
                            payload=payload_by_source_id.get(str(row.get("source_id") or "").strip(), {}),
                        )
                    )
                    is not None
                ]
                _payload_sidecars.upsert_catalog_payloads(sidecars, conn=tx_conn)
                rows.extend(grouped_rows)
        for row in rows:
            _sync_instagram_catalog_post_collaborators(row, conn=tx_conn)
        return rows


def _build_instagram_scraper_with_auth_fallback(
    *,
    browser_account_id: str | None = None,
    caller_context: str | None = None,
    require_validation: bool | None = None,
    allow_public_fallback: bool = True,
):
    from trr_backend.socials.instagram import InstagramScraper, build_authenticated_instagram_scraper

    effective_require_validation = (
        require_validation if require_validation is not None else _env_truthy("INSTAGRAM_AUTH_RESOLVER_V2")
    )
    scraper = build_authenticated_instagram_scraper(
        browser_account_id=browser_account_id,
        caller_context=caller_context,
        require_validation=effective_require_validation,
    )
    if scraper is not None or not allow_public_fallback:
        return scraper
    return InstagramScraper(
        cookies={},
        browser_account_id=_sanitize_instagram_browser_account_id(browser_account_id),
    )


def _build_shared_instagram_scraper(*, authenticated: bool = False, browser_account_id: str | None = None):
    from trr_backend.socials.instagram import InstagramScraper

    if authenticated:
        return _build_instagram_scraper_with_auth_fallback(
            browser_account_id=browser_account_id,
            caller_context="shared_instagram_scraper",
            allow_public_fallback=False,
        )

    # Shared-account catalog backfills prefer the warmed public path first to
    # avoid depending on auth cookies when the public transport is healthy.
    return InstagramScraper(cookies={}, browser_account_id=browser_account_id)


def _shared_instagram_graphql_page_has_posts(data: Mapping[str, Any] | None) -> bool:
    connection = ((data or {}).get("data") or {}).get("xdt_api__v1__feed__user_timeline_graphql_connection") or {}
    return bool(connection.get("edges") or [])


def _shared_instagram_graphql_candidate_scrapers(
    *,
    public_scraper: Any,
    auth_scraper: Any | None,
    preferred_transport: str | None,
    allow_public_fallback: bool = True,
) -> list[tuple[str, Any]]:
    candidates: list[tuple[str, Any]] = []
    normalized_preferred = str(preferred_transport or "").strip().lower()
    if normalized_preferred == "authenticated" and auth_scraper is not None:
        candidates.append(("authenticated", auth_scraper))
    if allow_public_fallback:
        candidates.append(("public", public_scraper))
    if auth_scraper is not None and normalized_preferred != "authenticated":
        candidates.append(("authenticated", auth_scraper))
    elif not candidates:
        candidates.append(("public", public_scraper))
    return candidates


def _shared_instagram_graphql_empty_page_meta(
    *,
    scraper: Any,
    data: Mapping[str, Any] | None,
    cursor: str | None,
    transport: str,
) -> dict[str, Any]:
    meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
    total_posts = None
    if data:
        try:
            total_posts = scraper._extract_profile_total_posts(data, source="graphql")
        except Exception:  # noqa: BLE001
            total_posts = None
    if total_posts is not None:
        meta["total_posts"] = total_posts
    meta.setdefault(
        "error_code",
        "instagram_graphql_cursor_empty_page" if cursor else "instagram_graphql_initial_empty_page",
    )
    meta.setdefault("error_class", "EmptyGraphQLPage")
    meta.setdefault("retryable", True)
    meta["graphql_cursor"] = str(cursor or "").strip() or None
    meta["transport"] = transport
    return meta


def _fetch_shared_instagram_graphql_page(
    *,
    account_handle: str,
    cursor: str | None,
    delay_seconds: float,
    public_scraper: Any,
    auth_scraper: Any | None = None,
    preferred_transport: str | None = None,
    allow_public_fallback: bool = True,
    page_size: int | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any], str | None]:
    last_meta: dict[str, Any] = {}
    chosen_transport: str | None = None
    for transport, scraper in _shared_instagram_graphql_candidate_scrapers(
        public_scraper=public_scraper,
        auth_scraper=auth_scraper,
        preferred_transport=preferred_transport,
        allow_public_fallback=allow_public_fallback,
    ):
        fetch_kwargs: dict[str, Any] = {
            "fast_mode": True,
            "allow_browser_fallback": False,
            "allow_recovery": False,
        }
        if page_size is not None:
            fetch_kwargs["page_size"] = page_size
        fetch_kwargs["request_timeout"] = _shared_instagram_catalog_graphql_request_timeout()
        data = scraper.fetch_posts_graphql(account_handle, cursor, delay_seconds, **fetch_kwargs)
        if data and _shared_instagram_graphql_page_has_posts(data):
            meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
            total_posts = scraper._extract_profile_total_posts(data, source="graphql")
            if total_posts is not None:
                meta["total_posts"] = total_posts
            meta["transport"] = transport
            return data, meta, transport
        if data:
            last_meta = _shared_instagram_graphql_empty_page_meta(
                scraper=scraper,
                data=data,
                cursor=cursor,
                transport=transport,
            )
        else:
            last_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
            last_meta["transport"] = transport
        chosen_transport = transport
    return None, last_meta, chosen_transport


def _shared_instagram_frontier_auth_validation(config: Mapping[str, Any] | None = None) -> tuple[bool, str | None]:
    _sync_core_overrides()
    metadata = _metadata_dict(config)
    if metadata.get("frontier_auth_allowed") is True:
        return True, None
    if metadata.get("frontier_auth_allowed") is False:
        return False, str(metadata.get("frontier_auth_reason") or "").strip().lower() or None

    # Frontier bootstrap/resume jobs run on remote workers. They need a quick
    # auth capability check, not the heavier confirmed repair path that can block
    # long enough to trip stale-heartbeat recovery before the resume handoff.
    cookies = _load_instagram_cookies_from_sources()
    if not cookies.get("sessionid"):
        return False, "sessionid_missing"
    is_valid, validation_reason = _validate_instagram_cookie_health(cookies)
    if not is_valid:
        return False, validation_reason
    return True, None


def _shared_instagram_frontier_auth_state(
    config: Mapping[str, Any] | None = None,
    *,
    frontier_metadata: Mapping[str, Any] | None = None,
) -> tuple[bool, str | None]:
    _sync_core_overrides()
    metadata = _metadata_dict(config)
    if metadata.get("frontier_auth_allowed") is True:
        return True, None
    if metadata.get("frontier_auth_allowed") is False:
        return False, str(metadata.get("frontier_auth_reason") or "").strip().lower() or None

    normalized_frontier_metadata = _metadata_dict(frontier_metadata)
    if normalized_frontier_metadata.get("auth_allowed") is True:
        return True, None
    if normalized_frontier_metadata.get("auth_allowed") is False:
        return False, str(normalized_frontier_metadata.get("auth_reason") or "").strip().lower() or None

    return True, None


def _shared_instagram_frontier_auth_error_code(auth_reason: str | None) -> str:
    normalized_reason = str(auth_reason or "").strip().lower()
    if normalized_reason == "checkpoint_required":
        return "instagram_graphql_checkpoint_required"
    if normalized_reason == "sessionid_missing":
        return "instagram_graphql_sessionid_missing"
    if normalized_reason == "graphql_validation_failed":
        return "instagram_graphql_auth_validation_failed"
    if normalized_reason:
        return f"instagram_graphql_auth_{normalized_reason}"
    return "instagram_graphql_auth_blocked"


def _shared_instagram_frontier_transport_preferences(
    config: Mapping[str, Any] | None,
    *,
    auth_allowed: bool,
) -> tuple[str, bool]:
    metadata = _metadata_dict(config)
    preferred_transport = (
        str(metadata.get("transport_preference") or metadata.get("frontier_transport") or "").strip().lower()
    )
    if preferred_transport not in {"authenticated", "public"}:
        preferred_transport = "authenticated" if auth_allowed else "public"

    configured_allow_public_fallback = metadata.get("allow_public_transport_fallback")
    if isinstance(configured_allow_public_fallback, bool):
        allow_public_fallback = configured_allow_public_fallback
    else:
        allow_public_fallback = not (preferred_transport == "authenticated" and auth_allowed)
    return preferred_transport, allow_public_fallback


def _shared_instagram_posted_at_bounds(posts: Sequence[Any]) -> tuple[datetime | None, datetime | None]:
    oldest_posted_at: datetime | None = None
    newest_posted_at: datetime | None = None
    for post in posts:
        posted_at = _instagram_catalog_posted_at(getattr(post, "taken_at", None))
        if posted_at is None:
            continue
        if oldest_posted_at is None or posted_at < oldest_posted_at:
            oldest_posted_at = posted_at
        if newest_posted_at is None or posted_at > newest_posted_at:
            newest_posted_at = posted_at
    return oldest_posted_at, newest_posted_at


def _shared_instagram_graphql_delay_seconds(*, jitter: bool = False) -> float:
    base_delay = float((os.getenv("SOCIAL_INSTAGRAM_DELAY_SEC") or "").strip() or "0.15")
    if not jitter:
        return base_delay
    seed = int(time_module.time() * 1000) % 9
    return base_delay + (0.03 * seed)


def _shared_instagram_account_lock_key(account_handle: str) -> int:
    normalized_account = str(account_handle or "").strip().lower() or "instagram"
    return int(hashlib.md5(f"instagram-account:{normalized_account}".encode()).hexdigest()[:15], 16) % (2**31)


def _shared_instagram_account_lock_max_attempts() -> int:
    raw_value = str(os.getenv("SOCIAL_INSTAGRAM_ACCOUNT_LOCK_MAX_ATTEMPTS") or "").strip()
    if raw_value:
        return max(1, _normalize_non_negative_int(raw_value) or 1)
    if os.getenv("PYTEST_CURRENT_TEST"):
        return 2
    return 40


def _shared_instagram_account_lock_wait_seconds() -> float:
    raw_value = str(os.getenv("SOCIAL_INSTAGRAM_ACCOUNT_LOCK_WAIT_SECONDS") or "").strip()
    if raw_value:
        try:
            return max(0.0, float(raw_value))
        except ValueError:
            logger.warning("Invalid SOCIAL_INSTAGRAM_ACCOUNT_LOCK_WAIT_SECONDS=%r; using default", raw_value)
    if os.getenv("PYTEST_CURRENT_TEST"):
        return 0.0
    return 15.0


def _shared_instagram_account_lock_heartbeat(
    progress_cb: Callable[[dict[str, Any]], None] | None,
) -> Callable[[dict[str, Any]], None] | None:
    if progress_cb is None:
        return None

    def _emit(payload: dict[str, Any]) -> None:
        progress_cb(
            {
                "phase": "account_lock_wait",
                "pages_scanned": 0,
                "posts_checked": 0,
                "matched_posts": 0,
                "saved_posts": 0,
                **payload,
            }
        )

    return _emit


@contextmanager
def _shared_instagram_account_execution(
    account_handle: str,
    *,
    heartbeat_cb: Callable[[dict[str, Any]], None] | None = None,
    lock_scope: str | None = None,
):
    from trr_backend.socials.account_browser_sessions import AccountBrowserSessionManager

    browser_sessions = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))
    resolved_account = browser_sessions.resolve_account_id(account_handle, fallback_account_id="instagram")
    normalized_lock_scope = str(lock_scope or "").strip().lower()
    scoped_lock_account = f"{resolved_account}:{normalized_lock_scope}" if normalized_lock_scope else resolved_account
    lock_key = _shared_instagram_account_lock_key(scoped_lock_account)
    lock_label = f"instagram-account-lock:{scoped_lock_account[:48]}"

    with browser_sessions.execution_lock(scoped_lock_account):
        logger.info("[instagram-account-lock] waiting for account=%s lock=%s", resolved_account, lock_key)
        # Hold the session-level advisory lock on an autocommit connection so
        # long-running scrapes do not trip idle_in_transaction_session_timeout.
        # Retry with backoff instead of failing immediately, so concurrent jobs
        # for the same account wait for the lock rather than all failing.
        with pg.db_read_connection(label=lock_label, pool_name="social_control") as conn:
            max_lock_attempts = _shared_instagram_account_lock_max_attempts()
            wait_seconds = _shared_instagram_account_lock_wait_seconds()
            lock_acquired = False
            for attempt in range(max_lock_attempts):
                with pg.db_cursor(conn=conn, label=lock_label) as cur:
                    lock_row = (
                        pg.fetch_one_with_cursor(
                            cur,
                            "select pg_try_advisory_lock(%s) as locked",
                            [lock_key],
                        )
                        or {}
                    )
                if bool(lock_row.get("locked")):
                    lock_acquired = True
                    break
                if attempt < max_lock_attempts - 1:
                    if heartbeat_cb is not None:
                        try:
                            heartbeat_cb(
                                {
                                    "phase": "account_lock_wait",
                                    "account": resolved_account,
                                    "lock_scope": normalized_lock_scope or None,
                                    "lock_key": lock_key,
                                    "lock_attempt": attempt + 1,
                                    "lock_max_attempts": max_lock_attempts,
                                    "lock_wait_seconds": wait_seconds,
                                }
                            )
                        except Exception:  # noqa: BLE001
                            logger.debug(
                                "[instagram-account-lock] heartbeat callback failed for account=%s lock=%s",
                                resolved_account,
                                lock_key,
                                exc_info=True,
                            )
                    logger.info(
                        "[instagram-account-lock] lock busy, retrying in %.0fs (attempt %d/%d) account=%s",
                        wait_seconds,
                        attempt + 1,
                        max_lock_attempts,
                        resolved_account,
                    )
                    time_module.sleep(wait_seconds)
            if not lock_acquired:
                raise SharedStageRuntimeError(
                    f"Instagram account execution lock for @{resolved_account} is already held.",
                    error_code=SHARED_ACCOUNT_EXECUTION_LOCK_UNAVAILABLE_ERROR_CODE,
                    retryable=True,
                    runtime_metadata={
                        "platform": "instagram",
                        "account": resolved_account,
                        "lock_key": lock_key,
                    },
                )
            logger.info("[instagram-account-lock] acquired account=%s lock=%s", resolved_account, lock_key)
            if heartbeat_cb is not None:
                try:
                    heartbeat_cb(
                        {
                            "phase": "account_lock_acquired",
                            "account": resolved_account,
                            "lock_scope": normalized_lock_scope or None,
                            "lock_key": lock_key,
                        }
                    )
                except Exception:  # noqa: BLE001
                    logger.debug(
                        "[instagram-account-lock] acquired heartbeat callback failed for account=%s lock=%s",
                        resolved_account,
                        lock_key,
                        exc_info=True,
                    )
            try:
                yield resolved_account
            finally:
                try:
                    with pg.db_cursor(conn=conn, label=lock_label) as cur:
                        pg.fetch_one_with_cursor(cur, "select pg_advisory_unlock(%s) as unlocked", [lock_key])
                except Exception:  # noqa: BLE001
                    logger.debug(
                        "[instagram-account-lock] advisory unlock failed for account=%s lock=%s",
                        resolved_account,
                        lock_key,
                        exc_info=True,
                    )


def _fetch_shared_instagram_graphql_posts_page(
    *,
    account_handle: str,
    cursor: str | None,
    delay_seconds: float,
    preferred_transport: str | None,
    allow_public_fallback: bool = True,
    auth_allowed: bool,
) -> tuple[list[Any], dict[str, Any], dict[str, Any], str | None]:
    _sync_core_overrides()
    from trr_backend.socials.account_browser_sessions import (
        AccountBrowserSessionManager,
        BrowserSessionExecutionLockTimeout,
    )
    from trr_backend.socials.instagram import ScrapeConfig

    build_scraper = _room_callable("_build_shared_instagram_scraper", _build_shared_instagram_scraper)
    fetch_graphql_page = _room_callable(
        "_fetch_shared_instagram_graphql_page",
        _fetch_shared_instagram_graphql_page,
    )
    browser_sessions = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))
    try:
        with browser_sessions.execution_lock(
            account_handle,
            timeout_seconds=_shared_instagram_browser_session_lock_timeout_seconds(),
        ):
            public_scraper = build_scraper(browser_account_id=account_handle)
            auth_scraper = (
                build_scraper(authenticated=True, browser_account_id=account_handle) if auth_allowed else None
            )
            scrape_config = ScrapeConfig(
                username=account_handle,
                hashtags=[],
                delay_seconds=delay_seconds,
                max_pages=None,
            )
            data, page_meta, selected_transport = fetch_graphql_page(
                account_handle=account_handle,
                cursor=cursor,
                delay_seconds=delay_seconds,
                public_scraper=public_scraper,
                auth_scraper=auth_scraper,
                preferred_transport=preferred_transport,
                allow_public_fallback=allow_public_fallback,
            )
    except BrowserSessionExecutionLockTimeout as exc:
        selected_transport = str(preferred_transport or "").strip().lower() or None
        return (
            [],
            {},
            {
                "error_code": "instagram_browser_session_lock_timeout",
                "error_class": exc.__class__.__name__,
                "error_message": str(exc),
                "retryable": True,
                "graphql_cursor": str(cursor or "").strip() or None,
                "transport": selected_transport,
                "lock_timeout_seconds": exc.timeout_seconds,
            },
            selected_transport,
        )
    if not data:
        return [], {}, dict(page_meta or {}), selected_transport
    page_info: dict[str, Any] = {}
    page_posts: list[Any] = []
    for node, pi in public_scraper._iter_posts_from_graphql(data):
        page_info = pi
        page_posts.append(public_scraper._parse_post_node(node, scrape_config))
    meta = dict(page_meta or {})
    if page_posts and meta.get("total_posts") is None:
        total_posts = public_scraper._extract_profile_total_posts(data, source="graphql")
        if total_posts is not None:
            meta["total_posts"] = total_posts
    return page_posts, page_info, meta, selected_transport


def _discover_instagram_cursor_partitions(
    *,
    account_handle: str,
    runner_count: int,
    auth_allowed: bool,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    partition_callback: Callable[[SharedAccountCursorPartition, int | None], None] | None = None,
) -> tuple[list[SharedAccountCursorPartition], dict[str, Any]]:
    _sync_core_overrides()
    account_execution = _room_callable("_shared_instagram_account_execution", _shared_instagram_account_execution)
    build_scraper = _room_callable("_build_shared_instagram_scraper", _build_shared_instagram_scraper)
    fetch_graphql_page = _room_callable(
        "_fetch_shared_instagram_graphql_page",
        _fetch_shared_instagram_graphql_page,
    )
    with _context_manager_from_callable(
        account_execution,
        account_handle,
        heartbeat_cb=_shared_instagram_account_lock_heartbeat(progress_cb),
    ):
        public_scraper = build_scraper(browser_account_id=account_handle)
        auth_scraper = build_scraper(authenticated=True, browser_account_id=account_handle) if auth_allowed else None
        # Avoid the public web_profile_info endpoint here; on Modal it frequently
        # 429s before we ever reach the warmed public GraphQL path.
        total_posts = None
        expected_partition_count: int | None = None
        base_delay_seconds = float((os.getenv("SOCIAL_INSTAGRAM_DELAY_SEC") or "").strip() or "0.15")
        catalog_page_size = _shared_instagram_catalog_graphql_page_size()
        target_posts_per_shard = _catalog_full_history_posts_per_shard("instagram")
        partitions: list[SharedAccountCursorPartition] = []
        lanes = _catalog_backfill_run_scheduler_lanes(runner_count)
        shard_index = 0
        cursor: str | None = None
        partition_start_cursor: str | None = None
        partition_start_at: datetime | None = None
        partition_posts = 0
        partition_pages = 0
        pages_scanned = 0
        posts_checked = 0
        seen_cursors: set[str] = set()
        selected_transport: str | None = "authenticated" if auth_allowed else None
        last_retrieval_meta: dict[str, Any] = {}
        discovery_had_error = False

        consecutive_rate_limit_failures = 0
        max_rate_limit_retries = 3

        # Brief pause after auth validation probe before page 1.
        time_module.sleep(5.0)
        while True:
            pages_scanned += 1
            adaptive_delay = _shared_instagram_catalog_delay_seconds(
                base_delay=base_delay_seconds,
                success_streak=max(0, pages_scanned - 1),
                discovery=True,
            )
            data, page_meta, selected_transport = fetch_graphql_page(
                account_handle=account_handle,
                cursor=cursor,
                delay_seconds=adaptive_delay,
                public_scraper=public_scraper,
                auth_scraper=auth_scraper,
                preferred_transport=selected_transport,
                allow_public_fallback=True,
                page_size=catalog_page_size,
            )
            if page_meta:
                last_retrieval_meta = dict(page_meta)
            if not data:
                error_status = int(last_retrieval_meta.get("error_status_code") or 0)
                error_msg = str(last_retrieval_meta.get("error_message") or "").strip().lower()
                is_rate_limit = error_status in (401, 429) and "wait" in error_msg
                if is_rate_limit and consecutive_rate_limit_failures < max_rate_limit_retries:
                    consecutive_rate_limit_failures += 1
                    pages_scanned -= 1  # don't count failed page
                    backoff = float(60 * consecutive_rate_limit_failures)
                    logger.info(
                        "Discovery rate-limited (attempt %d/%d), backing off %.0fs before retry",
                        consecutive_rate_limit_failures,
                        max_rate_limit_retries,
                        backoff,
                    )
                    time_module.sleep(backoff)
                    continue
                discovery_had_error = bool(
                    last_retrieval_meta.get("error_code") or last_retrieval_meta.get("error_class")
                )
                if not discovery_had_error and (cursor or posts_checked > 0):
                    last_retrieval_meta.setdefault("error_code", "instagram_graphql_discovery_cursor_empty_page")
                    last_retrieval_meta.setdefault("error_class", "InstagramGraphQLEmptyOrErrorPage")
                    last_retrieval_meta.setdefault("retryable", True)
                    last_retrieval_meta.setdefault("graphql_cursor", str(cursor or "").strip() or None)
                    discovery_had_error = True
                if discovery_had_error:
                    last_retrieval_meta.setdefault("error_code", "instagram_graphql_discovery_page_failed")
                    last_retrieval_meta.setdefault("error_class", "InstagramDiscoveryPageFailed")
                    last_retrieval_meta.setdefault("retryable", True)
                    last_retrieval_meta.setdefault("graphql_cursor", str(cursor or "").strip() or None)
                    if partition_posts > 0:
                        partition_key = _shared_account_partition_key(
                            run_id="pending",
                            platform="instagram",
                            account_handle=account_handle,
                            shard_index=shard_index,
                            cursor_start=partition_start_cursor,
                            cursor_end=None,
                        )
                        partition = SharedAccountCursorPartition(
                            partition_key=partition_key,
                            shard_index=shard_index,
                            shard_total=0,
                            runner_lane=lanes[shard_index % len(lanes)],
                            cursor_start=partition_start_cursor,
                            cursor_end=None,
                            boundary_start_at=partition_start_at,
                            boundary_end_at=None,
                            metadata={
                                "pages_scanned": partition_pages,
                                "posts_discovered": partition_posts,
                                "incomplete_coverage": True,
                                "partial_discovery": True,
                                "discovery_error_code": last_retrieval_meta.get("error_code"),
                                "graphql_cursor": last_retrieval_meta.get("graphql_cursor"),
                            },
                        )
                        partitions.append(partition)
                        if partition_callback:
                            partition_callback(partition, expected_partition_count)
                break
            consecutive_rate_limit_failures = 0
            page_info: dict[str, Any] = {}
            timestamps: list[int] = []
            page_posts = 0
            for node, pi in public_scraper._iter_posts_from_graphql(data):
                page_info = pi
                timestamp = public_scraper._extract_timestamp(node)
                if timestamp > 0:
                    timestamps.append(timestamp)
                page_posts += 1
            if page_posts <= 0:
                break
            if total_posts is None:
                total_posts = public_scraper._extract_profile_total_posts(data, source="graphql")
                if total_posts is not None and total_posts > 0 and target_posts_per_shard > 0:
                    expected_partition_count = -(-total_posts // target_posts_per_shard)
            posts_checked += page_posts
            partition_posts += page_posts
            partition_pages += 1
            page_newest_at = datetime.fromtimestamp(max(timestamps), tz=UTC) if timestamps else None
            page_oldest_at = datetime.fromtimestamp(min(timestamps), tz=UTC) if timestamps else None
            if partition_start_at is None:
                partition_start_at = page_newest_at
            next_cursor = _normalize_cursor_partition_token(page_info.get("end_cursor"))
            has_next = bool(page_info.get("has_next_page"))
            if progress_cb:
                progress_cb(
                    {
                        "phase": "discover_history",
                        "pages_scanned": pages_scanned,
                        "posts_checked": posts_checked,
                        "matched_posts": 0,
                        "saved_posts": 0,
                        "total_posts": total_posts,
                        "discovered_partitions": len(partitions),
                    }
                )
            should_finalize = partition_posts >= target_posts_per_shard or not has_next or not next_cursor
            if next_cursor and next_cursor in seen_cursors:
                should_finalize = True
                has_next = False
                next_cursor = None
            if should_finalize:
                partition_key = _shared_account_partition_key(
                    run_id="pending",
                    platform="instagram",
                    account_handle=account_handle,
                    shard_index=shard_index,
                    cursor_start=partition_start_cursor,
                    cursor_end=next_cursor,
                )
                partition = SharedAccountCursorPartition(
                    partition_key=partition_key,
                    shard_index=shard_index,
                    shard_total=0,
                    runner_lane=lanes[shard_index % len(lanes)],
                    cursor_start=partition_start_cursor,
                    cursor_end=next_cursor,
                    boundary_start_at=partition_start_at,
                    boundary_end_at=page_oldest_at,
                    metadata={
                        "pages_scanned": partition_pages,
                        "posts_discovered": partition_posts,
                    },
                )
                partitions.append(partition)
                if partition_callback:
                    partition_callback(partition, expected_partition_count)
                shard_index += 1
                partition_start_cursor = next_cursor
                partition_start_at = None
                partition_posts = 0
                partition_pages = 0
            if not has_next or not next_cursor:
                break
            cursor = next_cursor
            seen_cursors.add(next_cursor)
        shard_total = max(1, len(partitions))
        for partition in partitions:
            partition.shard_total = shard_total
        return partitions, {
            "pages_scanned": pages_scanned,
            "posts_checked": posts_checked,
            "total_posts": total_posts,
            "partition_strategy": CATALOG_FULL_HISTORY_CURSOR_PARTITION_STRATEGY,
            "complete_coverage": not discovery_had_error,
            "partial_discovery": discovery_had_error,
            "incomplete_coverage": discovery_had_error,
            "retrieval_transport": selected_transport
            or str(last_retrieval_meta.get("transport") or "").strip()
            or None,
            **{
                key: value
                for key, value in last_retrieval_meta.items()
                if key
                in {
                    "error_code",
                    "error_class",
                    "error_status_code",
                    "error_message",
                    "retryable",
                    "graphql_cursor",
                }
                and value is not None
            },
        }


def _scrape_shared_instagram_posts_partitioned(
    *,
    run_id: str | None,
    account_handle: str,
    config: Mapping[str, Any],
    job_id: str | None = None,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _sync_core_overrides()
    from trr_backend.socials.instagram import ScrapeConfig

    account_execution = _room_callable("_shared_instagram_account_execution", _shared_instagram_account_execution)
    build_scraper = _room_callable("_build_shared_instagram_scraper", _build_shared_instagram_scraper)
    auth_validation = _room_callable(
        "_shared_instagram_frontier_auth_validation",
        _shared_instagram_frontier_auth_validation,
    )
    fetch_graphql_page = _room_callable(
        "_fetch_shared_instagram_graphql_page",
        _fetch_shared_instagram_graphql_page,
    )
    with _context_manager_from_callable(
        account_execution,
        account_handle,
        heartbeat_cb=_shared_instagram_account_lock_heartbeat(progress_cb),
    ):
        public_scraper = build_scraper(browser_account_id=account_handle)
        auth_allowed, _auth_reason = auth_validation(config)
        auth_scraper = build_scraper(authenticated=True, browser_account_id=account_handle) if auth_allowed else None
        base_delay_seconds = float((os.getenv("SOCIAL_INSTAGRAM_DELAY_SEC") or "").strip() or "0.15")
        catalog_page_size = _shared_instagram_catalog_graphql_page_size()
        scrape_config = ScrapeConfig(
            username=account_handle,
            hashtags=[],
            delay_seconds=base_delay_seconds,
            max_pages=None,
        )
        cursor = _normalize_cursor_partition_token(config.get("cursor_start"))
        end_cursor = _normalize_cursor_partition_token(config.get("cursor_end"))
        profile_total_posts = _normalize_non_negative_int(config.get("discovery_total_posts"))
        selected_transport: str | None = str(config.get("transport_preference") or "").strip().lower() or (
            "authenticated" if auth_allowed else None
        )
        allow_public_fallback = bool(config.get("allow_public_transport_fallback", True))
        pages_scanned = 0
        posts_checked = 0
        rows: list[dict[str, Any]] = []
        seen_cursors: set[str] = set()
        last_retrieval_meta: dict[str, Any] = {}
        reached_boundary = False
        partition_stop_reason: str | None = None
        consecutive_rate_limit_failures = 0
        max_rate_limit_retries = 3
        media_mirror_jobs_enqueued = 0

        # Brief pause after auth validation probe before page 1.
        time_module.sleep(5.0)
        while True:
            pages_scanned += 1
            adaptive_delay = _shared_instagram_catalog_delay_seconds(
                base_delay=base_delay_seconds,
                success_streak=max(0, pages_scanned - 1),
            )
            data, page_meta, selected_transport = fetch_graphql_page(
                account_handle=account_handle,
                cursor=cursor,
                delay_seconds=adaptive_delay,
                public_scraper=public_scraper,
                auth_scraper=auth_scraper,
                preferred_transport=selected_transport,
                allow_public_fallback=allow_public_fallback,
                page_size=catalog_page_size,
            )
            if page_meta:
                last_retrieval_meta = dict(page_meta)
            if not data:
                error_status = int(last_retrieval_meta.get("error_status_code") or 0)
                error_msg = str(last_retrieval_meta.get("error_message") or "").strip().lower()
                is_rate_limit = error_status in (401, 429) and "wait" in error_msg
                if is_rate_limit and consecutive_rate_limit_failures < max_rate_limit_retries:
                    consecutive_rate_limit_failures += 1
                    pages_scanned -= 1
                    backoff = float(60 * consecutive_rate_limit_failures)
                    logger.info(
                        "Partition fetch rate-limited (attempt %d/%d), backing off %.0fs",
                        consecutive_rate_limit_failures,
                        max_rate_limit_retries,
                        backoff,
                    )
                    time_module.sleep(backoff)
                    continue
                if last_retrieval_meta.get("error_code") or last_retrieval_meta.get("error_class"):
                    last_retrieval_meta.setdefault("retryable", True)
                    last_retrieval_meta.setdefault("graphql_cursor", str(cursor or "").strip() or None)
                    partition_stop_reason = str(last_retrieval_meta.get("error_code") or "request_failed")
                else:
                    partition_stop_reason = "empty_page"
                    if cursor or posts_checked > 0:
                        last_retrieval_meta.setdefault("error_code", "instagram_graphql_cursor_empty_page")
                        last_retrieval_meta.setdefault("error_class", "InstagramGraphQLEmptyOrErrorPage")
                        last_retrieval_meta.setdefault("retryable", True)
                        last_retrieval_meta.setdefault("graphql_cursor", str(cursor or "").strip() or None)
                        partition_stop_reason = "instagram_graphql_cursor_empty_page"
                break
            consecutive_rate_limit_failures = 0
            page_info: dict[str, Any] = {}
            page_posts: list[Any] = []
            for node, pi in public_scraper._iter_posts_from_graphql(data):
                page_info = pi
                page_posts.append(public_scraper._parse_post_node(node, scrape_config))
            if not page_posts:
                break
            posts_checked += len(page_posts)
            batch_rows, _source_ids, persist_meta = _normalize_shared_catalog_posts_batch_result(
                _persist_shared_catalog_posts_batch(
                    platform="instagram",
                    run_id=run_id,
                    account_handle=account_handle,
                    posts=page_posts,
                    job_id=job_id,
                    source_scope=str(config.get("source_scope") or "network"),
                    enable_media_followups=not bool(config.get("details_refresh_skip_media_followups")),
                )
            )
            rows.extend(batch_rows)
            media_mirror_jobs_enqueued += _normalize_non_negative_int(persist_meta.get("media_mirror_jobs_enqueued"))
            if progress_cb:
                progress_cb(
                    {
                        "phase": "scrape_partition_page",
                        "pages_scanned": pages_scanned,
                        "posts_checked": posts_checked,
                        "matched_posts": len(rows),
                        "saved_posts": len(rows),
                        "total_posts": profile_total_posts or None,
                    }
                )
            next_cursor = str(page_info.get("end_cursor") or "").strip() or None
            has_next = bool(page_info.get("has_next_page"))
            if end_cursor and next_cursor == end_cursor:
                reached_boundary = True
                partition_stop_reason = "cursor_boundary_reached"
                break
            if not has_next or not next_cursor:
                reached_boundary = True
                partition_stop_reason = "no_more_pages"
                break
            if next_cursor in seen_cursors:
                last_retrieval_meta.setdefault("error_code", "instagram_graphql_repeating_cursor")
                last_retrieval_meta.setdefault("error_class", "InstagramGraphQLRepeatingCursor")
                last_retrieval_meta.setdefault("retryable", True)
                last_retrieval_meta.setdefault("graphql_cursor", next_cursor)
                partition_stop_reason = "repeating_cursor"
                break
            cursor = next_cursor
            seen_cursors.add(cursor)
        # Detect partial scrape: if we stopped early due to errors (not end-of-feed)
        has_error = bool(last_retrieval_meta.get("error_code") or last_retrieval_meta.get("error_class"))
        is_partial = has_error and not reached_boundary and posts_checked > 0
        retrieval_meta = {
            "retrieval_mode": "graphql_partition",
            "pages_scanned": pages_scanned,
            "posts_checked": posts_checked,
            "total_posts": profile_total_posts or None,
            "partition_strategy": CATALOG_FULL_HISTORY_CURSOR_PARTITION_STRATEGY,
            "retrieval_transport": selected_transport
            or str(last_retrieval_meta.get("transport") or "").strip()
            or None,
            "persist_counters": {"posts_upserted": len(rows), "comments_upserted": 0},
            "partial_scrape": is_partial,
            "reached_partition_boundary": reached_boundary,
            "partition_stop_reason": partition_stop_reason,
        }
        if media_mirror_jobs_enqueued:
            retrieval_meta["media_mirror_jobs_enqueued"] = media_mirror_jobs_enqueued
        for key in (
            "error_code",
            "error_class",
            "error_status_code",
            "error_message",
            "retryable",
            "graphql_cursor",
        ):
            value = last_retrieval_meta.get(key)
            if value is not None:
                retrieval_meta[key] = value
        return rows, retrieval_meta


INSTAGRAM_DETAIL_REFRESH_DEFAULT_STALE_METADATA_DAYS = 30
INSTAGRAM_DETAIL_REFRESH_WRITE_BATCH_SIZE = 50
INSTAGRAM_DETAIL_REFRESH_PROGRESS_EVERY_POSTS = 25
INSTAGRAM_DETAIL_REFRESH_PROGRESS_INTERVAL_SECONDS = 5.0


def _instagram_detail_refresh_policy(config: Mapping[str, Any]) -> str:
    raw_policy = str(config.get("details_refresh_policy") or config.get("detail_refresh_policy") or "").strip().lower()
    if raw_policy:
        return raw_policy.replace("-", "_")
    if bool(config.get("details_refresh_dry_run")):
        return "dry_run"
    if bool(config.get("force_network_detail_fetch")) or bool(config.get("details_refresh_legacy_force_network")):
        return "force_network_detail"
    return "smart"


def _instagram_detail_refresh_force_network_enabled(
    *,
    policy: str,
    config: Mapping[str, Any],
    skip_detail_fetch: bool,
) -> bool:
    if skip_detail_fetch:
        return False
    normalized_policy = str(policy or "").strip().lower().replace("-", "_")
    if normalized_policy in {
        "force_network",
        "force_network_detail",
        "force_network_detail_fetch",
        "legacy",
        "legacy_force_network",
        "legacy_force_network_detail",
    }:
        return True
    if bool(config.get("force_network_detail_fetch")) or bool(config.get("details_refresh_legacy_force_network")):
        return True
    env_force = (os.getenv("SOCIAL_INSTAGRAM_DETAILS_REFRESH_FORCE_NETWORK") or "").strip().lower()
    return env_force in {"1", "true", "yes", "on"}


def _instagram_detail_refresh_legacy_inline_enrichment_enabled(
    *,
    policy: str,
    config: Mapping[str, Any],
) -> bool:
    normalized_policy = str(policy or "").strip().lower().replace("-", "_")
    if normalized_policy in {"legacy", "legacy_force_network", "legacy_force_network_detail"}:
        return True
    return bool(config.get("details_refresh_legacy_inline_enrichment"))


def _instagram_detail_refresh_dry_run_enabled(*, policy: str, config: Mapping[str, Any]) -> bool:
    normalized_policy = str(policy or "").strip().lower().replace("-", "_")
    return normalized_policy in {"dry_run", "classify_only"} or bool(config.get("details_refresh_dry_run"))


def _instagram_detail_refresh_stale_metadata_age(config: Mapping[str, Any] | None = None) -> timedelta:
    config = config or {}
    raw_days = str(
        config.get("details_refresh_stale_metadata_days")
        or os.getenv("SOCIAL_INSTAGRAM_DETAILS_REFRESH_STALE_METADATA_DAYS")
        or ""
    ).strip()
    try:
        days = int(raw_days or INSTAGRAM_DETAIL_REFRESH_DEFAULT_STALE_METADATA_DAYS)
    except ValueError:
        days = INSTAGRAM_DETAIL_REFRESH_DEFAULT_STALE_METADATA_DAYS
    return timedelta(days=max(1, min(days, 365)))


def _instagram_detail_refresh_write_batch_size(config: Mapping[str, Any]) -> int:
    raw_size = str(
        config.get("details_refresh_write_batch_size")
        or os.getenv("SOCIAL_INSTAGRAM_DETAILS_REFRESH_WRITE_BATCH_SIZE")
        or ""
    ).strip()
    try:
        size = int(raw_size or INSTAGRAM_DETAIL_REFRESH_WRITE_BATCH_SIZE)
    except ValueError:
        size = INSTAGRAM_DETAIL_REFRESH_WRITE_BATCH_SIZE
    return max(1, min(size, 200))


def _instagram_detail_refresh_progress_every_posts(config: Mapping[str, Any]) -> int:
    raw_count = str(
        config.get("details_refresh_progress_every_posts")
        or os.getenv("SOCIAL_INSTAGRAM_DETAILS_REFRESH_PROGRESS_EVERY_POSTS")
        or ""
    ).strip()
    try:
        count = int(raw_count or INSTAGRAM_DETAIL_REFRESH_PROGRESS_EVERY_POSTS)
    except ValueError:
        count = INSTAGRAM_DETAIL_REFRESH_PROGRESS_EVERY_POSTS
    return max(1, min(count, 500))


def _instagram_detail_refresh_progress_interval_seconds(config: Mapping[str, Any]) -> float:
    raw_seconds = str(
        config.get("details_refresh_progress_interval_seconds")
        or os.getenv("SOCIAL_INSTAGRAM_DETAILS_REFRESH_PROGRESS_INTERVAL_SECONDS")
        or ""
    ).strip()
    try:
        seconds = float(raw_seconds or INSTAGRAM_DETAIL_REFRESH_PROGRESS_INTERVAL_SECONDS)
    except ValueError:
        seconds = INSTAGRAM_DETAIL_REFRESH_PROGRESS_INTERVAL_SECONDS
    return max(0.25, min(seconds, 60.0))


def _instagram_gallery_metric_value(gallery_metrics: Mapping[str, Any] | None, key: str) -> int | None:
    if not isinstance(gallery_metrics, Mapping):
        return None
    value = gallery_metrics.get(key)
    if value is None:
        return None
    return _normalize_non_negative_int(value)


def _instagram_existing_metric_available(row: Mapping[str, Any], key: str) -> bool:
    if key not in row:
        return False
    value = row.get(key)
    return value is not None and str(value).strip() != ""


def _instagram_detail_metadata_is_stale(
    row: Mapping[str, Any],
    *,
    now_utc: Any,
    stale_metadata_age: timedelta,
) -> bool:
    observed_at = (
        _coerce_dt(row.get("metadata_scraped_at"))
        or _coerce_dt(row.get("scraped_at"))
        or _coerce_dt(row.get("updated_at"))
    )
    if observed_at is None:
        return False
    try:
        return now_utc - observed_at > stale_metadata_age
    except TypeError:
        return False


def classify_instagram_detail_refresh_need(
    row: Mapping[str, Any],
    gallery_metrics: Mapping[str, Any] | None = None,
    selected_tasks: Sequence[str] | None = None,
    refresh_policy: str | None = None,
    *,
    force_network_detail_fetch: bool = False,
    skip_detail_fetch: bool = False,
    require_fresh_metadata: bool = False,
    now_utc: Any | None = None,
    stale_metadata_age: timedelta | None = None,
) -> dict[str, Any]:
    """Classify whether detail refresh needs an external per-post detail fetch."""
    normalized_policy = str(refresh_policy or "smart").strip().lower().replace("-", "_")
    normalized_tasks = {
        str(task or "").strip().lower().replace("-", "_") for task in (selected_tasks or []) if str(task or "").strip()
    }
    now_utc = now_utc or _now_utc()
    stale_metadata_age = stale_metadata_age or _instagram_detail_refresh_stale_metadata_age()

    existing_likes_available = _instagram_existing_metric_available(row, "likes")
    existing_comments_available = _instagram_existing_metric_available(row, "comments_count")
    existing_views_available = _instagram_existing_metric_available(row, "views")
    gallery_likes_available = _instagram_gallery_metric_value(gallery_metrics, "likes") is not None
    gallery_comments_available = _instagram_gallery_metric_value(gallery_metrics, "comments") is not None
    gallery_views_available = _instagram_gallery_metric_value(gallery_metrics, "views_observed") is not None

    metrics_missing: list[str] = []
    if not (existing_likes_available or gallery_likes_available):
        metrics_missing.append("likes")
    if not (existing_comments_available or gallery_comments_available):
        metrics_missing.append("comments")
    if not (existing_views_available or gallery_views_available):
        metrics_missing.append("views")

    source_media_required = bool(normalized_tasks & {"source_media_refresh", "post_source_media"})
    source_media_missing = source_media_required and not (
        _as_text_list(row.get("media_urls")) or str(row.get("thumbnail_url") or "").strip()
    )
    permalink_metadata_required = bool(
        normalized_tasks & {"permalink_metadata", "permalink_metadata_refresh", "facebook_crosspost_refresh"}
    )
    permalink_missing = permalink_metadata_required and not str(row.get("permalink") or row.get("url") or "").strip()
    metadata_stale = bool(require_fresh_metadata) and _instagram_detail_metadata_is_stale(
        row,
        now_utc=now_utc,
        stale_metadata_age=stale_metadata_age,
    )

    reasons: list[str] = []
    if skip_detail_fetch:
        fetch_needed = False
    elif force_network_detail_fetch:
        fetch_needed = True
        reasons.append("force_network_detail_fetch")
    else:
        if metrics_missing:
            reasons.append("metrics_unavailable")
        if source_media_missing:
            reasons.append("missing_required_source_media")
        if permalink_missing:
            reasons.append("missing_required_permalink_metadata")
        if metadata_stale:
            reasons.append("stale_metadata")
        fetch_needed = bool(reasons)

    satisfaction_source = "none"
    if not fetch_needed and not metrics_missing:
        if gallery_likes_available or gallery_comments_available or gallery_views_available:
            satisfaction_source = "gallery"
        else:
            satisfaction_source = "existing"

    return {
        "fetch_needed": fetch_needed,
        "reasons": reasons,
        "metrics_missing": metrics_missing,
        "satisfaction_source": satisfaction_source,
        "policy": normalized_policy,
        "metadata_stale": metadata_stale,
        "stale_metadata_days": max(1, int(stale_metadata_age.total_seconds() // 86400)),
    }


def _scrape_shared_instagram_post_details_refresh(
    *,
    run_id: str | None,
    account_handle: str,
    config: Mapping[str, Any],
    job_id: str,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _sync_core_overrides()
    from trr_backend.socials.instagram import ScrapeConfig

    account_execution = _room_callable("_shared_instagram_account_execution", _shared_instagram_account_execution)
    build_scraper = _room_callable("_build_shared_instagram_scraper", _build_shared_instagram_scraper)
    auth_validation = _room_callable(
        "_shared_instagram_frontier_auth_validation",
        _shared_instagram_frontier_auth_validation,
    )
    upsert_instagram_post = _room_callable("_upsert_instagram_post", _upsert_instagram_post)
    selected_tasks_raw = config.get("selected_tasks") or config.get("effective_selected_tasks") or []
    if isinstance(selected_tasks_raw, str):
        selected_tasks = [task.strip() for task in selected_tasks_raw.split(",") if task.strip()]
    else:
        selected_tasks = [str(task).strip() for task in (selected_tasks_raw or []) if str(task).strip()]
    refresh_policy = _instagram_detail_refresh_policy(config)
    dry_run = _instagram_detail_refresh_dry_run_enabled(policy=refresh_policy, config=config)
    skip_detail_fetch = bool(config.get("details_refresh_skip_detail_fetch")) or dry_run
    force_network_detail_fetch = _instagram_detail_refresh_force_network_enabled(
        policy=refresh_policy,
        config=config,
        skip_detail_fetch=skip_detail_fetch,
    ) or (bool(config.get("details_refresh_force_detail_fetch")) and not skip_detail_fetch)
    legacy_inline_enrichment = _instagram_detail_refresh_legacy_inline_enrichment_enabled(
        policy=refresh_policy,
        config=config,
    )
    require_fresh_metadata = bool(config.get("details_refresh_require_fresh_metadata")) or refresh_policy in {
        "smart_metadata",
        "metadata_refresh",
    }
    stale_metadata_age = _instagram_detail_refresh_stale_metadata_age(config)
    write_batch_size = _instagram_detail_refresh_write_batch_size(config)
    media_followups_enabled = not dry_run and not bool(config.get("details_refresh_skip_media_followups"))
    detail_shard_count = max(1, _normalize_non_negative_int(config.get("details_refresh_shard_count")) or 1)
    detail_shard_index = _normalize_non_negative_int(config.get("details_refresh_shard_index"))
    if detail_shard_count > 1:
        detail_shard_index = min(detail_shard_index, detail_shard_count - 1)
    detail_refresh_lock_scope = (
        f"details-refresh:{detail_shard_index}:of:{detail_shard_count}" if detail_shard_count > 1 else None
    )
    with _context_manager_from_callable(
        account_execution,
        account_handle,
        heartbeat_cb=_shared_instagram_account_lock_heartbeat(progress_cb),
        lock_scope=detail_refresh_lock_scope,
    ):
        auth_allowed, _auth_reason = auth_validation(config)
        scraper = (
            build_scraper(authenticated=True, browser_account_id=account_handle) if auth_allowed else None
        ) or build_scraper(browser_account_id=account_handle)
        detail_config = ScrapeConfig(
            username=account_handle,
            hashtags=[],
            date_start=_coerce_dt(config.get("date_start")),
            date_end=_coerce_dt(config.get("date_end")),
            delay_seconds=float((os.getenv("SOCIAL_INSTAGRAM_DELAY_SEC") or "").strip() or "0.15"),
            max_pages=None,
        )
        existing_posts = _load_existing_social_account_posts(
            "instagram",
            account_handle,
            _coerce_dt(config.get("date_start")),
            _coerce_dt(config.get("date_end")),
        )
        all_existing_posts_count = len(existing_posts)
        bounded_window = _catalog_backfill_has_bounded_window(
            date_start=_coerce_dt(config.get("date_start")),
            date_end=_coerce_dt(config.get("date_end")),
        )
        if detail_shard_count > 1:
            existing_posts = [
                post for index, post in enumerate(existing_posts) if index % detail_shard_count == detail_shard_index
            ]
        expected_total_posts = _shared_account_expected_total_posts_from_config(
            config,
            platform="instagram",
            account_handle=account_handle,
        )
        progress_total_posts = (
            len(existing_posts) if bounded_window else max(all_existing_posts_count, expected_total_posts)
        )
        metrics_pages_scanned = 0
        metrics_posts_checked = 0
        progress_every_posts = _instagram_detail_refresh_progress_every_posts(config)
        progress_interval_seconds = _instagram_detail_refresh_progress_interval_seconds(config)
        progress_last_emit_at = 0.0
        progress_last_checked = 0

        def _emit_progress(*, phase: str, posts_checked: int, saved_posts: int) -> None:
            nonlocal progress_last_emit_at, progress_last_checked
            if not progress_cb:
                return
            now_monotonic = time_module.monotonic()
            normalized_posts_checked = max(0, int(posts_checked or 0))
            force_emit = normalized_posts_checked <= 1 or normalized_posts_checked >= progress_total_posts
            count_due = normalized_posts_checked - progress_last_checked >= progress_every_posts
            time_due = now_monotonic - progress_last_emit_at >= progress_interval_seconds
            if not (force_emit or count_due or time_due):
                return
            progress_last_emit_at = now_monotonic
            progress_last_checked = normalized_posts_checked
            payload: dict[str, Any] = {
                "phase": phase,
                "pages_scanned": metrics_pages_scanned,
                "posts_checked": normalized_posts_checked,
                "matched_posts": saved_posts,
                "saved_posts": saved_posts,
                "total_posts": progress_total_posts,
            }
            progress_cb(payload)

        def _on_metrics_progress(payload: dict[str, Any]) -> None:
            nonlocal metrics_pages_scanned, metrics_posts_checked
            metrics_pages_scanned = max(
                metrics_pages_scanned,
                _normalize_non_negative_int(payload.get("pages_scanned")),
            )
            metrics_posts_checked = max(
                metrics_posts_checked,
                _normalize_non_negative_int(payload.get("posts_checked")),
            )
            _emit_progress(
                phase=str(payload.get("phase") or "details_refresh_metrics_index"),
                posts_checked=metrics_posts_checked,
                saved_posts=0,
            )

        if dry_run or skip_detail_fetch or force_network_detail_fetch:
            metrics_index = {}
        else:
            try:
                metrics_index = scraper.scrape_metrics_index(detail_config, progress_cb=_on_metrics_progress)
            except Exception:  # noqa: BLE001
                metrics_index = {}
                logger.exception(
                    (
                        "[instagram] Failed to build shared-account gallery metrics index for account=%s; "
                        "falling back to per-post detail fetch"
                    ),
                    account_handle,
                )

    detail_fetch_cap: int | None = None

    now_utc = _now_utc()
    details_refreshed_posts = 0
    details_refresh_errors = 0
    details_refresh_error_reasons: Counter[str] = Counter()
    details_refresh_views_updated = 0
    details_refresh_views_preserved_missing = 0
    details_refresh_views_sources: Counter[str] = Counter()
    details_refresh_detail_fetch_attempts = 0
    details_refresh_detail_fetch_skipped_limit = 0
    details_refresh_skipped_required_fields = 0
    details_refresh_rows_seen = 0
    details_refresh_fetch_avoided = 0
    details_refresh_fetch_reason_counts: Counter[str] = Counter()
    details_refresh_fetch_avoid_reason_counts: Counter[str] = Counter()
    details_refresh_rows_satisfied_from_gallery = 0
    details_refresh_rows_satisfied_from_existing = 0
    details_refresh_write_batches = 0
    details_refresh_rows_per_batch: list[int] = []
    media_mirror_jobs_enqueued = 0
    media_mirror_job_enqueue_errors = 0
    media_mirror_enqueue_batches = 0
    media_mirror_jobs_deduped = 0
    media_mirror_job_post_ids_seen: set[str] = set()
    refreshed_rows: list[dict[str, Any]] = []
    pending_writes: list[dict[str, Any]] = []

    def _queue_write(
        *,
        existing_row: dict[str, Any],
        post_db_id: str,
        likes_candidate: int,
        comments_candidate: int,
        resolved_views: int | None,
        views_source: str | None,
        views_raw_candidates: list[Any],
        parsed_post: Any | None,
        write_metrics: bool,
    ) -> None:
        if dry_run:
            refreshed_rows.append(dict(existing_row))
            return
        pending_writes.append(
            {
                "existing_row": dict(existing_row),
                "post_db_id": post_db_id,
                "likes": likes_candidate,
                "comments_count": comments_candidate,
                "views": resolved_views,
                "views_source": views_source,
                "views_raw_candidates": list(views_raw_candidates or []),
                "parsed_post": parsed_post,
                "write_metrics": write_metrics,
            }
        )

    def _flush_pending_writes(*, force: bool = False) -> None:
        nonlocal details_refresh_write_batches, media_mirror_jobs_enqueued
        nonlocal media_mirror_job_enqueue_errors, media_mirror_enqueue_batches, media_mirror_jobs_deduped
        if dry_run or not pending_writes:
            return
        if not force and len(pending_writes) < write_batch_size:
            return
        batch = list(pending_writes)
        pending_writes.clear()
        details_refresh_write_batches += 1
        details_refresh_rows_per_batch.append(len(batch))
        batch_enqueue_attempted = False
        with pg.db_connection(label="instagram_details_refresh_batch") as details_conn:
            for item in batch:
                existing_row = dict(item["existing_row"])
                post_db_id = str(item["post_db_id"] or "")
                parsed_post = item.get("parsed_post")
                refreshed_row = existing_row
                if item.get("write_metrics"):
                    _refresh_instagram_post_metrics_only(
                        post_db_id=post_db_id,
                        likes=int(item["likes"] or 0),
                        comments_count=int(item["comments_count"] or 0),
                        views=item.get("views"),
                        views_source=item.get("views_source"),
                        views_raw_candidates=list(item.get("views_raw_candidates") or []),
                        conn=details_conn,
                    )
                    refreshed_row = {
                        **refreshed_row,
                        "likes": int(item["likes"] or 0),
                        "comments_count": int(item["comments_count"] or 0),
                        "views": item.get("views"),
                    }
                if parsed_post is not None:
                    upserted = (
                        upsert_instagram_post(
                            None,
                            job_id=job_id,
                            account=account_handle,
                            post=parsed_post,
                            conn=details_conn,
                        )
                        or refreshed_row
                    )
                    refreshed_row = dict(upserted or refreshed_row)
                if media_followups_enabled and refreshed_row:
                    followup_post_id = str(refreshed_row.get("id") or post_db_id or "").strip()
                    if followup_post_id in media_mirror_job_post_ids_seen:
                        media_mirror_jobs_deduped += 1
                    elif followup_post_id:
                        media_mirror_job_post_ids_seen.add(followup_post_id)
                        try:
                            needs_media_followup = skip_detail_fetch or _platform_post_needs_media_mirror(
                                "instagram",
                                refreshed_row,
                                conn=details_conn,
                            )
                            if needs_media_followup:
                                batch_enqueue_attempted = True
                                mirror_job_id = _enqueue_instagram_media_mirror_job(
                                    None,
                                    run_id=run_id,
                                    source_scope=str(config.get("source_scope") or "network"),
                                    account=account_handle,
                                    post_row=dict(refreshed_row),
                                    week_index=None,
                                    parent_job_id=job_id,
                                    conn=details_conn,
                                )
                                if mirror_job_id:
                                    media_mirror_jobs_enqueued += 1
                        except Exception:  # noqa: BLE001
                            media_mirror_job_enqueue_errors += 1
                            logger.exception(
                                "[instagram] Failed to enqueue shared-account media mirror job for post=%s",
                                followup_post_id,
                            )
                refreshed_rows.append(dict(refreshed_row))
        if batch_enqueue_attempted:
            media_mirror_enqueue_batches += 1

    for index, row in enumerate(existing_posts, start=1):
        existing_row = dict(row)
        shortcode = str(existing_row.get("shortcode") or "").strip()
        post_db_id = str(existing_row.get("id") or "").strip()
        if not shortcode or not post_db_id:
            continue
        details_refresh_rows_seen += 1
        _emit_progress(phase="details_refresh_fetch", posts_checked=index, saved_posts=details_refreshed_posts)
        try:
            existing_likes = _normalize_non_negative_int(existing_row.get("likes"))
            existing_comments = _normalize_non_negative_int(existing_row.get("comments_count"))
            existing_views = _normalize_non_negative_int(existing_row.get("views"))
            gallery_metrics = metrics_index.get(shortcode) if isinstance(metrics_index, dict) else None
            gallery_likes = (
                _normalize_non_negative_int((gallery_metrics or {}).get("likes"))
                if isinstance((gallery_metrics or {}).get("likes"), (int, float))
                else None
            )
            gallery_comments = (
                _normalize_non_negative_int((gallery_metrics or {}).get("comments"))
                if isinstance((gallery_metrics or {}).get("comments"), (int, float))
                else None
            )
            gallery_views = (
                _normalize_non_negative_int((gallery_metrics or {}).get("views_observed"))
                if (gallery_metrics or {}).get("views_observed") is not None
                else None
            )
            gallery_views_source = str((gallery_metrics or {}).get("views_source") or "").strip() or None
            gallery_views_raw_candidates = (
                (gallery_metrics or {}).get("views_raw_candidates")
                if isinstance((gallery_metrics or {}).get("views_raw_candidates"), list)
                else []
            )
            classification = classify_instagram_detail_refresh_need(
                existing_row,
                gallery_metrics if isinstance(gallery_metrics, Mapping) else None,
                selected_tasks,
                refresh_policy,
                force_network_detail_fetch=force_network_detail_fetch,
                skip_detail_fetch=skip_detail_fetch,
                require_fresh_metadata=require_fresh_metadata,
                now_utc=now_utc,
                stale_metadata_age=stale_metadata_age,
            )
            if classification.get("fetch_needed"):
                for reason in classification.get("reasons") or ["unknown"]:
                    details_refresh_fetch_reason_counts[str(reason)] += 1
            else:
                details_refresh_fetch_avoided += 1
                source = str(classification.get("satisfaction_source") or "existing")
                details_refresh_fetch_avoid_reason_counts[source] += 1
                if source == "gallery":
                    details_refresh_rows_satisfied_from_gallery += 1
                elif source == "existing":
                    details_refresh_rows_satisfied_from_existing += 1
            parsed_post = None
            detail_fetch_skipped = False
            if bool(classification.get("fetch_needed")):
                if detail_fetch_cap == 0 or (
                    detail_fetch_cap is not None and details_refresh_detail_fetch_attempts >= detail_fetch_cap
                ):
                    details_refresh_detail_fetch_skipped_limit += 1
                    detail_fetch_skipped = True
                else:
                    details_refresh_detail_fetch_attempts += 1
                    post_payload = scraper.fetch_post_info(shortcode, delay=detail_config.delay_seconds)
                    node = _extract_instagram_post_detail_node(post_payload)
                    parsed_post = scraper._parse_post_node(node, detail_config) if node else None  # noqa: SLF001
            if parsed_post is not None and legacy_inline_enrichment:
                try:
                    _enrich_instagram_post_from_permalink(post=parsed_post, scraper=scraper, now_utc=now_utc)
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "[instagram] Shared-account metadata enrichment failed during details refresh for %s",
                        shortcode,
                    )
                    parsed_post.metadata_source = None
                    _mark_instagram_metadata_attempt(
                        post=parsed_post,
                        now_utc=now_utc,
                        success=False,
                        error_code="metadata_enrichment_exception",
                    )
                    details_refresh_error_reasons["metadata_enrichment_exception"] += 1
            if parsed_post is not None and not legacy_inline_enrichment:
                if not str(getattr(parsed_post, "metadata_source", "") or "").strip():
                    parsed_post.metadata_source = "api_permalink"
                _mark_instagram_metadata_attempt(post=parsed_post, now_utc=now_utc, success=True)
            unresolved_required_detail_fields = detail_fetch_skipped and (
                bool(classification.get("metrics_missing"))
                or "missing_required_source_media" in (classification.get("reasons") or [])
                or "missing_required_permalink_metadata" in (classification.get("reasons") or [])
            )
            if (
                bool(classification.get("fetch_needed"))
                and not parsed_post
                and bool(classification.get("metrics_missing"))
            ):
                if detail_fetch_skipped:
                    details_refresh_skipped_required_fields += 1
                    details_refresh_error_reasons["detail_fetch_skipped_limit_missing_required_fields"] += 1
                else:
                    details_refresh_errors += 1
                    details_refresh_error_reasons["detail_metrics_unavailable"] += 1
                continue
            likes_candidate = max(
                existing_likes,
                gallery_likes if gallery_likes is not None else existing_likes,
                _normalize_non_negative_int(getattr(parsed_post, "likes", 0)) if parsed_post is not None else 0,
            )
            comments_candidate = max(
                existing_comments,
                gallery_comments if gallery_comments is not None else existing_comments,
                _normalize_non_negative_int(getattr(parsed_post, "comments", 0)) if parsed_post is not None else 0,
            )
            detail_views_observed = (
                _normalize_non_negative_int(getattr(parsed_post, "video_views_observed", 0))
                if parsed_post is not None and getattr(parsed_post, "video_views_observed", None) is not None
                else None
            )
            views_candidate = gallery_views if gallery_views is not None else detail_views_observed
            views_source = (
                gallery_views_source
                if gallery_views is not None
                else (
                    str(getattr(parsed_post, "video_views_source", "") or "").strip()
                    if parsed_post is not None
                    else None
                )
            )
            views_raw_candidates = (
                gallery_views_raw_candidates
                if gallery_views is not None and gallery_views_raw_candidates
                else (
                    list(getattr(parsed_post, "video_views_raw_candidates", []) or [])
                    if parsed_post is not None
                    else []
                )
            )
            resolved_views = max(existing_views, views_candidate) if views_candidate is not None else None
            if parsed_post is not None:
                parsed_post.likes = likes_candidate
                parsed_post.comments = comments_candidate
                parsed_post.video_views_observed = resolved_views
                parsed_post.video_views_source = views_source
                parsed_post.video_views_raw_candidates = views_raw_candidates
            _queue_write(
                existing_row=existing_row,
                post_db_id=post_db_id,
                likes_candidate=likes_candidate,
                comments_candidate=comments_candidate,
                resolved_views=resolved_views,
                views_source=views_source,
                views_raw_candidates=views_raw_candidates,
                parsed_post=parsed_post,
                write_metrics=not skip_detail_fetch,
            )
            _flush_pending_writes()
            if resolved_views is None:
                details_refresh_views_preserved_missing += 1
            elif resolved_views > existing_views:
                details_refresh_views_updated += 1
            if views_source:
                details_refresh_views_sources[views_source] += 1
            if unresolved_required_detail_fields:
                details_refresh_skipped_required_fields += 1
                details_refresh_error_reasons["detail_fetch_skipped_limit_missing_required_fields"] += 1
            else:
                details_refreshed_posts += 1
            _emit_progress(phase="details_refresh_update", posts_checked=index, saved_posts=details_refreshed_posts)
        except Exception:  # noqa: BLE001
            details_refresh_errors += 1
            details_refresh_error_reasons["details_refresh_exception"] += 1
            logger.exception(
                "[instagram] Shared-account details refresh failed for post=%s shortcode=%s",
                post_db_id,
                shortcode,
            )

    _flush_pending_writes(force=True)

    details_refresh_completion_target_posts = (
        len(existing_posts) if detail_shard_count > 1 else all_existing_posts_count
    )
    retrieval_meta: dict[str, Any] = {
        "source": "db_metrics_refresh",
        "expected_total_posts": expected_total_posts if bounded_window else expected_total_posts or None,
        "total_posts": progress_total_posts,
        "completion_target_posts": details_refresh_completion_target_posts,
        "completion_target_source": "bounded_catalog" if bounded_window else None,
        "details_refresh_account_rows_seen": all_existing_posts_count,
        "details_refreshed_posts": details_refreshed_posts,
        "details_refresh_views_updated": details_refresh_views_updated,
        "details_refresh_views_preserved_missing": details_refresh_views_preserved_missing,
        "details_refresh_detail_fetch_attempts": details_refresh_detail_fetch_attempts,
        "details_refresh_detail_fetch_skipped_limit": details_refresh_detail_fetch_skipped_limit,
        "details_refresh_skipped_required_fields": details_refresh_skipped_required_fields,
        "details_refresh_detail_fetch_cap": detail_fetch_cap,
        "details_refresh_metrics_index_size": len(metrics_index),
        "details_refresh_metrics_pages_scanned": metrics_pages_scanned,
        "details_refresh_metrics_posts_checked": metrics_posts_checked,
        "details_refresh_skip_detail_fetch": skip_detail_fetch,
        "details_refresh_force_detail_fetch": force_network_detail_fetch,
        "force_network_detail_fetch": force_network_detail_fetch,
        "details_refresh_rows_seen": details_refresh_rows_seen,
        "details_refresh_fetch_attempts": details_refresh_detail_fetch_attempts,
        "details_refresh_fetch_avoided": details_refresh_fetch_avoided,
        "details_refresh_rows_satisfied_from_gallery": details_refresh_rows_satisfied_from_gallery,
        "details_refresh_rows_satisfied_from_existing": details_refresh_rows_satisfied_from_existing,
        "details_refresh_shard_index": detail_shard_index if detail_shard_count > 1 else None,
        "details_refresh_shard_count": detail_shard_count if detail_shard_count > 1 else None,
        "details_refresh_skip_media_followups": bool(config.get("details_refresh_skip_media_followups")),
        "details_refresh_write_batch_size": write_batch_size,
        "details_refresh_write_batches": details_refresh_write_batches,
        "details_refresh_rows_per_batch": list(details_refresh_rows_per_batch),
        "details_refresh_progress_every_posts": progress_every_posts,
        "details_refresh_progress_interval_seconds": progress_interval_seconds,
        "persist_counters": {"posts_upserted": len(refreshed_rows), "comments_upserted": 0},
    }
    if media_mirror_jobs_enqueued:
        retrieval_meta["media_mirror_jobs_enqueued"] = media_mirror_jobs_enqueued
    if media_mirror_job_enqueue_errors:
        retrieval_meta["media_mirror_job_enqueue_errors"] = media_mirror_job_enqueue_errors
    if media_mirror_enqueue_batches:
        retrieval_meta["media_mirror_enqueue_batches"] = media_mirror_enqueue_batches
    if media_mirror_jobs_deduped:
        retrieval_meta["media_mirror_jobs_deduped"] = media_mirror_jobs_deduped
    if details_refresh_views_sources:
        retrieval_meta["details_refresh_views_sources"] = dict(details_refresh_views_sources)
    if details_refresh_fetch_reason_counts:
        retrieval_meta["details_refresh_fetch_reason_counts"] = dict(details_refresh_fetch_reason_counts)
    if details_refresh_fetch_avoid_reason_counts:
        retrieval_meta["details_refresh_fetch_avoid_reason_counts"] = dict(details_refresh_fetch_avoid_reason_counts)
    if details_refresh_errors:
        retrieval_meta["details_refresh_errors"] = details_refresh_errors
    if details_refresh_error_reasons:
        retrieval_meta["details_refresh_error_reasons"] = dict(details_refresh_error_reasons)
    return refreshed_rows, retrieval_meta


def _scrape_shared_instagram_posts(
    *,
    run_id: str | None,
    account_handle: str,
    config: Mapping[str, Any],
    job_id: str,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _sync_core_overrides()
    normalized_ingest_mode = str(config.get("ingest_mode") or "").strip().lower()
    if normalized_ingest_mode == "details_refresh":
        details_refresh = _room_callable(
            "_scrape_shared_instagram_post_details_refresh",
            _scrape_shared_instagram_post_details_refresh,
        )
        return details_refresh(
            run_id=run_id,
            account_handle=account_handle,
            config=config,
            job_id=job_id,
            progress_cb=progress_cb,
        )
    if str(config.get("partition_strategy") or "").strip().lower() == CATALOG_FULL_HISTORY_CURSOR_PARTITION_STRATEGY:
        partitioned_scrape = _room_callable(
            "_scrape_shared_instagram_posts_partitioned",
            _scrape_shared_instagram_posts_partitioned,
        )
        return partitioned_scrape(
            run_id=run_id,
            account_handle=account_handle,
            config=config,
            job_id=job_id,
            progress_cb=progress_cb,
        )

    from trr_backend.socials.instagram import ScrapeConfig

    account_execution = _room_callable("_shared_instagram_account_execution", _shared_instagram_account_execution)
    build_scraper = _room_callable("_build_shared_instagram_scraper", _build_shared_instagram_scraper)
    auth_validation = _room_callable(
        "_shared_instagram_frontier_auth_validation",
        _shared_instagram_frontier_auth_validation,
    )
    upsert_instagram_post = _room_callable("_upsert_instagram_post", _upsert_instagram_post)
    with _context_manager_from_callable(
        account_execution,
        account_handle,
        heartbeat_cb=_shared_instagram_account_lock_heartbeat(progress_cb),
    ):
        auth_allowed, _auth_reason = auth_validation(config)
        scraper = (
            build_scraper(authenticated=True, browser_account_id=account_handle) if auth_allowed else None
        ) or build_scraper(browser_account_id=account_handle)
        scrape_config = ScrapeConfig(
            username=account_handle,
            hashtags=[],
            date_start=_coerce_dt(config.get("date_start")),
            date_end=_coerce_dt(config.get("date_end")),
            delay_seconds=float((os.getenv("SOCIAL_INSTAGRAM_DELAY_SEC") or "").strip() or "0.15"),
            max_pages=None,
        )
        posts = scraper.scrape(scrape_config, progress_cb=progress_cb)
        # If auth scraper returned nothing, fall back to public transport.
        if not posts and auth_allowed:
            public_scraper = build_scraper(browser_account_id=account_handle)
            posts = public_scraper.scrape(scrape_config, progress_cb=progress_cb)
            scraper = public_scraper
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
    oldest_posted_at_seen, newest_posted_at_seen = _shared_instagram_posted_at_bounds(posts)
    retrieval_meta["oldest_posted_at_seen"] = _iso(oldest_posted_at_seen)
    retrieval_meta["newest_posted_at_seen"] = _iso(newest_posted_at_seen)
    rows: list[dict[str, Any]] = []
    if _shared_catalog_mode(config):
        progress_interval = _shared_catalog_progress_interval_posts()
        pages_scanned = _normalize_non_negative_int(retrieval_meta.get("pages_scanned"))
        posts_checked = max(_normalize_non_negative_int(retrieval_meta.get("posts_checked")), len(posts))
        matched_posts = len(posts)
        total_posts = retrieval_meta.get("total_posts")
        media_mirror_jobs_enqueued = 0

        def _emit_persist_progress(saved_posts: int, *, force: bool = False) -> None:
            if not progress_cb:
                return
            if not force and saved_posts > 0 and saved_posts % progress_interval != 0:
                return
            payload: dict[str, Any] = {
                "phase": "persist_catalog_posts",
                "pages_scanned": pages_scanned,
                "posts_checked": posts_checked,
                "matched_posts": matched_posts,
                "saved_posts": max(0, int(saved_posts)),
            }
            if total_posts is not None:
                payload["total_posts"] = _normalize_non_negative_int(total_posts)
            progress_cb(payload)

        _emit_persist_progress(0, force=True)
        rows, _source_ids, persist_meta = _normalize_shared_catalog_posts_batch_result(
            _persist_shared_catalog_posts_batch(
                platform="instagram",
                run_id=run_id,
                account_handle=account_handle,
                posts=posts,
                job_id=job_id,
                source_scope=str(config.get("source_scope") or "network"),
                enable_media_followups=not bool(config.get("details_refresh_skip_media_followups")),
            )
        )
        media_mirror_jobs_enqueued = _normalize_non_negative_int(persist_meta.get("media_mirror_jobs_enqueued"))
        _emit_persist_progress(len(rows), force=True)
        catalog_posts_upserted = persist_meta.get("catalog_posts_upserted")
        if catalog_posts_upserted is None:
            catalog_posts_upserted = len(rows)
        retrieval_meta["persist_counters"] = {
            "posts_upserted": len(rows),
            "catalog_posts_upserted": _normalize_non_negative_int(catalog_posts_upserted),
            "comments_upserted": 0,
        }
        if media_mirror_jobs_enqueued:
            retrieval_meta["media_mirror_jobs_enqueued"] = media_mirror_jobs_enqueued
    else:
        for post in posts:
            row = upsert_instagram_post(None, job_id=job_id, account=account_handle, post=post)
            if row:
                rows.append(row)
    return rows, retrieval_meta


_LOCAL_ROOM_NAMES.update(
    {
        "_shared_instagram_catalog_graphql_page_size",
        "_shared_instagram_catalog_delay_seconds",
        "_instagram_post_payload",
        "_upsert_instagram_post",
        "_batch_upsert_instagram_posts",
        "_upsert_shared_catalog_instagram_post",
        "_shared_catalog_instagram_post_payload",
        "_batch_upsert_shared_catalog_instagram_posts",
        "_build_instagram_scraper_with_auth_fallback",
        "_build_shared_instagram_scraper",
        "_shared_instagram_graphql_page_has_posts",
        "_shared_instagram_graphql_candidate_scrapers",
        "_shared_instagram_graphql_empty_page_meta",
        "_fetch_shared_instagram_graphql_page",
        "_shared_instagram_frontier_auth_validation",
        "_shared_instagram_frontier_auth_state",
        "_shared_instagram_frontier_auth_error_code",
        "_shared_instagram_frontier_transport_preferences",
        "_shared_instagram_posted_at_bounds",
        "_shared_instagram_graphql_delay_seconds",
        "_shared_instagram_account_lock_key",
        "_shared_instagram_account_lock_max_attempts",
        "_shared_instagram_account_lock_wait_seconds",
        "_shared_instagram_account_execution",
        "_shared_instagram_account_lock_heartbeat",
        "_fetch_shared_instagram_graphql_posts_page",
        "_discover_instagram_cursor_partitions",
        "_scrape_shared_instagram_posts_partitioned",
        "_scrape_shared_instagram_post_details_refresh",
        "_scrape_shared_instagram_posts",
    }
)
_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_shared_instagram_catalog_graphql_page_size",
    "_shared_instagram_catalog_delay_seconds",
    "_instagram_post_payload",
    "_upsert_instagram_post",
    "_batch_upsert_instagram_posts",
    "_upsert_shared_catalog_instagram_post",
    "_shared_catalog_instagram_post_payload",
    "_batch_upsert_shared_catalog_instagram_posts",
    "_build_instagram_scraper_with_auth_fallback",
    "_build_shared_instagram_scraper",
    "_shared_instagram_graphql_page_has_posts",
    "_shared_instagram_graphql_candidate_scrapers",
    "_shared_instagram_graphql_empty_page_meta",
    "_fetch_shared_instagram_graphql_page",
    "_shared_instagram_frontier_auth_validation",
    "_shared_instagram_frontier_auth_state",
    "_shared_instagram_frontier_auth_error_code",
    "_shared_instagram_frontier_transport_preferences",
    "_shared_instagram_posted_at_bounds",
    "_shared_instagram_graphql_delay_seconds",
    "_shared_instagram_account_lock_key",
    "_shared_instagram_account_lock_max_attempts",
    "_shared_instagram_account_lock_wait_seconds",
    "_shared_instagram_account_execution",
    "_shared_instagram_account_lock_heartbeat",
    "_fetch_shared_instagram_graphql_posts_page",
    "_discover_instagram_cursor_partitions",
    "_scrape_shared_instagram_posts_partitioned",
    "_scrape_shared_instagram_post_details_refresh",
    "_scrape_shared_instagram_posts",
]
