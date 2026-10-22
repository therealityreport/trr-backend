# ruff: noqa: F821, UP037
"""Shared-account TikTok catalog cursor-partition discovery for full-history backfills.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from trr_backend.socials.control_plane.run_lifecycle import _legacy_module as _published_legacy_module
from trr_backend.socials.provider_registry import (
    LateNamespaceProvider,
    LateProviderProxy,
    adopt_published,
    publish_module_slot,
)

if TYPE_CHECKING:
    # Repository names resolved through the published provider namespace.
    CATALOG_FULL_HISTORY_CURSOR_PARTITION_STRATEGY: Any

    class SharedAccountCursorPartition: ...

    def _bootstrap_shared_tiktok_account_context(*args: Any, **kwargs: Any) -> Any: ...
    def _catalog_backfill_run_scheduler_lanes(*args: Any, **kwargs: Any) -> Any: ...
    def _catalog_full_history_posts_per_shard(*args: Any, **kwargs: Any) -> Any: ...
    def _metadata_dict(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_non_negative_int(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_account_partition_key(*args: Any, **kwargs: Any) -> Any: ...

    logger: Any


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_discover_tiktok_cursor_partitions",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_SHARED_CATALOG_DISCOVERY_PROVIDER",
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


def _discover_tiktok_cursor_partitions(
    *,
    account_handle: str,
    runner_count: int,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
    partition_callback: Callable[[SharedAccountCursorPartition, int | None], None] | None = None,
) -> tuple[list[SharedAccountCursorPartition], dict[str, Any]]:
    _sync_core_overrides()
    context = _bootstrap_shared_tiktok_account_context(
        account_handle=account_handle,
        delay_seconds=0.35,
    )
    scraper = context["scraper"]
    sec_uid = str(context.get("sec_uid") or "").strip()
    if not sec_uid:
        profile_snapshot = _metadata_dict(context.get("profile_snapshot"))
        total_posts = _normalize_non_negative_int(context.get("total_posts")) or _normalize_non_negative_int(
            profile_snapshot.get("total_posts")
        )
        logger.warning(
            "TikTok discovery missing sec_uid for @%s; falling back to direct full-history posts job.",
            account_handle,
        )
        return [], {
            "error_code": "tiktok_discovery_sec_uid_missing",
            "error_class": "TikTokDiscoverySecUidMissing",
            "retryable": True,
            "pages_scanned": 0,
            "posts_checked": 0,
            "total_posts": total_posts or None,
            "partition_strategy": CATALOG_FULL_HISTORY_CURSOR_PARTITION_STRATEGY,
            "profile_snapshot": profile_snapshot,
        }
    total_posts = _normalize_non_negative_int(context.get("total_posts"))
    target_posts_per_shard = _catalog_full_history_posts_per_shard("tiktok")
    partitions: list[SharedAccountCursorPartition] = []
    lanes = _catalog_backfill_run_scheduler_lanes(runner_count)
    shard_index = 0
    cursor = 0
    partition_start_cursor = 0
    partition_start_at: datetime | None = None
    partition_posts = 0
    partition_pages = 0
    pages_scanned = 0
    posts_checked = 0
    seen_cursors: set[int] = set()
    expected_partition_count = (
        -(-total_posts // target_posts_per_shard) if total_posts > 0 and target_posts_per_shard > 0 else None
    )

    while True:
        pages_scanned += 1
        data = scraper.fetch_posts(account_handle, sec_uid, cursor, 0.35)
        if not data:
            break
        items = list(data.get("itemList") or [])
        if not items:
            break
        timestamps = [
            scraper._coerce_timestamp(item.get("createTime") or item.get("createTimeISO"))
            for item in items
            if scraper._coerce_timestamp(item.get("createTime") or item.get("createTimeISO")) > 0
        ]
        page_posts = len(items)
        posts_checked += page_posts
        partition_posts += page_posts
        partition_pages += 1
        page_newest_at = datetime.fromtimestamp(max(timestamps), tz=UTC) if timestamps else None
        page_oldest_at = datetime.fromtimestamp(min(timestamps), tz=UTC) if timestamps else None
        if partition_start_at is None:
            partition_start_at = page_newest_at
        next_cursor = int(data.get("cursor") or 0)
        has_more = bool(data.get("hasMore"))
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
        should_finalize = partition_posts >= target_posts_per_shard or not has_more or next_cursor <= 0
        if next_cursor > 0 and next_cursor in seen_cursors:
            should_finalize = True
            has_more = False
            next_cursor = 0
        if should_finalize:
            partition_key = _shared_account_partition_key(
                run_id="pending",
                platform="tiktok",
                account_handle=account_handle,
                shard_index=shard_index,
                cursor_start=str(partition_start_cursor),
                cursor_end=str(next_cursor) if next_cursor > 0 else None,
            )
            partition = SharedAccountCursorPartition(
                partition_key=partition_key,
                shard_index=shard_index,
                shard_total=0,
                runner_lane=lanes[shard_index % len(lanes)],
                cursor_start=str(partition_start_cursor),
                cursor_end=str(next_cursor) if next_cursor > 0 else None,
                boundary_start_at=partition_start_at,
                boundary_end_at=page_oldest_at,
                metadata={
                    "pages_scanned": partition_pages,
                    "posts_discovered": partition_posts,
                    "sec_uid": sec_uid,
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
        if not has_more or next_cursor <= 0:
            break
        cursor = next_cursor
        seen_cursors.add(cursor)
    shard_total = max(1, len(partitions))
    for partition in partitions:
        partition.shard_total = shard_total
    retrieval_meta = {
        "pages_scanned": pages_scanned,
        "posts_checked": posts_checked,
        "total_posts": total_posts or None,
        "sec_uid": sec_uid,
        "partition_strategy": CATALOG_FULL_HISTORY_CURSOR_PARTITION_STRATEGY,
        "profile_snapshot": _metadata_dict(context.get("profile_snapshot")),
    }
    endpoint_responses = _metadata_dict(
        _metadata_dict(getattr(scraper, "last_retrieval_meta", {}) or {}).get("endpoint_responses")
    )
    if endpoint_responses:
        retrieval_meta["endpoint_responses"] = endpoint_responses
    if pages_scanned == 1 and posts_checked <= 0 and total_posts > 0:
        retrieval_meta["error_code"] = "tiktok_discovery_empty_first_page"
        retrieval_meta["error_class"] = "TikTokDiscoveryEmptyFirstPage"
        retrieval_meta["retryable"] = False
        fetch_posts_response = _metadata_dict(endpoint_responses.get("fetch_posts"))
        logger.warning(
            "TikTok discovery got an empty first page for @%s despite "
            "total_posts=%s (request_id=%s, http_status=%s, content_length=%s)",
            account_handle,
            total_posts,
            str(fetch_posts_response.get("request_id") or "").strip() or "unknown",
            _normalize_non_negative_int(fetch_posts_response.get("http_status")) or "unknown",
            _normalize_non_negative_int(fetch_posts_response.get("content_length")),
        )
    logger.info(
        "TikTok discovery complete for @%s: pages=%s posts=%s partitions=%s total_posts=%s sec_uid_prefix=%s",
        account_handle,
        pages_scanned,
        posts_checked,
        len(partitions),
        total_posts or 0,
        sec_uid[:8] if sec_uid else "missing",
    )
    return partitions, retrieval_meta


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_discover_tiktok_cursor_partitions",
]
