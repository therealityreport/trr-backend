# ruff: noqa: F821, UP037
"""Shared-account TikTok catalog configuration: effective task selection, comment toggles,
delays, per-post comment caps, comment-fetch timeouts and deadlines, and the catalog
comment override switch.

``_SharedTikTokCommentFetchTimeoutError`` stays in the legacy repository because it
subclasses the repository-owned YouTube operation timeout; ``_shared_tiktok_comment_fetch_deadline``
reaches it through the published provider namespace.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
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
    class _SharedTikTokCommentFetchTimeoutError: ...

    def _normalize_non_negative_int(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_optional_social_account_catalog_backfill_selected_tasks(*args: Any, **kwargs: Any) -> Any: ...
    def _resolve_positive_int_env(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_catalog_mode(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_youtube_operation_deadline(*args: Any, **kwargs: Any) -> Any: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_shared_tiktok_catalog_effective_tasks",
    "_shared_tiktok_catalog_comments_enabled",
    "_shared_tiktok_catalog_direct_comment_override",
    "_shared_tiktok_catalog_comment_delay_seconds",
    "_shared_tiktok_catalog_max_comments_per_post",
    "_shared_tiktok_comment_fetch_timeout_seconds",
    "_shared_tiktok_comment_fetch_deadline",
    "_tiktok_catalog_comment_override_enabled",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_SHARED_CATALOG_CONFIG_PROVIDER",
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


def _shared_tiktok_catalog_effective_tasks(config: Mapping[str, Any]) -> set[str]:
    _sync_core_overrides()
    tasks = _normalize_optional_social_account_catalog_backfill_selected_tasks(
        config.get("effective_selected_tasks")
    ) or _normalize_optional_social_account_catalog_backfill_selected_tasks(config.get("selected_tasks"))
    if tasks:
        return set(tasks)
    if not _shared_catalog_mode(config):
        return set()
    task_hint_present = any(
        key in config
        for key in (
            "details_refresh_skip_detail_fetch",
            "details_refresh_skip_media_followups",
            "tiktok_comments_in_posts_stage",
            "tiktok_direct_comment_api_override",
        )
    )
    if not task_hint_present:
        return set()

    inferred: list[str] = []
    if not bool(config.get("details_refresh_skip_detail_fetch")):
        inferred.append("post_details")
    if bool(config.get("tiktok_comments_in_posts_stage")):
        inferred.append("comments")
    if not bool(config.get("details_refresh_skip_media_followups")):
        inferred.append("media")
    return set(_normalize_optional_social_account_catalog_backfill_selected_tasks(inferred) or inferred)


def _shared_tiktok_catalog_comments_enabled(config: Mapping[str, Any]) -> bool:
    return "comments" in _room_callable(
        "_shared_tiktok_catalog_effective_tasks", _shared_tiktok_catalog_effective_tasks
    )(config) and bool(config.get("tiktok_comments_in_posts_stage"))


def _shared_tiktok_catalog_direct_comment_override(config: Mapping[str, Any]) -> bool | None:
    if _room_callable("_shared_tiktok_catalog_comments_enabled", _shared_tiktok_catalog_comments_enabled)(
        config
    ) and bool(config.get("tiktok_direct_comment_api_override")):
        return True
    return None


def _shared_tiktok_catalog_comment_delay_seconds() -> float:
    raw = str(os.getenv("SOCIAL_TIKTOK_CATALOG_COMMENT_DELAY_SECONDS") or "").strip()
    if not raw:
        return 0.35
    try:
        return max(0.05, float(raw))
    except ValueError:
        return 0.35


def _shared_tiktok_catalog_max_comments_per_post(config: Mapping[str, Any]) -> int:
    _sync_core_overrides()
    for key in ("max_comments_per_post", "catalog_max_comments_per_post"):
        if config.get(key) is not None:
            return _normalize_non_negative_int(config.get(key))
    return 0


def _shared_tiktok_comment_fetch_timeout_seconds(config: Mapping[str, Any]) -> int:
    _sync_core_overrides()
    for key in ("comment_fetch_timeout_seconds", "tiktok_comment_fetch_timeout_seconds"):
        if config.get(key) is not None:
            value = _normalize_non_negative_int(config.get(key))
            return max(5, value) if value > 0 else 0
    return max(5, _resolve_positive_int_env("SOCIAL_TIKTOK_COMMENT_FETCH_TIMEOUT_SECONDS", 180, minimum=5))


def _shared_tiktok_comment_fetch_deadline(seconds: int, *, video_id: str) -> Any:
    _sync_core_overrides()
    return _shared_youtube_operation_deadline(
        seconds,
        message=f"TikTok comment fetch timed out for {video_id} after {seconds}s",
        timeout_type=_SharedTikTokCommentFetchTimeoutError,
    )


def _tiktok_catalog_comment_override_enabled() -> bool:
    raw = (os.getenv("SOCIAL_TIKTOK_CATALOG_BACKFILL_COMMENT_OVERRIDE_ENABLED") or "").strip().lower()
    if not raw:
        return True
    return raw in {"1", "true", "yes", "on"}


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_shared_tiktok_catalog_comment_delay_seconds",
    "_shared_tiktok_catalog_comments_enabled",
    "_shared_tiktok_catalog_direct_comment_override",
    "_shared_tiktok_catalog_effective_tasks",
    "_shared_tiktok_catalog_max_comments_per_post",
    "_shared_tiktok_comment_fetch_deadline",
    "_shared_tiktok_comment_fetch_timeout_seconds",
    "_tiktok_catalog_comment_override_enabled",
]
