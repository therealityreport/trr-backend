# ruff: noqa: F821, UP037
"""Shared-account TikTok catalog post persistence into the platform catalog table.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

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
    PLATFORM_CATALOG_POST_TABLES: Any

    def _as_text_list(*args: Any, **kwargs: Any) -> Any: ...
    def _first_non_empty_str(*args: Any, **kwargs: Any) -> Any: ...
    def _parse_hashtags(*args: Any, **kwargs: Any) -> Any: ...
    def _parse_mentions(*args: Any, **kwargs: Any) -> Any: ...

    _parse_tiktok_time: Any

    def _pg_upsert(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_catalog_payload_base(*args: Any, **kwargs: Any) -> Any: ...
    def _shared_catalog_post_url(*args: Any, **kwargs: Any) -> Any: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_upsert_shared_catalog_tiktok_post",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_SHARED_CATALOG_PERSISTENCE_PROVIDER",
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


def _upsert_shared_catalog_tiktok_post(
    *,
    run_id: str | None,
    account_handle: str,
    post: Any,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    _sync_core_overrides()
    video_id = str(getattr(post, "video_id", "") or "").strip()
    if not video_id:
        return None
    description = str(getattr(post, "description", "") or "")
    media_urls = [str(url).strip() for url in (getattr(post, "media_urls", []) or []) if str(url).strip()]
    payload = _shared_catalog_payload_base(
        source_id=video_id,
        account_handle=account_handle,
        posted_at=_parse_tiktok_time(getattr(post, "create_time", None)),
        permalink=_shared_catalog_post_url(
            "tiktok",
            account_handle=account_handle,
            source_id=video_id,
            explicit=_first_non_empty_str(getattr(post, "video_url", None), getattr(post, "permalink", None)),
        ),
        description=description or None,
        media_type="video",
        media_urls=media_urls,
        thumbnail_url=str(getattr(post, "thumbnail_url", "") or "").strip() or (media_urls[0] if media_urls else None),
        hashtags=_as_text_list(
            [*_as_text_list(getattr(post, "hashtags", []), strip_prefix="#"), *_parse_hashtags(description)],
            strip_prefix="#",
        ),
        mentions=_as_text_list(
            [
                *_as_text_list(getattr(post, "mentions", []), prefix="@", strip_prefix="@"),
                *_parse_mentions(description),
            ],
            prefix="@",
            strip_prefix="@",
        ),
        likes=getattr(post, "likes", 0),
        comments_count=getattr(post, "comments", 0),
        views=getattr(post, "views", 0),
        shares=getattr(post, "shares", 0),
        raw_data=post.to_dict() if hasattr(post, "to_dict") else {},
        run_id=run_id,
    )
    return _pg_upsert(PLATFORM_CATALOG_POST_TABLES["tiktok"], payload, conflict_col="source_id", conn=conn)


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_upsert_shared_catalog_tiktok_post",
]
