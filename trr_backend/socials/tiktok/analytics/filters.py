# ruff: noqa: F821, UP037
"""TikTok analytics SQL helpers: comments-column checks, saves and canonical-URL expressions,
canonical URL resolution, and the shared ``_tiktok_filter_sql`` clause builder.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
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
    def _coerce_dt(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_tiktok_sound_id(*args: Any, **kwargs: Any) -> Any: ...
    def _platform_posts_has_column(*args: Any, **kwargs: Any) -> Any: ...
    def _relation_exists(*args: Any, **kwargs: Any) -> Any: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_tiktok_comments_has_column",
    "_tiktok_raw_saves_expr",
    "_tiktok_saves_expr",
    "_tiktok_canonical_url_expr",
    "_resolve_tiktok_canonical_url",
    "_tiktok_filter_sql",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_ANALYTICS_FILTERS_PROVIDER",
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


def _tiktok_comments_has_column(column: str, *, conn: Any | None = None) -> bool:
    return _core._platform_comments_has_column("tiktok", column, conn=conn)


def _tiktok_raw_saves_expr(alias: str) -> str:
    return (
        f"case when coalesce({alias}.raw_data ->> 'saves', '') ~ '^-?[0-9]+$' "
        f"then greatest((({alias}.raw_data ->> 'saves')::int), 0) else 0 end"
    )


def _tiktok_saves_expr(alias: str) -> str:
    raw_expr = _room_callable("_tiktok_raw_saves_expr", _tiktok_raw_saves_expr)(alias)
    if _core._platform_posts_has_column("tiktok", "saves"):
        return f"coalesce({alias}.saves, {raw_expr}, 0)"
    return f"coalesce({raw_expr}, 0)"


def _tiktok_canonical_url_expr(alias: str) -> str:
    url_candidates = [f"nullif({alias}.raw_data ->> 'url', '')"]
    if _core._platform_posts_has_column("tiktok", "url"):
        url_candidates.append(f"nullif({alias}.url, '')")
    url_candidates.append(
        f"case when coalesce({alias}.video_id, '') <> '' "
        f"then 'https://www.tiktok.com/@_/video/' || {alias}.video_id else null end"
    )
    return f"coalesce({', '.join(url_candidates)})"


def _resolve_tiktok_canonical_url(*, source_id: str, row_json: Mapping[str, Any], account: str | None = None) -> str:
    raw_data = _core._metadata_dict(row_json.get("raw_data"))
    candidates = [
        raw_data.get("url"),
        row_json.get("url"),
        (f"https://www.tiktok.com/@_/video/{source_id}" if str(source_id or "").strip() else None),
    ]
    username = str(row_json.get("username") or account or "").strip().lstrip("@")
    if username and str(source_id or "").strip():
        candidates.append(f"https://www.tiktok.com/@{username}/video/{source_id}")
    for candidate in candidates:
        normalized = str(candidate or "").strip()
        if normalized:
            return normalized
    return ""


def _tiktok_filter_sql(
    *,
    season_id: str,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    cast_member_id: str | None = None,
    hashtag: str | None = None,
    keyword: str | None = None,
    sound_id: str | None = None,
) -> tuple[str, list[Any]]:
    _sync_core_overrides()
    where_parts = ["p.season_id = %s::uuid"]
    params: list[Any] = [season_id]
    start_dt = _coerce_dt(date_start)
    end_dt = _coerce_dt(date_end)
    if start_dt:
        where_parts.append("p.posted_at >= %s")
        params.append(start_dt)
    if end_dt:
        where_parts.append("p.posted_at <= %s")
        params.append(end_dt)

    normalized_sound_id = _normalize_tiktok_sound_id(sound_id)
    if normalized_sound_id and _platform_posts_has_column("tiktok", "sound_id"):
        where_parts.append("coalesce(p.sound_id, '') = %s")
        params.append(normalized_sound_id)

    normalized_cast_member_id = str(cast_member_id or "").strip()
    if normalized_cast_member_id and _relation_exists("social.tiktok_post_cast_members"):
        where_parts.append(
            """
            exists (
              select 1
              from social.tiktok_post_cast_members cm
              where cm.post_id = p.id
                and cm.cast_member_id = %s::uuid
            )
            """
        )
        params.append(normalized_cast_member_id)

    normalized_hashtag = str(hashtag or "").strip().lstrip("#").lower()
    if normalized_hashtag and _relation_exists("social.tiktok_post_tokens"):
        where_parts.append(
            """
            exists (
              select 1
              from social.tiktok_post_tokens tk
              where tk.post_id = p.id
                and tk.token_type = 'hashtag'
                and tk.normalized_token = %s
            )
            """
        )
        params.append(normalized_hashtag)

    normalized_keyword = str(keyword or "").strip().lower()
    if normalized_keyword and _relation_exists("social.tiktok_post_tokens"):
        where_parts.append(
            """
            exists (
              select 1
              from social.tiktok_post_tokens tk
              where tk.post_id = p.id
                and tk.token_type = 'keyword'
                and tk.normalized_token = %s
            )
            """
        )
        params.append(normalized_keyword)

    return " and ".join(where_parts), params


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_resolve_tiktok_canonical_url",
    "_tiktok_canonical_url_expr",
    "_tiktok_comments_has_column",
    "_tiktok_filter_sql",
    "_tiktok_raw_saves_expr",
    "_tiktok_saves_expr",
]
