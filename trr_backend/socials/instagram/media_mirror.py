# ruff: noqa: F821, UP037
"""Instagram media mirror stage and queue helpers."""

from __future__ import annotations

from typing import Any

import trr_backend.socials.social_season_analytics_impl as _core

_RESERVED_CORE_EXPORTS = {
    "__builtins__",
    "__cached__",
    "__doc__",
    "__file__",
    "__loader__",
    "__name__",
    "__package__",
    "__spec__",
    "_core",
    "_IMPORTED_CORE_NAMES",
    "_LOCAL_ROOM_NAMES",
    "_RESERVED_CORE_EXPORTS",
    "_sync_core_overrides",
}
_IMPORTED_CORE_NAMES: set[str] = set()
for _name, _value in _core.__dict__.items():
    if _name in _RESERVED_CORE_EXPORTS:
        continue
    globals()[_name] = _value
    _IMPORTED_CORE_NAMES.add(_name)
_LOCAL_ROOM_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


def _sync_core_overrides() -> None:
    for _name in _IMPORTED_CORE_NAMES - _LOCAL_ROOM_NAMES:
        if hasattr(_core, _name):
            globals()[_name] = getattr(_core, _name)


def _room_callable(name: str, local_impl: Any) -> Any:
    candidate = getattr(_core, name, None)
    if callable(candidate) and candidate is not _CORE_ROOM_WRAPPERS.get(name):
        return candidate
    return local_impl


def _instagram_post_source_urls(post_row: dict[str, Any]) -> tuple[str, list[str]]:
    source_media_urls = _normalize_unique_terms(_as_text_list(post_row.get("media_urls")))
    source_thumbnail_url = str(post_row.get("thumbnail_url") or "").strip() or (
        source_media_urls[0] if source_media_urls else ""
    )
    return source_thumbnail_url, source_media_urls


def _instagram_post_needs_media_mirror(post_row: dict[str, Any], *, conn: Any | None = None) -> bool:
    return _platform_post_needs_media_mirror("instagram", post_row, conn=conn)


def _update_instagram_post_media_mirror_fields(
    *,
    post_id: str,
    hosted_thumbnail_url: str | None | object = FIELD_UNSET,
    hosted_media_urls: list[str] | object = FIELD_UNSET,
    media_mirror_status: str | None | object = FIELD_UNSET,
    media_mirror_error: str | None | object = FIELD_UNSET,
    media_mirror_attempt_count: int | object = FIELD_UNSET,
    media_mirror_last_attempt_at: datetime | None | object = FIELD_UNSET,
    media_mirror_last_job_id: str | None | object = FIELD_UNSET,
    conn: Any | None = None,
) -> None:
    _update_platform_post_media_mirror_fields(
        platform="instagram",
        post_id=post_id,
        hosted_thumbnail_url=hosted_thumbnail_url,
        hosted_media_urls=hosted_media_urls,
        media_mirror_status=media_mirror_status,
        media_mirror_error=media_mirror_error,
        media_mirror_attempt_count=media_mirror_attempt_count,
        media_mirror_last_attempt_at=media_mirror_last_attempt_at,
        media_mirror_last_job_id=media_mirror_last_job_id,
        conn=conn,
    )


def _update_instagram_post_source_media_fields(
    *,
    post_id: str,
    thumbnail_url: str | None | object = FIELD_UNSET,
    media_urls: list[str] | object = FIELD_UNSET,
    conn: Any | None = None,
) -> None:
    assignments: list[str] = []
    params: list[Any] = []

    def _add(column: str, value: Any, *, as_jsonb: bool = False) -> None:
        if as_jsonb:
            assignments.append(f"{column} = %s::jsonb")
            params.append(json.dumps(value))
            return
        assignments.append(f"{column} = %s")
        params.append(value)

    if thumbnail_url is not FIELD_UNSET and _instagram_posts_has_column("thumbnail_url", conn=conn):
        _add("thumbnail_url", thumbnail_url)
    if media_urls is not FIELD_UNSET and _instagram_posts_has_column("media_urls", conn=conn):
        _add("media_urls", list(media_urls or []), as_jsonb=True)

    if not assignments:
        return

    sql = f"update social.instagram_posts set {', '.join(assignments)} where id = %s::uuid returning id::text"
    params.append(post_id)
    with pg.db_cursor(conn=conn) as cur:
        pg.fetch_one_with_cursor(cur, sql, params)


def _enqueue_instagram_media_mirror_job(
    context: SeasonContext | None,
    *,
    run_id: str | None,
    source_scope: str,
    account: str,
    post_row: dict[str, Any],
    week_index: int | None,
    parent_job_id: str | None,
    conn: Any | None = None,
) -> str | None:
    return _enqueue_platform_media_mirror_job(
        context,
        platform="instagram",
        run_id=run_id,
        source_scope=source_scope,
        account=account,
        post_row=post_row,
        week_index=week_index,
        parent_job_id=parent_job_id,
        conn=conn,
    )


def _run_instagram_media_mirror_stage(
    *,
    context: SeasonContext,
    job_id: str,
    config: dict[str, Any],
) -> tuple[int, int, dict[str, Any]]:
    return _run_platform_media_mirror_stage(
        context=context,
        platform="instagram",
        job_id=job_id,
        config=config,
    )


def requeue_instagram_media_mirror_jobs(
    season_id: str,
    *,
    source_scope: str = "network",
    limit: int = 1000,
    failed_only: bool = False,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    return requeue_media_mirror_jobs(
        season_id,
        platform="instagram",
        source_scope=source_scope,
        limit=limit,
        failed_only=failed_only,
        date_start=date_start,
        date_end=date_end,
    )


_LOCAL_ROOM_NAMES = {
    "_instagram_post_source_urls",
    "_instagram_post_needs_media_mirror",
    "_update_instagram_post_media_mirror_fields",
    "_update_instagram_post_source_media_fields",
    "_enqueue_instagram_media_mirror_job",
    "_run_instagram_media_mirror_stage",
    "requeue_instagram_media_mirror_jobs",
}
_LOCAL_ROOM_FUNCTIONS = {_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES}
_CORE_ROOM_WRAPPERS = {_name: getattr(_core, _name, None) for _name in _LOCAL_ROOM_NAMES}
__all__ = [
    "_instagram_post_source_urls",
    "_instagram_post_needs_media_mirror",
    "_update_instagram_post_media_mirror_fields",
    "_update_instagram_post_source_media_fields",
    "_enqueue_instagram_media_mirror_job",
    "_run_instagram_media_mirror_stage",
    "requeue_instagram_media_mirror_jobs",
]
