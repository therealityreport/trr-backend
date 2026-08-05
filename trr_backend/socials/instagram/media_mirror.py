"""Instagram media mirror stage and queue helpers."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from trr_backend.db import pg
from trr_backend.socials.model_types import SeasonContext
from trr_backend.socials.provider_registry import register_legacy_patchable_namespace

_LOCAL_ROOM_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_LEGACY_NAMESPACE: dict[str, Any] | None = None
_LEGACY_ORIGINALS: dict[str, Any] = {}
_MISSING = object()
_NO_LEGACY_FALLBACK = object()


def _configure_legacy_provider(
    namespace: dict[str, Any],
    originals: Mapping[str, Any],
) -> None:
    """Bind the supported monolith patch surface without importing it."""

    global _LEGACY_NAMESPACE, _LEGACY_ORIGINALS

    _LEGACY_NAMESPACE = namespace
    _LEGACY_ORIGINALS = dict(originals)


def _legacy_value(name: str, local_value: Any = _NO_LEGACY_FALLBACK) -> Any:
    namespace = _LEGACY_NAMESPACE
    if namespace is not None and name in namespace:
        return namespace[name]
    if local_value is not _NO_LEGACY_FALLBACK:
        return local_value
    raise RuntimeError(f"Instagram media-mirror provider is not configured: {name}")


def _legacy_callable(name: str, local_impl: Any = _NO_LEGACY_FALLBACK) -> Any:
    candidate = _legacy_value(name, local_impl)
    if not callable(candidate):
        raise TypeError(f"Instagram media-mirror provider is not callable: {name}")
    return candidate


def _room_callable(name: str, local_impl: Any) -> Any:
    candidate = _legacy_value(name, None)
    if callable(candidate) and candidate is not _LEGACY_ORIGINALS.get(name):
        return candidate
    return local_impl


def _instagram_post_source_urls(post_row: dict[str, Any]) -> tuple[str, list[str]]:
    normalize_unique_terms = _legacy_callable("_normalize_unique_terms")
    as_text_list = _legacy_callable("_as_text_list")
    source_media_urls = normalize_unique_terms(as_text_list(post_row.get("media_urls")))
    source_thumbnail_url = str(post_row.get("thumbnail_url") or "").strip() or (
        source_media_urls[0] if source_media_urls else ""
    )
    return source_thumbnail_url, source_media_urls


def _instagram_post_needs_media_mirror(post_row: dict[str, Any], *, conn: Any | None = None) -> bool:
    platform_post_needs_media_mirror = _legacy_callable("_platform_post_needs_media_mirror")
    return platform_post_needs_media_mirror("instagram", post_row, conn=conn)


def _update_instagram_post_media_mirror_fields(
    *,
    post_id: str,
    hosted_thumbnail_url: str | None | object = _MISSING,
    hosted_media_urls: list[str] | object = _MISSING,
    media_mirror_status: str | None | object = _MISSING,
    media_mirror_error: str | None | object = _MISSING,
    media_mirror_attempt_count: int | object = _MISSING,
    media_mirror_last_attempt_at: datetime | None | object = _MISSING,
    media_mirror_last_job_id: str | None | object = _MISSING,
    conn: Any | None = None,
) -> None:
    field_unset = _legacy_value("FIELD_UNSET")
    update_platform_post_media_mirror_fields = _legacy_callable("_update_platform_post_media_mirror_fields")

    def _provider_value(value: Any) -> Any:
        return field_unset if value is _MISSING else value

    update_platform_post_media_mirror_fields(
        platform="instagram",
        post_id=post_id,
        hosted_thumbnail_url=_provider_value(hosted_thumbnail_url),
        hosted_media_urls=_provider_value(hosted_media_urls),
        media_mirror_status=_provider_value(media_mirror_status),
        media_mirror_error=_provider_value(media_mirror_error),
        media_mirror_attempt_count=_provider_value(media_mirror_attempt_count),
        media_mirror_last_attempt_at=_provider_value(media_mirror_last_attempt_at),
        media_mirror_last_job_id=_provider_value(media_mirror_last_job_id),
        conn=conn,
    )


def _update_instagram_post_source_media_fields(
    *,
    post_id: str,
    thumbnail_url: str | None | object = _MISSING,
    media_urls: list[str] | object = _MISSING,
    conn: Any | None = None,
) -> None:
    field_unset = _legacy_value("FIELD_UNSET", _MISSING)
    instagram_posts_has_column = _legacy_callable("_instagram_posts_has_column")
    pg_runtime = _legacy_value("pg", pg)
    assignments: list[str] = []
    params: list[Any] = []

    def _add(column: str, value: Any, *, as_jsonb: bool = False) -> None:
        if as_jsonb:
            assignments.append(f"{column} = %s::jsonb")
            params.append(json.dumps(value))
            return
        assignments.append(f"{column} = %s")
        params.append(value)

    if (
        thumbnail_url is not _MISSING
        and thumbnail_url is not field_unset
        and instagram_posts_has_column("thumbnail_url", conn=conn)
    ):
        _add("thumbnail_url", thumbnail_url)
    if (
        media_urls is not _MISSING
        and media_urls is not field_unset
        and instagram_posts_has_column("media_urls", conn=conn)
    ):
        _add("media_urls", list(media_urls or []), as_jsonb=True)

    if not assignments:
        return

    sql = f"update social.instagram_posts set {', '.join(assignments)} where id = %s::uuid returning id::text"
    params.append(post_id)
    with pg_runtime.db_cursor(conn=conn) as cur:
        pg_runtime.fetch_one_with_cursor(cur, sql, params)


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
    enqueue_platform_media_mirror_job = _legacy_callable("_enqueue_platform_media_mirror_job")
    return enqueue_platform_media_mirror_job(
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
    run_platform_media_mirror_stage = _legacy_callable("_run_platform_media_mirror_stage")
    return run_platform_media_mirror_stage(
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
    requeue_media_mirror_jobs = _legacy_callable("requeue_media_mirror_jobs")
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
__all__ = [
    "_instagram_post_source_urls",
    "_instagram_post_needs_media_mirror",
    "_update_instagram_post_media_mirror_fields",
    "_update_instagram_post_source_media_fields",
    "_enqueue_instagram_media_mirror_job",
    "_run_instagram_media_mirror_stage",
    "requeue_instagram_media_mirror_jobs",
]

# Keep the route-facing leaf callable patchable through the published legacy
# repository namespace while the monolith is being retired.
register_legacy_patchable_namespace(globals(), (*__all__,))
