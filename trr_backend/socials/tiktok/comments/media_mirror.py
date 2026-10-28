# ruff: noqa: F821, UP037
"""TikTok comment media mirroring: mirror-needed checks, mirror field updates, mirror job
enqueueing, and the ``comment_media_mirror`` stage runner.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``. The
``_FIELD_UNSET`` sentinel below replaces the repository's ``FIELD_UNSET`` as the
keyword default so the module imports without the repository; no caller passes
the repository sentinel explicitly.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

from trr_backend.socials.control_plane.run_lifecycle import _legacy_module as _published_legacy_module
from trr_backend.socials.provider_registry import (
    LateNamespaceProvider,
    LateProviderProxy,
    adopt_published,
    publish_module_slot,
)

# Local keyword-default sentinel; see the module docstring.
_FIELD_UNSET = object()

if TYPE_CHECKING:
    from trr_backend.db import pg
    from trr_backend.socials.model_types import SeasonContext

    # Repository names resolved through the published provider namespace.
    COMMENT_MEDIA_MIRROR_STAGE: Any
    TIKTOK_COMMENT_MEDIA_MIRROR_JOB_TYPE: Any

    def _as_text_list(*args: Any, **kwargs: Any) -> Any: ...
    def _coerce_dt(*args: Any, **kwargs: Any) -> Any: ...
    def _create_job(*args: Any, **kwargs: Any) -> Any: ...
    def _iso(*args: Any, **kwargs: Any) -> Any: ...
    def _mirror_platform_media_to_s3_result(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_non_negative_int(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_unique_terms(*args: Any, **kwargs: Any) -> Any: ...
    def _now_utc(*args: Any, **kwargs: Any) -> Any: ...
    def _platform_comment_media_needs_mirror(*args: Any, **kwargs: Any) -> Any: ...
    def _resolve_week_windows(*args: Any, **kwargs: Any) -> Any: ...
    def _tiktok_comments_has_column(*args: Any, **kwargs: Any) -> Any: ...
    def _week_for_timestamp(*args: Any, **kwargs: Any) -> Any: ...
    def is_queue_enabled(*args: Any, **kwargs: Any) -> Any: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_tiktok_comment_needs_media_mirror",
    "_update_tiktok_comment_media_mirror_fields",
    "_enqueue_tiktok_comment_media_mirror_job",
    "_run_tiktok_comment_media_mirror_stage",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_COMMENT_MEDIA_MIRROR_PROVIDER",
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


def _tiktok_comment_needs_media_mirror(comment_row: dict[str, Any]) -> bool:
    _sync_core_overrides()
    return _platform_comment_media_needs_mirror("tiktok", comment_row)


def _update_tiktok_comment_media_mirror_fields(
    *,
    comment_id: str,
    hosted_media_urls: list[str] | object = _FIELD_UNSET,
    media_mirror_status: str | None | object = _FIELD_UNSET,
    media_mirror_error: str | None | object = _FIELD_UNSET,
    media_mirror_attempt_count: int | object = _FIELD_UNSET,
    media_mirror_last_attempt_at: datetime | None | object = _FIELD_UNSET,
    media_mirror_last_job_id: str | None | object = _FIELD_UNSET,
    conn: Any | None = None,
) -> None:
    _sync_core_overrides()
    assignments: list[str] = []
    params: list[Any] = []

    def _add(column: str, value: Any, *, as_jsonb: bool = False) -> None:
        if as_jsonb:
            assignments.append(f"{column} = %s::jsonb")
            params.append(json.dumps(value))
            return
        assignments.append(f"{column} = %s")
        params.append(value)

    if hosted_media_urls is not _FIELD_UNSET and _tiktok_comments_has_column("hosted_media_urls", conn=conn):
        _add("hosted_media_urls", list(cast(Iterable[Any], hosted_media_urls or [])), as_jsonb=True)
    if media_mirror_status is not _FIELD_UNSET and _tiktok_comments_has_column("media_mirror_status", conn=conn):
        _add("media_mirror_status", media_mirror_status)
    if media_mirror_error is not _FIELD_UNSET and _tiktok_comments_has_column("media_mirror_error", conn=conn):
        _add("media_mirror_error", media_mirror_error)
    if media_mirror_attempt_count is not _FIELD_UNSET and _tiktok_comments_has_column(
        "media_mirror_attempt_count", conn=conn
    ):
        _add("media_mirror_attempt_count", max(0, int(cast(Any, media_mirror_attempt_count))))
    if media_mirror_last_attempt_at is not _FIELD_UNSET and _tiktok_comments_has_column(
        "media_mirror_last_attempt_at", conn=conn
    ):
        _add("media_mirror_last_attempt_at", media_mirror_last_attempt_at)
    if media_mirror_last_job_id is not _FIELD_UNSET and _tiktok_comments_has_column(
        "media_mirror_last_job_id", conn=conn
    ):
        _add("media_mirror_last_job_id", media_mirror_last_job_id)

    if not assignments:
        return

    params.append(comment_id)
    with pg.db_cursor(conn=conn) as cur:
        pg.fetch_one_with_cursor(
            cur,
            f"update social.tiktok_comments set {', '.join(assignments)} where comment_id = %s returning id::text",
            params,
        )


def _enqueue_tiktok_comment_media_mirror_job(
    context: SeasonContext,
    *,
    run_id: str | None,
    source_scope: str,
    account: str,
    comment_row: dict[str, Any],
    parent_job_id: str | None,
    conn: Any | None = None,
) -> str | None:
    _sync_core_overrides()
    if not _tiktok_comments_has_column("media_urls", conn=conn):
        return None

    comment_id = str(comment_row.get("comment_id") or "").strip()
    comment_db_id = str(comment_row.get("id") or "").strip()
    post_id = str(comment_row.get("post_id") or "").strip()
    if not comment_id or not post_id:
        return None
    if not _room_callable("_tiktok_comment_needs_media_mirror", _tiktok_comment_needs_media_mirror)(comment_row):
        return None

    with pg.db_cursor(conn=conn) as cur:
        existing = pg.fetch_one_with_cursor(
            cur,
            """
            select id::text as id
            from social.scrape_jobs
            where platform = 'tiktok'
              and status in ('queued', 'pending', 'retrying', 'running')
              and coalesce(config->>'stage', metadata->>'stage', job_type) = %s
              and config->>'comment_id' = %s
              and (%s::uuid is null or run_id = %s::uuid)
            order by created_at desc
            limit 1
            """,
            [COMMENT_MEDIA_MIRROR_STAGE, comment_id, run_id, run_id],
        )
    if existing and existing.get("id"):
        return str(existing["id"])

    mirror_job_status = "queued" if is_queue_enabled() else "pending"
    config = {
        "run_id": run_id,
        "season_id": context.season_id,
        "show_id": context.show_id,
        "platform": "tiktok",
        "source_scope": source_scope,
        "stage": COMMENT_MEDIA_MIRROR_STAGE,
        "job_type": TIKTOK_COMMENT_MEDIA_MIRROR_JOB_TYPE,
        "account": account,
        "comment_id": comment_id,
        "comment_db_id": comment_db_id,
        "post_id": post_id,
        "parent_job_id": parent_job_id,
    }
    mirror_job_id = _create_job(
        context,
        run_id=run_id,
        platform="tiktok",
        source_scope=source_scope,
        job_type=TIKTOK_COMMENT_MEDIA_MIRROR_JOB_TYPE,
        stage=COMMENT_MEDIA_MIRROR_STAGE,
        config=config,
        initiated_by=None,
        status=mirror_job_status,
        priority=275,
        conn=conn,
    )
    _room_callable("_update_tiktok_comment_media_mirror_fields", _update_tiktok_comment_media_mirror_fields)(
        comment_id=comment_id,
        media_mirror_status="pending",
        media_mirror_error=None,
        media_mirror_last_job_id=mirror_job_id,
        conn=conn,
    )
    return mirror_job_id


def _run_tiktok_comment_media_mirror_stage(
    *,
    context: SeasonContext,
    job_id: str,
    config: dict[str, Any],
) -> tuple[int, int, dict[str, Any]]:
    _sync_core_overrides()
    if not _tiktok_comments_has_column("media_urls"):
        return (
            0,
            0,
            {
                "activity": {"phase": "comment_media_mirror_end", "last_progress_at": _iso(_now_utc())},
                "comment_media_mirror": {
                    "status": "skipped",
                    "error": "tiktok_comment_media_columns_missing",
                },
            },
        )

    comment_id = str(config.get("comment_id") or "").strip()
    comment_db_id = str(config.get("comment_db_id") or "").strip()
    if not comment_id and not comment_db_id:
        raise ValueError("comment_media_mirror_missing_comment_id")

    comment_row = pg.fetch_one(
        """
        select
          c.id::text as id,
          c.comment_id,
          c.post_id::text as post_id,
          coalesce(to_jsonb(c) -> 'media_urls', '[]'::jsonb) as media_urls,
          coalesce(to_jsonb(c) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
          coalesce(to_jsonb(c) ->> 'media_mirror_status', '') as media_mirror_status,
          coalesce(to_jsonb(c) ->> 'media_mirror_error', '') as media_mirror_error,
          p.video_id as video_id,
          p.posted_at
        from social.tiktok_comments c
        left join social.tiktok_posts p on p.id = c.post_id
        where c.season_id = %s
          and (
            (%s <> '' and c.comment_id = %s)
            or (%s <> '' and c.id = nullif(%s, '')::uuid)
          )
        order by c.created_at desc nulls last
        limit 1
        """,
        [context.season_id, comment_id, comment_id, comment_db_id, comment_db_id],
    )
    if not comment_row:
        raise ValueError("comment_media_mirror_comment_not_found")

    if not _room_callable("_tiktok_comment_needs_media_mirror", _tiktok_comment_needs_media_mirror)(comment_row):
        return (
            1,
            0,
            {
                "activity": {"phase": "comment_media_mirror_end", "last_progress_at": _iso(_now_utc())},
                "comment_media_mirror": {
                    "status": "up_to_date",
                    "comment_id": str(comment_row.get("comment_id") or ""),
                    "source_count": len(_as_text_list(comment_row.get("media_urls"))),
                    "mirrored_count": len(_as_text_list(comment_row.get("hosted_media_urls"))),
                },
            },
        )

    source_media_urls = _normalize_unique_terms(_as_text_list(comment_row.get("media_urls")))
    if not source_media_urls:
        _room_callable("_update_tiktok_comment_media_mirror_fields", _update_tiktok_comment_media_mirror_fields)(
            comment_id=str(comment_row.get("comment_id") or ""),
            media_mirror_status="failed",
            media_mirror_error="tiktok_comment_media_not_found",
            media_mirror_last_attempt_at=_now_utc(),
            media_mirror_last_job_id=job_id,
        )
        return (
            1,
            0,
            {
                "activity": {"phase": "comment_media_mirror_end", "last_progress_at": _iso(_now_utc())},
                "comment_media_mirror": {
                    "status": "failed",
                    "error": "tiktok_comment_media_not_found",
                    "comment_id": str(comment_row.get("comment_id") or ""),
                    "source_count": 0,
                    "mirrored_count": 0,
                },
            },
        )

    attempt_count = max(1, _normalize_non_negative_int(config.get("_attempt_count")))
    now_utc = _now_utc()
    _room_callable("_update_tiktok_comment_media_mirror_fields", _update_tiktok_comment_media_mirror_fields)(
        comment_id=str(comment_row.get("comment_id") or ""),
        media_mirror_status="pending",
        media_mirror_last_attempt_at=now_utc,
        media_mirror_attempt_count=attempt_count,
        media_mirror_last_job_id=job_id,
    )

    week_index_raw = config.get("week_index")
    week_index: int | None = None
    if week_index_raw is not None and str(week_index_raw).strip():
        try:
            week_index = int(week_index_raw)
        except (TypeError, ValueError):
            week_index = None
    if week_index is None:
        posted_at = _coerce_dt(comment_row.get("posted_at"))
        if posted_at is not None:
            try:
                windows, _ = _resolve_week_windows(
                    context,
                    timezone="America/New_York",
                    source_scope=str(config.get("source_scope") or "network"),
                    now_utc=_now_utc(),
                )
                week_window = _week_for_timestamp(posted_at, windows=windows, timezone="America/New_York")
                week_index = week_window.week_index if week_window else None
            except Exception:  # noqa: BLE001
                week_index = None

    mirror_comment = SimpleNamespace(
        id=str(comment_row.get("id") or ""),
        video_id=f"comment-{str(comment_row.get('comment_id') or '')}",
        thumbnail_url="",
        media_urls=source_media_urls,
    )
    result = _mirror_platform_media_to_s3_result(
        context,
        platform="tiktok",
        post=mirror_comment,
        week_index=week_index,
        prefer_first_media_as_thumbnail=False,
    )
    _room_callable("_update_tiktok_comment_media_mirror_fields", _update_tiktok_comment_media_mirror_fields)(
        comment_id=str(comment_row.get("comment_id") or ""),
        hosted_media_urls=list(result.get("hosted_media_urls") or []),
        media_mirror_status=str(result.get("status") or "") or None,
        media_mirror_error=str(result.get("error") or "") or None,
        media_mirror_last_attempt_at=now_utc,
        media_mirror_attempt_count=attempt_count,
        media_mirror_last_job_id=job_id,
    )
    if bool(result.get("retryable_error")) and str(result.get("status") or "") in {"partial", "failed"}:
        raise RuntimeError(str(result.get("error") or "comment_media_mirror_retryable"))

    mirrored_assets = _normalize_non_negative_int(result.get("mirrored_count"))
    metadata = {
        "activity": {"phase": "comment_media_mirror_end", "last_progress_at": _iso(now_utc)},
        "comment_media_mirror": {
            "platform": "tiktok",
            "comment_id": str(comment_row.get("comment_id") or ""),
            "post_id": str(comment_row.get("post_id") or ""),
            "status": str(result.get("status") or ""),
            "error": str(result.get("error") or "") or None,
            "source_count": _normalize_non_negative_int(result.get("source_count")),
            "mirrored_count": mirrored_assets,
            "week_index": week_index,
        },
    }
    return 1, mirrored_assets, metadata


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_enqueue_tiktok_comment_media_mirror_job",
    "_run_tiktok_comment_media_mirror_stage",
    "_tiktok_comment_needs_media_mirror",
    "_update_tiktok_comment_media_mirror_fields",
]
