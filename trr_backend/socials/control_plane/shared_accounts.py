# ruff: noqa: F822
"""Shared-account catalog and profile flows for the social control plane."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import ModuleType
from typing import Any

from trr_backend.db import pg
from trr_backend.modal_dispatch import cancel_modal_function_call
from trr_backend.socials.control_plane.dispatch_runtime import legacy as _legacy
from trr_backend.socials.control_plane.shared_source_config import (
    get_shared_account_sources,
    put_shared_account_sources,
)
from trr_backend.socials.control_plane.shared_status_reads import (
    get_season_shared_status,
    list_shared_runs,
)
from trr_backend.socials.instagram.persistence import _batch_upsert_shared_catalog_instagram_posts
from trr_backend.socials.pipelines.account_catalog.progress import get_social_account_catalog_run_progress
from trr_backend.socials.provider_registry import LateNamespaceProvider
from trr_backend.socials.read_models.account_profile.common import (
    get_social_account_profile_collaborators_tags,
    get_social_account_profile_comments,
    get_social_account_profile_hashtags,
    get_social_account_profile_posts,
    get_social_account_profile_summary,
)

batch_upsert_shared_catalog_instagram_posts = _batch_upsert_shared_catalog_instagram_posts

_PROVIDER_BINDINGS = {
    "_default_targets": "_default_targets",
    "_normalize_catalog_backfill_window": "_normalize_catalog_backfill_window",
    "_shared_account_catalog_requires_modal_executor": "_shared_account_catalog_requires_modal_executor",
    "cancel_shared_run": "cancel_shared_run",
    "dismiss_social_account_catalog_run": "dismiss_social_account_catalog_run",
    "get_season_context": "get_season_context",
    "get_social_account_catalog_freshness": "get_social_account_catalog_freshness",
    "get_social_account_catalog_gap_analysis_status": "get_social_account_catalog_gap_analysis_status",
    "get_social_account_catalog_posts": "get_social_account_catalog_posts",
    "get_social_account_catalog_review_queue": "get_social_account_catalog_review_queue",
    "get_social_account_catalog_verification": "get_social_account_catalog_verification",
    "get_social_account_profile_hashtag_timeline": "get_social_account_profile_hashtag_timeline",
    "get_targets": "get_targets",
    "list_shared_review_queue": "list_shared_review_queue",
    "put_social_account_profile_hashtags": "put_social_account_profile_hashtags",
    "put_targets": "put_targets",
    "resolve_shared_review_queue_item": "resolve_shared_review_queue_item",
    "resolve_social_account_catalog_review_queue_item": "resolve_social_account_catalog_review_queue_item",
    "_legacy_cancel_social_account_catalog_run": "cancel_social_account_catalog_run",
}
_PUBLIC_PROVIDER_ALIASES = {
    "default_targets": "_default_targets",
    "normalize_catalog_backfill_window": "_normalize_catalog_backfill_window",
    "shared_account_catalog_requires_modal_executor": "_shared_account_catalog_requires_modal_executor",
}
_PROVIDER_BINDING_SOURCES = _PROVIDER_BINDINGS | {
    alias: _PROVIDER_BINDINGS[source]
    for alias, source in _PUBLIC_PROVIDER_ALIASES.items()
}


def _unconfigured_legacy_cancel_social_account_catalog_run(*args: Any, **kwargs: Any) -> Any:
    return _require_provider_ready()["cancel_social_account_catalog_run"](*args, **kwargs)


_legacy_cancel_social_account_catalog_run = _unconfigured_legacy_cancel_social_account_catalog_run


def _publish_provider_binding(name: str, value: Any) -> None:
    globals()[name] = value


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="SHARED_ACCOUNTS_PROVIDER",
    bindings=_PROVIDER_BINDING_SOURCES,
    publisher=lambda name, value: _publish_provider_binding(name, value),
    unconfigured_message="SHARED_ACCOUNTS_PROVIDER_UNCONFIGURED: provider publication has not completed",
)

if isinstance(_legacy, ModuleType):
    _PROVIDER.configure(_legacy.__dict__)
del _legacy


def _require_provider_ready() -> dict[str, Any]:
    return _PROVIDER.require()  # type: ignore[return-value]


def _configure_legacy_provider(provider: dict[str, Any]) -> None:
    """Copy the exact compatibility bindings after provider publication."""

    _PROVIDER.configure(provider)


def __getattr__(name: str) -> Any:
    if name in _PROVIDER_BINDINGS or name in _PUBLIC_PROVIDER_ALIASES:
        _require_provider_ready()
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def cancel_social_account_catalog_run(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
    cancelled_by: str | None = None,
    reconcile_summary: bool = True,
    conn: Any | None = None,
) -> dict[str, Any]:
    """Cancel every active lane and Modal call in the catalog launch group."""

    _require_provider_ready()
    normalized_platform = str(platform or "").strip().lower()
    normalized_account = str(account_handle or "").strip().lower().lstrip("@")
    parent = pg.fetch_one(
        "select id::text as id, config from social.scrape_runs where id = %s::uuid",
        [run_id],
        conn=conn,
    )
    launch_group_id = str(((parent or {}).get("config") or {}).get("launch_group_id") or "").strip()
    if not launch_group_id:
        return _legacy_cancel_social_account_catalog_run(
            platform=platform,
            account_handle=account_handle,
            run_id=run_id,
            cancelled_by=cancelled_by,
            reconcile_summary=reconcile_summary,
            conn=conn,
        )

    runs = pg.fetch_all(
        """
        select id::text as id, status, config
        from social.scrape_runs
        where coalesce(config->>'launch_group_id', '') = %s
          and lower(coalesce(config->>'platform', config->'platforms'->>0, '')) = %s
          and ltrim(lower(coalesce(
            config->>'account', config->>'account_handle', config->'accounts_override'->>0, ''
          )), '@') = %s
        """,
        [launch_group_id, normalized_platform, normalized_account],
        conn=conn,
    )
    run_ids = [str(row.get("id") or "").strip() for row in runs if str(row.get("id") or "").strip()]
    if not run_ids:
        run_ids = [str(run_id)]

    jobs = pg.fetch_all(
        """
        select
          id::text as id,
          run_id::text as run_id,
          status,
          nullif(metadata #>> '{dispatch,remote_invocation_id}', '') as remote_invocation_id
        from social.scrape_jobs
        where run_id::text = any(%s::text[])
           or coalesce(config->>'launch_group_id', '') = %s
        """,
        [run_ids, launch_group_id],
        conn=conn,
    )
    cancel_requested_at = _utcnow_iso()
    pg.execute_returning(
        """
        update social.scrape_runs
        set
          status = 'cancelled',
          cancelled_at = coalesce(cancelled_at, now()),
          completed_at = coalesce(completed_at, now()),
          config = coalesce(config, '{}'::jsonb) || %s::jsonb,
          summary = coalesce(summary, '{}'::jsonb) || %s::jsonb
        where id::text = any(%s::text[])
          and status in ('queued', 'pending', 'retrying', 'running', 'cancelling')
        returning id::text as id
        """,
        [
            json.dumps({"launch_state": "cancelled", "launch_task_resolution_pending": False}),
            json.dumps({"cancelled_by": cancelled_by, "cancel_requested_at": cancel_requested_at}),
            run_ids,
        ],
        conn=conn,
    )
    cancelled_jobs = pg.execute_returning(
        """
        update social.scrape_jobs
        set
          status = 'cancelled',
          completed_at = coalesce(completed_at, now()),
          error_message = coalesce(error_message, 'Cancelled by launch-group request'),
          metadata = coalesce(metadata, '{}'::jsonb) || %s::jsonb
        where (run_id::text = any(%s::text[]) or coalesce(config->>'launch_group_id', '') = %s)
          and status in (
            'queued', 'pending', 'retrying', 'running', 'claimed', 'dispatched', 'processing', 'cancelling'
          )
        returning id::text as id
        """,
        [
            json.dumps(
                {
                    "cancel_reason": "catalog_launch_group_cancelled",
                    "cancelled_by": cancelled_by,
                    "cancelled_at": cancel_requested_at,
                    "launch_group_id": launch_group_id,
                }
            ),
            run_ids,
            launch_group_id,
        ],
        conn=conn,
    )

    remote_results: dict[str, dict[str, Any]] = {}
    for job in jobs:
        call_id = str(job.get("remote_invocation_id") or "").strip()
        if call_id and call_id not in remote_results:
            remote_results[call_id] = cancel_modal_function_call(call_id)
        result = remote_results.get(call_id)
        if not result:
            continue
        inspection = dict(result.get("inspection") or {})
        pg.fetch_one(
            """
            update social.scrape_jobs
            set metadata = jsonb_set(
              coalesce(metadata, '{}'::jsonb),
              '{dispatch}',
              coalesce(metadata->'dispatch', '{}'::jsonb) || %s::jsonb,
              true
            )
            where id = %s::uuid
            returning id::text as id
            """,
            [
                json.dumps(
                    {
                        "remote_cancel_requested_at": result.get("cancel_requested_at"),
                        "remote_cancel_checked_at": result.get("checked_at"),
                        "remote_cancel_requested": bool(result.get("cancel_requested")),
                        "remote_invocation_status": inspection.get("status") or "unknown",
                        "remote_invocation_checked_at": inspection.get("checked_at") or result.get("checked_at"),
                        "remote_blocked_reason": inspection.get("reason") or result.get("reason"),
                    }
                ),
                str(job.get("id") or ""),
            ],
            conn=conn,
        )

    draining_call_ids = sorted(call_id for call_id, result in remote_results.items() if bool(result.get("draining")))
    return {
        "run_id": str(run_id),
        "status": "cancelled",
        "accepted": True,
        "launch_group_id": launch_group_id,
        "cancel_requested_at": cancel_requested_at,
        "cancelled_runs": len(run_ids),
        "cancelled_run_ids": run_ids,
        "cancelled_jobs": len(cancelled_jobs),
        "cancelled_job_ids": [str(row.get("id") or "") for row in cancelled_jobs],
        "remote_cancellations": list(remote_results.values()),
        "draining_remote_call_ids": draining_call_ids,
        "remote_drain_complete": not draining_call_ids,
    }


__all__ = [
    "_batch_upsert_shared_catalog_instagram_posts",
    "_default_targets",
    "_normalize_catalog_backfill_window",
    "_shared_account_catalog_requires_modal_executor",
    "batch_upsert_shared_catalog_instagram_posts",
    "cancel_shared_run",
    "cancel_social_account_catalog_run",
    "default_targets",
    "dismiss_social_account_catalog_run",
    "get_season_context",
    "get_season_shared_status",
    "get_shared_account_sources",
    "get_social_account_catalog_freshness",
    "get_social_account_catalog_gap_analysis_status",
    "get_social_account_catalog_posts",
    "get_social_account_catalog_review_queue",
    "get_social_account_catalog_run_progress",
    "get_social_account_catalog_verification",
    "get_social_account_profile_collaborators_tags",
    "get_social_account_profile_comments",
    "get_social_account_profile_hashtag_timeline",
    "get_social_account_profile_hashtags",
    "get_social_account_profile_posts",
    "get_social_account_profile_summary",
    "get_targets",
    "list_shared_review_queue",
    "list_shared_runs",
    "normalize_catalog_backfill_window",
    "put_shared_account_sources",
    "put_social_account_profile_hashtags",
    "put_targets",
    "resolve_shared_review_queue_item",
    "resolve_social_account_catalog_review_queue_item",
    "shared_account_catalog_requires_modal_executor",
]
