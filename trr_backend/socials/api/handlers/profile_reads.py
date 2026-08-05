"""Profile and catalog read handlers for the admin socials route surface."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Literal


def normalize_profile_summary_detail(value: str | None) -> str:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    try:
        return social_core._normalize_social_account_profile_summary_detail(value)
    except RuntimeError as error:
        if "RUN_LIFECYCLE_PROVIDER_UNCONFIGURED" not in str(error):
            raise
        normalized = str(value or "lite").strip().lower()
        return normalized if normalized in {"full", "distribution"} else "lite"


def get_profile_summary(*, platform: str, account_handle: str, detail: str) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_profile_summary(
        platform=platform,
        account_handle=account_handle,
        detail=detail,
    )


def get_live_profile_total(*, platform: str, account_handle: str) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_live_profile_total(platform=platform, account_handle=account_handle)


def get_profile_posts(
    *,
    platform: str,
    account_handle: str,
    page: int,
    page_size: int,
    search: str | None,
    comments_only: bool,
    comment_filter: str | None,
    sort_by: str | None,
    sort_dir: str | None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_profile_posts(
        platform=platform,
        account_handle=account_handle,
        page=page,
        page_size=page_size,
        search=search,
        comments_only=comments_only,
        comment_filter=comment_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


def get_profile_comments(
    *,
    platform: str,
    account_handle: str,
    page: int,
    page_size: int,
    post_source_id: str | None,
    search: str | None,
    sort_by: str | None,
    sort_dir: str | None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_profile_comments(
        platform=platform,
        account_handle=account_handle,
        page=page,
        page_size=page_size,
        post_source_id=post_source_id,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


def get_profile_hashtags(
    *,
    platform: str,
    account_handle: str,
    window: Literal["all", "30d", "365d"] | None,
    assignment_status: Literal["all", "assigned", "unassigned"] | None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_profile_hashtags(
        platform=platform,
        account_handle=account_handle,
        window=window,
        assignment_status=assignment_status,
    )


def get_profile_hashtag_timeline(
    *,
    platform: str,
    account_handle: str,
    window: Literal["all", "30d", "365d"] | None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_profile_hashtag_timeline(
        platform=platform,
        account_handle=account_handle,
        window=window,
    )


def get_profile_collaborators_tags(*, platform: str, account_handle: str) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_profile_collaborators_tags(
        platform=platform,
        account_handle=account_handle,
    )


def get_catalog_posts(
    *,
    platform: str,
    account_handle: str,
    page: int,
    page_size: int,
    assignment_status: Literal["assigned", "unassigned", "ambiguous", "needs_review"] | None,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_catalog_posts(
        platform=platform,
        account_handle=account_handle,
        page=page,
        page_size=page_size,
        assignment_status=assignment_status,
    )


def get_catalog_post_detail(*, platform: str, account_handle: str, source_id: str) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_catalog_post_detail(
        platform=platform,
        account_handle=account_handle,
        source_id=source_id,
    )


def get_catalog_review_queue(*, platform: str, account_handle: str) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_catalog_review_queue(platform=platform, account_handle=account_handle)


def get_catalog_run_progress(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
    recent_log_limit: int,
    fast: bool,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_catalog_run_progress(
        platform=platform,
        account_handle=account_handle,
        run_id=run_id,
        recent_log_limit=recent_log_limit,
        fast=fast,
    )


def get_catalog_budget_decision(*, platform: str, account_handle: str) -> dict[str, Any]:
    from trr_backend.socials.control_plane.budget import build_budget_decision

    return build_budget_decision(
        lane=f"{str(platform or '').strip().lower() or 'instagram'}_backfill",
        platform=platform,
        account=account_handle,
    )


def get_catalog_run_diagnostics(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
) -> dict[str, Any]:
    # Import the canonical owner directly (no core-wrapper indirection needed).
    from trr_backend.socials.pipelines.account_catalog.progress import (
        get_social_account_catalog_run_diagnostics,
    )

    return get_social_account_catalog_run_diagnostics(
        platform=platform,
        account_handle=account_handle,
        run_id=run_id,
    )


def get_catalog_verification(*, platform: str, account_handle: str, run_id: str | None) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_catalog_verification(
        platform=platform,
        account_handle=account_handle,
        run_id=run_id,
    )


def get_catalog_gap_analysis_status(*, platform: str, account_handle: str) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_catalog_gap_analysis_status(platform=platform, account_handle=account_handle)


def get_catalog_freshness(
    *,
    platform: str,
    account_handle: str,
    use_cached_live_total_only: bool = False,
    statement_timeout_ms: int = 3000,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core.get_social_account_catalog_freshness(
        platform=platform,
        account_handle=account_handle,
        use_cached_live_total_only=use_cached_live_total_only,
        statement_timeout_ms=statement_timeout_ms,
    )


_CATALOG_SELECTED_TASKS = {"post_details", "comments", "media"}
_CATALOG_ACTIVE_FOLLOWUP_STATES = {"queued", "pending", "retrying", "running", "attached", "cancelling"}
_CATALOG_TERMINAL_STATUSES = {"cancelled", "failed"}


def _catalog_text(value: object, *, lowercase: bool = False) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text.lower() if lowercase else text


def _catalog_timestamp(value: object) -> str | None:
    if isinstance(value, datetime):
        normalized = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return normalized.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return _catalog_text(value)


def _catalog_record(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _catalog_selected_tasks(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    tasks: list[str] = []
    for item in value:
        task = _catalog_text(item, lowercase=True)
        if task in _CATALOG_SELECTED_TASKS and task not in tasks:
            tasks.append(task)
    return tasks


def _catalog_followups(value: object, *, run_status: str | None) -> dict[str, dict[str, Any]]:
    raw_followups = _catalog_record(value)
    comments = _catalog_record(raw_followups.get("comments"))
    media = _catalog_record(raw_followups.get("media"))
    followups: dict[str, dict[str, Any]] = {}

    comments_run_id = _catalog_text(comments.get("run_id"))
    comments_status = _catalog_text(comments.get("status"), lowercase=True)
    comments_state = _catalog_text(comments.get("state"), lowercase=True)
    if run_status in _CATALOG_TERMINAL_STATUSES and comments_state in _CATALOG_ACTIVE_FOLLOWUP_STATES:
        comments_state = run_status
    comments_source = _catalog_text(comments.get("source"), lowercase=True)
    if comments_run_id or comments_status or comments_state or comments_source:
        followups["comments"] = {
            "run_id": comments_run_id,
            "status": (
                run_status
                if run_status in _CATALOG_TERMINAL_STATUSES and not comments_run_id
                else comments_status
            ),
            "state": comments_state,
            "source": (
                comments_source
                if comments_source in {"new_run", "reused_run", "deferred_after_catalog"}
                else "deferred_after_catalog"
            ),
            "error_message": _catalog_text(comments.get("error_message")),
            "failed_at": _catalog_timestamp(comments.get("failed_at")),
            "retryable": comments.get("retryable") if isinstance(comments.get("retryable"), bool) else None,
        }

    raw_media_job_ids = media.get("enqueued_job_ids")
    media_job_ids = (
        [text for item in raw_media_job_ids if (text := _catalog_text(item))]
        if isinstance(raw_media_job_ids, list)
        else []
    )
    media_status = _catalog_text(media.get("status"), lowercase=True)
    media_state = _catalog_text(media.get("state"), lowercase=True)
    if run_status in _CATALOG_TERMINAL_STATUSES and media_state in _CATALOG_ACTIVE_FOLLOWUP_STATES:
        media_state = run_status
    media_source = _catalog_text(media.get("source"), lowercase=True)
    raw_media_count = media.get("enqueued_job_count", len(media_job_ids))
    try:
        media_count = max(0, int(raw_media_count))
    except (TypeError, ValueError):
        media_count = len(media_job_ids)
    if (
        _catalog_text(media.get("attachment_id"))
        or media_status
        or media_state
        or media_source
        or media_job_ids
        or media_count > 0
    ):
        followups["media"] = {
            "attachment_id": _catalog_text(media.get("attachment_id")),
            "status": run_status if run_status in _CATALOG_TERMINAL_STATUSES and not media_job_ids else media_status,
            "state": media_state,
            "source": (
                "comments_media_followups"
                if media_source == "comments_media_followups"
                else "catalog_media_mirror"
            ),
            "enqueued_job_ids": media_job_ids,
            "enqueued_job_count": media_count,
        }

    return followups


def get_catalog_recent_runs(*, platform: str, account_handle: str, limit: int) -> dict[str, Any]:
    """Return the stable, lightweight recent catalog-run envelope for one profile."""

    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    normalized_platform = _catalog_text(platform, lowercase=True) or ""
    normalized_handle = (_catalog_text(account_handle, lowercase=True) or "").lstrip("@")
    rows = social_core._catalog_recent_runs(
        normalized_platform,
        normalized_handle,
        limit=limit,
    )
    runs: list[dict[str, Any]] = []
    for raw_row in rows:
        row = _catalog_record(raw_row)
        run_id = _catalog_text(row.get("run_id"))
        if not run_id:
            continue
        status = _catalog_text(row.get("status"), lowercase=True)
        selected_tasks = _catalog_selected_tasks(row.get("selected_tasks"))
        effective_selected_tasks = _catalog_selected_tasks(row.get("effective_selected_tasks")) or selected_tasks
        runs.append(
            {
                "job_id": _catalog_text(row.get("job_id")) or "",
                "run_id": run_id,
                "status": status,
                "created_at": _catalog_timestamp(row.get("created_at")),
                "started_at": _catalog_timestamp(row.get("started_at")),
                "completed_at": _catalog_timestamp(row.get("completed_at")),
                "error_message": _catalog_text(row.get("error_message")),
                "catalog_action": _catalog_text(row.get("catalog_action"), lowercase=True),
                "catalog_action_scope": _catalog_text(row.get("catalog_action_scope"), lowercase=True),
                "date_start": _catalog_timestamp(row.get("date_start")),
                "date_end": _catalog_timestamp(row.get("date_end")),
                "launch_group_id": _catalog_text(row.get("launch_group_id")),
                "launch_state": _catalog_text(row.get("launch_state"), lowercase=True),
                "selected_tasks": selected_tasks,
                "effective_selected_tasks": effective_selected_tasks,
                "comments_run_id": _catalog_text(row.get("comments_run_id")),
                "attached_followups": _catalog_followups(row.get("attached_followups"), run_status=status),
            }
        )
    return {
        "platform": normalized_platform,
        "handle": normalized_handle,
        "catalog_recent_runs": runs,
    }
