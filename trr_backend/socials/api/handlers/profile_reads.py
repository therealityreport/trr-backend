"""Profile and catalog read handlers for the admin socials route surface."""

from __future__ import annotations

from typing import Any, Literal


def normalize_profile_summary_detail(value: str | None) -> str:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as social_core

    return social_core._normalize_social_account_profile_summary_detail(value)


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
