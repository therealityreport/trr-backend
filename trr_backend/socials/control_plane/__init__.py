"""Lazy canonical import surface for the backend social control plane."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORT_GROUPS: dict[str, tuple[str, ...]] = {
    "trr_backend.socials.control_plane.analytics": (
        "_build_drivers",
        "_build_ingest_shard_schedule",
        "_normalize_week_totals_payload",
        "_resolve_depth_defaults",
        "_rows_for_platform",
        "_rule_based_sentiment_for_text",
        "_text_contains_any_term",
        "_text_is_trailer_marker",
        "_threads_post_matches_show_terms",
        "_video_matches_season",
        "_week_detail_instagram",
        "_week_detail_tiktok",
        "_week_summary_fast_threads",
        "_week_summary_fast_tiktok",
        "_week_summary_fast_youtube",
        "_youtube_post_matches_show_terms",
        "_youtube_title_is_cross_show_excluded",
        "_youtube_video_matches_show_terms",
        "build_csv",
        "build_pdf",
        "get_analytics",
        "get_comments_coverage",
        "get_mirror_coverage",
        "get_post_comments",
        "get_tiktok_cast_members",
        "get_tiktok_content_health",
        "get_tiktok_hashtags",
        "get_tiktok_overview",
        "get_tiktok_post_detail",
        "get_tiktok_sentiment_trends",
        "get_tiktok_sound_detail",
        "get_tiktok_sound_posts",
        "get_tiktok_sounds",
        "get_week_detail",
        "get_week_detail_summary",
        "get_week_detail_summary_fast",
        "get_week_live_health_snapshot",
        "pdf_filename",
        "sentiment_for_text",
    ),
    "trr_backend.socials.control_plane.background_tasks": (
        "background_task_snapshot",
        "submit_named_background_task",
    ),
    "trr_backend.socials.control_plane.budget": (
        "STATE_IDENTITY_BLOCKED",
        "STATE_NORMAL",
        "STATE_PAUSED",
        "STATE_REDUCED",
        "build_budget_decision",
        "get_budget_decision",
    ),
    "trr_backend.socials.control_plane.dispatch": (
        "SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE",
        "build_social_account_catalog_gap_analysis_operation_producer",
        "cancel_run",
        "claim_and_process_social_job",
        "claim_next_queued_jobs",
        "ensure_media_mirror_s3_ready",
        "execute_run",
        "execute_run_with_inline_worker_registration",
        "execute_social_account_catalog_run_auth_repair",
        "get_run_progress_snapshot",
        "ingest_season",
        "ingest_shared_accounts",
        "list_jobs",
        "list_run_summaries",
        "list_runs",
        "orchestrate_season_ingest",
        "preview_ingest_schedule",
        "process_claimed_job",
        "recover_and_dispatch_due_social_jobs",
        "refresh_post",
        "register_week_detail_cache_invalidator",
        "request_social_account_catalog_run_auth_repair",
        "requeue_instagram_media_mirror_jobs",
        "requeue_media_mirror_jobs",
        "start_social_account_catalog_backfill",
        "sync_newer_social_account_catalog",
        "sync_recent_social_account_catalog",
    ),
    "trr_backend.socials.control_plane.models": (
        "COMMENT_MEDIA_MIRROR_STAGE",
        "IngestOptions",
        "SeasonContext",
        "SentimentAnalyzerContext",
        "WeekWindow",
    ),
    "trr_backend.socials.control_plane.recovery": (
        "cancel_active_jobs",
        "cancel_claimed_job_before_processing",
        "cancel_dispatch_blocked_jobs",
        "cancel_stuck_jobs",
        "debug_ingest_job_with_openai",
        "dismiss_recent_failures",
        "reconcile_run_summaries",
        "recover_stale_running_jobs",
        "reset_social_ingest_health",
    ),
    "trr_backend.socials.control_plane.runtime": (
        "SocialIngestConflictError",
        "SocialIngestValidationError",
        "SocialWorkerUnavailableError",
        "_adapt_payload_json_values",
        "_load_facebook_cookies",
        "_load_instagram_cookies",
        "_load_threads_cookies",
        "_load_tiktok_cookies",
        "_load_twikit_credentials",
        "_load_twitter_auth",
        "_pg_upsert_many",
        "_resolve_runtime_version_stamp",
        "check_platform_cookie_health",
        "refresh_platform_cookies_interactive",
    ),
    "trr_backend.socials.control_plane.shared_accounts": (
        "_batch_upsert_shared_catalog_instagram_posts",
        "_default_targets",
        "_normalize_catalog_backfill_window",
        "_shared_account_catalog_requires_modal_executor",
        "cancel_shared_run",
        "cancel_social_account_catalog_run",
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
        "put_shared_account_sources",
        "put_social_account_profile_hashtags",
        "put_targets",
        "resolve_shared_review_queue_item",
        "resolve_social_account_catalog_review_queue_item",
    ),
    "trr_backend.socials.control_plane.windowing": ("resolve_week_window",),
    "trr_backend.socials.control_plane.worker_health": (
        "assert_worker_available_when_queue_enabled",
        "get_queue_status",
        "get_trusted_local_worker_health",
        "get_worker_auth_capabilities",
        "get_worker_detail",
        "get_worker_health",
        "get_worker_health_for_lane",
        "is_queue_enabled",
        "mark_worker_stopped",
        "probe_remote_auth_health",
        "purge_inactive_workers",
        "update_worker_heartbeat",
    ),
}
_EXPORT_MODULES = {
    name: module_name
    for module_name, names in _EXPORT_GROUPS.items()
    for name in names
}
__all__ = [
    name
    for names in _EXPORT_GROUPS.values()
    for name in names
]


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
