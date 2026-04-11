"""Dispatch and execution entrypoints for the social control plane."""

from __future__ import annotations

from trr_backend.repositories.social_season_analytics import (
    SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE,
    build_social_account_catalog_gap_analysis_operation_producer,
    cancel_run,
    ensure_media_mirror_s3_ready,
    execute_run,
    execute_run_with_inline_worker_registration,
    execute_social_account_catalog_run_auth_repair,
    ingest_season,
    ingest_shared_accounts,
    list_jobs,
    orchestrate_season_ingest,
    preview_ingest_schedule,
    refresh_post,
    register_week_detail_cache_invalidator,
    request_social_account_catalog_run_auth_repair,
    requeue_instagram_media_mirror_jobs,
    requeue_media_mirror_jobs,
    start_social_account_catalog_backfill,
    sync_newer_social_account_catalog,
    sync_recent_social_account_catalog,
)
from trr_backend.socials.control_plane.dispatch_runtime import (
    claim_and_process_social_job,
    claim_next_queued_jobs,
    process_claimed_job,
    recover_and_dispatch_due_social_jobs,
)
from trr_backend.socials.control_plane.run_reads import (
    get_run_progress_snapshot,
    list_run_summaries,
    list_runs,
)

__all__ = [
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
]
