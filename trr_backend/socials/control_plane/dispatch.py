"""Dispatch and execution entrypoints for the social control plane."""

from __future__ import annotations

from trr_backend.socials.control_plane.dispatch_runtime import (
    claim_and_process_social_job,
    claim_next_queued_jobs,
    process_claimed_job,
    recover_and_dispatch_due_social_jobs,
)
from trr_backend.socials.control_plane.dispatch_runtime import legacy as _legacy
from trr_backend.socials.control_plane.run_reads import (
    get_run_progress_snapshot,
    list_run_summaries,
    list_runs,
)
from trr_backend.socials.instagram.media_mirror import requeue_instagram_media_mirror_jobs
from trr_backend.socials.pipelines.account_catalog.launch import start_social_account_catalog_backfill

SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE = _legacy.SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE
build_social_account_catalog_gap_analysis_operation_producer = (
    _legacy.build_social_account_catalog_gap_analysis_operation_producer
)
cancel_run = _legacy.cancel_run
ensure_media_mirror_s3_ready = _legacy.ensure_media_mirror_s3_ready
execute_run = _legacy.execute_run
execute_run_with_inline_worker_registration = _legacy.execute_run_with_inline_worker_registration
execute_social_account_catalog_run_auth_repair = _legacy.execute_social_account_catalog_run_auth_repair
ingest_season = _legacy.ingest_season
ingest_shared_accounts = _legacy.ingest_shared_accounts
list_jobs = _legacy.list_jobs
orchestrate_season_ingest = _legacy.orchestrate_season_ingest
preview_ingest_schedule = _legacy.preview_ingest_schedule
refresh_post = _legacy.refresh_post
register_week_detail_cache_invalidator = _legacy.register_week_detail_cache_invalidator
request_social_account_catalog_run_auth_repair = _legacy.request_social_account_catalog_run_auth_repair
requeue_media_mirror_jobs = _legacy.requeue_media_mirror_jobs
sync_newer_social_account_catalog = _legacy.sync_newer_social_account_catalog
sync_recent_social_account_catalog = _legacy.sync_recent_social_account_catalog
del _legacy

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
