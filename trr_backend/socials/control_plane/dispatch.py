# ruff: noqa: F822
"""Dispatch and execution entrypoints for the social control plane."""

from __future__ import annotations

from importlib import import_module
from typing import Any

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
from trr_backend.socials.instagram.media_mirror import requeue_instagram_media_mirror_jobs
from trr_backend.socials.pipelines.account_catalog.launch import start_social_account_catalog_backfill
from trr_backend.socials.provider_registry import LateNamespaceProvider, register_legacy_patchable_aliases

_LEGACY_EXPORT_NAMES = (
    "SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE",
    "build_social_account_catalog_gap_analysis_operation_producer",
    "cancel_run",
    "ensure_media_mirror_s3_ready",
    "execute_run",
    "execute_run_with_inline_worker_registration",
    "execute_social_account_catalog_run_auth_repair",
    "ingest_season",
    "ingest_shared_accounts",
    "list_jobs",
    "orchestrate_season_ingest",
    "preview_ingest_schedule",
    "refresh_post",
    "register_week_detail_cache_invalidator",
    "request_social_account_catalog_run_auth_repair",
    "requeue_media_mirror_jobs",
    "sync_newer_social_account_catalog",
    "sync_recent_social_account_catalog",
)


def _publish_provider_binding(name: str, value: Any) -> None:
    globals()[name] = value


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="SOCIAL_DISPATCH_PROVIDER",
    bindings={name: name for name in _LEGACY_EXPORT_NAMES},
    publisher=lambda name, value: _publish_provider_binding(name, value),
    unconfigured_message="SOCIAL_DISPATCH_PROVIDER_UNCONFIGURED: provider publication has not completed",
)


def _require_provider_ready() -> dict[str, Any]:
    return _PROVIDER.require()  # type: ignore[return-value]


def _configure_legacy_provider(provider: dict[str, Any]) -> None:
    """Publish compatibility exports after the exact provider finishes loading."""

    _PROVIDER.configure(provider)


def __getattr__(name: str) -> Any:
    if name in _LEGACY_EXPORT_NAMES:
        _require_provider_ready()
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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

_CANONICAL_EXPORT_MODULES = {
    "claim_and_process_social_job": "trr_backend.socials.control_plane.dispatch_runtime",
    "claim_next_queued_jobs": "trr_backend.socials.control_plane.dispatch_runtime",
    "get_run_progress_snapshot": "trr_backend.socials.control_plane.run_reads",
    "list_run_summaries": "trr_backend.socials.control_plane.run_reads",
    "list_runs": "trr_backend.socials.control_plane.run_reads",
    "process_claimed_job": "trr_backend.socials.control_plane.dispatch_runtime",
    "recover_and_dispatch_due_social_jobs": "trr_backend.socials.control_plane.dispatch_runtime",
    "requeue_instagram_media_mirror_jobs": "trr_backend.socials.instagram.media_mirror",
    "start_social_account_catalog_backfill": "trr_backend.socials.pipelines.account_catalog.launch",
}


def _refresh_legacy_patchable_export(name: str) -> Any:
    return getattr(import_module(_CANONICAL_EXPORT_MODULES[name]), name)


register_legacy_patchable_aliases(globals(), _CANONICAL_EXPORT_MODULES, _refresh_legacy_patchable_export)
