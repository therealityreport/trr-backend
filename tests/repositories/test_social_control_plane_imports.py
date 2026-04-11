"""Smoke tests for the social control-plane import surface."""

from __future__ import annotations

import trr_backend.repositories.social_season_analytics as legacy_repo
import trr_backend.socials.control_plane as control_plane
import trr_backend.socials.control_plane.dispatch as dispatch_reads
import trr_backend.socials.control_plane.recovery as recovery_surface
import trr_backend.socials.control_plane.shared_accounts as shared_reads
import trr_backend.socials.control_plane.worker_health as worker_health


def test_control_plane_reexports_worker_health_surface() -> None:
    assert control_plane.get_worker_health is legacy_repo.get_worker_health
    assert control_plane.get_queue_status is worker_health.get_queue_status
    assert control_plane.get_queue_status is not legacy_repo.get_queue_status
    assert control_plane.update_worker_heartbeat is legacy_repo.update_worker_heartbeat


def test_control_plane_reexports_run_read_surface() -> None:
    assert control_plane.list_runs is dispatch_reads.list_runs
    assert control_plane.list_runs is not legacy_repo.list_runs
    assert control_plane.get_run_progress_snapshot is dispatch_reads.get_run_progress_snapshot
    assert control_plane.get_run_progress_snapshot is not legacy_repo.get_run_progress_snapshot


def test_control_plane_reexports_shared_status_read_surface() -> None:
    assert control_plane.get_season_shared_status is shared_reads.get_season_shared_status
    assert control_plane.get_season_shared_status is not legacy_repo.get_season_shared_status
    assert control_plane.list_shared_runs is shared_reads.list_shared_runs
    assert control_plane.list_shared_runs is not legacy_repo.list_shared_runs


def test_control_plane_reexports_run_lifecycle_surface() -> None:
    assert control_plane.reconcile_run_summaries is recovery_surface.reconcile_run_summaries
    assert control_plane.reconcile_run_summaries is not legacy_repo.reconcile_run_summaries


def test_control_plane_reexports_runtime_helpers() -> None:
    assert control_plane._load_instagram_cookies is legacy_repo._load_instagram_cookies
    assert control_plane._load_tiktok_cookies is legacy_repo._load_tiktok_cookies
    assert control_plane.SocialWorkerUnavailableError is legacy_repo.SocialWorkerUnavailableError


def test_control_plane_reexports_shared_account_surface() -> None:
    assert control_plane.start_social_account_catalog_backfill is legacy_repo.start_social_account_catalog_backfill
    assert control_plane.get_social_account_catalog_verification is legacy_repo.get_social_account_catalog_verification
    assert control_plane._default_targets is legacy_repo._default_targets
