"""Smoke tests for the social control-plane import surface."""

from __future__ import annotations

import trr_backend.repositories.social_season_analytics as legacy_repo
import trr_backend.socials.control_plane as control_plane


def test_control_plane_reexports_worker_health_surface() -> None:
    assert control_plane.get_worker_health is legacy_repo.get_worker_health
    assert control_plane.get_queue_status is legacy_repo.get_queue_status
    assert control_plane.update_worker_heartbeat is legacy_repo.update_worker_heartbeat


def test_control_plane_reexports_runtime_helpers() -> None:
    assert control_plane._load_instagram_cookies is legacy_repo._load_instagram_cookies
    assert control_plane._load_tiktok_cookies is legacy_repo._load_tiktok_cookies
    assert control_plane.SocialWorkerUnavailableError is legacy_repo.SocialWorkerUnavailableError


def test_control_plane_reexports_shared_account_surface() -> None:
    assert control_plane.start_social_account_catalog_backfill is legacy_repo.start_social_account_catalog_backfill
    assert control_plane.get_social_account_catalog_verification is legacy_repo.get_social_account_catalog_verification
    assert control_plane._default_targets is legacy_repo._default_targets
