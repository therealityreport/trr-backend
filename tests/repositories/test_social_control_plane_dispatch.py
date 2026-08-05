"""Compatibility contracts for the control-plane dispatch facade."""

from __future__ import annotations

import inspect
import subprocess
import sys
from pathlib import Path

import pytest

import trr_backend.socials.control_plane as control_plane
import trr_backend.socials.control_plane.dispatch as dispatch
import trr_backend.socials.control_plane.dispatch_runtime as dispatch_runtime
import trr_backend.socials.social_season_analytics_impl as legacy_impl
from trr_backend.repositories import social_season_analytics as legacy_repo

_DISPATCH_LEGACY_NAMES = (
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
_CALLABLE_NAMES = _DISPATCH_LEGACY_NAMES[1:]


def test_dispatch_exports_exact_legacy_objects_and_signatures() -> None:
    assert legacy_repo.__dict__ is legacy_impl.__dict__
    assert dispatch_runtime.legacy is legacy_impl
    assert "_legacy" not in dispatch.__dict__
    for name in _DISPATCH_LEGACY_NAMES:
        dispatch_object = getattr(dispatch, name)
        legacy_object = getattr(legacy_impl, name)
        assert dispatch_object is legacy_object
        assert getattr(control_plane, name) is dispatch_object
    for name in _CALLABLE_NAMES:
        assert inspect.signature(getattr(dispatch, name)) == inspect.signature(getattr(legacy_impl, name))


def test_dispatch_preserves_import_time_binding_after_late_legacy_patch(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _DISPATCH_LEGACY_NAMES:
        bound = getattr(dispatch, name)
        monkeypatch.setattr(legacy_repo, name, object())
        assert getattr(dispatch, name) is bound


def test_dispatch_cold_import_completes_with_exact_namespace_identities() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    names = repr(_DISPATCH_LEGACY_NAMES)
    code = "\n".join(
        [
            "import importlib",
            f"names = {names}",
            "dispatch = importlib.import_module('trr_backend.socials.control_plane.dispatch')",
            "legacy = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
            "repo = importlib.import_module('trr_backend.repositories.social_season_analytics')",
            "control = importlib.import_module('trr_backend.socials.control_plane')",
            "runtime = importlib.import_module('trr_backend.socials.control_plane.dispatch_runtime')",
            "assert repo.__dict__ is legacy.__dict__",
            "assert runtime.legacy is legacy",
            "assert all(getattr(dispatch, name) is getattr(legacy, name) for name in names)",
            "assert all(getattr(control, name) is getattr(dispatch, name) for name in names)",
            "assert '_legacy' not in dispatch.__dict__",
        ]
    )
    result = subprocess.run([sys.executable, "-c", code], cwd=backend_root, check=False, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
