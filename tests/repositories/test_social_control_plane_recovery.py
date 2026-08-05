"""Compatibility contracts for the control-plane recovery facade."""

from __future__ import annotations

import inspect
import subprocess
import sys
from pathlib import Path

import pytest

import trr_backend.socials.control_plane as control_plane
import trr_backend.socials.control_plane.recovery as recovery
import trr_backend.socials.social_season_analytics_impl as legacy_impl
from trr_backend.repositories import social_season_analytics as legacy_repo

_RECOVERY_NAMES = (
    "cancel_active_jobs",
    "cancel_claimed_job_before_processing",
    "cancel_dispatch_blocked_jobs",
    "cancel_stuck_jobs",
    "debug_ingest_job_with_openai",
    "dismiss_recent_failures",
    "recover_stale_running_jobs",
    "reset_social_ingest_health",
)


def test_recovery_exports_exact_legacy_callables_and_signatures() -> None:
    assert legacy_repo.__dict__ is legacy_impl.__dict__
    assert set(recovery.__all__) == {*_RECOVERY_NAMES, "reconcile_run_summaries"}
    assert "_legacy" not in recovery.__dict__

    for name in _RECOVERY_NAMES:
        recovery_callable = getattr(recovery, name)
        legacy_callable = getattr(legacy_impl, name)
        assert recovery_callable is legacy_callable
        assert getattr(control_plane, name) is recovery_callable
        assert inspect.signature(recovery_callable) == inspect.signature(legacy_callable)


def test_recovery_preserves_import_time_binding_after_late_legacy_patch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in _RECOVERY_NAMES:
        bound = getattr(recovery, name)
        monkeypatch.setattr(legacy_repo, name, object())
        assert getattr(recovery, name) is bound


def test_recovery_cold_import_completes_with_exact_namespace_identities() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    names = repr(_RECOVERY_NAMES)
    code = "\n".join(
        [
            "import importlib",
            f"names = {names}",
            "recovery = importlib.import_module('trr_backend.socials.control_plane.recovery')",
            "legacy = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
            "repo = importlib.import_module('trr_backend.repositories.social_season_analytics')",
            "control = importlib.import_module('trr_backend.socials.control_plane')",
            "assert repo.__dict__ is legacy.__dict__",
            "assert all(getattr(recovery, name) is getattr(legacy, name) for name in names)",
            "assert all(getattr(control, name) is getattr(recovery, name) for name in names)",
            "assert '_legacy' not in recovery.__dict__",
        ]
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=backend_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
