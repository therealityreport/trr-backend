from __future__ import annotations

import json
from pathlib import Path

import pytest

from api import main as api_main


def _clear_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "TRR_LOCAL_DEV",
        "APP_ENV",
        "ENVIRONMENT",
        "TRR_ENV",
        "TRR_ENVIRONMENT",
        "PYTHON_ENV",
        "TRR_INTERNAL_ADMIN_SHARED_SECRET",
        "SUPABASE_JWT_SECRET",
        "TRR_MODAL_MAINTENANCE_OWNER_REQUIRED",
        "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED",
        "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED",
    ):
        monkeypatch.delenv(key, raising=False)


def test_validate_startup_config_allows_local_workspace_without_deployed_only_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("TRR_LOCAL_DEV", "1")
    monkeypatch.setattr(api_main, "log_database_resolution_summary", lambda: None)
    monkeypatch.setattr(
        api_main,
        "resolve_database_url_candidate_details",
        lambda: ({"connection_class": "session", "source": "TRR_DB_URL"},),
    )

    api_main._validate_startup_config()


def test_validate_startup_config_requires_deployed_only_secrets_for_deployed_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", "1")
    monkeypatch.setattr(api_main, "log_database_resolution_summary", lambda: None)
    monkeypatch.setattr(
        api_main,
        "resolve_database_url_candidate_details",
        lambda: ({"connection_class": "session", "source": "TRR_DB_URL"},),
    )

    with pytest.raises(RuntimeError) as excinfo:
        api_main._validate_startup_config()

    message = str(excinfo.value)
    assert "TRR_INTERNAL_ADMIN_SHARED_SECRET" in message
    assert "SUPABASE_JWT_SECRET" in message


def test_validate_startup_config_does_not_require_retired_screenalytics_envs_for_deployed_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", "1")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "jwt-secret")
    monkeypatch.setattr(api_main, "log_database_resolution_summary", lambda: None)
    monkeypatch.setattr(
        api_main,
        "resolve_database_url_candidate_details",
        lambda: ({"connection_class": "session", "source": "TRR_DB_URL"},),
    )

    api_main._validate_startup_config()


def test_modal_runtime_scheduler_is_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", raising=False)
    monkeypatch.setattr(
        "trr_backend.modal_dispatch.modal_dispatch_enabled",
        lambda: True,
    )

    assert api_main._modal_runtime_scheduler_enabled() is False


def test_modal_runtime_scheduler_follows_modal_dispatch_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", "1")
    monkeypatch.setattr(
        "trr_backend.modal_dispatch.modal_dispatch_enabled",
        lambda: True,
    )

    assert api_main._modal_runtime_scheduler_enabled() is True


def test_modal_runtime_scheduler_can_be_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", "0")
    monkeypatch.setattr(
        "trr_backend.modal_dispatch.modal_dispatch_enabled",
        lambda: True,
    )

    assert api_main._modal_runtime_scheduler_enabled() is False


def test_modal_runtime_scheduler_startup_rejects_no_maintenance_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", "1")

    with pytest.raises(RuntimeError, match="no active owner") as excinfo:
        api_main._validate_modal_maintenance_owner_config()
    message = str(excinfo.value)
    assert "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED=1" in message
    assert "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED=1" in message
    assert "TRR_MODAL_MAINTENANCE_OWNER_REQUIRED=1" in message
    assert "scripts/modal/prepare_named_secrets.py --apply" in message


def test_modal_runtime_scheduler_startup_fails_closed_without_required_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_runtime_env(monkeypatch)

    with pytest.raises(RuntimeError, match="no active owner"):
        api_main._validate_modal_maintenance_owner_config()


def test_modal_runtime_scheduler_startup_fails_closed_when_disabled_outside_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", "0")

    with pytest.raises(RuntimeError, match="no active owner"):
        api_main._validate_modal_maintenance_owner_config()


def test_modal_runtime_scheduler_startup_allows_explicit_local_dev_bypass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("TRR_LOCAL_DEV", "1")

    assert api_main._validate_modal_maintenance_owner_config() is None


def test_modal_runtime_scheduler_startup_rejects_duplicate_maintenance_owners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", "1")
    monkeypatch.setenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", "1")
    monkeypatch.setenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", "1")

    with pytest.raises(RuntimeError, match="duplicate active owners") as excinfo:
        api_main._validate_modal_maintenance_owner_config()
    message = str(excinfo.value)
    assert "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED='1'" in message
    assert "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED='1'" in message
    assert "scripts/modal/prepare_named_secrets.py --apply" in message


def test_modal_runtime_scheduler_startup_accepts_api_fallback_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", "1")
    monkeypatch.setenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", "1")

    assert api_main._validate_modal_maintenance_owner_config() == "api_runtime_scheduler"


def test_workspace_shared_env_manifest_matches_backend_contract() -> None:
    manifest_path = Path(__file__).resolve().parents[2] / "docs/workspace/shared-env-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    canonical = set(manifest["canonical"].keys())
    transitional = set(manifest["transitional"].keys())
    backend_contract = manifest["repo_validation"]["TRR-Backend"]

    assert {"TRR_DB_DIRECT_URL", "TRR_DB_URL", "TRR_DB_FALLBACK_URL", "TRR_INTERNAL_ADMIN_SHARED_SECRET"} <= canonical
    assert "SCREENALYTICS_API_URL" not in transitional
    assert "SCREENALYTICS_SERVICE_TOKEN" not in transitional
    assert set(backend_contract["db_any_of"]) == {
        "TRR_DB_DIRECT_URL",
        "TRR_DB_SESSION_URL",
        "TRR_DB_URL",
        "TRR_DB_FALLBACK_URL",
    }
    assert set(backend_contract["required_in_deployed"]) == {"TRR_INTERNAL_ADMIN_SHARED_SECRET", "SUPABASE_JWT_SECRET"}
    assert "SCREENALYTICS_API_URL" not in set(backend_contract["transitional_compat"])
    assert "SCREENALYTICS_SERVICE_TOKEN" not in set(backend_contract["transitional_compat"])
