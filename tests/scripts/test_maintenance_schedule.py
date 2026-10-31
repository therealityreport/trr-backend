from __future__ import annotations

import pytest

from scripts.modal.verify_maintenance_schedule import SCHEDULES, validate_observed_maintenance
from trr_backend.modal_runtime_config import (
    MAINTENANCE_API_ENV,
    MAINTENANCE_CRON_ENV,
    maintenance_runtime_env,
    validate_maintenance_deployment,
)


@pytest.mark.parametrize("cron", [False, True])
def test_deployment_projection_overrides_opposite_stale_runtime_secrets(cron: bool) -> None:
    deployment = {
        "TRR_MODAL_API_MAX_CONTAINERS": "1",
        MAINTENANCE_CRON_ENV: str(int(cron)),
        MAINTENANCE_API_ENV: str(int(not cron)),
    }
    projected = validate_maintenance_deployment(deployment)
    stale_secret = {
        MAINTENANCE_CRON_ENV: str(int(not cron)),
        MAINTENANCE_API_ENV: str(int(cron)),
        "TRR_MODAL_RUNTIME_HEARTBEAT_INTERVAL_SECONDS": "300",
        "TRR_MODAL_RUNTIME_SOCIAL_RECOVERY_INTERVAL_SECONDS": "600",
    }
    # Modal appends function env after named secrets; emulate that precedence.
    runtime_env = {**stale_secret, **projected}
    assert runtime_env[MAINTENANCE_CRON_ENV] == deployment[MAINTENANCE_CRON_ENV]
    assert runtime_env[MAINTENANCE_API_ENV] == deployment[MAINTENANCE_API_ENV]
    assert runtime_env["TRR_MODAL_RUNTIME_HEARTBEAT_INTERVAL_SECONDS"] == "60"
    assert runtime_env["TRR_MODAL_RUNTIME_SOCIAL_RECOVERY_INTERVAL_SECONDS"] == "120"
    schedules = {
        name: {"cron": {"cronString": expr, "timezone": "America/New_York"}} if cron else {}
        for name, expr in SCHEDULES.items()
    }
    runtime = {
        "state_free": True,
        "owner": "modal_singleton_cron" if cron else "api_runtime_scheduler",
        "env": runtime_env,
    }
    validate_observed_maintenance(projected, runtime, schedules, timezone="America/New_York")


@pytest.mark.parametrize(
    "env",
    [{}, {MAINTENANCE_CRON_ENV: "0", MAINTENANCE_API_ENV: "0"}, {MAINTENANCE_CRON_ENV: "1", MAINTENANCE_API_ENV: "1"}],
)
def test_deployment_rejects_missing_or_duplicate_owner(env: dict) -> None:
    with pytest.raises(RuntimeError, match="active owner"):
        validate_maintenance_deployment(env)


def test_state_free_projection_without_owner_remains_importable() -> None:
    assert maintenance_runtime_env({})[MAINTENANCE_API_ENV] == "0"


def test_readiness_rejects_live_duplicate_cron_with_api_owner() -> None:
    expected = validate_maintenance_deployment({MAINTENANCE_API_ENV: "1", "TRR_MODAL_API_MAX_CONTAINERS": "1"})
    runtime = {"state_free": True, "owner": "api_runtime_scheduler", "env": expected}
    schedules = {
        name: {"cron": {"cronString": expr, "timezone": "America/New_York"}} for name, expr in SCHEDULES.items()
    }
    with pytest.raises(RuntimeError, match="Deployed schedule"):
        validate_observed_maintenance(expected, runtime, schedules, timezone="America/New_York")


@pytest.mark.parametrize(
    "key,value",
    [
        (MAINTENANCE_CRON_ENV, "1"),
        ("TRR_MODAL_RUNTIME_HEARTBEAT_INTERVAL_SECONDS", "300"),
        ("TRR_MODAL_RUNTIME_SOCIAL_RECOVERY_INTERVAL_SECONDS", "600"),
    ],
)
def test_readiness_rejects_runtime_owner_or_interval_drift(key: str, value: str) -> None:
    expected = validate_maintenance_deployment({MAINTENANCE_API_ENV: "1", "TRR_MODAL_API_MAX_CONTAINERS": "1"})
    runtime = {"state_free": True, "owner": "api_runtime_scheduler", "env": {**expected, key: value}}
    with pytest.raises(RuntimeError, match="environment or intervals"):
        validate_observed_maintenance(expected, runtime, dict.fromkeys(SCHEDULES, {}), timezone="America/New_York")


@pytest.mark.parametrize("owner", [{}, {MAINTENANCE_CRON_ENV: "1", MAINTENANCE_API_ENV: "1"}])
def test_deploy_preflight_rejects_invalid_owner_before_provider_access(monkeypatch, owner):
    from scripts.modal import deploy_backend

    monkeypatch.setattr(deploy_backend, "pinned_modal_env", lambda: owner.copy())

    def unexpected(**kwargs):
        pytest.fail("invalid owner reached provider access")

    monkeypatch.setattr(deploy_backend, "verify_required_workspace", unexpected)
    assert deploy_backend.main(["--dry-run"]) == 2


@pytest.mark.parametrize(
    "scaling",
    [
        {},
        {"TRR_MODAL_API_MAX_CONTAINERS": "8"},
        {"TRR_MODAL_API_MIN_CONTAINERS": "0", "TRR_MODAL_API_MAX_CONTAINERS": "1"},
    ],
)
def test_api_owner_rejects_missing_or_multiple_replicas(scaling):
    with pytest.raises(RuntimeError, match="min/max containers"):
        validate_maintenance_deployment({MAINTENANCE_API_ENV: "1", **scaling})
