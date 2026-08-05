from __future__ import annotations

import json
import subprocess

import pytest

from scripts.modal import cleanup_wrong_workspace_deploy as cli


def _completed(command: list[str], stdout: object = None) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        command,
        0,
        stdout=json.dumps([] if stdout is None else stdout),
        stderr="",
    )


def test_cleanup_blocks_when_authoritative_workspace_is_not_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_run(command, **kwargs):
        calls.append({"command": command, **kwargs})
        if "verify_modal_readiness.py" in " ".join(command):
            return _completed(command, {"ok": False, "blocking_probe_failures": ["modal_workspace_mismatch"]})
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(cli, "python_command", lambda: "python")
    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    summary = cli.cleanup_wrong_workspace_deploy(
        wrong_profile="thb-bbl",
        wrong_workspace="tommy-hulihan-basketball",
        app_name="trr-backend-jobs",
        stop=True,
    )

    assert summary["ok"] is False
    assert summary["failure_reason"] == "authoritative_workspace_not_ready"
    assert calls[0]["env"]["MODAL_PROFILE"] == "admin-56995"


def test_cleanup_stops_wrong_workspace_app_after_authoritative_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_run(command, **kwargs):
        calls.append({"command": command, **kwargs})
        joined = " ".join(command)
        if "verify_modal_readiness.py" in joined:
            return _completed(
                command,
                {
                    "ok": True,
                    "modal_workspace": {
                        "profile": "admin-56995",
                        "workspace": "admin-56995",
                        "workspace_ok": True,
                    },
                    "blocking_probe_failures": [],
                },
            )
        if "profile list" in joined:
            return _completed(command, [{"name": "thb-bbl", "workspace": "tommy-hulihan-basketball", "active": True}])
        if "app list" in joined:
            return _completed(command, [{"Description": "trr-backend-jobs"}])
        if "app history trr-backend-jobs" in joined:
            return _completed(command, [{"Version": "1"}])
        if "app stop trr-backend-jobs" in joined:
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(cli, "python_command", lambda: "python")
    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    summary = cli.cleanup_wrong_workspace_deploy(
        wrong_profile="thb-bbl",
        wrong_workspace="tommy-hulihan-basketball",
        app_name="trr-backend-jobs",
        stop=True,
    )

    assert summary["ok"] is True
    assert summary["wrong_app_present"] is True
    assert summary["wrong_app_history_count"] == 1
    assert summary["stopped"] is True
    assert ["python", "-m", "modal", "app", "stop", "trr-backend-jobs", "--env", "main"] in [
        call["command"] for call in calls
    ]
    assert calls[0]["env"]["MODAL_PROFILE"] == "admin-56995"
    assert all(call["env"]["MODAL_PROFILE"] == "thb-bbl" for call in calls[1:])
    assert all(call["env"]["MODAL_WORKSPACE"] == "tommy-hulihan-basketball" for call in calls[1:])
    assert all(call["env"]["MODAL_ENVIRONMENT"] == "main" for call in calls[1:])
    assert all(call["env"]["TRR_MODAL_APP_NAME"] == "trr-backend-jobs" for call in calls[1:])
    assert all(call["env"]["TRR_MODAL_APP_REF"] == "trr_backend.modal_jobs" for call in calls[1:])


def test_cleanup_refuses_when_wrong_profile_is_authoritative(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def fake_run(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("Modal subprocess must not run for an authoritative-profile stop")

    monkeypatch.setattr(cli, "python_command", lambda: "python")
    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    with pytest.raises(ValueError, match="wrong_profile=admin-56995"):
        cli.cleanup_wrong_workspace_deploy(
            wrong_profile="admin-56995",
            wrong_workspace="tommy-hulihan-basketball",
            app_name="trr-backend-jobs",
            stop=True,
        )

    assert called is False


@pytest.mark.parametrize(
    ("overrides", "expected_fragment"),
    [
        ({"wrong_profile": "another-profile"}, "wrong_profile=another-profile"),
        ({"wrong_workspace": "another-workspace"}, "wrong_workspace=another-workspace"),
        ({"app_name": "another-app"}, "app_name=another-app"),
        ({"modal_environment": "staging"}, "environment=staging"),
    ],
)
def test_cleanup_stop_rejects_target_override_before_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, str],
    expected_fragment: str,
) -> None:
    called = False

    def fail_if_called(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("Modal subprocess must not run for a target override")

    monkeypatch.setattr(cli.subprocess, "run", fail_if_called)
    kwargs = {
        "wrong_profile": cli.DEFAULT_WRONG_PROFILE,
        "wrong_workspace": cli.DEFAULT_WRONG_WORKSPACE,
        "app_name": cli.DEFAULT_APP_NAME,
        "modal_environment": "main",
        "stop": True,
    }
    kwargs.update(overrides)

    with pytest.raises(ValueError, match=expected_fragment):
        cli.cleanup_wrong_workspace_deploy(**kwargs)

    assert called is False
