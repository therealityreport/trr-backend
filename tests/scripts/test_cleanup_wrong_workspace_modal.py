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
    assert ["python", "-m", "modal", "app", "stop", "trr-backend-jobs", "--yes"] in [
        call["command"] for call in calls
    ]
    assert calls[0]["env"]["MODAL_PROFILE"] == "admin-56995"
    assert all(call["env"]["MODAL_PROFILE"] == "thb-bbl" for call in calls[1:])


def test_cleanup_refuses_when_wrong_profile_is_authoritative(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(command, **_kwargs):
        joined = " ".join(command)
        if "verify_modal_readiness.py" in joined:
            return _completed(command, {"ok": True, "modal_workspace": {"workspace_ok": True}})
        if "profile list" in joined:
            return _completed(command, [{"name": "admin-56995", "workspace": "admin-56995", "active": True}])
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(cli, "python_command", lambda: "python")
    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    summary = cli.cleanup_wrong_workspace_deploy(
        wrong_profile="admin-56995",
        wrong_workspace="tommy-hulihan-basketball",
        app_name="trr-backend-jobs",
        stop=True,
    )

    assert summary["ok"] is False
    assert summary["failure_reason"] == "wrong_profile_resolves_to_authoritative_workspace"
