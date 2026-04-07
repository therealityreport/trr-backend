from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.modal import repair_instagram_auth as cli


def _completed(*, stdout: str = "", returncode: int = 0) -> cli.subprocess.CompletedProcess[str]:
    return cli.subprocess.CompletedProcess(args=["python"], returncode=returncode, stdout=stdout, stderr="")


def test_run_repair_stops_when_local_validation_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check is True
        assert capture_output is True
        assert text is True
        commands.append(list(command))
        joined = " ".join(command)
        if "refresh_cookies.py" in joined:
            if "--force" in command:
                return _completed(stdout='{"platform":"instagram","validated":true,"reason":null}\n')
            return _completed(stdout='{"platform":"instagram","validated":false,"reason":"cookie_schema_invalid"}\n')
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "_python_command", lambda: "/venv/bin/python")
    monkeypatch.setattr(cli.subprocess, "run", _fake_run)

    summary = cli.run_repair(
        source_env=Path("/tmp/source.env"),
        modal_environment="main",
    )

    assert summary["ok"] is False
    assert [step["name"] for step in summary["steps"]] == ["refresh", "validate_local"]
    assert summary["steps"][-1]["status"] == "failed"
    assert summary["failure_reason"] == "local_validation_failed"
    assert all("sessionid" not in json.dumps(step, sort_keys=True) for step in summary["steps"])
    assert not any("prepare_named_secrets.py" in " ".join(command) for command in commands)


def test_run_repair_stops_when_remote_probe_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check is True
        assert capture_output is True
        assert text is True
        commands.append(list(command))
        joined = " ".join(command)
        if "refresh_cookies.py" in joined:
            if "--force" in command:
                return _completed(stdout='{"platform":"instagram","validated":true,"reason":null}\n')
            return _completed(stdout='{"platform":"instagram","validated":true,"reason":null}\n')
        if "prepare_named_secrets.py" in joined:
            return _completed(stdout="")
        if "modal deploy -m trr_backend.modal_jobs" in joined:
            return _completed(stdout="")
        if "verify_modal_readiness.py" in joined:
            return _completed(
                stdout=json.dumps(
                    {
                        "ok": False,
                        "remote_auth_probe": {
                            "platform": "instagram",
                            "ready": False,
                            "reason": "checkpoint_required",
                        },
                    }
                )
                + "\n"
            )
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "_python_command", lambda: "/venv/bin/python")
    monkeypatch.setattr(cli.subprocess, "run", _fake_run)

    summary = cli.run_repair(
        source_env=Path("/tmp/source.env"),
        modal_environment="main",
    )

    assert summary["ok"] is False
    assert [step["name"] for step in summary["steps"]] == [
        "refresh",
        "validate_local",
        "apply_named_secrets",
        "deploy_modal_app",
        "verify_remote_auth",
    ]
    assert summary["steps"][-1]["status"] == "failed"
    assert summary["failure_reason"] == "remote_probe_failed"
    assert summary["remote_auth_probe"]["reason"] == "checkpoint_required"


def test_main_emits_json_summary_without_leaking_command_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "source_env": Path("/tmp/source.env"),
                "modal_environment": "main",
                "json": True,
            },
        )(),
    )
    monkeypatch.setattr(
        cli,
        "run_repair",
        lambda **_kwargs: {
            "ok": True,
            "steps": [{"name": "refresh", "status": "ok"}],
            "failure_reason": None,
            "remote_auth_probe": {"platform": "instagram", "ready": True, "reason": None},
        },
    )

    rc = cli.main()
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["ok"] is True
    assert "sessionid" not in json.dumps(payload, sort_keys=True)
