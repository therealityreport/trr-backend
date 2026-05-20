from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.modal import repair_instagram_auth as cli


def _completed(*, stdout: str = "", returncode: int = 0) -> cli.subprocess.CompletedProcess[str]:
    return cli.subprocess.CompletedProcess(args=["python"], returncode=returncode, stdout=stdout, stderr="")


def test_parse_last_json_line_accepts_pretty_json_stdout() -> None:
    payload = cli._parse_last_json_line('{\n  "ok": true,\n  "value": 1\n}\n', step_name="verify")

    assert payload == {"ok": True, "value": 1}


def test_run_repair_stops_when_local_validation_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check in {True, False}
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        commands.append(list(command))
        joined = " ".join(command)
        if "refresh_cookies.py" in joined:
            if "--force" in command:
                return _completed(stdout='{"platform":"instagram","validated":true,"reason":null}\n')
            return _completed(
                stdout='{"platform":"instagram","validated":false,"reason":"cookie_schema_invalid"}\n',
                returncode=1,
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
        "validate_local",
        "refresh",
        "validate_local_after_refresh",
    ]
    assert summary["steps"][-1]["status"] == "failed"
    assert summary["failure_reason"] == "local_validation_failed"
    assert all("sessionid" not in json.dumps(step, sort_keys=True) for step in summary["steps"])
    refresh_commands = [command for command in commands if "refresh_cookies.py" in " ".join(command)]
    assert refresh_commands
    assert all(command[command.index("--validation-mode") + 1] == "comments_endpoint" for command in refresh_commands)
    assert not any("prepare_named_secrets.py" in " ".join(command) for command in commands)


def test_run_repair_does_not_refresh_when_validation_reports_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check is False
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        commands.append(list(command))
        return _completed(
            stdout='{"platform":"instagram","validated":false,"reason":"checkpoint_required"}\n',
            returncode=1,
        )

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "_python_command", lambda: "/venv/bin/python")
    monkeypatch.setattr(cli.subprocess, "run", _fake_run)

    summary = cli.run_repair(
        source_env=Path("/tmp/source.env"),
        modal_environment="main",
    )

    assert summary["ok"] is False
    assert summary["failure_reason"] == cli.MANUAL_CHECKPOINT_REQUIRED_REASON
    assert summary["next_action"] == cli.MANUAL_CHECKPOINT_NEXT_ACTION
    assert summary["modal_secret_apply_reached"] is False
    assert summary["modal_deploy_reached"] is False
    assert [step["name"] for step in summary["steps"]] == ["validate_local"]
    assert all("--force" not in command for command in commands)


def test_run_repair_stops_when_remote_probe_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check in {True, False}
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
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
        "validate_local",
        "apply_named_secrets",
        "deploy_modal_app",
        "verify_remote_auth",
    ]
    assert summary["steps"][-1]["status"] == "failed"
    assert summary["failure_reason"] == "remote_probe_failed"
    assert summary["remote_auth_probe"]["reason"] == "checkpoint_required"
    refresh_commands = [command for command in commands if "refresh_cookies.py" in " ".join(command)]
    assert refresh_commands
    assert all(command[command.index("--validation-mode") + 1] == "comments_endpoint" for command in refresh_commands)


def test_run_repair_ignores_unrelated_missing_getty_probe_when_instagram_remote_auth_is_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check in {True, False}
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        commands.append(list(command))
        joined = " ".join(command)
        if "refresh_cookies.py" in joined:
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
                        "app_found": True,
                        "missing_secrets": [],
                        "missing_web_endpoints": [],
                        "missing_functions": ["probe_getty_remote_access"],
                        "remote_auth_probe": {
                            "platform": "instagram",
                            "ready": True,
                            "reason": None,
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

    assert summary["ok"] is True
    assert summary["failure_reason"] is None
    assert [step["name"] for step in summary["steps"]] == [
        "validate_local",
        "apply_named_secrets",
        "deploy_modal_app",
        "verify_remote_auth",
    ]
    assert summary["steps"][-1]["status"] == "ok"
    assert summary["remote_auth_probe"]["ready"] is True


def test_run_repair_verifies_instagram_posts_endpoint_when_account_is_supplied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check in {True, False}
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        commands.append(list(command))
        joined = " ".join(command)
        if "refresh_cookies.py" in joined:
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
                        "app_found": True,
                        "missing_secrets": [],
                        "missing_web_endpoints": [],
                        "missing_functions": [],
                        "remote_auth_probe": {
                            "platform": "instagram",
                            "ready": True,
                            "reason": None,
                        },
                        "instagram_posts_auth_probe": {
                            "platform": "instagram",
                            "account_handle": "thetraitorsus",
                            "ready": False,
                            "status": "transport_blocked",
                            "reason": "Error",
                            "execution_backend": "modal",
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
        account_handle="thetraitorsus",
    )

    verify_commands = [command for command in commands if "verify_modal_readiness.py" in " ".join(command)]
    assert verify_commands
    assert "--probe-instagram-posts-auth" in verify_commands[-1]
    assert "--probe-instagram-comments-auth" in verify_commands[-1]
    assert "thetraitorsus" in verify_commands[-1]
    assert summary["ok"] is False
    assert summary["failure_reason"] == "instagram_posts_transport_probe_failed"
    assert summary["instagram_posts_auth_probe"]["status"] == "transport_blocked"


def test_run_repair_blocks_browser_session_invalidated_comments_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check in {True, False}
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        commands.append(list(command))
        joined = " ".join(command)
        if "refresh_cookies.py" in joined:
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
                        "app_found": True,
                        "missing_secrets": [],
                        "missing_web_endpoints": [],
                        "missing_functions": [],
                        "remote_auth_probe": {
                            "platform": "instagram",
                            "ready": True,
                            "reason": None,
                        },
                        "instagram_comments_auth_probe": {
                            "platform": "instagram",
                            "account_handle": "thetraitorsus",
                            "ready": False,
                            "status": "auth_blocked",
                            "reason": cli.BROWSER_SESSION_INVALIDATED_REASON,
                            "execution_backend": "modal",
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
        account_handle="thetraitorsus",
    )

    verify_commands = [command for command in commands if "verify_modal_readiness.py" in " ".join(command)]
    assert verify_commands
    assert "--probe-instagram-comments-auth" in verify_commands[-1]
    assert summary["ok"] is False
    assert summary["failure_reason"] == "instagram_comments_browser_session_invalidated"
    assert summary["instagram_comments_auth_probe"]["reason"] == cli.BROWSER_SESSION_INVALIDATED_REASON


def test_verify_modal_readiness_failure_reason_maps_comments_html_challenge() -> None:
    failure_reason = cli._verify_modal_readiness_failure_reason(
        {
            "ok": False,
            "app_found": True,
            "missing_secrets": [],
            "missing_web_endpoints": [],
            "missing_functions": [],
            "remote_auth_probe": {
                "platform": "instagram",
                "ready": True,
                "reason": None,
            },
            "instagram_comments_auth_probe": {
                "platform": "instagram",
                "account_handle": "thetraitorsus",
                "ready": False,
                "status": "auth_blocked",
                "reason": "html_challenge_or_auth_required",
                "execution_backend": "modal",
            },
        }
    )

    assert failure_reason == "instagram_comments_html_challenge_or_auth_required"


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
                "account_handle": "",
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
