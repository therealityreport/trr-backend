from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.modal import repair_instagram_auth as cli

SCHEMA_FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "scripts" / "instagram_auth_repair_summary_schema.json"
)


def _completed(*, stdout: str = "", returncode: int = 0) -> cli.subprocess.CompletedProcess[str]:
    return cli.subprocess.CompletedProcess(args=["python"], returncode=returncode, stdout=stdout, stderr="")


@pytest.fixture(autouse=True)
def _isolated_auth_repair_cooldown(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cli, "_cooldown_state_path", lambda: tmp_path / "instagram-auth-repair-cooldown.json")


def test_run_repair_rejects_non_main_environment_before_subprocess(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subprocess_called = False

    def fake_run(*_args, **_kwargs):
        nonlocal subprocess_called
        subprocess_called = True
        return _completed(stdout='{"platform":"instagram","validated":true,"reason":null}\n')

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    with pytest.raises(ValueError, match="environment=staging"):
        cli.run_repair(
            source_env=Path("/tmp/source.env"),
            modal_environment="staging",
            dry_run=True,
        )

    assert subprocess_called is False


def test_run_repair_passes_full_pinned_identity_to_subprocess(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_envs: list[dict[str, str]] = []
    pinned = {
        "MODAL_PROFILE": "admin-56995",
        "MODAL_WORKSPACE": "admin-56995",
        "MODAL_ENVIRONMENT": "main",
        "TRR_MODAL_APP_NAME": "trr-backend-jobs",
        "TRR_MODAL_APP_REF": "trr_backend.modal_jobs",
    }

    def fake_run(
        _command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
        env: dict[str, str],
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check is False
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        captured_envs.append(dict(env))
        return _completed(stdout='{"platform":"instagram","validated":true,"reason":null}\n')

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "pinned_modal_env", lambda: dict(pinned))
    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    summary = cli.run_repair(
        source_env=Path("/tmp/source.env"),
        modal_environment="main",
        dry_run=True,
    )

    assert summary["ok"] is True
    assert captured_envs == [pinned]


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("TRR_MODAL_APP_NAME", "other-app"),
        ("TRR_MODAL_APP_REF", "other.module"),
    ],
)
def test_run_repair_rejects_wrong_app_or_module_before_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    key: str,
    value: str,
) -> None:
    subprocess_called = False
    pinned = {
        "MODAL_PROFILE": "admin-56995",
        "MODAL_WORKSPACE": "admin-56995",
        "MODAL_ENVIRONMENT": "main",
        "TRR_MODAL_APP_NAME": "trr-backend-jobs",
        "TRR_MODAL_APP_REF": "trr_backend.modal_jobs",
    }
    pinned[key] = value

    def fake_run(*_args, **_kwargs):
        nonlocal subprocess_called
        subprocess_called = True
        return _completed(stdout='{"platform":"instagram","validated":true,"reason":null}\n')

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "pinned_modal_env", lambda: dict(pinned))
    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    with pytest.raises(ValueError, match=key):
        cli.run_repair(
            source_env=Path("/tmp/source.env"),
            modal_environment="main",
            dry_run=True,
        )

    assert subprocess_called is False


def test_parse_last_json_line_accepts_pretty_json_stdout() -> None:
    payload = cli._parse_last_json_line('{\n  "ok": true,\n  "value": 1\n}\n', step_name="verify")

    assert payload == {"ok": True, "value": 1}


def test_failed_repair_summary_matches_schema_snapshot() -> None:
    schema = json.loads(SCHEMA_FIXTURE.read_text(encoding="utf-8"))

    summary = cli._failed_summary(steps=[], failure_reason=cli.MANUAL_CHECKPOINT_REQUIRED_REASON)

    assert schema["type"] == "object"
    assert set(schema["required"]).issubset(summary.keys())
    for key, property_schema in schema["properties"].items():
        assert key in summary
        expected_type = property_schema.get("type")
        if isinstance(expected_type, list):
            allowed = set(expected_type)
        else:
            allowed = {expected_type}
        value = summary[key]
        if value is None:
            actual_type = "null"
        elif isinstance(value, list):
            actual_type = "array"
        elif isinstance(value, bool):
            actual_type = "boolean"
        elif isinstance(value, dict):
            actual_type = "object"
        elif isinstance(value, str):
            actual_type = "string"
        else:
            actual_type = type(value).__name__
        assert actual_type in allowed, key

    advisory_schema = schema["properties"]["safe_advisory_state"]
    assert set(advisory_schema["required"]).issubset(summary["safe_advisory_state"].keys())


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
        env: dict[str, str] | None = None,
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

    summary = cli.run_repair(source_env=Path("/tmp/source.env"), modal_environment="main")

    assert summary["ok"] is False
    assert [step["name"] for step in summary["steps"]] == [
        "validate_local",
        "refresh",
    ]
    assert summary["steps"][-1]["status"] == "skipped"
    assert summary["steps"][-1]["reason"] == cli.AUTOMATED_COOKIE_REFRESH_DISABLED_REASON
    assert summary["failure_reason"] == cli.AUTOMATED_COOKIE_REFRESH_DISABLED_REASON
    assert summary["next_action"] == cli.MANUAL_AUTH_NEXT_ACTION
    assert summary["safety_stop"] is True
    assert summary["automated_cookie_refresh_allowed"] is False
    assert summary["cooldown_written"] is False
    assert summary["cooldown"] is None
    assert all("sessionid" not in json.dumps(step, sort_keys=True) for step in summary["steps"])
    refresh_commands = [command for command in commands if "refresh_cookies.py" in " ".join(command)]
    assert refresh_commands
    assert all(command[command.index("--validation-mode") + 1] == "comments_endpoint" for command in refresh_commands)
    assert all("--force" not in command for command in refresh_commands)
    assert not any("prepare_named_secrets.py" in " ".join(command) for command in commands)


def test_validate_local_only_does_not_apply_deploy_verify_or_write_cooldown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    commands: list[list[str]] = []
    cooldown_path = tmp_path / "instagram-auth-repair-cooldown.json"
    monkeypatch.setattr(cli, "_cooldown_state_path", lambda: cooldown_path)

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check is False
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        commands.append(list(command))
        joined = " ".join(command)
        if "refresh_cookies.py" in joined:
            return _completed(
                stdout='{"platform":"instagram","validated":false,"reason":"cookie_schema_invalid"}\n',
                returncode=1,
            )
        raise AssertionError(f"validate-local-only must not call side-effect command: {command}")

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "_python_command", lambda: "/venv/bin/python")
    monkeypatch.setattr(cli.subprocess, "run", _fake_run)

    summary = cli.run_validate_local_only()

    assert summary["ok"] is False
    assert summary["mode"] == "validate_local_only"
    assert summary["failure_reason"] == "cookie_schema_invalid"
    assert summary["cooldown_written"] is False
    assert summary["modal_secret_apply_reached"] is False
    assert summary["modal_deploy_reached"] is False
    assert summary["remote_verify_reached"] is False
    assert [step["name"] for step in summary["steps"]] == ["validate_local"]
    assert not cooldown_path.exists()
    assert not any("prepare_named_secrets.py" in " ".join(command) for command in commands)
    assert not any("modal deploy" in " ".join(command) for command in commands)
    assert not any("verify_modal_readiness.py" in " ".join(command) for command in commands)


def test_run_repair_allows_one_explicit_cookie_refresh_for_non_auth_validation_failure(
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
        env: dict[str, str] | None = None,
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
            if len([cmd for cmd in commands if "refresh_cookies.py" in " ".join(cmd) and "--force" not in cmd]) == 1:
                return _completed(
                    stdout='{"platform":"instagram","validated":false,"reason":"cookie_schema_invalid"}\n',
                    returncode=1,
                )
            return _completed(stdout='{"platform":"instagram","validated":true,"reason":null}\n')
        if "prepare_named_secrets.py" in joined:
            return _completed(stdout="")
        if "modal deploy -m trr_backend.modal_jobs" in joined:
            return _completed(stdout="")
        if "verify_modal_readiness.py" in joined:
            return _completed(
                stdout=json.dumps(
                    {
                        "ok": True,
                        "app_found": True,
                        "missing_secrets": [],
                        "missing_web_endpoints": [],
                        "missing_functions": [],
                        "remote_auth_probe": {"platform": "instagram", "ready": True, "reason": None},
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
        allow_cookie_refresh=True,
        confirm_instagram_refresh=cli.INSTAGRAM_REFRESH_CONFIRMATION,
    )

    assert summary["ok"] is True
    assert summary["automated_cookie_refresh_allowed"] is True
    assert [step["name"] for step in summary["steps"]] == [
        "validate_local",
        "refresh",
        "validate_local_after_refresh",
        "apply_named_secrets",
        "deploy_modal_app",
        "verify_remote_auth",
    ]
    refresh_commands = [command for command in commands if "refresh_cookies.py" in " ".join(command)]
    assert any("--force" in command for command in refresh_commands)


def test_run_repair_does_not_refresh_when_validation_reports_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    commands: list[list[str]] = []
    cooldown_path = tmp_path / "instagram-auth-repair-cooldown.json"
    monkeypatch.setattr(cli, "_cooldown_state_path", lambda: cooldown_path)

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
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
    assert summary["safety_stop"] is True
    assert summary["modal_secret_apply_reached"] is False
    assert summary["modal_deploy_reached"] is False
    assert [step["name"] for step in summary["steps"]] == ["validate_local"]
    assert cooldown_path.exists()
    assert all("--force" not in command for command in commands)


def test_run_repair_dry_run_plans_modal_steps_without_side_effects(
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
        env: dict[str, str] | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check is False
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        commands.append(list(command))
        if "refresh_cookies.py" in " ".join(command):
            return _completed(stdout='{"platform":"instagram","validated":true,"reason":null}\n')
        raise AssertionError(f"dry-run must not call side-effect command: {command}")

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "_python_command", lambda: "/venv/bin/python")
    monkeypatch.setattr(cli.subprocess, "run", _fake_run)

    summary = cli.run_repair(
        source_env=Path("/tmp/source.env"),
        modal_environment="main",
        account_handle="thetraitorsus",
        dry_run=True,
    )

    assert summary["ok"] is True
    assert summary["mode"] == "repair_dry_run"
    assert summary["dry_run"] is True
    assert summary["modal_secret_apply_reached"] is False
    assert summary["modal_deploy_reached"] is False
    assert summary["remote_verify_reached"] is False
    assert [step["name"] for step in summary["planned_modal_steps"]] == [
        "apply_named_secrets",
        "deploy_modal_app",
        "verify_remote_auth",
    ]
    assert [step["name"] for step in summary["steps"]] == [
        "validate_local",
        "apply_named_secrets",
        "deploy_modal_app",
        "verify_remote_auth",
    ]
    assert any("refresh_cookies.py" in " ".join(command) for command in commands)
    assert not any("prepare_named_secrets.py" in " ".join(command) for command in commands)
    assert not any("modal deploy" in " ".join(command) for command in commands)
    assert not any("verify_modal_readiness.py" in " ".join(command) for command in commands)


def test_run_repair_dry_run_checkpoint_does_not_write_cooldown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cooldown_path = tmp_path / "instagram-auth-repair-cooldown.json"
    monkeypatch.setattr(cli, "_cooldown_state_path", lambda: cooldown_path)

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check is False
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        assert "refresh_cookies.py" in " ".join(command)
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
        dry_run=True,
    )

    assert summary["ok"] is False
    assert summary["failure_reason"] == cli.MANUAL_CHECKPOINT_REQUIRED_REASON
    assert summary["cooldown"]["dry_run"] is True
    assert summary["cooldown"]["would_write"] is True
    assert summary["cooldown"]["path"] == str(cooldown_path)
    assert not cooldown_path.exists()


def test_run_repair_blocks_login_prompt_even_when_cookie_refresh_is_allowed(
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
        env: dict[str, str] | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check is False
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        commands.append(list(command))
        return _completed(
            stdout='{"platform":"instagram","validated":false,"reason":"login_required"}\n',
            returncode=1,
        )

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "_python_command", lambda: "/venv/bin/python")
    monkeypatch.setattr(cli.subprocess, "run", _fake_run)

    summary = cli.run_repair(
        source_env=Path("/tmp/source.env"),
        modal_environment="main",
        allow_cookie_refresh=True,
        confirm_instagram_refresh=cli.INSTAGRAM_REFRESH_CONFIRMATION,
    )

    assert summary["ok"] is False
    assert summary["failure_reason"] == cli.MANUAL_AUTH_REQUIRED_REASON
    assert summary["next_action"] == cli.MANUAL_AUTH_NEXT_ACTION
    assert summary["automated_cookie_refresh_allowed"] is True
    assert summary["safety_stop"] is True
    assert [step["name"] for step in summary["steps"]] == ["validate_local", "refresh"]
    assert summary["steps"][-1]["reason"] == cli.MANUAL_AUTH_REQUIRED_REASON
    assert all("--force" not in command for command in commands)


@pytest.mark.parametrize(
    ("validation_reason", "expected_failure_reason", "expected_next_action"),
    [
        ("challenge_required", cli.MANUAL_CHECKPOINT_REQUIRED_REASON, cli.MANUAL_CHECKPOINT_NEXT_ACTION),
        ("verification_required", cli.MANUAL_AUTH_REQUIRED_REASON, cli.MANUAL_AUTH_NEXT_ACTION),
    ],
)
def test_run_repair_blocks_manual_challenge_and_verification_states(
    monkeypatch: pytest.MonkeyPatch,
    validation_reason: str,
    expected_failure_reason: str,
    expected_next_action: str,
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
        env: dict[str, str] | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check is False
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        commands.append(list(command))
        return _completed(
            stdout=json.dumps(
                {
                    "platform": "instagram",
                    "validated": False,
                    "reason": validation_reason,
                }
            )
            + "\n",
            returncode=1,
        )

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "_python_command", lambda: "/venv/bin/python")
    monkeypatch.setattr(cli.subprocess, "run", _fake_run)

    summary = cli.run_repair(
        source_env=Path("/tmp/source.env"),
        modal_environment="main",
        allow_cookie_refresh=True,
        confirm_instagram_refresh=cli.INSTAGRAM_REFRESH_CONFIRMATION,
    )

    assert summary["ok"] is False
    assert summary["failure_reason"] == expected_failure_reason
    assert summary["next_action"] == expected_next_action
    assert summary["modal_secret_apply_reached"] is False
    assert summary["modal_deploy_reached"] is False
    assert summary["remote_verify_reached"] is False
    assert summary["safe_advisory_state"]["active"] is True
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
        env: dict[str, str] | None = None,
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
    assert summary["failure_reason"] == cli.MANUAL_CHECKPOINT_REQUIRED_REASON
    assert summary["next_action"] == cli.MANUAL_CHECKPOINT_NEXT_ACTION
    assert summary["safety_stop"] is True
    assert summary["modal_secret_apply_reached"] is True
    assert summary["modal_deploy_reached"] is True
    assert summary["remote_verify_reached"] is True
    assert summary["safe_advisory_state"]["active"] is True
    assert summary["safe_advisory_state"]["clear_requires_validation"] is True
    assert summary["remote_auth_probe"]["reason"] == "checkpoint_required"
    refresh_commands = [command for command in commands if "refresh_cookies.py" in " ".join(command)]
    assert refresh_commands
    assert all(command[command.index("--validation-mode") + 1] == "comments_endpoint" for command in refresh_commands)


@pytest.mark.parametrize(
    ("verify_payload", "expected_reason"),
    [
        ({"ok": False, "app_found": False}, "modal_app_missing"),
        ({"ok": False, "app_found": True, "missing_secrets": ["trr-social-auth"]}, "missing_named_secrets"),
        (
            {
                "ok": False,
                "app_found": True,
                "missing_secrets": [],
                "missing_web_endpoints": [],
                "remote_auth_probe": {"platform": "instagram", "ready": False, "reason": "transport_timeout"},
            },
            "remote_probe_failed",
        ),
    ],
)
def test_run_repair_keeps_infra_remote_failures_retryable_without_cooldown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    verify_payload: dict[str, object],
    expected_reason: str,
) -> None:
    commands: list[list[str]] = []
    cooldown_path = tmp_path / "instagram-auth-repair-cooldown.json"
    monkeypatch.setattr(cli, "_cooldown_state_path", lambda: cooldown_path)

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
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
            return _completed(stdout=json.dumps(verify_payload) + "\n")
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "_python_command", lambda: "/venv/bin/python")
    monkeypatch.setattr(cli.subprocess, "run", _fake_run)

    summary = cli.run_repair(
        source_env=Path("/tmp/source.env"),
        modal_environment="main",
    )

    assert summary["ok"] is False
    assert summary["failure_reason"] == expected_reason
    assert summary["cooldown_written"] is False
    assert summary["cooldown"] is None
    assert not cooldown_path.exists()
    assert summary["modal_secret_apply_reached"] is True
    assert summary["modal_deploy_reached"] is True
    assert summary["remote_verify_reached"] is True


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
        env: dict[str, str] | None = None,
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
        env: dict[str, str] | None = None,
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
    assert "--strict-instagram-comments-auth" in verify_commands[-1]
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
        env: dict[str, str] | None = None,
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
    assert "--strict-instagram-comments-auth" in verify_commands[-1]
    assert summary["ok"] is False
    assert summary["failure_reason"] == cli.MANUAL_AUTH_REQUIRED_REASON
    assert summary["next_action"] == cli.MANUAL_AUTH_NEXT_ACTION
    assert summary["safety_stop"] is True
    assert summary["remote_verify_reached"] is True
    assert summary["safe_advisory_state"]["active"] is True
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

    assert failure_reason == cli.MANUAL_CHECKPOINT_REQUIRED_REASON


def test_clear_auth_repair_cooldown_requires_passing_local_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cooldown_path = tmp_path / "instagram-auth-repair-cooldown.json"
    monkeypatch.setattr(cli, "_cooldown_state_path", lambda: cooldown_path)
    cli._write_cooldown_state(reason=cli.MANUAL_CHECKPOINT_REQUIRED_REASON)

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check is False
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        assert "refresh_cookies.py" in " ".join(command)
        assert "--force" not in command
        return _completed(
            stdout='{"platform":"instagram","validated":false,"reason":"checkpoint_required"}\n',
            returncode=1,
        )

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "_python_command", lambda: "/venv/bin/python")
    monkeypatch.setattr(cli.subprocess, "run", _fake_run)

    summary = cli.run_clear_auth_repair_cooldown()

    assert summary["ok"] is False
    assert summary["mode"] == "clear_auth_repair_cooldown"
    assert summary["cleared"] is False
    assert summary["failure_reason"] == cli.MANUAL_CHECKPOINT_REQUIRED_REASON
    assert summary["safe_advisory_state"]["active"] is True
    assert cooldown_path.exists()


def test_clear_auth_repair_cooldown_clears_after_passing_local_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cooldown_path = tmp_path / "instagram-auth-repair-cooldown.json"
    monkeypatch.setattr(cli, "_cooldown_state_path", lambda: cooldown_path)
    cli._write_cooldown_state(reason=cli.MANUAL_AUTH_REQUIRED_REASON)

    def _fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        cwd: Path,
        text: bool,
        timeout: int | None = None,
        env: dict[str, str] | None = None,
    ) -> cli.subprocess.CompletedProcess[str]:
        assert check is False
        assert capture_output is True
        assert cwd == cli.REPO_ROOT
        assert text is True
        assert timeout is not None
        assert "refresh_cookies.py" in " ".join(command)
        assert "--force" not in command
        return _completed(stdout='{"platform":"instagram","validated":true,"reason":null}\n')

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "_python_command", lambda: "/venv/bin/python")
    monkeypatch.setattr(cli.subprocess, "run", _fake_run)

    summary = cli.run_clear_auth_repair_cooldown()

    assert summary["ok"] is True
    assert summary["mode"] == "clear_auth_repair_cooldown"
    assert summary["cleared"] is True
    assert summary["remote_verify_reached"] is False
    assert summary["safe_advisory_state"]["active"] is False
    assert summary["safe_advisory_state"]["cleared"] is True
    assert [step["name"] for step in summary["steps"]] == ["validate_local", "clear_auth_repair_cooldown"]
    assert not cooldown_path.exists()


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
                "allow_cookie_refresh": False,
                "confirm_instagram_refresh": "",
                "clear_auth_repair_cooldown": False,
                "dry_run": False,
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
