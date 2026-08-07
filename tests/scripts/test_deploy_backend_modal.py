from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import contextmanager
from typing import Any

import pytest

from scripts.modal import api_canary
from scripts.modal import deploy_backend as cli


def test_pinned_modal_env_forces_admin_profile() -> None:
    env = cli.pinned_modal_env(
        {
            "MODAL_PROFILE": "thb-bbl",
            "TRR_MODAL_APP_NAME": "other-app",
            "TRR_MODAL_INSTAGRAM_PAYLOAD_READ_MODE": "compare",
            "TRR_MODAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE": "0.1",
            "OTHER": "1",
        }
    )

    assert env["MODAL_PROFILE"] == "admin-56995"
    assert env["MODAL_WORKSPACE"] == "admin-56995"
    assert env["MODAL_ENVIRONMENT"] == "main"
    assert env["TRR_MODAL_APP_NAME"] == "trr-backend-jobs"
    assert env["TRR_MODAL_APP_REF"] == "trr_backend.modal_jobs"
    assert env["TRR_MODAL_INSTAGRAM_PAYLOAD_READ_MODE"] == "compare"
    assert env["TRR_MODAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE"] == "0.1"
    assert env["OTHER"] == "1"


def test_pinned_modal_env_hydrates_only_missing_ownership_settings(
    tmp_path,
) -> None:
    source_env = tmp_path / "modal.env"
    source_env.write_text(
        "TRR_MODAL_MAINTENANCE_OWNER_REQUIRED=1\n"
        "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED=0\n"
        "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED=1\n"
        "DATABASE_URL=must-not-leak\n",
        encoding="utf-8",
    )

    env = cli.pinned_modal_env(
        {
            "TRR_MODAL_SOURCE_ENV": str(source_env),
            "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED": "0",
        }
    )

    assert env["TRR_MODAL_MAINTENANCE_OWNER_REQUIRED"] == "1"
    assert env["TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED"] == "0"
    assert env["TRR_MODAL_RUNTIME_SCHEDULER_ENABLED"] == "0"
    assert "DATABASE_URL" not in env


def test_pinned_modal_env_respects_an_explicit_empty_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", "1")

    env = cli.pinned_modal_env({"TRR_MODAL_SOURCE_ENV": str(tmp_path / "missing.env")})

    assert "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED" not in env


def test_pinned_modal_env_resolves_relative_source_from_repo_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    source_dir = cli.REPO_ROOT / f".tmp-test-modal-env-{tmp_path.name}"
    source_dir.mkdir()
    source_env = source_dir / "modal.env"
    source_env.write_text(
        "TRR_MODAL_MAINTENANCE_OWNER_REQUIRED=1\n"
        "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED=0\n"
        "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED=1\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    try:
        env = cli.pinned_modal_env({"TRR_MODAL_SOURCE_ENV": str(source_env.relative_to(cli.REPO_ROOT))})
    finally:
        source_env.unlink(missing_ok=True)
        source_dir.rmdir()

    assert env["TRR_MODAL_MAINTENANCE_OWNER_REQUIRED"] == "1"
    assert env["TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED"] == "0"
    assert env["TRR_MODAL_RUNTIME_SCHEDULER_ENABLED"] == "1"


def test_readiness_import_hydrates_ownership_before_modal_jobs_import(tmp_path) -> None:
    source_env = tmp_path / "modal.env"
    source_env.write_text(
        "TRR_MODAL_MAINTENANCE_OWNER_REQUIRED=1\n"
        "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED=0\n"
        "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED=1\n",
        encoding="utf-8",
    )
    env = {key: value for key, value in os.environ.items() if key not in cli.MODAL_OWNERSHIP_ENV_KEYS}
    env["TRR_MODAL_SOURCE_ENV"] = str(source_env)

    completed = subprocess.run(
        [sys.executable, "-c", "import scripts.modal.verify_modal_readiness"],
        cwd=cli.REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


def test_verify_required_workspace_accepts_admin_workspace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli,
        "modal_profile_rows",
        lambda *, env=None: [{"name": "admin-56995", "workspace": "admin-56995", "active": True}],
    )

    context = cli.verify_required_workspace(env={})

    assert context["active_profile"] == "admin-56995"
    assert context["active_workspace"] == "admin-56995"


def test_verify_required_workspace_blocks_wrong_workspace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli,
        "modal_profile_rows",
        lambda *, env=None: [{"name": "thb-bbl", "workspace": "tommy-hulihan-basketball", "active": True}],
    )

    with pytest.raises(RuntimeError, match="Modal deploy blocked"):
        cli.verify_required_workspace(env={})


def test_modal_profile_rows_uses_pinned_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run(command, **kwargs):
        calls.append({"command": command, **kwargs})
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps([{"name": "admin-56995", "workspace": "admin-56995", "active": True}]),
            stderr="",
        )

    monkeypatch.setattr(cli.subprocess, "run", fake_run)
    monkeypatch.setattr(cli, "python_command", lambda: "python")

    rows = cli.modal_profile_rows(env={"MODAL_PROFILE": "thb-bbl"})

    assert rows == [{"name": "admin-56995", "workspace": "admin-56995", "active": True}]
    assert calls[0]["command"] == ["python", "-m", "modal", "profile", "list", "--json"]
    assert calls[0]["env"]["MODAL_PROFILE"] == "admin-56995"


def test_build_deploy_command_defaults_to_modal_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "python_command", lambda: "python")
    args = cli.parse_args([])

    assert cli.build_deploy_command(args) == [
        "python",
        "-m",
        "modal",
        "deploy",
        "-m",
        "trr_backend.modal_jobs",
        "--env",
        "main",
    ]


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--app-ref", "other.module"], "app_ref"),
        (["--app-name", "other-app"], "app_name"),
        (["--env", "staging"], "environment"),
        (["--name", "other-app"], "deployment_name"),
    ],
)
def test_modal_mutation_target_cannot_be_overridden(args: list[str], message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        cli.validate_target_identity(cli.parse_args(args))


def test_dry_run_asserts_full_modal_identity(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "verify_required_workspace",
        lambda *, env=None: {
            "active_profile": "admin-56995",
            "active_workspace": "admin-56995",
        },
    )
    monkeypatch.setattr(cli, "python_command", lambda: "python")

    assert cli.main(["--dry-run"]) == 0

    output = capsys.readouterr().out
    assert "profile=admin-56995" in output
    assert "workspace=admin-56995" in output
    assert "environment=main" in output
    assert "app_name=trr-backend-jobs" in output
    assert "app_ref=trr_backend.modal_jobs" in output


def test_build_readiness_command_passes_modal_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "python_command", lambda: "python")
    args = cli.parse_args(["--env", "main"])

    assert cli.build_readiness_command(args) == [
        "python",
        str(cli.REPO_ROOT / "scripts" / "modal" / "verify_modal_readiness.py"),
        "--json",
        "--env",
        "main",
    ]


def test_verify_deployed_readiness_blocks_failed_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(command, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps({"ok": False, "blocking_probe_failures": ["modal_workspace_mismatch"]}),
            stderr="",
        )

    monkeypatch.setattr(cli.subprocess, "run", fake_run)
    monkeypatch.setattr(cli, "python_command", lambda: "python")

    with pytest.raises(RuntimeError, match="Modal readiness failed after deploy"):
        cli.verify_deployed_readiness(cli.parse_args([]), env={})


def test_health_url_targets_health_endpoint() -> None:
    assert api_canary.health_url("https://admin-56995--trr-backend-api.modal.run/") == (
        "https://admin-56995--trr-backend-api.modal.run/health"
    )


def test_health_url_rejects_missing_scheme() -> None:
    with pytest.raises(RuntimeError, match="Invalid URL scheme"):
        api_canary.health_url("admin-56995--trr-backend-api.modal.run")


def test_run_api_cold_start_canary_returns_success(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        status = 200

        def getcode(self):
            return 200

        def read(self, _limit):
            return b'{"status":"healthy"}'

    @contextmanager
    def fake_urlopen(request, timeout):
        assert request.full_url == "https://admin-56995--trr-backend-api.modal.run/health"
        assert timeout == 5
        yield FakeResponse()

    monkeypatch.setattr(api_canary.urllib.request, "urlopen", fake_urlopen)

    summary = api_canary.run_api_cold_start_canary(
        "https://admin-56995--trr-backend-api.modal.run",
        timeout_seconds=5,
    )

    assert summary["ok"] is True
    assert summary["status"] == 200
    assert summary["attempt"] == 1


def test_format_deploy_history_stamp_includes_recent_versions() -> None:
    stamp = cli.format_deploy_history_stamp(
        history_rows=[
            {
                "Version": "v441",
                "Time deployed": "2026-05-28 11:46:53-04:00",
                "Deployed by": "admin-56995",
                "Commit": "c150a64*",
                "Client": "1.4.0",
            }
        ],
        canary={"url": "https://admin-56995--trr-backend-api.modal.run/health", "status": 200, "attempt": 1},
        workspace_context={"active_workspace": "admin-56995", "active_profile": "admin-56995"},
    )

    assert cli.HISTORY_STAMP_START in stamp
    assert "| v441 | 2026-05-28 11:46:53-04:00 | admin-56995 | c150a64* | 1.4.0 |" in stamp
    assert "HTTP `200`" in stamp


def test_format_deploy_history_stamp_accepts_lowercase_modal_json() -> None:
    stamp = cli.format_deploy_history_stamp(
        history_rows=[
            {
                "version": "v28",
                "time_deployed": "2026-07-22",
                "deployed_by": "admin-56995",
                "commit": "0a3d2b5",
                "client": "1.5.3",
            }
        ],
        canary={"url": "https://example.test", "status": 200, "attempt": 1},
        workspace_context={"active_workspace": "admin-56995", "active_profile": "admin-56995"},
    )
    assert "| v28 | 2026-07-22 | admin-56995 | 0a3d2b5 | 1.5.3 |" in stamp


def test_stamp_incident_note_replaces_existing_stamp(tmp_path) -> None:
    note_path = tmp_path / "incident.md"
    note_path.write_text(f"# Incident\n\n{cli.HISTORY_STAMP_START}\nold\n{cli.HISTORY_STAMP_END}\n\n## Tail\n")

    stamped = cli.stamp_incident_note(
        note_path=note_path,
        history_rows=[
            {
                "Version": "v442",
                "Time deployed": "2026-05-28 12:05:00-04:00",
                "Deployed by": "admin-56995",
                "Commit": "abc123*",
                "Client": "1.4.0",
            }
        ],
        canary={"url": "https://admin-56995--trr-backend-api.modal.run/health", "status": 200, "attempt": 1},
        workspace_context={"active_workspace": "admin-56995", "active_profile": "admin-56995"},
    )

    updated = note_path.read_text()
    assert stamped is True
    assert "old" not in updated
    assert "| v442 | 2026-05-28 12:05:00-04:00 | admin-56995 | abc123* | 1.4.0 |" in updated
    assert "## Tail" in updated


def test_resolve_incident_note_path_accepts_named_note() -> None:
    assert cli.resolve_incident_note_path(
        incident_note="ignored.md",
        incident_note_name="modal-v439-v440-serve-backend-api-crash-loop-2026-05-28",
    ) == (cli.INCIDENT_NOTES_DIR / "modal-v439-v440-serve-backend-api-crash-loop-2026-05-28.md")


def test_resolve_incident_note_path_accepts_named_note_with_extension() -> None:
    assert (
        cli.resolve_incident_note_path(
            incident_note="ignored.md",
            incident_note_name="custom-note.md",
        )
        == cli.INCIDENT_NOTES_DIR / "custom-note.md"
    )
