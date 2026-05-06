from __future__ import annotations

import json
import subprocess

import pytest

from scripts.modal import reconcile_modal_runtime as cli


def test_reconcile_modal_runtime_skips_when_modal_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKSPACE_TRR_MODAL_ENABLED", "0")

    result = cli.reconcile_modal_runtime()

    assert result["state"] == "skipped"
    assert result["reason"] == "modal_disabled"


def test_verify_readiness_checks_modal_auth_and_getty_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cli.verify_modal_readiness, "expected_function_names", lambda: ("serve_backend_api",))

    def _fake_verify_modal_readiness(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(cli.verify_modal_readiness, "verify_modal_readiness", _fake_verify_modal_readiness)

    result = cli.verify_readiness()

    assert result == {"ok": True}
    assert captured["probe_remote_auth_platform"] == "instagram"
    assert captured["probe_getty_remote_access"] is True


def test_reconcile_modal_runtime_is_ok_when_readiness_passes_and_fingerprint_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKSPACE_TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("WORKSPACE_TRR_REMOTE_EXECUTOR", "modal")
    monkeypatch.setattr(
        cli,
        "verify_readiness",
        lambda repo_root=cli.REPO_ROOT: {
            "ok": True,
            "core_ok": True,
            "getty_remote_probe": {"platform": "getty", "ready": False, "reason": "challenge_page"},
            "advisory_probe_failures": ["challenge_page"],
        },
    )
    monkeypatch.setattr(cli, "build_modal_fingerprint", lambda repo_root=cli.REPO_ROOT: "abc")
    monkeypatch.setattr(cli, "load_saved_fingerprint", lambda repo_root=cli.REPO_ROOT: "abc")

    result = cli.reconcile_modal_runtime()

    assert result["state"] == "ok"
    assert result["deployed"] is False
    assert result["fingerprint_changed"] is False
    assert result["readiness"]["advisory_probe_failures"] == ["challenge_page"]


def test_reconcile_modal_runtime_deploys_when_readiness_missing_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKSPACE_TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("WORKSPACE_TRR_REMOTE_EXECUTOR", "modal")
    states = iter([{"ok": False, "missing_functions": ["run_social_job"]}, {"ok": True}])
    monkeypatch.setattr(cli, "verify_readiness", lambda repo_root=cli.REPO_ROOT: next(states))
    monkeypatch.setattr(cli, "build_modal_fingerprint", lambda repo_root=cli.REPO_ROOT: "new")
    monkeypatch.setattr(cli, "load_saved_fingerprint", lambda repo_root=cli.REPO_ROOT: "old")
    monkeypatch.setattr(
        cli,
        "apply_named_secrets",
        lambda repo_root=cli.REPO_ROOT: subprocess.CompletedProcess(["python"], 0, stdout="ok", stderr=""),
    )
    monkeypatch.setattr(
        cli,
        "deploy_modal_app",
        lambda repo_root=cli.REPO_ROOT: subprocess.CompletedProcess(["python"], 0, stdout="ok", stderr=""),
    )
    saved: dict[str, str] = {}
    monkeypatch.setattr(
        cli,
        "save_fingerprint",
        lambda fingerprint, repo_root=cli.REPO_ROOT: saved.update({"value": fingerprint}),
    )

    result = cli.reconcile_modal_runtime()

    assert result["state"] == "fixed"
    assert result["deployed"] is True
    assert result["fingerprint_changed"] is True
    assert saved["value"] == "new"


def test_reconcile_modal_runtime_deploys_when_fingerprint_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKSPACE_TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("WORKSPACE_TRR_REMOTE_EXECUTOR", "modal")
    states = iter([{"ok": True}, {"ok": True}])
    monkeypatch.setattr(cli, "verify_readiness", lambda repo_root=cli.REPO_ROOT: next(states))
    monkeypatch.setattr(cli, "build_modal_fingerprint", lambda repo_root=cli.REPO_ROOT: "new")
    monkeypatch.setattr(cli, "load_saved_fingerprint", lambda repo_root=cli.REPO_ROOT: "old")
    monkeypatch.setattr(
        cli,
        "apply_named_secrets",
        lambda repo_root=cli.REPO_ROOT: subprocess.CompletedProcess(["python"], 0, stdout="ok", stderr=""),
    )
    monkeypatch.setattr(
        cli,
        "deploy_modal_app",
        lambda repo_root=cli.REPO_ROOT: subprocess.CompletedProcess(["python"], 0, stdout="ok", stderr=""),
    )
    monkeypatch.setattr(cli, "save_fingerprint", lambda fingerprint, repo_root=cli.REPO_ROOT: None)

    result = cli.reconcile_modal_runtime()

    assert result["state"] == "fixed"
    assert result["fingerprint_changed"] is True


def test_reconcile_modal_runtime_blocks_when_secret_apply_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKSPACE_TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("WORKSPACE_TRR_REMOTE_EXECUTOR", "modal")
    monkeypatch.setattr(cli, "verify_readiness", lambda repo_root=cli.REPO_ROOT: {"ok": False})
    monkeypatch.setattr(cli, "build_modal_fingerprint", lambda repo_root=cli.REPO_ROOT: "new")
    monkeypatch.setattr(cli, "load_saved_fingerprint", lambda repo_root=cli.REPO_ROOT: "old")
    monkeypatch.setattr(
        cli,
        "apply_named_secrets",
        lambda repo_root=cli.REPO_ROOT: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(cmd=["python"], timeout=300)
        ),
    )

    result = cli.reconcile_modal_runtime()

    assert result["state"] == "blocked"
    assert result["reason"] == "modal_secret_apply_failed"


def test_reconcile_modal_runtime_blocks_when_verify_still_fails_after_deploy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKSPACE_TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("WORKSPACE_TRR_REMOTE_EXECUTOR", "modal")
    states = iter([{"ok": False}, {"ok": False}])
    monkeypatch.setattr(cli, "verify_readiness", lambda repo_root=cli.REPO_ROOT: next(states))
    monkeypatch.setattr(cli, "post_deploy_verify_attempts", lambda: 1)
    monkeypatch.setattr(cli, "build_modal_fingerprint", lambda repo_root=cli.REPO_ROOT: "new")
    monkeypatch.setattr(cli, "load_saved_fingerprint", lambda repo_root=cli.REPO_ROOT: "old")
    monkeypatch.setattr(
        cli,
        "apply_named_secrets",
        lambda repo_root=cli.REPO_ROOT: subprocess.CompletedProcess(["python"], 0, stdout="ok", stderr=""),
    )
    monkeypatch.setattr(
        cli,
        "deploy_modal_app",
        lambda repo_root=cli.REPO_ROOT: subprocess.CompletedProcess(["python"], 0, stdout="ok", stderr=""),
    )

    result = cli.reconcile_modal_runtime()

    assert result["state"] == "blocked"
    assert result["reason"] == "modal_verify_failed"


def test_reconcile_modal_runtime_retries_post_deploy_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKSPACE_TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("WORKSPACE_TRR_REMOTE_EXECUTOR", "modal")
    states = iter(
        [
            {"ok": False, "blocking_probe_failures": ["cookie_schema_invalid"]},
            {"ok": False, "blocking_probe_failures": ["cookie_schema_invalid"]},
            {"ok": True, "blocking_probe_failures": []},
        ]
    )
    monkeypatch.setattr(cli, "verify_readiness", lambda repo_root=cli.REPO_ROOT: next(states))
    monkeypatch.setattr(cli, "post_deploy_verify_delay_seconds", lambda: 0.0)
    monkeypatch.setattr(cli, "build_modal_fingerprint", lambda repo_root=cli.REPO_ROOT: "new")
    monkeypatch.setattr(cli, "load_saved_fingerprint", lambda repo_root=cli.REPO_ROOT: "old")
    monkeypatch.setattr(
        cli,
        "apply_named_secrets",
        lambda repo_root=cli.REPO_ROOT: subprocess.CompletedProcess(["python"], 0, stdout="ok", stderr=""),
    )
    monkeypatch.setattr(
        cli,
        "deploy_modal_app",
        lambda repo_root=cli.REPO_ROOT: subprocess.CompletedProcess(["python"], 0, stdout="ok", stderr=""),
    )
    saved: dict[str, str] = {}
    monkeypatch.setattr(
        cli,
        "save_fingerprint",
        lambda fingerprint, repo_root=cli.REPO_ROOT: saved.update({"value": fingerprint}),
    )

    result = cli.reconcile_modal_runtime()

    assert result["state"] == "fixed"
    assert result["readiness"]["ok"] is True
    assert saved["value"] == "new"


def test_save_and_load_fingerprint_round_trip(tmp_path) -> None:
    repo_root = tmp_path / "TRR-Backend"
    repo_root.mkdir()

    cli.save_fingerprint("abc123", repo_root)

    loaded = cli.load_saved_fingerprint(repo_root)

    assert loaded == "abc123"
    saved = json.loads((repo_root.parent / ".logs" / "workspace" / "modal-runtime-fingerprint.json").read_text())
    assert saved["fingerprint"] == "abc123"


def test_modal_fingerprint_changes_when_instagram_comments_fetcher_changes(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "TRR-Backend"
    comments_dir = repo_root / "trr_backend" / "socials" / "instagram" / "comments_scrapling"
    comments_dir.mkdir(parents=True)
    (repo_root / "trr_backend").mkdir(exist_ok=True)
    for relative_path in (
        "trr_backend/modal_jobs.py",
        "trr_backend/modal_dispatch.py",
        "trr_backend/socials/social_season_analytics_impl.py",
        "trr_backend/socials/instagram/comments_scrapling/fetcher.py",
        "trr_backend/socials/instagram/comments_scrapling/job_runner.py",
        "trr_backend/socials/instagram/comments_scrapling/persistence.py",
        "requirements.txt",
        "requirements.lock.txt",
    ):
        path = repo_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{relative_path}: v1\n", encoding="utf-8")

    monkeypatch.setattr(cli.prepare_named_secrets, "_load_source_env", lambda _path: {})
    monkeypatch.setattr(cli.prepare_named_secrets, "_split_env", lambda _env: ({}, {}))
    monkeypatch.setattr(
        cli.prepare_named_secrets,
        "_apply_runtime_overrides",
        lambda values, *, disabled=False: values,
    )

    first = cli.build_modal_fingerprint(repo_root)
    (comments_dir / "fetcher.py").write_text("fetcher: v2\n", encoding="utf-8")
    second = cli.build_modal_fingerprint(repo_root)

    assert first != second
