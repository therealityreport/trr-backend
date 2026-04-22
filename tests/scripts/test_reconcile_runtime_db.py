from __future__ import annotations

import subprocess

import pytest

from scripts.dev import reconcile_runtime_db as cli
from trr_backend.db.preflight import DatabasePreflightError


def test_reconcile_runtime_db_blocks_without_runtime_db_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRR_DB_URL", raising=False)
    monkeypatch.setattr(cli, "read_local_versions", lambda: ["20260422094500"])
    monkeypatch.setattr(cli, "APP_ENV_PATH", cli.Path("/tmp/does-not-exist.env"))

    result = cli.reconcile_runtime_db()

    assert result["state"] == "blocked"
    assert result["reason"] == "missing_runtime_db_url"


def test_ensure_runtime_db_env_loaded_reads_app_env_file(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    app_env = tmp_path / ".env.local"
    app_env.write_text("TRR_DB_URL=postgresql://runtime-from-app\n", encoding="utf-8")
    monkeypatch.delenv("TRR_DB_URL", raising=False)
    monkeypatch.setattr(cli, "APP_ENV_PATH", app_env)

    cli.ensure_runtime_db_env_loaded()

    assert cli.os.environ["TRR_DB_URL"] == "postgresql://runtime-from-app"


def test_reconcile_runtime_db_blocks_when_core_schema_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_DB_URL", "postgresql://runtime")
    monkeypatch.setattr(cli, "read_local_versions", lambda: ["20260422094500"])
    monkeypatch.setattr(cli, "assert_migration_safe", lambda require_core_schema=True: None)
    monkeypatch.setattr(cli, "resolve_direct_runtime_db_url", lambda: "postgresql://direct")
    monkeypatch.setattr(cli, "read_remote_versions", lambda _db_url: (_ for _ in ()).throw(DatabasePreflightError("missing core")))

    result = cli.reconcile_runtime_db()

    assert result["state"] == "blocked"
    assert result["reason"] == "missing_core_schema"


def test_reconcile_runtime_db_blocks_on_remote_only_history(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_DB_URL", "postgresql://runtime")
    monkeypatch.setattr(cli, "assert_migration_safe", lambda require_core_schema=True: None)
    monkeypatch.setattr(cli, "resolve_direct_runtime_db_url", lambda: "postgresql://direct")
    monkeypatch.setattr(cli, "read_local_versions", lambda: ["20260422094500"])
    monkeypatch.setattr(cli, "read_remote_versions", lambda _db_url: ["20260422094500", "20260422111500"])

    result = cli.reconcile_runtime_db()

    assert result["state"] == "blocked"
    assert result["reason"] == "remote_only_history"


def test_reconcile_runtime_db_blocks_when_pending_not_allowlisted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_DB_URL", "postgresql://runtime")
    monkeypatch.setattr(cli, "assert_migration_safe", lambda require_core_schema=True: None)
    monkeypatch.setattr(cli, "resolve_direct_runtime_db_url", lambda: "postgresql://direct")
    monkeypatch.setattr(cli, "read_local_versions", lambda: ["20260422094500", "20260422111500"])
    monkeypatch.setattr(cli, "read_remote_versions", lambda _db_url: ["20260422094500"])
    monkeypatch.setattr(cli, "read_allowlist", lambda path=cli.ALLOWLIST_PATH: {"20260422094500"})

    result = cli.reconcile_runtime_db()

    assert result["state"] == "blocked"
    assert result["reason"] == "pending_not_allowlisted"


def test_reconcile_runtime_db_blocks_when_pending_not_suffix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_DB_URL", "postgresql://runtime")
    monkeypatch.setattr(cli, "assert_migration_safe", lambda require_core_schema=True: None)
    monkeypatch.setattr(cli, "resolve_direct_runtime_db_url", lambda: "postgresql://direct")
    monkeypatch.setattr(cli, "read_local_versions", lambda: ["20260422094500", "20260422111500", "20260422113000"])
    monkeypatch.setattr(cli, "read_remote_versions", lambda _db_url: ["20260422111500"])
    monkeypatch.setattr(
        cli,
        "read_allowlist",
        lambda path=cli.ALLOWLIST_PATH: {"20260422094500", "20260422113000"},
    )

    result = cli.reconcile_runtime_db()

    assert result["state"] == "blocked"
    assert result["reason"] == "pending_not_suffix"


def test_reconcile_runtime_db_blocks_when_pending_exceeds_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_DB_URL", "postgresql://runtime")
    monkeypatch.setenv("WORKSPACE_RUNTIME_DB_MAX_AUTO_APPLY", "1")
    monkeypatch.setattr(cli, "assert_migration_safe", lambda require_core_schema=True: None)
    monkeypatch.setattr(cli, "resolve_direct_runtime_db_url", lambda: "postgresql://direct")
    monkeypatch.setattr(cli, "read_local_versions", lambda: ["20260422094500", "20260422111500"])
    monkeypatch.setattr(cli, "read_remote_versions", lambda _db_url: [])
    monkeypatch.setattr(
        cli,
        "read_allowlist",
        lambda path=cli.ALLOWLIST_PATH: {"20260422094500", "20260422111500"},
    )

    result = cli.reconcile_runtime_db()

    assert result["state"] == "blocked"
    assert result["reason"] == "too_many_pending"


def test_reconcile_runtime_db_auto_applies_safe_allowlisted_suffix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_DB_URL", "postgresql://runtime")
    monkeypatch.setattr(cli, "assert_migration_safe", lambda require_core_schema=True: None)
    monkeypatch.setattr(cli, "resolve_direct_runtime_db_url", lambda: "postgresql://direct")
    monkeypatch.setattr(cli, "read_local_versions", lambda: ["20260422094500", "20260422111500"])
    calls = {"count": 0}

    def _read_remote_versions(_db_url: str) -> list[str]:
        calls["count"] += 1
        if calls["count"] == 1:
            return []
        return ["20260422094500", "20260422111500"]

    monkeypatch.setattr(cli, "read_remote_versions", _read_remote_versions)
    monkeypatch.setattr(
        cli,
        "read_allowlist",
        lambda path=cli.ALLOWLIST_PATH: {"20260422094500", "20260422111500"},
    )
    monkeypatch.setattr(
        cli,
        "run_supabase_db_push",
        lambda _repo_root: subprocess.CompletedProcess(["supabase"], 0, stdout="ok", stderr=""),
    )

    result = cli.reconcile_runtime_db()

    assert result["state"] == "fixed"
    assert result["applied_versions"] == ["20260422094500", "20260422111500"]
