from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from scripts.dev import reconcile_runtime_db as cli
from trr_backend.db.preflight import DatabasePreflightError

ORIGINAL_READ_DIRECT_DB_IDENTITY = cli.read_direct_db_identity


@pytest.fixture(autouse=True)
def _clear_runtime_db_env(monkeypatch: pytest.MonkeyPatch):
    runtime_envs = (
        "TRR_DB_DIRECT_URL",
        "TRR_DB_SESSION_URL",
        "TRR_DB_TRANSACTION_URL",
        "TRR_DB_URL",
        "TRR_DB_FALLBACK_URL",
        "TRR_DB_RUNTIME_LANE",
        "TRR_DB_TRANSACTION_FLIGHT_TEST",
    )
    for name in runtime_envs:
        os.environ.pop(name, None)
    monkeypatch.setattr(
        cli,
        "read_direct_db_identity",
        lambda _db_url: {
            "project_ref": cli.EXPECTED_PROJECT_REF,
            "host": cli.EXPECTED_DIRECT_HOST,
            "database": "postgres",
            "current_user": "postgres",
            "server_version": "PostgreSQL 17",
        },
    )
    yield
    for name in runtime_envs:
        os.environ.pop(name, None)


def _read_runtime_reconcile_decisions(path: Path) -> tuple[set[str], set[str], str | None]:
    auto_apply: set[str] = set()
    manual_only: set[str] = set()
    decision_required_after: str | None = None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            comment = line[1:].strip()
            lower_comment = comment.lower()
            if lower_comment.startswith("manual:"):
                version = comment.split(":", 1)[1].strip().split(maxsplit=1)[0]
                if version:
                    manual_only.add(version)
            elif lower_comment.startswith("decision-required-after:"):
                version = comment.split(":", 1)[1].strip().split(maxsplit=1)[0]
                if version:
                    decision_required_after = version
            continue
        auto_apply.add(line.split(maxsplit=1)[0])

    return auto_apply, manual_only, decision_required_after


def test_reconcile_runtime_db_blocks_without_runtime_db_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRR_DB_DIRECT_URL", raising=False)
    monkeypatch.delenv("TRR_DB_SESSION_URL", raising=False)
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


def test_ensure_runtime_db_env_loaded_prefers_direct_app_env_file(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    app_env = tmp_path / ".env.local"
    app_env.write_text(
        "TRR_DB_DIRECT_URL=postgresql://direct-from-app\nTRR_DB_SESSION_URL=postgresql://session-from-app\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("TRR_DB_DIRECT_URL", raising=False)
    monkeypatch.delenv("TRR_DB_SESSION_URL", raising=False)
    monkeypatch.delenv("TRR_DB_URL", raising=False)
    monkeypatch.setattr(cli, "APP_ENV_PATH", app_env)

    cli.ensure_runtime_db_env_loaded()

    assert cli.os.environ["TRR_DB_DIRECT_URL"] == "postgresql://direct-from-app"


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
        lambda _repo_root, _db_url: subprocess.CompletedProcess(["supabase"], 0, stdout="ok", stderr=""),
    )

    result = cli.reconcile_runtime_db()

    assert result["state"] == "fixed"
    assert result["applied_versions"] == ["20260422094500", "20260422111500"]


def test_direct_db_identity_rejects_wrong_project_host() -> None:
    with pytest.raises(RuntimeError, match="direct_db_identity_mismatch"):
        ORIGINAL_READ_DIRECT_DB_IDENTITY("postgresql://postgres:secret@db.wrong-ref.supabase.co:5432/postgres")


def test_reconcile_runtime_db_sanitizes_failed_supabase_output(monkeypatch: pytest.MonkeyPatch) -> None:
    db_url = "postgresql://postgres:secret@db.abcdefghijklmnopqrst.supabase.co:5432/postgres"
    monkeypatch.setattr(cli, "EXPECTED_PROJECT_REF", "abcdefghijklmnopqrst")
    monkeypatch.setattr(cli, "EXPECTED_DIRECT_HOST", "db.abcdefghijklmnopqrst.supabase.co")
    monkeypatch.setenv("TRR_DB_DIRECT_URL", db_url)
    monkeypatch.setattr(cli, "assert_migration_safe", lambda require_core_schema=True: None)
    monkeypatch.setattr(cli, "resolve_direct_runtime_db_url", lambda: db_url)
    monkeypatch.setattr(cli, "read_local_versions", lambda: ["20260422094500"])
    calls = {"count": 0}

    def _read_remote_versions(_db_url: str) -> list[str]:
        calls["count"] += 1
        return [] if calls["count"] == 1 else []

    monkeypatch.setattr(cli, "read_remote_versions", _read_remote_versions)
    monkeypatch.setattr(cli, "read_allowlist", lambda path=cli.ALLOWLIST_PATH: {"20260422094500"})
    monkeypatch.setattr(
        cli,
        "run_supabase_db_push",
        lambda _repo_root, _db_url: subprocess.CompletedProcess(
            ["supabase"],
            1,
            stdout="",
            stderr=f"failed for {db_url}",
        ),
    )

    result = cli.reconcile_runtime_db()

    assert result["state"] == "blocked"
    assert result["reason"] == "supabase_push_failed"
    assert db_url not in result["remediation"]
    assert "secret" not in result["remediation"]


def test_runtime_reconcile_tail_migrations_have_explicit_startup_decisions() -> None:
    auto_apply, manual_only, decision_required_after = _read_runtime_reconcile_decisions(cli.ALLOWLIST_PATH)

    assert decision_required_after, (
        f"{cli.ALLOWLIST_PATH.name} must declare '# decision-required-after: <version>' so tail migrations "
        "cannot skip an explicit startup decision."
    )

    decided_versions = auto_apply | manual_only
    tail_versions = [
        version for version in cli.read_local_versions() if version > decision_required_after
    ]
    undecided_tail = [version for version in tail_versions if version not in decided_versions]

    assert not undecided_tail, (
        "Tail migrations newer than the runtime-reconcile decision baseline must be explicitly marked "
        f"for startup auto-apply or manual-only review in {cli.ALLOWLIST_PATH.name}: {undecided_tail}"
    )
