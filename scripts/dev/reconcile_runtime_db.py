#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._db_url import resolve_direct_db_url  # noqa: E402
from trr_backend.db.preflight import (  # noqa: E402
    DatabasePreflightError,
    assert_core_schema_exists_sql,
    assert_migration_safe,
)

RUNBOOK_PATH = "TRR-Backend/docs/runbooks/supabase_migration_history_repair.md"
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"
ALLOWLIST_PATH = Path(__file__).with_name("runtime_reconcile_migration_allowlist.txt")
APP_ENV_PATH = REPO_ROOT.parent / "TRR-APP" / "apps" / "web" / ".env.local"
EXPECTED_PROJECT_REF = os.getenv("TRR_SUPABASE_PROJECT_REF", "vwxfvzutyufrkhfgoeaa")
EXPECTED_DIRECT_HOST = f"db.{EXPECTED_PROJECT_REF}.supabase.co"


def default_result() -> dict[str, Any]:
    return {
        "state": "ok",
        "reason": None,
        "applied_versions": [],
        "remediation": None,
        "pending_local": [],
        "remote_only": [],
        "local_versions": [],
        "remote_versions": [],
    }


def blocked_result(reason: str, remediation: str, **extra: Any) -> dict[str, Any]:
    payload = default_result()
    payload.update({"state": "blocked", "reason": reason, "remediation": remediation})
    payload.update(extra)
    return payload


def advisory_result(reason: str, remediation: str, **extra: Any) -> dict[str, Any]:
    payload = default_result()
    payload.update({"state": "advisory", "reason": reason, "remediation": remediation})
    payload.update(extra)
    return payload


def fixed_result(*, applied_versions: list[str], **extra: Any) -> dict[str, Any]:
    payload = default_result()
    payload.update({"state": "fixed", "applied_versions": applied_versions})
    payload.update(extra)
    return payload


def ok_result(**extra: Any) -> dict[str, Any]:
    payload = default_result()
    payload.update(extra)
    return payload


def read_allowlist(path: Path = ALLOWLIST_PATH) -> set[str]:
    if not path.is_file():
        return set()
    allowed: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if line:
            allowed.add(line)
    return allowed


def read_local_versions(migrations_dir: Path = MIGRATIONS_DIR) -> list[str]:
    versions: list[str] = []
    if not migrations_dir.is_dir():
        return versions
    for path in sorted(migrations_dir.glob("*.sql")):
        version = path.name.split("_", 1)[0].strip()
        if version:
            versions.append(version)
    return versions


def _read_env_file_value(path: Path, key: str) -> str | None:
    if not path.is_file():
        return None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        current_key, raw_value = line.split("=", 1)
        if current_key.strip() != key:
            continue
        value = raw_value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        return value or None
    return None


def ensure_runtime_db_env_loaded() -> None:
    if (os.getenv("TRR_DB_DIRECT_URL") or os.getenv("TRR_DB_SESSION_URL") or os.getenv("TRR_DB_URL") or "").strip():
        return
    loaded: dict[str, str] = {}
    for key in ("TRR_DB_DIRECT_URL", "TRR_DB_SESSION_URL", "TRR_DB_URL", "TRR_DB_FALLBACK_URL"):
        fallback = _read_env_file_value(APP_ENV_PATH, key)
        if fallback:
            os.environ[key] = fallback
            loaded[key] = fallback
    if (
        loaded.get("TRR_DB_DIRECT_URL")
        and not loaded.get("TRR_DB_SESSION_URL")
        and not loaded.get("TRR_DB_URL")
        and not loaded.get("TRR_DB_FALLBACK_URL")
    ):
        os.environ.setdefault("TRR_DB_URL", loaded["TRR_DB_DIRECT_URL"])


def resolve_direct_runtime_db_url() -> str:
    if not (os.getenv("TRR_DB_DIRECT_URL") or os.getenv("TRR_DB_SESSION_URL") or os.getenv("TRR_DB_URL") or "").strip():
        raise RuntimeError("missing_runtime_db_url")
    resolved = resolve_direct_db_url(
        allow_database_url=False,
        allow_deprecated_supabase_db_url=False,
        allow_local_supabase_status=False,
    )
    return resolved.value


def _runtime_session_escape_requested() -> bool:
    return (os.getenv("WORKSPACE_TRR_DB_LANE") or "").strip().lower() == "session"


def _session_runtime_db_url_candidate() -> str | None:
    for key in ("TRR_DB_SESSION_URL", "TRR_DB_URL", "TRR_DB_FALLBACK_URL"):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
        fallback = _read_env_file_value(APP_ENV_PATH, key)
        if fallback:
            return fallback
    return None


def resolve_runtime_db_url() -> str:
    if _runtime_session_escape_requested():
        session_url = _session_runtime_db_url_candidate()
        if not session_url:
            raise RuntimeError("missing_session_runtime_db_url")
        return session_url
    return resolve_direct_runtime_db_url()


def _direct_database_from_url(db_url: str) -> str:
    parsed = urlsplit(db_url)
    database = parsed.path.lstrip("/").strip()
    return database or "postgres"


def _sanitize_db_text(text: str, db_url: str) -> str:
    sanitized = text.replace(db_url, "[redacted-db-url]")
    parsed = urlsplit(db_url)
    if parsed.password:
        sanitized = sanitized.replace(parsed.password, "[redacted-password]")
    return sanitized


def read_direct_db_identity(db_url: str) -> dict[str, str]:
    parsed = urlsplit(db_url)
    host = (parsed.hostname or "").lower()
    database = _direct_database_from_url(db_url)

    if host != EXPECTED_DIRECT_HOST:
        raise RuntimeError("direct_db_identity_mismatch")
    if database != "postgres":
        raise RuntimeError("direct_db_database_mismatch")

    try:
        with psycopg2.connect(db_url, connect_timeout=8) as conn, conn.cursor() as cursor:
            cursor.execute("select version(), current_database(), current_user")
            row = cursor.fetchone()
    except psycopg2.Error as exc:
        raise RuntimeError("direct_db_unreachable") from exc

    if not row:
        raise RuntimeError("direct_db_identity_unavailable")

    server_version, current_database, current_user = (str(value or "") for value in row)
    if current_database != "postgres":
        raise RuntimeError("direct_db_database_mismatch")

    return {
        "project_ref": EXPECTED_PROJECT_REF,
        "host": EXPECTED_DIRECT_HOST,
        "database": current_database,
        "current_user": current_user,
        "server_version": server_version,
    }


def read_session_db_identity(db_url: str) -> dict[str, str]:
    parsed = urlsplit(db_url)
    host = (parsed.hostname or "").lower()
    database = _direct_database_from_url(db_url)
    username = parsed.username or ""
    try:
        port = parsed.port
    except ValueError:
        port = None

    if not host.endswith("pooler.supabase.com"):
        raise RuntimeError("session_db_identity_mismatch")
    if port != 5432:
        raise RuntimeError("session_db_port_mismatch")
    if username != f"postgres.{EXPECTED_PROJECT_REF}":
        raise RuntimeError("session_db_identity_mismatch")
    if database != "postgres":
        raise RuntimeError("session_db_database_mismatch")

    try:
        with psycopg2.connect(db_url, connect_timeout=8) as conn, conn.cursor() as cursor:
            cursor.execute("select version(), current_database(), current_user")
            row = cursor.fetchone()
    except psycopg2.Error as exc:
        raise RuntimeError("session_db_unreachable") from exc

    if not row:
        raise RuntimeError("session_db_identity_unavailable")

    server_version, current_database, current_user = (str(value or "") for value in row)
    if current_database != "postgres":
        raise RuntimeError("session_db_database_mismatch")

    return {
        "project_ref": EXPECTED_PROJECT_REF,
        "host": host,
        "database": current_database,
        "current_user": current_user,
        "server_version": server_version,
        "lane": "session",
    }


def read_runtime_db_identity(db_url: str) -> dict[str, str]:
    if _runtime_session_escape_requested():
        return read_session_db_identity(db_url)
    return read_direct_db_identity(db_url)


def _can_fallback_to_session_after_direct_failure(reason: str) -> bool:
    return reason == "direct_db_unreachable" and _session_runtime_db_url_candidate() is not None


def read_remote_versions(db_url: str) -> list[str]:
    with psycopg2.connect(db_url) as conn, conn.cursor() as cursor:
        assert_core_schema_exists_sql(conn)
        cursor.execute("select version::text from supabase_migrations.schema_migrations order by version")
        rows = cursor.fetchall()
    return [str(row[0]) for row in rows]


def is_contiguous_suffix(local_versions: list[str], pending_local: list[str]) -> bool:
    if not pending_local:
        return True
    return local_versions[-len(pending_local) :] == pending_local


def run_supabase_db_push(repo_root: Path, db_url: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "supabase",
            "db",
            "push",
            "--db-url",
            db_url,
            "--include-all",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )


def reconcile_runtime_db() -> dict[str, Any]:
    """Reconcile backend-owned shared-schema migrations for workspace startup."""
    local_versions = read_local_versions()
    allowlist = read_allowlist()
    ensure_runtime_db_env_loaded()
    if not (os.getenv("TRR_DB_DIRECT_URL") or os.getenv("TRR_DB_SESSION_URL") or os.getenv("TRR_DB_URL") or "").strip():
        return blocked_result(
            "missing_runtime_db_url",
            "Configure TRR_DB_DIRECT_URL, TRR_DB_SESSION_URL, or TRR_DB_URL in TRR-APP/apps/web/.env.local "
            "or export one before running make dev.",
            local_versions=local_versions,
        )
    try:
        assert_migration_safe(require_core_schema=True)
        db_url = resolve_runtime_db_url()
        session_fallback = False
        try:
            db_identity = read_runtime_db_identity(db_url)
        except RuntimeError as exc:
            reason = str(exc) or "direct_db_identity_failed"
            if not _runtime_session_escape_requested() and _can_fallback_to_session_after_direct_failure(reason):
                db_url = _session_runtime_db_url_candidate() or db_url
                db_identity = read_session_db_identity(db_url)
                session_fallback = True
            else:
                raise
        remote_versions = read_remote_versions(db_url)
    except DatabasePreflightError as exc:
        if "No database URL configured" in str(exc):
            return blocked_result(
                "missing_runtime_db_url",
                "Configure TRR_DB_DIRECT_URL, TRR_DB_SESSION_URL, or TRR_DB_URL in TRR-APP/apps/web/.env.local "
                "or export one before running make dev.",
                local_versions=local_versions,
            )
        return blocked_result(
            "missing_core_schema",
            str(exc),
            local_versions=local_versions,
        )
    except RuntimeError as exc:
        reason = str(exc) or "direct_db_identity_failed"
        remediation = (
            f"Validate TRR_DB_DIRECT_URL points at project {EXPECTED_PROJECT_REF}, "
            f"host {EXPECTED_DIRECT_HOST}, database postgres, then rerun preflight."
        )
        if reason == "missing_runtime_db_url":
            remediation = (
                "Configure TRR_DB_DIRECT_URL, TRR_DB_SESSION_URL, or TRR_DB_URL in "
                "TRR-APP/apps/web/.env.local or export one before running make dev."
            )
        elif reason == "missing_session_runtime_db_url":
            remediation = (
                "WORKSPACE_TRR_DB_LANE=session was requested, but no TRR_DB_SESSION_URL, "
                "TRR_DB_URL, or TRR_DB_FALLBACK_URL is configured."
            )
        elif reason.startswith("session_db_"):
            remediation = (
                f"Validate the session-pooler URL points at project {EXPECTED_PROJECT_REF}, "
                "uses pooler.supabase.com:5432, and database postgres, then rerun preflight."
            )
        return blocked_result(
            reason,
            remediation,
            local_versions=local_versions,
        )

    pending_local = [version for version in local_versions if version not in remote_versions]
    remote_only = [version for version in remote_versions if version not in local_versions]
    base = {
        "local_versions": local_versions,
        "remote_versions": remote_versions,
        "pending_local": pending_local,
        "remote_only": remote_only,
        "db_identity": db_identity,
    }
    if session_fallback:
        base["session_fallback_reason"] = "direct_db_unreachable"

    if remote_only:
        return blocked_result(
            "remote_only_history",
            f"Remote migration history differs from local files. Use {RUNBOOK_PATH}.",
            **base,
        )
    if not is_contiguous_suffix(local_versions, pending_local):
        return blocked_result(
            "pending_not_suffix",
            "Local pending migrations are not a contiguous suffix of backend-owned supabase/migrations; review before "
            "startup applies anything.",
            **base,
        )
    not_allowlisted = [version for version in pending_local if version not in allowlist]
    if not_allowlisted:
        return advisory_result(
            "pending_not_allowlisted",
            "Pending backend-owned shared-schema migrations are not allowlisted for startup auto-apply, "
            "so startup will not apply them automatically. The workspace can still start; review or apply "
            "the migrations separately when the task requires those schema changes.",
            not_allowlisted=not_allowlisted,
            **base,
        )
    max_auto_apply = int(os.getenv("WORKSPACE_RUNTIME_DB_MAX_AUTO_APPLY") or "3")
    if len(pending_local) > max_auto_apply:
        return blocked_result(
            "too_many_pending",
            f"Startup will not auto-apply more than {max_auto_apply} backend-owned shared-schema migrations.",
            **base,
        )
    if pending_local and _runtime_session_escape_requested():
        return advisory_result(
            "session_lane_auto_apply_skipped",
            "WORKSPACE_TRR_DB_LANE=session uses the session pooler for startup; pending migrations were not "
            "auto-applied. Use the direct DB lane or apply migrations separately when the task requires them.",
            **base,
        )
    if pending_local and session_fallback:
        return advisory_result(
            "direct_db_unreachable_session_fallback",
            "Direct DB is unreachable, so startup is using the session pooler fallback. Pending migrations were not "
            "auto-applied; use the direct DB lane or apply migrations separately when the task requires them.",
            **base,
        )
    if not pending_local or os.getenv("WORKSPACE_RUNTIME_DB_AUTO_APPLY_ENABLED", "1") != "1":
        if session_fallback:
            return advisory_result(
                "direct_db_unreachable_session_fallback",
                "Direct DB is unreachable, so startup is using the session pooler fallback. No pending local "
                "migrations were detected.",
                **base,
            )
        return ok_result(**base)

    completed = run_supabase_db_push(REPO_ROOT, db_url)
    if completed.returncode != 0:
        return blocked_result(
            "supabase_push_failed",
            _sanitize_db_text((completed.stderr or completed.stdout or "supabase db push failed").strip(), db_url),
            **base,
        )

    refreshed_remote = read_remote_versions(db_url)
    refreshed_pending = [version for version in local_versions if version not in refreshed_remote]
    refreshed_remote_only = [version for version in refreshed_remote if version not in local_versions]
    refreshed_base = {
        "local_versions": local_versions,
        "remote_versions": refreshed_remote,
        "pending_local": refreshed_pending,
        "remote_only": refreshed_remote_only,
        "db_identity": db_identity,
    }
    if refreshed_pending or refreshed_remote_only:
        return blocked_result(
            "supabase_push_failed",
            "supabase db push completed but runtime drift remains; inspect migration history before continuing.",
            **refreshed_base,
        )
    return fixed_result(applied_versions=pending_local, **refreshed_base)


def main() -> int:
    emit_json = "--json" in sys.argv[1:]
    result = reconcile_runtime_db()
    if emit_json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(json.dumps(result, sort_keys=True))
    return 1 if result["state"] == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
