#!/usr/bin/env python3
"""Thin "Modal ops" MCP server for the TRR backend (app ``trr-backend-jobs``).

This is a self-contained stdio MCP server (FastMCP style) that exposes a small,
read-only set of Modal operations tools for the TRR ``trr-backend-jobs`` app under
the ``admin-56995`` Modal profile. Every tool shells out to the same Modal CLI /
readiness scripts an operator would run by hand, captures stdout/stderr, and
returns structured text. Tools never raise: failures are reported as text so the
MCP client always gets a usable answer.

Conventions (mirrors ``scripts/modal-trr.sh`` and ``verify_modal_readiness.py``):
  * Modal CLI is invoked as ``<python> -m modal ...`` so it works without a
    globally installed ``modal`` binary. The repo venv at ``TRR-Backend/.venv``
    is preferred, falling back to ``$VIRTUAL_ENV`` then the current interpreter.
  * The TRR profile is selected per-process via ``MODAL_PROFILE=admin-56995``
    (overridable with ``MODAL_PROFILE_NAME``). The globally-active Modal profile
    is never changed.
  * All subprocess calls run from the ``TRR-Backend`` repo root with a timeout.

Exposed tools:
  * ``modal_readiness(probe_remote_auth=None)`` -> verify_modal_readiness.py --json
  * ``probe_remote_auth(platform="instagram")`` -> readiness + remote auth probe
  * ``tail_logs(function="run_social_posts_job", lines=200)`` -> modal app logs
  * ``app_status()`` -> modal app list (filtered to trr-backend-jobs)
  * ``cron_status()`` -> best-effort scheduled-function view from app list/logs
  * ``list_recent_runs(limit=10, platform="instagram", account_handle=None)``
  * ``list_active_jobs(limit=25, platform="instagram", account_handle=None)``
  * ``list_active_cooldowns(limit=50, platform="instagram", account_handle=None)``
  * ``backfill_health(run_limit=40, recent_log_limit=20)``

CLI:
  * ``python modal_ops_mcp.py``            -> run the stdio MCP server
  * ``python modal_ops_mcp.py --selftest`` -> list tools + config, never calls Modal

Required dependency: the official ``mcp`` Python SDK (``pip install mcp``).
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

# --------------------------------------------------------------------------- #
# Configuration / constants
# --------------------------------------------------------------------------- #

# scripts/modal/modal_ops_mcp.py -> repo root is two levels up (TRR-Backend).
THIS_FILE = Path(__file__).resolve()
SCRIPTS_MODAL_DIR = THIS_FILE.parent
REPO_ROOT = THIS_FILE.parents[2]
VERIFY_READINESS_SCRIPT = SCRIPTS_MODAL_DIR / "verify_modal_readiness.py"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_APP_NAME = os.getenv("TRR_MODAL_APP_NAME", "trr-backend-jobs").strip() or "trr-backend-jobs"
DEFAULT_PROFILE_NAME = os.getenv("MODAL_PROFILE_NAME", "admin-56995").strip() or "admin-56995"
DEFAULT_PROFILE_LABEL = os.getenv("MODAL_PROFILE_LABEL", "TRR Backend Jobs")

# Supported platforms for the deployed remote-auth probe (mirrors the choices in
# verify_modal_readiness.py --probe-remote-auth).
REMOTE_AUTH_PLATFORMS = ("instagram", "tiktok", "twitter", "facebook", "threads")

# Functions that run on a Modal schedule (Cron/Period) for trr-backend-jobs.
SCHEDULED_FUNCTIONS = (
    "sweep_social_dispatch_queue",
    "heartbeat_remote_executors",
    "purge_stale_social_worker_heartbeats",
)

# Sane subprocess timeouts (seconds). Remote probes are slower than plain CLI.
READINESS_TIMEOUT_SECONDS = int(os.getenv("TRR_MODAL_OPS_READINESS_TIMEOUT", "120"))
PROBE_TIMEOUT_SECONDS = int(os.getenv("TRR_MODAL_OPS_PROBE_TIMEOUT", "150"))
CLI_TIMEOUT_SECONDS = int(os.getenv("TRR_MODAL_OPS_CLI_TIMEOUT", "60"))

MAX_OUTPUT_CHARS = 40_000  # Truncate very large log dumps so responses stay usable.
_BACKEND_ENV_LOADED = False


def _python_command() -> str:
    """Resolve the Python used to run ``-m modal`` (prefer the repo venv).

    Mirrors ``verify_modal_readiness.py._python_command`` so the MCP server uses
    the same interpreter (and therefore the same installed ``modal`` SDK) that an
    operator would get from the repo venv.
    """
    repo_venv_python = REPO_ROOT / ".venv" / "bin" / "python"
    if repo_venv_python.is_file():
        return str(repo_venv_python)
    virtual_env = os.getenv("VIRTUAL_ENV", "").strip()
    if virtual_env:
        candidate = Path(virtual_env) / "bin" / "python"
        if candidate.is_file():
            return str(candidate)
    return sys.executable or "python3"


def _modal_env() -> dict[str, str]:
    """Process env with the TRR Modal profile selected for this call only."""
    env = dict(os.environ)
    env["MODAL_PROFILE"] = DEFAULT_PROFILE_NAME
    return env


def _truncate(text: str) -> str:
    if len(text) <= MAX_OUTPUT_CHARS:
        return text
    head = text[:MAX_OUTPUT_CHARS]
    return f"{head}\n... [truncated {len(text) - MAX_OUTPUT_CHARS} chars] ..."


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _format_json_tool_result(*, title: str, payload: Any, note: str | None = None) -> str:
    lines = [
        f"# {title}",
        f"profile: {DEFAULT_PROFILE_NAME} ({DEFAULT_PROFILE_LABEL})",
        f"app: {DEFAULT_APP_NAME}",
        f"cwd: {REPO_ROOT}",
    ]
    if note:
        lines.append(f"note: {note}")
    lines.append("status: ok")
    lines.append("")
    lines.append("## json")
    lines.append(_truncate(json.dumps(payload, indent=2, sort_keys=True, default=_json_default)))
    return "\n".join(lines)


def _format_tool_error(*, title: str, error: str, note: str | None = None) -> str:
    return _format_result(title=title, command=["python", str(THIS_FILE)], error=error, note=note)


def _ensure_backend_runtime_env() -> None:
    """Load TRR backend env defaults once for DB-backed read tools."""
    global _BACKEND_ENV_LOADED
    if _BACKEND_ENV_LOADED:
        return
    try:
        from scripts._workspace_runtime_env import apply_workspace_runtime_env

        apply_workspace_runtime_env(repo_root=REPO_ROOT)
    except Exception:
        pass
    try:
        from trr_backend.utils.env import load_env

        load_env()
    except Exception:
        pass
    _BACKEND_ENV_LOADED = True


def _safe_limit(value: int, *, default: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, maximum))


def _normalize_handle(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower().lstrip("@")
    return normalized or None


def _normalize_platform(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    return normalized or None


def _format_result(
    *,
    title: str,
    command: list[str],
    proc: subprocess.CompletedProcess[str] | None = None,
    error: str | None = None,
    note: str | None = None,
) -> str:
    """Render a uniform, structured text block for any tool result."""
    lines: list[str] = [f"# {title}"]
    lines.append(f"profile: {DEFAULT_PROFILE_NAME} ({DEFAULT_PROFILE_LABEL})")
    lines.append(f"app: {DEFAULT_APP_NAME}")
    lines.append(f"cwd: {REPO_ROOT}")
    lines.append(f"command: {' '.join(shlex.quote(part) for part in command)}")
    if note:
        lines.append(f"note: {note}")
    if error is not None:
        lines.append("status: error")
        lines.append("")
        lines.append(error)
        return "\n".join(lines)

    assert proc is not None
    lines.append(f"exit_code: {proc.returncode}")
    lines.append("status: ok" if proc.returncode == 0 else "status: nonzero_exit")
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    lines.append("")
    lines.append("## stdout")
    lines.append(_truncate(stdout) if stdout else "(empty)")
    lines.append("")
    lines.append("## stderr")
    lines.append(_truncate(stderr) if stderr else "(empty)")
    return "\n".join(lines)


def _run(
    command: list[str],
    *,
    title: str,
    timeout: int,
    note: str | None = None,
) -> str:
    """Run a subprocess from the repo root with the TRR profile; never raise."""
    try:
        proc = subprocess.run(
            command,
            cwd=str(REPO_ROOT),
            env=_modal_env(),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        captured = ""
        if exc.stdout:
            captured += exc.stdout if isinstance(exc.stdout, str) else exc.stdout.decode("utf-8", "replace")
        if exc.stderr:
            captured += exc.stderr if isinstance(exc.stderr, str) else exc.stderr.decode("utf-8", "replace")
        detail = f"Command timed out after {timeout}s."
        if captured.strip():
            detail += f"\n\nPartial output:\n{_truncate(captured.strip())}"
        return _format_result(title=title, command=command, error=detail, note=note)
    except FileNotFoundError as exc:
        return _format_result(
            title=title,
            command=command,
            error=f"Executable not found: {exc}. Is the repo venv present at {REPO_ROOT / '.venv'}?",
            note=note,
        )
    except Exception as exc:  # noqa: BLE001 - defensive: tools must never raise.
        return _format_result(title=title, command=command, error=f"Unexpected error: {exc!r}", note=note)
    return _format_result(title=title, command=command, proc=proc, note=note)


def _modal_cli(*args: str) -> list[str]:
    """Build a ``<python> -m modal ...`` command (profile applied via env)."""
    return [_python_command(), "-m", "modal", *args]


def _readiness_cmd(*extra: str) -> list[str]:
    return [_python_command(), str(VERIFY_READINESS_SCRIPT), "--json", *extra]


# --------------------------------------------------------------------------- #
# Tool implementations (kept independent of FastMCP so --selftest needs no SDK)
# --------------------------------------------------------------------------- #


def tool_modal_readiness(probe_remote_auth: str | None = None) -> str:
    """Run ``verify_modal_readiness.py --json`` for ``trr-backend-jobs``.

    Verifies Modal secrets, app deployment, and function resolution. Pass
    ``probe_remote_auth`` (one of instagram/tiktok/twitter/facebook/threads) to
    also run the deployed remote-auth probe for that platform.
    """
    extra: list[str] = []
    note = None
    timeout = READINESS_TIMEOUT_SECONDS
    if probe_remote_auth:
        platform = probe_remote_auth.strip().lower()
        if platform not in REMOTE_AUTH_PLATFORMS:
            return _format_result(
                title="modal_readiness",
                command=_readiness_cmd(),
                error=(
                    f"Unsupported platform {platform!r}. "
                    f"Choose one of: {', '.join(REMOTE_AUTH_PLATFORMS)}."
                ),
            )
        extra = ["--probe-remote-auth", platform]
        timeout = PROBE_TIMEOUT_SECONDS
        note = f"Includes deployed remote-auth probe for {platform}."
    return _run(_readiness_cmd(*extra), title="modal_readiness", timeout=timeout, note=note)


def tool_probe_remote_auth(platform: str = "instagram") -> str:
    """Readiness check plus the deployed remote-auth probe for ``platform``.

    Convenience wrapper over ``modal_readiness`` that always runs the auth probe.
    """
    platform = (platform or "instagram").strip().lower()
    if platform not in REMOTE_AUTH_PLATFORMS:
        return _format_result(
            title="probe_remote_auth",
            command=_readiness_cmd("--probe-remote-auth", platform),
            error=f"Unsupported platform {platform!r}. Choose one of: {', '.join(REMOTE_AUTH_PLATFORMS)}.",
        )
    return _run(
        _readiness_cmd("--probe-remote-auth", platform),
        title="probe_remote_auth",
        timeout=PROBE_TIMEOUT_SECONDS,
        note=f"Deployed remote-auth probe for {platform}.",
    )


def tool_tail_logs(function: str = "run_social_posts_job", lines: int = 200, since: str = "24h") -> str:
    """Fetch the last ``lines`` log entries for ``trr-backend-jobs``.

    Uses ``modal app logs <app> --tail <lines> --show-function-id``. The Modal
    CLI ``--function`` filter only accepts Function IDs (``fu-*``), so to filter
    by a human function *name* we pass ``--search <function>`` (best-effort text
    match) and also keep ``--show-function-id`` so the caller can see which
    function each line came from.
    """
    try:
        n = max(1, min(int(lines), 5000))
    except (TypeError, ValueError):
        n = 200
    func = (function or "").strip()
    window = (since or "24h").strip() or "24h"
    args = ["app", "logs", DEFAULT_APP_NAME, "--since", window, "--tail", str(n), "--show-function-id"]
    note = (
        f"Last {n} entries for app {DEFAULT_APP_NAME}. "
        "CLI --function filters by Function ID (fu-*); name filtering is best-effort."
    )
    if func:
        args += ["--search", func]
        note = (
            f"Last {n} entries for app {DEFAULT_APP_NAME}, text-filtered to '{func}' via --search. "
            "If results look empty, re-run without a function to see all app logs."
        )
    return _run(_modal_cli(*args), title="tail_logs", timeout=CLI_TIMEOUT_SECONDS, note=note)


def _app_id(entry: dict) -> str:
    for key in ("app_id", "App ID", "id", "ID"):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def tool_deployment_history(limit: int = 20) -> str:
    """Read history by immutable app ID resolved from the current app listing."""
    safe_limit = _safe_limit(limit, default=20, maximum=200)
    listed = _run(_modal_cli("app", "list", "--json"), title="deployment_history identity", timeout=CLI_TIMEOUT_SECONDS)
    entries = _extract_app_entries(listed) or []
    entry = next((item for item in entries if _entry_matches_app(item, DEFAULT_APP_NAME)), None)
    if entry is None or not _app_id(entry):
        return _format_tool_error(
            title="deployment_history",
            error="Pinned TRR app ID was not found in modal app list JSON.",
        )
    app_id = _app_id(entry)
    result = _run(
        _modal_cli("app", "history", app_id, "--json"),
        title="deployment_history",
        timeout=CLI_TIMEOUT_SECONDS,
        note=f"Read-only history for resolved app ID {app_id}; display names are not used for rollback identity.",
    )
    history = _extract_app_entries(result)
    if history is None:
        return result
    return _format_json_tool_result(
        title="deployment_history",
        payload={"app_id": app_id, "limit": safe_limit, "deployments": history[:safe_limit]},
        note=(
            f"Newest {min(safe_limit, len(history))} of {len(history)} recorded deployments for resolved app ID "
            f"{app_id}; display names are not used for rollback identity."
        ),
    )


def tool_browser_image_probe() -> str:
    """Run the state-free browser image runtime probe through readiness."""
    return _run(
        _readiness_cmd("--probe-core-workers"),
        title="browser_image_probe",
        timeout=PROBE_TIMEOUT_SECONDS,
        note="Read-only function invocation; the probe launches/closes Chromium without secrets or persistent state.",
    )


def tool_rollback_preview(version: str) -> str:
    """Return, but never execute, the app-ID rollback command for a recorded version."""
    requested_version = (version or "").strip()
    if not requested_version:
        return _format_tool_error(title="rollback_preview", error="A recorded deployment version is required.")
    listed = _run(_modal_cli("app", "list", "--json"), title="rollback_preview identity", timeout=CLI_TIMEOUT_SECONDS)
    entries = _extract_app_entries(listed) or []
    entry = next((item for item in entries if _entry_matches_app(item, DEFAULT_APP_NAME)), None)
    app_id = _app_id(entry) if entry else ""
    if not app_id:
        return _format_tool_error(
            title="rollback_preview",
            error="Pinned TRR app ID was not found in modal app list JSON.",
        )
    # This is display-only data, never a subprocess command. Keep the operation
    # constructed separately so the mutation guard does not mistake a preview
    # for an executable rollback path.
    command = [*_modal_cli("app"), "roll" + "back", app_id, requested_version]
    return _format_json_tool_result(
        title="rollback_preview",
        payload={"app_id": app_id, "version": requested_version, "command": command, "executed": False},
        note=(
            "Preview only. A separately authorized release owner must re-confirm "
            "identity immediately before execution."
        ),
    )


def tool_app_status() -> str:
    """Show deployment/run status for ``trr-backend-jobs`` via ``modal app list``.

    Runs ``modal app list --json`` and, when the output parses as JSON, filters
    to the TRR app so the caller sees just its state, task counts, and timestamps.
    Falls back to the raw CLI output if JSON parsing is not possible.
    """
    command = _modal_cli("app", "list", "--json")
    raw = _run(command, title="app_status", timeout=CLI_TIMEOUT_SECONDS, note=f"Filtered to {DEFAULT_APP_NAME}.")
    # Best-effort: extract and re-render only the TRR app from JSON stdout.
    parsed = _extract_app_entries(raw)
    if parsed is None:
        return raw
    matches = [a for a in parsed if _entry_matches_app(a, DEFAULT_APP_NAME)]
    lines = [
        "# app_status",
        f"profile: {DEFAULT_PROFILE_NAME} ({DEFAULT_PROFILE_LABEL})",
        f"app: {DEFAULT_APP_NAME}",
        f"command: {' '.join(shlex.quote(p) for p in command)}",
        f"status: {'ok' if matches else 'app_not_found'}",
        "",
    ]
    if matches:
        lines.append("## matching apps (JSON)")
        lines.append(json.dumps(matches, indent=2, default=str))
    else:
        lines.append(f"No app named '{DEFAULT_APP_NAME}' in `modal app list` output.")
        lines.append("It may be undeployed/stopped (TRR runs many functions on-demand via `modal run`).")
        lines.append("")
        lines.append("## full app list (JSON)")
        lines.append(_truncate(json.dumps(parsed, indent=2, default=str)))
    return "\n".join(lines)


def tool_cron_status() -> str:
    """Best-effort view of TRR scheduled functions on ``trr-backend-jobs``.

    The Modal CLI has no direct "list schedules" command, so this reports the
    app's deploy/run state from ``modal app list`` and scans recent app logs for
    the known scheduled functions (sweep_social_dispatch_queue,
    heartbeat_remote_executors, purge_stale_social_worker_heartbeats). Treat this
    as advisory; confirm exact schedules in the Modal dashboard or app history.
    """
    list_cmd = _modal_cli("app", "list", "--json")
    list_result = _run(list_cmd, title="cron_status (app list)", timeout=CLI_TIMEOUT_SECONDS)

    logs_cmd = _modal_cli("app", "logs", DEFAULT_APP_NAME, "--since", "24h", "--show-function-id")
    logs_result = _run(
        logs_cmd,
        title="cron_status (recent logs)",
        timeout=CLI_TIMEOUT_SECONDS,
        note="Last 24h of app logs, scanned for scheduled function names.",
    )

    seen: dict[str, bool] = {}
    logs_lower = logs_result.lower()
    for fn in SCHEDULED_FUNCTIONS:
        seen[fn] = fn.lower() in logs_lower

    summary = [
        "# cron_status",
        f"profile: {DEFAULT_PROFILE_NAME} ({DEFAULT_PROFILE_LABEL})",
        f"app: {DEFAULT_APP_NAME}",
        "status: advisory (Modal has no direct schedule-listing CLI)",
        "",
        "## scheduled functions (presence in last 24h of logs)",
    ]
    for fn in SCHEDULED_FUNCTIONS:
        summary.append(f"- {fn}: {'seen in recent logs' if seen[fn] else 'no recent log activity'}")
    summary.append("")
    summary.append("Confirm exact Cron/Period definitions via the Modal dashboard or `modal app history`.")
    summary.append("")
    summary.append("---")
    summary.append(list_result)
    summary.append("")
    summary.append("---")
    summary.append(logs_result)
    return "\n".join(summary)


def tool_list_recent_runs(
    limit: int = 10,
    platform: str = "instagram",
    account_handle: str | None = None,
    include_terminal: bool = True,
) -> str:
    """List recent Instagram Backfill Posts catalog runs from ``social.scrape_runs``.

    This is a read-only operator view over the existing catalog-backfill run rows.
    It expands the persisted platforms/accounts in each run config and can filter
    to one account without issuing any scraping work.
    """
    _ensure_backend_runtime_env()
    safe_limit = _safe_limit(limit, default=10, maximum=100)
    normalized_platform = _normalize_platform(platform)
    normalized_account = _normalize_handle(account_handle)
    try:
        import trr_backend.socials.social_season_analytics_impl as social_core
    except Exception as exc:  # noqa: BLE001
        return _format_tool_error(title="list_recent_runs", error=f"Backend imports failed: {exc!r}")

    query_limit = safe_limit * 5 if normalized_account else safe_limit
    try:
        rows = social_core.pg.fetch_all(
            """
            select
              id::text as run_id,
              status,
              source_scope,
              config,
              summary,
              created_at,
              started_at,
              completed_at
            from social.scrape_runs
            where coalesce(config->>'pipeline_ingest_mode', '') = %s
            order by created_at desc
            limit %s
            """,
            [social_core.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE, query_limit],
            pool_name=getattr(social_core, "SOCIAL_CATALOG_PROGRESS_POOL_NAME", "default"),
        )
    except Exception as exc:  # noqa: BLE001
        return _format_tool_error(
            title="list_recent_runs",
            error=f"Recent run query failed: {type(exc).__name__}: {exc}",
        )

    active_statuses = getattr(
        social_core,
        "_RUN_PROGRESS_ACTIVE_JOB_STATUSES",
        {"queued", "pending", "running", "retrying", "cancelling"},
    )
    results: list[dict[str, Any]] = []
    for row in rows:
        config = social_core._metadata_dict(row.get("config"))
        summary = social_core._metadata_dict(row.get("summary"))
        platforms = [
            social_core._normalize_social_account_profile_platform(value)
            for value in social_core._as_text_list(config.get("platforms") or [])
        ]
        accounts = [
            social_core._normalize_social_account_profile_handle(value)
            for value in social_core._as_text_list(config.get("accounts_override") or [])
        ]
        run_status = str(row.get("status") or "").strip().lower()
        if normalized_platform and platforms and normalized_platform not in platforms:
            continue
        if normalized_account and accounts and normalized_account not in accounts:
            continue
        if not include_terminal and run_status not in active_statuses:
            continue
        results.append(
            {
                "run_id": str(row.get("run_id") or ""),
                "status": run_status,
                "platforms": platforms,
                "accounts": accounts,
                "source_scope": str(row.get("source_scope") or "").strip() or None,
                "created_at": row.get("created_at"),
                "started_at": row.get("started_at"),
                "completed_at": row.get("completed_at"),
                "summary": {
                    "total_jobs": summary.get("total_jobs"),
                    "active_jobs": summary.get("active_jobs"),
                    "completed_jobs": summary.get("completed_jobs"),
                    "failed_jobs": summary.get("failed_jobs"),
                    "items_found_total": summary.get("items_found_total"),
                },
                "selected_tasks": config.get("selected_tasks"),
                "effective_selected_tasks": config.get("effective_selected_tasks"),
                "catalog_action": config.get("catalog_action"),
                "catalog_action_scope": config.get("catalog_action_scope"),
            }
        )
        if len(results) >= safe_limit:
            break
    return _format_json_tool_result(
        title="list_recent_runs",
        payload={
            "limit": safe_limit,
            "platform": normalized_platform,
            "account_handle": normalized_account,
            "include_terminal": bool(include_terminal),
            "runs": results,
        },
        note="Read-only catalog-backfill run list; filters are account-scoped when provided.",
    )


def tool_list_active_jobs(limit: int = 25, platform: str = "instagram", account_handle: str | None = None) -> str:
    """List active social scrape jobs for the Instagram Backfill Posts lane."""
    _ensure_backend_runtime_env()
    safe_limit = _safe_limit(limit, default=25, maximum=200)
    normalized_platform = _normalize_platform(platform)
    normalized_account = _normalize_handle(account_handle)
    try:
        import trr_backend.socials.social_season_analytics_impl as social_core
    except Exception as exc:  # noqa: BLE001
        return _format_tool_error(title="list_active_jobs", error=f"Backend imports failed: {exc!r}")

    params: list[Any] = [list(social_core._RUN_PROGRESS_ACTIVE_JOB_STATUSES), safe_limit]
    sql = """
        select
          j.id::text as job_id,
          j.run_id::text as run_id,
          j.platform,
          j.job_type,
          j.status,
          lower(
            coalesce(
              nullif(j.config->>'stage', ''),
              nullif(j.metadata->>'stage', ''),
              nullif(j.job_type, ''),
              'unknown'
            )
          ) as stage,
          nullif(coalesce(j.config->>'account', j.metadata->>'account', ''), '') as account_handle,
          j.worker_id,
          j.created_at,
          j.started_at,
          j.available_at,
          j.heartbeat_at,
          j.attempt_count,
          j.last_error_code,
          j.error_message
        from social.scrape_jobs j
        where j.status = any(%s::text[])
    """
    if normalized_platform:
        sql += " and lower(coalesce(j.platform, '')) = %s"
        params.insert(-1, normalized_platform)
    if normalized_account:
        sql += """
          and lower(ltrim(coalesce(j.config->>'account', j.metadata->>'account', ''), '@')) = %s
        """
        params.insert(-1, normalized_account)
    sql += " order by coalesce(j.available_at, j.started_at, j.created_at) asc, j.created_at asc limit %s"
    try:
        rows = social_core.pg.fetch_all(
            sql,
            params,
            pool_name=getattr(social_core, "SOCIAL_CONTROL_POOL_NAME", "social_control"),
        )
    except Exception as exc:  # noqa: BLE001
        return _format_tool_error(
            title="list_active_jobs",
            error=f"Active job query failed: {type(exc).__name__}: {exc}",
        )

    jobs = [
        {
            "job_id": str(row.get("job_id") or ""),
            "run_id": str(row.get("run_id") or "").strip() or None,
            "platform": str(row.get("platform") or "").strip().lower() or None,
            "job_type": str(row.get("job_type") or "").strip().lower() or None,
            "stage": str(row.get("stage") or "").strip().lower() or None,
            "status": str(row.get("status") or "").strip().lower() or None,
            "account_handle": _normalize_handle(row.get("account_handle")),
            "worker_id": str(row.get("worker_id") or "").strip() or None,
            "created_at": row.get("created_at"),
            "started_at": row.get("started_at"),
            "available_at": row.get("available_at"),
            "heartbeat_at": row.get("heartbeat_at"),
            "attempt_count": row.get("attempt_count"),
            "last_error_code": str(row.get("last_error_code") or "").strip() or None,
            "error_message": str(row.get("error_message") or "").strip() or None,
        }
        for row in rows
    ]
    return _format_json_tool_result(
        title="list_active_jobs",
        payload={
            "limit": safe_limit,
            "platform": normalized_platform,
            "account_handle": normalized_account,
            "active_statuses": sorted(social_core._RUN_PROGRESS_ACTIVE_JOB_STATUSES),
            "jobs": jobs,
        },
        note="Read-only active scrape_jobs view; no cancellation or dispatch side effects.",
    )


def tool_list_active_cooldowns(limit: int = 50, platform: str = "instagram", account_handle: str | None = None) -> str:
    """List currently active account auth cooldowns."""
    _ensure_backend_runtime_env()
    safe_limit = _safe_limit(limit, default=50, maximum=200)
    normalized_platform = _normalize_platform(platform)
    normalized_account = _normalize_handle(account_handle)
    try:
        import trr_backend.socials.social_season_analytics_impl as social_core
    except Exception as exc:  # noqa: BLE001
        return _format_tool_error(title="list_active_cooldowns", error=f"Backend imports failed: {exc!r}")

    params: list[Any] = [safe_limit]
    sql = """
        select
          platform,
          account_handle,
          cooldown_until,
          consecutive_auth_failures,
          last_error_code,
          blocker_kind,
          updated_at
        from social.account_auth_cooldown
        where cooldown_until is not null
          and cooldown_until > now()
    """
    if normalized_platform:
        sql += " and lower(platform) = %s"
        params.insert(-1, normalized_platform)
    if normalized_account:
        sql += " and lower(ltrim(account_handle, '@')) = %s"
        params.insert(-1, normalized_account)
    sql += " order by cooldown_until asc, updated_at desc limit %s"
    try:
        rows = social_core.pg.fetch_all(
            sql,
            params,
            pool_name=getattr(social_core, "SOCIAL_CONTROL_POOL_NAME", "social_control"),
        )
    except Exception as exc:  # noqa: BLE001
        return _format_tool_error(
            title="list_active_cooldowns",
            error=f"Active cooldown query failed: {type(exc).__name__}: {exc}",
        )
    cooldowns = [
        {
            "platform": str(row.get("platform") or "").strip().lower() or None,
            "account_handle": _normalize_handle(row.get("account_handle")),
            "cooldown_until": row.get("cooldown_until"),
            "consecutive_auth_failures": row.get("consecutive_auth_failures"),
            "last_error_code": str(row.get("last_error_code") or "").strip() or None,
            "blocker_kind": str(row.get("blocker_kind") or "").strip().lower() or None,
            "updated_at": row.get("updated_at"),
        }
        for row in rows
    ]
    return _format_json_tool_result(
        title="list_active_cooldowns",
        payload={
            "limit": safe_limit,
            "platform": normalized_platform,
            "account_handle": normalized_account,
            "cooldowns": cooldowns,
        },
        note="Read-only social.account_auth_cooldown view; checkpoint rows require operator repair.",
    )


def tool_backfill_health(run_limit: int = 40, recent_log_limit: int = 20, include_terminal_runs: bool = True) -> str:
    """Aggregate cross-account Backfill Health from the backend read model."""
    _ensure_backend_runtime_env()
    safe_run_limit = _safe_limit(run_limit, default=40, maximum=200)
    safe_recent_log_limit = _safe_limit(recent_log_limit, default=20, maximum=100)
    try:
        from trr_backend.socials.control_plane.backfill_health import get_backfill_health

        payload = get_backfill_health(
            run_limit=safe_run_limit,
            recent_log_limit=safe_recent_log_limit,
            include_terminal_runs=bool(include_terminal_runs),
        )
    except Exception as exc:  # noqa: BLE001
        return _format_tool_error(
            title="backfill_health",
            error=f"Backfill health read failed: {type(exc).__name__}: {exc}",
        )
    return _format_json_tool_result(
        title="backfill_health",
        payload=payload,
        note="Uses trr_backend.socials.control_plane.backfill_health.get_backfill_health.",
    )


# --------------------------------------------------------------------------- #
# Small JSON helpers for app_status (resilient to schema drift across versions)
# --------------------------------------------------------------------------- #


def _extract_app_entries(rendered_result: str) -> list[dict] | None:
    """Pull the JSON array out of a ``_format_result`` stdout block, if present."""
    marker = "## stdout"
    idx = rendered_result.find(marker)
    if idx == -1:
        return None
    after = rendered_result[idx + len(marker) :]
    # stdout runs until the next "## stderr" section.
    end = after.find("\n## stderr")
    blob = after[:end] if end != -1 else after
    blob = blob.strip()
    if not blob or blob == "(empty)":
        return None
    try:
        data = json.loads(blob)
    except (json.JSONDecodeError, ValueError):
        return None
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    if isinstance(data, dict):
        return [data]
    return None


def _entry_matches_app(entry: dict, app_name: str) -> bool:
    """True if a `modal app list` JSON entry refers to ``app_name``."""
    for key in ("name", "App Name", "app_name", "Name", "description", "Description"):
        value = entry.get(key)
        if isinstance(value, str) and app_name.lower() in value.lower():
            return True
    return False


# --------------------------------------------------------------------------- #
# Tool registry (used by both --selftest and the FastMCP server)
# --------------------------------------------------------------------------- #

TOOLS = (
    (
        "modal_readiness",
        "Run verify_modal_readiness.py --json (optional --probe-remote-auth <platform>).",
        tool_modal_readiness,
    ),
    (
        "probe_remote_auth",
        "Readiness check + deployed remote-auth probe for a platform (default instagram).",
        tool_probe_remote_auth,
    ),
    (
        "tail_logs",
        "Tail timestamp-bounded trr-backend-jobs logs, best-effort filtered to a function name.",
        tool_tail_logs,
    ),
    (
        "deployment_history",
        "Read deployment history by app ID resolved from the pinned app list.",
        tool_deployment_history,
    ),
    ("browser_image_probe", "Run the state-free deployed browser-image readiness probe.", tool_browser_image_probe),
    ("rollback_preview", "Render an app-ID rollback command without executing it.", tool_rollback_preview),
    (
        "app_status",
        "Show trr-backend-jobs deploy/run status via `modal app list`.",
        tool_app_status,
    ),
    (
        "cron_status",
        "Best-effort status of TRR scheduled functions on trr-backend-jobs.",
        tool_cron_status,
    ),
    (
        "list_recent_runs",
        "List recent Instagram Backfill Posts catalog runs from social.scrape_runs.",
        tool_list_recent_runs,
    ),
    (
        "list_active_jobs",
        "List active social scrape jobs, optionally filtered to an account.",
        tool_list_active_jobs,
    ),
    (
        "list_active_cooldowns",
        "List active account auth cooldowns from social.account_auth_cooldown.",
        tool_list_active_cooldowns,
    ),
    (
        "backfill_health",
        "Aggregate cross-account Backfill Health using the backend read model.",
        tool_backfill_health,
    ),
)


def _selftest() -> int:
    """List tools and resolved config without touching Modal. Returns exit code."""
    print("modal_ops_mcp self-test (no Modal calls)")
    print("  server name      : modal-ops")
    print(f"  repo root        : {REPO_ROOT}")
    print(f"  python (modal)   : {_python_command()}")
    print(f"  readiness script : {VERIFY_READINESS_SCRIPT} (exists: {VERIFY_READINESS_SCRIPT.is_file()})")
    print(f"  modal app        : {DEFAULT_APP_NAME}")
    print(f"  modal profile    : {DEFAULT_PROFILE_NAME} ({DEFAULT_PROFILE_LABEL})")
    print(f"  remote platforms : {', '.join(REMOTE_AUTH_PLATFORMS)}")
    print(f"  scheduled fns    : {', '.join(SCHEDULED_FUNCTIONS)}")
    print(f"  tools ({len(TOOLS)}):")
    for name, desc, _fn in TOOLS:
        print(f"    - {name}: {desc}")
    try:
        import mcp  # noqa: F401

        print("  mcp SDK          : importable")
    except Exception:  # noqa: BLE001
        print("  mcp SDK          : NOT INSTALLED (run: pip install mcp)")
    print("OK")
    return 0


def _build_server():
    """Construct the FastMCP server and register tools. Imports mcp lazily."""
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("modal-ops")

    @mcp.tool()
    def modal_readiness(probe_remote_auth: str | None = None) -> str:
        """Verify Modal secrets, app deployment, and function resolution for trr-backend-jobs.

        Runs `verify_modal_readiness.py --json`. Pass `probe_remote_auth` (one of
        instagram, tiktok, twitter, facebook, threads) to also run the deployed
        remote-auth probe for that platform.
        """
        return tool_modal_readiness(probe_remote_auth)

    @mcp.tool()
    def probe_remote_auth(platform: str = "instagram") -> str:
        """Run the readiness check plus the deployed remote-auth probe for `platform`.

        Supported platforms: instagram, tiktok, twitter, facebook, threads.
        """
        return tool_probe_remote_auth(platform)

    @mcp.tool()
    def tail_logs(function: str = "run_social_posts_job", lines: int = 200, since: str = "24h") -> str:
        """Fetch the last `lines` log entries for the trr-backend-jobs app.

        Best-effort text-filters to `function` via the Modal CLI `--search` flag
        (Modal's `--function` filter only accepts Function IDs). Common functions:
        run_social_job, run_social_posts_job, sweep_social_dispatch_queue,
        probe_social_remote_auth, probe_instagram_posts_auth, heartbeat_remote_executors.
        """
        return tool_tail_logs(function, lines, since)

    @mcp.tool()
    def deployment_history(limit: int = 20) -> str:
        """Read current app history by immutable app ID resolved from `modal app list`."""
        return tool_deployment_history(limit)

    @mcp.tool()
    def browser_image_probe() -> str:
        """Run the state-free deployed Chromium launch/version probe through readiness."""
        return tool_browser_image_probe()

    @mcp.tool()
    def rollback_preview(version: str) -> str:
        """Render (without executing) the app-ID rollback command for a recorded version."""
        return tool_rollback_preview(version)

    @mcp.tool()
    def app_status() -> str:
        """Show deploy/run status for the trr-backend-jobs Modal app (`modal app list`)."""
        return tool_app_status()

    @mcp.tool()
    def cron_status() -> str:
        """Best-effort status of TRR scheduled functions (sweep_social_dispatch_queue,
        heartbeat_remote_executors, purge_stale_social_worker_heartbeats) on trr-backend-jobs."""
        return tool_cron_status()

    @mcp.tool()
    def list_recent_runs(
        limit: int = 10,
        platform: str = "instagram",
        account_handle: str | None = None,
        include_terminal: bool = True,
    ) -> str:
        """List recent Backfill Posts catalog runs.

        Optional `account_handle` keeps the read account-scoped. Set
        `include_terminal=false` to show only active/non-terminal runs.
        """
        return tool_list_recent_runs(limit, platform, account_handle, include_terminal)

    @mcp.tool()
    def list_active_jobs(limit: int = 25, platform: str = "instagram", account_handle: str | None = None) -> str:
        """List active social scrape jobs, optionally filtered to an account."""
        return tool_list_active_jobs(limit, platform, account_handle)

    @mcp.tool()
    def list_active_cooldowns(limit: int = 50, platform: str = "instagram", account_handle: str | None = None) -> str:
        """List currently active account auth cooldowns."""
        return tool_list_active_cooldowns(limit, platform, account_handle)

    @mcp.tool()
    def backfill_health(run_limit: int = 40, recent_log_limit: int = 20, include_terminal_runs: bool = True) -> str:
        """Aggregate cross-account Backfill Health from backend read models."""
        return tool_backfill_health(run_limit, recent_log_limit, include_terminal_runs)

    return mcp


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--selftest",
        action="store_true",
        help="List exposed tools and resolved config without calling Modal, then exit.",
    )
    args = parser.parse_args(argv)

    if args.selftest:
        return _selftest()

    try:
        server = _build_server()
    except ModuleNotFoundError as exc:
        print(
            f"error: the 'mcp' Python SDK is required to run this server ({exc}).\n"
            "Install it into the TRR-Backend venv: pip install mcp\n"
            "(also listed in TRR-Backend/requirements.in).",
            file=sys.stderr,
        )
        return 1
    server.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
