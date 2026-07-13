#!/usr/bin/env python3
# ruff: noqa: E402
"""Deploy the TRR Modal backend app through the required workspace profile."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.modal.api_canary import DEFAULT_CANARY_TIMEOUT_SECONDS, run_api_cold_start_canary

DEFAULT_APP_REF = "trr_backend.modal_jobs"
DEFAULT_APP_NAME = "trr-backend-jobs"
DEFAULT_INCIDENT_NOTE = (
    REPO_ROOT / "docs" / "observability" / "modal-v439-v440-serve-backend-api-crash-loop-2026-05-28.md"
)
INCIDENT_NOTES_DIR = REPO_ROOT / "docs" / "observability"
REQUIRED_MODAL_PROFILE = "admin-56995"
REQUIRED_MODAL_WORKSPACE = "admin-56995"
DEFAULT_HISTORY_LIMIT = 5
HISTORY_STAMP_START = "<!-- modal-deploy-history:start -->"
HISTORY_STAMP_END = "<!-- modal-deploy-history:end -->"


def python_command() -> str:
    repo_venv_python = REPO_ROOT / ".venv" / "bin" / "python"
    if repo_venv_python.is_file():
        return str(repo_venv_python)
    return sys.executable or "python3.11"


def pinned_modal_env(environ: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(environ or os.environ)
    env["MODAL_PROFILE"] = REQUIRED_MODAL_PROFILE
    env["TRR_MODAL_APP_NAME"] = DEFAULT_APP_NAME
    return env


def modal_profile_rows(*, env: dict[str, str] | None = None) -> list[dict[str, Any]]:
    completed = subprocess.run(
        [python_command(), "-m", "modal", "profile", "list", "--json"],
        cwd=REPO_ROOT,
        env=pinned_modal_env(env),
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout or "[]")
    return payload if isinstance(payload, list) else []


def modal_workspace_context(*, env: dict[str, str] | None = None) -> dict[str, Any]:
    rows = modal_profile_rows(env=env)
    active = next((row for row in rows if isinstance(row, dict) and row.get("active") is True), None)
    return {
        "required_profile": REQUIRED_MODAL_PROFILE,
        "required_workspace": REQUIRED_MODAL_WORKSPACE,
        "active_profile": str(active.get("name") or "") if active else "",
        "active_workspace": str(active.get("workspace") or "") if active else "",
    }


def verify_required_workspace(*, env: dict[str, str] | None = None) -> dict[str, Any]:
    context = modal_workspace_context(env=env)
    if context["active_profile"] != REQUIRED_MODAL_PROFILE or context["active_workspace"] != REQUIRED_MODAL_WORKSPACE:
        raise RuntimeError(
            "Modal deploy blocked: expected profile/workspace "
            f"{REQUIRED_MODAL_PROFILE}/{REQUIRED_MODAL_WORKSPACE}, got "
            f"{context['active_profile'] or '<none>'}/{context['active_workspace'] or '<none>'}."
        )
    return context


def build_deploy_command(args: argparse.Namespace) -> list[str]:
    command = [python_command(), "-m", "modal", "deploy", "-m", args.app_ref]
    if args.env:
        command.extend(["--env", args.env])
    if args.name:
        command.extend(["--name", args.name])
    if args.tag:
        command.extend(["--tag", args.tag])
    if args.strategy:
        command.extend(["--strategy", args.strategy])
    if args.stream_logs:
        command.append("--stream-logs")
    return command


def build_readiness_command(args: argparse.Namespace) -> list[str]:
    command = [
        python_command(),
        str(REPO_ROOT / "scripts" / "modal" / "verify_modal_readiness.py"),
        "--json",
    ]
    if args.env:
        command.extend(["--env", args.env])
    return command


def verify_deployed_readiness(args: argparse.Namespace, *, env: dict[str, str]) -> dict[str, Any]:
    completed = subprocess.run(
        build_readiness_command(args),
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout or "{}")
    if not isinstance(payload, dict):
        raise RuntimeError("Modal readiness emitted a non-object JSON payload.")
    if not payload.get("ok"):
        raise RuntimeError(
            "Modal readiness failed after deploy: "
            + json.dumps(
                {
                    "blocking_probe_failures": payload.get("blocking_probe_failures"),
                    "modal_workspace": payload.get("modal_workspace"),
                    "missing_functions": payload.get("missing_functions"),
                    "missing_web_endpoints": payload.get("missing_web_endpoints"),
                },
                sort_keys=True,
            )
        )
    return payload


def build_deploy_history_command(args: argparse.Namespace) -> list[str]:
    command = [python_command(), "-m", "modal", "app", "history", args.app_name, "--json"]
    if args.env:
        command.extend(["--env", args.env])
    return command


def fetch_deploy_history(args: argparse.Namespace, *, env: dict[str, str]) -> list[dict[str, Any]]:
    completed = subprocess.run(
        build_deploy_history_command(args),
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout or "[]")
    return [row for row in payload if isinstance(row, dict)]


def format_deploy_history_stamp(
    *,
    history_rows: list[dict[str, Any]],
    canary: dict[str, Any],
    workspace_context: dict[str, Any],
    limit: int = DEFAULT_HISTORY_LIMIT,
) -> str:
    canary_url = str(canary.get("url") or "<unknown>")
    canary_status = str(canary.get("status") or "<unknown>")
    canary_attempt = str(canary.get("attempt") or "<unknown>")
    lines = [
        HISTORY_STAMP_START,
        "## Deploy History Stamp",
        "",
        f"- Last stamped: `{datetime.now().astimezone().isoformat(timespec='seconds')}`",
        f"- Workspace: `{workspace_context['active_workspace']}`",
        f"- Profile: `{workspace_context['active_profile']}`",
        f"- Canary: `{canary_url}` HTTP `{canary_status}` on attempt `{canary_attempt}`",
        "",
        "| Version | Deployed At | Deployed By | Commit | Client |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in history_rows[: max(1, limit)]:
        lines.append(
            "| "
            f"{row.get('Version') or ''} | "
            f"{row.get('Time deployed') or ''} | "
            f"{row.get('Deployed by') or ''} | "
            f"{row.get('Commit') or ''} | "
            f"{row.get('Client') or ''} |"
        )
    lines.extend(["", HISTORY_STAMP_END, ""])
    return "\n".join(lines)


def stamp_incident_note(
    *,
    note_path: Path,
    history_rows: list[dict[str, Any]],
    canary: dict[str, Any],
    workspace_context: dict[str, Any],
) -> bool:
    if not note_path.is_file():
        return False
    stamp = format_deploy_history_stamp(
        history_rows=history_rows,
        canary=canary,
        workspace_context=workspace_context,
    )
    current = note_path.read_text(encoding="utf-8")
    if HISTORY_STAMP_START in current and HISTORY_STAMP_END in current:
        before, rest = current.split(HISTORY_STAMP_START, 1)
        _old, after = rest.split(HISTORY_STAMP_END, 1)
        updated = before.rstrip() + "\n\n" + stamp.rstrip() + after
    else:
        updated = current.rstrip() + "\n\n" + stamp
    note_path.write_text(updated, encoding="utf-8")
    return True


def resolve_incident_note_path(*, incident_note: str, incident_note_name: str = "") -> Path:
    name = str(incident_note_name or "").strip()
    if name:
        note_name = name if name.endswith(".md") else f"{name}.md"
        return INCIDENT_NOTES_DIR / note_name
    return Path(str(incident_note or "").strip() or DEFAULT_INCIDENT_NOTE)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--app-ref",
        default=DEFAULT_APP_REF,
        help=f"Modal app module to deploy (default: {DEFAULT_APP_REF})",
    )
    parser.add_argument("--env", default="", help="Optional Modal environment name.")
    parser.add_argument("--app-name", default=DEFAULT_APP_NAME, help=f"Modal app name (default: {DEFAULT_APP_NAME}).")
    parser.add_argument("--name", default="", help="Optional Modal deployment name override.")
    parser.add_argument("--tag", default="", help="Optional Modal deployment tag.")
    parser.add_argument(
        "--strategy",
        choices=("rolling", "recreate"),
        default="",
        help="Optional Modal deploy strategy.",
    )
    parser.add_argument("--stream-logs", action="store_true", help="Stream Modal app logs after deployment.")
    parser.add_argument("--dry-run", action="store_true", help="Run workspace preflight and print the deploy command.")
    parser.add_argument(
        "--canary-timeout-seconds",
        type=int,
        default=DEFAULT_CANARY_TIMEOUT_SECONDS,
        help=f"Seconds per /health canary request after deploy (default: {DEFAULT_CANARY_TIMEOUT_SECONDS}).",
    )
    parser.add_argument(
        "--incident-note",
        default=str(DEFAULT_INCIDENT_NOTE),
        help="Markdown incident note to stamp with recent Modal deploy history after a successful canary.",
    )
    parser.add_argument(
        "--incident-note-name",
        default="",
        help="Incident note name under docs/observability to stamp, with or without .md.",
    )
    parser.add_argument("--skip-incident-stamp", action="store_true", help="Do not update the incident note.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    env = pinned_modal_env()
    context = verify_required_workspace(env=env)
    command = build_deploy_command(args)
    print(
        "Modal deploy target: "
        f"profile={context['active_profile']} workspace={context['active_workspace']} "
        f"app_ref={args.app_ref}",
        flush=True,
    )
    if args.dry_run:
        print(f"MODAL_PROFILE={REQUIRED_MODAL_PROFILE} {' '.join(command)}")
        return 0
    completed = subprocess.run(command, cwd=REPO_ROOT, env=env, check=False)
    if completed.returncode != 0:
        return int(completed.returncode)
    try:
        readiness = verify_deployed_readiness(args, env=env)
        canary = dict(readiness.get("api_canary") or {})
        if canary.get("ok") is not True:
            canary = run_api_cold_start_canary(
                str(readiness.get("api_web_url") or ""),
                timeout_seconds=max(1, int(args.canary_timeout_seconds or DEFAULT_CANARY_TIMEOUT_SECONDS)),
            )
    except Exception as exc:  # noqa: BLE001
        print(f"Modal deploy canary failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(
        "Modal API cold-start canary passed: "
        f"url={canary.get('url', '<unknown>')} "
        f"status={canary.get('status', '<unknown>')} "
        f"attempt={canary.get('attempt', '<unknown>')}",
        flush=True,
    )
    if not args.skip_incident_stamp:
        try:
            history_rows = fetch_deploy_history(args, env=env)
            incident_note_path = resolve_incident_note_path(
                incident_note=args.incident_note,
                incident_note_name=args.incident_note_name,
            )
            stamped = stamp_incident_note(
                note_path=incident_note_path,
                history_rows=history_rows,
                canary=canary,
                workspace_context=context,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"Modal incident note stamp skipped: {exc}", file=sys.stderr, flush=True)
        else:
            if stamped:
                print(f"Modal incident note stamped: {incident_note_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
