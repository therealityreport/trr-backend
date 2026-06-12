#!/usr/bin/env python3
"""Clean up a mistaken TRR Modal deployment from a non-authoritative workspace."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.modal.deploy_backend import (  # noqa: E402
    REQUIRED_MODAL_PROFILE,
    REQUIRED_MODAL_WORKSPACE,
    pinned_modal_env,
)

DEFAULT_APP_NAME = "trr-backend-jobs"
DEFAULT_WRONG_PROFILE = "thb-bbl"
DEFAULT_WRONG_WORKSPACE = "tommy-hulihan-basketball"


def python_command() -> str:
    repo_venv_python = REPO_ROOT / ".venv" / "bin" / "python"
    if repo_venv_python.is_file():
        return str(repo_venv_python)
    return sys.executable or "python3.11"


def modal_profile_env(profile: str, environ: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(environ or os.environ)
    env["MODAL_PROFILE"] = profile
    return env


def _modal_command(*args: str, modal_environment: str = "") -> list[str]:
    command = [python_command(), "-m", "modal", *args]
    if modal_environment:
        command.extend(["--env", modal_environment])
    return command


def _run_json(command: list[str], *, env: dict[str, str], timeout_seconds: int = 120) -> Any:
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    return json.loads(completed.stdout or "[]")


def _run_text(command: list[str], *, env: dict[str, str], timeout_seconds: int = 120) -> str:
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    return completed.stdout


def _active_workspace(profile: str, *, env: dict[str, str] | None = None) -> dict[str, Any]:
    rows = _run_json(
        _modal_command("profile", "list") + ["--json"],
        env=modal_profile_env(profile, env),
    )
    active = next((row for row in rows if isinstance(row, dict) and row.get("active") is True), None)
    return {
        "profile": str(active.get("name") or "") if active else "",
        "workspace": str(active.get("workspace") or "") if active else "",
    }


def _verify_authoritative_workspace(*, modal_environment: str = "") -> dict[str, Any]:
    command = [
        python_command(),
        str(REPO_ROOT / "scripts" / "modal" / "verify_modal_readiness.py"),
        "--json",
    ]
    if modal_environment:
        command.extend(["--env", modal_environment])
    payload = _run_json(command, env=pinned_modal_env(), timeout_seconds=180)
    modal_workspace = payload.get("modal_workspace") if isinstance(payload, dict) else None
    return {
        "ok": bool(payload.get("ok")) if isinstance(payload, dict) else False,
        "modal_workspace": modal_workspace if isinstance(modal_workspace, dict) else {},
        "blocking_probe_failures": (
            list(payload.get("blocking_probe_failures") or []) if isinstance(payload, dict) else []
        ),
    }


def _app_rows(*, profile: str, modal_environment: str = "") -> list[dict[str, Any]]:
    payload = _run_json(
        _modal_command("app", "list", modal_environment=modal_environment) + ["--json"],
        env=modal_profile_env(profile),
    )
    return [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []


def _app_is_present(rows: list[dict[str, Any]], app_name: str) -> bool:
    for row in rows:
        values = {str(value).strip() for value in row.values() if isinstance(value, str)}
        if app_name in values:
            return True
    return False


def cleanup_wrong_workspace_deploy(
    *,
    wrong_profile: str,
    wrong_workspace: str,
    app_name: str,
    modal_environment: str = "",
    stop: bool = False,
) -> dict[str, Any]:
    authoritative = _verify_authoritative_workspace(modal_environment=modal_environment)
    if not authoritative["ok"]:
        return {
            "ok": False,
            "failure_reason": "authoritative_workspace_not_ready",
            "authoritative": authoritative,
            "wrong_workspace": None,
            "wrong_app_present": None,
            "stopped": False,
        }

    wrong_context = _active_workspace(wrong_profile)
    if wrong_context["profile"] == REQUIRED_MODAL_PROFILE or wrong_context["workspace"] == REQUIRED_MODAL_WORKSPACE:
        return {
            "ok": False,
            "failure_reason": "wrong_profile_resolves_to_authoritative_workspace",
            "authoritative": authoritative,
            "wrong_workspace": wrong_context,
            "wrong_app_present": None,
            "stopped": False,
        }
    if wrong_workspace and wrong_context["workspace"] != wrong_workspace:
        return {
            "ok": False,
            "failure_reason": "wrong_workspace_mismatch",
            "authoritative": authoritative,
            "wrong_workspace": wrong_context,
            "expected_wrong_workspace": wrong_workspace,
            "wrong_app_present": None,
            "stopped": False,
        }

    rows = _app_rows(profile=wrong_profile, modal_environment=modal_environment)
    app_present = _app_is_present(rows, app_name)
    history = _run_json(
        _modal_command("app", "history", app_name, modal_environment=modal_environment) + ["--json"],
        env=modal_profile_env(wrong_profile),
    ) if app_present else []

    stopped = False
    if stop and app_present:
        _run_text(
            # `modal app stop` has no confirmation flag in the pinned CLI version.
            _modal_command("app", "stop", app_name, modal_environment=modal_environment),
            env=modal_profile_env(wrong_profile),
        )
        stopped = True

    return {
        "ok": True,
        "failure_reason": None,
        "authoritative": authoritative,
        "wrong_workspace": wrong_context,
        "wrong_app_name": app_name,
        "wrong_app_present": app_present,
        "wrong_app_history_count": len(history) if isinstance(history, list) else None,
        "stopped": stopped,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--wrong-profile",
        default=DEFAULT_WRONG_PROFILE,
        help="Modal profile that owns the mistaken app.",
    )
    parser.add_argument(
        "--wrong-workspace",
        default=DEFAULT_WRONG_WORKSPACE,
        help="Expected non-authoritative workspace name for the wrong profile.",
    )
    parser.add_argument(
        "--app-name",
        default=DEFAULT_APP_NAME,
        help=f"Wrong-workspace app name (default: {DEFAULT_APP_NAME}).",
    )
    parser.add_argument("--env", default="", help="Optional Modal environment name.")
    parser.add_argument(
        "--stop",
        action="store_true",
        help="Stop the wrong-workspace app after the authoritative workspace is healthy.",
    )
    parser.add_argument("--json", action="store_true", help="Emit the cleanup summary as JSON.")
    return parser.parse_args(argv)


def _print_text(summary: dict[str, Any]) -> None:
    wrong_workspace = summary.get("wrong_workspace") or {}
    print("Wrong-workspace Modal cleanup")
    print(f"  Authoritative ready: {bool((summary.get('authoritative') or {}).get('ok'))}")
    print(f"  Wrong profile: {wrong_workspace.get('profile') or '<unknown>'}")
    print(f"  Wrong workspace: {wrong_workspace.get('workspace') or '<unknown>'}")
    print(f"  Wrong app present: {summary.get('wrong_app_present')}")
    print(f"  Stopped: {summary.get('stopped')}")
    print(f"  Result: {'ok' if summary.get('ok') else summary.get('failure_reason')}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = cleanup_wrong_workspace_deploy(
        wrong_profile=str(args.wrong_profile or "").strip(),
        wrong_workspace=str(args.wrong_workspace or "").strip(),
        app_name=str(args.app_name or "").strip() or DEFAULT_APP_NAME,
        modal_environment=str(args.env or "").strip(),
        stop=bool(args.stop),
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        _print_text(summary)
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
