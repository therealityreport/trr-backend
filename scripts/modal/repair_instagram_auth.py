#!/usr/bin/env python3
"""Repair shared Instagram auth for the Modal worker plane."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trr_backend.utils.env import load_env  # noqa: E402


def _python_command() -> str:
    repo_venv_python = REPO_ROOT / ".venv" / "bin" / "python"
    if repo_venv_python.is_file():
        return str(repo_venv_python)
    return sys.executable or "python3.11"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-env",
        type=Path,
        default=REPO_ROOT / ".env",
        help=f"Source env file used to render named Modal secrets (default: {REPO_ROOT / '.env'})",
    )
    parser.add_argument(
        "--modal-environment",
        default="",
        help="Optional Modal environment name passed through to apply/deploy/verify steps.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the repair summary as JSON.",
    )
    return parser.parse_args()


def _run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )


def _parse_last_json_line(stdout: str, *, step_name: str) -> dict[str, Any]:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        raise ValueError(f"{step_name} did not emit JSON output")
    try:
        payload = json.loads(lines[-1])
    except json.JSONDecodeError as exc:
        raise ValueError(f"{step_name} emitted invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{step_name} emitted a non-object JSON payload")
    return payload


def _refresh_command(*, python_command: str, force: bool) -> list[str]:
    command = [
        python_command,
        str(REPO_ROOT / "scripts" / "socials" / "refresh_cookies.py"),
        "--platform",
        "instagram",
        "--validation-mode",
        "comments_endpoint",
    ]
    if force:
        command.extend(["--force", "--headed"])
    else:
        command.append("--validate-only")
    return command


def _apply_named_secrets_command(
    *,
    python_command: str,
    source_env: Path,
    modal_environment: str,
) -> list[str]:
    command = [
        python_command,
        str(REPO_ROOT / "scripts" / "modal" / "prepare_named_secrets.py"),
        "--source-env",
        str(source_env),
        "--apply",
    ]
    if modal_environment:
        command.extend(["--modal-environment", modal_environment])
    return command


def _deploy_modal_command(*, python_command: str, modal_environment: str) -> list[str]:
    command = [python_command, "-m", "modal", "deploy", "-m", "trr_backend.modal_jobs"]
    if modal_environment:
        command.extend(["--env", modal_environment])
    return command


def _verify_remote_auth_command(*, python_command: str, modal_environment: str) -> list[str]:
    command = [
        python_command,
        str(REPO_ROOT / "scripts" / "modal" / "verify_modal_readiness.py"),
        "--json",
        "--probe-remote-auth",
        "instagram",
    ]
    if modal_environment:
        command.extend(["--env", modal_environment])
    return command


def _failed_summary(
    *,
    steps: list[dict[str, Any]],
    failure_reason: str,
    remote_auth_probe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "ok": False,
        "failure_reason": failure_reason,
        "steps": steps,
        "remote_auth_probe": remote_auth_probe,
    }


def _verify_modal_readiness_failure_reason(verify_payload: dict[str, Any]) -> str | None:
    remote_auth_probe = (
        dict(verify_payload.get("remote_auth_probe") or {})
        if isinstance(verify_payload.get("remote_auth_probe"), dict)
        else {}
    )
    if not bool(remote_auth_probe.get("ready")):
        return "remote_probe_failed"
    if not bool(verify_payload.get("app_found")):
        return "modal_app_missing"
    if list(verify_payload.get("missing_secrets") or []):
        return "missing_named_secrets"
    if list(verify_payload.get("missing_web_endpoints") or []):
        return "missing_web_endpoints"

    ignored_missing_functions = {"probe_getty_remote_access"}
    missing_functions = {
        str(name).strip()
        for name in (verify_payload.get("missing_functions") or [])
        if str(name).strip()
    }
    relevant_missing_functions = sorted(missing_functions - ignored_missing_functions)
    if relevant_missing_functions:
        return "missing_required_functions"
    return None


def run_repair(
    *,
    source_env: Path,
    modal_environment: str = "",
) -> dict[str, Any]:
    load_env()

    python_command = _python_command()
    steps: list[dict[str, Any]] = []

    try:
        refresh_payload = _parse_last_json_line(
            _run_command(_refresh_command(python_command=python_command, force=True)).stdout,
            step_name="refresh",
        )
    except Exception as exc:  # noqa: BLE001
        steps.append({"name": "refresh", "status": "failed", "error": type(exc).__name__})
        return _failed_summary(steps=steps, failure_reason="refresh_failed")
    steps.append({"name": "refresh", "status": "ok", "result": {"validated": bool(refresh_payload.get("validated"))}})

    try:
        validation_payload = _parse_last_json_line(
            _run_command(_refresh_command(python_command=python_command, force=False)).stdout,
            step_name="validate_local",
        )
    except Exception as exc:  # noqa: BLE001
        steps.append({"name": "validate_local", "status": "failed", "error": type(exc).__name__})
        return _failed_summary(steps=steps, failure_reason="local_validation_failed")
    if not bool(validation_payload.get("validated")):
        steps.append(
            {
                "name": "validate_local",
                "status": "failed",
                "result": {
                    "validated": False,
                    "reason": validation_payload.get("reason"),
                },
            }
        )
        return _failed_summary(steps=steps, failure_reason="local_validation_failed")
    steps.append({"name": "validate_local", "status": "ok", "result": {"validated": True}})

    try:
        _run_command(
            _apply_named_secrets_command(
                python_command=python_command,
                source_env=source_env,
                modal_environment=modal_environment,
            )
        )
    except Exception as exc:  # noqa: BLE001
        steps.append({"name": "apply_named_secrets", "status": "failed", "error": type(exc).__name__})
        return _failed_summary(steps=steps, failure_reason="apply_named_secrets_failed")
    steps.append({"name": "apply_named_secrets", "status": "ok"})

    try:
        _run_command(_deploy_modal_command(python_command=python_command, modal_environment=modal_environment))
    except Exception as exc:  # noqa: BLE001
        steps.append({"name": "deploy_modal_app", "status": "failed", "error": type(exc).__name__})
        return _failed_summary(steps=steps, failure_reason="deploy_failed")
    steps.append({"name": "deploy_modal_app", "status": "ok"})

    try:
        verify_command = _verify_remote_auth_command(
            python_command=python_command,
            modal_environment=modal_environment,
        )
        verify_payload = _parse_last_json_line(
            _run_command(verify_command).stdout,
            step_name="verify_remote_auth",
        )
    except Exception as exc:  # noqa: BLE001
        steps.append({"name": "verify_remote_auth", "status": "failed", "error": type(exc).__name__})
        return _failed_summary(steps=steps, failure_reason="remote_probe_failed")
    remote_auth_probe = (
        dict(verify_payload.get("remote_auth_probe") or {})
        if isinstance(verify_payload.get("remote_auth_probe"), dict)
        else None
    )
    verify_failure_reason = _verify_modal_readiness_failure_reason(verify_payload)
    if verify_failure_reason is not None:
        steps.append(
            {
                "name": "verify_remote_auth",
                "status": "failed",
                "result": {
                    "ok": bool(verify_payload.get("ok")),
                    "failure_reason": verify_failure_reason,
                    "reason": (remote_auth_probe or {}).get("reason"),
                },
            }
        )
        return _failed_summary(
            steps=steps,
            failure_reason=verify_failure_reason,
            remote_auth_probe=remote_auth_probe,
        )
    steps.append({"name": "verify_remote_auth", "status": "ok", "result": {"ready": True}})

    return {
        "ok": True,
        "failure_reason": None,
        "steps": steps,
        "remote_auth_probe": remote_auth_probe,
    }


def _print_text_summary(summary: dict[str, Any]) -> None:
    print("Instagram remote auth repair")
    for step in summary.get("steps") or []:
        print(f"  - {step['name']}: {step['status']}")
    probe = summary.get("remote_auth_probe") or {}
    if probe:
        probe_reason = f" ({probe.get('reason')})" if probe.get("reason") else ""
        print(f"  Remote auth probe: {'ready' if probe.get('ready') else 'not ready'}{probe_reason}")
    print(f"  Result: {'ok' if summary.get('ok') else 'failed'}")


def main() -> int:
    args = parse_args()
    summary = run_repair(
        source_env=args.source_env,
        modal_environment=str(args.modal_environment or "").strip(),
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        _print_text_summary(summary)
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
