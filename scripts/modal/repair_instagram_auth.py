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

VALIDATE_LOCAL_TIMEOUT_SECONDS = 120
FORCE_REFRESH_TIMEOUT_SECONDS = 420
APPLY_NAMED_SECRETS_TIMEOUT_SECONDS = 180
DEPLOY_MODAL_TIMEOUT_SECONDS = 900
VERIFY_REMOTE_AUTH_TIMEOUT_SECONDS = 120
BROWSER_SESSION_INVALIDATED_REASON = "browser_session_invalidated"
MANUAL_CHECKPOINT_REQUIRED_REASON = "manual_checkpoint_required"
MANUAL_CHECKPOINT_NEXT_ACTION = "complete_instagram_email_checkpoint_in_profile_13"
_MANUAL_CHECKPOINT_MARKERS = (
    "checkpoint_required",
    "challenge_required",
    "graphql_validation_challenge",
    "html_challenge_or_auth_required",
    "redirect_to_checkpoint",
    "warmup_auth_challenge",
)


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
        "--account-handle",
        default="",
        help="Optional Instagram account handle used to verify the deployed profile-posts endpoint.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the repair summary as JSON.",
    )
    return parser.parse_args()


def _run_command(command: list[str], *, timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=True,
        capture_output=True,
        cwd=REPO_ROOT,
        text=True,
        timeout=timeout_seconds,
    )


def _run_json_command_allow_failure(
    command: list[str],
    *,
    step_name: str,
    timeout_seconds: int,
) -> tuple[dict[str, Any], int]:
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        cwd=REPO_ROOT,
        text=True,
        timeout=timeout_seconds,
    )
    try:
        payload = _parse_last_json_line(completed.stdout, step_name=step_name)
    except Exception as exc:
        if completed.returncode != 0:
            raise subprocess.CalledProcessError(
                completed.returncode,
                command,
                output=completed.stdout,
                stderr=completed.stderr,
            ) from exc
        raise
    return payload, int(completed.returncode or 0)


def _parse_last_json_line(stdout: str, *, step_name: str) -> dict[str, Any]:
    stripped = str(stdout or "").strip()
    if stripped:
        try:
            payload = json.loads(stripped)
            if isinstance(payload, dict):
                return payload
        except json.JSONDecodeError:
            pass
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


def _validation_requires_manual_checkpoint(validation_payload: dict[str, Any]) -> bool:
    reason = str(validation_payload.get("reason") or "").strip().lower()
    detail = validation_payload.get("detail")
    try:
        detail_text = json.dumps(detail, sort_keys=True).lower()
    except TypeError:
        detail_text = str(detail or "").lower()
    combined = f"{reason} {detail_text}"
    return any(marker in combined for marker in _MANUAL_CHECKPOINT_MARKERS)


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
        command.append("--force")
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


def _verify_remote_auth_command(
    *,
    python_command: str,
    modal_environment: str,
    account_handle: str | None = None,
) -> list[str]:
    command = [
        python_command,
        str(REPO_ROOT / "scripts" / "modal" / "verify_modal_readiness.py"),
        "--json",
        "--probe-remote-auth",
        "instagram",
    ]
    normalized_account = str(account_handle or "").strip().lstrip("@")
    if normalized_account:
        command.extend(["--probe-instagram-posts-auth", normalized_account])
        command.extend(["--probe-instagram-comments-auth", normalized_account])
    if modal_environment:
        command.extend(["--env", modal_environment])
    return command


def _failed_summary(
    *,
    steps: list[dict[str, Any]],
    failure_reason: str,
    next_action: str | None = None,
    remote_auth_probe: dict[str, Any] | None = None,
    instagram_posts_auth_probe: dict[str, Any] | None = None,
    instagram_comments_auth_probe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "ok": False,
        "failure_reason": failure_reason,
        "next_action": next_action,
        "modal_secret_apply_reached": any(step.get("name") == "apply_named_secrets" for step in steps),
        "modal_deploy_reached": any(step.get("name") == "deploy_modal_app" for step in steps),
        "steps": steps,
        "remote_auth_probe": remote_auth_probe,
        "instagram_posts_auth_probe": instagram_posts_auth_probe,
        "instagram_comments_auth_probe": instagram_comments_auth_probe,
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

    instagram_posts_auth_probe = (
        dict(verify_payload.get("instagram_posts_auth_probe") or {})
        if isinstance(verify_payload.get("instagram_posts_auth_probe"), dict)
        else {}
    )
    if instagram_posts_auth_probe and not bool(instagram_posts_auth_probe.get("ready")):
        status = (
            str(instagram_posts_auth_probe.get("status") or instagram_posts_auth_probe.get("result") or "")
            .strip()
            .lower()
        )
        if status == "transport_blocked":
            return "instagram_posts_transport_probe_failed"
        return "instagram_posts_auth_probe_failed"

    instagram_comments_auth_probe = (
        dict(verify_payload.get("instagram_comments_auth_probe") or {})
        if isinstance(verify_payload.get("instagram_comments_auth_probe"), dict)
        else {}
    )
    if instagram_comments_auth_probe and not bool(instagram_comments_auth_probe.get("ready")):
        status = (
            str(instagram_comments_auth_probe.get("status") or instagram_comments_auth_probe.get("result") or "")
            .strip()
            .lower()
        )
        reason = str(instagram_comments_auth_probe.get("reason") or "").strip().lower()
        if reason == BROWSER_SESSION_INVALIDATED_REASON:
            return "instagram_comments_browser_session_invalidated"
        if reason == "html_challenge_or_auth_required":
            return "instagram_comments_html_challenge_or_auth_required"
        if status == "transport_blocked":
            return "instagram_comments_transport_probe_failed"
        if status == "fetch_blocked":
            return "instagram_comments_probe_unavailable"
        return "instagram_comments_auth_probe_failed"

    ignored_missing_functions = {"probe_getty_remote_access"}
    missing_functions = {
        str(name).strip() for name in (verify_payload.get("missing_functions") or []) if str(name).strip()
    }
    relevant_missing_functions = sorted(missing_functions - ignored_missing_functions)
    if relevant_missing_functions:
        return "missing_required_functions"
    return None


def run_repair(
    *,
    source_env: Path,
    modal_environment: str = "",
    account_handle: str | None = None,
) -> dict[str, Any]:
    load_env()

    python_command = _python_command()
    steps: list[dict[str, Any]] = []

    try:
        validation_payload, _validation_rc = _run_json_command_allow_failure(
            _refresh_command(python_command=python_command, force=False),
            step_name="validate_local",
            timeout_seconds=VALIDATE_LOCAL_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001
        steps.append({"name": "validate_local", "status": "failed", "error": type(exc).__name__})
        return _failed_summary(steps=steps, failure_reason="local_validation_failed")
    local_valid = bool(validation_payload.get("validated"))
    steps.append(
        {
            "name": "validate_local",
            "status": "ok" if local_valid else "failed",
            "result": {
                "validated": local_valid,
                "reason": validation_payload.get("reason"),
            },
        }
    )

    if not local_valid:
        if _validation_requires_manual_checkpoint(validation_payload):
            return _failed_summary(
                steps=steps,
                failure_reason=MANUAL_CHECKPOINT_REQUIRED_REASON,
                next_action=MANUAL_CHECKPOINT_NEXT_ACTION,
            )
        try:
            refresh_payload = _parse_last_json_line(
                _run_command(
                    _refresh_command(python_command=python_command, force=True),
                    timeout_seconds=FORCE_REFRESH_TIMEOUT_SECONDS,
                ).stdout,
                step_name="refresh",
            )
        except Exception as exc:  # noqa: BLE001
            steps.append({"name": "refresh", "status": "failed", "error": type(exc).__name__})
            return _failed_summary(steps=steps, failure_reason="refresh_failed")
        steps.append(
            {
                "name": "refresh",
                "status": "ok",
                "result": {"validated": bool(refresh_payload.get("validated"))},
            }
        )

        try:
            validation_payload, _validation_after_refresh_rc = _run_json_command_allow_failure(
                _refresh_command(python_command=python_command, force=False),
                step_name="validate_local_after_refresh",
                timeout_seconds=VALIDATE_LOCAL_TIMEOUT_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001
            steps.append({"name": "validate_local_after_refresh", "status": "failed", "error": type(exc).__name__})
            return _failed_summary(steps=steps, failure_reason="local_validation_failed")
        local_valid = bool(validation_payload.get("validated"))
        steps.append(
            {
                "name": "validate_local_after_refresh",
                "status": "ok" if local_valid else "failed",
                "result": {
                    "validated": local_valid,
                    "reason": validation_payload.get("reason"),
                },
            }
        )

    if not local_valid:
        return _failed_summary(steps=steps, failure_reason="local_validation_failed")

    try:
        _run_command(
            _apply_named_secrets_command(
                python_command=python_command,
                source_env=source_env,
                modal_environment=modal_environment,
            ),
            timeout_seconds=APPLY_NAMED_SECRETS_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001
        steps.append({"name": "apply_named_secrets", "status": "failed", "error": type(exc).__name__})
        return _failed_summary(steps=steps, failure_reason="apply_named_secrets_failed")
    steps.append({"name": "apply_named_secrets", "status": "ok"})

    try:
        _run_command(
            _deploy_modal_command(python_command=python_command, modal_environment=modal_environment),
            timeout_seconds=DEPLOY_MODAL_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001
        steps.append({"name": "deploy_modal_app", "status": "failed", "error": type(exc).__name__})
        return _failed_summary(steps=steps, failure_reason="deploy_failed")
    steps.append({"name": "deploy_modal_app", "status": "ok"})

    try:
        verify_command = _verify_remote_auth_command(
            python_command=python_command,
            modal_environment=modal_environment,
            account_handle=account_handle,
        )
        verify_payload, _verify_returncode = _run_json_command_allow_failure(
            verify_command,
            step_name="verify_remote_auth",
            timeout_seconds=VERIFY_REMOTE_AUTH_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001
        steps.append({"name": "verify_remote_auth", "status": "failed", "error": type(exc).__name__})
        return _failed_summary(steps=steps, failure_reason="remote_probe_failed")
    remote_auth_probe = (
        dict(verify_payload.get("remote_auth_probe") or {})
        if isinstance(verify_payload.get("remote_auth_probe"), dict)
        else None
    )
    instagram_posts_auth_probe = (
        dict(verify_payload.get("instagram_posts_auth_probe") or {})
        if isinstance(verify_payload.get("instagram_posts_auth_probe"), dict)
        else None
    )
    instagram_comments_auth_probe = (
        dict(verify_payload.get("instagram_comments_auth_probe") or {})
        if isinstance(verify_payload.get("instagram_comments_auth_probe"), dict)
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
                    "reason": (
                        instagram_comments_auth_probe or instagram_posts_auth_probe or remote_auth_probe or {}
                    ).get("reason"),
                },
            }
        )
        return _failed_summary(
            steps=steps,
            failure_reason=verify_failure_reason,
            remote_auth_probe=remote_auth_probe,
            instagram_posts_auth_probe=instagram_posts_auth_probe,
            instagram_comments_auth_probe=instagram_comments_auth_probe,
        )
    steps.append({"name": "verify_remote_auth", "status": "ok", "result": {"ready": True}})

    return {
        "ok": True,
        "failure_reason": None,
        "steps": steps,
        "remote_auth_probe": remote_auth_probe,
        "instagram_posts_auth_probe": instagram_posts_auth_probe,
        "instagram_comments_auth_probe": instagram_comments_auth_probe,
    }


def _print_text_summary(summary: dict[str, Any]) -> None:
    print("Instagram remote auth repair")
    for step in summary.get("steps") or []:
        print(f"  - {step['name']}: {step['status']}")
    probe = summary.get("remote_auth_probe") or {}
    if probe:
        probe_reason = f" ({probe.get('reason')})" if probe.get("reason") else ""
        print(f"  Remote auth probe: {'ready' if probe.get('ready') else 'not ready'}{probe_reason}")
    posts_probe = summary.get("instagram_posts_auth_probe") or {}
    if posts_probe:
        probe_reason = f" ({posts_probe.get('reason')})" if posts_probe.get("reason") else ""
        print(f"  Instagram posts auth probe: {'ready' if posts_probe.get('ready') else 'not ready'}{probe_reason}")
    comments_probe = summary.get("instagram_comments_auth_probe") or {}
    if comments_probe:
        probe_reason = f" ({comments_probe.get('reason')})" if comments_probe.get("reason") else ""
        print(
            f"  Instagram comments auth probe: {'ready' if comments_probe.get('ready') else 'not ready'}{probe_reason}"
        )
    print(f"  Result: {'ok' if summary.get('ok') else 'failed'}")
    if summary.get("next_action"):
        print(f"  Next action: {summary['next_action']}")


def main() -> int:
    args = parse_args()
    summary = run_repair(
        source_env=args.source_env,
        modal_environment=str(args.modal_environment or "").strip(),
        account_handle=str(args.account_handle or "").strip() or None,
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        _print_text_summary(summary)
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
