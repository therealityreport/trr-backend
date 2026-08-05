#!/usr/bin/env python3
"""Repair shared Instagram auth for the Modal worker plane."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.modal.deploy_backend import (  # noqa: E402
    DEFAULT_APP_NAME,
    DEFAULT_APP_REF,
    REQUIRED_MODAL_ENVIRONMENT,
    REQUIRED_MODAL_PROFILE,
    REQUIRED_MODAL_WORKSPACE,
    pinned_modal_env,
)
from trr_backend.utils.env import load_env  # noqa: E402

VALIDATE_LOCAL_TIMEOUT_SECONDS = 120
FORCE_REFRESH_TIMEOUT_SECONDS = 420
APPLY_NAMED_SECRETS_TIMEOUT_SECONDS = 180
DEPLOY_MODAL_TIMEOUT_SECONDS = 900
VERIFY_REMOTE_AUTH_TIMEOUT_SECONDS = 120
BROWSER_SESSION_INVALIDATED_REASON = "browser_session_invalidated"
MANUAL_CHECKPOINT_REQUIRED_REASON = "manual_checkpoint_required"
MANUAL_CHECKPOINT_NEXT_ACTION = "complete_instagram_email_checkpoint_in_profile_13"
MANUAL_AUTH_REQUIRED_REASON = "manual_auth_required"
MANUAL_AUTH_NEXT_ACTION = "manually_confirm_instagram_account_safe_in_profile_13_then_rerun_validation"
AUTOMATED_COOKIE_REFRESH_DISABLED_REASON = "automated_cookie_refresh_disabled"
AUTH_REPAIR_COOLDOWN_ACTIVE_REASON = "instagram_auth_repair_cooldown_active"
AUTH_REPAIR_COOLDOWN_SECONDS = 60 * 60
INSTAGRAM_REFRESH_CONFIRMATION = "I UNDERSTAND INSTAGRAM AUTH RISK"
INSTAGRAM_REFRESH_WARNING = (
    "Instagram cookie refresh can trigger login challenges or account locks. "
    "Only run it after manually confirming the account is safe."
)
_MANUAL_CHECKPOINT_MARKERS = (
    "checkpoint_required",
    "challenge_required",
    "graphql_validation_challenge",
    "html_challenge_or_auth_required",
    "redirect_to_checkpoint",
    "warmup_auth_challenge",
)
_MANUAL_AUTH_MARKERS = (
    *_MANUAL_CHECKPOINT_MARKERS,
    "browser_session_invalidated",
    "email_checkpoint",
    "login_prompt_detected",
    "login_required",
    "manual_auth_required",
    "redirect_login",
    "verification_required",
)
COOLDOWN_FAILURE_REASONS = {
    MANUAL_CHECKPOINT_REQUIRED_REASON,
    MANUAL_AUTH_REQUIRED_REASON,
    "refresh_failed",
    "local_validation_failed",
}


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
        choices=(REQUIRED_MODAL_ENVIRONMENT,),
        default=REQUIRED_MODAL_ENVIRONMENT,
        help=f"Pinned Modal environment for apply/deploy/verify steps (required: {REQUIRED_MODAL_ENVIRONMENT}).",
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
    parser.add_argument(
        "--allow-cookie-refresh",
        action="store_true",
        help=(
            "Allow one explicit Instagram cookie refresh attempt after local validation fails. "
            "Checkpoint, login, and verification states still stop for manual handling."
        ),
    )
    parser.add_argument(
        "--validate-local-only",
        action="store_true",
        help=(
            "Validate local Instagram cookie files only. "
            "Never refreshes cookies, applies Modal secrets, deploys, verifies remote auth, or writes cooldown state."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Validate local Instagram cookies and report the Modal repair steps that would run, "
            "without refreshing cookies, writing cooldown state, applying secrets, deploying, or verifying remote auth."
        ),
    )
    parser.add_argument(
        "--confirm-instagram-refresh",
        default="",
        help=f"Required with --allow-cookie-refresh. Exact value: {INSTAGRAM_REFRESH_CONFIRMATION!r}",
    )
    parser.add_argument(
        "--clear-auth-repair-cooldown",
        action="store_true",
        help=(
            "Clear the local Instagram auth repair cooldown only after the account has been manually recovered "
            "and local Instagram cookie validation passes."
        ),
    )
    return parser.parse_args()


def _run_command(command: list[str], *, timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=True,
        capture_output=True,
        cwd=REPO_ROOT,
        env=_pinned_repair_env(),
        text=True,
        timeout=timeout_seconds,
    )


def _require_main_modal_environment(modal_environment: str) -> str:
    normalized = str(modal_environment or REQUIRED_MODAL_ENVIRONMENT).strip()
    if normalized != REQUIRED_MODAL_ENVIRONMENT:
        raise ValueError(
            "Modal target override blocked: "
            f"environment={normalized or '<empty>'}; expected {REQUIRED_MODAL_ENVIRONMENT}."
        )
    return REQUIRED_MODAL_ENVIRONMENT


def _pinned_repair_env() -> dict[str, str]:
    env = pinned_modal_env()
    expected = {
        "MODAL_PROFILE": REQUIRED_MODAL_PROFILE,
        "MODAL_WORKSPACE": REQUIRED_MODAL_WORKSPACE,
        "MODAL_ENVIRONMENT": REQUIRED_MODAL_ENVIRONMENT,
        "TRR_MODAL_APP_NAME": DEFAULT_APP_NAME,
        "TRR_MODAL_APP_REF": DEFAULT_APP_REF,
    }
    for key, expected_value in expected.items():
        observed = str(env.get(key) or "").strip()
        if observed != expected_value:
            raise ValueError(
                f"Modal target override blocked: {key}={observed or '<empty>'}; expected {expected_value}."
            )
    return env


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
        env=_pinned_repair_env(),
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


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _cooldown_state_path() -> Path:
    return REPO_ROOT / ".locks" / "instagram-auth-repair-cooldown.json"


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _read_cooldown_state() -> dict[str, Any]:
    path = _cooldown_state_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _active_cooldown() -> dict[str, Any] | None:
    payload = _read_cooldown_state()
    expires_at = _parse_iso_datetime(payload.get("cooldown_until"))
    if expires_at is None:
        return None
    if expires_at <= _now_utc():
        _clear_cooldown_state()
        return None
    return {
        **payload,
        "cooldown_until": expires_at.isoformat().replace("+00:00", "Z"),
    }


def _cooldown_state_payload(*, reason: str) -> dict[str, Any]:
    now = _now_utc()
    return {
        "platform": "instagram",
        "reason": str(reason or "unknown").strip() or "unknown",
        "started_at": now.isoformat().replace("+00:00", "Z"),
        "cooldown_until": (now + timedelta(seconds=AUTH_REPAIR_COOLDOWN_SECONDS)).isoformat().replace("+00:00", "Z"),
        "cooldown_seconds": AUTH_REPAIR_COOLDOWN_SECONDS,
        "next_action": MANUAL_AUTH_NEXT_ACTION,
    }


def _write_cooldown_state(*, reason: str) -> dict[str, Any]:
    payload = _cooldown_state_payload(reason=reason)
    path = _cooldown_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _cooldown_preview(*, reason: str) -> dict[str, Any]:
    payload = _cooldown_state_payload(reason=reason)
    payload["dry_run"] = True
    payload["would_write"] = True
    payload["path"] = str(_cooldown_state_path())
    return payload


def _cooldown_for_mode(*, reason: str, dry_run: bool) -> dict[str, Any]:
    if dry_run:
        return _cooldown_preview(reason=reason)
    return _write_cooldown_state(reason=reason)


def _cooldown_for_failure_reason(*, reason: str, dry_run: bool) -> dict[str, Any] | None:
    normalized = str(reason or "").strip()
    if normalized not in COOLDOWN_FAILURE_REASONS:
        return None
    return _cooldown_for_mode(reason=normalized, dry_run=dry_run)


def _clear_cooldown_state() -> None:
    try:
        _cooldown_state_path().unlink()
    except FileNotFoundError:
        return


def _next_action_for_failure_reason(failure_reason: str | None) -> str | None:
    normalized = str(failure_reason or "").strip().lower()
    if not normalized:
        return None
    if any(marker in normalized for marker in _MANUAL_CHECKPOINT_MARKERS):
        return MANUAL_CHECKPOINT_NEXT_ACTION
    if any(marker in normalized for marker in _MANUAL_AUTH_MARKERS):
        return MANUAL_AUTH_NEXT_ACTION
    if normalized in {MANUAL_CHECKPOINT_REQUIRED_REASON, "instagram_refresh_confirmation_required"}:
        return MANUAL_CHECKPOINT_NEXT_ACTION
    if normalized in {
        MANUAL_AUTH_REQUIRED_REASON,
        AUTOMATED_COOKIE_REFRESH_DISABLED_REASON,
        AUTH_REPAIR_COOLDOWN_ACTIVE_REASON,
    }:
        return MANUAL_AUTH_NEXT_ACTION
    return None


def _safe_advisory_state(
    *,
    cooldown: dict[str, Any] | None = None,
    failure_reason: str | None = None,
    cleared: bool = False,
) -> dict[str, Any]:
    reason = str((cooldown or {}).get("reason") or failure_reason or "").strip() or None
    next_action = str((cooldown or {}).get("next_action") or "").strip() or _next_action_for_failure_reason(reason)
    active = bool(cooldown) and not cleared
    return {
        "platform": "instagram",
        "active": active,
        "reason": reason if active else None,
        "next_action": next_action if active else None,
        "clear_requires_validation": True,
        "clear_command": "scripts/modal/repair_instagram_auth.py --clear-auth-repair-cooldown --json",
        "validation_mode": "comments_endpoint",
        "cleared": bool(cleared),
    }


def _validation_requires_manual_checkpoint(validation_payload: dict[str, Any]) -> bool:
    reason = str(validation_payload.get("reason") or "").strip().lower()
    detail = validation_payload.get("detail")
    try:
        detail_text = json.dumps(detail, sort_keys=True).lower()
    except TypeError:
        detail_text = str(detail or "").lower()
    combined = f"{reason} {detail_text}"
    return any(marker in combined for marker in _MANUAL_CHECKPOINT_MARKERS)


def _validation_requires_manual_auth(validation_payload: dict[str, Any]) -> bool:
    reason = str(validation_payload.get("reason") or "").strip().lower()
    detail = validation_payload.get("detail")
    try:
        detail_text = json.dumps(detail, sort_keys=True).lower()
    except TypeError:
        detail_text = str(detail or "").lower()
    combined = f"{reason} {detail_text}"
    return any(marker in combined for marker in _MANUAL_AUTH_MARKERS)


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
    modal_environment = _require_main_modal_environment(modal_environment)
    command = [
        python_command,
        str(REPO_ROOT / "scripts" / "modal" / "prepare_named_secrets.py"),
        "--source-env",
        str(source_env),
        "--apply",
    ]
    command.extend(["--modal-environment", modal_environment])
    return command


def _deploy_modal_command(*, python_command: str, modal_environment: str) -> list[str]:
    modal_environment = _require_main_modal_environment(modal_environment)
    return [
        python_command,
        "-m",
        "modal",
        "deploy",
        "-m",
        DEFAULT_APP_REF,
        "--env",
        modal_environment,
    ]


def _verify_remote_auth_command(
    *,
    python_command: str,
    modal_environment: str,
    account_handle: str | None = None,
) -> list[str]:
    modal_environment = _require_main_modal_environment(modal_environment)
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
        command.append("--strict-instagram-comments-auth")
    command.extend(["--env", modal_environment])
    return command


def _failed_summary(
    *,
    steps: list[dict[str, Any]],
    failure_reason: str,
    next_action: str | None = None,
    automated_cookie_refresh_allowed: bool = False,
    safety_stop: bool = False,
    warning_message: str | None = None,
    cooldown: dict[str, Any] | None = None,
    dry_run: bool = False,
    remote_auth_probe: dict[str, Any] | None = None,
    instagram_posts_auth_probe: dict[str, Any] | None = None,
    instagram_comments_auth_probe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    inferred_next_action = next_action or _next_action_for_failure_reason(failure_reason)
    return {
        "ok": False,
        "failure_reason": failure_reason,
        "next_action": inferred_next_action,
        "automated_cookie_refresh_allowed": automated_cookie_refresh_allowed,
        "dry_run": bool(dry_run),
        "safety_stop": bool(safety_stop or inferred_next_action),
        "warning_message": warning_message,
        "cooldown": cooldown,
        "cooldown_written": bool(cooldown and not bool(cooldown.get("dry_run"))),
        "modal_secret_apply_reached": any(step.get("name") == "apply_named_secrets" for step in steps),
        "modal_deploy_reached": any(step.get("name") == "deploy_modal_app" for step in steps),
        "remote_verify_reached": any(step.get("name") == "verify_remote_auth" for step in steps),
        "safe_advisory_state": _safe_advisory_state(cooldown=cooldown, failure_reason=failure_reason),
        "steps": steps,
        "remote_auth_probe": remote_auth_probe,
        "instagram_posts_auth_probe": instagram_posts_auth_probe,
        "instagram_comments_auth_probe": instagram_comments_auth_probe,
    }


def _verify_modal_readiness_failure_reason(verify_payload: dict[str, Any]) -> str | None:
    if verify_payload.get("app_found") is False:
        return "modal_app_missing"
    if list(verify_payload.get("missing_secrets") or []):
        return "missing_named_secrets"
    if list(verify_payload.get("missing_web_endpoints") or []):
        return "missing_web_endpoints"

    remote_auth_probe = (
        dict(verify_payload.get("remote_auth_probe") or {})
        if isinstance(verify_payload.get("remote_auth_probe"), dict)
        else {}
    )
    if not bool(remote_auth_probe.get("ready")):
        if _validation_requires_manual_checkpoint(remote_auth_probe):
            return MANUAL_CHECKPOINT_REQUIRED_REASON
        if _validation_requires_manual_auth(remote_auth_probe):
            return MANUAL_AUTH_REQUIRED_REASON
        return "remote_probe_failed"

    instagram_posts_auth_probe = (
        dict(verify_payload.get("instagram_posts_auth_probe") or {})
        if isinstance(verify_payload.get("instagram_posts_auth_probe"), dict)
        else {}
    )
    if instagram_posts_auth_probe and not bool(instagram_posts_auth_probe.get("ready")):
        if _validation_requires_manual_checkpoint(instagram_posts_auth_probe):
            return MANUAL_CHECKPOINT_REQUIRED_REASON
        if _validation_requires_manual_auth(instagram_posts_auth_probe):
            return MANUAL_AUTH_REQUIRED_REASON
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
        if _validation_requires_manual_checkpoint(instagram_comments_auth_probe):
            return MANUAL_CHECKPOINT_REQUIRED_REASON
        if _validation_requires_manual_auth(instagram_comments_auth_probe):
            return MANUAL_AUTH_REQUIRED_REASON
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
    allow_cookie_refresh: bool = False,
    confirm_instagram_refresh: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    modal_environment = _require_main_modal_environment(modal_environment)
    _pinned_repair_env()
    load_env()

    python_command = _python_command()
    steps: list[dict[str, Any]] = []
    confirmation_ok = str(confirm_instagram_refresh or "").strip() == INSTAGRAM_REFRESH_CONFIRMATION
    if dry_run and allow_cookie_refresh:
        steps.append(
            {
                "name": "refresh",
                "status": "skipped",
                "reason": "dry_run_never_refreshes_cookies",
            }
        )
        allow_cookie_refresh = False

    if allow_cookie_refresh and not confirmation_ok:
        steps.append(
            {
                "name": "refresh",
                "status": "blocked",
                "reason": "instagram_refresh_confirmation_required",
            }
        )
        return _failed_summary(
            steps=steps,
            failure_reason="instagram_refresh_confirmation_required",
            next_action=MANUAL_AUTH_NEXT_ACTION,
            automated_cookie_refresh_allowed=False,
            safety_stop=True,
            warning_message=INSTAGRAM_REFRESH_WARNING,
            dry_run=dry_run,
        )

    cooldown = _active_cooldown()
    if cooldown is not None:
        steps.append(
            {
                "name": "cooldown",
                "status": "blocked",
                "reason": AUTH_REPAIR_COOLDOWN_ACTIVE_REASON,
                "cooldown_until": cooldown.get("cooldown_until"),
            }
        )
        return _failed_summary(
            steps=steps,
            failure_reason=AUTH_REPAIR_COOLDOWN_ACTIVE_REASON,
            next_action=MANUAL_AUTH_NEXT_ACTION,
            automated_cookie_refresh_allowed=False,
            safety_stop=True,
            warning_message=INSTAGRAM_REFRESH_WARNING,
            cooldown=cooldown,
            dry_run=dry_run,
        )

    try:
        validation_payload, _validation_rc = _run_json_command_allow_failure(
            _refresh_command(python_command=python_command, force=False),
            step_name="validate_local",
            timeout_seconds=VALIDATE_LOCAL_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001
        steps.append({"name": "validate_local", "status": "failed", "error": type(exc).__name__})
        return _failed_summary(steps=steps, failure_reason="local_validation_failed", dry_run=dry_run)
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
                automated_cookie_refresh_allowed=bool(allow_cookie_refresh),
                safety_stop=True,
                warning_message=INSTAGRAM_REFRESH_WARNING,
                cooldown=_cooldown_for_mode(reason=MANUAL_CHECKPOINT_REQUIRED_REASON, dry_run=dry_run),
                dry_run=dry_run,
            )
        if _validation_requires_manual_auth(validation_payload):
            steps.append(
                {
                    "name": "refresh",
                    "status": "skipped",
                    "reason": MANUAL_AUTH_REQUIRED_REASON,
                }
            )
            return _failed_summary(
                steps=steps,
                failure_reason=MANUAL_AUTH_REQUIRED_REASON,
                next_action=MANUAL_AUTH_NEXT_ACTION,
                automated_cookie_refresh_allowed=bool(allow_cookie_refresh),
                safety_stop=True,
                warning_message=INSTAGRAM_REFRESH_WARNING,
                cooldown=_cooldown_for_mode(reason=MANUAL_AUTH_REQUIRED_REASON, dry_run=dry_run),
                dry_run=dry_run,
            )
        if not allow_cookie_refresh:
            steps.append(
                {
                    "name": "refresh",
                    "status": "skipped",
                    "reason": AUTOMATED_COOKIE_REFRESH_DISABLED_REASON,
                }
            )
            return _failed_summary(
                steps=steps,
                failure_reason=AUTOMATED_COOKIE_REFRESH_DISABLED_REASON,
                next_action=MANUAL_AUTH_NEXT_ACTION,
                automated_cookie_refresh_allowed=False,
                safety_stop=True,
                warning_message=INSTAGRAM_REFRESH_WARNING,
                dry_run=dry_run,
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
            return _failed_summary(
                steps=steps,
                failure_reason="refresh_failed",
                warning_message=INSTAGRAM_REFRESH_WARNING,
                cooldown=_cooldown_for_mode(reason="refresh_failed", dry_run=dry_run),
                dry_run=dry_run,
            )
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
            return _failed_summary(
                steps=steps,
                failure_reason="local_validation_failed",
                warning_message=INSTAGRAM_REFRESH_WARNING,
                cooldown=_cooldown_for_mode(reason="local_validation_failed", dry_run=dry_run),
                dry_run=dry_run,
            )
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
        return _failed_summary(
            steps=steps,
            failure_reason="local_validation_failed",
            warning_message=INSTAGRAM_REFRESH_WARNING,
            cooldown=_cooldown_for_mode(reason="local_validation_failed", dry_run=dry_run),
            dry_run=dry_run,
        )

    if dry_run:
        planned_steps = [
            {
                "name": "apply_named_secrets",
                "status": "planned",
                "command": " ".join(
                    _apply_named_secrets_command(
                        python_command=python_command,
                        source_env=source_env,
                        modal_environment=modal_environment,
                    )
                ),
            },
            {
                "name": "deploy_modal_app",
                "status": "planned",
                "command": " ".join(
                    _deploy_modal_command(python_command=python_command, modal_environment=modal_environment)
                ),
            },
            {
                "name": "verify_remote_auth",
                "status": "planned",
                "command": " ".join(
                    _verify_remote_auth_command(
                        python_command=python_command,
                        modal_environment=modal_environment,
                        account_handle=account_handle,
                    )
                ),
            },
        ]
        steps.extend(planned_steps)
        return {
            "ok": True,
            "mode": "repair_dry_run",
            "dry_run": True,
            "failure_reason": None,
            "next_action": None,
            "automated_cookie_refresh_allowed": False,
            "safety_stop": False,
            "warning_message": INSTAGRAM_REFRESH_WARNING,
            "cooldown": None,
            "cooldown_written": False,
            "modal_secret_apply_reached": False,
            "modal_deploy_reached": False,
            "remote_verify_reached": False,
            "safe_advisory_state": _safe_advisory_state(),
            "steps": steps,
            "planned_modal_steps": planned_steps,
            "remote_auth_probe": None,
            "instagram_posts_auth_probe": None,
            "instagram_comments_auth_probe": None,
        }

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
        return _failed_summary(
            steps=steps,
            failure_reason="remote_probe_failed",
        )
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
            cooldown=_cooldown_for_failure_reason(reason=verify_failure_reason, dry_run=False),
            remote_auth_probe=remote_auth_probe,
            instagram_posts_auth_probe=instagram_posts_auth_probe,
            instagram_comments_auth_probe=instagram_comments_auth_probe,
        )
    steps.append({"name": "verify_remote_auth", "status": "ok", "result": {"ready": True}})
    _clear_cooldown_state()

    return {
        "ok": True,
        "failure_reason": None,
        "next_action": None,
        "automated_cookie_refresh_allowed": bool(allow_cookie_refresh),
        "safety_stop": False,
        "warning_message": INSTAGRAM_REFRESH_WARNING if allow_cookie_refresh else None,
        "cooldown": None,
        "modal_secret_apply_reached": any(step.get("name") == "apply_named_secrets" for step in steps),
        "modal_deploy_reached": any(step.get("name") == "deploy_modal_app" for step in steps),
        "remote_verify_reached": any(step.get("name") == "verify_remote_auth" for step in steps),
        "safe_advisory_state": _safe_advisory_state(cleared=True),
        "steps": steps,
        "remote_auth_probe": remote_auth_probe,
        "instagram_posts_auth_probe": instagram_posts_auth_probe,
        "instagram_comments_auth_probe": instagram_comments_auth_probe,
    }


def run_validate_local_only() -> dict[str, Any]:
    """Validate local Instagram cookies without triggering repair side effects."""
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
        return _failed_summary(
            steps=steps,
            failure_reason="local_validation_failed",
            safety_stop=True,
            warning_message=INSTAGRAM_REFRESH_WARNING,
        )

    local_valid = bool(validation_payload.get("validated"))
    reason = validation_payload.get("reason")
    steps.append(
        {
            "name": "validate_local",
            "status": "ok" if local_valid else "failed",
            "result": {
                "validated": local_valid,
                "reason": reason,
            },
        }
    )
    if local_valid:
        return {
            "ok": True,
            "mode": "validate_local_only",
            "failure_reason": None,
            "automated_cookie_refresh_allowed": False,
            "safety_stop": False,
            "warning_message": None,
            "cooldown": None,
            "cooldown_written": False,
            "modal_secret_apply_reached": False,
            "modal_deploy_reached": False,
            "remote_verify_reached": False,
            "safe_advisory_state": _safe_advisory_state(),
            "steps": steps,
            "remote_auth_probe": None,
            "instagram_posts_auth_probe": None,
            "instagram_comments_auth_probe": None,
        }

    failure_reason = str(reason or "local_validation_failed")
    next_action = None
    if _validation_requires_manual_checkpoint(validation_payload):
        failure_reason = MANUAL_CHECKPOINT_REQUIRED_REASON
        next_action = MANUAL_CHECKPOINT_NEXT_ACTION
    elif _validation_requires_manual_auth(validation_payload):
        failure_reason = MANUAL_AUTH_REQUIRED_REASON
        next_action = MANUAL_AUTH_NEXT_ACTION

    failed = _failed_summary(
        steps=steps,
        failure_reason=failure_reason,
        next_action=next_action,
        automated_cookie_refresh_allowed=False,
        safety_stop=True,
        warning_message=INSTAGRAM_REFRESH_WARNING,
    )
    failed["mode"] = "validate_local_only"
    failed["cooldown_written"] = False
    failed["remote_verify_reached"] = False
    return failed


def run_clear_auth_repair_cooldown() -> dict[str, Any]:
    """Clear the local repair cooldown only after local auth validation passes."""
    cooldown_before = _active_cooldown()
    validation_summary = run_validate_local_only()
    steps = list(validation_summary.get("steps") or [])
    if not bool(validation_summary.get("ok")):
        validation_summary["mode"] = "clear_auth_repair_cooldown"
        validation_summary["cleared"] = False
        validation_summary["cooldown"] = cooldown_before
        validation_summary["safe_advisory_state"] = _safe_advisory_state(
            cooldown=cooldown_before,
            failure_reason=str(validation_summary.get("failure_reason") or ""),
        )
        return validation_summary

    _clear_cooldown_state()
    return {
        "ok": True,
        "mode": "clear_auth_repair_cooldown",
        "failure_reason": None,
        "next_action": None,
        "automated_cookie_refresh_allowed": False,
        "safety_stop": False,
        "warning_message": None,
        "cooldown": None,
        "cooldown_before_clear": cooldown_before,
        "cleared": True,
        "cooldown_written": False,
        "modal_secret_apply_reached": False,
        "modal_deploy_reached": False,
        "remote_verify_reached": False,
        "safe_advisory_state": _safe_advisory_state(cleared=True),
        "steps": steps + [{"name": "clear_auth_repair_cooldown", "status": "ok"}],
        "remote_auth_probe": None,
        "instagram_posts_auth_probe": None,
        "instagram_comments_auth_probe": None,
    }


def _print_text_summary(summary: dict[str, Any]) -> None:
    print("Instagram remote auth repair")
    if summary.get("mode") == "repair_dry_run" or summary.get("dry_run"):
        print("  Mode: dry run; no cookies, cooldown files, Modal secrets, deploys, or remote probes were changed")
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
    advisory = summary.get("safe_advisory_state") or {}
    if advisory.get("clear_requires_validation") and not summary.get("ok"):
        print("  Cooldown clearing: complete manual recovery, then run the clear command after local validation passes")
        if advisory.get("clear_command"):
            print(f"  Clear command: {advisory['clear_command']}")


def main() -> int:
    # Pin the full Modal identity before any secret/deploy/verify path runs.
    os.environ.update(_pinned_repair_env())
    args = parse_args()
    if bool(args.clear_auth_repair_cooldown):
        summary = run_clear_auth_repair_cooldown()
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
        else:
            _print_text_summary(summary)
        return 0 if summary.get("ok") else 1
    if bool(getattr(args, "validate_local_only", False)):
        summary = run_validate_local_only()
    else:
        summary = run_repair(
            source_env=args.source_env,
            modal_environment=str(args.modal_environment or "").strip(),
            account_handle=str(args.account_handle or "").strip() or None,
            allow_cookie_refresh=bool(args.allow_cookie_refresh),
            confirm_instagram_refresh=str(args.confirm_instagram_refresh or ""),
            dry_run=bool(getattr(args, "dry_run", False)),
        )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        _print_text_summary(summary)
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
