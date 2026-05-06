#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.modal import prepare_named_secrets, verify_modal_readiness  # noqa: E402


def workspace_logs_dir(repo_root: Path = REPO_ROOT) -> Path:
    return repo_root.parent / ".logs" / "workspace"


def fingerprint_path(repo_root: Path = REPO_ROOT) -> Path:
    return workspace_logs_dir(repo_root) / "modal-runtime-fingerprint.json"


def default_result() -> dict[str, Any]:
    return {
        "state": "ok",
        "reason": None,
        "remediation": None,
        "deployed": False,
        "fingerprint_changed": False,
        "readiness": None,
    }


def command_timeout_seconds() -> int:
    raw = str(os.getenv("WORKSPACE_RUNTIME_MODAL_COMMAND_TIMEOUT_SECONDS") or "300").strip()
    try:
        parsed = int(raw)
    except ValueError:
        return 300
    return max(30, parsed)


def post_deploy_verify_attempts() -> int:
    raw = str(os.getenv("WORKSPACE_RUNTIME_MODAL_POST_DEPLOY_VERIFY_ATTEMPTS") or "3").strip()
    try:
        parsed = int(raw)
    except ValueError:
        return 3
    return max(1, parsed)


def post_deploy_verify_delay_seconds() -> float:
    raw = str(os.getenv("WORKSPACE_RUNTIME_MODAL_POST_DEPLOY_VERIFY_DELAY_SECONDS") or "5").strip()
    try:
        parsed = float(raw)
    except ValueError:
        return 5.0
    return max(0.0, parsed)


def build_modal_fingerprint(repo_root: Path = REPO_ROOT) -> str:
    source_env = prepare_named_secrets._load_source_env(repo_root / ".env")
    runtime_values, social_values = prepare_named_secrets._split_env(source_env)
    runtime_values = prepare_named_secrets._apply_runtime_overrides(
        runtime_values,
        disabled=False,
    )
    fingerprint_files = [
        repo_root / "trr_backend" / "modal_jobs.py",
        repo_root / "trr_backend" / "modal_dispatch.py",
        repo_root / "trr_backend" / "repositories" / "social_sync_orchestrator.py",
        repo_root / "trr_backend" / "socials" / "social_season_analytics_impl.py",
        repo_root / "trr_backend" / "socials" / "pipelines" / "comments" / "instagram.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "auth_resolver.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "scrapling_session.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "comments_scrapling" / "session.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "comments_scrapling" / "fetcher.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "comments_scrapling" / "job_runner.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "comments_scrapling" / "persistence.py",
        repo_root / "requirements.txt",
        repo_root / "requirements.lock.txt",
    ]
    payload = {
        "runtime_values": runtime_values,
        "social_values": social_values,
        "app_name": os.getenv("TRR_MODAL_APP_NAME") or "trr-backend-jobs",
        "runtime_secret_name": os.getenv("TRR_MODAL_RUNTIME_SECRET_NAME") or "trr-backend-runtime",
        "social_secret_name": os.getenv("TRR_MODAL_SOCIAL_SECRET_NAME") or "trr-social-auth",
        "files": {
            str(path.relative_to(repo_root)): path.read_text(encoding="utf-8")
            for path in fingerprint_files
            if path.is_file()
        },
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def load_saved_fingerprint(repo_root: Path = REPO_ROOT) -> str | None:
    path = fingerprint_path(repo_root)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    fingerprint = payload.get("fingerprint")
    return fingerprint if isinstance(fingerprint, str) and fingerprint else None


def save_fingerprint(fingerprint: str, repo_root: Path = REPO_ROOT) -> None:
    path = fingerprint_path(repo_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"fingerprint": fingerprint}, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def verify_readiness(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    runtime_secret_name = str(os.getenv("TRR_MODAL_RUNTIME_SECRET_NAME") or "trr-backend-runtime").strip()
    social_secret_name = str(os.getenv("TRR_MODAL_SOCIAL_SECRET_NAME") or "trr-social-auth").strip()
    return verify_modal_readiness.verify_modal_readiness(
        app_name=str(os.getenv("TRR_MODAL_APP_NAME") or "trr-backend-jobs").strip() or "trr-backend-jobs",
        runtime_secret_name=runtime_secret_name or "trr-backend-runtime",
        social_secret_name=social_secret_name or "trr-social-auth",
        function_names=verify_modal_readiness.expected_function_names(),
        modal_environment=str(os.getenv("MODAL_ENVIRONMENT") or "").strip(),
        probe_remote_auth_platform="instagram",
        # Getty transport remains advisory within verify_modal_readiness. Keep
        # probing it so runtime reconcile/status output preserves the nested
        # transport diagnostics without turning challenge-page noise into a
        # startup outage.
        probe_getty_remote_access=True,
    )


def verify_readiness_after_deploy(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    attempts = post_deploy_verify_attempts()
    delay_seconds = post_deploy_verify_delay_seconds()
    refreshed: dict[str, Any] = {}
    for attempt in range(attempts):
        refreshed = verify_readiness(repo_root)
        if refreshed.get("ok"):
            return refreshed
        if attempt < attempts - 1 and delay_seconds > 0:
            time.sleep(delay_seconds)
    return refreshed


def apply_named_secrets(repo_root: Path = REPO_ROOT) -> subprocess.CompletedProcess[str]:
    python_cmd = prepare_named_secrets._python_command()
    return subprocess.run(
        [
            python_cmd,
            "scripts/modal/prepare_named_secrets.py",
            "--apply",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
        timeout=command_timeout_seconds(),
    )


def deploy_modal_app(repo_root: Path = REPO_ROOT) -> subprocess.CompletedProcess[str]:
    python_cmd = prepare_named_secrets._python_command()
    return subprocess.run(
        [python_cmd, "-m", "modal", "deploy", "-m", "trr_backend.modal_jobs"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
        timeout=command_timeout_seconds(),
    )


def reconcile_modal_runtime(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    modal_disabled = (
        os.getenv("WORKSPACE_TRR_MODAL_ENABLED", "1") != "1"
        or os.getenv("WORKSPACE_TRR_REMOTE_EXECUTOR", "modal") != "modal"
    )
    if modal_disabled:
        payload = default_result()
        payload["state"] = "skipped"
        payload["reason"] = "modal_disabled"
        return payload

    readiness = verify_readiness(repo_root)
    current_fingerprint = build_modal_fingerprint(repo_root)
    saved_fingerprint = load_saved_fingerprint(repo_root)
    fingerprint_changed = current_fingerprint != saved_fingerprint

    if readiness.get("ok") and not fingerprint_changed:
        payload = default_result()
        payload["readiness"] = readiness
        return payload

    if os.getenv("WORKSPACE_RUNTIME_MODAL_AUTO_DEPLOY", "1") != "1":
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_verify_failed"
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = readiness
        payload["remediation"] = "Modal auto-deploy is disabled, but readiness drift was detected."
        return payload

    try:
        secrets_completed = apply_named_secrets(repo_root)
    except subprocess.TimeoutExpired:
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_secret_apply_failed"
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = readiness
        payload["remediation"] = f"Timed out while applying Modal secrets after {command_timeout_seconds()} seconds."
        return payload
    if secrets_completed.returncode != 0:
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_secret_apply_failed"
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = readiness
        payload["remediation"] = (
            secrets_completed.stderr or secrets_completed.stdout or "Failed to apply Modal secrets"
        ).strip()
        return payload

    try:
        deploy_completed = deploy_modal_app(repo_root)
    except subprocess.TimeoutExpired:
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_deploy_failed"
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = readiness
        payload["remediation"] = f"Timed out while deploying Modal app after {command_timeout_seconds()} seconds."
        return payload
    if deploy_completed.returncode != 0:
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_deploy_failed"
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = readiness
        payload["remediation"] = (deploy_completed.stderr or deploy_completed.stdout or "Modal deploy failed").strip()
        return payload

    refreshed = verify_readiness_after_deploy(repo_root)
    if not refreshed.get("ok"):
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_verify_failed"
        payload["deployed"] = True
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = refreshed
        payload["remediation"] = "Modal deploy completed but readiness checks still failed."
        return payload

    save_fingerprint(current_fingerprint, repo_root)
    payload = default_result()
    payload["state"] = "fixed"
    payload["deployed"] = True
    payload["fingerprint_changed"] = fingerprint_changed
    payload["readiness"] = refreshed
    return payload


def main() -> int:
    emit_json = "--json" in sys.argv[1:]
    try:
        result = reconcile_modal_runtime()
    except Exception as exc:  # noqa: BLE001
        result = default_result()
        result["state"] = "blocked"
        result["reason"] = "modal_verify_failed"
        result["remediation"] = str(exc)
    if emit_json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(json.dumps(result, sort_keys=True))
    return 1 if result["state"] == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
