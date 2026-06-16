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

from scripts.modal import deploy_backend, prepare_named_secrets, verify_modal_readiness  # noqa: E402


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
        "actions": [],
        "skipped": [],
        "readiness": None,
    }


def command_timeout_seconds() -> int:
    raw = str(os.getenv("WORKSPACE_RUNTIME_MODAL_COMMAND_TIMEOUT_SECONDS") or "300").strip()
    try:
        parsed = int(raw)
    except ValueError:
        return 300
    return max(30, parsed)


def readiness_timeout_seconds() -> int:
    raw = str(
        os.getenv("WORKSPACE_RUNTIME_MODAL_VERIFY_TIMEOUT_SECONDS")
        or os.getenv("WORKSPACE_RUNTIME_MODAL_COMMAND_TIMEOUT_SECONDS")
        or "120"
    ).strip()
    try:
        parsed = int(raw)
    except ValueError:
        return 120
    return max(10, parsed)


def readiness_remote_probe_timeout_seconds() -> int:
    raw = str(
        os.getenv("WORKSPACE_RUNTIME_MODAL_REMOTE_PROBE_TIMEOUT_SECONDS")
        or os.getenv("TRR_MODAL_REMOTE_PROBE_TIMEOUT_SECONDS")
        or "10"
    ).strip()
    try:
        parsed = int(raw)
    except ValueError:
        return 10
    return max(1, parsed)


def startup_core_worker_probes_enabled() -> bool:
    raw = str(os.getenv("WORKSPACE_RUNTIME_MODAL_PROBE_CORE_WORKERS") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


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


def _list_field(payload: dict[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    return list(value) if isinstance(value, list) else []


def _remote_auth_probe_failures(readiness: dict[str, Any]) -> list[str]:
    blocking_failures = [
        str(reason).strip() for reason in _list_field(readiness, "blocking_probe_failures") if str(reason).strip()
    ]
    probe = readiness.get("remote_auth_probe")
    if not isinstance(probe, dict) or bool(probe.get("ready")):
        return []
    reason = str(probe.get("reason") or "").strip() or "remote_auth_probe_failed"
    if blocking_failures and any(failure != reason for failure in blocking_failures):
        return []
    return blocking_failures or [reason]


def _modal_resources_ready_for_startup(readiness: dict[str, Any]) -> bool:
    if bool(readiness.get("ok")):
        return True
    if not bool(readiness.get("app_found")):
        return False
    for key in (
        "missing_secrets",
        "missing_functions",
        "missing_required_social_functions",
        "missing_web_endpoints",
    ):
        if _list_field(readiness, key):
            return False
    return bool(_remote_auth_probe_failures(readiness))


def _modal_auth_advisory(readiness: dict[str, Any]) -> dict[str, Any] | None:
    failures = _remote_auth_probe_failures(readiness)
    if bool(readiness.get("ok")) or not failures or not _modal_resources_ready_for_startup(readiness):
        return None
    return {
        "reason": "modal_auth_probe_failed",
        "failures": failures,
        "remediation": (
            "Modal resources are ready, but Instagram remote auth is blocked. "
            "Run scripts/modal/repair_instagram_auth.py after completing Instagram checkpoint/login."
        ),
    }


def modal_fingerprint_files(repo_root: Path = REPO_ROOT) -> list[Path]:
    return [
        repo_root / "trr_backend" / "modal_jobs.py",
        repo_root / "trr_backend" / "modal_dispatch.py",
        repo_root / "trr_backend" / "job_plane.py",
        repo_root / "trr_backend" / "pipeline" / "admin_operations.py",
        repo_root / "api" / "routers" / "admin_show_news.py",
        repo_root / "trr_backend" / "repositories" / "reddit_refresh.py",
        repo_root / "trr_backend" / "vision" / "people_count_engine.py",
        repo_root / "scripts" / "workers" / "admin_operations_worker.py",
        repo_root / "scripts" / "workers" / "google_news_worker.py",
        repo_root / "scripts" / "workers" / "reddit_refresh_worker.py",
        repo_root / "scripts" / "modal" / "verify_modal_readiness.py",
        repo_root / "trr_backend" / "media" / "s3_mirror.py",
        repo_root / "trr_backend" / "repositories" / "social_sync_orchestrator.py",
        repo_root / "trr_backend" / "repositories" / "socialblade_growth.py",
        repo_root / "trr_backend" / "scraping" / "url_image_scraper.py",
        repo_root / "trr_backend" / "socials" / "control_plane" / "__init__.py",
        repo_root / "trr_backend" / "socials" / "control_plane" / "dispatch.py",
        repo_root / "trr_backend" / "socials" / "control_plane" / "dispatch_runtime.py",
        repo_root / "trr_backend" / "socials" / "control_plane" / "recovery.py",
        repo_root / "trr_backend" / "socials" / "control_plane" / "run_lifecycle.py",
        repo_root / "trr_backend" / "socials" / "social_season_analytics_impl.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "auth_runtime.py",
        repo_root / "trr_backend" / "socials" / "socialblade" / "auth.py",
        repo_root / "trr_backend" / "socials" / "socialblade" / "fetcher.py",
        repo_root / "trr_backend" / "socials" / "socialblade" / "proxy.py",
        repo_root / "trr_backend" / "socials" / "socialblade" / "scraper.py",
        repo_root / "trr_backend" / "socials" / "socialblade" / "service.py",
        repo_root / "trr_backend" / "socials" / "pipelines" / "comments" / "instagram.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "auth_resolver.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "scrapling_session.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "comments_scrapling" / "session.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "comments_scrapling" / "fetcher.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "comments_scrapling" / "job_runner.py",
        repo_root / "trr_backend" / "socials" / "instagram" / "comments_scrapling" / "persistence.py",
        repo_root / "trr_backend" / "socials" / "threads" / "media_resolver.py",
        repo_root / "trr_backend" / "socials" / "tiktok" / "media_resolver.py",
        repo_root / "trr_backend" / "socials" / "tiktok" / "scraper.py",
        repo_root / "trr_backend" / "socials" / "youtube" / "media_resolver.py",
        repo_root / "requirements.txt",
        repo_root / "requirements.lock.txt",
        repo_root / "requirements.modal.lean.in",
        repo_root / "requirements.modal.lean.lock.txt",
        repo_root / "requirements.modal.browser.in",
        repo_root / "requirements.modal.browser.lock.txt",
        repo_root / "requirements.modal.vision.in",
        repo_root / "requirements.modal.vision.lock.txt",
    ]


def build_modal_fingerprint(repo_root: Path = REPO_ROOT) -> str:
    source_env = prepare_named_secrets._load_source_env(repo_root / ".env")
    runtime_values, social_values = prepare_named_secrets._split_env(source_env)
    runtime_values = prepare_named_secrets._apply_runtime_overrides(
        runtime_values,
        disabled=False,
    )
    fingerprint_files = modal_fingerprint_files(repo_root)
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
    getty_remote_enabled = str(os.getenv("TRR_GETTY_REMOTE_TRANSPORT_ENABLED") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    app_name = str(os.getenv("TRR_MODAL_APP_NAME") or "trr-backend-jobs").strip() or "trr-backend-jobs"
    modal_environment = str(os.getenv("MODAL_ENVIRONMENT") or "").strip()
    command = [
        prepare_named_secrets._python_command(),
        "scripts/modal/verify_modal_readiness.py",
        "--json",
        "--app-name",
        app_name,
        "--runtime-secret-name",
        runtime_secret_name or "trr-backend-runtime",
        "--social-secret-name",
        social_secret_name or "trr-social-auth",
        "--probe-remote-auth",
        "instagram",
        "--remote-probe-timeout-seconds",
        str(readiness_remote_probe_timeout_seconds()),
        "--modal-lookup-timeout-seconds",
        str(
            int(
                os.getenv("TRR_MODAL_LOOKUP_TIMEOUT_SECONDS")
                or verify_modal_readiness.DEFAULT_MODAL_LOOKUP_TIMEOUT_SECONDS
            )
        ),
    ]
    if modal_environment:
        command.extend(["--env", modal_environment])
    if getty_remote_enabled:
        # Getty remote Decodo transport is optional unless explicitly enabled.
        # Do not surface stale proxy credentials as a startup readiness advisory
        # while the runtime is configured to fall back to local browser transport.
        command.append("--probe-getty-remote-access")
    if startup_core_worker_probes_enabled():
        command.append("--probe-core-workers")

    timeout_seconds = readiness_timeout_seconds()
    try:
        completed = subprocess.run(
            command,
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
            env=deploy_backend.pinned_modal_env(),
        )
    except subprocess.TimeoutExpired:
        expected_functions = list(verify_modal_readiness.expected_function_names())
        return {
            "ok": False,
            "core_ok": False,
            "app_name": app_name,
            "app_found": False,
            "app_lookup_error": f"Modal readiness verification timed out after {timeout_seconds} seconds.",
            "runtime_secret_name": runtime_secret_name or "trr-backend-runtime",
            "social_secret_name": social_secret_name or "trr-social-auth",
            "missing_secrets": [],
            "function_results": [
                {"name": function_name, "resolved": False, "error": "modal_readiness_timeout"}
                for function_name in expected_functions
            ],
            "missing_functions": expected_functions,
            "missing_required_social_functions": list(verify_modal_readiness.required_social_function_names()),
            "missing_web_endpoints": [],
            "remote_auth_probe": {
                "platform": "instagram",
                "ready": False,
                "reason": "modal_readiness_timeout",
            },
            "runtime_probes": [],
            "blocking_probe_failures": ["modal_readiness_timeout"],
            "readiness_timeout": True,
        }
    try:
        payload = json.loads((completed.stdout or "").strip() or "{}")
    except json.JSONDecodeError:
        payload = {
            "ok": False,
            "core_ok": False,
            "app_found": False,
            "app_lookup_error": (completed.stderr or completed.stdout or "Modal readiness output was invalid.").strip(),
            "blocking_probe_failures": ["modal_readiness_output_invalid"],
        }
    if not isinstance(payload, dict):
        payload = {
            "ok": False,
            "core_ok": False,
            "app_found": False,
            "app_lookup_error": "Modal readiness returned a non-object payload.",
            "blocking_probe_failures": ["modal_readiness_output_invalid"],
        }
    if completed.returncode != 0 and bool(payload.get("ok")):
        payload["ok"] = False
        payload["core_ok"] = False
    return payload


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
        env=deploy_backend.pinned_modal_env(),
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
        env=deploy_backend.pinned_modal_env(),
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
        payload["skipped"].append("modal_disabled")
        return payload

    readiness = verify_readiness(repo_root)
    current_fingerprint = build_modal_fingerprint(repo_root)
    saved_fingerprint = load_saved_fingerprint(repo_root)
    fingerprint_changed = current_fingerprint != saved_fingerprint
    fingerprint_file_count = sum(1 for path in modal_fingerprint_files(repo_root) if path.is_file())
    startup_resources_ready = _modal_resources_ready_for_startup(readiness)
    startup_auth_advisory = _modal_auth_advisory(readiness)

    if bool(readiness.get("readiness_timeout")):
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_verify_timeout"
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = readiness
        payload["fingerprint_file_count"] = fingerprint_file_count
        payload["remediation"] = str(
            readiness.get("app_lookup_error")
            or f"Modal readiness verification timed out after {readiness_timeout_seconds()} seconds."
        )
        return payload

    if startup_resources_ready and not fingerprint_changed:
        payload = default_result()
        if startup_auth_advisory:
            payload["state"] = "advisory"
            payload["reason"] = startup_auth_advisory["reason"]
            payload["remediation"] = startup_auth_advisory["remediation"]
            payload["auth_advisory_failures"] = startup_auth_advisory["failures"]
            payload["skipped"].append("deploy_not_needed_auth_only_advisory")
        else:
            payload["skipped"].append("deploy_not_needed_readiness_ok_fingerprint_unchanged")
        payload["readiness"] = readiness
        payload["fingerprint_file_count"] = fingerprint_file_count
        return payload

    if os.getenv("WORKSPACE_RUNTIME_MODAL_AUTO_DEPLOY", "1") != "1":
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_verify_failed"
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = readiness
        payload["fingerprint_file_count"] = fingerprint_file_count
        payload["skipped"].append("auto_deploy_disabled")
        payload["remediation"] = "Modal auto-deploy is disabled, but readiness drift was detected."
        return payload

    try:
        secret_start = time.monotonic()
        secrets_completed = apply_named_secrets(repo_root)
    except subprocess.TimeoutExpired:
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_secret_apply_failed"
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = readiness
        payload["fingerprint_file_count"] = fingerprint_file_count
        payload["remediation"] = f"Timed out while applying Modal secrets after {command_timeout_seconds()} seconds."
        return payload
    payload_action_secret = {
        "name": "apply_named_secrets",
        "status": "ok" if secrets_completed.returncode == 0 else "failed",
        "elapsed_ms": int((time.monotonic() - secret_start) * 1000),
    }
    if secrets_completed.returncode != 0:
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_secret_apply_failed"
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = readiness
        payload["fingerprint_file_count"] = fingerprint_file_count
        payload["actions"].append(payload_action_secret)
        payload["remediation"] = (
            secrets_completed.stderr or secrets_completed.stdout or "Failed to apply Modal secrets"
        ).strip()
        return payload

    try:
        deploy_start = time.monotonic()
        deploy_completed = deploy_modal_app(repo_root)
    except subprocess.TimeoutExpired:
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_deploy_failed"
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = readiness
        payload["fingerprint_file_count"] = fingerprint_file_count
        payload["actions"].append(payload_action_secret)
        payload["remediation"] = f"Timed out while deploying Modal app after {command_timeout_seconds()} seconds."
        return payload
    payload_action_deploy = {
        "name": "deploy_modal_app",
        "status": "ok" if deploy_completed.returncode == 0 else "failed",
        "elapsed_ms": int((time.monotonic() - deploy_start) * 1000),
    }
    if deploy_completed.returncode != 0:
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_deploy_failed"
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = readiness
        payload["fingerprint_file_count"] = fingerprint_file_count
        payload["actions"].extend([payload_action_secret, payload_action_deploy])
        payload["remediation"] = (deploy_completed.stderr or deploy_completed.stdout or "Modal deploy failed").strip()
        return payload

    verify_start = time.monotonic()
    refreshed = verify_readiness_after_deploy(repo_root)
    payload_action_verify = {
        "name": "verify_after_deploy",
        "status": "ok" if _modal_resources_ready_for_startup(refreshed) else "failed",
        "elapsed_ms": int((time.monotonic() - verify_start) * 1000),
    }
    refreshed_startup_resources_ready = _modal_resources_ready_for_startup(refreshed)
    refreshed_auth_advisory = _modal_auth_advisory(refreshed)
    if not refreshed_startup_resources_ready:
        payload = default_result()
        payload["state"] = "blocked"
        payload["reason"] = "modal_verify_failed"
        payload["deployed"] = True
        payload["fingerprint_changed"] = fingerprint_changed
        payload["readiness"] = refreshed
        payload["fingerprint_file_count"] = fingerprint_file_count
        payload["actions"].extend([payload_action_secret, payload_action_deploy, payload_action_verify])
        payload["remediation"] = "Modal deploy completed but readiness checks still failed."
        return payload

    save_fingerprint(current_fingerprint, repo_root)
    payload = default_result()
    payload["state"] = "fixed"
    payload["deployed"] = True
    payload["fingerprint_changed"] = fingerprint_changed
    payload["readiness"] = refreshed
    payload["fingerprint_file_count"] = fingerprint_file_count
    payload["actions"].extend([payload_action_secret, payload_action_deploy, payload_action_verify])
    if refreshed_auth_advisory:
        payload["reason"] = refreshed_auth_advisory["reason"]
        payload["remediation"] = refreshed_auth_advisory["remediation"]
        payload["auth_advisory_failures"] = refreshed_auth_advisory["failures"]
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
