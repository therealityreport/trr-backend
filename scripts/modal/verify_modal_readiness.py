#!/usr/bin/env python3
"""Verify Modal secrets, app deployment, and function resolution for TRR cutover."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

DEFAULT_APP_NAME = "trr-backend-jobs"
DEFAULT_RUNTIME_SECRET = "trr-backend-runtime"
DEFAULT_SOCIAL_SECRET = "trr-social-auth"
DEFAULT_API_FUNCTION = "serve_backend_api"
DEFAULT_SOCIAL_AUTH_PROBE_FUNCTION = "probe_social_remote_auth"
DEFAULT_INSTAGRAM_POSTS_AUTH_PROBE_FUNCTION = "probe_instagram_posts_auth"
DEFAULT_GETTY_REMOTE_PROBE_FUNCTION = "probe_getty_remote_access"
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts._workspace_runtime_env import apply_workspace_runtime_env
from trr_backend.modal_dispatch import (
    modal_social_comments_job_function_name,
    modal_social_job_function_names,
    modal_social_job_function_name,
    modal_social_media_job_function_name,
    modal_social_posts_job_function_name,
)


def _load_get_app_objects() -> Callable[..., dict[str, Any]]:
    try:
        from modal.experimental import get_app_objects as modal_get_app_objects
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Modal experimental helpers are unavailable") from exc
    return modal_get_app_objects


def _load_modal_function_class() -> Any:
    try:
        import modal
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Modal SDK is unavailable") from exc
    function_class = getattr(modal, "Function", None)
    if function_class is None:
        raise RuntimeError("Modal Function helpers are unavailable")
    return function_class


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--app-name",
        default=str(os.getenv("TRR_MODAL_APP_NAME") or DEFAULT_APP_NAME).strip() or DEFAULT_APP_NAME,
        help=f"Modal app name to verify (default: {DEFAULT_APP_NAME})",
    )
    parser.add_argument(
        "--runtime-secret-name",
        default=(
            str(os.getenv("TRR_MODAL_RUNTIME_SECRET_NAME") or DEFAULT_RUNTIME_SECRET).strip() or DEFAULT_RUNTIME_SECRET
        ),
        help=f"Runtime secret name to verify (default: {DEFAULT_RUNTIME_SECRET})",
    )
    parser.add_argument(
        "--social-secret-name",
        default=(
            str(os.getenv("TRR_MODAL_SOCIAL_SECRET_NAME") or DEFAULT_SOCIAL_SECRET).strip() or DEFAULT_SOCIAL_SECRET
        ),
        help=f"Social auth secret name to verify (default: {DEFAULT_SOCIAL_SECRET})",
    )
    parser.add_argument(
        "--env",
        default=str(os.getenv("MODAL_ENVIRONMENT") or "").strip(),
        help="Optional Modal environment name to pass to secret/app lookups and function resolution.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the verification summary as JSON.",
    )
    parser.add_argument(
        "--probe-remote-auth",
        choices=("instagram", "tiktok", "twitter", "facebook", "threads"),
        default="",
        help="Optionally run the deployed remote auth probe for a supported platform.",
    )
    parser.add_argument(
        "--probe-instagram-posts-auth",
        default="",
        metavar="HANDLE",
        help="Optionally run the deployed Instagram profile-posts auth probe for an account handle.",
    )
    parser.add_argument(
        "--probe-getty-remote-access",
        action="store_true",
        help="Optionally run the deployed Getty remote access probe on Modal.",
    )
    parser.add_argument(
        "--strict-probes",
        action="store_true",
        help="Treat advisory probe failures as a non-zero CLI exit.",
    )
    return parser.parse_args()


def _python_command() -> str:
    repo_venv_python = os.path.join(REPO_ROOT, ".venv", "bin", "python")
    if os.path.isfile(repo_venv_python):
        return repo_venv_python
    virtual_env = str(os.getenv("VIRTUAL_ENV") or "").strip()
    if virtual_env:
        candidate = os.path.join(virtual_env, "bin", "python")
        if os.path.isfile(candidate):
            return candidate
    return sys.executable or "python3.11"


def _run_modal_json(*args: str, modal_environment: str = "") -> Any:
    command = [_python_command(), "-m", "modal", *args, "--json"]
    if modal_environment:
        command.extend(["--env", modal_environment])
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout or "[]")


def list_secret_names(*, modal_environment: str = "") -> set[str]:
    payload = _run_modal_json("secret", "list", modal_environment=modal_environment)
    names: set[str] = set()
    if not isinstance(payload, list):
        return names
    for row in payload:
        if not isinstance(row, dict):
            continue
        name = row.get("Name")
        if isinstance(name, str) and name.strip():
            names.add(name.strip())
    return names


def list_app_descriptions(*, modal_environment: str = "") -> set[str]:
    payload = _run_modal_json("app", "list", modal_environment=modal_environment)
    descriptions: set[str] = set()
    if not isinstance(payload, list):
        return descriptions
    for row in payload:
        if not isinstance(row, dict):
            continue
        description = row.get("Description")
        if isinstance(description, str) and description.strip():
            descriptions.add(description.strip())
    return descriptions


def social_jobs_enabled() -> bool:
    raw = str(os.getenv("SOCIAL_QUEUE_ENABLED") or "").strip().lower()
    if not raw:
        return True
    return raw in {"1", "true", "yes", "on"}


def required_social_function_names(*, enabled: bool | None = None) -> tuple[str, ...]:
    social_jobs_are_enabled = enabled if enabled is not None else social_jobs_enabled()
    if not social_jobs_are_enabled:
        return ()
    return (
        modal_social_job_function_name(),
        modal_social_posts_job_function_name(),
        modal_social_media_job_function_name(),
        modal_social_comments_job_function_name(),
    )


def expected_function_names() -> tuple[str, ...]:
    social_function_names = required_social_function_names()
    return (
        str(os.getenv("TRR_MODAL_API_FUNCTION") or DEFAULT_API_FUNCTION).strip() or DEFAULT_API_FUNCTION,
        str(os.getenv("TRR_MODAL_ADMIN_OPERATION_FUNCTION") or "run_admin_operation_v2").strip()
        or "run_admin_operation_v2",
        str(os.getenv("TRR_MODAL_GOOGLE_NEWS_FUNCTION") or "run_google_news_sync").strip() or "run_google_news_sync",
        str(os.getenv("TRR_MODAL_REDDIT_REFRESH_FUNCTION") or "run_reddit_refresh").strip() or "run_reddit_refresh",
        str(os.getenv("TRR_MODAL_REDDIT_RUNTIME_PROBE_FUNCTION") or "probe_reddit_refresh_runtime").strip()
        or "probe_reddit_refresh_runtime",
        str(os.getenv("TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION") or DEFAULT_SOCIAL_AUTH_PROBE_FUNCTION).strip()
        or DEFAULT_SOCIAL_AUTH_PROBE_FUNCTION,
        str(os.getenv("TRR_MODAL_INSTAGRAM_POSTS_AUTH_PROBE_FUNCTION") or DEFAULT_INSTAGRAM_POSTS_AUTH_PROBE_FUNCTION)
        .strip()
        or DEFAULT_INSTAGRAM_POSTS_AUTH_PROBE_FUNCTION,
        str(os.getenv("TRR_MODAL_GETTY_REMOTE_PROBE_FUNCTION") or DEFAULT_GETTY_REMOTE_PROBE_FUNCTION).strip()
        or DEFAULT_GETTY_REMOTE_PROBE_FUNCTION,
        str(os.getenv("TRR_MODAL_SOCIAL_RECOVERY_FUNCTION") or "sweep_social_dispatch_queue").strip()
        or "sweep_social_dispatch_queue",
        str(os.getenv("TRR_MODAL_VISION_FUNCTION") or "run_admin_vision").strip() or "run_admin_vision",
        str(os.getenv("TRR_MODAL_SOCIALBLADE_FUNCTION") or "run_socialblade_scrape").strip()
        or "run_socialblade_scrape",
        "heartbeat_remote_executors",
        *social_function_names,
    )


def api_function_name() -> str:
    return str(os.getenv("TRR_MODAL_API_FUNCTION") or DEFAULT_API_FUNCTION).strip() or DEFAULT_API_FUNCTION


def social_auth_probe_function_name() -> str:
    return (
        str(os.getenv("TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION") or DEFAULT_SOCIAL_AUTH_PROBE_FUNCTION).strip()
        or DEFAULT_SOCIAL_AUTH_PROBE_FUNCTION
    )


def instagram_posts_auth_probe_function_name() -> str:
    return (
        str(os.getenv("TRR_MODAL_INSTAGRAM_POSTS_AUTH_PROBE_FUNCTION") or DEFAULT_INSTAGRAM_POSTS_AUTH_PROBE_FUNCTION)
        .strip()
        or DEFAULT_INSTAGRAM_POSTS_AUTH_PROBE_FUNCTION
    )


def getty_remote_probe_function_name() -> str:
    return (
        str(os.getenv("TRR_MODAL_GETTY_REMOTE_PROBE_FUNCTION") or DEFAULT_GETTY_REMOTE_PROBE_FUNCTION).strip()
        or DEFAULT_GETTY_REMOTE_PROBE_FUNCTION
    )


def get_app_function_handles(*, app_name: str, modal_environment: str = "") -> dict[str, Any]:
    return _load_get_app_objects()(
        app_name,
        environment_name=modal_environment or None,
    )


def get_named_function_handles(
    *,
    app_name: str,
    function_names: tuple[str, ...] | list[str],
    modal_environment: str = "",
) -> dict[str, Any]:
    function_class = _load_modal_function_class()
    handles: dict[str, Any] = {}
    for function_name in function_names:
        handles[function_name] = function_class.from_name(
            app_name,
            function_name,
            environment_name=modal_environment or None,
        )
    return handles


def hydrate_function_handle(function_handle: Any) -> tuple[bool, str | None]:
    try:
        function_handle.hydrate()
        return True, None
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def resolve_function_web_url_from_handle(
    *,
    function_handle: Any,
) -> tuple[str | None, str | None]:
    try:
        function_handle.hydrate()
        url = str(function_handle.get_web_url() or "").strip() or None
        if not url:
            return None, "web_url_missing"
        return url, None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def invoke_remote_auth_probe(
    *,
    function_handle: Any,
    platform: str,
) -> dict[str, Any]:
    try:
        payload = function_handle.remote(platform)
    except Exception as exc:  # noqa: BLE001
        return {
            "platform": platform,
            "ready": False,
            "reason": "probe_invocation_failed",
            "detail": {
                "phase": "remote_probe",
                "exception_class": type(exc).__name__,
                "message": str(exc)[:240],
            },
        }
    if not isinstance(payload, dict):
        return {
            "platform": platform,
            "ready": False,
            "reason": "probe_payload_invalid",
            "detail": {
                "phase": "remote_probe",
                "message": f"Expected dict payload, got {type(payload).__name__}",
            },
        }
    return dict(payload)


def verify_modal_readiness(
    *,
    app_name: str,
    runtime_secret_name: str,
    social_secret_name: str,
    function_names: tuple[str, ...] | list[str],
    modal_environment: str = "",
    probe_remote_auth_platform: str | None = None,
    probe_instagram_posts_auth_handle: str | None = None,
    probe_getty_remote_access: bool = False,
) -> dict[str, Any]:
    social_jobs_are_enabled = social_jobs_enabled()
    secret_names = list_secret_names(modal_environment=modal_environment)
    app_descriptions = list_app_descriptions(modal_environment=modal_environment)
    app_function_handles: dict[str, Any] = {}
    app_lookup_error: str | None = None
    missing_secrets = [
        secret_name
        for secret_name in (runtime_secret_name, social_secret_name)
        if secret_name and secret_name not in secret_names
    ]
    app_found = app_name in app_descriptions
    if app_found:
        try:
            app_function_handles = get_app_function_handles(
                app_name=app_name,
                modal_environment=modal_environment,
            )
        except Exception:  # noqa: BLE001
            try:
                app_function_handles = get_named_function_handles(
                    app_name=app_name,
                    function_names=function_names,
                    modal_environment=modal_environment,
                )
            except Exception as fallback_exc:  # noqa: BLE001
                app_function_handles = {}
                app_lookup_error = str(fallback_exc)

    function_results: list[dict[str, Any]] = []
    missing_functions: list[str] = []
    resolved_function_names: set[str] = set()
    for function_name in function_names:
        function_handle = app_function_handles.get(function_name)
        if function_handle is None:
            ok = False
            error = "Function missing"
        else:
            ok, error = hydrate_function_handle(function_handle)
        if not ok:
            missing_functions.append(function_name)
        else:
            resolved_function_names.add(function_name)
        function_results.append(
            {
                "name": function_name,
                "resolved": ok,
                "error": error,
            }
        )

    required_social_functions = list(required_social_function_names(enabled=social_jobs_are_enabled))
    missing_required_social_functions = [
        function_name
        for function_name in required_social_functions
        if function_name and function_name not in resolved_function_names
    ]

    api_name = api_function_name()
    api_web_url: str | None = None
    missing_web_endpoints: list[str] = []
    if app_found and api_name in function_names and api_name not in missing_functions:
        api_web_url, web_error = resolve_function_web_url_from_handle(
            function_handle=app_function_handles[api_name],
        )
        if web_error:
            missing_web_endpoints.append(api_name)

    remote_auth_probe: dict[str, Any] | None = None
    if probe_remote_auth_platform:
        probe_function_name = social_auth_probe_function_name()
        probe_handle = app_function_handles.get(probe_function_name)
        if probe_handle is None or probe_function_name in missing_functions:
            remote_auth_probe = {
                "platform": probe_remote_auth_platform,
                "ready": False,
                "reason": "probe_function_unavailable",
            }
        else:
            remote_auth_probe = invoke_remote_auth_probe(
                function_handle=probe_handle,
                platform=probe_remote_auth_platform,
            )

    instagram_posts_auth_probe: dict[str, Any] | None = None
    if probe_instagram_posts_auth_handle:
        probe_function_name = instagram_posts_auth_probe_function_name()
        probe_handle = app_function_handles.get(probe_function_name)
        if probe_handle is None or probe_function_name in missing_functions:
            instagram_posts_auth_probe = {
                "platform": "instagram",
                "account_handle": probe_instagram_posts_auth_handle,
                "ready": False,
                "reason": "probe_function_unavailable",
                "execution_backend": "modal",
            }
        else:
            try:
                payload = probe_handle.remote(probe_instagram_posts_auth_handle)
            except Exception as exc:  # noqa: BLE001
                instagram_posts_auth_probe = {
                    "platform": "instagram",
                    "account_handle": probe_instagram_posts_auth_handle,
                    "ready": False,
                    "reason": "probe_invocation_failed",
                    "execution_backend": "modal",
                    "detail": {
                        "phase": "posts_auth_probe",
                        "exception_class": type(exc).__name__,
                        "message": str(exc)[:240],
                    },
                }
            else:
                instagram_posts_auth_probe = (
                    dict(payload)
                    if isinstance(payload, dict)
                    else {
                        "platform": "instagram",
                        "account_handle": probe_instagram_posts_auth_handle,
                        "ready": False,
                        "reason": "probe_payload_invalid",
                        "execution_backend": "modal",
                    }
                )

    getty_remote_probe: dict[str, Any] | None = None
    if probe_getty_remote_access:
        probe_function_name = getty_remote_probe_function_name()
        probe_handle = app_function_handles.get(probe_function_name)
        if probe_handle is None or probe_function_name in missing_functions:
            getty_remote_probe = {
                "platform": "getty",
                "ready": False,
                "reason": "probe_function_unavailable",
            }
        else:
            try:
                payload = probe_handle.remote()
            except Exception as exc:  # noqa: BLE001
                getty_remote_probe = {
                    "platform": "getty",
                    "ready": False,
                    "reason": "probe_invocation_failed",
                    "detail": {
                        "phase": "remote_probe",
                        "exception_class": type(exc).__name__,
                        "message": str(exc)[:240],
                    },
                }
            else:
                getty_remote_probe = dict(payload) if isinstance(payload, dict) else {
                    "platform": "getty",
                    "ready": False,
                    "reason": "probe_payload_invalid",
                }

    blocking_probe_failures: list[str] = []
    advisory_probe_failures: list[str] = []
    if remote_auth_probe is not None and not bool(remote_auth_probe.get("ready")):
        blocking_probe_failures.append(
            str(remote_auth_probe.get("reason") or "remote_auth_probe_failed").strip() or "remote_auth_probe_failed"
        )
    if instagram_posts_auth_probe is not None and not bool(instagram_posts_auth_probe.get("ready")):
        blocking_probe_failures.append(
            str(instagram_posts_auth_probe.get("reason") or "instagram_posts_auth_probe_failed").strip()
            or "instagram_posts_auth_probe_failed"
        )
    if getty_remote_probe is not None and not bool(getty_remote_probe.get("ready")):
        advisory_probe_failures.append(
            str(getty_remote_probe.get("reason") or "getty_remote_probe_failed").strip() or "getty_remote_probe_failed"
        )

    core_ok = (
        app_found
        and not missing_secrets
        and not missing_functions
        and not missing_required_social_functions
        and not missing_web_endpoints
        and not blocking_probe_failures
    )

    return {
        "ok": core_ok,
        "core_ok": core_ok,
        "modal_environment": modal_environment or None,
        "app_name": app_name,
        "app_found": app_found,
        "app_lookup_error": app_lookup_error,
        "runtime_secret_name": runtime_secret_name,
        "social_secret_name": social_secret_name,
        "social_jobs_enabled": social_jobs_are_enabled,
        "configured_social_function_names": modal_social_job_function_names(),
        "required_social_function_names": required_social_functions,
        "missing_secrets": missing_secrets,
        "function_results": function_results,
        "missing_functions": missing_functions,
        "missing_required_social_functions": missing_required_social_functions,
        "api_function_name": api_name,
        "api_web_url": api_web_url,
        "missing_web_endpoints": missing_web_endpoints,
        "remote_auth_probe": remote_auth_probe,
        "instagram_posts_auth_probe": instagram_posts_auth_probe,
        "getty_remote_probe": getty_remote_probe,
        "blocking_probe_failures": blocking_probe_failures,
        "advisory_probe_failures": advisory_probe_failures,
    }


def _print_text_summary(summary: dict[str, Any]) -> None:
    print("Modal readiness summary")
    print(f"  App: {summary['app_name']}")
    print(f"  Environment: {summary['modal_environment'] or 'default'}")
    print(f"  App deployed: {'yes' if summary['app_found'] else 'no'}")
    if summary.get("app_lookup_error"):
        print(f"  App lookup error: {summary['app_lookup_error']}")
    print(f"  Named secrets: {summary['runtime_secret_name']}, {summary['social_secret_name']}")
    print(f"  Social jobs enabled: {'yes' if summary['social_jobs_enabled'] else 'no'}")
    print("  Required social functions: " + ", ".join(summary["required_social_function_names"]))
    print("  Configured social functions: " + ", ".join(summary["configured_social_function_names"]))
    if summary["missing_secrets"]:
        print("  Missing secrets: " + ", ".join(summary["missing_secrets"]))
    else:
        print("  Missing secrets: none")
    if summary["missing_required_social_functions"]:
        print("  Missing required social functions: " + ", ".join(summary["missing_required_social_functions"]))
    else:
        print("  Missing required social functions: none")
    print("  Function resolution:")
    for result in summary["function_results"]:
        status = "ok" if result["resolved"] else "missing"
        suffix = f" ({result['error']})" if result["error"] else ""
        print(f"    - {result['name']}: {status}{suffix}")
    print(f"  API endpoint URL: {summary['api_web_url'] or '<missing>'}")
    if summary.get("remote_auth_probe"):
        probe = summary["remote_auth_probe"]
        probe_reason = f" ({probe.get('reason')})" if probe.get("reason") else ""
        print(
            "  Remote auth probe: "
            f"{probe.get('platform')}: {'ready' if probe.get('ready') else 'not ready'}{probe_reason}"
        )
    if summary.get("instagram_posts_auth_probe"):
        probe = summary["instagram_posts_auth_probe"]
        probe_reason = f" ({probe.get('reason')})" if probe.get("reason") else ""
        print(
            "  Instagram posts auth probe: "
            f"{probe.get('account_handle')}: {'ready' if probe.get('ready') else 'not ready'}{probe_reason}"
        )
    if summary.get("getty_remote_probe"):
        probe = summary["getty_remote_probe"]
        probe_reason = f" ({probe.get('reason')})" if probe.get("reason") else ""
        print(f"  Getty remote probe: {'ready' if probe.get('ready') else 'not ready'}{probe_reason}")
    if summary.get("advisory_probe_failures"):
        print("  Advisory probe failures: " + ", ".join(summary["advisory_probe_failures"]))
    print(f"  Core ready: {'yes' if summary.get('core_ok') else 'no'}")
    print(f"  Ready: {'yes' if summary['ok'] else 'no'}")


def main() -> int:
    apply_workspace_runtime_env(repo_root=Path(REPO_ROOT))
    args = _parse_args()
    summary = verify_modal_readiness(
        app_name=args.app_name,
        runtime_secret_name=args.runtime_secret_name,
        social_secret_name=args.social_secret_name,
        function_names=expected_function_names(),
        modal_environment=args.env,
        probe_remote_auth_platform=str(args.probe_remote_auth or "").strip() or None,
        probe_instagram_posts_auth_handle=str(args.probe_instagram_posts_auth or "").strip() or None,
        probe_getty_remote_access=bool(args.probe_getty_remote_access),
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        _print_text_summary(summary)
    advisory_failures = list(summary.get("advisory_probe_failures") or [])
    return 0 if summary["ok"] and (not args.strict_probes or not advisory_failures) else 1


if __name__ == "__main__":
    raise SystemExit(main())
