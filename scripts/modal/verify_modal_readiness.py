#!/usr/bin/env python3
"""Verify Modal secrets, app deployment, and function resolution for TRR cutover."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Callable
from typing import Any

DEFAULT_APP_NAME = "trr-backend-jobs"
DEFAULT_RUNTIME_SECRET = "trr-backend-runtime"
DEFAULT_SOCIAL_SECRET = "trr-social-auth"
DEFAULT_API_FUNCTION = "serve_backend_api"
DEFAULT_SOCIAL_AUTH_PROBE_FUNCTION = "probe_social_remote_auth"
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


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
        choices=("instagram",),
        default="",
        help="Optionally run the deployed remote auth probe for a supported platform.",
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


def expected_function_names() -> tuple[str, ...]:
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
        str(os.getenv("TRR_MODAL_SOCIAL_JOB_FUNCTION") or "run_social_job").strip() or "run_social_job",
        str(os.getenv("TRR_MODAL_SOCIAL_RECOVERY_FUNCTION") or "sweep_social_dispatch_queue").strip()
        or "sweep_social_dispatch_queue",
        str(os.getenv("TRR_MODAL_VISION_FUNCTION") or "run_admin_vision").strip() or "run_admin_vision",
        str(os.getenv("TRR_MODAL_SOCIALBLADE_FUNCTION") or "run_socialblade_scrape").strip()
        or "run_socialblade_scrape",
        "heartbeat_remote_executors",
    )


def api_function_name() -> str:
    return str(os.getenv("TRR_MODAL_API_FUNCTION") or DEFAULT_API_FUNCTION).strip() or DEFAULT_API_FUNCTION


def social_auth_probe_function_name() -> str:
    return (
        str(os.getenv("TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION") or DEFAULT_SOCIAL_AUTH_PROBE_FUNCTION).strip()
        or DEFAULT_SOCIAL_AUTH_PROBE_FUNCTION
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
) -> dict[str, Any]:
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
    for function_name in function_names:
        function_handle = app_function_handles.get(function_name)
        if function_handle is None:
            ok = False
            error = "Function missing"
        else:
            ok, error = hydrate_function_handle(function_handle)
        if not ok:
            missing_functions.append(function_name)
        function_results.append(
            {
                "name": function_name,
                "resolved": ok,
                "error": error,
            }
        )

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

    probe_ready = True if remote_auth_probe is None else bool(remote_auth_probe.get("ready"))

    return {
        "ok": app_found and not missing_secrets and not missing_functions and not missing_web_endpoints and probe_ready,
        "modal_environment": modal_environment or None,
        "app_name": app_name,
        "app_found": app_found,
        "app_lookup_error": app_lookup_error,
        "runtime_secret_name": runtime_secret_name,
        "social_secret_name": social_secret_name,
        "missing_secrets": missing_secrets,
        "function_results": function_results,
        "missing_functions": missing_functions,
        "api_function_name": api_name,
        "api_web_url": api_web_url,
        "missing_web_endpoints": missing_web_endpoints,
        "remote_auth_probe": remote_auth_probe,
    }


def _print_text_summary(summary: dict[str, Any]) -> None:
    print("Modal readiness summary")
    print(f"  App: {summary['app_name']}")
    print(f"  Environment: {summary['modal_environment'] or 'default'}")
    print(f"  App deployed: {'yes' if summary['app_found'] else 'no'}")
    if summary.get("app_lookup_error"):
        print(f"  App lookup error: {summary['app_lookup_error']}")
    print(f"  Named secrets: {summary['runtime_secret_name']}, {summary['social_secret_name']}")
    if summary["missing_secrets"]:
        print("  Missing secrets: " + ", ".join(summary["missing_secrets"]))
    else:
        print("  Missing secrets: none")
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
    print(f"  Ready: {'yes' if summary['ok'] else 'no'}")


def main() -> int:
    args = _parse_args()
    summary = verify_modal_readiness(
        app_name=args.app_name,
        runtime_secret_name=args.runtime_secret_name,
        social_secret_name=args.social_secret_name,
        function_names=expected_function_names(),
        modal_environment=args.env,
        probe_remote_auth_platform=str(args.probe_remote_auth or "").strip() or None,
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        _print_text_summary(summary)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
