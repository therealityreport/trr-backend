#!/usr/bin/env python3
"""Render or apply the named Modal secrets used by the TRR backend job plane."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.modal.deploy_backend import pinned_modal_env  # noqa: E402

DEFAULT_SOURCE_ENV = REPO_ROOT / ".env"
DEFAULT_OUTPUT_DIR = REPO_ROOT / ".artifacts" / "modal-secrets"
DEFAULT_RUNTIME_SECRET = "trr-backend-runtime"
DEFAULT_SOCIAL_SECRET = "trr-social-auth"
DEFAULT_SOCIALBLADE_COOKIE_FILE = REPO_ROOT / "scripts" / "socials" / "socialblade" / "socialblade_cookies.json"
SOCIALBLADE_REQUIRED_COOKIE_NAMES = ("cf_clearance", "session")
CANONICAL_DB_ENV = "TRR_DB_URL"
RETIRED_DB_ENV_NAMES = ("SUPABASE_DB_URL", "DATABASE_URL")
REMOTE_RUNTIME_EXCLUDED_ENV_NAMES = ("TRR_DB_DIRECT_URL",)
CANONICAL_REMOTE_RUNTIME_OVERRIDES = {
    "TRR_DB_POOL_MINCONN": "1",
    "TRR_DB_POOL_MAXCONN": "2",
    "TRR_SOCIAL_PROFILE_DB_POOL_MINCONN": "1",
    "TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN": "1",
    "TRR_SOCIAL_CONTROL_DB_POOL_MINCONN": "1",
    "TRR_SOCIAL_CONTROL_DB_POOL_MAXCONN": "1",
    "TRR_SOCIAL_PROGRESS_DB_POOL_MINCONN": "1",
    "TRR_SOCIAL_PROGRESS_DB_POOL_MAXCONN": "1",
    "TRR_HEALTH_DB_POOL_MINCONN": "1",
    "TRR_HEALTH_DB_POOL_MAXCONN": "1",
    "TRR_DB_POOL_CLOSE_AFTER_RETURN": "1",
    "TRR_DB_POOL_ACQUIRE_ATTEMPTS": "30",
    "TRR_DB_POOL_ACQUIRE_SLEEP_MS": "200",
    "TRR_JOB_PLANE_MODE": "remote",
    "TRR_LONG_JOB_ENFORCE_REMOTE": "1",
    "TRR_REMOTE_EXECUTOR": "modal",
    "TRR_MODAL_ENABLED": "1",
    "TRR_MODAL_APP_NAME": "trr-backend-jobs",
    "TRR_MODAL_API_FUNCTION": "serve_backend_api",
    "TRR_MODAL_API_LABEL": "trr-backend-api",
    "TRR_MODAL_ADMIN_OPERATION_FUNCTION": "run_admin_operation_v2",
    "TRR_MODAL_GOOGLE_NEWS_FUNCTION": "run_google_news_sync",
    "TRR_MODAL_REDDIT_REFRESH_FUNCTION": "run_reddit_refresh",
    "TRR_MODAL_SOCIAL_JOB_FUNCTION": "run_social_job",
    "TRR_MODAL_SOCIAL_POSTS_JOB_FUNCTION": "run_social_posts_job",
    "TRR_MODAL_SOCIAL_MEDIA_JOB_FUNCTION": "run_social_media_job",
    "TRR_MODAL_SOCIAL_COMMENTS_JOB_FUNCTION": "run_social_comments_job",
    "TRR_MODAL_SOCIAL_COMMENTS_RECOVERY_JOB_FUNCTION": "run_social_comments_recovery_job",
    "TRR_MODAL_SOCIAL_RECOVERY_FUNCTION": "sweep_social_dispatch_queue",
    "TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION": "probe_social_remote_auth",
    "TRR_MODAL_GETTY_REMOTE_PROBE_FUNCTION": "probe_getty_remote_access",
    "TRR_MODAL_VISION_FUNCTION": "run_admin_vision",
    "TRR_MODAL_CAST_SCREENTIME_FUNCTION": "run_cast_screentime_analysis",
    "TRR_MODAL_SOCIALBLADE_FUNCTION": "run_socialblade_scrape",
    "SOCIAL_INSTAGRAM_POSTS_USE_STICKY_PROXY": "true",
    "SOCIAL_INSTAGRAM_POSTS_ANONYMOUS_ENABLED": "false",
    "SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER": "none",
    "SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY": "true",
    "SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY": "false",
    "SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS": "600",
    "INSTAGRAM_BROWSER_NETWORK_POLICY_ENABLED": "true",
    "INSTAGRAM_BROWSER_BLOCK_STATIC_ASSETS": "true",
    "INSTAGRAM_BROWSER_DISABLE_EXTRA_RESOURCES": "true",
    "INSTAGRAM_BROWSER_NETWORK_POLICY_REPORT_ONLY": "false",
    "TRR_MODAL_MAINTENANCE_OWNER_REQUIRED": "1",
    "TRR_MODAL_RUNTIME_SECRET_NAME": DEFAULT_RUNTIME_SECRET,
    "TRR_MODAL_SOCIAL_SECRET_NAME": DEFAULT_SOCIAL_SECRET,
    "TRR_ADMIN_IMAGE_EXECUTION_BACKEND": "modal",
    "SOCIAL_QUEUE_ENABLED": "true",
}
MODAL_ALWAYS_ON_SAFE_DEFAULTS = {
    "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED": "0",
    "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED": "1",
    "TRR_MODAL_API_MIN_CONTAINERS": "0",
    "TRR_MODAL_ADMIN_KEEP_WARM": "0",
}
CANONICAL_REMOTE_SOCIAL_CAP_DEFAULTS = {
    "TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT": "8",
    "TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT": "10",
    "TRR_MODAL_SOCIAL_COMMENTS_RECOVERY_JOB_CONCURRENCY_LIMIT": "10",
    "SOCIAL_MODAL_DISPATCH_LIMIT": "25",
    "SOCIAL_WORKER_POOL_POSTS": "1",
    "SOCIAL_WORKER_POOL_COMMENTS": "10",
    "SOCIAL_WORKER_POOL_SHARED_ACCOUNT_DISCOVERY": "3",
    "SOCIAL_WORKER_POOL_SHARED_ACCOUNT_POSTS": "8",
    "SOCIAL_SHARED_ACCOUNT_POSTS_PLATFORM_CAP_INSTAGRAM": "2",
    "SOCIAL_CATALOG_RUN_IN_FLIGHT_CAP": "8",
    "SOCIAL_WORKER_POOL_MEDIA_MIRROR": "1",
    "SOCIAL_WORKER_POOL_COMMENT_MEDIA_MIRROR": "1",
    "SOCIAL_POSTS_COMMENTS_PLATFORM_CAP_INSTAGRAM": "10",
    "SOCIAL_INSTAGRAM_COMMENTS_PROFILE_SHARD_COUNT": "8",
    "SOCIAL_INSTAGRAM_COMMENTS_MAX_SHARD_COUNT": "1000",
    "SOCIAL_INSTAGRAM_COMMENTS_GLOBAL_RATE_LIMIT_MODE": "advisory",
    "SOCIAL_INSTAGRAM_COMMENTS_PER_POST_CONCURRENCY": "2",
    "SOCIAL_THREADS_POSTS_SCRAPLING_ENABLED": "true",
    "SOCIAL_THREADS_POSTS_PROXY_PROVIDER": "decodo",
    "SOCIAL_TIKTOK_COMMENT_FETCH_TIMEOUT_SECONDS": "180",
    "SOCIAL_PLATFORM_CAP_PER_ACCOUNT_SCALING": "false",
}
LOCAL_ONLY_ENV_KEYS = {
    "FIREBASE_SERVICE_ACCOUNT_FILE",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_SERVICE_ACCOUNT_FILE",
}
SHELL_RUNTIME_ENV_KEYS = {
    "DECODO_PROXY_URL",
}
DEPLOY_ONLY_ENV_KEYS = {
    "MODAL_TOKEN_ID",
    "MODAL_TOKEN_SECRET",
}
SOCIAL_AUTH_EXACT_KEYS = {
    "SOCIAL_TWITTER_BEARER_TOKEN",
    "TWITTER_BEARER_TOKEN",
    "SOCIALBLADE_EMAIL",
    "SOCIALBLADE_PASSWORD",
    "SOCIAL_INSTAGRAM_IG_WWW_CLAIM",
    "INSTAGRAM_WEB_IG_WWW_CLAIM",
    "SOCIAL_INSTAGRAM_WEB_SESSION_ID",
    "INSTAGRAM_WEB_SESSION_ID",
    "INSTAGRAM_WEB_BLOKS_VERSION_ID",
    "INSTAGRAM_WEB_X_ASBD_ID",
}
FILE_BACKED_SOCIAL_AUTH_ENV_MAP = {
    "SOCIAL_INSTAGRAM_COOKIES_FILE": "SOCIAL_INSTAGRAM_COOKIES_JSON",
    "INSTAGRAM_COOKIES_FILE": "SOCIAL_INSTAGRAM_COOKIES_JSON",
    "SOCIAL_TIKTOK_COOKIES_FILE": "SOCIAL_TIKTOK_COOKIES_JSON",
    "TIKTOK_COOKIES_FILE": "SOCIAL_TIKTOK_COOKIES_JSON",
    "SOCIAL_FACEBOOK_COOKIES_FILE": "SOCIAL_FACEBOOK_COOKIES_JSON",
    "FACEBOOK_COOKIES_FILE": "SOCIAL_FACEBOOK_COOKIES_JSON",
    "SOCIAL_THREADS_COOKIES_FILE": "SOCIAL_THREADS_COOKIES_JSON",
    "THREADS_COOKIES_FILE": "SOCIAL_THREADS_COOKIES_JSON",
    "SOCIAL_TWITTER_COOKIES_FILE": "SOCIAL_TWITTER_COOKIES_JSON",
    "TWITTER_COOKIES_FILE": "SOCIAL_TWITTER_COOKIES_JSON",
    "TWIKIT_COOKIES_FILE": "TWIKIT_COOKIES_JSON",
    "SOCIALBLADE_COOKIES_FILE": "SOCIALBLADE_COOKIES_JSON",
}


def _python_command() -> str:
    repo_venv_python = REPO_ROOT / ".venv" / "bin" / "python"
    if repo_venv_python.is_file():
        return str(repo_venv_python)
    return sys.executable or "python3.11"


def _default_source_env() -> Path:
    configured = str(os.getenv("TRR_MODAL_SOURCE_ENV") or "").strip()
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_SOURCE_ENV


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-env",
        type=Path,
        default=_default_source_env(),
        help=f"Source env file to split (default: {DEFAULT_SOURCE_ENV})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for rendered secret env files (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--runtime-secret-name",
        default=DEFAULT_RUNTIME_SECRET,
        help=f"Modal runtime secret name (default: {DEFAULT_RUNTIME_SECRET})",
    )
    parser.add_argument(
        "--social-secret-name",
        default=DEFAULT_SOCIAL_SECRET,
        help=f"Modal social-auth secret name (default: {DEFAULT_SOCIAL_SECRET})",
    )
    parser.add_argument(
        "--modal-environment",
        default="",
        help="Optional Modal environment name passed to `modal secret create --env`.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Create/update the named secrets via `python -m modal secret create --force`.",
    )
    parser.add_argument(
        "--keep-rendered-files",
        action="store_true",
        help="Keep the rendered env files after `--apply` instead of deleting them.",
    )
    parser.add_argument(
        "--no-canonical-remote-overrides",
        action="store_true",
        help="Do not inject the canonical remote+modal runtime defaults into the runtime secret.",
    )
    return parser.parse_args()


def _is_local_only_env_key(key: str) -> bool:
    normalized = key.strip().upper()
    return (
        normalized in LOCAL_ONLY_ENV_KEYS
        or normalized in DEPLOY_ONLY_ENV_KEYS
        or normalized.endswith("_FILE")
        or normalized.endswith("_NETSCAPE_FILE")
    )


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _is_social_auth_env_key(key: str) -> bool:
    normalized = key.strip().upper()
    return (
        normalized in SOCIAL_AUTH_EXACT_KEYS
        or normalized.startswith("TWIKIT_")
        or normalized.endswith("_COOKIES_JSON")
        or normalized.endswith("_COOKIES_HEADER")
    )


def _resolve_env_file_path(raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = (REPO_ROOT / candidate).resolve()
    return candidate


def _compact_secret_value(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        return stripped
    try:
        return json.dumps(json.loads(stripped), separators=(",", ":"))
    except json.JSONDecodeError:
        return " ".join(line.strip() for line in stripped.splitlines() if line.strip())


def _secret_payload_has_content(value: str | None) -> bool:
    stripped = str(value or "").strip()
    if not stripped:
        return False
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return True
    if isinstance(parsed, (list, dict, str)):
        return bool(parsed)
    return parsed is not None


def _read_non_empty_secret_file(path: Path, *, env_key: str) -> str:
    file_contents = path.read_text(encoding="utf-8").strip()
    if not file_contents:
        raise ValueError(
            f"{env_key} resolved to an empty file: {path}. Remote Modal secrets require non-empty auth payloads."
        )
    return _compact_secret_value(file_contents)


def _cookie_names_from_secret_payload(value: str) -> set[str]:
    stripped = str(value or "").strip()
    if not stripped:
        return set()
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return {
            part.split("=", 1)[0].strip()
            for part in stripped.split(";")
            if "=" in part and part.split("=", 1)[0].strip()
        }
    if isinstance(parsed, dict):
        return {str(name).strip() for name, cookie_value in parsed.items() if str(name).strip() and cookie_value}
    if isinstance(parsed, list):
        names: set[str] = set()
        for item in parsed:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            cookie_value = item.get("value")
            if name and cookie_value:
                names.add(name)
        return names
    return set()


def _validate_socialblade_cookie_secret(value: str | None) -> None:
    if not _secret_payload_has_content(value):
        return
    names = _cookie_names_from_secret_payload(str(value or ""))
    missing = [name for name in SOCIALBLADE_REQUIRED_COOKIE_NAMES if name not in names]
    if missing:
        raise ValueError(
            "SOCIALBLADE_COOKIES_JSON is missing required authenticated cookie names: "
            + ", ".join(missing)
        )


def _materialize_file_backed_social_auth(
    source_values: dict[str, str],
    social_values: dict[str, str],
) -> dict[str, str]:
    rendered = dict(social_values)
    for file_env_key, inline_env_key in FILE_BACKED_SOCIAL_AUTH_ENV_MAP.items():
        file_path = (source_values.get(file_env_key) or "").strip()
        if not file_path:
            if (rendered.get(inline_env_key) or "").strip():
                continue
            continue
        resolved_path = _resolve_env_file_path(file_path)
        if not resolved_path.is_file():
            raise FileNotFoundError(
                f"{file_env_key} points to a missing file: {resolved_path}. "
                f"Remote Modal secrets cannot use local-only *_FILE auth paths."
            )
        rendered[inline_env_key] = _read_non_empty_secret_file(resolved_path, env_key=file_env_key)
    if (
        not _secret_payload_has_content(rendered.get("SOCIALBLADE_COOKIES_JSON"))
        and DEFAULT_SOCIALBLADE_COOKIE_FILE.is_file()
    ):
        rendered["SOCIALBLADE_COOKIES_JSON"] = _read_non_empty_secret_file(
            DEFAULT_SOCIALBLADE_COOKIE_FILE,
            env_key="SOCIALBLADE_COOKIES_FILE",
        )
    _validate_socialblade_cookie_secret(rendered.get("SOCIALBLADE_COOKIES_JSON"))
    return rendered


def _load_source_env(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise FileNotFoundError(f"Source env file not found: {path}")
    loaded = dotenv_values(path)
    result: dict[str, str] = {}
    for key, value in loaded.items():
        if not key:
            continue
        normalized = str(key).strip()
        if not normalized:
            continue
        if value is None:
            continue
        rendered = str(value).strip()
        if not rendered:
            continue
        result[normalized] = rendered
    return result


def _overlay_shell_runtime_env_values(values: dict[str, str]) -> dict[str, str]:
    rendered = dict(values)
    for key in SHELL_RUNTIME_ENV_KEYS:
        if (rendered.get(key) or "").strip():
            continue
        value = str(os.getenv(key) or "").strip()
        if value:
            rendered[key] = value
    return rendered


def _split_env(values: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    runtime_values: dict[str, str] = {}
    social_values: dict[str, str] = {}
    for key, value in values.items():
        if _is_local_only_env_key(key):
            continue
        if _is_social_auth_env_key(key):
            social_values[key] = value
            continue
        runtime_values[key] = value
    return runtime_values, _materialize_file_backed_social_auth(values, social_values)


def _apply_runtime_overrides(values: dict[str, str], *, disabled: bool) -> dict[str, str]:
    merged = dict(values)
    for retired_name in (*RETIRED_DB_ENV_NAMES, *REMOTE_RUNTIME_EXCLUDED_ENV_NAMES):
        merged.pop(retired_name, None)
    canonical_db_url = (merged.get(CANONICAL_DB_ENV) or "").strip()
    if not canonical_db_url:
        raise KeyError(f"{CANONICAL_DB_ENV} is required in the source env to render the Modal runtime secret.")
    merged[CANONICAL_DB_ENV] = canonical_db_url
    if disabled:
        return merged
    merged.update(CANONICAL_REMOTE_RUNTIME_OVERRIDES)
    if not _truthy(os.getenv("WORKSPACE_ALLOW_MODAL_ALWAYS_ON_BILLING")):
        merged.update(MODAL_ALWAYS_ON_SAFE_DEFAULTS)
    for key, value in CANONICAL_REMOTE_SOCIAL_CAP_DEFAULTS.items():
        merged.setdefault(key, value)
    return merged


def _write_env_file(path: Path, values: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{key}={json.dumps(str(value))}" for key, value in sorted(values.items())]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _modal_secret_create_command(secret_name: str, env_file: Path, *, modal_environment: str) -> list[str]:
    python_cmd = _python_command()
    command = [python_cmd, "-m", "modal", "secret", "create", secret_name, "--force", "--from-dotenv", str(env_file)]
    if modal_environment:
        command.extend(["--env", modal_environment])
    return command


def _run_command(command: list[str]) -> None:
    raw_timeout = str(os.getenv("TRR_MODAL_SECRET_CREATE_TIMEOUT_SECONDS") or "120").strip()
    try:
        timeout_seconds = int(raw_timeout)
    except ValueError:
        timeout_seconds = 120
    timeout_seconds = max(10, timeout_seconds)
    subprocess.run(command, check=True, timeout=timeout_seconds, env=pinned_modal_env())


def _cleanup_rendered_files(*paths: Path) -> None:
    for path in paths:
        try:
            path.unlink(missing_ok=True)
        except TypeError:
            if path.exists():
                path.unlink()


def main() -> int:
    args = _parse_args()
    source_values = _overlay_shell_runtime_env_values(_load_source_env(args.source_env))
    runtime_values, social_values = _split_env(source_values)
    runtime_values = _apply_runtime_overrides(
        runtime_values,
        disabled=args.no_canonical_remote_overrides,
    )
    runtime_file = args.output_dir / "trr-backend-runtime.env"
    social_file = args.output_dir / "trr-social-auth.env"
    _write_env_file(runtime_file, runtime_values)
    _write_env_file(social_file, social_values)

    print(f"Source env: {args.source_env}")
    print(f"Runtime secret env file: {runtime_file} ({len(runtime_values)} keys)")
    print(f"Social auth env file: {social_file} ({len(social_values)} keys)")

    runtime_command = _modal_secret_create_command(
        args.runtime_secret_name,
        runtime_file,
        modal_environment=args.modal_environment,
    )
    social_command = _modal_secret_create_command(
        args.social_secret_name,
        social_file,
        modal_environment=args.modal_environment,
    )

    print("\nModal secret commands:")
    print("  " + shlex.join(runtime_command))
    print("  " + shlex.join(social_command))

    if args.apply:
        _run_command(runtime_command)
        _run_command(social_command)
        print("\nModal secrets updated.")
        if not args.keep_rendered_files:
            _cleanup_rendered_files(runtime_file, social_file)
            print("Rendered env files deleted after apply.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
