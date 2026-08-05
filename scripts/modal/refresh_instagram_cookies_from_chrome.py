#!/usr/bin/env python3
"""Extract Instagram cookies from a Chrome profile and run staged auth actions.

By default this script validates the Chrome session in memory only. Writing
local cookie files, pushing Modal secrets, deploying Modal, and remote auth
verification are explicit stages.

Usage:
    python scripts/modal/refresh_instagram_cookies_from_chrome.py --validate-chrome-only
    python scripts/modal/refresh_instagram_cookies_from_chrome.py \
        --sync-local --confirm-instagram-refresh "I UNDERSTAND INSTAGRAM AUTH RISK"
    python scripts/modal/refresh_instagram_cookies_from_chrome.py \
        --push-to-modal --confirm-instagram-refresh "I UNDERSTAND INSTAGRAM AUTH RISK"
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.modal.deploy_backend import (  # noqa: E402
    DEFAULT_APP_REF,
    REQUIRED_MODAL_ENVIRONMENT,
    REQUIRED_MODAL_PROFILE,
    pinned_modal_env,
)
from trr_backend.modal_dispatch import get_trr_modal_function_handle  # noqa: E402

CHROME_PROFILE_DIR = Path.home() / "Library" / "Application Support" / "Google" / "Chrome"
REQUIRED_COOKIE_FIELDS = ("sessionid", "csrftoken", "ds_user_id")
INSTAGRAM_REFRESH_CONFIRMATION = "I UNDERSTAND INSTAGRAM AUTH RISK"
INSTAGRAM_REFRESH_WARNING = (
    "Instagram cookie extraction and Modal sync can propagate a challenged or unsafe session. "
    "Only run it after manually confirming the account is safe."
)
COMPATIBILITY_WARNING = (
    "Compatibility change: Chrome cookie refresh is now validation-only by default. "
    "Pass --sync-local to write local cookie files."
)

# Cookie file locations to update
COOKIE_FILE_PATHS = [
    REPO_ROOT / "data" / "instagram_cookies.json",
    REPO_ROOT / "scripts" / "socials" / "instagram" / "instagram_cookies.json",
]
INSTAGRAM_COOKIE_ENV_KEYS = {
    "SOCIAL_INSTAGRAM_COOKIES_FILE",
    "INSTAGRAM_COOKIES_FILE",
    "SOCIAL_INSTAGRAM_COOKIES_JSON",
    "INSTAGRAM_COOKIES_JSON",
}
COOKIE_EXTRACTION_DEPENDENCY_PACKAGE = "pycookiecheat"
COOKIE_EXTRACTION_DEPENDENCY_FAILURE_REASON = "cookie_extraction_dependency_missing"
COOKIE_EXTRACTION_DEPENDENCY_INSTALL_COMMAND = ".venv/bin/python -m pip install -r requirements.txt"
COOKIE_EXTRACTION_DEPENDENCY_MESSAGE = (
    "Chrome cookie extraction requires pycookiecheat. "
    f"Install backend dependencies with `{COOKIE_EXTRACTION_DEPENDENCY_INSTALL_COMMAND}` from TRR-Backend."
)


class CookieExtractionDependencyError(RuntimeError):
    """Raised when the local Chrome cookie extraction dependency is unavailable."""

    def __init__(self) -> None:
        super().__init__(COOKIE_EXTRACTION_DEPENDENCY_MESSAGE)


def _cookie_fingerprint(cookies: dict[str, str]) -> str:
    payload = json.dumps(
        sorted((str(key), str(value)) for key, value in cookies.items()),
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _find_chrome_profile(profile_name: str) -> Path:
    """Find a Chrome profile directory by its display name."""
    for entry in CHROME_PROFILE_DIR.iterdir():
        prefs_file = entry / "Preferences"
        if not prefs_file.is_file():
            continue
        try:
            prefs = json.loads(prefs_file.read_text())
            name = prefs.get("profile", {}).get("name", "")
            # Also check the account info / email
            account_info = prefs.get("account_info", [])
            emails = [a.get("email", "") for a in account_info if isinstance(a, dict)]
            if name.lower() == profile_name.lower() or profile_name.lower() in [e.lower() for e in emails]:
                return entry
        except (json.JSONDecodeError, OSError):
            continue
    raise FileNotFoundError(f"Chrome profile '{profile_name}' not found. Available profiles in {CHROME_PROFILE_DIR}")


def _extract_cookies(profile_path: Path) -> dict[str, str]:
    """Extract instagram.com cookies from a Chrome profile's cookie DB."""
    try:
        from pycookiecheat import chrome_cookies
    except ImportError as exc:
        raise CookieExtractionDependencyError() from exc

    cookie_file = profile_path / "Cookies"
    if not cookie_file.is_file():
        raise FileNotFoundError(f"Cookie database not found at {cookie_file}")

    cookies = chrome_cookies(
        "https://www.instagram.com",
        cookie_file=str(cookie_file),
    )
    return cookies


def _validate_cookies(cookies: dict[str, str]) -> tuple[bool, str]:
    """Validate that required cookie fields are present and non-empty."""
    missing = [f for f in REQUIRED_COOKIE_FIELDS if not cookies.get(f)]
    if missing:
        return False, f"missing required fields: {', '.join(missing)}"
    return True, "all required fields present"


def _validate_cookies_live(cookies: dict[str, str], *, validation_username: str) -> tuple[bool, str]:
    """Validate cookies against the same profile-posts GraphQL path used by backfill."""
    normalized_username = str(validation_username or "").strip().lstrip("@") or "bravotv"
    try:
        from trr_backend.socials.instagram.cookie_refresh import _validate_saved_cookies_via_graphql

        valid, reason = _validate_saved_cookies_via_graphql(
            cookies,
            validation_username=normalized_username,
            timeout_seconds=45,
        )
        if valid:
            return True, f"profile-posts GraphQL validation passed for @{normalized_username}"
        return False, str(reason or "graphql_validation_failed")
    except Exception as exc:
        return False, f"live validation error: {exc}"


def _write_cookies(cookies: dict[str, str]) -> list[str]:
    """Write cookies to all configured file paths."""
    written = []
    for path in COOKIE_FILE_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(cookies, indent=2) + "\n")
        written.append(str(path))
    return written


def _write_browser_session(cookies: dict[str, str], *, account_handle: str) -> list[str]:
    """Write the account-scoped browser-session cache used by the auth resolver."""
    normalized_account = str(account_handle or "").strip().lstrip("@") or "bravotv"
    from trr_backend.socials.account_browser_sessions import AccountBrowserSessionManager

    manager = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))
    paths = manager.import_bootstrapped_session(normalized_account, cookies, fallback_account_id=normalized_account)
    return [str(paths.cookie_file_path), str(paths.storage_state_path)]


def _source_env_value(source_env: Path, *keys: str) -> str | None:
    if not source_env.is_file():
        return None
    wanted = {key.strip() for key in keys if key.strip()}
    try:
        lines = source_env.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in wanted:
            continue
        normalized = value.strip().strip("\"'")
        if normalized:
            return normalized
    return None


def _browser_session_account_ids(*, validation_username: str, source_env: Path) -> list[str]:
    candidates = [
        validation_username,
        _source_env_value(
            source_env,
            "SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID",
            "SOCIAL_AUTH_INSTAGRAM_USERNAME",
            "INSTAGRAM_USERNAME",
        ),
    ]
    account_ids: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = str(candidate or "").strip().lstrip("@").lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        account_ids.append(normalized)
    return account_ids


def _python_command() -> str:
    repo_venv = REPO_ROOT / ".venv" / "bin" / "python"
    if repo_venv.is_file():
        return str(repo_venv)
    return sys.executable or "python3"


def _modal_source_env_with_cookies(cookies: dict[str, str], *, source_env: Path) -> Path:
    source_values = {
        str(key): str(value)
        for key, value in dotenv_values(source_env).items()
        if key and value is not None and str(key) not in INSTAGRAM_COOKIE_ENV_KEYS
    }
    source_values["SOCIAL_INSTAGRAM_COOKIES_JSON"] = json.dumps(cookies, separators=(",", ":"), sort_keys=True)
    temp_dir = REPO_ROOT / ".artifacts" / "modal-secrets"
    temp_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        prefix="instagram-modal-source-",
        suffix=".env",
        dir=str(temp_dir),
        delete=False,
    )
    temp_path = Path(handle.name)
    try:
        for key, value in sorted(source_values.items()):
            handle.write(f"{key}={json.dumps(str(value))}\n")
    finally:
        handle.close()
    return temp_path


def _push_to_modal(source_env: Path, *, cookies: dict[str, str] | None = None) -> tuple[bool, str]:
    """Push cookies to Modal named secrets via prepare_named_secrets.py."""
    generated_source_env: Path | None = None
    effective_source_env = source_env
    if cookies is not None:
        try:
            generated_source_env = _modal_source_env_with_cookies(cookies, source_env=source_env)
            effective_source_env = generated_source_env
        except Exception as exc:  # noqa: BLE001
            return False, f"failed to render Modal source env with validated cookies: {exc}"
    cmd = [
        _python_command(),
        str(REPO_ROOT / "scripts" / "modal" / "prepare_named_secrets.py"),
        "--source-env",
        str(effective_source_env),
        "--apply",
    ]
    try:
        subprocess.run(
            cmd, check=True, capture_output=True, cwd=REPO_ROOT, text=True, timeout=120, env=pinned_modal_env()
        )
        return True, "secrets pushed successfully"
    except subprocess.CalledProcessError as exc:
        return False, f"prepare_named_secrets failed: {exc.stderr[:200]}"
    except subprocess.TimeoutExpired:
        return False, "prepare_named_secrets timed out"
    finally:
        if generated_source_env is not None:
            try:
                generated_source_env.unlink()
            except FileNotFoundError:
                pass


def _deploy_modal() -> tuple[bool, str]:
    """Deploy Modal app to pick up new secrets."""
    cmd = [
        _python_command(),
        "-m",
        "modal",
        "deploy",
        "-m",
        DEFAULT_APP_REF,
        "--env",
        REQUIRED_MODAL_ENVIRONMENT,
    ]
    try:
        subprocess.run(
            cmd, check=True, capture_output=True, cwd=REPO_ROOT, text=True, timeout=300, env=pinned_modal_env()
        )
        return True, "modal app deployed"
    except subprocess.CalledProcessError as exc:
        return False, f"modal deploy failed: {exc.stderr[:200]}"
    except subprocess.TimeoutExpired:
        return False, "modal deploy timed out"


def _verify_remote_auth() -> tuple[bool, str]:
    """Verify Instagram auth on Modal workers via probe function."""
    try:
        fn = get_trr_modal_function_handle("probe_social_remote_auth")
        result = fn.remote(platform="instagram")
        if result.get("ready"):
            return True, "remote auth verified: ready"
        return False, f"remote auth not ready: {result.get('reason', 'unknown')}"
    except Exception as exc:
        return False, f"remote auth probe failed: {exc}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--chrome-profile",
        default="codex@thereality.report",
        help="Chrome profile display name or email (default: codex@thereality.report)",
    )
    parser.add_argument(
        "--validate-chrome-only",
        action="store_true",
        help="Validate Chrome cookies in memory only. This is the default and writes no files.",
    )
    parser.add_argument(
        "--sync-local",
        action="store_true",
        help="Write validated Chrome cookies to local cookie files after explicit confirmation.",
    )
    parser.add_argument(
        "--push-to-modal",
        action="store_true",
        help="Push refreshed cookies to Modal named secrets after validation.",
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Deploy Modal app as a separate explicit stage. Does not imply --push-to-modal.",
    )
    parser.add_argument(
        "--verify-remote",
        action="store_true",
        help="Verify remote auth as a separate explicit stage. Does not imply --deploy.",
    )
    parser.add_argument(
        "--source-env",
        type=Path,
        default=REPO_ROOT / ".env",
        help="Source .env file for Modal secrets rendering.",
    )
    parser.add_argument(
        "--skip-live-validation",
        action="store_true",
        help="Skip the live HTTP validation (schema check only).",
    )
    parser.add_argument(
        "--validation-username",
        default="bravotv",
        help="Instagram handle used for profile-posts GraphQL validation (default: bravotv).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit summary as JSON.",
    )
    parser.add_argument(
        "--confirm-instagram-refresh",
        default="",
        help=(
            "Required before syncing local cookies, pushing Modal secrets, deploying, or remote verification. "
            f"Exact value: {INSTAGRAM_REFRESH_CONFIRMATION!r}"
        ),
    )
    return parser.parse_args()


def main() -> int:
    # Pin the Modal workspace before the lazy Modal SDK import in
    # _verify_remote_auth resolves MODAL_PROFILE.
    os.environ["MODAL_PROFILE"] = REQUIRED_MODAL_PROFILE
    args = parse_args()

    side_effect_requested = bool(args.sync_local or args.push_to_modal or args.deploy or args.verify_remote)
    validation_only = bool(args.validate_chrome_only or not side_effect_requested)
    confirmation_ok = str(args.confirm_instagram_refresh or "").strip() == INSTAGRAM_REFRESH_CONFIRMATION

    summary: dict[str, Any] = {
        "ok": False,
        "mode": "validate_chrome_only" if validation_only else "staged_action",
        "profile": args.chrome_profile,
        "steps": [],
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "chrome_profile": args.chrome_profile,
        "validation_username": str(args.validation_username or "").strip().lstrip("@") or "bravotv",
        "browser_session_accounts": [],
        "cookie_fingerprint": None,
        "cookie_fingerprint_algorithm": "sha256:16",
        "pushed_cookie_fingerprint": None,
        "cookie_schema_valid": False,
        "live_validation_valid": None if args.skip_live_validation else False,
        "wrote_cookie_files": False,
        "pushed_to_modal": False,
        "deployed_modal": False,
        "verified_remote": False,
        "writes": {"requested": bool(args.sync_local), "performed": False, "files": []},
        "modal_push": {"requested": bool(args.push_to_modal), "performed": False},
        "deploy": {"requested": bool(args.deploy), "performed": False},
        "remote_verification": {"requested": bool(args.verify_remote), "performed": False},
    }

    if args.validate_chrome_only and side_effect_requested:
        summary["failure_reason"] = "incompatible_stage_flags"
        summary["warning_message"] = "Use --validate-chrome-only by itself, or omit it when running an explicit stage."
        _emit(summary, args.json)
        return 2

    if validation_only and not args.validate_chrome_only:
        summary["compatibility_warning"] = COMPATIBILITY_WARNING
    if validation_only and args.confirm_instagram_refresh and not args.sync_local:
        summary["compatibility_warning"] = COMPATIBILITY_WARNING

    if side_effect_requested and not confirmation_ok:
        summary["failure_reason"] = "instagram_refresh_confirmation_required"
        summary["warning_message"] = INSTAGRAM_REFRESH_WARNING
        summary["required_confirmation"] = INSTAGRAM_REFRESH_CONFIRMATION
        _emit(summary, args.json)
        return 2

    # Step 1: Find Chrome profile
    try:
        profile_path = _find_chrome_profile(args.chrome_profile)
        summary["steps"].append({"name": "find_profile", "status": "ok", "profile": str(profile_path)})
        summary["chrome_profile"] = str(profile_path)
    except FileNotFoundError as exc:
        summary["steps"].append({"name": "find_profile", "status": "failed", "error": str(exc)})
        summary["failure_reason"] = "chrome_profile_not_found"
        _emit(summary, args.json)
        return 1

    # Step 2: Extract cookies
    try:
        cookies = _extract_cookies(profile_path)
        summary["cookie_fingerprint"] = _cookie_fingerprint(cookies)
        summary["steps"].append(
            {
                "name": "extract_cookies",
                "status": "ok",
                "cookie_count": len(cookies),
                "has_sessionid": bool(cookies.get("sessionid")),
                "cookie_fingerprint": summary["cookie_fingerprint"],
                "cookie_fingerprint_algorithm": summary["cookie_fingerprint_algorithm"],
            }
        )
    except CookieExtractionDependencyError as exc:
        setup_error = {
            "category": "dependency_setup",
            "package": COOKIE_EXTRACTION_DEPENDENCY_PACKAGE,
            "message": str(exc),
            "install_command": COOKIE_EXTRACTION_DEPENDENCY_INSTALL_COMMAND,
        }
        summary["steps"].append(
            {
                "name": "extract_cookies",
                "status": "failed",
                "reason": COOKIE_EXTRACTION_DEPENDENCY_FAILURE_REASON,
                "setup_error": setup_error,
            }
        )
        summary["failure_reason"] = COOKIE_EXTRACTION_DEPENDENCY_FAILURE_REASON
        summary["next_action"] = COOKIE_EXTRACTION_DEPENDENCY_INSTALL_COMMAND
        summary["setup_error"] = setup_error
        _emit(summary, args.json)
        return 1
    except Exception as exc:
        summary["steps"].append({"name": "extract_cookies", "status": "failed", "error": str(exc)})
        summary["failure_reason"] = "extraction_failed"
        _emit(summary, args.json)
        return 1

    # Step 3: Schema validation
    valid, reason = _validate_cookies(cookies)
    summary["cookie_schema_valid"] = valid
    summary["steps"].append({"name": "schema_validation", "status": "ok" if valid else "failed", "reason": reason})
    if not valid:
        summary["failure_reason"] = "schema_validation_failed"
        _emit(summary, args.json)
        return 1

    # Step 4: Live validation
    if not args.skip_live_validation:
        valid, reason = _validate_cookies_live(cookies, validation_username=summary["validation_username"])
        summary["live_validation_valid"] = valid
        summary["steps"].append({"name": "live_validation", "status": "ok" if valid else "failed", "reason": reason})
        if not valid:
            summary["failure_reason"] = "live_validation_failed"
            _emit(summary, args.json)
            return 1

    # Step 5: Write cookies to disk only when explicitly requested
    if args.sync_local:
        written = _write_cookies(cookies)
        browser_session_accounts = _browser_session_account_ids(
            validation_username=summary["validation_username"],
            source_env=args.source_env,
        )
        summary["browser_session_accounts"] = browser_session_accounts
        for account_handle in browser_session_accounts:
            written.extend(_write_browser_session(cookies, account_handle=account_handle))
        summary["wrote_cookie_files"] = True
        summary["writes"] = {"requested": True, "performed": True, "files": written}
        summary["steps"].append({"name": "write_cookies", "status": "ok", "files": written})
    else:
        summary["steps"].append({"name": "write_cookies", "status": "skipped", "reason": "sync_local_not_requested"})

    # Step 6: Push to Modal
    if args.push_to_modal:
        ok, reason = _push_to_modal(args.source_env, cookies=cookies)
        summary["pushed_to_modal"] = ok
        summary["pushed_cookie_fingerprint"] = summary["cookie_fingerprint"] if ok else None
        summary["modal_push"] = {
            "requested": True,
            "performed": ok,
            "reason": reason,
            "cookie_fingerprint": summary["pushed_cookie_fingerprint"],
            "cookie_fingerprint_algorithm": summary["cookie_fingerprint_algorithm"],
        }
        summary["steps"].append(
            {
                "name": "push_to_modal",
                "status": "ok" if ok else "failed",
                "reason": reason,
                "cookie_fingerprint": summary["pushed_cookie_fingerprint"],
                "cookie_fingerprint_algorithm": summary["cookie_fingerprint_algorithm"],
            }
        )
        if not ok:
            summary["failure_reason"] = "push_to_modal_failed"
            _emit(summary, args.json)
            return 1

    # Step 7: Deploy Modal
    if args.deploy:
        ok, reason = _deploy_modal()
        summary["deployed_modal"] = ok
        summary["deploy"] = {"requested": True, "performed": ok, "reason": reason}
        summary["steps"].append({"name": "deploy_modal", "status": "ok" if ok else "failed", "reason": reason})
        if not ok:
            summary["failure_reason"] = "deploy_failed"
            _emit(summary, args.json)
            return 1

    # Step 8: Verify remote auth
    if args.verify_remote:
        ok, reason = _verify_remote_auth()
        summary["verified_remote"] = ok
        summary["remote_verification"] = {"requested": True, "performed": ok, "reason": reason}
        summary["steps"].append({"name": "verify_remote", "status": "ok" if ok else "failed", "reason": reason})
        if not ok:
            summary["failure_reason"] = "remote_verification_failed"
            _emit(summary, args.json)
            return 1

    summary["ok"] = True
    summary["failure_reason"] = None
    _emit(summary, args.json)
    return 0


def _emit(summary: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(summary, indent=2))
    else:
        status = "OK" if summary.get("ok") else "FAILED"
        print(f"Instagram cookie refresh: {status}")
        if summary.get("compatibility_warning"):
            print(f"  Warning: {summary['compatibility_warning']}")
        if summary.get("cookie_fingerprint"):
            print(
                f"  Cookie fingerprint: {summary['cookie_fingerprint']} ({summary.get('cookie_fingerprint_algorithm')})"
            )
        if summary.get("pushed_cookie_fingerprint"):
            print(
                "  Pushed cookie fingerprint: "
                f"{summary['pushed_cookie_fingerprint']} ({summary.get('cookie_fingerprint_algorithm')})"
            )
        for step in summary.get("steps", []):
            detail = step.get("reason") or step.get("error") or ""
            detail_str = f" — {detail}" if detail else ""
            print(f"  [{step['status']}] {step['name']}{detail_str}")
        if not summary.get("ok"):
            print(f"  Failure: {summary.get('failure_reason')}")


if __name__ == "__main__":
    raise SystemExit(main())
