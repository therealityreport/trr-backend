#!/usr/bin/env python3
"""Extract Instagram cookies from a Chrome profile, validate, and push to Modal.

This script closes the cookie lifecycle gap: instead of requiring a full
Playwright login (repair_instagram_auth.py), it extracts cookies from an
already-logged-in Chrome profile using pycookiecheat, validates them with
a lightweight GraphQL probe, writes them to the local cookie files, and
optionally pushes to Modal via prepare_named_secrets.py --apply.

Designed to run on a cron (e.g. every 12 hours) to keep cookies fresh.

Usage:
    python scripts/modal/refresh_instagram_cookies_from_chrome.py
    python scripts/modal/refresh_instagram_cookies_from_chrome.py --push-to-modal
    python scripts/modal/refresh_instagram_cookies_from_chrome.py --push-to-modal --deploy
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
CHROME_PROFILE_DIR = Path.home() / "Library" / "Application Support" / "Google" / "Chrome"
REQUIRED_COOKIE_FIELDS = ("sessionid", "csrftoken", "ds_user_id")

# Cookie file locations to update
COOKIE_FILE_PATHS = [
    REPO_ROOT / "data" / "instagram_cookies.json",
    REPO_ROOT / "scripts" / "socials" / "instagram" / "instagram_cookies.json",
]


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
    except ImportError:
        print("ERROR: pycookiecheat not installed. Run: pip install pycookiecheat", file=sys.stderr)
        sys.exit(1)

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


def _validate_cookies_live(cookies: dict[str, str]) -> tuple[bool, str]:
    """Validate cookies by making a lightweight Instagram API request."""
    try:
        import urllib.request

        headers = {
            "Cookie": "; ".join(f"{k}={v}" for k, v in cookies.items()),
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "X-CSRFToken": cookies.get("csrftoken", ""),
            "X-IG-App-ID": "936619743392459",
        }
        req = urllib.request.Request(
            "https://www.instagram.com/api/v1/users/web_profile_info/?username=instagram",
            headers=headers,
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            status = resp.status
            if status == 200:
                return True, "live validation passed (HTTP 200)"
            return False, f"live validation failed (HTTP {status})"
    except urllib.error.HTTPError as exc:
        if exc.code == 401:
            return False, "live validation failed: unauthorized (401) — cookies are expired"
        if exc.code == 429:
            return True, "rate-limited (429) but cookies likely valid"
        return False, f"live validation failed (HTTP {exc.code})"
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


def _python_command() -> str:
    repo_venv = REPO_ROOT / ".venv" / "bin" / "python"
    if repo_venv.is_file():
        return str(repo_venv)
    return sys.executable or "python3"


def _push_to_modal(source_env: Path) -> tuple[bool, str]:
    """Push cookies to Modal named secrets via prepare_named_secrets.py."""
    cmd = [
        _python_command(),
        str(REPO_ROOT / "scripts" / "modal" / "prepare_named_secrets.py"),
        "--source-env",
        str(source_env),
        "--apply",
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, cwd=REPO_ROOT, text=True, timeout=120)
        return True, "secrets pushed successfully"
    except subprocess.CalledProcessError as exc:
        return False, f"prepare_named_secrets failed: {exc.stderr[:200]}"
    except subprocess.TimeoutExpired:
        return False, "prepare_named_secrets timed out"


def _deploy_modal() -> tuple[bool, str]:
    """Deploy Modal app to pick up new secrets."""
    cmd = [_python_command(), "-m", "modal", "deploy", "-m", "trr_backend.modal_jobs"]
    try:
        subprocess.run(cmd, check=True, capture_output=True, cwd=REPO_ROOT, text=True, timeout=300)
        return True, "modal app deployed"
    except subprocess.CalledProcessError as exc:
        return False, f"modal deploy failed: {exc.stderr[:200]}"
    except subprocess.TimeoutExpired:
        return False, "modal deploy timed out"


def _verify_remote_auth() -> tuple[bool, str]:
    """Verify Instagram auth on Modal workers via probe function."""
    try:
        import modal

        fn = modal.Function.from_name("trr-backend-jobs", "probe_social_remote_auth")
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
        "--push-to-modal",
        action="store_true",
        help="Push refreshed cookies to Modal named secrets after validation.",
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Deploy Modal app after pushing secrets (implies --push-to-modal).",
    )
    parser.add_argument(
        "--verify-remote",
        action="store_true",
        help="Verify remote auth after deploy (implies --deploy).",
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
        "--json",
        action="store_true",
        help="Emit summary as JSON.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Implied flags
    if args.verify_remote:
        args.deploy = True
    if args.deploy:
        args.push_to_modal = True

    summary: dict[str, Any] = {"ok": False, "steps": [], "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z")}

    # Step 1: Find Chrome profile
    try:
        profile_path = _find_chrome_profile(args.chrome_profile)
        summary["steps"].append({"name": "find_profile", "status": "ok", "profile": str(profile_path)})
    except FileNotFoundError as exc:
        summary["steps"].append({"name": "find_profile", "status": "failed", "error": str(exc)})
        summary["failure_reason"] = "chrome_profile_not_found"
        _emit(summary, args.json)
        return 1

    # Step 2: Extract cookies
    try:
        cookies = _extract_cookies(profile_path)
        summary["steps"].append(
            {
                "name": "extract_cookies",
                "status": "ok",
                "cookie_count": len(cookies),
                "has_sessionid": bool(cookies.get("sessionid")),
            }
        )
    except Exception as exc:
        summary["steps"].append({"name": "extract_cookies", "status": "failed", "error": str(exc)})
        summary["failure_reason"] = "extraction_failed"
        _emit(summary, args.json)
        return 1

    # Step 3: Schema validation
    valid, reason = _validate_cookies(cookies)
    summary["steps"].append({"name": "schema_validation", "status": "ok" if valid else "failed", "reason": reason})
    if not valid:
        summary["failure_reason"] = "schema_validation_failed"
        _emit(summary, args.json)
        return 1

    # Step 4: Live validation
    if not args.skip_live_validation:
        valid, reason = _validate_cookies_live(cookies)
        summary["steps"].append({"name": "live_validation", "status": "ok" if valid else "failed", "reason": reason})
        if not valid:
            summary["failure_reason"] = "live_validation_failed"
            _emit(summary, args.json)
            return 1

    # Step 5: Write cookies to disk
    written = _write_cookies(cookies)
    summary["steps"].append({"name": "write_cookies", "status": "ok", "files": written})

    # Step 6: Push to Modal
    if args.push_to_modal:
        ok, reason = _push_to_modal(args.source_env)
        summary["steps"].append({"name": "push_to_modal", "status": "ok" if ok else "failed", "reason": reason})
        if not ok:
            summary["failure_reason"] = "push_to_modal_failed"
            _emit(summary, args.json)
            return 1

    # Step 7: Deploy Modal
    if args.deploy:
        ok, reason = _deploy_modal()
        summary["steps"].append({"name": "deploy_modal", "status": "ok" if ok else "failed", "reason": reason})
        if not ok:
            summary["failure_reason"] = "deploy_failed"
            _emit(summary, args.json)
            return 1

    # Step 8: Verify remote auth
    if args.verify_remote:
        ok, reason = _verify_remote_auth()
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
        for step in summary.get("steps", []):
            detail = step.get("reason") or step.get("error") or ""
            detail_str = f" — {detail}" if detail else ""
            print(f"  [{step['status']}] {step['name']}{detail_str}")
        if not summary.get("ok"):
            print(f"  Failure: {summary.get('failure_reason')}")


if __name__ == "__main__":
    raise SystemExit(main())
