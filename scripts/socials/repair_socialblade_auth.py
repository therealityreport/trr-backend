#!/usr/bin/env python3
"""Repair SocialBlade cookies from the real Codex Chrome profile."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.modal.deploy_backend import pinned_modal_env  # noqa: E402
from trr_backend.socials.browser_cookie_refresh import write_cookie_file  # noqa: E402
from trr_backend.socials.socialblade.auth import (  # noqa: E402
    SocialBladeValidationBlockedError,
    extract_socialblade_cookies_from_chrome_profile,
    socialblade_cookie_file_path,
    socialblade_cookie_health_report,
)
from trr_backend.utils.env import load_env  # noqa: E402


def _python_command() -> str:
    repo_venv_python = REPO_ROOT / ".venv" / "bin" / "python"
    if repo_venv_python.is_file():
        return str(repo_venv_python)
    return sys.executable or "python3"


def _apply_modal_secret(source_env: Path) -> dict[str, Any]:
    command = [
        _python_command(),
        str(REPO_ROOT / "scripts" / "modal" / "prepare_named_secrets.py"),
        "--source-env",
        str(source_env),
        "--apply",
    ]
    subprocess.run(command, cwd=REPO_ROOT, env=pinned_modal_env(), check=True, timeout=180)
    return {"applied": True, "command": " ".join(command[:3] + ["...", "--apply"])}


def _ensure_repair_proxy_defaults() -> dict[str, str]:
    """Route repair validation through the SocialBlade production proxy defaults.

    Sticky Decodo usernames are disabled for this lane because the current TRR
    Decodo gateway rejects session-scoped usernames with 407. ``setdefault``
    respects an explicit env value, so ``SOCIALBLADE_PROXY_PROVIDER=`` (empty)
    still forces the unproxied visible-browser path for testing.
    """
    os.environ.setdefault("SOCIALBLADE_PROXY_PROVIDER", "decodo")
    os.environ.setdefault("SOCIALBLADE_USE_STICKY_PROXY", "false")
    return {
        "provider": os.environ.get("SOCIALBLADE_PROXY_PROVIDER", ""),
        "sticky": os.environ.get("SOCIALBLADE_USE_STICKY_PROXY", ""),
    }


def _blocked_next_action(proxy_defaults: dict[str, str]) -> str:
    provider = proxy_defaults.get("provider") or "unset"
    return " ".join(
        [
            "SocialBlade validation was blocked by Cloudflare (1020); the identical headless "
            "fetch is not retried. Recovery options:",
            f"(1) Ensure the Decodo residential proxy is active (SOCIALBLADE_PROXY_PROVIDER=decodo, "
            f"current={provider}; requires DECODO_USERNAME/PASSWORD/GATEWAY).",
            "(2) Log into SocialBlade in the codex@thereality.report Chrome profile and clear any "
            "Cloudflare challenge, then rerun.",
            "(3) If the required cookies are present and you trust this session, rerun with "
            "ALLOW_BLOCKED_VALIDATION=1 (make) or --allow-blocked-validation to push them anyway.",
        ]
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-env", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument(
        "--chrome-profile",
        default="codex@thereality.report",
        help="Chrome profile display name or email used as the cookie source",
    )
    parser.add_argument(
        "--validation-handle",
        default=os.getenv("SOCIALBLADE_VALIDATION_HANDLE") or os.getenv("ACCOUNT_HANDLE") or "",
        help="Real SocialBlade/Instagram handle used to validate the extracted cookies",
    )
    parser.add_argument("--apply-modal", action="store_true", help="Apply refreshed cookies to Modal named secrets")
    parser.add_argument(
        "--allow-blocked-validation",
        action="store_true",
        help=(
            "If live validation is blocked by Cloudflare (1020) but the required cookies are "
            "present, persist the freshly-extracted cookies and continue to the Modal secret "
            "write anyway (marks modal_applied_unvalidated). Non-1020 failures still abort."
        ),
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    return parser.parse_args()


def _emit(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    for key, value in payload.items():
        print(f"{key}: {value}")


def main() -> int:
    args = _parse_args()
    load_env()
    proxy_defaults = _ensure_repair_proxy_defaults()
    validation_handle = str(args.validation_handle or "").strip().lstrip("@").lower()
    summary: dict[str, Any] = {
        "ok": False,
        "chrome_profile": args.chrome_profile,
        "validation_handle": validation_handle or None,
        "cookie_file": str(socialblade_cookie_file_path()),
        "proxy_provider": proxy_defaults.get("provider") or None,
        "exported": False,
        "validation_bypassed": False,
        "modal_applied": False,
    }

    if not validation_handle:
        summary["reason"] = "validation_handle_required"
        summary["next_action"] = "Set ACCOUNT_HANDLE=<real-instagram-handle> or pass --validation-handle."
        _emit(summary, as_json=args.json)
        return 2

    try:
        extract_socialblade_cookies_from_chrome_profile(
            chrome_profile=args.chrome_profile,
            validation_handle=validation_handle,
            allow_visible_browser_retry=True,
        )
        summary["exported"] = True
    except SocialBladeValidationBlockedError as blocked:
        # Cookies are structurally complete; only the validation egress was 1020-blocked.
        summary["validation_blocked_reason"] = blocked.reason
        if not args.allow_blocked_validation:
            summary["reason"] = str(blocked) or "validation_blocked_by_cloudflare"
            summary["cookie_health"] = socialblade_cookie_health_report(
                validate=False,
                validation_handle=validation_handle,
            )
            summary["next_action"] = _blocked_next_action(proxy_defaults)
            _emit(summary, as_json=args.json)
            return 2
        write_cookie_file(socialblade_cookie_file_path(), blocked.cookies)
        summary["exported"] = True
        summary["validation_bypassed"] = True
    except Exception as exc:  # noqa: BLE001
        summary["reason"] = str(exc) or type(exc).__name__
        summary["cookie_health"] = socialblade_cookie_health_report(
            validate=True,
            validation_handle=validation_handle,
        )
        summary["next_action"] = _blocked_next_action(proxy_defaults)
        _emit(summary, as_json=args.json)
        return 2

    if summary["validation_bypassed"]:
        # Skip a second live scrape that would re-trigger the same Cloudflare block;
        # trust the structural (schema) health of the freshly-extracted cookies.
        health = socialblade_cookie_health_report(validate=False, validation_handle=validation_handle)
    else:
        health = socialblade_cookie_health_report(
            validate=True,
            validation_handle=validation_handle,
            allow_visible_browser_retry=True,
        )
    summary["cookie_health"] = health
    if not bool(health.get("healthy")):
        summary["reason"] = health.get("reason") or "cookie_health_failed"
        _emit(summary, as_json=args.json)
        return 3

    if args.apply_modal:
        try:
            modal_result = _apply_modal_secret(args.source_env)
            summary["modal_applied"] = True
            if summary["validation_bypassed"]:
                summary["modal_applied_unvalidated"] = True
            summary["modal_result"] = modal_result
        except Exception as exc:  # noqa: BLE001
            summary["reason"] = f"modal_apply_failed:{exc}"
            _emit(summary, as_json=args.json)
            return 4

    summary["ok"] = True
    _emit(summary, as_json=args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
