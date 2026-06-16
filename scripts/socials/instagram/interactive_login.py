#!/usr/bin/env python3
"""Open a headed Chrome browser for manual Instagram login.

Uses the codex@thereality.report Chrome profile so Instagram sees a familiar browser fingerprint.
Once you complete login (including any captchas/2FA), cookies are extracted
and saved to the local cookie file.

Usage:
    python scripts/socials/instagram/interactive_login.py
    python scripts/socials/instagram/interactive_login.py --push-to-modal
    python scripts/socials/instagram/interactive_login.py --chrome-profile "codex@thereality.report"
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

INSTAGRAM_REFRESH_CONFIRMATION = "I UNDERSTAND INSTAGRAM AUTH RISK"
INSTAGRAM_REFRESH_WARNING = (
    "Instagram interactive login can trigger login challenges or account locks. "
    "Only run it after manually confirming the account is safe."
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--chrome-profile",
        default="codex@thereality.report",
        help="Chrome profile display name or email (default: codex@thereality.report)",
    )
    parser.add_argument(
        "--cookie-file",
        default="data/instagram_cookies.json",
        help="Cookie file output path (default: data/instagram_cookies.json)",
    )
    parser.add_argument(
        "--validation-username",
        default="bravotv",
        help="Instagram handle to visit after login for session warm-up (default: bravotv)",
    )
    parser.add_argument(
        "--validation-mode",
        choices=["comments_endpoint", "schema_only", "graphql_profile"],
        default="graphql_profile",
        help="Validation mode to use after login (default: graphql_profile)",
    )
    parser.add_argument(
        "--comments-mode",
        action="store_true",
        help="Use comments-lane validation semantics without profile GraphQL probing.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Seconds to wait for login completion (default: 300)",
    )
    parser.add_argument(
        "--push-to-modal",
        action="store_true",
        help="Push cookies to Modal secrets after login.",
    )
    parser.add_argument(
        "--confirm-instagram-refresh",
        default="",
        help=f"Required to open Instagram login. Exact value: {INSTAGRAM_REFRESH_CONFIRMATION!r}",
    )
    args = parser.parse_args()

    if str(args.confirm_instagram_refresh or "").strip() != INSTAGRAM_REFRESH_CONFIRMATION:
        print(INSTAGRAM_REFRESH_WARNING, file=sys.stderr)
        print(f"Rerun with --confirm-instagram-refresh {INSTAGRAM_REFRESH_CONFIRMATION!r}", file=sys.stderr)
        return 2

    from trr_backend.socials.instagram.cookie_refresh import interactive_chrome_login

    try:
        cookies = interactive_chrome_login(
            chrome_profile_name=args.chrome_profile,
            cookie_file=args.cookie_file,
            timeout_seconds=args.timeout,
            validation_username=args.validation_username,
            validation_mode="comments_endpoint" if args.comments_mode else args.validation_mode,
        )
    except Exception as exc:
        print(f"\nLogin failed: {exc}", file=sys.stderr)
        return 1

    print(f"\nCaptured {len(cookies)} cookies:")
    for key in ("sessionid", "csrftoken", "ds_user_id"):
        value = cookies.get(key, "")
        print(f"  {key}: {value[:12]}…" if value else f"  {key}: MISSING")

    if args.push_to_modal:
        print("\nPushing to Modal secrets…")
        source_env = REPO_ROOT / ".env"
        cmd = [
            sys.executable,
            str(REPO_ROOT / "scripts" / "modal" / "prepare_named_secrets.py"),
            "--source-env",
            str(source_env),
            "--apply",
        ]
        try:
            subprocess.run(cmd, check=True, timeout=120)
            print("Modal secrets updated.")
        except Exception as exc:
            print(f"Modal push failed: {exc}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
