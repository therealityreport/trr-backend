#!/usr/bin/env python3
"""Validate and refresh social auth cookies."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Add project root to path (scripts/socials -> project root)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from trr_backend.socials.ops.cookie_refresh import PLATFORM_HANDLERS, PlatformHandlers, run_platform  # noqa: F401
from trr_backend.utils.env import load_env

INSTAGRAM_REFRESH_CONFIRMATION = "I UNDERSTAND INSTAGRAM AUTH RISK"
INSTAGRAM_REFRESH_WARNING = (
    "Instagram cookie refresh can trigger login challenges or account locks. "
    "Only run it after manually confirming the account is safe."
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate or refresh social auth cookies using canonical social ops helpers",
    )
    parser.add_argument(
        "--platform",
        choices=[*PLATFORM_HANDLERS.keys(), "all"],
        default="all",
        help="Target platform (default: all)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force cookie regeneration instead of only loading/validating current cookies",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate stored cookies without triggering auto-refresh loaders",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run cookie refresh in headed browser mode for platforms that need interactive auth",
    )
    parser.add_argument(
        "--validation-mode",
        choices=["comments_endpoint", "schema_only", "graphql_profile"],
        default="graphql_profile",
        help="Instagram validation mode for forced/headed refreshes (default: graphql_profile)",
    )
    parser.add_argument(
        "--confirm-instagram-refresh",
        default="",
        help=f"Required for Instagram --force refresh. Exact value: {INSTAGRAM_REFRESH_CONFIRMATION!r}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env()

    # Composition root: publish the late-bound legacy control-plane provider so
    # the cookie-refresh leaf's proxy attributes resolve.  Importing the leaf
    # alone leaves the provider UNCONFIGURED (by design — the leaf must stay
    # importable for --help without the monolith); the CLI is the layer allowed
    # to load it, mirroring scripts/socials/worker.py.  Idempotent once published.
    import trr_backend.socials.social_season_analytics_impl  # noqa: F401

    platform_names = list(PLATFORM_HANDLERS) if args.platform == "all" else [args.platform]
    if (
        bool(args.force)
        and not bool(args.validate_only)
        and "instagram" in platform_names
        and str(args.confirm_instagram_refresh or "").strip() != INSTAGRAM_REFRESH_CONFIRMATION
    ):
        print(
            json.dumps(
                {
                    "platform": "instagram",
                    "action": "blocked",
                    "validated": False,
                    "reason": "instagram_refresh_confirmation_required",
                    "warning_message": INSTAGRAM_REFRESH_WARNING,
                    "required_confirmation": INSTAGRAM_REFRESH_CONFIRMATION,
                },
                sort_keys=True,
            )
        )
        return 2
    exit_code = 0
    for platform_name in platform_names:
        handlers = PLATFORM_HANDLERS[platform_name]
        rc, result = run_platform(
            handlers,
            force=bool(args.force),
            validate_only=bool(args.validate_only),
            headed=bool(args.headed),
            validation_mode=str(getattr(args, "validation_mode", "graphql_profile")),
        )
        exit_code = max(exit_code, rc)
        print(json.dumps(result, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
