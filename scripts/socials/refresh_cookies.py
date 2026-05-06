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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env()

    platform_names = list(PLATFORM_HANDLERS) if args.platform == "all" else [args.platform]
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
