#!/usr/bin/env python3
"""Prepare separate BravoTV 2026 social post backfill commands."""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATE_START = "2026-01-01T00:00:00Z"
DEFAULT_DATE_END = "2026-12-31T23:59:59Z"
PLATFORM_DEFAULT_ACCOUNTS = {
    "tiktok": "bravotv",
    "twitter": "bravotv",
    "youtube": "bravo",
}
SUPPORTED_PLATFORMS = tuple(PLATFORM_DEFAULT_ACCOUNTS)


@dataclass(frozen=True)
class PreparedBackfillCommand:
    platform: str
    account: str
    display_argv: tuple[str, ...]
    run_argv: tuple[str, ...]


def _normalize_platforms(values: Sequence[str] | None) -> tuple[str, ...]:
    if not values:
        return SUPPORTED_PLATFORMS
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        platform = str(value or "").strip().lower()
        if platform not in PLATFORM_DEFAULT_ACCOUNTS or platform in seen:
            continue
        seen.add(platform)
        ordered.append(platform)
    return tuple(ordered)


def _account_for_platform(args: argparse.Namespace, platform: str) -> str:
    override = {
        "tiktok": args.tiktok_account,
        "twitter": args.twitter_account,
        "youtube": args.youtube_account,
    }.get(platform)
    return str(override or PLATFORM_DEFAULT_ACCOUNTS[platform]).strip().lstrip("@")


def _validate_bounded_window(*, date_start: str, date_end: str) -> None:
    try:
        start = datetime.fromisoformat(str(date_start).strip().replace("Z", "+00:00"))
        end = datetime.fromisoformat(str(date_end).strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("--date-start and --date-end must be valid ISO-8601 timestamps") from exc
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("--date-start and --date-end must include a timezone")
    if end < start:
        raise ValueError("--date-end must not be earlier than --date-start")


def build_prepared_commands(args: argparse.Namespace) -> list[PreparedBackfillCommand]:
    commands: list[PreparedBackfillCommand] = []
    for platform in _normalize_platforms(args.platform):
        account = _account_for_platform(args, platform)
        argv = (
            "scripts/socials/local_catalog_action.py",
            "--platform",
            platform,
            "--account",
            account,
            "--source-scope",
            "network",
            "--action",
            "backfill",
            "--execution-owner",
            "queue",
            "--date-start",
            args.date_start,
            "--date-end",
            args.date_end,
        )
        commands.append(
            PreparedBackfillCommand(
                platform=platform,
                account=account,
                display_argv=("python", *argv),
                run_argv=(sys.executable, *argv),
            )
        )
    return commands


def _print_commands(commands: Sequence[PreparedBackfillCommand], *, as_json: bool) -> None:
    if as_json:
        print(
            json.dumps(
                {
                    "commands": [
                        {
                            "platform": command.platform,
                            "account": command.account,
                            "command": list(command.display_argv),
                            "shell": shlex.join(command.display_argv),
                        }
                        for command in commands
                    ]
                },
                indent=2,
            )
        )
        return

    print("BravoTV 2026 posts backfill commands")
    print(f"Working directory: {BACKEND_ROOT}")
    for index, command in enumerate(commands, start=1):
        print()
        print(f"{index}. {command.platform} @{command.account}")
        print(f"   {shlex.join(command.display_argv)}")


def _run_commands(commands: Sequence[PreparedBackfillCommand]) -> int:
    for command in commands:
        print(f"\nRunning {command.platform}: {shlex.join(command.display_argv)}")
        result = subprocess.run(command.run_argv, cwd=BACKEND_ROOT, check=False)
        if result.returncode != 0:
            print(f"{command.platform} failed with exit code {result.returncode}", file=sys.stderr)
            return int(result.returncode)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare separate BravoTV 2026 TikTok, Twitter/X, and YouTube post backfills.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")
    mode.add_argument("--run", action="store_true", help="Execute the prepared commands.")
    parser.add_argument(
        "--platform",
        action="append",
        choices=SUPPORTED_PLATFORMS,
        help="Limit to one platform. Repeat for multiple platforms.",
    )
    parser.add_argument("--date-start", default=DEFAULT_DATE_START, help="Bounded backfill start timestamp.")
    parser.add_argument("--date-end", default=DEFAULT_DATE_END, help="Bounded backfill end timestamp.")
    parser.add_argument("--tiktok-account", default=PLATFORM_DEFAULT_ACCOUNTS["tiktok"])
    parser.add_argument("--twitter-account", default=PLATFORM_DEFAULT_ACCOUNTS["twitter"])
    parser.add_argument("--youtube-account", default=PLATFORM_DEFAULT_ACCOUNTS["youtube"])
    parser.add_argument("--json", action="store_true", help="Print commands as JSON.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        _validate_bounded_window(date_start=args.date_start, date_end=args.date_end)
    except ValueError as exc:
        parser.error(str(exc))
    commands = build_prepared_commands(args)
    if not commands:
        parser.error("at least one supported platform is required")
    if not args.run:
        _print_commands(commands, as_json=args.json)
        if not args.json:
            print("\nPass --run to execute these commands from the backend root.")
        return 0
    return _run_commands(commands)


if __name__ == "__main__":
    raise SystemExit(main())
