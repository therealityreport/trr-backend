#!/usr/bin/env python3
"""List or run Threads, Twitter/X, and Facebook remote-auth smoke checks."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]
PLATFORMS = ("twitter", "facebook", "threads")
SMOKE_KEYS = tuple(f"remote-auth-{platform}" for platform in PLATFORMS)


@dataclass(frozen=True)
class SmokeCommand:
    key: str
    description: str
    display_argv: tuple[str, ...]
    run_argv: tuple[str, ...]


def build_smoke_commands() -> list[SmokeCommand]:
    commands: list[SmokeCommand] = []
    for platform in PLATFORMS:
        argv = (
            "scripts/modal/verify_modal_readiness.py",
            "--probe-remote-auth",
            platform,
            "--json",
        )
        commands.append(
            SmokeCommand(
                key=f"remote-auth-{platform}",
                description=f"Verify {platform} auth readiness in the Modal worker plane.",
                display_argv=("python", *argv),
                run_argv=(sys.executable, *argv),
            )
        )
    return commands


def _selected_commands(commands: Sequence[SmokeCommand], selected_keys: Sequence[str] | None) -> list[SmokeCommand]:
    if not selected_keys:
        return list(commands)
    selected = set(selected_keys)
    return [command for command in commands if command.key in selected]


def _print_commands(commands: Sequence[SmokeCommand], *, dry_run: bool) -> None:
    label = "DRY RUN" if dry_run else "RUN"
    print(f"Threads/Twitter/Facebook remote-auth smoke checks ({label})")
    print(f"Working directory: {BACKEND_ROOT}")
    for index, command in enumerate(commands, start=1):
        print()
        print(f"{index}. {command.key}")
        print(f"   {command.description}")
        print(f"   {shlex.join(command.display_argv)}")


def _run_commands(commands: Sequence[SmokeCommand]) -> int:
    for command in commands:
        print(f"\nRunning {command.key}: {shlex.join(command.display_argv)}")
        result = subprocess.run(command.run_argv, cwd=BACKEND_ROOT, check=False)
        if result.returncode != 0:
            print(f"{command.key} failed with exit code {result.returncode}", file=sys.stderr)
            return int(result.returncode)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="List or run Threads, Twitter/X, and Facebook remote-auth smoke checks.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")
    mode.add_argument("--run", action="store_true", help="Execute the selected commands.")
    parser.add_argument(
        "--only",
        action="append",
        choices=SMOKE_KEYS,
        help="Limit to one command key. May be supplied more than once.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    commands = _selected_commands(build_smoke_commands(), args.only)
    dry_run = not args.run
    _print_commands(commands, dry_run=dry_run)
    if dry_run:
        print("\nPass --run to execute these commands from the backend root.")
        return 0
    return _run_commands(commands)


if __name__ == "__main__":
    raise SystemExit(main())
