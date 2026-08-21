#!/usr/bin/env python3
"""List or run TikTok/Twitter/YouTube posts smoke checks.

Dry-run is safe for local tests: it prints the commands without importing
backend modules, opening network connections, or requiring credentials.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]
SMOKE_KEYS = ("remote-auth-tiktok", "tiktok-posts", "twitter-posts", "youtube-posts")


@dataclass(frozen=True)
class SmokeCommand:
    key: str
    description: str
    display_argv: tuple[str, ...]
    run_argv: tuple[str, ...]


def _parse_iso_date(parser: argparse.ArgumentParser, value: str, *, option: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        parser.error(f"{option} must be in YYYY-MM-DD format")
    raise AssertionError("unreachable")


def _resolve_youtube_window(parser: argparse.ArgumentParser, args: argparse.Namespace) -> tuple[str, str]:
    if args.youtube_days < 1:
        parser.error("--youtube-days must be at least 1")

    end = _parse_iso_date(parser, args.youtube_end, option="--youtube-end") if args.youtube_end else date.today()
    start = (
        _parse_iso_date(parser, args.youtube_start, option="--youtube-start")
        if args.youtube_start
        else end - timedelta(days=args.youtube_days)
    )
    if start > end:
        parser.error("--youtube-start must be on or before --youtube-end")
    return start.isoformat(), end.isoformat()


def _resolve_twitter_window(parser: argparse.ArgumentParser, args: argparse.Namespace) -> tuple[str, str]:
    start = _parse_iso_date(parser, args.twitter_start, option="--twitter-start") if args.twitter_start else None
    end = _parse_iso_date(parser, args.twitter_end, option="--twitter-end") if args.twitter_end else None
    if start is None and end is None:
        return _resolve_youtube_window(parser, args)
    if start is None or end is None:
        parser.error("--twitter-start and --twitter-end must be supplied together")
    if start > end:
        parser.error("--twitter-start must be on or before --twitter-end")
    return start.isoformat(), end.isoformat()


def build_smoke_commands(args: argparse.Namespace, parser: argparse.ArgumentParser) -> list[SmokeCommand]:
    youtube_start, youtube_end = _resolve_youtube_window(parser, args)
    twitter_start, twitter_end = _resolve_twitter_window(parser, args)
    youtube_keywords_arg = args.youtube_keyword or ["Bravo"]
    youtube_keywords = tuple(keyword.strip() for keyword in youtube_keywords_arg if keyword.strip())
    if not youtube_keywords:
        parser.error("at least one --youtube-keyword value is required")

    remote_auth = (
        "scripts/modal/verify_modal_readiness.py",
        "--probe-remote-auth",
        "tiktok",
        "--json",
    )
    tiktok = (
        "-m",
        "scripts.socials.tiktok.smoke_posts_scrapling",
        "--account",
        args.tiktok_account.strip().lstrip("@"),
        "--max-pages",
        str(args.tiktok_max_pages),
    )
    twitter = (
        "-m",
        "scripts.socials.twitter.scrape",
        "--query",
        args.twitter_query.strip() or f"from:{args.twitter_account.strip().lstrip('@')}",
        "--start",
        twitter_start,
        "--end",
        twitter_end,
        "--max-pages",
        str(args.twitter_max_pages),
    )
    youtube = (
        "-m",
        "scripts.socials.youtube.scrape",
        "--channel",
        args.youtube_channel.strip().lstrip("@"),
        "--keywords",
        *youtube_keywords,
        "--start",
        youtube_start,
        "--end",
        youtube_end,
        "--max-results",
        str(args.youtube_max_results),
        "--max-pages",
        str(args.youtube_max_pages),
        "--no-ytdlp-supplement",
    )

    return [
        SmokeCommand(
            key="remote-auth-tiktok",
            description="Verify TikTok auth readiness in the Modal worker plane.",
            display_argv=("python", *remote_auth),
            run_argv=(sys.executable, *remote_auth),
        ),
        SmokeCommand(
            key="tiktok-posts",
            description="Seed and run one TikTok posts Scrapling job.",
            display_argv=("python", *tiktok),
            run_argv=(sys.executable, *tiktok),
        ),
        SmokeCommand(
            key="twitter-posts",
            description="Run a bounded Twitter/X account posts search.",
            display_argv=("python", *twitter),
            run_argv=(sys.executable, *twitter),
        ),
        SmokeCommand(
            key="youtube-posts",
            description="Run a bounded YouTube channel posts scrape without comments mode.",
            display_argv=("python", *youtube),
            run_argv=(sys.executable, *youtube),
        ),
    ]


def _selected_commands(commands: Sequence[SmokeCommand], selected_keys: Sequence[str] | None) -> list[SmokeCommand]:
    if not selected_keys:
        return list(commands)
    selected = set(selected_keys)
    return [command for command in commands if command.key in selected]


def _print_commands(commands: Sequence[SmokeCommand], *, dry_run: bool) -> None:
    label = "DRY RUN" if dry_run else "RUN"
    print(f"TikTok/Twitter/YouTube posts smoke checks ({label})")
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
        description="List or run TikTok, Twitter/X, and YouTube posts smoke checks.",
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
    parser.add_argument("--tiktok-account", default="bravotv", help="TikTok account handle for the smoke job.")
    parser.add_argument("--tiktok-max-pages", type=int, default=1, help="Max TikTok pages for the smoke job.")
    parser.add_argument("--twitter-account", default="BravoTV", help="Twitter/X account handle for the posts search.")
    parser.add_argument(
        "--twitter-query",
        default="from:BravoTV",
        help="Twitter/X search query for the posts smoke.",
    )
    parser.add_argument("--twitter-start", help="Twitter/X scrape start date, YYYY-MM-DD. Defaults to YouTube window.")
    parser.add_argument("--twitter-end", help="Twitter/X scrape end date, YYYY-MM-DD. Defaults to YouTube window.")
    parser.add_argument("--twitter-max-pages", type=int, default=2, help="Max Twitter/X search pages.")
    parser.add_argument("--youtube-channel", default="bravo", help="YouTube channel handle for the smoke scrape.")
    parser.add_argument(
        "--youtube-keyword",
        action="append",
        help="YouTube keyword passed to --keywords. Repeat for multiple values. Defaults to Bravo.",
    )
    parser.add_argument("--youtube-start", help="YouTube scrape start date, YYYY-MM-DD. Defaults from --youtube-days.")
    parser.add_argument("--youtube-end", help="YouTube scrape end date, YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--youtube-days", type=int, default=30, help="Default YouTube lookback window in days.")
    parser.add_argument("--youtube-max-results", type=int, default=5, help="Max YouTube videos for the smoke scrape.")
    parser.add_argument("--youtube-max-pages", type=int, default=2, help="Max YouTube continuation pages per surface.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    commands = _selected_commands(build_smoke_commands(args, parser), args.only)
    dry_run = not args.run
    _print_commands(commands, dry_run=dry_run)
    if dry_run:
        print("\nPass --run to execute these commands from the backend root.")
        return 0
    return _run_commands(commands)


if __name__ == "__main__":
    raise SystemExit(main())
