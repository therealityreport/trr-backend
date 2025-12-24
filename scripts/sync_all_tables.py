#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from scripts._sync_common import add_show_filter_args


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_all_tables",
        description="Run multiple table sync scripts in order.",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Comma-separated list of tables to sync (default: all).",
    )
    add_show_filter_args(parser)
    return parser.parse_args(argv)


def _build_forward_args(args: argparse.Namespace) -> list[str]:
    forwarded: list[str] = []
    if args.all:
        forwarded.append("--all")
    for show_id in args.show_id or []:
        forwarded.extend(["--show-id", str(show_id)])
    for tmdb_id in args.tmdb_show_id or []:
        forwarded.extend(["--tmdb-show-id", str(tmdb_id)])
    for imdb_id in args.imdb_series_id or []:
        forwarded.extend(["--imdb-series-id", str(imdb_id)])
    if args.limit is not None:
        forwarded.extend(["--limit", str(args.limit)])
    if args.dry_run:
        forwarded.append("--dry-run")
    if args.verbose:
        forwarded.append("--verbose")
    if args.incremental is False:
        forwarded.append("--no-incremental")
    if args.resume is False:
        forwarded.append("--no-resume")
    if args.force:
        forwarded.append("--force")
    if args.since:
        forwarded.extend(["--since", str(args.since)])
    return forwarded


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])

    from scripts import (
        sync_episodes,
        sync_episode_appearances,
        sync_people,
        sync_seasons,
        sync_show_cast,
        sync_shows,
    )

    runners = {
        "shows": sync_shows.main,
        "seasons": sync_seasons.main,
        "episodes": sync_episodes.main,
        "people": sync_people.main,
        "show_cast": sync_show_cast.main,
        "episode_appearances": sync_episode_appearances.main,
    }

    if args.tables:
        requested = [t.strip() for t in str(args.tables).split(",") if t.strip()]
        table_order = [t for t in requested if t in runners]
    else:
        table_order = [
            "shows",
            "seasons",
            "episodes",
            "people",
            "show_cast",
            "episode_appearances",
        ]

    if not table_order:
        print("No valid tables requested.")
        return 1

    forwarded = _build_forward_args(args)
    exit_code = 0
    for table in table_order:
        print(f"Running sync for {table}...")
        code = runners[table](forwarded)
        if code:
            exit_code = code
            print(f"Sync failed for {table} (exit={code}).")
            break
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
