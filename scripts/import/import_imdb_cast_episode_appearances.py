#!/usr/bin/env python3
"""Legacy convenience wrapper for syncing IMDb cast + episode presence.

The legacy write targets (`core.show_cast`, `core.episode_appearances`) were
replaced by the credits v2 model:
- `core.credits`
- `core.credit_occurrences`

This script now delegates to:
- `scripts/sync/sync_show_cast.py`
- `scripts/sync/sync_episode_appearances.py`
"""

from __future__ import annotations

import argparse
import sys

from scripts.sync import sync_episode_appearances, sync_show_cast


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="import_imdb_cast_episode_appearances.py",
        description="Sync core.credits + core.credit_occurrences from IMDb (Self only).",
    )
    parser.add_argument("--imdb-series-id", required=True, help="IMDb series id (tt...).")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and summarize without writing to Supabase.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    parser.add_argument("--limit-cast", type=int, default=None, help="Optional cap on number of cast members.")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Parallelism for IMDb episodic credits fetch (default: 4).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])

    common = ["--imdb-series-id", str(args.imdb_series_id).strip()]
    if args.dry_run:
        common.append("--dry-run")
    if args.verbose:
        common.append("--verbose")

    code = sync_show_cast.main(list(common))
    if code != 0:
        return code

    ep_args = list(common)
    ep_args.extend(["--concurrency", str(int(args.concurrency or 1))])
    if args.limit_cast is not None:
        ep_args.extend(["--limit-cast", str(int(args.limit_cast))])

    return sync_episode_appearances.main(ep_args)


if __name__ == "__main__":
    raise SystemExit(main())
