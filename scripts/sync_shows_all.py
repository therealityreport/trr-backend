#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

import scripts.sync_shows as sync_shows
import scripts.sync_tmdb_show_entities as sync_tmdb_show_entities
import scripts.sync_tmdb_watch_providers as sync_tmdb_watch_providers
from scripts._sync_common import add_show_filter_args
from trr_backend.db.postgrest_cache import PostgrestCacheError, reload_postgrest_schema
from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_shows_all",
        description="Sync show metadata, TMDb entities, and watch providers.",
    )
    add_show_filter_args(parser)
    parser.add_argument("--skip-s3", action="store_true", help="Skip S3 logo mirroring.")
    parser.add_argument(
        "--reload-schema-cache",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Reload PostgREST schema cache before running syncs.",
    )
    return parser.parse_args(argv)


def _build_common_args(args: argparse.Namespace) -> list[str]:
    argv: list[str] = []
    if args.all:
        argv.append("--all")
    for value in args.show_id or []:
        argv.extend(["--show-id", str(value)])
    for value in args.tmdb_show_id or []:
        argv.extend(["--tmdb-show-id", str(value)])
    for value in args.imdb_series_id or []:
        argv.extend(["--imdb-series-id", str(value)])
    if args.limit is not None:
        argv.extend(["--limit", str(int(args.limit))])
    if args.dry_run:
        argv.append("--dry-run")
    if args.verbose:
        argv.append("--verbose")
    if not args.incremental:
        argv.append("--no-incremental")
    if not args.resume:
        argv.append("--no-resume")
    if args.force:
        argv.append("--force")
    if args.since:
        argv.extend(["--since", str(args.since)])
    return argv


def _maybe_reload_schema_cache(enabled: bool) -> None:
    if not enabled:
        return
    try:
        reload_postgrest_schema()
    except PostgrestCacheError as exc:
        print(
            "WARN: Failed to reload PostgREST schema cache."
            " If you hit PGRST204 errors, run:\n"
            '  psql "$SUPABASE_DB_URL" -f scripts/db/reload_postgrest_schema.sql'
        )
        print(f"  Details: {exc}")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    _maybe_reload_schema_cache(bool(args.reload_schema_cache))

    common_args = _build_common_args(args)

    code = sync_shows.main(list(common_args))
    if code != 0:
        return code

    entity_args = list(common_args)
    if args.skip_s3:
        entity_args.append("--skip-s3")
    code = sync_tmdb_show_entities.main(entity_args)
    if code != 0:
        return code

    watch_args = list(common_args)
    if args.skip_s3:
        watch_args.append("--skip-s3")
    return sync_tmdb_watch_providers.main(watch_args)


if __name__ == "__main__":
    raise SystemExit(main())
