#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from scripts._sync_common import (
    add_show_filter_args,
    build_candidates,
    extract_most_recent_episode,
    extract_show_total_seasons,
    fetch_show_rows,
    filter_show_rows_for_sync,
    load_env_and_db,
    reconcile_show_total_seasons,
)
from trr_backend.ingestion.show_importer import parse_imdb_headers_json_env, upsert_candidates_into_supabase
from trr_backend.repositories.sync_state import (
    assert_core_sync_state_table_exists,
    mark_sync_state_failed,
    mark_sync_state_in_progress,
    mark_sync_state_success,
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_episodes",
        description="Sync core.episodes from IMDb for existing shows.",
    )
    add_show_filter_args(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()
    if not args.dry_run:
        assert_core_sync_state_table_exists(db)

    show_rows = fetch_show_rows(db, args)
    if not show_rows:
        print("No shows matched the filters.")
        return 0

    filter_result = filter_show_rows_for_sync(
        db,
        show_rows,
        table_name="episodes",
        incremental=bool(args.incremental),
        resume=bool(args.resume),
        force=bool(args.force),
        since=args.since,
        check_total_seasons=True,
        verbose=bool(args.verbose),
    )
    show_rows = filter_result.selected
    if not show_rows:
        print("No shows need episodes sync.")
        return 0

    total_created = 0
    total_updated = 0
    total_skipped = 0
    failures: list[str] = []

    extra_headers = parse_imdb_headers_json_env()
    for show in show_rows:
        show_id = str(show.get("id") or "").strip()
        if not show_id:
            continue
        if not args.dry_run:
            mark_sync_state_in_progress(db, table_name="episodes", show_id=show_id)

        try:
            candidate = build_candidates([show])[0]
            result = upsert_candidates_into_supabase(
                [candidate],
                dry_run=bool(args.dry_run),
                annotate_imdb_episodic=False,
                tmdb_fetch_details=False,
                tmdb_fetch_seasons=False,
                imdb_fetch_episodes=True,
                imdb_fetch_cast=False,
                enrich_show_metadata=False,
                supabase_client=None if args.dry_run else db,
                imdb_episodic_extra_headers=extra_headers,
            )
            total_created += result.created
            total_updated += result.updated
            total_skipped += result.skipped

            if not args.dry_run:
                reconcile_show_total_seasons(
                    db,
                    show_id=show_id,
                    current_total_seasons=extract_show_total_seasons(show),
                    verbose=bool(args.verbose),
                )
                mark_sync_state_success(
                    db,
                    table_name="episodes",
                    show_id=show_id,
                    last_seen_most_recent_episode=extract_most_recent_episode(show),
                )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{show_id}: {exc}")
            if not args.dry_run:
                mark_sync_state_failed(db, table_name="episodes", show_id=show_id, error=exc)

    print(
        "EPISODES summary "
        f"created={total_created} "
        f"updated={total_updated} "
        f"skipped={total_skipped} "
        f"failures={len(failures)}"
    )
    if failures:
        for failure in failures[:10]:
            print(f"- {failure}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
