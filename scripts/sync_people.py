#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from scripts._sync_common import (
    add_show_filter_args,
    extract_imdb_series_id,
    extract_most_recent_episode,
    fetch_show_rows,
    filter_show_rows_for_sync,
    load_env_and_db,
)
from trr_backend.ingestion.show_importer import parse_imdb_headers_json_env
from trr_backend.integrations.imdb.fullcredits_cast_parser import fetch_fullcredits_cast, filter_self_cast_rows
from trr_backend.repositories.people import assert_core_people_table_exists, fetch_people_by_imdb_ids, insert_people
from trr_backend.repositories.show_cast import assert_core_show_cast_table_exists, upsert_show_cast
from trr_backend.repositories.sync_state import (
    assert_core_sync_state_table_exists,
    mark_sync_state_failed,
    mark_sync_state_in_progress,
    mark_sync_state_success,
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_people",
        description="Sync core.people and core.show_cast from IMDb full credits (Self only).",
    )
    add_show_filter_args(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()
    assert_core_people_table_exists(db)
    assert_core_show_cast_table_exists(db)
    if not args.dry_run:
        assert_core_sync_state_table_exists(db)

    show_rows = fetch_show_rows(db, args)
    if not show_rows:
        print("No shows matched the filters.")
        return 0

    extra_headers = parse_imdb_headers_json_env()
    cast_rows_total = 0
    cast_rows_self = 0
    failures: list[str] = []
    people_cache: dict[str, str] = {}
    people_inserted = 0
    show_cast_upserted = 0

    filter_people = filter_show_rows_for_sync(
        db,
        show_rows,
        table_name="people",
        incremental=bool(args.incremental),
        resume=bool(args.resume),
        force=bool(args.force),
        since=args.since,
        check_total_seasons=False,
        verbose=bool(args.verbose),
    )
    filter_cast = filter_show_rows_for_sync(
        db,
        show_rows,
        table_name="show_cast",
        incremental=bool(args.incremental),
        resume=bool(args.resume),
        force=bool(args.force),
        since=args.since,
        check_total_seasons=False,
        verbose=bool(args.verbose),
    )
    show_map = {str(row.get("id")): row for row in filter_people.selected if row.get("id")}
    for row in filter_cast.selected:
        show_id = str(row.get("id") or "")
        if show_id and show_id not in show_map:
            show_map[show_id] = row
    show_rows = list(show_map.values())
    if not show_rows:
        print("No shows need people sync.")
        return 0

    for show in show_rows:
        imdb_id = extract_imdb_series_id(show)
        show_id = str(show.get("id") or "")
        if not show_id or not imdb_id:
            if args.verbose:
                print(f"SKIP show id={show_id or show.get('id')} (missing imdb_series_id)")
            continue
        if not args.dry_run:
            mark_sync_state_in_progress(db, table_name="people", show_id=show_id)
            mark_sync_state_in_progress(db, table_name="show_cast", show_id=show_id)

        try:
            cast_rows = fetch_fullcredits_cast(imdb_id, extra_headers=extra_headers)

            cast_rows_total += len(cast_rows)
            self_rows = filter_self_cast_rows(cast_rows)
            cast_rows_self += len(self_rows)

            name_ids = [row.name_id.strip().lower() for row in self_rows if row.name_id]
            missing_ids = [name_id for name_id in name_ids if name_id not in people_cache]
            if missing_ids:
                existing_people = fetch_people_by_imdb_ids(db, missing_ids)
                for person in existing_people:
                    imdb_value = str((person.get("external_ids") or {}).get("imdb") or "").strip().lower()
                    if imdb_value:
                        people_cache[imdb_value] = str(person.get("id"))

                new_people_map: dict[str, str] = {}
                for row in self_rows:
                    key = row.name_id.strip().lower()
                    if not key or key in people_cache:
                        continue
                    new_people_map.setdefault(key, row.name)

                new_people_rows = [
                    {"full_name": name, "external_ids": {"imdb": imdb_value}}
                    for imdb_value, name in new_people_map.items()
                ]
                if new_people_rows and not args.dry_run:
                    inserted = insert_people(db, new_people_rows)
                    people_inserted += len(inserted)
                    for person in inserted:
                        imdb_value = str((person.get("external_ids") or {}).get("imdb") or "").strip().lower()
                        if imdb_value:
                            people_cache[imdb_value] = str(person.get("id"))
                elif new_people_rows:
                    people_inserted += len(new_people_rows)
                    for row in new_people_rows:
                        imdb_value = str((row.get("external_ids") or {}).get("imdb") or "").strip().lower()
                        if imdb_value:
                            people_cache[imdb_value] = f"dry-run-{imdb_value}"

            show_cast_rows: list[dict[str, object]] = []
            for row in self_rows:
                person_id = people_cache.get(row.name_id.strip().lower())
                if not person_id:
                    continue
                show_cast_rows.append(
                    {
                        "show_id": show_id,
                        "person_id": person_id,
                        "billing_order": row.billing_order,
                        "role": row.raw_role_text,
                        "credit_category": "Self",
                    }
                )

            if show_cast_rows and not args.dry_run:
                show_cast_upserted += len(upsert_show_cast(db, show_cast_rows))
            elif show_cast_rows:
                show_cast_upserted += len(show_cast_rows)

            if not args.dry_run:
                last_seen = extract_most_recent_episode(show)
                mark_sync_state_success(
                    db,
                    table_name="people",
                    show_id=show_id,
                    last_seen_most_recent_episode=last_seen,
                )
                mark_sync_state_success(
                    db,
                    table_name="show_cast",
                    show_id=show_id,
                    last_seen_most_recent_episode=last_seen,
                )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{imdb_id}: {exc}")
            if not args.dry_run:
                mark_sync_state_failed(db, table_name="people", show_id=show_id, error=exc)
                mark_sync_state_failed(db, table_name="show_cast", show_id=show_id, error=exc)

    print("Summary")
    print(f"shows_processed={len(show_rows)}")
    print(f"cast_rows_total={cast_rows_total}")
    print(f"cast_rows_self={cast_rows_self}")
    print(f"people_inserted={people_inserted}")
    print(f"show_cast_upserted={show_cast_upserted}")
    print(f"failures={len(failures)}")

    if failures:
        for failure in failures[:10]:
            print(f"- {failure}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
