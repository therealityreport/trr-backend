#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

import requests

from scripts._sync_common import (
    add_show_filter_args,
    extract_imdb_series_id,
    extract_most_recent_episode,
    fetch_show_rows,
    filter_show_rows_for_sync,
    load_env_and_db,
)
from trr_backend.ingestion.show_importer import parse_imdb_headers_json_env
from trr_backend.integrations.imdb.fullcredits_cast_parser import (
    fetch_fullcredits_cast_with_fallback,
    filter_self_cast_rows,
)
from trr_backend.integrations.tmdb.client import TmdbClientError, find_by_imdb_id, resolve_api_key
from trr_backend.repositories.credits import assert_core_credits_table_exists, insert_credits_ignore_conflicts
from trr_backend.repositories.people import assert_core_people_table_exists, fetch_people_by_imdb_ids, insert_people
from trr_backend.repositories.person_images import upsert_person_images
from trr_backend.repositories.sync_state import (
    assert_core_sync_state_table_exists,
    mark_sync_state_failed,
    mark_sync_state_in_progress,
    mark_sync_state_success,
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_show_cast",
        description="Sync show-level cast credits from IMDb full credits (Self only).",
    )
    add_show_filter_args(parser)
    return parser.parse_args(argv)


def _merge_external_ids(existing: object, updates: dict[str, object]) -> dict[str, object] | None:
    existing_map = existing if isinstance(existing, dict) else {}
    merged: dict[str, object] = dict(existing_map)
    changed = False
    for key, value in updates.items():
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
            if not value:
                continue
        if merged.get(key) != value:
            merged[key] = value
            changed = True
    return merged if changed else None


def _fetch_tmdb_find_payload(
    imdb_id: str,
    *,
    api_key: str | None,
    session: requests.Session,
    cache: dict[str, dict[str, object] | None],
) -> dict[str, object] | None:
    if not imdb_id:
        return None
    if imdb_id in cache:
        return cache[imdb_id]
    if not api_key:
        cache[imdb_id] = None
        return None
    try:
        payload = find_by_imdb_id(imdb_id, api_key=api_key, session=session)
    except TmdbClientError as exc:
        print(f"TMDb find failed imdb_id={imdb_id} (HTTP {exc.status_code})", file=sys.stderr)
        payload = None
    except Exception as exc:  # noqa: BLE001
        print(f"TMDb find failed imdb_id={imdb_id} (unexpected error={exc})", file=sys.stderr)
        payload = None
    cache[imdb_id] = payload
    return payload


def _extract_tmdb_person_id(payload: dict[str, object] | None) -> int | None:
    if not isinstance(payload, dict):
        return None
    results = payload.get("person_results")
    if not isinstance(results, list):
        return None
    for item in results:
        if not isinstance(item, dict):
            continue
        tmdb_id = item.get("id")
        if isinstance(tmdb_id, int):
            return tmdb_id
    return None


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    if args.skip_db:
        print("ERROR: --skip-db is not supported for this script (database access required).", file=sys.stderr)
        return 2
    db = load_env_and_db(skip_db=args.skip_db)
    assert_core_people_table_exists(db)
    assert_core_credits_table_exists(db)
    if not args.dry_run and not args.skip_db:
        assert_core_sync_state_table_exists(db)

    show_rows = fetch_show_rows(db, args)
    if not show_rows:
        print("No shows matched the filters.")
        return 0

    extra_headers = parse_imdb_headers_json_env()
    tmdb_find_api_key = resolve_api_key()
    tmdb_find_session = requests.Session()
    tmdb_find_cache: dict[str, dict[str, object] | None] = {}
    cast_rows_total = 0
    cast_rows_self = 0
    person_images_upserted = 0
    failures: list[str] = []
    people_cache: dict[str, str] = {}
    people_inserted = 0
    credits_inserted = 0

    filter_result = filter_show_rows_for_sync(
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
    show_rows = filter_result.selected
    if not show_rows:
        print("No shows need show_cast sync.")
        return 0

    for show in show_rows:
        imdb_id = extract_imdb_series_id(show)
        show_id = str(show.get("id") or "")
        if not show_id or not imdb_id:
            if args.verbose:
                print(f"SKIP show id={show_id or show.get('id')} (missing imdb_series_id)")
            continue
        if not args.dry_run:
            mark_sync_state_in_progress(db, table_name="show_cast", show_id=show_id)

        try:
            # Use centralized fallback function (returns cast_rows + source_type + person_images)
            cast_rows, source_type, person_images = fetch_fullcredits_cast_with_fallback(
                imdb_id,
                extra_headers=extra_headers,
                verbose=bool(args.verbose),
                primary_source="graphql",
            )

            cast_rows_total += len(cast_rows)
            self_rows = filter_self_cast_rows(cast_rows)
            cast_rows_self += len(self_rows)

            name_ids = [row.name_id.strip().lower() for row in self_rows if row.name_id]
            missing_ids = [name_id for name_id in name_ids if name_id not in people_cache]
            if missing_ids:
                existing_people = fetch_people_by_imdb_ids(db, missing_ids)
                existing_updates: list[tuple[str, dict[str, object]]] = []
                for person in existing_people:
                    imdb_value = str((person.get("external_ids") or {}).get("imdb") or "").strip().lower()
                    if imdb_value:
                        people_cache[imdb_value] = str(person.get("id"))
                        if not (person.get("external_ids") or {}).get("tmdb"):
                            payload = _fetch_tmdb_find_payload(
                                imdb_value,
                                api_key=tmdb_find_api_key,
                                session=tmdb_find_session,
                                cache=tmdb_find_cache,
                            )
                            tmdb_person_id = _extract_tmdb_person_id(payload)
                            merged_external_ids = _merge_external_ids(
                                person.get("external_ids"),
                                {"tmdb": tmdb_person_id},
                            )
                            person_id = person.get("id")
                            if merged_external_ids is not None and isinstance(person_id, str):
                                existing_updates.append((person_id, merged_external_ids))
                for person_id, merged_external_ids in existing_updates:
                    try:
                        db.schema("core").table("people").update({"external_ids": merged_external_ids}).eq(
                            "id", person_id
                        ).execute()
                    except Exception as exc:  # noqa: BLE001
                        print(f"WARNING: failed to update person external_ids id={person_id} error={exc}")

                new_people_map: dict[str, str] = {}
                for row in self_rows:
                    key = row.name_id.strip().lower()
                    if not key or key in people_cache:
                        continue
                    new_people_map.setdefault(key, row.name)

                new_people_rows = [
                    {
                        "full_name": name,
                        "external_ids": _merge_external_ids(
                            {},
                            {
                                "imdb": imdb_value,
                                "tmdb": _extract_tmdb_person_id(
                                    _fetch_tmdb_find_payload(
                                        imdb_value,
                                        api_key=tmdb_find_api_key,
                                        session=tmdb_find_session,
                                        cache=tmdb_find_cache,
                                    )
                                ),
                            },
                        )
                        or {"imdb": imdb_value},
                    }
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

            # Persist person images from GraphQL tier after ensuring people exist
            if person_images and not args.dry_run:
                if args.verbose:
                    print(f"  Upserting {len(person_images)} person images...")
                upserted_images = upsert_person_images(db, person_images, verbose=bool(args.verbose))
                person_images_upserted += len(upserted_images)

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
                # Replace all non-manual scraped Self credits so this run is authoritative.
                delete_resp = (
                    db.schema("core")
                    .table("credits")
                    .delete()
                    .eq("show_id", show_id)
                    .eq("credit_category", "Self")
                    .neq("source_type", "manual")
                    .execute()
                )
                if hasattr(delete_resp, "error") and delete_resp.error:
                    raise RuntimeError(f"Supabase error deleting credits for show_id={show_id}: {delete_resp.error}")

                credit_rows = [
                    {
                        "show_id": row["show_id"],
                        "person_id": row["person_id"],
                        "credit_category": row.get("credit_category") or "Self",
                        "role": row.get("role"),
                        "billing_order": row.get("billing_order"),
                        "source_type": source_type,
                        "metadata": {},
                    }
                    for row in show_cast_rows
                ]
                inserted = insert_credits_ignore_conflicts(db, credit_rows)
                credits_inserted += len(inserted)
            elif show_cast_rows:
                credits_inserted += len(show_cast_rows)

            if not args.dry_run:
                mark_sync_state_success(
                    db,
                    table_name="show_cast",
                    show_id=show_id,
                    last_seen_most_recent_episode=extract_most_recent_episode(show),
                )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{imdb_id}: {exc}")
            if not args.dry_run:
                mark_sync_state_failed(db, table_name="show_cast", show_id=show_id, error=exc)

    print("Summary")
    print(f"shows_processed={len(show_rows)}")
    print(f"cast_rows_total={cast_rows_total}")
    print(f"cast_rows_self={cast_rows_self}")
    print(f"people_inserted={people_inserted}")
    print(f"credits_inserted={credits_inserted}")
    print(f"person_images_upserted={person_images_upserted}")
    print(f"failures={len(failures)}")

    if failures:
        for failure in failures[:10]:
            print(f"- {failure}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
