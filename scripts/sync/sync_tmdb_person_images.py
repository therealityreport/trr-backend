#!/usr/bin/env python3
"""Sync TMDb person profile images to cast_photos table."""

from __future__ import annotations

import argparse
import sys
from typing import Any
from uuid import UUID

from trr_backend.db.supabase import create_supabase_admin_client
from trr_backend.ingestion.tmdb_person_images import (
    build_tmdb_cast_photo_rows,
    fetch_tmdb_person_profile_images,
)
from trr_backend.repositories.cast_photos import (
    assert_core_cast_photos_table_exists,
    upsert_cast_photos,
)
from trr_backend.repositories.cast_tmdb import get_cast_tmdb_by_person_id, get_cast_tmdb_by_tmdb_id
from trr_backend.repositories.people import fetch_people_by_imdb_ids
from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_tmdb_person_images",
        description="Fetch TMDb person profile images and store in cast_photos.",
    )
    parser.add_argument(
        "--person-id",
        action="append",
        default=[],
        help="core.people UUID. Repeatable.",
    )
    parser.add_argument(
        "--tmdb-person-id",
        action="append",
        default=[],
        type=int,
        help="TMDb person ID (integer). Repeatable.",
    )
    parser.add_argument(
        "--imdb-person-id",
        action="append",
        default=[],
        help="IMDb person ID (nm...). Repeatable. Will resolve to person_id.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max images to import per person.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned work without writing to database.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    return parser.parse_args(argv)


def _resolve_person_from_imdb_id(db, imdb_id: str) -> dict[str, Any] | None:
    """Resolve a person record from IMDb person ID."""
    people = fetch_people_by_imdb_ids(db, [imdb_id])
    if people:
        return people[0]
    return None


def _get_tmdb_person_id(db, person_id: str) -> int | None:
    """Get TMDb person ID from cast_tmdb table or people.external_ids."""
    # First check cast_tmdb table
    cast_tmdb = get_cast_tmdb_by_person_id(db, person_id)
    if cast_tmdb and cast_tmdb.get("tmdb_id"):
        return int(cast_tmdb["tmdb_id"])

    # Fall back to people.external_ids
    try:
        response = db.schema("core").table("people").select("external_ids").eq("id", person_id).limit(1).execute()
        if response.data and isinstance(response.data, list) and response.data:
            external_ids = response.data[0].get("external_ids") or {}
            tmdb_id = external_ids.get("tmdb_id") or external_ids.get("tmdb")
            if tmdb_id:
                return int(tmdb_id)
    except Exception:
        pass

    return None


def _get_imdb_person_id(db, person_id: str) -> str | None:
    """Get IMDb person ID from people.external_ids."""
    try:
        response = db.schema("core").table("people").select("external_ids").eq("id", person_id).limit(1).execute()
        if response.data and isinstance(response.data, list) and response.data:
            external_ids = response.data[0].get("external_ids") or {}
            return external_ids.get("imdb")
    except Exception:
        pass
    return None


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    db = create_supabase_admin_client()
    assert_core_cast_photos_table_exists(db)

    # Collect targets: list of (person_id, tmdb_person_id, imdb_person_id)
    targets: list[tuple[str, int, str | None]] = []

    # Process --person-id flags
    for pid in args.person_id:
        pid = str(pid).strip()
        if not pid:
            continue
        tmdb_id = _get_tmdb_person_id(db, pid)
        if not tmdb_id:
            print(f"WARN: No TMDb person ID found for person {pid}, skipping.")
            continue
        imdb_id = _get_imdb_person_id(db, pid)
        targets.append((pid, tmdb_id, imdb_id))

    # Process --tmdb-person-id flags
    for tmdb_id in args.tmdb_person_id:
        if not tmdb_id:
            continue
        # Look up person_id from cast_tmdb
        cast_tmdb = get_cast_tmdb_by_tmdb_id(db, tmdb_id)
        if cast_tmdb and cast_tmdb.get("person_id"):
            pid = str(cast_tmdb["person_id"])
            imdb_id = cast_tmdb.get("imdb_id") or _get_imdb_person_id(db, pid)
            targets.append((pid, tmdb_id, imdb_id))
        else:
            print(f"WARN: No person found for TMDb ID {tmdb_id}, skipping.")

    # Process --imdb-person-id flags
    for imdb_id in args.imdb_person_id:
        imdb_id = str(imdb_id).strip()
        if not imdb_id:
            continue
        person = _resolve_person_from_imdb_id(db, imdb_id)
        if not person:
            print(f"WARN: No person found for IMDb ID {imdb_id}, skipping.")
            continue
        pid = str(person["id"])
        tmdb_id = _get_tmdb_person_id(db, pid)
        if not tmdb_id:
            print(f"WARN: No TMDb person ID found for {imdb_id}, skipping.")
            continue
        targets.append((pid, tmdb_id, imdb_id))

    if not targets:
        print("No valid targets found. Provide --person-id, --tmdb-person-id, or --imdb-person-id.")
        return 1

    # Dedupe by person_id
    seen: set[str] = set()
    unique_targets: list[tuple[str, int, str | None]] = []
    for t in targets:
        if t[0] not in seen:
            seen.add(t[0])
            unique_targets.append(t)

    total_fetched = 0
    total_upserted = 0

    for person_id, tmdb_person_id, imdb_person_id in unique_targets:
        if args.verbose:
            print(f"Fetching TMDb images for person_id={person_id}, tmdb_id={tmdb_person_id}")

        try:
            images = fetch_tmdb_person_profile_images(tmdb_person_id)
        except Exception as exc:
            print(f"WARN: Failed to fetch TMDb images for {tmdb_person_id}: {exc}")
            continue

        if not images:
            if args.verbose:
                print(f"  No images found for TMDb person {tmdb_person_id}")
            continue

        if args.limit:
            images = images[: args.limit]

        total_fetched += len(images)

        rows = build_tmdb_cast_photo_rows(
            person_id=UUID(person_id),
            tmdb_person_id=tmdb_person_id,
            images=images,
            imdb_person_id=imdb_person_id,
        )

        if args.dry_run:
            for row in rows:
                print(f"DRY RUN: would upsert {row.image_url_canonical}")
            total_upserted += len(rows)
            continue

        try:
            result = upsert_cast_photos(db, rows, dedupe_on="image_url_canonical")
            total_upserted += len(result)
            if args.verbose:
                for r in result:
                    print(f"  OK: {r.get('image_url_canonical') or r.get('id')}")
        except Exception as exc:
            print(f"WARN: Failed to upsert cast photos: {exc}")

    print("\nSummary")
    print(f"persons_processed={len(unique_targets)}")
    print(f"images_fetched={total_fetched}")
    print(f"images_upserted={total_upserted}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
