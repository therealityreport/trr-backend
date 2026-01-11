#!/usr/bin/env python3
"""
Mirror cast_photos images to S3 and optionally prune orphaned objects.

Usage:
    # Mirror all fandom photos
    PYTHONPATH=. python scripts/mirror_cast_photos_to_s3.py --source fandom

    # Mirror TMDb photos for a specific person
    PYTHONPATH=. python scripts/mirror_cast_photos_to_s3.py --source tmdb --imdb-person-id nm11883948

    # Mirror Fandom gallery photos for a specific person
    PYTHONPATH=. python scripts/mirror_cast_photos_to_s3.py --source fandom-gallery --imdb-person-id nm11883948

    # Mirror and prune orphaned S3 objects
    PYTHONPATH=. python scripts/mirror_cast_photos_to_s3.py --source all --prune --imdb-person-id nm11883948
"""

from __future__ import annotations

import argparse
import sys

from trr_backend.db.supabase import create_supabase_admin_client
from trr_backend.media.s3_mirror import (
    get_cdn_base_url,
    mirror_cast_photo_row,
    prune_orphaned_cast_photo_objects,
)
from trr_backend.repositories.cast_photos import (
    assert_core_cast_photos_table_exists,
    fetch_cast_photos_missing_hosted,
    update_cast_photo_hosted_fields,
)
from trr_backend.repositories.people import fetch_people_by_imdb_ids
from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mirror_cast_photos_to_s3",
        description="Mirror core.cast_photos images to S3 and store hosted_url.",
    )
    parser.add_argument(
        "--source",
        default="fandom",
        choices=["fandom", "imdb", "tmdb", "fandom-gallery", "all"],
        help="Image source to mirror (default: fandom).",
    )
    parser.add_argument("--person-id", action="append", default=[], help="core.people id (UUID). Repeatable.")
    parser.add_argument("--imdb-person-id", action="append", default=[], help="IMDb person id (nm...). Repeatable.")
    parser.add_argument("--limit", type=int, default=200, help="Max rows to mirror (default: 200).")
    parser.add_argument("--dry-run", action="store_true", help="Print planned work without writing to Supabase/S3.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing hosted fields.")
    parser.add_argument("--prune", action="store_true", help="Prune orphaned S3 objects after mirroring.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return parser.parse_args(argv)


def _coerce_str_list(values: list[str]) -> list[str]:
    out: list[str] = []
    for raw in values:
        text = str(raw or "").strip()
        if text:
            out.append(text)
    return out


def _resolve_person_ids(db, imdb_ids: list[str]) -> list[str]:
    people = fetch_people_by_imdb_ids(db, imdb_ids)
    return [str(row.get("id")) for row in people if row.get("id")]


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    db = create_supabase_admin_client()
    assert_core_cast_photos_table_exists(db)

    person_ids = _coerce_str_list(args.person_id)
    imdb_ids = _coerce_str_list(args.imdb_person_id)

    # Resolve IMDb IDs to person IDs and track the mapping for prune
    imdb_to_person: dict[str, str] = {}
    if imdb_ids:
        people = fetch_people_by_imdb_ids(db, imdb_ids)
        for person in people:
            pid = str(person.get("id") or "")
            external_ids = person.get("external_ids") or {}
            imdb = external_ids.get("imdb")
            if pid:
                person_ids.append(pid)
                if imdb:
                    imdb_to_person[imdb] = pid

    cdn_base_url = None if args.force else get_cdn_base_url()
    rows = fetch_cast_photos_missing_hosted(
        db,
        source=args.source,
        person_ids=person_ids or None,
        limit=args.limit,
        include_hosted=True,
        cdn_base_url=cdn_base_url,
    )

    if not rows:
        print("No cast photos matched the filters.")
        # Still run prune if requested
        if args.prune and imdb_ids:
            return _run_prune(db, imdb_ids, args.dry_run, args.verbose)
        return 0

    scanned = 0
    mirrored = 0
    skipped = 0
    failed = 0

    for row in rows:
        scanned += 1
        try:
            patch = mirror_cast_photo_row(row, force=bool(args.force))
            if not patch:
                skipped += 1
                continue
            if args.dry_run:
                mirrored += 1
                if args.verbose:
                    print(f"DRY RUN: would update {row.get('id')} -> {patch.get('hosted_url')}")
                continue

            updated = update_cast_photo_hosted_fields(db, str(row.get("id")), patch)
            mirrored += 1
            if args.verbose:
                print(f"Updated {updated.get('id')} -> {updated.get('hosted_url')}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"WARN {row.get('id')}: {exc}")

    print("Summary")
    print(f"scanned={scanned}")
    print(f"mirrored={mirrored}")
    print(f"skipped={skipped}")
    print(f"failed={failed}")

    # Prune orphaned S3 objects if requested
    if args.prune and imdb_ids:
        _run_prune(db, imdb_ids, args.dry_run, args.verbose)

    return 0


def _run_prune(db, imdb_ids: list[str], dry_run: bool, verbose: bool) -> int:
    """Run prune for each IMDb person ID."""
    total_pruned = 0
    for imdb_id in imdb_ids:
        if verbose:
            print(f"\nPruning orphaned S3 objects for {imdb_id}...")
        try:
            orphaned = prune_orphaned_cast_photo_objects(
                db,
                imdb_id,
                dry_run=dry_run,
                verbose=verbose,
            )
            total_pruned += len(orphaned)
        except Exception as exc:  # noqa: BLE001
            print(f"WARN prune {imdb_id}: {exc}")

    print(f"pruned={total_pruned}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
