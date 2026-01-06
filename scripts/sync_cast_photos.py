#!/usr/bin/env python3
"""
Unified multi-source cast photo sync with S3 mirroring and pruning.

Fetches photos from IMDb, TMDb, Fandom person pages, and Fandom galleries,
then mirrors them to S3 and prunes orphaned objects.

Usage:
    # Sync all sources for a person by IMDb ID
    PYTHONPATH=. python scripts/sync_cast_photos.py --imdb-person-id nm11883948

    # Sync only TMDb and Fandom sources
    PYTHONPATH=. python scripts/sync_cast_photos.py --imdb-person-id nm11883948 --source tmdb --source fandom

    # Sync without S3 mirroring
    PYTHONPATH=. python scripts/sync_cast_photos.py --imdb-person-id nm11883948 --no-s3

    # Sync without pruning orphaned S3 objects
    PYTHONPATH=. python scripts/sync_cast_photos.py --imdb-person-id nm11883948 --no-prune

    # Dry run (no mutations)
    PYTHONPATH=. python scripts/sync_cast_photos.py --imdb-person-id nm11883948 --dry-run
"""
from __future__ import annotations

import argparse
import sys
from typing import Any, Iterable

try:
    import requests
except Exception:  # noqa: BLE001
    requests = None

from trr_backend.db.postgrest_cache import is_pgrst204_error
from trr_backend.db.supabase import create_supabase_admin_client
from trr_backend.ingestion.cast_photo_sources import fetch_all_cast_photos
from trr_backend.repositories.cast_photos import (
    assert_core_cast_photos_table_exists,
    fetch_cast_photos_missing_hosted,
    update_cast_photo_hosted_fields,
    upsert_cast_photos,
)
from trr_backend.repositories.cast_tmdb import get_cast_tmdb_by_person_id
from trr_backend.repositories.people import assert_core_people_table_exists, fetch_people_by_imdb_ids
from trr_backend.utils.env import load_env

ALL_SOURCES = ["imdb", "tmdb", "fandom", "fandom-gallery"]


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_cast_photos",
        description="Unified multi-source cast photo sync with S3 mirroring.",
    )

    # Input filters
    parser.add_argument(
        "--person-id",
        action="append",
        default=[],
        help="core.people UUID. Repeatable.",
    )
    parser.add_argument(
        "--imdb-person-id",
        action="append",
        default=[],
        help="IMDb person ID (nm...). Repeatable.",
    )
    parser.add_argument(
        "--show-id",
        action="append",
        default=[],
        help="core.shows UUID - sync all cast from show. Repeatable.",
    )
    parser.add_argument(
        "--imdb-show-id",
        action="append",
        default=[],
        help="IMDb series ID (tt...) - sync all cast from show. Repeatable.",
    )

    # Source selection
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        choices=ALL_SOURCES + ["all"],
        help=f"Photo sources to sync. Repeatable. Default: all ({', '.join(ALL_SOURCES)})",
    )
    parser.add_argument("--no-imdb", action="store_true", help="Skip IMDb source.")
    parser.add_argument("--no-tmdb", action="store_true", help="Skip TMDb source.")
    parser.add_argument("--no-fandom", action="store_true", help="Skip Fandom person page source.")
    parser.add_argument("--no-fandom-gallery", action="store_true", help="Skip Fandom gallery source.")

    # S3 options
    parser.add_argument("--no-s3", action="store_true", help="Skip S3 mirroring.")
    parser.add_argument("--no-prune", action="store_true", help="Skip S3 orphan pruning.")
    parser.add_argument("--force-mirror", action="store_true", help="Re-download and re-upload even if hosted.")

    # Limits and behavior
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max photos per source per person (default: 50).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print actions without mutating DB or S3.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")

    return parser.parse_args(argv)


def _coerce_str_list(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    for raw in values:
        text = str(raw or "").strip()
        if text:
            out.append(text)
    return out


def _resolve_sources(args: argparse.Namespace) -> list[str]:
    """Determine which sources to fetch based on args."""
    if args.source:
        if "all" in args.source:
            sources = list(ALL_SOURCES)
        else:
            sources = list(set(args.source))
    else:
        sources = list(ALL_SOURCES)

    # Apply --no-* exclusions
    if args.no_imdb and "imdb" in sources:
        sources.remove("imdb")
    if args.no_tmdb and "tmdb" in sources:
        sources.remove("tmdb")
    if args.no_fandom and "fandom" in sources:
        sources.remove("fandom")
    if args.no_fandom_gallery and "fandom-gallery" in sources:
        sources.remove("fandom-gallery")

    return sources


def _fetch_show_ids_by_imdb(db, imdb_id: str) -> list[str]:
    imdb_id = str(imdb_id or "").strip()
    if not imdb_id:
        return []

    for column in ("imdb_id", "imdb_series_id"):
        try:
            response = db.schema("core").table("shows").select("id").eq(column, imdb_id).execute()
        except Exception as exc:  # noqa: BLE001
            if is_pgrst204_error(exc):
                continue
            raise RuntimeError(f"Supabase error fetching show for {imdb_id}: {exc}") from exc
        if hasattr(response, "error") and response.error:
            if is_pgrst204_error(response.error):
                continue
            raise RuntimeError(f"Supabase error fetching show for {imdb_id}: {response.error}")
        data = response.data or []
        ids = [str(row.get("id")) for row in data if row.get("id")]
        if ids:
            return ids
    return []


def _fetch_person_ids_for_show(db, show_id: str) -> list[str]:
    person_ids: set[str] = set()
    for table in ("episode_appearances", "show_cast"):
        try:
            response = db.schema("core").table(table).select("person_id").eq("show_id", show_id).execute()
        except Exception:  # noqa: BLE001
            continue
        if hasattr(response, "error") and response.error:
            continue
        data = response.data or []
        for row in data:
            person_id = row.get("person_id")
            if person_id:
                person_ids.add(str(person_id))
    return list(person_ids)


def _fetch_people_by_ids(db, person_ids: Iterable[str]) -> list[dict[str, Any]]:
    ids = [str(pid) for pid in person_ids if str(pid).strip()]
    if not ids:
        return []
    rows: list[dict[str, Any]] = []
    chunk_size = 200
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]
        response = db.schema("core").table("people").select("id,full_name,external_ids").in_("id", chunk).execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error listing people: {response.error}")
        data = response.data or []
        if isinstance(data, list):
            rows.extend(data)
    return rows


def _extract_imdb_person_id(person: dict[str, Any]) -> str | None:
    external_ids = person.get("external_ids")
    if isinstance(external_ids, dict):
        imdb_id = str(external_ids.get("imdb") or "").strip()
        if imdb_id:
            return imdb_id
    return None


def _get_tmdb_person_id(db, person_id: str) -> int | None:
    """Get TMDb person ID from cast_tmdb table or people.external_ids."""
    cast_tmdb = get_cast_tmdb_by_person_id(db, person_id)
    if cast_tmdb and cast_tmdb.get("tmdb_id"):
        return int(cast_tmdb["tmdb_id"])

    # Fall back to people.external_ids
    try:
        response = (
            db.schema("core")
            .table("people")
            .select("external_ids")
            .eq("id", person_id)
            .limit(1)
            .execute()
        )
        if response.data and isinstance(response.data, list) and response.data:
            external_ids = response.data[0].get("external_ids") or {}
            tmdb_id = external_ids.get("tmdb_id") or external_ids.get("tmdb")
            if tmdb_id:
                return int(tmdb_id)
    except Exception:  # noqa: BLE001
        pass

    return None


def _mirror_person_photos(
    db,
    person_id: str,
    imdb_person_id: str | None,
    *,
    force: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
) -> tuple[int, int]:
    """
    Mirror all unhosted photos for a person to S3.

    Returns:
        Tuple of (mirrored_count, failed_count)
    """
    from trr_backend.media.s3_mirror import get_cdn_base_url, mirror_cast_photo_row

    cdn_base_url = None if force else get_cdn_base_url()
    rows = fetch_cast_photos_missing_hosted(
        db,
        person_ids=[person_id],
        include_hosted=True,
        cdn_base_url=cdn_base_url,
    )

    if not rows:
        return 0, 0

    mirrored = 0
    failed = 0

    for row in rows:
        # Ensure imdb_person_id is set for S3 path
        if not row.get("imdb_person_id") and imdb_person_id:
            row["imdb_person_id"] = imdb_person_id

        try:
            patch = mirror_cast_photo_row(row, force=force)
            if not patch:
                continue

            if dry_run:
                if verbose:
                    print(f"    DRY RUN: would mirror {row.get('id')} -> {patch.get('hosted_url')}")
                mirrored += 1
                continue

            update_cast_photo_hosted_fields(db, str(row.get("id")), patch)
            mirrored += 1
            if verbose:
                print(f"    Mirrored {row.get('id')} -> {patch.get('hosted_url')}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            if verbose:
                print(f"    WARN mirror {row.get('id')}: {exc}")

    return mirrored, failed


def _prune_person_s3_objects(
    db,
    person_identifier: str,
    *,
    dry_run: bool = False,
    verbose: bool = False,
) -> int:
    """
    Prune orphaned S3 objects for a person.

    Returns:
        Count of pruned objects
    """
    from trr_backend.media.s3_mirror import prune_orphaned_cast_photo_objects

    try:
        orphaned = prune_orphaned_cast_photo_objects(
            db,
            person_identifier,
            dry_run=dry_run,
            verbose=verbose,
        )
        return len(orphaned)
    except Exception as exc:  # noqa: BLE001
        if verbose:
            print(f"    WARN prune: {exc}")
        return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()
    db = create_supabase_admin_client()
    assert_core_people_table_exists(db)
    assert_core_cast_photos_table_exists(db)

    sources = _resolve_sources(args)
    if not sources:
        print("No sources enabled. Nothing to do.")
        return 0

    if args.verbose:
        print(f"Enabled sources: {', '.join(sources)}")

    # Collect person IDs from all input methods
    person_ids = _coerce_str_list(args.person_id)
    imdb_person_ids = _coerce_str_list(args.imdb_person_id)
    show_ids = _coerce_str_list(args.show_id)
    imdb_show_ids = _coerce_str_list(args.imdb_show_id)

    # Resolve IMDb show IDs to show UUIDs
    for imdb_show_id in imdb_show_ids:
        show_ids.extend(_fetch_show_ids_by_imdb(db, imdb_show_id))

    # Collect people from all sources
    people: list[dict[str, Any]] = []

    # From IMDb person IDs
    if imdb_person_ids:
        people.extend(fetch_people_by_imdb_ids(db, imdb_person_ids))

    # From show IDs (cast members)
    for show_id in show_ids:
        cast_person_ids = _fetch_person_ids_for_show(db, show_id)
        people.extend(_fetch_people_by_ids(db, cast_person_ids))

    # From person UUIDs directly
    if person_ids:
        people.extend(_fetch_people_by_ids(db, person_ids))

    # Deduplicate by person ID
    people_map: dict[str, dict[str, Any]] = {}
    for person in people:
        pid = str(person.get("id") or "").strip()
        if pid:
            people_map[pid] = person

    if not people_map:
        print("No people matched the filters.")
        return 0

    if args.verbose:
        print(f"Processing {len(people_map)} people...")

    session = requests.Session() if requests is not None else None

    # Stats
    total_fetched = 0
    total_upserted = 0
    total_mirrored = 0
    total_pruned = 0
    total_failed = 0

    for person_id, person in people_map.items():
        imdb_person_id = _extract_imdb_person_id(person)
        name = str(person.get("full_name") or "").strip()

        if args.verbose:
            print(f"\n{'='*60}")
            print(f"Person: {name or person_id}")
            if imdb_person_id:
                print(f"  IMDb ID: {imdb_person_id}")

        # Get TMDb ID if tmdb source is enabled
        tmdb_person_id = None
        if "tmdb" in sources:
            tmdb_person_id = _get_tmdb_person_id(db, person_id)
            if args.verbose and tmdb_person_id:
                print(f"  TMDb ID: {tmdb_person_id}")

        # Fetch photos from all sources
        rows = fetch_all_cast_photos(
            person_id,
            imdb_person_id=imdb_person_id,
            tmdb_person_id=tmdb_person_id,
            person_name=name,
            sources=sources,
            limit_per_source=args.limit,
            session=session,
            verbose=args.verbose,
        )

        total_fetched += len(rows)

        if not rows:
            if args.verbose:
                print("  No photos found from any source.")
            continue

        if args.verbose:
            print(f"  Total photos fetched: {len(rows)}")

        # Upsert to database - split by source to use appropriate dedupe key
        # IMDb uses source_image_id, others use image_url_canonical
        imdb_rows = [r for r in rows if r.get("source") == "imdb"]
        other_rows = [r for r in rows if r.get("source") != "imdb"]

        if args.dry_run:
            if args.verbose:
                print(f"  DRY RUN: would upsert {len(imdb_rows)} IMDb + {len(other_rows)} other photos")
            total_upserted += len(rows)
        else:
            # Upsert IMDb photos using source_image_id
            if imdb_rows:
                try:
                    upserted = upsert_cast_photos(db, imdb_rows, dedupe_on="source_image_id")
                    total_upserted += len(upserted)
                    if args.verbose:
                        print(f"  Upserted {len(upserted)} IMDb photos")
                except Exception as exc:  # noqa: BLE001
                    total_failed += 1
                    print(f"  ERROR upserting IMDb photos: {exc}")

            # Upsert other sources using image_url_canonical
            if other_rows:
                try:
                    upserted = upsert_cast_photos(db, other_rows, dedupe_on="image_url_canonical")
                    total_upserted += len(upserted)
                    if args.verbose:
                        print(f"  Upserted {len(upserted)} other photos")
                except Exception as exc:  # noqa: BLE001
                    total_failed += 1
                    print(f"  ERROR upserting other photos: {exc}")

        # Mirror to S3
        if not args.no_s3:
            if args.verbose:
                print("  Mirroring to S3...")
            mirrored, failed = _mirror_person_photos(
                db,
                person_id,
                imdb_person_id,
                force=args.force_mirror,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
            total_mirrored += mirrored
            total_failed += failed
            if args.verbose:
                print(f"  Mirrored: {mirrored}, Failed: {failed}")

        # Prune orphaned S3 objects
        if not args.no_s3 and not args.no_prune:
            person_identifier = imdb_person_id or person_id
            if args.verbose:
                print(f"  Pruning orphaned S3 objects under {person_identifier}...")
            pruned = _prune_person_s3_objects(
                db,
                person_identifier,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
            total_pruned += pruned

    # Summary
    print(f"\n{'='*60}")
    print("Summary")
    print(f"  People processed: {len(people_map)}")
    print(f"  Sources: {', '.join(sources)}")
    print(f"  Photos fetched: {total_fetched}")
    print(f"  Photos upserted: {total_upserted}")
    if not args.no_s3:
        print(f"  Photos mirrored: {total_mirrored}")
    if not args.no_s3 and not args.no_prune:
        print(f"  S3 objects pruned: {total_pruned}")
    if total_failed:
        print(f"  Failures: {total_failed}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
