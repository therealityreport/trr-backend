#!/usr/bin/env python3
"""
Enrich all cast members for a show: Fandom profiles, TMDb profiles, gallery photos, and S3 mirroring.

Usage:
    # Enrich cast for a show by IMDb ID
    PYTHONPATH=. python scripts/enrich_show_cast.py --imdb-id tt1628033

    # Enrich cast for a show by name (searches DB)
    PYTHONPATH=. python scripts/enrich_show_cast.py --show-name "Real Housewives of Salt Lake City"

    # Skip S3 mirroring (just import photos to DB)
    PYTHONPATH=. python scripts/enrich_show_cast.py --imdb-id tt1628033 --skip-s3

    # Dry run mode
    PYTHONPATH=. python scripts/enrich_show_cast.py --imdb-id tt1628033 --dry-run

    # Limit number of cast members to process
    PYTHONPATH=. python scripts/enrich_show_cast.py --imdb-id tt1628033 --limit 5

    # Only run TMDb enrichment (skip Fandom and gallery)
    PYTHONPATH=. python scripts/enrich_show_cast.py --imdb-id tt1628033 --skip-fandom-profile --skip-gallery --skip-s3

    # Skip TMDb enrichment
    PYTHONPATH=. python scripts/enrich_show_cast.py --imdb-id tt1628033 --skip-tmdb
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from datetime import UTC, datetime
from typing import Any

from trr_backend.db.supabase import create_supabase_admin_client
from trr_backend.ingestion.fandom_person_scraper import (
    fetch_fandom_person_html,
    parse_fandom_person_html,
)
from trr_backend.integrations.fandom import (
    build_real_housewives_wiki_url_from_name,
    fetch_fandom_gallery,
    is_fandom_page_missing,
    search_real_housewives_wiki,
)
from trr_backend.integrations.tmdb_person import (
    TMDbPersonFull,
    fetch_tmdb_person_full,
)
from trr_backend.media.s3_mirror import get_cdn_base_url, mirror_cast_photo_row
from trr_backend.repositories.cast_fandom import upsert_cast_fandom
from trr_backend.repositories.cast_photos import (
    fetch_cast_photos_missing_hosted,
    update_cast_photo_hosted_fields,
    upsert_cast_photos,
)
from trr_backend.repositories.cast_tmdb import get_cast_tmdb_by_person_id, upsert_cast_tmdb
from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="enrich_show_cast",
        description="Enrich all cast members for a show: Fandom profiles, gallery photos, and S3 mirroring.",
    )
    parser.add_argument(
        "--imdb-id",
        help="IMDb show ID (e.g., tt1628033 for RHOSLC).",
    )
    parser.add_argument(
        "--show-name",
        help="Show name to search for in DB.",
    )
    parser.add_argument(
        "--show-id",
        help="Internal show UUID.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of cast members to process (0 = no limit).",
    )
    parser.add_argument(
        "--gallery-limit",
        type=int,
        default=50,
        help="Max gallery images per person (default: 50).",
    )
    parser.add_argument(
        "--skip-fandom-profile",
        action="store_true",
        help="Skip Fandom profile enrichment.",
    )
    parser.add_argument(
        "--skip-gallery",
        action="store_true",
        help="Skip Fandom gallery import.",
    )
    parser.add_argument(
        "--skip-tmdb",
        action="store_true",
        help="Skip TMDb profile enrichment.",
    )
    parser.add_argument(
        "--skip-s3",
        action="store_true",
        help="Skip S3 mirroring (just import to DB).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned work without writing to DB/S3.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    parser.add_argument(
        "--delay-ms",
        type=int,
        default=1000,
        help="Delay between cast members in milliseconds (default: 1000).",
    )
    parser.add_argument(
        "--force-s3",
        action="store_true",
        help="Re-mirror photos even if already hosted.",
    )
    return parser.parse_args(argv)


def _find_show_by_imdb_id(db, imdb_id: str) -> dict[str, Any] | None:
    """Find a show by IMDb ID."""
    response = db.schema("core").table("shows").select("id,name,imdb_id").eq("imdb_id", imdb_id).limit(1).execute()
    if response.data:
        return response.data[0]
    return None


def _find_show_by_name(db, name: str) -> dict[str, Any] | None:
    """Find a show by name (fuzzy search)."""
    response = db.schema("core").table("shows").select("id,name,imdb_id").ilike("name", f"%{name}%").limit(1).execute()
    if response.data:
        return response.data[0]
    return None


def _get_cast_for_show(db, show_id: str) -> list[dict[str, Any]]:
    """Get all cast members for a show via show_cast."""
    response = (
        db.schema("core")
        .table("show_cast")
        .select("person_id,people:person_id(id,full_name,external_ids)")
        .eq("show_id", show_id)
        .execute()
    )
    if response.data:
        seen = set()
        people = []
        for row in response.data:
            person = row.get("people")
            if isinstance(person, dict) and person.get("id"):
                person_id = person["id"]
                if person_id not in seen:
                    seen.add(person_id)
                    people.append(person)
        return people

    return []


def _canonical_url(url: str) -> str:
    """Create a canonical URL for deduplication."""
    if "?" in url:
        url = url.split("?")[0]
    return url.lower().strip()


def _url_hash(url: str) -> str:
    """Generate a short hash of a URL for source_image_id."""
    return hashlib.sha256(_canonical_url(url).encode()).hexdigest()[:16]


def _enrich_fandom_profile(
    db,
    person: dict[str, Any],
    *,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict[str, Any] | None:
    """Enrich a person's Fandom profile."""
    person_id = person.get("id")
    full_name = person.get("full_name")

    if not full_name:
        if verbose:
            print(f"    Skipping Fandom profile - no name for person {person_id}")
        return None

    # Try to find Fandom page
    fandom_url = search_real_housewives_wiki(full_name)
    if not fandom_url:
        fandom_url = build_real_housewives_wiki_url_from_name(full_name)

    if verbose:
        print(f"    Fetching Fandom profile: {fandom_url}")

    try:
        html, final_url = fetch_fandom_person_html(fandom_url)
        if not html or is_fandom_page_missing(html, 200):
            if verbose:
                print(f"    Fandom page not found for {full_name}")
            return None

        cast_fandom, photos = parse_fandom_person_html(html, source_url=final_url)
        cast_fandom["person_id"] = person_id

        if dry_run:
            print(f"    DRY RUN: Would save Fandom profile with {len(photos)} photos")
            return {"fandom": cast_fandom, "photos": photos}

        # Save to cast_fandom
        result = upsert_cast_fandom(db, cast_fandom)

        # Save photos to cast_photos
        if photos:
            for photo in photos:
                photo["person_id"] = person_id
                photo["source"] = "fandom"
                if photo.get("image_url"):
                    photo["source_image_id"] = f"fandom-{_url_hash(photo['image_url'])}"
            upsert_cast_photos(db, photos, dedupe_on="image_url_canonical")

        if verbose:
            print(f"    Saved Fandom profile with {len(photos)} photos")

        return {"fandom": result, "photos_count": len(photos)}

    except Exception as e:
        print(f"    Error fetching Fandom profile: {e}")
        return None


def _enrich_tmdb_profile(
    db,
    person: dict[str, Any],
    *,
    dry_run: bool = False,
    verbose: bool = False,
    force: bool = False,
) -> dict[str, Any] | None:
    """Enrich a person's TMDb profile."""
    person_id = person.get("id")
    full_name = person.get("full_name")
    external_ids = person.get("external_ids") or {}

    # Get TMDb ID from external_ids
    tmdb_id = external_ids.get("tmdb_id") or external_ids.get("tmdb")
    if not tmdb_id:
        if verbose:
            print(f"    Skipping TMDb - no TMDb ID for {full_name}")
        return None

    tmdb_id = int(tmdb_id)

    # Check if already enriched (unless forcing)
    if not force:
        existing = get_cast_tmdb_by_person_id(db, str(person_id))
        if existing:
            if verbose:
                print(f"    TMDb profile already exists for {full_name}")
            return existing

    if verbose:
        print(f"    Fetching TMDb profile for {full_name} (TMDb ID: {tmdb_id})")

    try:
        person_data: TMDbPersonFull | None = fetch_tmdb_person_full(tmdb_id)

        if not person_data:
            if verbose:
                print(f"    TMDb profile not found for {full_name}")
            return None

        # Build row for cast_tmdb table
        now = datetime.now(UTC).isoformat()
        row = {
            "person_id": str(person_id),
            "tmdb_id": tmdb_id,
            "name": person_data.details.name if person_data.details else None,
            "also_known_as": person_data.details.also_known_as if person_data.details else None,
            "biography": person_data.details.biography if person_data.details else None,
            "birthday": person_data.details.birthday if person_data.details else None,
            "deathday": person_data.details.deathday if person_data.details else None,
            "gender": person_data.details.gender if person_data.details else 0,
            "adult": person_data.details.adult if person_data.details else None,
            "homepage": person_data.details.homepage if person_data.details else None,
            "known_for_department": person_data.details.known_for_department if person_data.details else None,
            "place_of_birth": person_data.details.place_of_birth if person_data.details else None,
            "popularity": person_data.details.popularity if person_data.details else None,
            "profile_path": person_data.details.profile_path if person_data.details else None,
            "fetched_at": now,
        }

        # Add external IDs if available
        if person_data.external_ids:
            ext = person_data.external_ids
            row["imdb_id"] = ext.imdb_id
            row["freebase_mid"] = ext.freebase_mid
            row["freebase_id"] = ext.freebase_id
            row["tvrage_id"] = ext.tvrage_id
            row["wikidata_id"] = ext.wikidata_id
            row["facebook_id"] = ext.facebook_id
            row["instagram_id"] = ext.instagram_id
            row["tiktok_id"] = ext.tiktok_id
            row["twitter_id"] = ext.twitter_id
            row["youtube_id"] = ext.youtube_id

        if dry_run:
            aka_count = len(row.get("also_known_as") or [])
            print(f"    DRY RUN: Would save TMDb profile ({aka_count} alt names)")
            return row

        result = upsert_cast_tmdb(db, row)

        if verbose:
            aka_count = len(row.get("also_known_as") or [])
            print(f"    Saved TMDb profile ({aka_count} alternative names)")

        return result

    except Exception as e:
        print(f"    Error fetching TMDb profile: {e}")
        return None


def _import_gallery_photos(
    db,
    person: dict[str, Any],
    *,
    limit: int = 50,
    dry_run: bool = False,
    verbose: bool = False,
) -> int:
    """Import gallery photos for a person."""
    person_id = person.get("id")
    full_name = person.get("full_name")

    if not full_name:
        if verbose:
            print(f"    Skipping gallery - no name for person {person_id}")
        return 0

    if verbose:
        print(f"    Fetching gallery for {full_name}")

    try:
        gallery = fetch_fandom_gallery(full_name)

        if gallery.error:
            if verbose:
                print(f"    Gallery error: {gallery.error}")
            return 0

        if not gallery.images:
            if verbose:
                print("    No gallery images found")
            return 0

        images_to_import = gallery.images[:limit]

        if dry_run:
            print(f"    DRY RUN: Would import {len(images_to_import)} gallery photos")
            return len(images_to_import)

        # Convert to cast_photo rows
        now = datetime.now(UTC).isoformat()
        rows = []
        for img in images_to_import:
            rows.append(
                {
                    "person_id": person_id,
                    "source": "fandom",
                    "source_page_url": img.source_page_url,
                    "source_image_id": f"fandom-gallery-{_url_hash(img.url)}",
                    "image_url": img.url,
                    "thumb_url": img.thumb_url,
                    "image_url_canonical": _canonical_url(img.url),
                    "caption": img.caption,
                    "fetched_at": now,
                }
            )

        result = upsert_cast_photos(db, rows, dedupe_on="image_url_canonical")
        imported_count = len(result) if result else 0

        if verbose:
            print(f"    Imported {imported_count} gallery photos")

        return imported_count

    except Exception as e:
        print(f"    Error importing gallery: {e}")
        return 0


def _mirror_photos_to_s3(
    db,
    person_id: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
) -> tuple[int, int]:
    """Mirror a person's photos to S3."""
    try:
        cdn_base_url = None if force else get_cdn_base_url()
        rows = fetch_cast_photos_missing_hosted(
            db,
            source="fandom",
            person_ids=[person_id],
            limit=200,
            include_hosted=True,
            cdn_base_url=cdn_base_url,
        )

        if not rows:
            if verbose:
                print("    No photos to mirror")
            return 0, 0

        mirrored = 0
        failed = 0

        for row in rows:
            try:
                patch = mirror_cast_photo_row(row, force=force)
                if not patch:
                    continue

                if dry_run:
                    mirrored += 1
                    continue

                update_cast_photo_hosted_fields(db, str(row.get("id")), patch)
                mirrored += 1

            except Exception as e:
                failed += 1
                if verbose:
                    print(f"    Error mirroring photo: {e}")

        if verbose:
            print(f"    Mirrored {mirrored} photos, {failed} failed")

        return mirrored, failed

    except Exception as e:
        print(f"    Error fetching photos for mirroring: {e}")
        return 0, 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    if not args.imdb_id and not args.show_name and not args.show_id:
        print("Error: Must specify --imdb-id, --show-name, or --show-id")
        return 1

    db = create_supabase_admin_client()

    # Find the show
    show = None
    if args.show_id:
        response = db.schema("core").table("shows").select("id,name,imdb_id").eq("id", args.show_id).limit(1).execute()
        if response.data:
            show = response.data[0]
    elif args.imdb_id:
        show = _find_show_by_imdb_id(db, args.imdb_id)
    elif args.show_name:
        show = _find_show_by_name(db, args.show_name)

    if not show:
        print("Error: Show not found")
        return 1

    show_id = show.get("id")
    show_name = show.get("name")
    print(f"Found show: {show_name} (ID: {show_id})")

    # Get cast members
    cast = _get_cast_for_show(db, show_id)
    if not cast:
        print("No cast members found for this show")
        return 0

    if args.limit > 0:
        cast = cast[: args.limit]

    print(f"Processing {len(cast)} cast members...")

    # Stats
    total_fandom_profiles = 0
    total_tmdb_profiles = 0
    total_gallery_photos = 0
    total_s3_mirrored = 0
    total_s3_failed = 0

    for idx, person in enumerate(cast):
        person_id = person.get("id")
        full_name = person.get("full_name") or "Unknown"

        if idx > 0 and args.delay_ms > 0:
            time.sleep(args.delay_ms / 1000.0)

        print(f"\n[{idx + 1}/{len(cast)}] {full_name}")

        # 1. Enrich Fandom profile
        if not args.skip_fandom_profile:
            result = _enrich_fandom_profile(
                db,
                person,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
            if result:
                total_fandom_profiles += 1

        # 2. Enrich TMDb profile
        if not args.skip_tmdb:
            result = _enrich_tmdb_profile(
                db,
                person,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
            if result:
                total_tmdb_profiles += 1

        # 3. Import gallery photos
        if not args.skip_gallery:
            gallery_count = _import_gallery_photos(
                db,
                person,
                limit=args.gallery_limit,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
            total_gallery_photos += gallery_count

        # 4. Mirror to S3
        if not args.skip_s3:
            mirrored, failed = _mirror_photos_to_s3(
                db,
                person_id,
                force=args.force_s3,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
            total_s3_mirrored += mirrored
            total_s3_failed += failed

    # Summary
    print(f"\n{'=' * 50}")
    print("SUMMARY")
    print(f"{'=' * 50}")
    print(f"Show: {show_name}")
    print(f"Cast members processed: {len(cast)}")
    print(f"Fandom profiles enriched: {total_fandom_profiles}")
    print(f"TMDb profiles enriched: {total_tmdb_profiles}")
    print(f"Gallery photos imported: {total_gallery_photos}")
    if not args.skip_s3:
        print(f"Photos mirrored to S3: {total_s3_mirrored}")
        print(f"S3 mirror failures: {total_s3_failed}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
