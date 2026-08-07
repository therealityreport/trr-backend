#!/usr/bin/env python3
"""
Import photos from Fandom gallery pages into core.cast_photos.

Usage:
    # Import gallery photos for a specific person by name
    PYTHONPATH=. python scripts/import/import_fandom_gallery_photos.py --name "Lisa Barlow"

    # Import gallery photos for multiple people
    PYTHONPATH=. python scripts/import/import_fandom_gallery_photos.py --name "Lisa Barlow" --name "Teresa Giudice"

    # Import gallery photos for people by IMDb ID
    PYTHONPATH=. python scripts/import/import_fandom_gallery_photos.py --imdb-person-id nm11883948

    # Dry run (no DB writes)
    PYTHONPATH=. python scripts/import/import_fandom_gallery_photos.py --name "Lisa Barlow" --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
from datetime import UTC, datetime
from typing import Any, cast

from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.ingestion.cast_photo_sources import (
    _is_real_housewives_fandom_source,
    _should_keep_real_housewives_fandom_image,
)
from trr_backend.integrations.fandom import (
    FandomGalleryImage,
    fetch_fandom_file_metadata,
    fetch_fandom_gallery,
)
from trr_backend.repositories.cast_photos import upsert_cast_photos
from trr_backend.repositories.people import fetch_people_by_imdb_ids
from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="import_fandom_gallery_photos",
        description="Import photos from Fandom gallery pages into core.cast_photos.",
    )
    parser.add_argument(
        "--name",
        action="append",
        default=[],
        help="Person name to fetch gallery for (repeatable).",
    )
    parser.add_argument(
        "--imdb-person-id",
        action="append",
        default=[],
        help="IMDb person ID to fetch gallery for (repeatable).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max images to import per person (default: 50).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned work without writing to Supabase.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    parser.add_argument(
        "--delay-ms",
        type=int,
        default=500,
        help="Delay between gallery fetches in milliseconds (default: 500).",
    )
    return parser.parse_args(argv)


def _canonical_url(url: str) -> str:
    """Create a canonical URL for deduplication."""
    # Remove query params and normalize
    if "?" in url:
        url = url.split("?")[0]
    return url.lower().strip()


def _url_hash(url: str) -> str:
    """Generate a short hash of a URL for source_image_id."""
    return hashlib.sha256(_canonical_url(url).encode()).hexdigest()[:16]


def _normalize_section_label(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"[_-]+", " ", value).strip()
    if not cleaned:
        return None
    if cleaned.lower() == cleaned:
        return cleaned.title()
    return cleaned


def _infer_content_type(*values: str | None) -> str | None:
    text = " ".join([v for v in values if v]).strip().lower()
    text = re.sub(r"[_-]+", " ", text)
    if not text:
        return None
    if "confessional" in text or "confession" in text:
        return "CONFESSIONAL"
    if "intro" in text or "tagline" in text or "opening" in text or "chapter card" in text:
        return "INTRO"
    if "reunion" in text:
        return "REUNION"
    if "promo" in text or "promotional" in text:
        return "PROMO"
    if "episode" in text or "still" in text:
        return "EPISODE STILL"
    return "OTHER"


def _extract_season_number(*values: str | None) -> int | None:
    for value in values:
        if not value:
            continue
        match = re.search(r"\\b(?:season|s)\\s*([0-9]{1,2})\\b", value, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue
        match = re.search(r"\\b([0-9]{1,2})\\b", value)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue
    return None


def _gallery_image_to_cast_photo(
    image: FandomGalleryImage,
    *,
    person_id: str,
    file_meta: Any | None = None,
) -> dict[str, Any]:
    """Convert a FandomGalleryImage to a cast_photos row."""
    now = datetime.now(UTC).isoformat()
    section_label = _normalize_section_label(image.section_label)
    content_type = _infer_content_type(section_label, image.caption)
    season = _extract_season_number(section_label, image.caption)
    resolved_url = image.url
    resolved_width = image.width
    resolved_height = image.height
    resolved_mime = None
    resolved_created_at = None
    if file_meta:
        resolved_url = file_meta.file_url or resolved_url
        resolved_width = file_meta.width
        resolved_height = file_meta.height
        resolved_mime = file_meta.mime_type
        resolved_created_at = file_meta.created_at
    if _is_real_housewives_fandom_source(
        image.source_page_url, image.url
    ) and not _should_keep_real_housewives_fandom_image(
        section_label,
        image.caption,
        resolved_url,
        image.file_page_url,
        image.source_page_url,
    ):
        raise ValueError(f"Skipping unsupported RH Fandom gallery image: {resolved_url}")
    metadata: dict[str, Any] = {
        "source_variant": "fandom_gallery",
        "source_page_url": image.file_page_url or image.source_page_url,
    }
    if image.source_page_url:
        metadata["source_gallery_url"] = image.source_page_url
    if image.file_page_url:
        metadata["source_file_url"] = image.file_page_url
    if content_type:
        metadata["content_type"] = content_type
    if season:
        metadata["season_number"] = season
    if resolved_width:
        metadata["image_width"] = resolved_width
    if resolved_height:
        metadata["image_height"] = resolved_height
    if resolved_mime:
        metadata["image_mime_type"] = resolved_mime
    if resolved_created_at:
        metadata["source_created_at"] = resolved_created_at
    return {
        "person_id": person_id,
        "source": "fandom",
        "source_page_url": image.file_page_url or image.source_page_url,
        "source_image_id": f"fandom-gallery-{_url_hash(resolved_url)}",
        "url": resolved_url,  # Required NOT NULL column
        "url_path": resolved_url,  # Required NOT NULL column
        "image_url": resolved_url,
        "thumb_url": image.thumb_url,
        "image_url_canonical": _canonical_url(resolved_url),
        "caption": image.caption,
        "season": season,
        "width": resolved_width,
        "height": resolved_height,
        "metadata": metadata,
        "fetched_at": now,
    }


def _resolve_people_by_imdb_ids(db, imdb_ids: list[str]) -> list[dict[str, Any]]:
    """Resolve IMDb IDs to people records with names."""
    if not imdb_ids:
        return []
    return fetch_people_by_imdb_ids(db, imdb_ids)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    db = create_supabase_admin_client()

    # Build list of (person_id, name) tuples to process
    targets: list[tuple[str | None, str]] = []

    # Add names directly provided
    for name in args.name:
        name = name.strip()
        if name:
            targets.append((None, name))

    # Resolve IMDb IDs to names
    imdb_ids = [id.strip() for id in args.imdb_person_id if id.strip()]
    if imdb_ids:
        people = _resolve_people_by_imdb_ids(db, imdb_ids)
        for person in people:
            person_id = person.get("id")
            name = person.get("full_name") or person.get("name")
            if person_id and name:
                targets.append((str(person_id), name))
            elif name:
                targets.append((None, name))

    if not targets:
        print("No targets specified. Use --name or --imdb-person-id.")
        return 1

    total_imported = 0
    total_skipped = 0
    total_failed = 0

    for idx, (person_id, name) in enumerate(targets):
        if idx > 0 and args.delay_ms > 0:
            time.sleep(args.delay_ms / 1000.0)

        print(f"\nFetching gallery for: {name}")
        gallery = fetch_fandom_gallery(name)

        if gallery.error:
            print(f"  Error: {gallery.error}")
            total_failed += 1
            continue

        if not gallery.images:
            print("  No images found in gallery")
            total_skipped += 1
            continue

        print(f"  Found {len(gallery.images)} images")

        # If we don't have person_id, try to look it up by name
        if not person_id:
            # Try to find person in DB by name
            try:
                response = (
                    db.schema("core").table("people").select("id").ilike("full_name", f"%{name}%").limit(1).execute()
                )
                if response.data:
                    person_id = str(response.data[0].get("id"))
                    print(f"  Resolved person_id: {person_id}")
            except Exception as e:
                print(f"  Warning: Could not resolve person_id: {e}")

        if not person_id:
            print(f"  Skipping - no person_id found for {name}")
            total_skipped += 1
            continue

        # Limit images
        images_to_import = gallery.images[: args.limit]

        # Convert to cast_photo rows
        file_meta_cache: dict[str, Any] = {}
        rows: list[dict[str, Any]] = []
        for img in images_to_import:
            file_meta = None
            if img.file_page_url:
                cached = file_meta_cache.get(img.file_page_url)
                if cached is None:
                    try:
                        cached = fetch_fandom_file_metadata(img.file_page_url)
                    except Exception as exc:
                        if args.verbose:
                            print(f"  WARN file page {img.file_page_url}: {exc}")
                        cached = None
                    file_meta_cache[img.file_page_url] = cached
                file_meta = cached
            try:
                row = _gallery_image_to_cast_photo(img, person_id=person_id, file_meta=file_meta)
            except ValueError as exc:
                if args.verbose:
                    print(f"  Skip: {exc}")
                continue
            rows.append(row)

        if args.dry_run:
            print(f"  DRY RUN: Would import {len(rows)} photos")
            for row in rows[:5]:
                print(f"    - {cast('str', row.get('image_url'))[:80]}...")
            if len(rows) > 5:
                print(f"    ... and {len(rows) - 5} more")
            total_imported += len(rows)
            continue

        try:
            result = upsert_cast_photos(db, rows, dedupe_on="image_url_canonical")
            imported_count = len(result) if result else 0
            print(f"  Imported {imported_count} photos (dedupe may have reduced count)")
            total_imported += imported_count
        except Exception as e:
            print(f"  Error importing photos: {e}")
            total_failed += 1

    print(f"\n{'=' * 40}")
    print("Summary")
    print(f"  Total imported: {total_imported}")
    print(f"  Total skipped: {total_skipped}")
    print(f"  Total failed: {total_failed}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
