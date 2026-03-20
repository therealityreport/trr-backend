#!/usr/bin/env python3
"""
Backfill flat top-level metadata keys for existing Getty/NBCUMV rows.

Existing rows store rich data in nested structures (metadata.getty.*,
metadata.nbcumv.*) but the frontend reads flat top-level keys
(metadata.season_number, metadata.people_names, etc.).

This script reads each row, extracts values from the nested structures,
and writes them as top-level keys so the frontend can display them.

Default mode is dry-run. Use --apply to write changes.
"""

from __future__ import annotations

import argparse
import re
import sys
from typing import Any

from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.utils.env import load_env

# ── People-count word map (mirrors getty.py) ──

_PEOPLE_COUNT_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_getty_nbcumv_metadata",
        description="Backfill flat top-level metadata keys for existing Getty/NBCUMV rows.",
    )
    parser.add_argument("--apply", action="store_true", help="Write changes to the database.")
    parser.add_argument("--table", choices=["cast_photos", "media_assets", "both"], default="both")
    parser.add_argument("--limit", type=int, default=None, help="Optional row cap for investigation.")
    parser.add_argument("--verbose", action="store_true", help="Print per-row details.")
    return parser.parse_args(argv)


# ── Extraction helpers ──


def _str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _parse_season_from_tags(tags: list[str]) -> int | None:
    """Extract season number from Getty tags like 'Season 5'."""
    for tag in tags:
        match = re.search(r"\bSeason\s+(\d+)\b", str(tag), re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _parse_episode_from_caption(caption: str | None) -> int | None:
    """Extract episode number from caption like 'Episode 20180'."""
    if not caption:
        return None
    match = re.search(r"Episode\s+(\d+)", caption, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def _infer_people_count(keyword_texts: list[str]) -> int | None:
    """Infer people count from Getty tags like 'One Person', 'Three People'."""
    for raw_value in keyword_texts:
        lowered = str(raw_value or "").strip().lower()
        if not lowered:
            continue
        word_match = re.fullmatch(r"([a-z]+)\s+(?:people|person)", lowered)
        if word_match:
            return _PEOPLE_COUNT_WORDS.get(word_match.group(1))
        number_match = re.fullmatch(r"(\d+)\s+(?:people|person)", lowered)
        if number_match:
            return int(number_match.group(1))
    return None


def _extract_getty_people(getty: dict[str, Any]) -> list[str]:
    """Extract person names from Getty's people array."""
    people_raw = getty.get("people")
    if not isinstance(people_raw, list):
        return []
    names: list[str] = []
    for entry in people_raw:
        if isinstance(entry, dict):
            name = str(entry.get("text") or "").strip()
        else:
            name = str(entry or "").strip()
        if name:
            names.append(name)
    return names


def _extract_getty_tags(metadata: dict[str, Any], getty: dict[str, Any]) -> list[str]:
    """Get Getty keyword texts from metadata or nested getty object."""
    tags = metadata.get("getty_tags")
    if isinstance(tags, list):
        return [str(t).strip() for t in tags if str(t).strip()]
    kw = getty.get("keyword_texts")
    if isinstance(kw, list):
        return [str(t).strip() for t in kw if str(t).strip()]
    return []


# ── Main enrichment logic ──


def _compute_flat_keys(metadata: dict[str, Any], *, table: str) -> dict[str, Any]:
    """Compute flat top-level metadata keys from nested structures.

    Returns only keys that are MISSING from the current metadata and have
    a value to fill. Existing values are never overwritten.
    """
    getty = metadata.get("getty") or {}
    nbcumv = metadata.get("nbcumv") or {}
    getty_tags = _extract_getty_tags(metadata, getty)
    updates: dict[str, Any] = {}

    def _set_if_missing(key: str, value: Any) -> None:
        if key not in metadata and value is not None:
            updates[key] = value

    # show_name — from resolved_show_name or gallery_bucket
    resolved_show_name = _str(metadata.get("resolved_show_name"))
    gallery_bucket = metadata.get("gallery_bucket")
    if isinstance(gallery_bucket, dict):
        resolved_show_name = resolved_show_name or _str(gallery_bucket.get("resolved_show_name"))
    _set_if_missing("show_name", resolved_show_name)

    # show_id — from resolved_show_id or gallery_bucket
    resolved_show_id = _str(metadata.get("resolved_show_id"))
    if isinstance(gallery_bucket, dict):
        resolved_show_id = resolved_show_id or _str(gallery_bucket.get("resolved_show_id"))
    _set_if_missing("show_id", resolved_show_id)

    # season_number — from NBCUMV lbx_seasonNumber/lbx_season or Getty tags
    season_number = (
        _int(nbcumv.get("lbx_seasonNumber")) or _int(nbcumv.get("lbx_season")) or _parse_season_from_tags(getty_tags)
    )
    # Filter out negative sentinel values (NBCUMV uses -2 for "unknown")
    if season_number is not None and season_number < 0:
        season_number = None
    _set_if_missing("season_number", season_number)

    # episode_number — from NBCUMV or caption
    episode_number = (
        _int(nbcumv.get("lbx_episodeNumber"))
        or _int(nbcumv.get("episodeNumber"))
        or _parse_episode_from_caption(_str(getty.get("caption")) or _str(metadata.get("caption")))
    )
    _set_if_missing("episode_number", episode_number)

    # episode_title — from NBCUMV
    episode_title = _str(nbcumv.get("lbx_episodeTitle"))
    _set_if_missing("episode_title", episode_title)

    # photographer — from NBCUMV or Getty
    photographer = _str(nbcumv.get("lbx_photographer")) or _str(nbcumv.get("lbx_credit")) or _str(getty.get("credit"))
    getty_details = metadata.get("getty_details") or {}
    if isinstance(getty_details, dict) and not photographer:
        photographer = _str(getty_details.get("credit_display")) or _str(getty_details.get("credit"))
    _set_if_missing("photographer", photographer)

    # company — from NBCUMV
    company = _str(nbcumv.get("company")) or _str(nbcumv.get("network")) or _str(nbcumv.get("brand"))
    _set_if_missing("company", company)

    # content_type — from NBCUMV lbx_type
    content_type = _str(nbcumv.get("lbx_type"))
    _set_if_missing("content_type", content_type)

    # people_names — from Getty people, tagged_people, or NBCUMV caption
    getty_people = _extract_getty_people(getty)
    tagged_people = metadata.get("tagged_people")
    if isinstance(tagged_people, list):
        people_names = [str(p).strip() for p in tagged_people if str(p).strip()]
    elif getty_people:
        people_names = getty_people
    else:
        people_names = None
    _set_if_missing("people_names", people_names if people_names else None)

    # people_count — from Getty keywords or len(people)
    people_count = _int(metadata.get("people_count")) or _infer_people_count(getty_tags)
    if people_count is None and people_names:
        people_count = len(people_names)
    _set_if_missing("people_count", people_count)

    # created_at — from NBCUMV or Getty date
    created_at = (
        _str(nbcumv.get("created"))
        or _str(nbcumv.get("liveDate"))
        or _str(getty.get("date_created"))
        or _str(metadata.get("published_at"))
    )
    _set_if_missing("created_at", created_at)

    return updates


def _chunked(values: list, size: int = 200):
    for i in range(0, len(values), size):
        yield values[i : i + size]


def _fetch_rows(db, *, table: str, sources: list[str], limit: int | None) -> list[dict[str, Any]]:
    query = db.schema("core").table(table).select("id,source,metadata")
    query = query.in_("source", sources)
    if limit:
        query = query.limit(limit)
    response = query.execute()
    if getattr(response, "error", None):
        raise RuntimeError(f"Supabase error fetching {table}: {response.error}")
    return response.data or []


def _update_metadata(db, *, table: str, row_id: str, metadata: dict[str, Any]) -> bool:
    response = db.schema("core").table(table).update({"metadata": metadata}).eq("id", row_id).execute()
    if getattr(response, "error", None):
        print(f"  ERROR updating {table} {row_id}: {response.error}", file=sys.stderr)
        return False
    return True


def _process_table(
    db,
    *,
    table: str,
    sources: list[str],
    limit: int | None,
    apply: bool,
    verbose: bool,
) -> dict[str, int]:
    rows = _fetch_rows(db, table=table, sources=sources, limit=limit)
    stats = {"total": len(rows), "needs_update": 0, "updated": 0, "skipped": 0, "errors": 0}

    for row in rows:
        row_id = str(row.get("id", ""))
        metadata = row.get("metadata") or {}
        if not isinstance(metadata, dict):
            stats["skipped"] += 1
            continue

        updates = _compute_flat_keys(metadata, table=table)
        if not updates:
            stats["skipped"] += 1
            continue

        stats["needs_update"] += 1
        if verbose:
            print(f"  {table} {row_id[:8]}... source={row.get('source')} +{len(updates)} keys: {list(updates.keys())}")

        if apply:
            merged = {**metadata, **updates}
            if _update_metadata(db, table=table, row_id=row_id, metadata=merged):
                stats["updated"] += 1
            else:
                stats["errors"] += 1

    return stats


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    load_env()
    db = create_supabase_admin_client()

    tables_to_process: list[tuple[str, list[str]]] = []
    if args.table in ("cast_photos", "both"):
        tables_to_process.append(("cast_photos", ["getty"]))
    if args.table in ("media_assets", "both"):
        tables_to_process.append(("media_assets", ["getty", "nbcumv"]))

    for table, sources in tables_to_process:
        print(f"\n{'─' * 40}")
        print(f"Processing {table} (sources: {', '.join(sources)})")
        print(f"{'─' * 40}")
        stats = _process_table(
            db,
            table=table,
            sources=sources,
            limit=args.limit,
            apply=args.apply,
            verbose=args.verbose,
        )
        print(f"  total={stats['total']}")
        print(f"  needs_update={stats['needs_update']}")
        if args.apply:
            print(f"  updated={stats['updated']}")
            print(f"  errors={stats['errors']}")
        print(f"  skipped={stats['skipped']}")

    if not args.apply:
        print("\nDry run only. Re-run with --apply to write changes.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
