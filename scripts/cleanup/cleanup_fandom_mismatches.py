#!/usr/bin/env python3
"""
Clean up Fandom profile/gallery data that was attached to the wrong person.

Default is dry-run. Use --apply to delete mismatched rows.
"""
from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from urllib.parse import unquote, urlparse

from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.utils.env import load_env


def _normalize_name(value: str | None) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\(.*?\)", " ", text)
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text)
    text = " ".join(text.split()).strip().lower()
    return text


def _names_match(expected: str | None, candidate: str | None) -> bool:
    expected_norm = _normalize_name(expected)
    candidate_norm = _normalize_name(candidate)
    if not expected_norm or not candidate_norm:
        return False
    if expected_norm == candidate_norm:
        return True
    if expected_norm in candidate_norm:
        return True
    if candidate_norm in expected_norm:
        return True
    expected_tokens = expected_norm.split()
    candidate_tokens = candidate_norm.split()
    if not expected_tokens or not candidate_tokens:
        return False
    if expected_tokens[-1] == candidate_tokens[-1]:
        return True
    if set(expected_tokens) & set(candidate_tokens):
        return True
    return False


def _name_from_fandom_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    path = parsed.path or ""
    if "/wiki/" not in path:
        return None
    slug = path.split("/wiki/", 1)[1]
    slug = slug.split("/", 1)[0]
    slug = unquote(slug)
    slug = slug.replace("_", " ")
    if slug.lower().endswith(" gallery"):
        slug = slug[: -len(" gallery")]
    return slug.strip() or None


def _matches_person(full_name: str | None, candidates: list[str | None]) -> bool:
    for cand in candidates:
        if _names_match(full_name, cand):
            return True
    return False


def _fetch_people_map(db, person_ids: list[str]) -> dict[str, str | None]:
    if not person_ids:
        return {}
    result: dict[str, str | None] = {}
    chunk = 200
    for i in range(0, len(person_ids), chunk):
        batch = person_ids[i : i + chunk]
        resp = (
            db.schema("core")
            .table("people")
            .select("id,full_name")
            .in_("id", batch)
            .execute()
        )
        if getattr(resp, "error", None):
            raise RuntimeError(f"Supabase error fetching people: {resp.error}")
        for row in resp.data or []:
            pid = row.get("id")
            if pid:
                result[pid] = row.get("full_name")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="cleanup_fandom_mismatches",
        description="Remove Fandom rows that do not match the person name.",
    )
    parser.add_argument("--apply", action="store_true", help="Delete mismatched rows.")
    parser.add_argument("--limit", type=int, default=None, help="Limit rows processed per table.")
    args = parser.parse_args(argv or sys.argv[1:])

    load_env()
    db = create_supabase_admin_client()

    # Fetch cast_fandom rows
    fandom_query = db.schema("core").table("cast_fandom").select(
        "id,person_id,source_url,page_title,full_name"
    )
    if args.limit:
        fandom_query = fandom_query.limit(args.limit)
    fandom_resp = fandom_query.execute()
    if getattr(fandom_resp, "error", None):
        raise RuntimeError(f"Supabase error fetching cast_fandom: {fandom_resp.error}")
    fandom_rows = fandom_resp.data or []

    # Fetch cast_photos rows (fandom sources only)
    photos_query = (
        db.schema("core")
        .table("cast_photos")
        .select("id,person_id,source,source_page_url,image_url")
        .in_("source", ["fandom", "fandom-gallery"])
    )
    if args.limit:
        photos_query = photos_query.limit(args.limit)
    photos_resp = photos_query.execute()
    if getattr(photos_resp, "error", None):
        raise RuntimeError(f"Supabase error fetching cast_photos: {photos_resp.error}")
    photo_rows = photos_resp.data or []

    person_ids = list({row.get("person_id") for row in fandom_rows + photo_rows if row.get("person_id")})
    people_map = _fetch_people_map(db, person_ids)

    bad_fandom_ids: list[str] = []
    bad_fandom_sources: dict[str, list[str]] = {}
    for row in fandom_rows:
        pid = row.get("person_id")
        expected_name = people_map.get(pid)
        candidates = [
            row.get("full_name"),
            row.get("page_title"),
            _name_from_fandom_url(row.get("source_url")),
        ]
        if not _matches_person(expected_name, candidates):
            bad_fandom_ids.append(row.get("id"))
            source_url = row.get("source_url")
            if pid and source_url:
                bad_fandom_sources.setdefault(pid, []).append(source_url)

    bad_photo_ids: list[str] = []
    for row in photo_rows:
        pid = row.get("person_id")
        if not pid or pid not in bad_fandom_sources:
            continue
        source_page_url = row.get("source_page_url") or ""
        if not source_page_url:
            continue
        for bad_url in bad_fandom_sources.get(pid, []):
            if source_page_url == bad_url or source_page_url.startswith(f"{bad_url}/"):
                bad_photo_ids.append(row.get("id"))
                break

    print(f"cast_fandom rows checked: {len(fandom_rows)}")
    print(f"cast_photos rows checked: {len(photo_rows)}")
    print(f"cast_fandom mismatches: {len(bad_fandom_ids)}")
    print(f"cast_photos mismatches (from bad fandom pages): {len(bad_photo_ids)}")

    if not args.apply:
        if bad_fandom_ids[:5]:
            print(f"sample cast_fandom ids: {bad_fandom_ids[:5]}")
        if bad_photo_ids[:5]:
            print(f"sample cast_photos ids: {bad_photo_ids[:5]}")
        print("Dry run only. Re-run with --apply to delete.")
        return 0

    # Delete mismatched rows
    if bad_fandom_ids:
        resp = db.schema("core").table("cast_fandom").delete().in_("id", bad_fandom_ids).execute()
        if getattr(resp, "error", None):
            raise RuntimeError(f"Supabase error deleting cast_fandom: {resp.error}")
        print(f"Deleted cast_fandom rows: {len(resp.data or [])}")

    if bad_photo_ids:
        resp = db.schema("core").table("cast_photos").delete().in_("id", bad_photo_ids).execute()
        if getattr(resp, "error", None):
            raise RuntimeError(f"Supabase error deleting cast_photos: {resp.error}")
        print(f"Deleted cast_photos rows: {len(resp.data or [])}")

        # Clean media_links that were generated from those cast_photos
        links_resp = (
            db.schema("core")
            .table("media_links")
            .select("id,context")
            .eq("entity_type", "person")
            .execute()
        )
        if getattr(links_resp, "error", None):
            raise RuntimeError(f"Supabase error fetching media_links: {links_resp.error}")

        link_ids = []
        bad_set = {str(pid) for pid in bad_photo_ids}
        for row in links_resp.data or []:
            ctx = row.get("context") or {}
            if ctx.get("legacy_table") != "cast_photos":
                continue
            legacy_id = str(ctx.get("legacy_id") or "")
            if legacy_id and legacy_id in bad_set:
                link_ids.append(row.get("id"))

        if link_ids:
            resp = db.schema("core").table("media_links").delete().in_("id", link_ids).execute()
            if getattr(resp, "error", None):
                raise RuntimeError(f"Supabase error deleting media_links: {resp.error}")
            print(f"Deleted media_links rows: {len(resp.data or [])}")
        else:
            print("No media_links to delete.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
