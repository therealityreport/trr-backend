#!/usr/bin/env python3
"""
Sync BravoTV.com photo galleries into media_assets / media_links.

Workflow:
  1. Query BravoTV JSON API for galleries tagged with a person (field_cast)
     or a show (field_related_shows).
  2. For each gallery, fetch the HTML page and extract:
     - Image URLs from the slideshow markup
     - Structured metadata from the embedded drupal-settings-json script
       (cast names, show, season, episode)
  3. Resolve show/season from metadata to TRR database IDs.
  4. Create media_assets (source="bravotv") and media_links at show and/or
     season level with kind="gallery".

Default mode is dry-run. Use --apply to write changes.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
import time
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trr_backend.db.admin import create_supabase_admin_client  # noqa: E402
from trr_backend.utils.env import load_env  # noqa: E402

# ── Constants ──

_BRAVO_BASE = "https://www.bravotv.com"
_JSONAPI_BASE = f"{_BRAVO_BASE}/jsonapi"

_ASSET_ID_NAMESPACE = UUID("52f296b6-0f8d-4bfb-8f39-6e7e5ea8a3a6")
_LINK_ID_NAMESPACE = UUID("3e73e1b4-6b0f-4cbf-a0f4-9029a4f9f2b7")

# Season slug patterns from BravoTV analytics metadata
_SEASON_RE = re.compile(r"Season[- ](\d+)", re.IGNORECASE)

# Drupal settings JSON extraction
_SETTINGS_RE = re.compile(
    r'<script[^>]*data-drupal-selector="drupal-settings-json"[^>]*>(.*?)</script>',
    re.DOTALL,
)

# Image URL extraction from gallery slideshow HTML
_GALLERY_IMG_RE = re.compile(
    r"/sites/bravo/files/(?:styles/media_gallery_computer/public/)?"
    r'((?:field_media_items|legacy/(?:photos|images/photo))/[^\s"\'?]+\.(?:jpg|jpeg|png))',
    re.IGNORECASE,
)


# ── CLI ──


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="sync_bravotv_galleries",
        description="Sync BravoTV photo galleries to media_assets / media_links.",
    )
    p.add_argument("--apply", action="store_true", help="Write changes to the database.")
    p.add_argument("--show", type=str, required=True, help="Show name to sync (exact match in DB).")
    p.add_argument(
        "--person-uuid",
        type=str,
        default=None,
        help=("BravoTV person node UUID (from JSON API). If provided, fetches galleries tagged with this person."),
    )
    p.add_argument(
        "--person-name",
        type=str,
        default=None,
        help="Person name to search on BravoTV (e.g. 'Brandi Glanville'). Used to find the person UUID.",
    )
    p.add_argument("--limit", type=int, default=None, help="Max galleries to process.")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


# ── HTTP helpers ──


def _get_json(client: httpx.Client, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    resp = client.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


def _get_html(client: httpx.Client, url: str) -> str:
    resp = client.get(url)
    resp.raise_for_status()
    return resp.text


# ── BravoTV JSON API helpers ──


def find_person_uuid(client: httpx.Client, name: str) -> str | None:
    """Find a person's UUID on BravoTV by name."""
    data = _get_json(
        client,
        f"{_JSONAPI_BASE}/node/person",
        params={
            "filter[title]": name,
            "page[limit]": "1",
        },
    )
    entries = data.get("data", [])
    if not entries:
        return None
    return entries[0].get("id")


def fetch_tagged_galleries(
    client: httpx.Client,
    person_uuid: str,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch all media_gallery nodes tagged with a person via field_cast."""
    galleries: list[dict[str, Any]] = []
    offset = 0
    page_size = 50

    while True:
        data = _get_json(
            client,
            f"{_JSONAPI_BASE}/node/media_gallery",
            params={
                "filter[field_cast.id]": person_uuid,
                "page[limit]": str(page_size),
                "page[offset]": str(offset),
            },
        )
        entries = data.get("data", [])
        for entry in entries:
            attrs = entry.get("attributes", {})
            path_obj = attrs.get("path")
            alias = path_obj.get("alias") if isinstance(path_obj, dict) else None
            galleries.append(
                {
                    "uuid": entry.get("id"),
                    "title": attrs.get("title"),
                    "nid": attrs.get("drupal_internal__nid"),
                    "path": alias,
                    "created": attrs.get("created"),
                }
            )

        if len(entries) < page_size:
            break
        offset += page_size
        if limit and len(galleries) >= limit:
            galleries = galleries[:limit]
            break
        time.sleep(0.3)  # rate limit

    return galleries


# ── HTML scraping helpers ──


def extract_drupal_settings(page_html: str) -> dict[str, Any]:
    """Extract the drupal-settings-json blob from a gallery page."""
    match = _SETTINGS_RE.search(page_html)
    if not match:
        return {}
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}


def extract_gallery_metadata(settings: dict[str, Any]) -> dict[str, Any]:
    """Extract structured metadata from drupal-settings-json."""
    meta: dict[str, Any] = {}

    # From mpscall.cag (ad targeting tags)
    cag = settings.get("mpscall", settings.get("mps", {})).get("cag", {})
    if isinstance(cag, dict):
        # Cast names (pipe-delimited, hyphenated)
        field_cast = cag.get("field-cast", "")
        if field_cast:
            meta["cast_slugs"] = [s.strip() for s in field_cast.split("|") if s.strip()]

        meta["show_slug"] = cag.get("show")
        meta["season_slug"] = cag.get("season")
        meta["episode_slug"] = cag.get("episode")
        meta["content_type"] = cag.get("type")

    # From ls_adobe_analytics (cleaner names)
    adobe = settings.get("ls_adobe_analytics", {})
    if isinstance(adobe, dict):
        meta["people"] = adobe.get("people", "")
        meta["show_name"] = adobe.get("showSite", "")
        meta["season_name"] = adobe.get("season", "")
        meta["content_id"] = adobe.get("contentID", "")
        meta["published_date"] = adobe.get("publishedDate", "")
        meta["page_title"] = adobe.get("pageName", "")

    # Slide count from viewsSlideshowCycle
    cycle = settings.get("viewsSlideshowCycle", {})
    for _key, val in cycle.items():
        if isinstance(val, dict) and "num_divs" in val:
            meta["slide_count"] = val["num_divs"]
            break

    return {k: v for k, v in meta.items() if v}


def extract_gallery_images(page_html: str) -> list[str]:
    """Extract unique original-resolution image URLs from gallery HTML."""
    raw_matches = _GALLERY_IMG_RE.findall(page_html)
    seen: set[str] = set()
    urls: list[str] = []
    for rel_path in raw_matches:
        # Normalize to original (unstyled) path
        url = f"{_BRAVO_BASE}/sites/bravo/files/{rel_path}"
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def resolve_season_number(meta: dict[str, Any]) -> int | None:
    """Extract season number from BravoTV metadata."""
    season_name = meta.get("season_name", "")
    if season_name:
        m = _SEASON_RE.search(season_name)
        if m:
            return int(m.group(1))
    season_slug = meta.get("season_slug", "")
    if season_slug:
        m = _SEASON_RE.search(season_slug.replace("-", " "))
        if m:
            return int(m.group(1))
    return None


# ── ID generation (matching existing patterns) ──


def _asset_id(source_url: str) -> str:
    return str(uuid5(_ASSET_ID_NAMESPACE, f"bravotv:url:{source_url}"))


def _link_id(entity_type: str, entity_id: str, asset_id: str, kind: str, context: dict) -> str:
    ctx_json = json.dumps(context, sort_keys=True, separators=(",", ":"))
    name = f"{entity_type}:{entity_id}:{asset_id}:{kind}:None:0:{ctx_json}"
    return str(uuid5(_LINK_ID_NAMESPACE, name))


# ── Core processing ──


def process_gallery(
    client: httpx.Client,
    gallery: dict[str, Any],
    *,
    show_id: str,
    season_map: dict[int, str],
    verbose: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Process a single gallery page → assets + links."""
    path = gallery.get("path")
    if not path:
        return [], []

    url = f"{_BRAVO_BASE}{path}"
    try:
        page_html = _get_html(client, url)
    except httpx.HTTPStatusError as exc:
        if verbose:
            print(f"  SKIP {path}: HTTP {exc.response.status_code}", file=sys.stderr)
        return [], []

    settings = extract_drupal_settings(page_html)
    meta = extract_gallery_metadata(settings)
    image_urls = extract_gallery_images(page_html)

    if not image_urls:
        if verbose:
            print(f"  SKIP {path}: no images found")
        return [], []

    season_number = resolve_season_number(meta)
    season_id = season_map.get(season_number) if season_number else None

    # Build people list from metadata
    people_names: list[str] = []
    raw_people = meta.get("people", "")
    if raw_people:
        people_names = [html.unescape(p.strip()) for p in raw_people.split(",") if p.strip()]

    gallery_title = html.unescape(gallery.get("title", ""))

    assets: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []

    for position, img_url in enumerate(image_urls):
        aid = _asset_id(img_url)

        asset = {
            "id": aid,
            "media_type": "image",
            "source": "bravotv",
            "source_url": img_url,
            "source_asset_id": img_url.replace(f"{_BRAVO_BASE}/sites/bravo/files/", ""),
            "metadata": {
                "gallery_title": gallery_title,
                "gallery_path": path,
                "gallery_nid": gallery.get("nid"),
                "people_names": people_names if people_names else None,
                "season_number": season_number,
                "show_name": meta.get("show_name"),
                "episode_slug": meta.get("episode_slug"),
                "published_date": meta.get("published_date"),
                "content_type": meta.get("content_type"),
            },
            "ingest_status": "pending",
        }
        # Remove None values from metadata
        asset["metadata"] = {k: v for k, v in asset["metadata"].items() if v is not None}
        assets.append(asset)

        # Link to show
        show_context = {
            "gallery_title": gallery_title,
            "gallery_path": path,
            "position_in_gallery": position,
        }
        if season_number:
            show_context["season_number"] = season_number

        show_link_id = _link_id("show", show_id, aid, "gallery", show_context)
        links.append(
            {
                "id": show_link_id,
                "entity_type": "show",
                "entity_id": show_id,
                "media_asset_id": aid,
                "kind": "gallery",
                "position": position,
                "is_primary": False,
                "context": show_context,
            }
        )

        # Link to season (if resolved)
        if season_id:
            season_context = {
                "gallery_title": gallery_title,
                "gallery_path": path,
                "position_in_gallery": position,
                "season_number": season_number,
            }
            season_link_id = _link_id("season", season_id, aid, "gallery", season_context)
            links.append(
                {
                    "id": season_link_id,
                    "entity_type": "season",
                    "entity_id": season_id,
                    "media_asset_id": aid,
                    "kind": "gallery",
                    "position": position,
                    "is_primary": False,
                    "context": season_context,
                }
            )

    return assets, links


# ── Database operations ──


def _chunked(items: list, size: int = 100):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def upsert_assets_and_links(
    db,
    all_assets: list[dict[str, Any]],
    all_links: list[dict[str, Any]],
    *,
    verbose: bool = False,
) -> dict[str, int]:
    stats = {"assets_upserted": 0, "links_upserted": 0, "errors": 0}

    # Deduplicate assets by ID
    unique_assets: dict[str, dict[str, Any]] = {}
    for a in all_assets:
        unique_assets[a["id"]] = a

    deduped_assets = list(unique_assets.values())
    if verbose:
        print(f"  Upserting {len(deduped_assets)} unique assets, {len(all_links)} links")

    for chunk in _chunked(deduped_assets, 100):
        try:
            resp = (
                db.schema("core").table("media_assets").upsert(chunk, on_conflict="id", default_to_null=False).execute()
            )
            if hasattr(resp, "error") and resp.error:
                print(f"  ERROR upserting assets: {resp.error}", file=sys.stderr)
                stats["errors"] += 1
            else:
                stats["assets_upserted"] += len(chunk)
        except Exception as exc:
            print(f"  ERROR upserting assets: {exc}", file=sys.stderr)
            stats["errors"] += 1

    for chunk in _chunked(all_links, 100):
        try:
            resp = (
                db.schema("core").table("media_links").upsert(chunk, on_conflict="id", default_to_null=False).execute()
            )
            if hasattr(resp, "error") and resp.error:
                print(f"  ERROR upserting links: {resp.error}", file=sys.stderr)
                stats["errors"] += 1
            else:
                stats["links_upserted"] += len(chunk)
        except Exception as exc:
            print(f"  ERROR upserting links: {exc}", file=sys.stderr)
            stats["errors"] += 1

    return stats


# ── Main ──


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    load_env()
    db = create_supabase_admin_client()

    # Resolve show
    resp = db.schema("core").table("shows").select("id,name").eq("name", args.show).execute()
    if not (resp.data or []):
        print(f"Show not found: {args.show}", file=sys.stderr)
        return 1
    show_id = resp.data[0]["id"]
    print(f"Show: {resp.data[0]['name']}  id={show_id}")

    # Build season map
    seasons = (
        db.schema("core")
        .table("seasons")
        .select("id,season_number")
        .eq("show_id", show_id)
        .order("season_number")
        .execute()
    )
    season_map: dict[int, str] = {}
    for s in seasons.data or []:
        sn = s.get("season_number")
        if sn is not None:
            season_map[sn] = s["id"]
    print(f"Seasons in DB: {sorted(season_map.keys())}")

    # Resolve BravoTV person UUID
    client = httpx.Client(
        timeout=30,
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        },
    )

    person_uuid = args.person_uuid
    if not person_uuid and args.person_name:
        print(f"Looking up person: {args.person_name}")
        person_uuid = find_person_uuid(client, args.person_name)
        if not person_uuid:
            print(f"Person not found on BravoTV: {args.person_name}", file=sys.stderr)
            return 1
        print(f"  BravoTV person UUID: {person_uuid}")

    if not person_uuid:
        print("Must provide --person-uuid or --person-name", file=sys.stderr)
        return 1

    # Fetch galleries from BravoTV API
    print(f"\nFetching galleries tagged with person {person_uuid}...")
    galleries = fetch_tagged_galleries(client, person_uuid, limit=args.limit)
    print(f"Found {len(galleries)} galleries")

    # Filter to show-specific galleries
    args.show.lower().replace(" ", "-").replace("the-", "")
    show_galleries = [
        g for g in galleries if g.get("path") and args.show.split()[-1].lower() in (g.get("path") or "").lower()
    ]
    # Include WWHL appearances too
    all_galleries = [g for g in galleries if g.get("path")]
    print(f"  Show-related galleries: {len(show_galleries)}")
    print(f"  All galleries with paths: {len(all_galleries)}")

    # Process each gallery
    print(f"\nProcessing {len(all_galleries)} galleries...")
    all_assets: list[dict[str, Any]] = []
    all_links: list[dict[str, Any]] = []
    processed = 0
    skipped = 0

    for i, gallery in enumerate(all_galleries):
        if args.verbose:
            print(f"\n  [{i + 1}/{len(all_galleries)}] {gallery['title']}")
            print(f"    path: {gallery['path']}")

        assets, links = process_gallery(
            client,
            gallery,
            show_id=show_id,
            season_map=season_map,
            verbose=args.verbose,
        )

        if assets:
            all_assets.extend(assets)
            all_links.extend(links)
            processed += 1
            if args.verbose:
                # Extract season info from first asset metadata
                meta = assets[0].get("metadata", {})
                season = meta.get("season_number", "?")
                people = meta.get("people_names", [])
                print(f"    images: {len(assets)}  season: S{season}  people: {len(people)}")
        else:
            skipped += 1

        time.sleep(0.5)  # rate limit between gallery page fetches

    # Summary
    print(f"\n{'─' * 50}")
    print(f"Galleries processed: {processed}")
    print(f"Galleries skipped: {skipped}")
    print(f"Total images: {len(all_assets)}")
    print(f"Total links: {len(all_links)}")

    # Deduplicate
    unique_asset_ids = {a["id"] for a in all_assets}
    print(f"Unique images: {len(unique_asset_ids)}")

    # Season breakdown
    season_counts: dict[str, int] = {}
    for a in all_assets:
        sn = a.get("metadata", {}).get("season_number")
        key = f"S{sn}" if sn else "Unknown"
        season_counts[key] = season_counts.get(key, 0) + 1
    print(f"By season: {dict(sorted(season_counts.items()))}")

    if args.apply:
        print("\nWriting to database...")
        stats = upsert_assets_and_links(db, all_assets, all_links, verbose=args.verbose)
        print(f"  Assets upserted: {stats['assets_upserted']}")
        print(f"  Links upserted: {stats['links_upserted']}")
        print(f"  Errors: {stats['errors']}")
    else:
        print("\nDry run only. Re-run with --apply to write changes.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
