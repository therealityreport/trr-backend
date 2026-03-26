#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.deps import get_supabase_admin_client
from api.routers import admin_person_images
from trr_backend.utils.env import load_env

GETTY_SOURCE = "getty"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Recompute Getty gallery bucket metadata for a person's Getty cast-photo rows "
            "and linked gallery media assets."
        )
    )
    parser.add_argument("--person-id", required=True, help="core.people UUID")
    parser.add_argument(
        "--discovery-json",
        default=None,
        help="Optional Getty discovery JSON payload from scripts/getty_scrape_json.py.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing.")
    return parser.parse_args()


def _candidate_show_titles_from_asset(asset: dict[str, Any]) -> list[str]:
    candidates: list[str] = []
    event_group_title = str(asset.get("getty_event_group_title") or asset.get("event_name") or "").strip()
    if event_group_title:
        candidates.append(event_group_title)
    search_title = str(asset.get("search_title") or "").strip()
    if search_title:
        candidates.append(search_title)
    title = str(asset.get("title") or "").strip()
    if title:
        candidates.append(title)
        if " - " in title:
            candidates.append(title.split(" - ", 1)[0].strip())
    caption = str(asset.get("caption") or "").strip()
    if caption and " -- " in caption:
        candidates.append(caption.split(" -- ", 1)[0].strip().title())
    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = admin_person_images._normalize_show_lookup_key(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(candidate)
    return deduped


def _resolve_asset_show(
    *,
    asset: dict[str, Any],
    show_lookup_by_alias: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    for candidate in _candidate_show_titles_from_asset(asset):
        resolved = admin_person_images._find_show_row_by_text_fragment(show_lookup_by_alias, candidate)
        if resolved:
            return resolved
    return None


def _build_asset_from_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    asset = dict(metadata.get("getty") or {}) if isinstance(metadata.get("getty"), dict) else {}
    fallback_map = {
        "event_name": metadata.get("getty_event_title"),
        "event_id": metadata.get("getty_event_id"),
        "event_url_slug": metadata.get("getty_event_slug"),
        "event_url": metadata.get("getty_event_url"),
        "event_date": metadata.get("getty_event_date"),
        "source_query_scope": metadata.get("source_query_scope"),
        "grouped_image_count": metadata.get("grouped_image_count"),
        "person_image_count": metadata.get("person_image_count"),
        "caption": metadata.get("caption"),
        "title": metadata.get("title"),
    }
    for key, value in fallback_map.items():
        if key not in asset or asset.get(key) in (None, "", []):
            asset[key] = value
    return asset


def _merge_bucket_metadata(
    *,
    metadata: dict[str, Any],
    source_editorial_id: str | None,
    show_lookup_by_alias: dict[str, dict[str, Any]],
    discovery_assets_by_editorial_id: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], bool]:
    asset = _build_asset_from_metadata(metadata)
    editorial_id = str(asset.get("editorial_id") or source_editorial_id or "").strip()
    discovery_asset = discovery_assets_by_editorial_id.get(editorial_id)
    if discovery_asset:
        asset = {**asset, **discovery_asset}
    if not asset:
        return metadata, False
    resolved_asset_show = _resolve_asset_show(asset=asset, show_lookup_by_alias=show_lookup_by_alias)
    bucket_metadata = admin_person_images._resolve_gallery_bucket_metadata(
        asset=asset,
        resolved_asset_show=resolved_asset_show,
        show_lookup_by_alias=show_lookup_by_alias,
    )
    updated = dict(metadata)
    updated["getty"] = asset
    updated["gallery_bucket"] = dict(bucket_metadata)
    updated.update(dict(bucket_metadata))
    updated["getty_event_title"] = str(asset.get("event_name") or "").strip() or None
    updated["getty_event_id"] = str(asset.get("event_id") or "").strip() or None
    updated["getty_event_slug"] = str(asset.get("event_url_slug") or "").strip() or None
    updated["getty_event_url"] = str(asset.get("event_url") or "").strip() or None
    updated["getty_event_date"] = str(asset.get("event_date") or "").strip() or None
    updated["source_query_scope"] = str(asset.get("source_query_scope") or "").strip() or None
    if bucket_metadata.get("bucket_type") == "show" and bucket_metadata.get("resolved_show_name"):
        updated["show_name"] = bucket_metadata.get("resolved_show_name")
    else:
        updated.pop("show_name", None)
    return updated, updated != metadata


def _fetch_cast_rows(db: Any, person_id: str) -> list[dict[str, Any]]:
    response = (
        db.schema("core")
        .table("cast_photos")
        .select("id,source_image_id,metadata")
        .eq("person_id", person_id)
        .eq("source", GETTY_SOURCE)
        .execute()
    )
    return [dict(row) for row in (response.data or []) if isinstance(row, dict)]


def _fetch_linked_asset_ids(db: Any, person_id: str) -> list[str]:
    response = (
        db.schema("core")
        .table("media_links")
        .select("media_asset_id")
        .eq("entity_type", "person")
        .eq("entity_id", person_id)
        .eq("kind", "gallery")
        .execute()
    )
    return sorted(
        {
            str(row.get("media_asset_id") or "").strip()
            for row in (response.data or [])
            if isinstance(row, dict) and str(row.get("media_asset_id") or "").strip()
        }
    )


def _fetch_assets_by_ids(db: Any, asset_ids: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for start in range(0, len(asset_ids), 200):
        chunk = asset_ids[start : start + 200]
        response = (
            db.schema("core")
            .table("media_assets")
            .select("id,source_asset_id,metadata")
            .eq("source", GETTY_SOURCE)
            .in_("id", chunk)
            .execute()
        )
        rows.extend(dict(row) for row in (response.data or []) if isinstance(row, dict))
    return rows


def _repair_rows(
    *,
    db: Any,
    table_name: str,
    rows: list[dict[str, Any]],
    key_field: str,
    source_editorial_id_field: str,
    show_lookup_by_alias: dict[str, dict[str, Any]],
    discovery_assets_by_editorial_id: dict[str, dict[str, Any]],
    dry_run: bool,
) -> int:
    updated_count = 0
    total = len(rows)
    for index, row in enumerate(rows, start=1):
        metadata = dict(row.get("metadata") or {})
        new_metadata, changed = _merge_bucket_metadata(
            metadata=metadata,
            source_editorial_id=str(row.get(source_editorial_id_field) or "").strip() or None,
            show_lookup_by_alias=show_lookup_by_alias,
            discovery_assets_by_editorial_id=discovery_assets_by_editorial_id,
        )
        if not changed:
            continue
        updated_count += 1
        if not dry_run:
            (
                db.schema("core")
                .table(table_name)
                .update({"metadata": new_metadata})
                .eq(key_field, row[key_field])
                .execute()
            )
        if updated_count % 100 == 0 or index == total:
            print(
                {
                    "table": table_name,
                    "processed": index,
                    "total": total,
                    "updated": updated_count,
                    "dry_run": dry_run,
                },
                flush=True,
            )
    return updated_count


def _load_discovery_assets(path_value: str | None) -> dict[str, dict[str, Any]]:
    if not path_value:
        return {}
    payload = json.loads(Path(path_value).read_text(encoding="utf-8"))
    merged = payload.get("merged") if isinstance(payload, dict) else []
    assets_by_editorial_id: dict[str, dict[str, Any]] = {}
    if not isinstance(merged, list):
        return assets_by_editorial_id
    for entry in merged:
        if not isinstance(entry, dict):
            continue
        editorial_id = str(entry.get("editorial_id") or "").strip()
        if not editorial_id:
            continue
        assets_by_editorial_id[editorial_id] = dict(entry)
    return assets_by_editorial_id


def main() -> None:
    args = _parse_args()
    load_env()
    db = get_supabase_admin_client()
    show_lookup_by_alias, _, _ = admin_person_images._build_show_lookup_maps(db)
    discovery_assets_by_editorial_id = _load_discovery_assets(args.discovery_json)

    cast_rows = _fetch_cast_rows(db, args.person_id)
    asset_rows = _fetch_assets_by_ids(db, _fetch_linked_asset_ids(db, args.person_id))

    updated_cast = _repair_rows(
        db=db,
        table_name="cast_photos",
        rows=cast_rows,
        key_field="id",
        source_editorial_id_field="source_image_id",
        show_lookup_by_alias=show_lookup_by_alias,
        discovery_assets_by_editorial_id=discovery_assets_by_editorial_id,
        dry_run=args.dry_run,
    )
    updated_assets = _repair_rows(
        db=db,
        table_name="media_assets",
        rows=asset_rows,
        key_field="id",
        source_editorial_id_field="source_asset_id",
        show_lookup_by_alias=show_lookup_by_alias,
        discovery_assets_by_editorial_id=discovery_assets_by_editorial_id,
        dry_run=args.dry_run,
    )

    print(
        {
            "person_id": args.person_id,
            "updated_cast_photos": updated_cast,
            "updated_media_assets": updated_assets,
            "dry_run": args.dry_run,
        }
    )


if __name__ == "__main__":
    main()
