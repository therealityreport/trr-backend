#!/usr/bin/env python3
"""
Remove Real Housewives Fandom person-gallery media that is not confessional or intro inventory.

Default mode is dry-run. Use --apply to delete matching cast_photos rows, linked media rows,
and hosted objects.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlparse

from trr_backend.db import pg
from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.ingestion.cast_photo_sources import (
    _is_real_housewives_fandom_source,
    _should_keep_real_housewives_fandom_image,
)
from trr_backend.media.s3_mirror import delete_s3_objects, get_public_base_url, get_s3_bucket, get_s3_client
from trr_backend.utils.env import load_env


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cleanup_non_confessional_fandom_person_media",
        description="Delete RH Fandom person-gallery rows that are not confessional or intro inventory.",
    )
    parser.add_argument("--apply", action="store_true", help="Delete matching rows and objects.")
    parser.add_argument("--person-id", action="append", default=[], help="Optional person UUID filter.")
    parser.add_argument("--limit", type=int, default=None, help="Optional row cap for investigation.")
    return parser.parse_args(argv)


def _chunked(values: list[str], size: int = 200) -> Iterable[list[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _normalize_row_id(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _extract_hosted_keys_from_metadata(metadata: Any, *, public_base_url: str) -> set[str]:
    keys: set[str] = set()
    public_prefix = f"{public_base_url.rstrip('/')}/"

    def _walk(value: Any, parent_key: str | None = None) -> None:
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                _walk(nested_value, nested_key)
            return
        if isinstance(value, list):
            for item in value:
                _walk(item, parent_key)
            return
        if not isinstance(value, str):
            return

        text = value.strip()
        if not text:
            return
        if parent_key in {"variant_key", "hosted_key"}:
            keys.add(text.lstrip("/"))
            return
        if text.startswith(public_prefix):
            parsed = urlparse(text)
            path = (parsed.path or "").lstrip("/")
            if path:
                keys.add(path)

    _walk(metadata)
    return keys


def _fetch_people_map(db, person_ids: list[str]) -> dict[str, str | None]:
    if not person_ids:
        return {}
    out: dict[str, str | None] = {}
    for batch in _chunked(person_ids):
        response = db.schema("core").table("people").select("id,full_name").in_("id", batch).execute()
        if getattr(response, "error", None):
            raise RuntimeError(f"Supabase error fetching people: {response.error}")
        for row in response.data or []:
            person_id = _normalize_row_id(row.get("id"))
            if person_id:
                out[person_id] = row.get("full_name")
    return out


def _fetch_candidate_cast_rows(db, *, person_ids: list[str], limit: int | None) -> list[dict[str, Any]]:
    query = (
        db.schema("core")
        .table("cast_photos")
        .select(
            "id,person_id,source,source_image_id,url,source_page_url,caption,context_type,hosted_bucket,hosted_key,metadata"
        )
        .in_("source", ["fandom", "fandom-gallery"])
    )
    if person_ids:
        query = query.in_("person_id", person_ids)
    if limit:
        query = query.limit(limit)
    response = query.execute()
    if getattr(response, "error", None):
        raise RuntimeError(f"Supabase error fetching cast_photos: {response.error}")
    return response.data or []


def _fetch_target_media_links(cast_photo_ids: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for batch in _chunked(cast_photo_ids):
        rows.extend(
            pg.fetch_all(
                """
                SELECT id, media_asset_id, entity_type, kind, context
                FROM core.media_links
                WHERE entity_type = 'person'
                  AND kind = 'gallery'
                  AND context->>'legacy_table' = 'cast_photos'
                  AND context->>'legacy_id' = ANY(%s)
                """,
                [batch],
            )
        )
    return rows


def _fetch_all_media_links_for_assets(asset_ids: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for batch in _chunked(asset_ids):
        rows.extend(
            pg.fetch_all(
                """
                SELECT id, media_asset_id, entity_type, kind, context
                FROM core.media_links
                WHERE media_asset_id::text = ANY(%s)
                """,
                [batch],
            )
        )
    return rows


def _fetch_media_assets(db, asset_ids: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for batch in _chunked(asset_ids):
        response = (
            db.schema("core")
            .table("media_assets")
            .select("id,hosted_bucket,hosted_key,source_url")
            .in_("id", batch)
            .execute()
        )
        if getattr(response, "error", None):
            raise RuntimeError(f"Supabase error fetching media_assets: {response.error}")
        rows.extend(response.data or [])
    return rows


def _fetch_media_asset_variants(db, asset_ids: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for batch in _chunked(asset_ids):
        response = (
            db.schema("core")
            .table("media_asset_variants")
            .select("id,media_asset_id,hosted_bucket,hosted_key")
            .in_("media_asset_id", batch)
            .execute()
        )
        if getattr(response, "error", None):
            raise RuntimeError(f"Supabase error fetching media_asset_variants: {response.error}")
        rows.extend(response.data or [])
    return rows


def _delete_rows(db, *, table: str, ids: list[str]) -> int:
    if not ids:
        return 0
    deleted = 0
    for batch in _chunked(ids):
        response = db.schema("core").table(table).delete().in_("id", batch).execute()
        if getattr(response, "error", None):
            raise RuntimeError(f"Supabase error deleting {table}: {response.error}")
        deleted += len(response.data or [])
    return deleted


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    load_env()

    db = create_supabase_admin_client()
    public_base_url = get_public_base_url()
    default_bucket = get_s3_bucket()

    cast_rows = _fetch_candidate_cast_rows(
        db,
        person_ids=[str(value).strip() for value in args.person_id if str(value).strip()],
        limit=args.limit,
    )

    target_rows: list[dict[str, Any]] = []
    for row in cast_rows:
        source_url = str(row.get("url") or "").strip() or None
        source_page_url = str(row.get("source_page_url") or "").strip() or None
        caption = str(row.get("caption") or "").strip() or None
        context_type = str(row.get("context_type") or "").strip() or None
        if not _is_real_housewives_fandom_source(source_url, source_page_url):
            continue
        if _should_keep_real_housewives_fandom_image(context_type, caption, source_url, source_page_url):
            continue
        target_rows.append(row)

    target_cast_ids = [str(row["id"]) for row in target_rows if row.get("id")]
    target_media_links = _fetch_target_media_links(target_cast_ids) if target_cast_ids else []
    asset_ids = list(
        dict.fromkeys(
            _normalize_row_id(row.get("media_asset_id"))
            for row in target_media_links
            if _normalize_row_id(row.get("media_asset_id"))
        )
    )
    media_links = _fetch_all_media_links_for_assets(asset_ids) if asset_ids else []
    links_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    target_link_ids = [link_id for row in target_media_links if (link_id := _normalize_row_id(row.get("id")))]
    for row in media_links:
        asset_id = _normalize_row_id(row.get("media_asset_id"))
        if not asset_id:
            continue
        links_by_asset[asset_id].append(row)

    target_link_id_set = set(target_link_ids)
    delete_asset_ids: list[str] = []
    for asset_id, links in links_by_asset.items():
        link_ids = {_normalize_row_id(link.get("id")) for link in links}
        link_ids.discard(None)
        if link_ids and link_ids.issubset(target_link_id_set):
            delete_asset_ids.append(asset_id)

    media_assets = _fetch_media_assets(db, delete_asset_ids) if delete_asset_ids else []
    media_variants = _fetch_media_asset_variants(db, delete_asset_ids) if delete_asset_ids else []

    s3_keys_by_bucket: dict[str, set[str]] = defaultdict(set)
    for row in target_rows:
        bucket = str(row.get("hosted_bucket") or "").strip() or default_bucket
        hosted_key = str(row.get("hosted_key") or "").strip()
        if hosted_key:
            s3_keys_by_bucket[bucket].add(hosted_key)
        for key in _extract_hosted_keys_from_metadata(row.get("metadata"), public_base_url=public_base_url):
            s3_keys_by_bucket[bucket].add(key)

    for row in media_assets:
        bucket = str(row.get("hosted_bucket") or "").strip() or default_bucket
        hosted_key = str(row.get("hosted_key") or "").strip()
        if hosted_key:
            s3_keys_by_bucket[bucket].add(hosted_key)

    variant_row_ids: list[str] = []
    for row in media_variants:
        variant_id = _normalize_row_id(row.get("id"))
        if variant_id:
            variant_row_ids.append(variant_id)
        bucket = str(row.get("hosted_bucket") or "").strip() or default_bucket
        hosted_key = str(row.get("hosted_key") or "").strip()
        if hosted_key:
            s3_keys_by_bucket[bucket].add(hosted_key)

    people_map = _fetch_people_map(
        db,
        list(
            dict.fromkeys(str(row.get("person_id")) for row in target_rows if str(row.get("person_id") or "").strip())
        ),
    )

    print(f"candidate_cast_photos={len(cast_rows)}")
    print(f"target_cast_photos={len(target_cast_ids)}")
    print(f"target_media_links={len(target_link_ids)}")
    print(f"target_media_assets={len(delete_asset_ids)}")
    print(f"target_media_asset_variants={len(variant_row_ids)}")
    print(f"target_s3_objects={sum(len(keys) for keys in s3_keys_by_bucket.values())}")
    for row in target_rows[:10]:
        person_id = str(row.get("person_id") or "")
        print(
            "sample"
            f" person={people_map.get(person_id) or person_id}"
            f" cast_photo_id={row.get('id')}"
            f" source={row.get('source')}"
            f" caption={row.get('caption')!r}"
            f" url={row.get('url')}"
        )

    if not args.apply:
        print("Dry run only. Re-run with --apply to delete.")
        return 0

    s3_client = get_s3_client()
    deleted_objects = 0
    for bucket, keys in s3_keys_by_bucket.items():
        deleted_objects += delete_s3_objects(s3_client, bucket, sorted(keys))

    deleted_links = _delete_rows(db, table="media_links", ids=target_link_ids)
    deleted_variants = _delete_rows(db, table="media_asset_variants", ids=variant_row_ids)
    deleted_assets = _delete_rows(db, table="media_assets", ids=delete_asset_ids)
    deleted_cast = _delete_rows(db, table="cast_photos", ids=target_cast_ids)

    print(f"deleted_s3_objects={deleted_objects}")
    print(f"deleted_media_links={deleted_links}")
    print(f"deleted_media_asset_variants={deleted_variants}")
    print(f"deleted_media_assets={deleted_assets}")
    print(f"deleted_cast_photos={deleted_cast}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
