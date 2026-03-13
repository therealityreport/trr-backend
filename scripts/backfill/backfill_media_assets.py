#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from typing import Any

from scripts._sync_common import load_env_and_db
from trr_backend.db.session import DbSession
from trr_backend.media.image_variants import generate_media_asset_variants
from trr_backend.repositories.media_assets import upsert_media_with_links


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_media_assets",
        description=(
            "Backfill media_assets/media_links from legacy show/season/episode/person image tables "
            "and cast photos."
        ),
    )
    parser.add_argument(
        "--entity-type",
        choices=["show", "season", "episode", "person", "cast", "all"],
        default="all",
        help="Which entity type to backfill.",
    )
    parser.add_argument(
        "--table",
        choices=["show_images", "season_images", "episode_images", "person_images", "cast_photos"],
        default=None,
        help="Legacy table to backfill (alias for --entity-type).",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on number of rows to process.")
    parser.add_argument(
        "--with-variants",
        action="store_true",
        help="Generate thumb/card/detail variants for backfilled media assets.",
    )
    parser.add_argument(
        "--with-crops",
        action="store_true",
        help="When --with-variants is set, also generate crop variants if metadata.thumbnail_crop exists.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return parser.parse_args(argv)


def _resolve_entity_type(args: argparse.Namespace) -> str:
    if args.table:
        return {
            "show_images": "show",
            "season_images": "season",
            "episode_images": "episode",
            "person_images": "person",
            "cast_photos": "cast",
        }[args.table]
    return args.entity_type


def _fetch_show_images(db: DbSession, *, limit: int | None) -> list[dict[str, Any]]:
    fields = (
        "show_id,source,source_image_id,url,url_path,kind,iso_639_1,file_path,width,height,caption,position,"
        "image_type,metadata,hosted_bucket,hosted_key,hosted_url,hosted_sha256,hosted_content_type,hosted_bytes,"
        "hosted_etag,hosted_at,fetched_at,tmdb_id"
    )
    query = db.schema("core").table("show_images").select(fields)
    if limit is not None:
        query = query.limit(max(0, int(limit)))
    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing core.show_images: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _fetch_season_images(db: DbSession, *, limit: int | None) -> list[dict[str, Any]]:
    fields = (
        "id,show_id,season_id,season_number,source,source_image_id,url,url_original,url_path,kind,iso_639_1,"
        "file_path,width,height,caption,position,image_type,metadata,hosted_bucket,hosted_key,hosted_url,"
        "hosted_sha256,hosted_content_type,hosted_bytes,hosted_etag,hosted_at,fetched_at,tmdb_series_id"
    )
    query = db.schema("core").table("season_images").select(fields)
    if limit is not None:
        query = query.limit(max(0, int(limit)))
    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing core.season_images: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _fetch_episode_images(db: DbSession, *, limit: int | None) -> list[dict[str, Any]]:
    fields = (
        "id,show_id,season_id,episode_id,season_number,episode_number,source,source_image_id,url,url_original,"
        "kind,iso_639_1,file_path,width,height,caption,position,image_type,metadata,hosted_bucket,hosted_key,"
        "hosted_url,hosted_sha256,hosted_content_type,hosted_bytes,hosted_etag,hosted_at,fetched_at,tmdb_series_id"
    )
    query = db.schema("core").table("episode_images").select(fields)
    if limit is not None:
        query = query.limit(max(0, int(limit)))
    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing core.episode_images: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _fetch_person_images(db: DbSession, *, limit: int | None) -> list[dict[str, Any]]:
    fields = (
        "id,person_id,source,url,width,height,caption,is_primary,position,hosted_bucket,hosted_key,hosted_url,"
        "hosted_sha256,hosted_content_type,hosted_bytes,hosted_etag,hosted_at"
    )
    query = db.schema("core").table("person_images").select(fields)
    if limit is not None:
        query = query.limit(max(0, int(limit)))
    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing core.person_images: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _fetch_cast_photos(db: DbSession, *, limit: int | None) -> list[dict[str, Any]]:
    fields = (
        "id,person_id,source,source_image_id,image_url_canonical,image_url,url,thumb_url,width,height,caption,"
        "metadata,hosted_bucket,hosted_key,hosted_url,hosted_sha256,hosted_content_type,hosted_bytes,"
        "hosted_etag,hosted_at,fetched_at,viewer_id,mediaindex_url_path,mediaviewer_url_path,gallery_index,"
        "gallery_total"
    )
    query = db.schema("core").table("cast_photos").select(fields)
    if limit is not None:
        query = query.limit(max(0, int(limit)))
    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing core.cast_photos: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _generate_variants(
    db: DbSession,
    *,
    assets: list[dict[str, Any]],
    with_crops: bool,
    verbose: bool,
) -> int:
    generated = 0
    seen_asset_ids: set[str] = set()
    for asset in assets:
        asset_id = str(asset.get("id") or "").strip()
        if not asset_id or asset_id in seen_asset_ids:
            continue
        seen_asset_ids.add(asset_id)
        generate_media_asset_variants(db, asset_id=asset_id, force=False)
        metadata = asset.get("metadata") if isinstance(asset.get("metadata"), dict) else {}
        crop = metadata.get("thumbnail_crop") if isinstance(metadata, dict) else None
        if with_crops and isinstance(crop, dict):
            generate_media_asset_variants(db, asset_id=asset_id, crop=crop, force=False)
        generated += 1
        if verbose:
            print(f"backfill_media_assets: variants asset_id={asset_id} crop={'yes' if with_crops else 'no'}")
    return generated


def _backfill_entity(
    db: DbSession,
    *,
    entity_type: str,
    rows: list[dict[str, Any]],
    with_variants: bool,
    with_crops: bool,
    verbose: bool,
) -> None:
    if verbose:
        print(f"backfill_media_assets: {entity_type} rows={len(rows)}")
    if not rows:
        return
    assets, links = upsert_media_with_links(db, rows, entity_type=entity_type)
    if verbose:
        print(f"backfill_media_assets: {entity_type} assets={len(assets)} links={len(links)}")
    if with_variants and assets:
        generated = _generate_variants(db, assets=assets, with_crops=with_crops, verbose=verbose)
        if verbose:
            print(f"backfill_media_assets: {entity_type} variant_assets={generated}")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    entity_type = _resolve_entity_type(args)
    db = load_env_and_db()

    if entity_type in ("show", "all"):
        _backfill_entity(
            db,
            entity_type="show",
            rows=_fetch_show_images(db, limit=args.limit),
            with_variants=bool(args.with_variants),
            with_crops=bool(args.with_crops),
            verbose=bool(args.verbose),
        )
    if entity_type in ("season", "all"):
        _backfill_entity(
            db,
            entity_type="season",
            rows=_fetch_season_images(db, limit=args.limit),
            with_variants=bool(args.with_variants),
            with_crops=bool(args.with_crops),
            verbose=bool(args.verbose),
        )
    if entity_type in ("episode", "all"):
        _backfill_entity(
            db,
            entity_type="episode",
            rows=_fetch_episode_images(db, limit=args.limit),
            with_variants=bool(args.with_variants),
            with_crops=bool(args.with_crops),
            verbose=bool(args.verbose),
        )
    if entity_type in ("person", "all"):
        _backfill_entity(
            db,
            entity_type="person",
            rows=_fetch_person_images(db, limit=args.limit),
            with_variants=bool(args.with_variants),
            with_crops=bool(args.with_crops),
            verbose=bool(args.verbose),
        )
    if entity_type in ("cast", "all"):
        _backfill_entity(
            db,
            entity_type="cast",
            rows=_fetch_cast_photos(db, limit=args.limit),
            with_variants=bool(args.with_variants),
            with_crops=bool(args.with_crops),
            verbose=bool(args.verbose),
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
