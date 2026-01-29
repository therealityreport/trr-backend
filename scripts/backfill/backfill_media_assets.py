#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from typing import Any

from scripts._sync_common import load_env_and_db
from trr_backend.db.session import DbSession
from trr_backend.repositories.media_assets import upsert_media_with_links


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_media_assets",
        description="Backfill media_assets/media_links from legacy show_images/person_images tables.",
    )
    parser.add_argument(
        "--entity-type",
        choices=["show", "person", "all"],
        default="all",
        help="Which entity type to backfill.",
    )
    parser.add_argument(
        "--table",
        choices=["show_images", "person_images"],
        default=None,
        help="Legacy table to backfill (alias for --entity-type).",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on number of rows to process.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return parser.parse_args(argv)


def _resolve_entity_type(args: argparse.Namespace) -> str:
    if args.table:
        return "show" if args.table == "show_images" else "person"
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


def _fetch_person_images(db: DbSession, *, limit: int | None) -> list[dict[str, Any]]:
    fields = "person_id,source,url,width,height,caption,is_primary"
    query = db.schema("core").table("person_images").select(fields)
    if limit is not None:
        query = query.limit(max(0, int(limit)))
    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing core.person_images: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _backfill_show_images(db: DbSession, *, limit: int | None, verbose: bool) -> None:
    rows = _fetch_show_images(db, limit=limit)
    if verbose:
        print(f"backfill_media_assets: show_images rows={len(rows)}")
    if not rows:
        return
    assets, links = upsert_media_with_links(db, rows, entity_type="show")
    if verbose:
        print(f"backfill_media_assets: show assets={len(assets)} links={len(links)}")


def _backfill_person_images(db: DbSession, *, limit: int | None, verbose: bool) -> None:
    rows = _fetch_person_images(db, limit=limit)
    if verbose:
        print(f"backfill_media_assets: person_images rows={len(rows)}")
    if not rows:
        return
    assets, links = upsert_media_with_links(db, rows, entity_type="person")
    if verbose:
        print(f"backfill_media_assets: person assets={len(assets)} links={len(links)}")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    entity_type = _resolve_entity_type(args)
    db = load_env_and_db()

    if entity_type in ("show", "all"):
        _backfill_show_images(db, limit=args.limit, verbose=args.verbose)
    if entity_type in ("person", "all"):
        _backfill_person_images(db, limit=args.limit, verbose=args.verbose)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
