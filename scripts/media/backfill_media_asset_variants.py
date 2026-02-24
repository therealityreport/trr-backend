#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from typing import Any

from scripts._sync_common import load_env_and_db
from trr_backend.media.image_variants import generate_media_asset_variants


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_media_asset_variants",
        description="Backfill media_asset_variants rows and metadata URLs for existing media assets.",
    )
    parser.add_argument("--batch-size", type=int, default=50, help="Assets fetched per batch.")
    parser.add_argument("--start-offset", type=int, default=0, help="Initial row offset.")
    parser.add_argument(
        "--max-assets",
        type=int,
        default=None,
        help="Optional cap on processed assets (default: process all).",
    )
    parser.add_argument(
        "--source-contains",
        type=str,
        default=None,
        help="Optional source filter (ILIKE %%value%%).",
    )
    parser.add_argument("--force", action="store_true", help="Regenerate even when variants exist.")
    parser.add_argument(
        "--with-crops",
        action="store_true",
        help="Also backfill crop variants when metadata.thumbnail_crop exists.",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose progress output.")
    return parser.parse_args(argv)


def _fetch_batch(
    db,
    *,
    offset: int,
    batch_size: int,
    source_contains: str | None,
) -> list[dict[str, Any]]:
    query = (
        db.schema("core")
        .table("media_assets")
        .select("id, metadata")
        .not_.is_("hosted_url", "null")
        .order("created_at", desc=True)
        .range(offset, offset + batch_size - 1)
    )
    if source_contains:
        query = query.ilike("source", f"%{source_contains}%")
    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing media assets: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _extract_crop(metadata: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(metadata, dict):
        return None
    crop = metadata.get("thumbnail_crop")
    if not isinstance(crop, dict):
        return None
    if all(key in crop for key in ("x", "y", "zoom")):
        return crop
    return None


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    batch_size = max(1, int(args.batch_size))
    offset = max(0, int(args.start_offset))
    remaining = None if args.max_assets is None else max(0, int(args.max_assets))

    db = load_env_and_db()

    processed = 0
    succeeded = 0
    failed = 0
    crop_generated = 0

    while True:
        if remaining is not None and remaining <= 0:
            break
        current_batch_size = min(batch_size, remaining) if remaining is not None else batch_size
        rows = _fetch_batch(
            db,
            offset=offset,
            batch_size=current_batch_size,
            source_contains=args.source_contains,
        )
        if not rows:
            break

        for row in rows:
            processed += 1
            asset_id = str(row.get("id") or "").strip()
            if not asset_id:
                failed += 1
                continue

            try:
                base = generate_media_asset_variants(db, asset_id=asset_id, force=bool(args.force))
                if args.with_crops:
                    crop = _extract_crop(row.get("metadata") if isinstance(row.get("metadata"), dict) else None)
                    if crop is not None:
                        crop_rows = generate_media_asset_variants(
                            db, asset_id=asset_id, crop=crop, force=bool(args.force)
                        )
                        if crop_rows:
                            crop_generated += 1
                succeeded += 1
                if args.verbose:
                    print(f"[ok] {asset_id} base_variants={len(base)} crop={'yes' if args.with_crops else 'no'}")
            except Exception as exc:  # pragma: no cover - operational script
                failed += 1
                print(f"[error] {asset_id}: {exc}", file=sys.stderr)

        offset += len(rows)
        if remaining is not None:
            remaining -= len(rows)

    print(
        "backfill_media_asset_variants: "
        f"processed={processed} succeeded={succeeded} failed={failed} "
        f"crop_assets={crop_generated}"
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
