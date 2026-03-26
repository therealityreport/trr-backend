#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.deps import get_supabase_admin_client
from api.routers.admin_person_images import (
    _mirror_person_media_assets,
    _mirror_person_photos,
    _should_reset_getty_hosted_state,
)
from trr_backend.integrations import getty
from trr_backend.utils.env import load_env

logger = logging.getLogger("repair_person_getty_originals")

GETTY_SOURCE = "getty"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Repair Getty cast-photo/media-asset rows for a person by fetching canonical "
            "Getty detail records from editorial IDs, then re-mirroring repaired rows."
        )
    )
    parser.add_argument("--person-id", required=True, help="core.people UUID")
    parser.add_argument(
        "--detail-workers",
        type=int,
        default=6,
        help="Parallel Getty detail fetch workers (default: 6).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be repaired without writing or mirroring.",
    )
    return parser.parse_args()


def _normalize_url(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _normalize_canonical_url(value: Any) -> str | None:
    normalized = _normalize_url(value)
    if not normalized:
        return None
    return normalized.split("?", 1)[0].strip() or None


def _is_getty_preview_url(value: Any) -> bool:
    normalized = _normalize_url(value)
    if not normalized:
        return False
    return normalized.startswith("https://media.gettyimages.com/") and "p=1" in normalized


def _get_media_getty_size_hint(value: Any) -> tuple[int | None, int | None]:
    normalized = _normalize_url(value)
    if not normalized:
        return None, None
    parsed = urlparse(normalized)
    size_hint = next(iter(parse_qs(parsed.query).get("s", [])), "").strip().lower()
    if "x" not in size_hint:
        return None, None
    width_raw, height_raw = size_hint.split("x", 1)
    try:
        width = int(width_raw)
        height = int(height_raw)
    except ValueError:
        return None, None
    return width if width > 0 else None, height if height > 0 else None


def _is_weak_getty_original_url(value: Any, metadata: dict[str, Any] | None = None) -> bool:
    normalized = _normalize_url(value)
    if not normalized:
        return True
    if not normalized.startswith("https://media.gettyimages.com/"):
        return False
    metadata_dict = metadata or {}
    if str(metadata_dict.get("source_resolution") or "").strip() == "getty_discovery_preview":
        return True
    width, height = _get_media_getty_size_hint(normalized)
    if isinstance(width, int) and width < 1200:
        return True
    if isinstance(height, int) and height < 1200:
        return True
    return False


def _coerce_metadata(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _chunked(values: Iterable[str], size: int) -> list[list[str]]:
    items = list(values)
    return [items[index : index + size] for index in range(0, len(items), size)]


def _fetch_person_imdb_id(db: Any, person_id: str) -> str | None:
    response = (
        db.schema("core")
        .table("people")
        .select("imdb_id")
        .eq("id", person_id)
        .limit(1)
        .execute()
    )
    rows = response.data or []
    if not isinstance(rows, list) or not rows:
        return None
    imdb_id = str(rows[0].get("imdb_id") or "").strip()
    return imdb_id or None


def _fetch_person_getty_cast_rows(db: Any, person_id: str) -> list[dict[str, Any]]:
    response = (
        db.schema("core")
        .table("cast_photos")
        .select(
            "id, source_image_id, url, url_path, image_url, image_url_canonical, thumb_url, "
            "source_page_url, width, height, hosted_url, hosted_key, hosted_bucket, hosted_sha256, "
            "hosted_content_type, hosted_bytes, hosted_etag, hosted_at, metadata"
        )
        .eq("person_id", person_id)
        .eq("source", GETTY_SOURCE)
        .execute()
    )
    rows = response.data or []
    return [dict(row) for row in rows if isinstance(row, dict)]


def _fetch_person_getty_linked_assets(db: Any, person_id: str) -> list[dict[str, Any]]:
    response = (
        db.schema("core")
        .table("media_links")
        .select("media_asset_id")
        .eq("entity_type", "person")
        .eq("entity_id", person_id)
        .eq("kind", "gallery")
        .execute()
    )
    asset_ids = sorted(
        {
            str(row.get("media_asset_id") or "").strip()
            for row in (response.data or [])
            if isinstance(row, dict) and str(row.get("media_asset_id") or "").strip()
        }
    )
    if not asset_ids:
        return []

    assets: list[dict[str, Any]] = []
    select_columns = (
        "id, source, source_asset_id, source_url, width, height, hosted_url, hosted_key, "
        "hosted_bucket, hosted_sha256, hosted_content_type, hosted_bytes, hosted_etag, hosted_at, "
        "ingest_status, ingest_last_error, ingest_retry_count, ingest_failed_at, "
        "ingest_completed_at, ingest_next_retry_at, metadata"
    )
    for chunk in _chunked(asset_ids, 200):
        asset_response = (
            db.schema("core")
            .table("media_assets")
            .select(select_columns)
            .eq("source", GETTY_SOURCE)
            .in_("id", chunk)
            .execute()
        )
        for row in asset_response.data or []:
            if not isinstance(row, dict):
                continue
            asset_row = dict(row)
            asset_row["media_asset_id"] = str(asset_row.get("id") or "").strip()
            assets.append(asset_row)
    return assets


def _merge_getty_metadata(metadata: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    merged = dict(metadata)
    original_url = _normalize_url(detail.get("original_image_url"))
    preview_url = _normalize_url(detail.get("preview_image_url"))
    detail_url = _normalize_url(detail.get("detail_url"))
    title = _normalize_url(detail.get("title"))
    caption = _normalize_url(detail.get("caption"))
    editorial_id = _normalize_url(detail.get("editorial_id"))

    if original_url:
        merged["getty_original_image_url"] = original_url
        merged["source_url"] = original_url
        merged["original_source_url"] = original_url
        merged["original_source_file_url"] = original_url
    if preview_url:
        merged["getty_preview_image_url"] = preview_url
    if detail_url:
        merged["source_page_url"] = detail_url
        merged["original_source_page_url"] = detail_url
        merged["getty_detail_page_url"] = detail_url
    if title:
        merged["title"] = title
    if caption:
        merged["caption"] = caption
    if editorial_id:
        merged["getty_editorial_id"] = editorial_id
    return merged


def _fetch_getty_details(editorial_ids: list[str], *, max_workers: int) -> dict[str, dict[str, Any] | None]:
    def _fetch(editorial_id: str) -> tuple[str, dict[str, Any] | None]:
        detail_url = f"https://www.gettyimages.com/detail/news-photo/x/{editorial_id}"
        return editorial_id, getty.fetch_asset_detail(detail_url)

    results: dict[str, dict[str, Any] | None] = {}
    if not editorial_ids:
        return results
    worker_count = max(1, min(max_workers, len(editorial_ids)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {executor.submit(_fetch, editorial_id): editorial_id for editorial_id in editorial_ids}
        for future in as_completed(future_map):
            editorial_id = future_map[future]
            try:
                resolved_id, detail = future.result()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Getty detail fetch failed editorial_id=%s error=%s", editorial_id, exc)
                results[editorial_id] = None
                continue
            results[resolved_id] = detail
    return results


def _build_cast_patch(row: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    metadata = _coerce_metadata(row.get("metadata"))
    merged_metadata = _merge_getty_metadata(metadata, detail)
    desired_original_url = _normalize_url(merged_metadata.get("getty_original_image_url"))
    desired_preview_url = _normalize_url(merged_metadata.get("getty_preview_image_url"))
    desired_source_page_url = _normalize_url(merged_metadata.get("getty_detail_page_url"))
    desired_canonical_url = _normalize_canonical_url(desired_original_url)

    patch: dict[str, Any] = {}
    if desired_original_url and _normalize_url(row.get("url")) != desired_original_url:
        patch["url"] = desired_original_url
        patch["url_path"] = urlparse(desired_original_url).path or None
    if desired_original_url and _normalize_url(row.get("image_url")) != desired_original_url:
        patch["image_url"] = desired_original_url
    if desired_canonical_url and _normalize_url(row.get("image_url_canonical")) != desired_canonical_url:
        patch["image_url_canonical"] = desired_canonical_url
    if desired_preview_url and _normalize_url(row.get("thumb_url")) != desired_preview_url:
        patch["thumb_url"] = desired_preview_url
    if desired_source_page_url and _normalize_url(row.get("source_page_url")) != desired_source_page_url:
        patch["source_page_url"] = desired_source_page_url
    if merged_metadata != metadata:
        patch["metadata"] = merged_metadata
    if _should_reset_getty_hosted_state(
        desired_original_url=desired_original_url,
        current_source_url=row.get("url") or row.get("image_url"),
        hosted_url=row.get("hosted_url"),
        hosted_key=row.get("hosted_key"),
        metadata=metadata,
    ):
        patch.update(
            {
                "hosted_bucket": None,
                "hosted_key": None,
                "hosted_url": None,
                "hosted_sha256": None,
                "hosted_content_type": None,
                "hosted_bytes": None,
                "hosted_etag": None,
                "hosted_at": None,
            }
        )
    return patch


def _build_asset_patch(row: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    metadata = _coerce_metadata(row.get("metadata"))
    merged_metadata = _merge_getty_metadata(metadata, detail)
    desired_original_url = _normalize_url(merged_metadata.get("getty_original_image_url"))

    patch: dict[str, Any] = {}
    if desired_original_url and _normalize_url(row.get("source_url")) != desired_original_url:
        patch["source_url"] = desired_original_url
    if merged_metadata != metadata:
        patch["metadata"] = merged_metadata
    if _should_reset_getty_hosted_state(
        desired_original_url=desired_original_url,
        current_source_url=row.get("source_url"),
        hosted_url=row.get("hosted_url"),
        hosted_key=row.get("hosted_key"),
        metadata=metadata,
    ):
        patch.update(
            {
                "hosted_bucket": None,
                "hosted_key": None,
                "hosted_url": None,
                "hosted_sha256": None,
                "hosted_content_type": None,
                "hosted_bytes": None,
                "hosted_etag": None,
                "hosted_at": None,
                "ingest_status": "pending",
                "ingest_last_error": None,
                "ingest_retry_count": 0,
                "ingest_failed_at": None,
                "ingest_completed_at": None,
                "ingest_next_retry_at": None,
                "sha256": None,
            }
        )
    return patch


def main() -> int:
    args = _parse_args()
    load_env()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    person_id = str(args.person_id).strip()
    db = get_supabase_admin_client()
    imdb_person_id = _fetch_person_imdb_id(db, person_id)

    cast_rows = _fetch_person_getty_cast_rows(db, person_id)
    asset_rows = _fetch_person_getty_linked_assets(db, person_id)

    candidate_cast_rows = [
        row
        for row in cast_rows
        if _is_getty_preview_url(row.get("url"))
        or _is_weak_getty_original_url(
            _coerce_metadata(row.get("metadata")).get("getty_original_image_url"),
            _coerce_metadata(row.get("metadata")),
        )
        or not _normalize_url(row.get("hosted_key"))
        or not _normalize_url(_coerce_metadata(row.get("metadata")).get("getty_original_image_url"))
    ]
    candidate_asset_rows = [
        row
        for row in asset_rows
        if _is_getty_preview_url(row.get("source_url"))
        or _is_weak_getty_original_url(
            _coerce_metadata(row.get("metadata")).get("getty_original_image_url"),
            _coerce_metadata(row.get("metadata")),
        )
        or not _normalize_url(row.get("hosted_key"))
        or not _normalize_url(_coerce_metadata(row.get("metadata")).get("getty_original_image_url"))
    ]

    detail_ids = sorted(
        {
            str(row.get("source_image_id") or "").strip()
            for row in candidate_cast_rows
            if _is_weak_getty_original_url(
                _coerce_metadata(row.get("metadata")).get("getty_original_image_url"),
                _coerce_metadata(row.get("metadata")),
            )
            or not _normalize_url(_coerce_metadata(row.get("metadata")).get("getty_original_image_url"))
        }
        | {
            str(row.get("source_asset_id") or "").strip()
            for row in candidate_asset_rows
            if _is_weak_getty_original_url(
                _coerce_metadata(row.get("metadata")).get("getty_original_image_url"),
                _coerce_metadata(row.get("metadata")),
            )
            or not _normalize_url(_coerce_metadata(row.get("metadata")).get("getty_original_image_url"))
        }
    )
    detail_ids = [value for value in detail_ids if value]

    logger.info(
        "Loaded %s Getty cast rows and %s linked Getty assets for person_id=%s",
        len(cast_rows),
        len(asset_rows),
        person_id,
    )
    logger.info(
        "Candidate Getty repairs: %s cast rows, %s media assets, %s missing-detail editorial IDs",
        len(candidate_cast_rows),
        len(candidate_asset_rows),
        len(detail_ids),
    )

    details_by_id = _fetch_getty_details(detail_ids, max_workers=args.detail_workers)

    cast_updates = 0
    asset_updates = 0
    detail_failures = 0
    repaired_photo_ids: list[str] = []
    repaired_asset_ids: list[str] = []

    for row in candidate_cast_rows:
        row_id = str(row.get("id") or "").strip()
        editorial_id = str(row.get("source_image_id") or "").strip()
        metadata = _coerce_metadata(row.get("metadata"))
        detail = None
        if _normalize_url(metadata.get("getty_original_image_url")) and not _is_weak_getty_original_url(
            metadata.get("getty_original_image_url"),
            metadata,
        ):
            detail = {
                "editorial_id": editorial_id,
                "original_image_url": metadata.get("getty_original_image_url"),
                "preview_image_url": metadata.get("getty_preview_image_url") or row.get("thumb_url"),
                "detail_url": (
                    metadata.get("getty_detail_page_url")
                    or metadata.get("source_page_url")
                    or row.get("source_page_url")
                ),
                "title": metadata.get("title"),
                "caption": metadata.get("caption"),
            }
        else:
            detail = details_by_id.get(editorial_id)
            if detail is None:
                detail_failures += 1
                continue
        patch = _build_cast_patch(row, detail)
        if not patch:
            if not _normalize_url(row.get("hosted_key")):
                repaired_photo_ids.append(row_id)
            continue
        if args.dry_run:
            logger.info("DRY RUN cast row %s patch_keys=%s", row_id, sorted(patch.keys()))
        else:
            db.schema("core").table("cast_photos").update(patch).eq("id", row_id).execute()
        cast_updates += 1
        repaired_photo_ids.append(row_id)

    assets_by_source_asset_id: dict[str, list[dict[str, Any]]] = {}
    for asset_row in candidate_asset_rows:
        source_asset_id = str(asset_row.get("source_asset_id") or "").strip()
        if source_asset_id:
            assets_by_source_asset_id.setdefault(source_asset_id, []).append(asset_row)

    for source_asset_id, rows in assets_by_source_asset_id.items():
        for row in rows:
            asset_id = str(row.get("media_asset_id") or row.get("id") or "").strip()
            metadata = _coerce_metadata(row.get("metadata"))
            detail = None
            if _normalize_url(metadata.get("getty_original_image_url")) and not _is_weak_getty_original_url(
                metadata.get("getty_original_image_url"),
                metadata,
            ):
                detail = {
                    "editorial_id": source_asset_id,
                    "original_image_url": metadata.get("getty_original_image_url"),
                    "preview_image_url": metadata.get("getty_preview_image_url"),
                    "detail_url": metadata.get("getty_detail_page_url") or metadata.get("source_page_url"),
                    "title": metadata.get("title"),
                    "caption": metadata.get("caption"),
                }
            else:
                detail = details_by_id.get(source_asset_id)
                if detail is None:
                    detail_failures += 1
                    continue
            patch = _build_asset_patch(row, detail)
            if not patch:
                if not _normalize_url(row.get("hosted_key")):
                    repaired_asset_ids.append(asset_id)
                continue
            if args.dry_run:
                logger.info("DRY RUN media asset %s patch_keys=%s", asset_id, sorted(patch.keys()))
            else:
                db.schema("core").table("media_assets").update(patch).eq("id", asset_id).execute()
            asset_updates += 1
            repaired_asset_ids.append(asset_id)

    mirrored_photos = 0
    failed_photos = 0
    mirrored_assets = 0
    failed_assets = 0
    if not args.dry_run:
        if repaired_photo_ids:
            mirrored_photos, failed_photos = _mirror_person_photos(
                db,
                person_id,
                imdb_person_id,
                photo_ids=sorted(set(repaired_photo_ids)),
            )
        if repaired_asset_ids:
            mirrored_assets, failed_assets = _mirror_person_media_assets(
                db,
                person_id,
                asset_ids=sorted(set(repaired_asset_ids)),
            )

    logger.info(
        (
            "SUMMARY cast_updates=%s asset_updates=%s detail_failures=%s "
            "mirrored_photos=%s failed_photos=%s mirrored_assets=%s failed_assets=%s"
        ),
        cast_updates,
        asset_updates,
        detail_failures,
        mirrored_photos,
        failed_photos,
        mirrored_assets,
        failed_assets,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
