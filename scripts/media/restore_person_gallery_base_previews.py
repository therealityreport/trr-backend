#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Any

from scripts._sync_common import load_env_and_db
from trr_backend.media.image_variants import generate_cast_photo_variants


@dataclass(frozen=True)
class RestoreTarget:
    person_id: str
    person_name: str


@dataclass(frozen=True)
class RestoreSummary:
    person_id: str
    person_name: str
    cast_total: int
    media_links_total: int
    cast_crop_rows: int
    media_link_crop_rows: int
    cast_updated: int
    media_links_updated: int
    base_variants_regenerated: int


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="restore_person_gallery_base_previews",
        description=(
            "Reset person gallery auto-crop preview state to base previews only "
            "(cast_photos metadata + media_links context)."
        ),
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--person-id", type=str, help="Target person UUID.")
    target.add_argument("--person-name", type=str, help="Target person full name (case-insensitive exact match).")
    parser.add_argument("--apply", action="store_true", help="Apply updates. Default is dry-run.")
    parser.add_argument(
        "--force-base-variants",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Regenerate base cast-photo variants for updated rows.",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose row-level output.")
    return parser.parse_args(argv)


def _normalize_metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _is_auto_crop(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    return str(value.get("mode") or "").strip().lower() == "auto"


def _cast_row_needs_reset(metadata: dict[str, Any]) -> bool:
    active_crop_signature = str(metadata.get("active_crop_signature") or "").strip()
    return bool(active_crop_signature) or _is_auto_crop(metadata.get("thumbnail_crop"))


def _media_link_row_needs_reset(context: dict[str, Any]) -> bool:
    return _is_auto_crop(context.get("thumbnail_crop"))


def _clean_cast_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(metadata)
    cleaned.pop("thumbnail_crop", None)
    cleaned.pop("active_crop_signature", None)
    cleaned.pop("crop_display_url", None)
    cleaned.pop("crop_detail_url", None)
    return cleaned


def _clean_media_context(context: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(context)
    cleaned.pop("thumbnail_crop", None)
    return cleaned


def _resolve_target(db, *, person_id: str | None, person_name: str | None) -> RestoreTarget:
    table = db.schema("core").table("people")
    query = table.select("id,full_name").limit(5)

    if person_id:
        query = query.eq("id", person_id)
    else:
        normalized_name = str(person_name or "").strip()
        if not normalized_name:
            raise RuntimeError("person_name_required")
        query = query.ilike("full_name", normalized_name)

    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"person_lookup_failed: {response.error}")

    rows = response.data or []
    if not rows:
        raise RuntimeError("person_not_found")
    if len(rows) > 1:
        raise RuntimeError("person_name_ambiguous")

    row = rows[0] if isinstance(rows[0], dict) else {}
    resolved_person_id = str(row.get("id") or "").strip()
    resolved_name = str(row.get("full_name") or "").strip() or "Unknown"
    if not resolved_person_id:
        raise RuntimeError("person_not_found")
    return RestoreTarget(person_id=resolved_person_id, person_name=resolved_name)


def _fetch_cast_rows(db, person_id: str) -> list[dict[str, Any]]:
    response = db.schema("core").table("cast_photos").select("id,metadata").eq("person_id", person_id).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"cast_lookup_failed: {response.error}")
    rows = response.data or []
    return rows if isinstance(rows, list) else []


def _fetch_media_link_rows(db, person_id: str) -> list[dict[str, Any]]:
    response = (
        db.schema("core")
        .table("media_links")
        .select("id,context")
        .eq("entity_type", "person")
        .eq("entity_id", person_id)
        .eq("kind", "gallery")
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"media_link_lookup_failed: {response.error}")
    rows = response.data or []
    return rows if isinstance(rows, list) else []


def restore_person_gallery_base_previews(
    db,
    *,
    person_id: str,
    person_name: str,
    apply_updates: bool,
    force_base_variants: bool,
    verbose: bool,
) -> RestoreSummary:
    cast_rows = _fetch_cast_rows(db, person_id)
    media_link_rows = _fetch_media_link_rows(db, person_id)

    cast_to_reset: list[dict[str, Any]] = []
    for row in cast_rows:
        metadata = _normalize_metadata_dict(row.get("metadata"))
        if _cast_row_needs_reset(metadata):
            cast_to_reset.append({"id": str(row.get("id") or ""), "metadata": metadata})

    links_to_reset: list[dict[str, Any]] = []
    for row in media_link_rows:
        context = _normalize_metadata_dict(row.get("context"))
        if _media_link_row_needs_reset(context):
            links_to_reset.append({"id": str(row.get("id") or ""), "context": context})

    cast_updated = 0
    media_links_updated = 0
    base_variants_regenerated = 0

    if apply_updates:
        for row in cast_to_reset:
            cast_id = row["id"]
            if not cast_id:
                continue
            cleaned_metadata = _clean_cast_metadata(row["metadata"])
            (db.schema("core").table("cast_photos").update({"metadata": cleaned_metadata}).eq("id", cast_id).execute())
            cast_updated += 1
            if verbose:
                print(f"[cast] reset metadata {cast_id}")

            if force_base_variants:
                try:
                    generate_cast_photo_variants(db, photo_id=cast_id, crop=None, force=True)
                    base_variants_regenerated += 1
                    if verbose:
                        print(f"[cast] regenerated base variants {cast_id}")
                except Exception as exc:  # pragma: no cover - operational logging
                    print(f"[warn] failed base variant regen for {cast_id}: {exc}")

        for row in links_to_reset:
            link_id = row["id"]
            if not link_id:
                continue
            cleaned_context = _clean_media_context(row["context"])
            (db.schema("core").table("media_links").update({"context": cleaned_context}).eq("id", link_id).execute())
            media_links_updated += 1
            if verbose:
                print(f"[media_link] reset context {link_id}")

    return RestoreSummary(
        person_id=person_id,
        person_name=person_name,
        cast_total=len(cast_rows),
        media_links_total=len(media_link_rows),
        cast_crop_rows=len(cast_to_reset),
        media_link_crop_rows=len(links_to_reset),
        cast_updated=cast_updated,
        media_links_updated=media_links_updated,
        base_variants_regenerated=base_variants_regenerated,
    )


def _print_verification_snippets(summary: RestoreSummary) -> None:
    print("\nVerification SQL snippets (read-only):")
    print(
        "- specific SHA crop check:\n"
        "  SELECT id, hosted_sha256,\n"
        "         metadata->>'active_crop_signature', metadata->>'crop_detail_url',\n"
        "         metadata->>'detail_url'\n"
        "  FROM core.cast_photos\n"
        "  WHERE hosted_sha256 = 'd3a727384fb5fc298a088eb9d771941f18c8f74a15157c0d30d24f1283e8e23e';"
    )
    print(
        "- person crop-state check:\n"
        f"  WITH p AS (SELECT '{summary.person_id}'::uuid AS person_id)\n"
        "  SELECT\n"
        "    COUNT(*) FILTER (\n"
        "      WHERE coalesce(cp.metadata->>'active_crop_signature','') <> ''\n"
        "    ) AS cast_with_active_crop,\n"
        "    COUNT(*) FILTER (WHERE cp.metadata ? 'thumbnail_crop') AS cast_with_thumbnail_crop\n"
        "  FROM core.cast_photos cp JOIN p ON cp.person_id = p.person_id;\n"
        f"  WITH p AS (SELECT '{summary.person_id}'::uuid AS person_id)\n"
        "  SELECT COUNT(*) FILTER (\n"
        "    WHERE coalesce(ml.context,'{}'::jsonb) ? 'thumbnail_crop'\n"
        "  ) AS media_links_with_thumbnail_crop\n"
        "  FROM core.media_links ml\n"
        "  JOIN p ON ml.entity_type='person' AND ml.entity_id=p.person_id AND ml.kind='gallery';"
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()

    try:
        target = _resolve_target(
            db,
            person_id=str(args.person_id).strip() if args.person_id else None,
            person_name=str(args.person_name).strip() if args.person_name else None,
        )
    except RuntimeError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2

    summary = restore_person_gallery_base_previews(
        db,
        person_id=target.person_id,
        person_name=target.person_name,
        apply_updates=bool(args.apply),
        force_base_variants=bool(args.force_base_variants),
        verbose=bool(args.verbose),
    )

    mode = "apply" if args.apply else "dry-run"
    print(
        f"restore_person_gallery_base_previews ({mode}): person={summary.person_name} ({summary.person_id}) "
        f"cast_total={summary.cast_total} media_links_total={summary.media_links_total} "
        f"cast_crop_rows={summary.cast_crop_rows} media_link_crop_rows={summary.media_link_crop_rows} "
        f"cast_updated={summary.cast_updated} media_links_updated={summary.media_links_updated} "
        f"base_variants_regenerated={summary.base_variants_regenerated}"
    )

    _print_verification_snippets(summary)

    if not args.apply:
        print("\nDry-run only. Re-run with --apply to persist rollback.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
