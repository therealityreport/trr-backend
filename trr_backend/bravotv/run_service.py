from __future__ import annotations

import json
import logging
import mimetypes
import re
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from trr_backend.bravotv.get_images_pipeline import _upload_bytes, run_get_images_pipeline
from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.integrations.picdetective import ReverseImageCandidate
from trr_backend.media.getty_replacement import (
    resolve_best_public_replacement,
    search_public_replacement_candidates,
)
from trr_backend.media.image_variants import generate_media_asset_variants
from trr_backend.media.s3_mirror import (
    build_hosted_url,
    get_object_storage_bucket,
    get_object_storage_client,
    upload_bytes_to_s3,
)
from trr_backend.repositories.bravotv_image_runs import (
    attach_operation as attach_operation_to_run,
)
from trr_backend.repositories.bravotv_image_runs import (
    create_run,
    get_latest_run,
    get_run,
    update_progress,
)
from trr_backend.repositories.media_assets import asset_id_for, upsert_media_assets
from trr_backend.repositories.web_scrape_images import create_media_link_for_entity
from trr_backend.scraping.url_image_scraper import download_and_hash_image

logger = logging.getLogger(__name__)

ProgressEmitter = Callable[[str, dict[str, Any]], None]
_ARTIFACT_PREVIEW_LIMIT = 25


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _safe_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _safe_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    return []


def _emit(progress_cb: ProgressEmitter | None, stage: str, payload: dict[str, Any]) -> None:
    if progress_cb:
        progress_cb(stage, payload)


def _normalize_name(value: str | None) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def _fetch_show_row(show_id: str) -> dict[str, Any]:
    db = create_supabase_admin_client()
    response = db.schema("core").table("shows").select("id,name").eq("id", show_id).limit(1).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Failed to load show: {response.error}")
    rows = response.data or []
    if not rows:
        raise RuntimeError("Show not found")
    return rows[0]


def _fetch_person_row(person_id: str) -> dict[str, Any]:
    db = create_supabase_admin_client()
    response = db.schema("core").table("people").select("id,name,external_ids").eq("id", person_id).limit(1).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Failed to load person: {response.error}")
    rows = response.data or []
    if not rows:
        raise RuntimeError("Person not found")
    return rows[0]


def _fetch_person_aliases(person_id: str) -> set[str]:
    db = create_supabase_admin_client()
    aliases: set[str] = set()
    person = _fetch_person_row(person_id)
    aliases.add(_normalize_name(person.get("name")))
    overrides = (
        db.schema("core").table("people_overrides").select("full_name_override").eq("person_id", person_id).execute()
    )
    if not getattr(overrides, "error", None):
        for row in overrides.data or []:
            aliases.add(_normalize_name(row.get("full_name_override")))
    aliases.discard("")
    return aliases


def _fetch_season_map(show_id: str) -> dict[int, str]:
    db = create_supabase_admin_client()
    response = db.schema("core").table("seasons").select("id,season_number").eq("show_id", show_id).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Failed to load seasons: {response.error}")
    mapping: dict[int, str] = {}
    for row in response.data or []:
        try:
            season_number = int(row.get("season_number"))
        except (TypeError, ValueError):
            continue
        season_id = str(row.get("id") or "").strip()
        if season_id:
            mapping[season_number] = season_id
    return mapping


def _slugify_lookup_key(value: str | None) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").casefold())
    return re.sub(r"-{2,}", "-", text).strip("-")


def _fetch_episode_slug_map(show_id: str) -> dict[tuple[int | None, str], str]:
    db = create_supabase_admin_client()
    response = db.schema("core").table("episodes").select("id,title,season_number").eq("show_id", show_id).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Failed to load episodes: {response.error}")
    mapping: dict[tuple[int | None, str], str] = {}
    for row in response.data or []:
        episode_id = str(row.get("id") or "").strip()
        title_slug = _slugify_lookup_key(row.get("title"))
        if not episode_id or not title_slug:
            continue
        season_number_raw = row.get("season_number")
        try:
            season_number = int(season_number_raw) if season_number_raw is not None else None
        except (TypeError, ValueError):
            season_number = None
        mapping[(season_number, title_slug)] = episode_id
        mapping.setdefault((None, title_slug), episode_id)
    return mapping


def _resolve_episode_id_for_row(
    *,
    row: dict[str, Any],
    row_metadata: dict[str, Any],
    episode_slug_map: dict[tuple[int | None, str], str],
) -> str | None:
    explicit_episode_id = str(row.get("episode_id") or row_metadata.get("episode_id") or "").strip()
    if explicit_episode_id:
        return explicit_episode_id
    season_number = row.get("season") or row_metadata.get("season_number")
    try:
        season_number_int = int(season_number) if season_number is not None else None
    except (TypeError, ValueError):
        season_number_int = None
    candidates = (
        row_metadata.get("episode_slug"),
        row_metadata.get("page_title"),
        row.get("context_section"),
    )
    for candidate in candidates:
        slug = _slugify_lookup_key(candidate)
        if not slug:
            continue
        if (season_number_int, slug) in episode_slug_map:
            return episode_slug_map[(season_number_int, slug)]
        if (None, slug) in episode_slug_map:
            return episode_slug_map[(None, slug)]
    return None


def _load_people_index(db: Any) -> dict[str, list[dict[str, str]]]:
    from api.routers.admin_nbcumv import _load_eligible_people_index

    return _load_eligible_people_index(db)


def _match_people(index: dict[str, list[dict[str, str]]], names: list[str]) -> dict[str, Any]:
    from api.routers.admin_nbcumv import _match_people_names

    return _match_people_names(index, names)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def _artifact_content_type(path: Path) -> str:
    guessed = mimetypes.guess_type(str(path))[0]
    if guessed:
        return guessed
    if path.suffix == ".json":
        return "application/json"
    if path.suffix in {".txt", ".md"}:
        return "text/plain"
    return "application/octet-stream"


def _upload_artifacts(output_root: Path, *, run_id: str) -> dict[str, dict[str, Any]]:
    bucket = get_object_storage_bucket()
    client = get_object_storage_client()
    uploaded: dict[str, dict[str, Any]] = {}
    for file_path in sorted(output_root.rglob("*")):
        if not file_path.is_file():
            continue
        rel_path = file_path.relative_to(output_root).as_posix()
        key = f"bravotv-image-runs/{run_id}/{rel_path}"
        data = file_path.read_bytes()
        content_type = _artifact_content_type(file_path)
        etag, size_bytes = upload_bytes_to_s3(client, bucket=bucket, key=key, data=data, content_type=content_type)
        artifact_name = rel_path[:-5] if rel_path.endswith(".json") else rel_path
        uploaded[artifact_name] = {
            "key": key,
            "url": build_hosted_url(key),
            "bytes": size_bytes,
            "etag": etag,
            "content_type": content_type,
            "relative_path": rel_path,
        }
    return uploaded


def _build_summary(
    merged_catalog: list[dict[str, Any]],
    import_summary: dict[str, Any],
    review_summary: dict[str, Any],
) -> dict[str, Any]:
    replacement_pending = sum(
        1
        for record in merged_catalog
        if isinstance(record.get("acquisition"), dict)
        and str(record["acquisition"].get("status") or "").strip().lower() == "referenced_only"
    )
    return {
        "total_merged_records": len(merged_catalog),
        "imported_assets": int(import_summary.get("assets_upserted") or 0)
        + int(import_summary.get("supplemental_assets_upserted") or 0),
        "imported_links": int(import_summary.get("links_created") or 0),
        "review_count": int(review_summary.get("review_count") or 0),
        "replacement_pending_count": replacement_pending,
    }


def _reference_preview_payload(record: dict[str, Any], acquisition: dict[str, Any]) -> dict[str, Any]:
    preview_url = str(
        acquisition.get("source_url") or record.get("per_source", {}).get("getty", {}).get("source_url") or ""
    ).strip()
    return {
        "hosted_url": preview_url or None,
        "hosted_content_type": "image/jpeg" if preview_url else None,
        "hosted_bucket": None,
        "hosted_key": None,
        "hosted_sha256": None,
        "hosted_bytes": None,
        "hosted_etag": None,
        "hosted_at": None,
    }


def _canonical_asset_identity(record: dict[str, Any]) -> tuple[str, str | None, str | None]:
    getty_id = str(record.get("getty_editorial_id") or "").strip()
    per_source = _safe_json(record.get("per_source"))
    if getty_id:
        getty = _safe_json(per_source.get("getty"))
        return "getty", getty_id, str(getty.get("source_page_url") or getty.get("source_url") or "").strip() or None
    nbcumv = _safe_json(per_source.get("nbcumv"))
    nbcumv_source_id = str(nbcumv.get("source_id") or "").strip()
    if nbcumv_source_id:
        return "nbcumv", nbcumv_source_id, str(nbcumv.get("source_url") or "").strip() or None
    bravo = _safe_json(per_source.get("bravo"))
    bravo_source_id = str(bravo.get("source_id") or "").strip()
    return (
        "bravo",
        bravo_source_id or str(record.get("id") or "").strip() or None,
        str(bravo.get("source_url") or "").strip() or None,
    )


def _build_asset_payload(record: dict[str, Any], *, run_id: str) -> tuple[dict[str, Any], bool]:
    source, source_asset_id, source_url = _canonical_asset_identity(record)
    asset_id = asset_id_for(source, source_asset_id=source_asset_id, source_url=source_url)
    if asset_id is None:
        raise RuntimeError("Unable to derive deterministic media asset id")
    acquisition = _safe_json(record.get("acquisition"))
    acquisition_status = str(acquisition.get("status") or "").strip().lower()
    preview_only = acquisition_status == "referenced_only"
    hosted_payload = (
        _reference_preview_payload(record, acquisition)
        if preview_only
        else {
            "hosted_url": acquisition.get("hosted_url"),
            "hosted_content_type": acquisition.get("hosted_content_type"),
            "hosted_bucket": acquisition.get("hosted_bucket"),
            "hosted_key": acquisition.get("hosted_key"),
            "hosted_sha256": acquisition.get("hosted_sha256"),
            "hosted_bytes": acquisition.get("hosted_bytes"),
            "hosted_etag": acquisition.get("hosted_etag"),
            "hosted_at": acquisition.get("hosted_at"),
        }
    )
    metadata = {
        "run_id": run_id,
        "bridge_strategy": record.get("bridge_strategy"),
        "match_confidence": record.get("match_confidence"),
        "per_source": record.get("per_source"),
        "persons_pictured": record.get("persons_pictured") or [],
        "keywords": record.get("keywords") or [],
        "photographer": record.get("photographer"),
        "show_name": record.get("show_name"),
        "season_number": record.get("season_number"),
        "episode_title": record.get("episode_title"),
        "air_date": record.get("air_date"),
        "acquisition": acquisition,
        "replacement_pending": preview_only,
        "google_reverse_image_search_url": acquisition.get("google_reverse_image_search_url"),
        "source_page_url": acquisition.get("source_page_url")
        or _safe_json(_safe_json(record.get("per_source")).get("getty")).get("source_page_url")
        or _safe_json(_safe_json(record.get("per_source")).get("bravo")).get("source_page_url"),
    }
    payload = {
        "id": str(asset_id),
        "media_type": "image",
        "source": source,
        "source_asset_id": source_asset_id,
        "source_url": source_url,
        "width": record.get("width") or _safe_json(_safe_json(record.get("per_source")).get("getty")).get("width"),
        "height": record.get("height") or _safe_json(_safe_json(record.get("per_source")).get("getty")).get("height"),
        "caption": record.get("caption"),
        "alt_text": record.get("caption"),
        "metadata": {key: value for key, value in metadata.items() if value is not None},
        **{key: value for key, value in hosted_payload.items() if value is not None},
    }
    return payload, preview_only


def _refresh_getty_replacement(record: dict[str, Any]) -> dict[str, Any]:
    acquisition = _safe_json(record.get("acquisition"))
    if str(acquisition.get("status") or "").strip().lower() != "referenced_only":
        return record
    preview_url = str(acquisition.get("source_url") or "").strip()
    if not preview_url:
        return record
    try:
        replacement = resolve_best_public_replacement(
            preview_url,
            expected_width=_safe_json(_safe_json(record.get("per_source")).get("getty")).get("width"),
            expected_height=_safe_json(_safe_json(record.get("per_source")).get("getty")).get("height"),
            bravo_only=True,
            limit=5,
        )
        if replacement is None:
            candidates = search_public_replacement_candidates(preview_url, bravo_only=True, limit=5)
            acquisition["replacement_candidates"] = [
                {
                    "title": candidate.title,
                    "source_domain": candidate.source_domain,
                    "page_url": candidate.page_url,
                    "width": candidate.width,
                    "height": candidate.height,
                }
                for candidate in candidates
                if isinstance(candidate, ReverseImageCandidate)
            ]
            record["acquisition"] = acquisition
            return record
        data, sha256, content_type = download_and_hash_image(replacement.image_url, referer=replacement.page_url)
        # Reuse the existing pipeline uploader so hosted assets land in shared storage.
        uploaded = _upload_bytes(data, content_type=content_type)
        acquisition.update(
            {
                "status": "uploaded",
                "source": replacement.mode,
                "watermarked": False,
                "replacement_page_url": replacement.page_url,
                "replacement_source_domain": replacement.source_domain,
                "replacement_image_url": replacement.image_url,
                **uploaded,
            }
        )
        record["acquisition"] = acquisition
    except Exception as exc:  # noqa: BLE001
        acquisition["replacement_error"] = str(exc)
        record["acquisition"] = acquisition
    return record


def _import_catalog(
    *,
    mode: str,
    run_id: str,
    target_show_id: str | None,
    target_person_id: str | None,
    merged_catalog: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    db = create_supabase_admin_client()
    season_map = _fetch_season_map(target_show_id) if target_show_id else {}
    person_aliases = _fetch_person_aliases(target_person_id) if target_person_id else set()
    people_index = _load_people_index(db) if mode == "show" else {}

    imported_records: list[dict[str, Any]] = []
    review_candidates: list[dict[str, Any]] = []
    replacement_candidates: list[dict[str, Any]] = []
    assets_upserted = 0
    links_created = 0

    for record in merged_catalog:
        record = _refresh_getty_replacement(dict(record))
        asset_payload, preview_only = _build_asset_payload(record, run_id=run_id)
        upsert_media_assets(db, [asset_payload])
        assets_upserted += 1
        asset_id = str(asset_payload["id"])
        if not preview_only:
            try:
                generate_media_asset_variants(db, asset_id=asset_id, force=False)
            except Exception as exc:  # noqa: BLE001
                logger.warning("BRAVOTV variant generation failed for asset %s: %s", asset_id, exc)

        link_targets: list[str] = []
        show_name = str(record.get("show_name") or "").strip() or None
        season_number = record.get("season_number")
        if target_show_id:
            create_media_link_for_entity(
                db,
                entity_type="show",
                entity_id=target_show_id,
                media_asset_id=asset_id,
                kind="gallery",
                position=0,
                context={"run_id": run_id, "show_name": show_name, "season_number": season_number},
            )
            links_created += 1
            link_targets.append(f"show:{target_show_id}")
        if target_show_id and isinstance(season_number, int) and season_number in season_map:
            create_media_link_for_entity(
                db,
                entity_type="season",
                entity_id=season_map[season_number],
                media_asset_id=asset_id,
                kind="gallery",
                position=0,
                context={"run_id": run_id, "show_name": show_name, "season_number": season_number},
            )
            links_created += 1
            link_targets.append(f"season:{season_map[season_number]}")

        pictured = [str(value).strip() for value in (record.get("persons_pictured") or []) if str(value).strip()]
        if mode == "person" and target_person_id:
            normalized_people = {_normalize_name(value) for value in pictured}
            if normalized_people.intersection(person_aliases):
                create_media_link_for_entity(
                    db,
                    entity_type="person",
                    entity_id=target_person_id,
                    media_asset_id=asset_id,
                    kind="gallery",
                    position=0,
                    context={"run_id": run_id, "persons_pictured": pictured, "deterministic_match": True},
                )
                links_created += 1
                link_targets.append(f"person:{target_person_id}")
            else:
                review_candidates.append(
                    {
                        "group_id": record.get("id"),
                        "reason": "target_person_not_deterministic",
                        "persons_pictured": pictured,
                        "caption": record.get("caption"),
                        "show_name": show_name,
                    }
                )
        elif mode == "show" and pictured:
            matches = _match_people(people_index, pictured)
            for person in matches.get("resolved") or []:
                person_id = str(person.get("person_id") or "").strip()
                if not person_id:
                    continue
                create_media_link_for_entity(
                    db,
                    entity_type="person",
                    entity_id=person_id,
                    media_asset_id=asset_id,
                    kind="gallery",
                    position=0,
                    context={"run_id": run_id, "persons_pictured": pictured, "deterministic_match": True},
                )
                links_created += 1
                link_targets.append(f"person:{person_id}")
            unresolved_names = [
                *(_safe_list(matches.get("unmatched"))),
                *(_safe_list(matches.get("ambiguous"))),
            ]
            if unresolved_names:
                review_candidates.append(
                    {
                        "group_id": record.get("id"),
                        "reason": "person_assignment_needs_review",
                        "persons_pictured": pictured,
                        "unresolved_names": unresolved_names,
                        "caption": record.get("caption"),
                        "show_name": show_name,
                    }
                )

        acquisition = _safe_json(record.get("acquisition"))
        if str(acquisition.get("status") or "").strip().lower() == "referenced_only":
            replacement_candidates.append(
                {
                    "group_id": record.get("id"),
                    "media_asset_id": asset_id,
                    "caption": record.get("caption"),
                    "show_name": show_name,
                    "source_url": acquisition.get("source_url"),
                    "google_reverse_image_search_url": acquisition.get("google_reverse_image_search_url"),
                    "replacement_candidates": acquisition.get("replacement_candidates") or [],
                }
            )

        imported_records.append(
            {
                "group_id": record.get("id"),
                "media_asset_id": asset_id,
                "source": asset_payload.get("source"),
                "caption": record.get("caption"),
                "show_name": show_name,
                "season_number": season_number,
                "persons_pictured": pictured,
                "link_targets": link_targets,
                "replacement_pending": preview_only,
            }
        )

    import_summary = {
        "assets_upserted": assets_upserted,
        "links_created": links_created,
        "imported_records_count": len(imported_records),
        "replacement_pending_count": len(replacement_candidates),
        "review_count": len(review_candidates),
    }
    return import_summary, imported_records, review_candidates, replacement_candidates


def _import_supplemental_catalog(
    *,
    run_id: str,
    target_person_id: str | None,
    target_show_id: str | None,
    supplemental_catalog: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not target_person_id:
        return {"supplemental_assets_upserted": 0, "supplemental_links_created": 0}, []
    db = create_supabase_admin_client()
    season_map = _fetch_season_map(target_show_id) if target_show_id else {}
    episode_slug_map = _fetch_episode_slug_map(target_show_id) if target_show_id else {}
    imported: list[dict[str, Any]] = []
    assets_upserted = 0
    links_created = 0
    for source, rows in supplemental_catalog.items():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            raw_hosted = _safe_json(row.get("hosted"))
            row_metadata = _safe_json(row.get("metadata"))
            source_asset_id = str(row.get("source_image_id") or "").strip() or None
            source_url = str(row.get("image_url") or row.get("url") or "").strip() or None
            asset_id = asset_id_for(source, source_asset_id=source_asset_id, source_url=source_url)
            if asset_id is None:
                continue
            season_number = row.get("season") or row_metadata.get("season_number")
            link_person = bool(target_person_id) and bool(row.get("link_person", True))
            link_show = bool(target_show_id) and bool(row.get("link_show", True))
            link_season = bool(row.get("link_season", True))
            link_episode = bool(row.get("link_episode", False))
            content_type = str(row_metadata.get("content_type") or "").strip() or None
            context_type = str(row.get("context_type") or "").strip() or None
            context_section = (
                str(row_metadata.get("fandom_section_label") or "").strip()
                or str(row.get("context_section") or "").strip()
                or None
            )
            episode_id = (
                _resolve_episode_id_for_row(
                    row=row,
                    row_metadata=row_metadata,
                    episode_slug_map=episode_slug_map,
                )
                if link_episode
                else None
            )
            asset_metadata = {
                **row_metadata,
                "run_id": run_id,
                "source_variant": row_metadata.get("source_variant") or "supplemental_cast_photo",
                "people_names": row.get("people_names") or row_metadata.get("people_names") or [],
                "show_name": row_metadata.get("show_name"),
                "season_number": season_number,
                "episode_id": episode_id,
                "source_page_url": row.get("source_page_url") or row_metadata.get("source_page_url"),
            }
            if content_type:
                asset_metadata.setdefault("content_type", content_type)
                asset_metadata.setdefault("fandom_section_tag", content_type)
            if context_section:
                asset_metadata.setdefault("fandom_section_label", context_section)
            payload = {
                "id": str(asset_id),
                "media_type": "image",
                "source": source,
                "source_asset_id": source_asset_id,
                "source_url": source_url,
                "width": row.get("width"),
                "height": row.get("height"),
                "caption": row.get("caption"),
                "alt_text": row.get("alt_text") or row.get("caption"),
                "hosted_url": raw_hosted.get("hosted_url"),
                "hosted_key": raw_hosted.get("hosted_key"),
                "hosted_sha256": raw_hosted.get("hosted_sha256"),
                "hosted_content_type": raw_hosted.get("hosted_content_type"),
                "hosted_bytes": raw_hosted.get("hosted_bytes"),
                "metadata": asset_metadata,
            }
            upsert_media_assets(db, [payload])
            assets_upserted += 1
            if raw_hosted.get("hosted_url"):
                try:
                    generate_media_asset_variants(db, asset_id=str(asset_id), force=False)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("BRAVOTV supplemental variant generation failed for asset %s: %s", asset_id, exc)
            link_context = {
                "run_id": run_id,
                "supplemental_source": source,
                "show_name": row_metadata.get("show_name"),
                "season_number": season_number,
                "episode_id": episode_id,
                "people_names": row.get("people_names") or row_metadata.get("people_names") or [],
                "context_type": context_type,
                "context_section": context_section,
                "source_variant": asset_metadata.get("source_variant"),
            }
            if link_person:
                create_media_link_for_entity(
                    db,
                    entity_type="person",
                    entity_id=target_person_id,
                    media_asset_id=str(asset_id),
                    kind="gallery",
                    position=row.get("position"),
                    context=link_context,
                )
                links_created += 1
            if link_show and target_show_id:
                create_media_link_for_entity(
                    db,
                    entity_type="show",
                    entity_id=target_show_id,
                    media_asset_id=str(asset_id),
                    kind="gallery",
                    position=row.get("position"),
                    context=link_context,
                )
                links_created += 1
            if link_season and isinstance(season_number, int) and season_number in season_map:
                create_media_link_for_entity(
                    db,
                    entity_type="season",
                    entity_id=season_map[season_number],
                    media_asset_id=str(asset_id),
                    kind="gallery",
                    position=row.get("position"),
                    context=link_context,
                )
                links_created += 1
            if link_episode and episode_id:
                create_media_link_for_entity(
                    db,
                    entity_type="episode",
                    entity_id=episode_id,
                    media_asset_id=str(asset_id),
                    kind="gallery",
                    position=row.get("position"),
                    context=link_context,
                )
                links_created += 1
            imported.append(
                {
                    "media_asset_id": str(asset_id),
                    "source": source,
                    "caption": row.get("caption"),
                    "context_type": context_type,
                    "context_section": context_section,
                    "episode_id": episode_id,
                    "supplemental": True,
                }
            )
    return {
        "supplemental_assets_upserted": assets_upserted,
        "supplemental_links_created": links_created,
    }, imported


def execute_bravotv_image_run(
    *,
    mode: str,
    show_id: str | None = None,
    person_id: str | None = None,
    season: int | None = None,
    episode: int | None = None,
    sources: list[str] | None = None,
    getty_limit: int = 200,
    nbcumv_limit: int = 300,
    bravo_limit: int = 300,
    supplemental_limit: int = 100,
    force_all: bool = False,
    initiated_by: str | None = None,
    operation_id: str | None = None,
    progress_cb: ProgressEmitter | None = None,
) -> dict[str, Any]:
    if mode not in {"show", "person"}:
        raise ValueError("mode must be 'show' or 'person'")
    show_name = None
    person_name = None
    imdb_id = None
    tmdb_id = None
    if show_id:
        show_row = _fetch_show_row(show_id)
        show_name = str(show_row.get("name") or "").strip() or None
    if person_id:
        person_row = _fetch_person_row(person_id)
        person_name = str(person_row.get("name") or "").strip() or None
        external_ids = _safe_json(person_row.get("external_ids"))
        imdb_id = str(external_ids.get("imdb") or "").strip() or None
        tmdb_raw = str(external_ids.get("tmdb") or "").strip()
        tmdb_id = int(tmdb_raw) if tmdb_raw.isdigit() else None

    run = create_run(
        mode=mode,
        status="running",
        target_show_id=show_id,
        target_person_id=person_id,
        show_name=show_name,
        person_name=person_name,
        season=season,
        episode=episode,
        selected_sources=sources,
        request_payload={
            "mode": mode,
            "show_id": show_id,
            "person_id": person_id,
            "season": season,
            "episode": episode,
            "sources": sources or [],
            "getty_limit": getty_limit,
            "nbcumv_limit": nbcumv_limit,
            "bravo_limit": bravo_limit,
            "supplemental_limit": supplemental_limit,
            "force_all": force_all,
        },
        created_by=initiated_by,
        operation_id=operation_id,
    )
    run_id = str(run["id"])

    def _pipeline_progress(message: str) -> None:
        _emit(progress_cb, "progress", {"run_id": run_id, "stage": "pipeline", "message": message})

    _emit(progress_cb, "progress", {"run_id": run_id, "stage": "starting", "message": "Starting BRAVOTV image run."})
    with tempfile.TemporaryDirectory(prefix=f"bravotv-run-{run_id[:8]}-") as temp_dir:
        output_root = Path(temp_dir)
        supplemental_path = output_root / "supplemental_cast_photos.json"
        pipeline_result = run_get_images_pipeline(
            person_name=person_name,
            person_id=person_id,
            show_name=show_name,
            season=season,
            episode=episode,
            output_dir=output_root,
            sources=sources,
            getty_limit=getty_limit,
            nbcumv_limit=nbcumv_limit,
            bravo_limit=bravo_limit,
            supplemental_limit=supplemental_limit,
            imdb_id=imdb_id,
            tmdb_id=tmdb_id,
            force_all=force_all,
            progress_cb=_pipeline_progress,
        )
        merged_catalog = _safe_list(_read_json(output_root / "merged_catalog.json"))
        supplemental_catalog = _safe_json(_read_json(supplemental_path)) if supplemental_path.exists() else {}
        _emit(
            progress_cb,
            "progress",
            {
                "run_id": run_id,
                "stage": "importing",
                "message": "Importing BRAVOTV assets into gallery tables.",
            },
        )
        import_summary, imported_records, review_candidates, replacement_candidates = _import_catalog(
            mode=mode,
            run_id=run_id,
            target_show_id=show_id,
            target_person_id=person_id,
            merged_catalog=merged_catalog,
        )
        supplemental_summary, imported_supplemental = _import_supplemental_catalog(
            run_id=run_id,
            target_person_id=person_id,
            target_show_id=show_id,
            supplemental_catalog=supplemental_catalog,
        )
        imported_records.extend(imported_supplemental)
        import_summary.update(supplemental_summary)
        review_summary = {
            "review_count": len(review_candidates),
            "replacement_pending_count": len(replacement_candidates),
        }
        (output_root / "imported_records.json").write_text(
            json.dumps(imported_records, indent=2, ensure_ascii=True, default=_json_default) + "\n"
        )
        (output_root / "review_candidates.json").write_text(
            json.dumps(review_candidates, indent=2, ensure_ascii=True, default=_json_default) + "\n"
        )
        (output_root / "replacement_candidates.json").write_text(
            json.dumps(replacement_candidates, indent=2, ensure_ascii=True, default=_json_default) + "\n"
        )
        uploaded_artifacts = _upload_artifacts(output_root, run_id=run_id)
        manifest = _safe_json(pipeline_result.get("manifest"))
        summary = _build_summary(merged_catalog, import_summary, review_summary)
        run = update_progress(
            run_id,
            status="completed",
            refreshed_artifacts=manifest.get("refreshed_artifacts") or [],
            artifact_paths=uploaded_artifacts,
            manifest=manifest,
            summary=summary,
            import_summary=import_summary,
            review_summary=review_summary,
            completed=True,
        )
    complete_payload = {
        "run_id": run_id,
        "summary": run.get("summary") or {},
        "import_summary": run.get("import_summary") or {},
        "review_summary": run.get("review_summary") or {},
        "artifacts": {
            "merged_results": _ARTIFACT_PREVIEW_LIMIT,
            "imported_results": _ARTIFACT_PREVIEW_LIMIT,
            "needs_review": _ARTIFACT_PREVIEW_LIMIT,
            "replacement_candidates": _ARTIFACT_PREVIEW_LIMIT,
        },
    }
    _emit(progress_cb, "complete", complete_payload)
    return run


def execute_bravotv_image_run_from_request_payload(
    request_payload: dict[str, Any],
    progress_cb: ProgressEmitter | None = None,
) -> dict[str, Any]:
    payload = _safe_json(request_payload.get("payload"))
    return execute_bravotv_image_run(
        mode=str(request_payload.get("mode") or payload.get("mode") or "").strip(),
        show_id=str(request_payload.get("show_id") or payload.get("show_id") or "").strip() or None,
        person_id=str(request_payload.get("person_id") or payload.get("person_id") or "").strip() or None,
        season=payload.get("season"),
        episode=payload.get("episode"),
        sources=payload.get("sources") if isinstance(payload.get("sources"), list) else None,
        getty_limit=int(payload.get("getty_limit") or 200),
        nbcumv_limit=int(payload.get("nbcumv_limit") or 300),
        bravo_limit=int(payload.get("bravo_limit") or 300),
        supplemental_limit=int(payload.get("supplemental_limit") or 100),
        force_all=bool(payload.get("force_all") or False),
        initiated_by=str(request_payload.get("initiated_by") or "admin"),
        operation_id=str(request_payload.get("operation_id") or "").strip() or None,
        progress_cb=progress_cb,
    )


def get_bravotv_run(run_id: str) -> dict[str, Any] | None:
    return get_run(run_id)


def attach_operation(run_id: str, *, operation_id: str) -> dict[str, Any]:
    return attach_operation_to_run(run_id, operation_id=operation_id)


def get_latest_bravotv_run(
    *,
    mode: str,
    show_id: str | None = None,
    person_id: str | None = None,
) -> dict[str, Any] | None:
    return get_latest_run(mode=mode, target_show_id=show_id, target_person_id=person_id)
