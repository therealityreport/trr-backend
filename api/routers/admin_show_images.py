"""Admin endpoints for show-level image fetching from Getty/NBCUMV.

Provides a streaming endpoint that:
1. Resolves a TRR show to its NBCUMV counterpart
2. Fetches all NBCUMV images for the show (with Getty enrichment)
3. Searches Getty directly for additional images not in NBCUMV
4. For each image: S3 mirror → people matching → media links for show + people
5. Streams progress via SSE
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.auth import InternalAdminUser
from api.deps import SupabaseAdminClient
from trr_backend.media.getty_replacement import is_bravo_network_name, resolve_best_public_replacement

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/shows", tags=["admin-show-images"])

_GETTY_SOURCE = "getty"


class ShowGetImagesRequest(BaseModel):
    limit: int | None = Field(default=None, ge=1, le=2000)
    getty_limit: int | None = Field(default=None, ge=1)
    skip_s3: bool = False


def _sse_event(event_type: str, data: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(data, default=str)}\n\n"


def _fetch_show(db: SupabaseAdminClient, show_id: str) -> dict[str, Any]:
    response = db.schema("core").table("shows").select("id, name, networks").eq("id", show_id).limit(1).execute()
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to fetch show: {response.error}")
    rows = response.data or []
    if not rows:
        raise HTTPException(status_code=404, detail=f"Show not found: {show_id}")
    return rows[0]


def _show_is_bravo_family(show_row: dict[str, Any] | None) -> bool:
    if not isinstance(show_row, dict):
        return False
    networks = show_row.get("networks")
    if not isinstance(networks, list):
        return False
    return any(is_bravo_network_name(network) for network in networks)


def _dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for v in values:
        cleaned = str(v or "").strip()
        lowered = cleaned.casefold()
        if lowered and lowered not in seen:
            seen.add(lowered)
            result.append(cleaned)
    return result


def _import_show_images(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    show_name: str,
    show_is_bravo: bool,
    limit: int | None,
    getty_limit: int | None,
    progress_cb: Any | None = None,
) -> dict[str, Any]:
    """Fetch NBCUMV + Getty images for a show, create media links."""
    from api.routers.admin_nbcumv import (
        NbcumvImportItem,
        _ensure_sources,
        _import_single_item,
        _load_eligible_people_index,
        _match_people_names,
    )
    from trr_backend.integrations import getty, nbcumv
    from trr_backend.repositories.media_assets import asset_id_for
    from trr_backend.repositories.web_scrape_images import create_media_link_for_entity

    _ensure_sources(db)

    result: dict[str, Any] = {
        "show_id": show_id,
        "show_name": show_name,
        "nbcumv_show_id": None,
        "nbcumv_found": 0,
        "nbcumv_imported": 0,
        "nbcumv_skipped": 0,
        "nbcumv_failed": 0,
        "getty_found": 0,
        "getty_imported": 0,
        "getty_skipped": 0,
        "getty_failed": 0,
        "total_found": 0,
        "imported": 0,
        "skipped": 0,
        "failed": 0,
        "person_links_created": 0,
        "show_links_created": 0,
        "errors": [],
        "unmatched_people": [],
    }

    def _emit(stage: str, current: int, total: int, message: str) -> None:
        if progress_cb:
            progress_cb(stage, current, total, message)

    # ── Phase 0: Load shared resources ──

    people_index = _load_eligible_people_index(db)

    # ── Phase 1: NBCUMV images ──

    nbcumv_show = nbcumv.resolve_show_by_title(show_name)
    imported_object_names: set[str] = set()

    if nbcumv_show:
        nbcumv_show_id = str(nbcumv_show.get("id") or "")
        nbcumv_show_title = str(nbcumv_show.get("title") or "")
        result["nbcumv_show_id"] = nbcumv_show_id

        _emit("nbcumv_resolve", 0, 0, f"Resolved NBCUMV show: {nbcumv_show_title} ({nbcumv_show_id})")
        _emit("nbcumv_fetch", 0, 0, f"Fetching NBCUMV images for {nbcumv_show_title}...")

        images = nbcumv.list_show_images(nbcumv_show_id, limit=limit)
        nbcumv_total = len(images)
        result["nbcumv_found"] = nbcumv_total

        _emit("nbcumv_fetch", 0, nbcumv_total, f"Found {nbcumv_total} NBCUMV images")

        all_unmatched: list[str] = []
        for i, image in enumerate(images):
            lbx_id = str(image.get("lbx_id") or "").strip()
            lbx_filename = str(image.get("lbx_filename") or "").strip()
            if not lbx_id or not lbx_filename:
                result["nbcumv_skipped"] += 1
                _emit("nbcumv_import", i + 1, nbcumv_total, f"Skipped {i + 1}/{nbcumv_total} (missing ID/filename)")
                continue

            _emit("nbcumv_import", i + 1, nbcumv_total, f"Importing {lbx_filename} ({i + 1}/{nbcumv_total})")

            try:
                item = NbcumvImportItem(
                    lbx_id=lbx_id,
                    lbx_filename=lbx_filename,
                    location=image.get("location"),
                    nbcumv_image=image,
                    show_ids=image.get("showIds") or [],
                    link_show_ids=[UUID(show_id)],
                )
                import_result = _import_single_item(
                    db=db,
                    item=item,
                    assign_people=True,
                    people_index=people_index,
                )

                imported_object_names.add(lbx_filename.upper())

                if import_result.get("already_imported"):
                    result["nbcumv_skipped"] += 1
                else:
                    result["nbcumv_imported"] += 1

                result["person_links_created"] += len(import_result.get("created_person_ids") or [])
                result["show_links_created"] += len(import_result.get("created_show_ids") or [])
                all_unmatched.extend(import_result.get("unmatched_people") or [])

            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to import NBCUMV image %s: %s", lbx_filename, exc)
                result["nbcumv_failed"] += 1
                result["errors"].append(f"NBCUMV {lbx_filename}: {exc}")

    else:
        _emit("nbcumv_resolve", 0, 0, f"Could not resolve NBCUMV show for: {show_name}. Continuing with Getty only.")
        result["errors"].append(f"NBCUMV show not resolved for: {show_name}")
        all_unmatched = []

    # ── Phase 2: Getty search for additional images ──

    search_phrase = f"{show_name} Bravo"
    _emit("getty_search", 0, 0, f"Searching Getty for '{search_phrase}'...")

    def _getty_progress(current: int, total: int, message: str) -> None:
        _emit("getty_search", current, total, message)

    getty_assets = getty.search_editorial_assets(
        search_phrase,
        limit=getty_limit,
        progress_cb=_getty_progress,
    )

    if not getty_assets:
        # Retry with just the show name
        _emit("getty_search", 0, 0, f"No results for '{search_phrase}'. Trying '{show_name}'...")
        getty_assets = getty.search_editorial_assets(
            show_name,
            limit=getty_limit,
            progress_cb=_getty_progress,
            query_params={"sort": "best"},
        )

    result["getty_found"] = len(getty_assets)
    _emit("getty_search", len(getty_assets), len(getty_assets), f"Found {len(getty_assets)} Getty assets")

    # Filter to only new images not already imported from NBCUMV
    new_getty_assets = []
    for asset in getty_assets:
        object_name = str(asset.get("object_name") or "").strip().upper()
        editorial_id = str(asset.get("editorial_id") or "").strip()
        if not editorial_id:
            continue
        if object_name and object_name in imported_object_names:
            result["getty_skipped"] += 1
            continue
        new_getty_assets.append(asset)

    if new_getty_assets:
        _emit(
            "getty_import",
            0,
            len(new_getty_assets),
            f"{len(new_getty_assets)} Getty assets not in NBCUMV — importing...",
        )

    for i, asset in enumerate(new_getty_assets):
        editorial_id = str(asset.get("editorial_id") or "").strip()
        object_name = str(asset.get("object_name") or "").strip()
        label = object_name or editorial_id

        _emit("getty_import", i + 1, len(new_getty_assets), f"Getty {label} ({i + 1}/{len(new_getty_assets)})")

        # Try to find NBCUMV match by filename (may have been missed in Phase 1)
        nbcumv_image = None
        if object_name and nbcumv_show:
            try:
                nbcumv_image = nbcumv.find_show_image_by_filename(
                    str(nbcumv_show.get("id") or ""),
                    object_name,
                )
            except Exception:  # noqa: BLE001
                pass

        if isinstance(nbcumv_image, dict):
            # Found in NBCUMV — import via the full pipeline
            lbx_id = str(nbcumv_image.get("lbx_id") or "").strip()
            lbx_filename = str(nbcumv_image.get("lbx_filename") or "").strip()
            if lbx_id and lbx_filename:
                try:
                    item = NbcumvImportItem(
                        lbx_id=lbx_id,
                        lbx_filename=lbx_filename,
                        location=nbcumv_image.get("location"),
                        nbcumv_image=nbcumv_image,
                        show_ids=nbcumv_image.get("showIds") or [],
                        link_show_ids=[UUID(show_id)],
                        getty_detail_url=str(asset.get("detail_url") or ""),
                    )
                    import_result = _import_single_item(
                        db=db,
                        item=item,
                        assign_people=True,
                        people_index=people_index,
                    )
                    imported_object_names.add(lbx_filename.upper())
                    if import_result.get("already_imported"):
                        result["getty_skipped"] += 1
                    else:
                        result["getty_imported"] += 1
                    result["person_links_created"] += len(import_result.get("created_person_ids") or [])
                    result["show_links_created"] += len(import_result.get("created_show_ids") or [])
                    all_unmatched.extend(import_result.get("unmatched_people") or [])
                    continue
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Failed to import NBCUMV match for Getty %s: %s", editorial_id, exc)

        # No NBCUMV match — create media_asset from Getty preview URL
        preview_url = None
        for key in (
            "preview_image_url",
            "downloadableCompUrl",
            "galleryHighResCompUrl",
            "highResCompUrl",
            "galleryComp1024Url",
            "compUrl",
            "mainImageUrl",
            "thumbUrl",
            "thumb_url",
        ):
            value = str(asset.get(key) or "").strip()
            if value:
                preview_url = value
                break

        if not preview_url:
            result["getty_failed"] += 1
            result["errors"].append(f"Getty {editorial_id}: no preview URL available")
            continue

        try:
            # Extract people from Getty
            tagged_people = [
                str(entry.get("text") or "").strip()
                for entry in (asset.get("people") or [])
                if isinstance(entry, dict) and str(entry.get("text") or "").strip()
            ]
            # Clean " - Television Personality" suffixes
            cleaned_people: list[str] = []
            for name in tagged_people:
                if " - " in name:
                    name = name.split(" - ", 1)[0].strip()
                if name:
                    cleaned_people.append(name)
            tagged_people = cleaned_people

            matches = _match_people_names(people_index, tagged_people)
            resolved_people = matches["resolved"]

            # Build deterministic asset ID
            asset_id = asset_id_for(
                _GETTY_SOURCE,
                source_asset_id=editorial_id,
                source_url=preview_url,
            )
            if asset_id is None:
                result["getty_failed"] += 1
                continue

            # Determine dimensions
            width = None
            height = None
            for dim_field in ("assetDimensions", "actualMaxDimensions"):
                candidate = asset.get(dim_field)
                if isinstance(candidate, dict):
                    w, h = candidate.get("width"), candidate.get("height")
                    if isinstance(w, int) and isinstance(h, int) and w > 0 and h > 0:
                        width, height = w, h
                        break

            public_replacement = None
            detail_url = str(asset.get("detail_url") or "").strip() or None
            if show_is_bravo:
                try:
                    public_replacement = resolve_best_public_replacement(
                        preview_url,
                        expected_width=width,
                        expected_height=height,
                        bravo_only=True,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "Auto Getty replacement lookup failed for show_id=%s editorial_id=%s: %s",
                        show_id,
                        editorial_id,
                        exc,
                    )

            resolved_source_url = str(public_replacement.image_url).strip() if public_replacement else preview_url
            resolved_width = public_replacement.width if public_replacement and public_replacement.width else width
            resolved_height = public_replacement.height if public_replacement and public_replacement.height else height

            # Build metadata
            getty_tags = list(asset.get("keyword_texts") or []) if isinstance(asset.get("keyword_texts"), list) else []
            metadata: dict[str, Any] = {
                "getty": dict(asset),
                "getty_only_fallback": public_replacement is None,
                "source_domain": public_replacement.source_domain if public_replacement else "gettyimages.com",
                "source_url": resolved_source_url,
                "source_page_url": str(public_replacement.page_url).strip() if public_replacement else detail_url,
                "original_source": "getty",
                "original_source_url": preview_url,
                "original_source_page_url": detail_url,
                "source_resolution": public_replacement.mode if public_replacement else "getty_watermark_fallback",
                "getty_details": dict(asset.get("details") or {}) if isinstance(asset.get("details"), dict) else {},
                "getty_tags": getty_tags,
                "getty_event_title": str(asset.get("event_name") or "").strip() or None,
                "getty_event_url": str(asset.get("event_url") or "").strip() or None,
                "getty_event_id": str(asset.get("event_id") or "").strip() or None,
                "getty_event_slug": str(asset.get("event_url_slug") or "").strip() or None,
                "getty_event_date": str(asset.get("event_date") or "").strip() or None,
                "object_name": object_name or None,
                "tagged_people": tagged_people,
                "resolved_people": resolved_people,
                "unmatched_people": matches.get("unmatched", []),
                "show_name": show_name,
                "show_id": show_id,
                "people_names": tagged_people if tagged_people else None,
                "people_count": len(tagged_people) if tagged_people else None,
                "created_at": str(asset.get("date_created") or "").strip() or None,
            }
            if public_replacement:
                metadata["replaced_from"] = {
                    "url": public_replacement.page_url,
                    "domain": public_replacement.source_domain,
                    "image_url": public_replacement.image_url,
                    "width": public_replacement.width,
                    "height": public_replacement.height,
                    "mode": public_replacement.mode,
                }

            now = datetime.now(UTC).isoformat()

            # Check for existing asset
            existing_response = (
                db.schema("core")
                .table("media_assets")
                .select("id")
                .eq("source", _GETTY_SOURCE)
                .eq("source_asset_id", editorial_id)
                .limit(1)
                .execute()
            )
            existing = (existing_response.data or [{}])[0] if existing_response.data else None

            asset_row = {
                "id": str(asset_id),
                "media_type": "image",
                "source": _GETTY_SOURCE,
                "source_asset_id": editorial_id,
                "source_url": resolved_source_url,
                "width": resolved_width,
                "height": resolved_height,
                "caption": str(asset.get("caption") or "").strip() or None,
                "metadata": metadata,
                "fetched_at": now,
                "updated_at": now,
            }

            if existing:
                db.schema("core").table("media_assets").update(asset_row).eq("id", existing["id"]).execute()
                result["getty_skipped"] += 1
            else:
                asset_row["created_at"] = now
                db.schema("core").table("media_assets").insert(asset_row).execute()
                result["getty_imported"] += 1

            # Create show-level link
            link_context = {
                "source": _GETTY_SOURCE,
                "source_asset_id": editorial_id,
                "tagged_people": tagged_people,
                "resolved_people": resolved_people,
            }
            show_link = create_media_link_for_entity(
                db,
                entity_type="show",
                entity_id=show_id,
                media_asset_id=str(asset_id),
                kind="gallery",
                position=0,
                context=link_context,
            )
            if show_link:
                result["show_links_created"] += 1

            # Create person-level links
            for person in resolved_people:
                person_id = str(person["person_id"])
                person_link = create_media_link_for_entity(
                    db,
                    entity_type="person",
                    entity_id=person_id,
                    media_asset_id=str(asset_id),
                    kind="gallery",
                    position=0,
                    context=link_context,
                )
                if person_link:
                    result["person_links_created"] += 1

            all_unmatched.extend(matches.get("unmatched", []))
            all_unmatched.extend(matches.get("ambiguous", []))

            if object_name:
                imported_object_names.add(object_name.upper())

        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to import Getty asset %s: %s", editorial_id, exc)
            result["getty_failed"] += 1
            result["errors"].append(f"Getty {editorial_id}: {exc}")

    # ── Summarize ──

    result["total_found"] = result["nbcumv_found"] + result["getty_found"]
    result["imported"] = result["nbcumv_imported"] + result["getty_imported"]
    result["skipped"] = result["nbcumv_skipped"] + result["getty_skipped"]
    result["failed"] = result["nbcumv_failed"] + result["getty_failed"]
    result["unmatched_people"] = _dedupe_strings(all_unmatched)

    summary = (
        f"Done: {result['imported']} imported, {result['skipped']} skipped, {result['failed']} failed. "
        f"NBCUMV: {result['nbcumv_found']} found. Getty: {result['getty_found']} found, "
        f"{len(new_getty_assets)} new."
    )
    _emit("complete", result["imported"], result["total_found"], summary)

    return result


@router.post("/{show_id}/get-images/stream")
async def get_show_images_stream(
    show_id: UUID,
    connection: Request,
    request: ShowGetImagesRequest | None = None,
    db: SupabaseAdminClient = cast(SupabaseAdminClient, None),
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> StreamingResponse:
    """Fetch Getty+NBCUMV images for a show with SSE streaming progress."""
    request = request or ShowGetImagesRequest()
    show = _fetch_show(db, str(show_id))
    show_name = str(show.get("name") or "")
    show_is_bravo = _show_is_bravo_family(show)
    if not show_name:
        raise HTTPException(status_code=400, detail="Show has no name")

    progress_events: list[dict[str, Any]] = []

    def progress_cb(stage: str, current: int, total: int, message: str) -> None:
        progress_events.append(
            {
                "stage": stage,
                "current": current,
                "total": total,
                "message": message,
                "ts": time.time(),
            }
        )

    async def event_generator():
        import_task = asyncio.create_task(
            asyncio.to_thread(
                _import_show_images,
                db,
                show_id=str(show_id),
                show_name=show_name,
                show_is_bravo=show_is_bravo,
                limit=request.limit,
                getty_limit=request.getty_limit,
                progress_cb=progress_cb,
            )
        )

        last_sent = 0
        while not import_task.done():
            await asyncio.sleep(0.3)
            while last_sent < len(progress_events):
                yield _sse_event("progress", progress_events[last_sent])
                last_sent += 1

        while last_sent < len(progress_events):
            yield _sse_event("progress", progress_events[last_sent])
            last_sent += 1

        try:
            final_result = import_task.result()
            yield _sse_event("complete", final_result)
        except Exception as exc:
            logger.exception("Show get-images failed for %s", show_id)
            yield _sse_event(
                "error",
                {
                    "message": str(exc),
                    "is_terminal": True,
                },
            )

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
