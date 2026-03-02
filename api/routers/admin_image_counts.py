"""Admin endpoints for auto-counting people in images."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from trr_backend.clients.screenalytics import (
    ScreenalyticsClientError,
    auto_thumbnail_crop,
    count_people,
    face_centroid,
)
from trr_backend.media.face_crops import generate_and_upload_face_crops
from trr_backend.media.image_variants import generate_media_asset_variants
from trr_backend.media.s3_mirror import normalize_fandom_file_url
from trr_backend.repositories.cast_photo_tags import (
    get_tags_by_photo_ids,
    has_manual_tags,
    upsert_cast_photo_tags,
)
from trr_backend.repositories.identity_assignment import (
    build_identity_candidate_person_ids as build_identity_candidate_person_ids_shared,
)
from trr_backend.repositories.identity_assignment import (
    is_trr_show_eligible as is_trr_show_eligible_shared,
)
from trr_backend.repositories.media_links import (
    has_manual_people_tags,
    has_people_count,
    list_person_links_by_asset_id,
    update_person_links_context,
)

router = APIRouter(prefix="/admin", tags=["admin-images"])


def _is_http_url(value: str | None) -> bool:
    if not isinstance(value, str):
        return False
    trimmed = value.strip().lower()
    return trimmed.startswith("http://") or trimmed.startswith("https://")


def _iter_unique_urls(candidates: list[str | None]) -> list[str]:
    seen: set[str] = set()
    urls: list[str] = []
    for value in candidates:
        if not _is_http_url(value):
            continue
        normalized = str(value).strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls


def _build_cast_photo_count_urls(row: dict[str, Any]) -> list[str]:
    source = str(row.get("source") or "").lower()
    hosted_url = row.get("hosted_url")
    image_url = row.get("image_url")
    url = row.get("url")
    thumb_url = row.get("thumb_url")
    referer = row.get("source_page_url") if isinstance(row.get("source_page_url"), str) else None
    if source in {"fandom", "fandom-gallery"}:
        normalized = [
            normalize_fandom_file_url(str(value), referer=referer) if isinstance(value, str) else None
            for value in (image_url, url, thumb_url)
        ]
        return _iter_unique_urls([hosted_url, *normalized, image_url, url, thumb_url])
    return _iter_unique_urls([hosted_url, image_url, url, thumb_url])


def _build_media_asset_count_urls(row: dict[str, Any]) -> list[str]:
    hosted_url = row.get("hosted_url")
    source_url = row.get("source_url")
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    source_url_lower = source_url.lower() if isinstance(source_url, str) else ""
    referer = None
    if isinstance(metadata, dict):
        referer = (metadata.get("page_url") if isinstance(metadata.get("page_url"), str) else None) or (
            metadata.get("source_page_url") if isinstance(metadata.get("source_page_url"), str) else None
        )
    if isinstance(source_url, str) and (
        "fandom" in source_url_lower or "static.wikia.nocookie.net" in source_url_lower
    ):
        normalized = normalize_fandom_file_url(source_url, referer=referer)
        return _iter_unique_urls([hosted_url, normalized, source_url])
    return _iter_unique_urls([hosted_url, source_url])


def _normalize_face_coord(value: float) -> float:
    return round(max(0.0, min(1.0, value)), 4)


def _has_face_metadata_backfill_needed(metadata: Any) -> bool:
    if not isinstance(metadata, dict):
        return True
    face_boxes = metadata.get("face_boxes")
    face_crops = metadata.get("face_crops")
    return not isinstance(face_boxes, list) or not isinstance(face_crops, list)


def _is_trr_show_eligible(
    db: SupabaseAdminClient,
    *,
    metadata: Any,
    show_exists_cache: dict[str, bool] | None = None,
    show_name_cache: dict[str, str | None] | None = None,
) -> bool:
    return is_trr_show_eligible_shared(
        db,
        metadata=metadata,
        show_exists_cache=show_exists_cache,
        show_name_cache=show_name_cache,
    )


def _coerce_people_ids(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for entry in value:
        if not isinstance(entry, str):
            continue
        normalized = entry.strip()
        if normalized:
            out.append(normalized)
    return out


def _coerce_people_names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for entry in value:
        if not isinstance(entry, str):
            continue
        normalized = entry.strip()
        if normalized:
            out.append(normalized)
    return out


def _build_tagged_people(people_ids: Any, people_names: Any) -> list[dict[str, str | None]]:
    ids = _coerce_people_ids(people_ids)
    names = _coerce_people_names(people_names)
    count = max(len(ids), len(names))
    out: list[dict[str, str | None]] = []
    for idx in range(count):
        person_id = ids[idx] if idx < len(ids) else None
        person_name = names[idx] if idx < len(names) else None
        if person_id is None and person_name is None:
            continue
        out.append({"person_id": person_id, "person_name": person_name})
    return out


def _normalize_person_id(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _person_name_key(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized or None


def _resolve_owner_person_name(
    *,
    owner_person_id: str | None,
    owner_person_name: str | None,
    tagged_people_ids: Any,
    tagged_people_names: Any,
) -> str | None:
    explicit_name = str(owner_person_name or "").strip()
    if explicit_name:
        return explicit_name
    owner_id = _normalize_person_id(owner_person_id)
    tagged_people = _build_tagged_people(tagged_people_ids, tagged_people_names)
    if owner_id:
        for tagged in tagged_people:
            tagged_id = _normalize_person_id(tagged.get("person_id"))
            tagged_name = str(tagged.get("person_name") or "").strip()
            if tagged_id == owner_id and tagged_name:
                return tagged_name
    if len(tagged_people) == 1:
        tagged_name = str(tagged_people[0].get("person_name") or "").strip()
        if tagged_name:
            return tagged_name
    return None


def _build_identity_candidate_person_ids(
    *,
    db: SupabaseAdminClient | None,
    allow_identity_assignment: bool,
    owner_person_id: str | None,
    tagged_people_ids: Any,
    tagged_people_names: Any = None,
    person_name_id_cache: dict[str, str | None] | None = None,
) -> list[str]:
    return build_identity_candidate_person_ids_shared(
        db=db,
        allow_identity_assignment=allow_identity_assignment,
        owner_person_id=owner_person_id,
        tagged_people_ids=tagged_people_ids,
        tagged_people_names=tagged_people_names,
        person_name_id_cache=person_name_id_cache,
    )


def _promote_owner_similarity_assignment(
    boxes: list[dict[str, Any]],
    *,
    owner_person_id: str | None,
    owner_person_name: str | None,
    allow_identity_assignment: bool,
) -> None:
    if not allow_identity_assignment or not boxes:
        return
    owner_id = _normalize_person_id(owner_person_id)
    owner_name = str(owner_person_name or "").strip() or None
    owner_name_key = _person_name_key(owner_name)
    if not owner_id and not owner_name_key:
        return

    scored_indexes = [
        idx for idx, box in enumerate(boxes) if isinstance(box.get("match_similarity"), (int, float))
    ]
    if not scored_indexes:
        return

    winner_idx = max(
        scored_indexes,
        key=lambda idx: (
            float(boxes[idx].get("match_similarity") or 0.0),
            float(boxes[idx].get("confidence") or 0.0),
            float(boxes[idx].get("width") or 0.0) * float(boxes[idx].get("height") or 0.0),
        ),
    )

    for idx, box in enumerate(boxes):
        if idx == winner_idx:
            continue
        matches_owner_id = owner_id and _normalize_person_id(box.get("person_id")) == owner_id
        matches_owner_name = owner_name_key and _person_name_key(box.get("person_name")) == owner_name_key
        if not matches_owner_id and not matches_owner_name:
            continue
        box.pop("person_id", None)
        box.pop("person_name", None)
        if box.get("label_source") in {"identity_match", "owner_similarity_seed"}:
            box["label_source"] = "generic"
            box.pop("label", None)

    winner = boxes[winner_idx]
    if owner_id:
        winner["person_id"] = owner_id
    if owner_name:
        winner["person_name"] = owner_name
        winner["label"] = owner_name
    winner["label_source"] = "owner_similarity_seed"


def _apply_tagged_people_assignments(
    boxes: list[dict[str, Any]],
    *,
    tagged_people_ids: Any,
    tagged_people_names: Any,
) -> None:
    if not boxes:
        return
    tagged_people = _build_tagged_people(tagged_people_ids, tagged_people_names)
    if not tagged_people:
        return

    remaining_tags = list(tagged_people)
    assigned_name_keys = {
        _person_name_key(box.get("person_name"))
        for box in boxes
        if isinstance(box.get("person_name"), str) and box.get("person_name")
    }
    assigned_ids = {
        str(box.get("person_id")).strip()
        for box in boxes
        if isinstance(box.get("person_id"), str) and str(box.get("person_id")).strip()
    }
    filtered_remaining: list[dict[str, str | None]] = []
    for tagged in remaining_tags:
        tagged_id = str(tagged.get("person_id") or "").strip()
        tagged_name_key = _person_name_key(tagged.get("person_name"))
        if tagged_id and tagged_id in assigned_ids:
            continue
        if tagged_name_key and tagged_name_key in assigned_name_keys:
            continue
        filtered_remaining.append(tagged)
    remaining_tags = filtered_remaining

    unassigned_indexes = [
        idx
        for idx, box in enumerate(boxes)
        if not (
            (isinstance(box.get("person_id"), str) and str(box.get("person_id")).strip())
            or (isinstance(box.get("person_name"), str) and str(box.get("person_name")).strip())
        )
    ]
    if not unassigned_indexes or not remaining_tags:
        return

    # Best-effort mode: assign as many remaining tags as possible in stable left->right order.
    if len(remaining_tags) > len(unassigned_indexes):
        return

    sorted_unassigned = sorted(
        unassigned_indexes,
        key=lambda idx: (
            float(boxes[idx].get("x") or 0.0),
            float(boxes[idx].get("y") or 0.0),
        ),
    )
    for idx, tagged in zip(sorted_unassigned, remaining_tags, strict=False):
        tagged_id = str(tagged.get("person_id") or "").strip() or None
        tagged_name = str(tagged.get("person_name") or "").strip() or None
        if tagged_id:
            boxes[idx]["person_id"] = tagged_id
        if tagged_name:
            boxes[idx]["person_name"] = tagged_name
            boxes[idx]["label"] = tagged_name
        boxes[idx]["label_source"] = (
            "deterministic_tag_map"
            if len(remaining_tags) == len(sorted_unassigned)
            else "best_effort_tag_map"
        )


def _extract_square_crop_bbox(square_crop_bbox_raw: Any) -> list[float] | None:
    if isinstance(square_crop_bbox_raw, list) and len(square_crop_bbox_raw) >= 4:
        try:
            sx1 = _normalize_face_coord(float(square_crop_bbox_raw[0]))
            sy1 = _normalize_face_coord(float(square_crop_bbox_raw[1]))
            sx2 = _normalize_face_coord(float(square_crop_bbox_raw[2]))
            sy2 = _normalize_face_coord(float(square_crop_bbox_raw[3]))
            if sx2 > sx1 and sy2 > sy1:
                return [sx1, sy1, sx2, sy2]
        except (TypeError, ValueError):
            return None
    return None


def _extract_detection_boxes(result: Any, *, kind: str) -> list[dict[str, Any]]:
    detections = getattr(result, "detections", None) or []
    boxes: list[dict[str, Any]] = []
    for det in detections:
        det_kind = str(getattr(det, "kind", "face")).strip().lower()
        if det_kind != kind:
            continue
        try:
            x1 = float(det.x1)
            y1 = float(det.y1)
            x2 = float(det.x2)
            y2 = float(det.y2)
        except (AttributeError, TypeError, ValueError):
            continue

        x1 = _normalize_face_coord(x1)
        y1 = _normalize_face_coord(y1)
        x2 = _normalize_face_coord(x2)
        y2 = _normalize_face_coord(y2)
        width = round(max(0.0, x2 - x1), 4)
        height = round(max(0.0, y2 - y1), 4)
        if width <= 0 or height <= 0:
            continue

        confidence_raw = getattr(det, "confidence", None)
        confidence = (
            round(max(0.0, min(1.0, float(confidence_raw))), 4) if isinstance(confidence_raw, (int, float)) else None
        )
        person_id_raw = getattr(det, "person_id", None)
        person_name_raw = getattr(det, "person_name", None)
        label_raw = getattr(det, "label", None)
        match_similarity_raw = getattr(det, "match_similarity", None)
        match_status_raw = getattr(det, "match_status", None)
        square_crop_bbox_raw = getattr(det, "square_crop_bbox", None)
        person_id = str(person_id_raw).strip() if isinstance(person_id_raw, str) and person_id_raw.strip() else None
        person_name = (
            str(person_name_raw).strip() if isinstance(person_name_raw, str) and person_name_raw.strip() else None
        )
        label = str(label_raw).strip() if isinstance(label_raw, str) and label_raw.strip() else None
        match_similarity = (
            round(max(0.0, min(1.0, float(match_similarity_raw))), 4)
            if isinstance(match_similarity_raw, (int, float))
            else None
        )
        match_status = (
            str(match_status_raw).strip().lower()
            if isinstance(match_status_raw, str) and match_status_raw.strip()
            else None
        )
        square_crop_bbox = _extract_square_crop_bbox(square_crop_bbox_raw)
        boxes.append(
            {
                "kind": kind,
                "x": x1,
                "y": y1,
                "width": width,
                "height": height,
                "confidence": confidence,
                **({"person_id": person_id} if person_id else {}),
                **({"person_name": person_name} if person_name else {}),
                **({"label": label} if label else {}),
                **({"match_similarity": match_similarity} if match_similarity is not None else {}),
                **({"match_status": match_status} if match_status else {}),
                **({"square_crop_bbox": square_crop_bbox} if square_crop_bbox else {}),
            }
        )
    return boxes


def _build_detection_boxes(
    result: Any,
    *,
    tagged_people_ids: Any = None,
    tagged_people_names: Any = None,
    owner_person_id: str | None = None,
    owner_person_name: str | None = None,
    allow_identity_assignment: bool = True,
) -> list[dict[str, Any]]:
    face_boxes = _extract_detection_boxes(result, kind="face")
    if face_boxes:
        out: list[dict[str, Any]] = []
        for idx, box in enumerate(face_boxes, start=1):
            has_identity = allow_identity_assignment and bool(box.get("person_id") or box.get("person_name"))
            out.append(
                {
                    "index": idx,
                    "kind": "face",
                    "x": box["x"],
                    "y": box["y"],
                    "width": box["width"],
                    "height": box["height"],
                    "confidence": box.get("confidence"),
                    "source_kind": "face",
                    "label_source": "identity_match" if has_identity else "generic",
                    **(
                        {"person_id": box.get("person_id")}
                        if allow_identity_assignment and box.get("person_id")
                        else {}
                    ),
                    **(
                        {"person_name": box.get("person_name")}
                        if allow_identity_assignment and box.get("person_name")
                        else {}
                    ),
                    **({"label": box.get("label")} if allow_identity_assignment and box.get("label") else {}),
                    **(
                        {"match_similarity": box.get("match_similarity")}
                        if allow_identity_assignment and box.get("match_similarity") is not None
                        else {}
                    ),
                    **(
                        {"match_status": box.get("match_status")}
                        if allow_identity_assignment and box.get("match_status")
                        else {}
                    ),
                    **({"square_crop_bbox": box.get("square_crop_bbox")} if box.get("square_crop_bbox") else {}),
                }
            )
        if allow_identity_assignment:
            resolved_owner_name = _resolve_owner_person_name(
                owner_person_id=owner_person_id,
                owner_person_name=owner_person_name,
                tagged_people_ids=tagged_people_ids,
                tagged_people_names=tagged_people_names,
            )
            _promote_owner_similarity_assignment(
                out,
                owner_person_id=owner_person_id,
                owner_person_name=resolved_owner_name,
                allow_identity_assignment=allow_identity_assignment,
            )
            _apply_tagged_people_assignments(
                out,
                tagged_people_ids=tagged_people_ids,
                tagged_people_names=tagged_people_names,
            )
        return out

    person_boxes = _extract_detection_boxes(result, kind="person")
    if not person_boxes:
        return []

    person_boxes = sorted(
        person_boxes,
        key=lambda box: (
            -(box.get("confidence") if isinstance(box.get("confidence"), (int, float)) else 0.0),
            float(box.get("x") or 0.0),
            float(box.get("y") or 0.0),
        ),
    )
    tagged_people = _build_tagged_people(tagged_people_ids, tagged_people_names)
    deterministic_assignments: dict[int, dict[str, str | None]] = {}
    if allow_identity_assignment and len(tagged_people) == 1:
        deterministic_assignments[0] = tagged_people[0]
    elif allow_identity_assignment and len(tagged_people) == len(person_boxes):
        for idx, tagged in enumerate(tagged_people):
            deterministic_assignments[idx] = tagged

    out: list[dict[str, Any]] = []
    for idx, box in enumerate(person_boxes, start=1):
        assignment = deterministic_assignments.get(idx - 1)
        assigned_person_id = assignment.get("person_id") if assignment else None
        assigned_person_name = assignment.get("person_name") if assignment else None
        fallback_label = str(box.get("label") or "").strip() or f"Person {idx}"
        out.append(
            {
                "index": idx,
                "kind": "face",
                "x": box["x"],
                "y": box["y"],
                "width": box["width"],
                "height": box["height"],
                "confidence": box.get("confidence"),
                "source_kind": "person_fallback",
                "label_source": "deterministic_tag_map" if assignment else "generic",
                "fallback_reason": "no_faces_detected",
                "label": assigned_person_name or fallback_label,
                **({"person_id": assigned_person_id} if assigned_person_id else {}),
                **({"person_name": assigned_person_name} if assigned_person_name else {}),
                **({"square_crop_bbox": box.get("square_crop_bbox")} if box.get("square_crop_bbox") else {}),
            }
        )
    return out


def _build_face_boxes(result: Any) -> list[dict[str, Any]]:
    return _build_detection_boxes(result)


def _auto_people_from_face_boxes(face_boxes: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    people_ids: list[str] = []
    people_names: list[str] = []
    seen_ids: set[str] = set()
    seen_names: set[str] = set()
    for box in face_boxes:
        person_id = box.get("person_id")
        person_name = box.get("person_name")
        if isinstance(person_id, str) and person_id.strip():
            normalized = person_id.strip()
            if normalized not in seen_ids:
                seen_ids.add(normalized)
                people_ids.append(normalized)
        if isinstance(person_name, str) and person_name.strip():
            normalized_name = person_name.strip()
            key = normalized_name.lower()
            if key not in seen_names:
                seen_names.add(key)
                people_names.append(normalized_name)
    return people_ids, people_names


def _normalize_thumbnail_crop_payload(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    try:
        x = float(value.get("x"))
        y = float(value.get("y"))
        zoom = float(value.get("zoom"))
    except (TypeError, ValueError):
        return None
    x_value = x * 100.0 if 0.0 <= x <= 1.0 else x
    y_value = y * 100.0 if 0.0 <= y <= 1.0 else y

    mode_raw = str(value.get("mode") or "auto").strip().lower()
    mode = "manual" if mode_raw == "manual" else "auto"
    payload: dict[str, Any] = {
        "x": round(max(0.0, min(100.0, x_value)), 4),
        "y": round(max(0.0, min(100.0, y_value)), 4),
        "zoom": round(max(1.0, min(4.0, zoom)), 4),
        "mode": mode,
    }
    strategy = value.get("strategy")
    if isinstance(strategy, str) and strategy.strip():
        payload["strategy"] = strategy.strip()
    generated_at = value.get("generated_at")
    if isinstance(generated_at, str) and generated_at.strip():
        payload["generated_at"] = generated_at.strip()
    return payload


class FaceBox(BaseModel):
    index: int
    kind: str = "face"
    x: float
    y: float
    width: float
    height: float
    confidence: float | None = None
    person_id: str | None = None
    person_name: str | None = None
    label: str | None = None
    source_kind: str | None = None
    label_source: str | None = None
    fallback_reason: str | None = None
    match_similarity: float | None = None
    match_status: str | None = None
    square_crop_bbox: list[float] | None = None


class FaceCrop(BaseModel):
    index: int
    x: float
    y: float
    width: float
    height: float
    variant_key: str | None = None
    variant_url: str | None = None
    size: int


class AutoCountResponse(BaseModel):
    people_count: int
    face_count: int
    detector: str
    model: str | None = None
    people_count_source: str = "auto"
    face_boxes: list[FaceBox] = []
    face_crops: list[FaceCrop] = []
    thumbnail_crop: dict[str, Any] | None = None


class AutoCountShowImagesRequest(BaseModel):
    season_number: int | None = None
    force: bool = False


class AutoCountShowImagesResponse(BaseModel):
    assets_total: int
    assets_counted: int
    assets_skipped: int
    assets_failed: int


@router.post("/cast-photos/{photo_id}/auto-count", response_model=AutoCountResponse)
def auto_count_cast_photo(
    photo_id: UUID,
    force: bool = Query(default=False),
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> AutoCountResponse:
    response = (
        db.schema("core")
        .table("cast_photos")
        .select("id, person_id, hosted_url, url, image_url, thumb_url, source, source_page_url, metadata")
        .eq("id", str(photo_id))
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching cast photo")
    if not response.data:
        raise HTTPException(status_code=404, detail="Cast photo not found")

    row = response.data[0]
    image_urls = _build_cast_photo_count_urls(row)
    if not image_urls:
        raise HTTPException(
            status_code=409,
            detail="Cast photo has no valid image URL to analyze",
        )

    tag_rows = get_tags_by_photo_ids(db, [str(photo_id)])
    tag_row = tag_rows.get(str(photo_id))
    if has_manual_tags(tag_row) and not force:
        raise HTTPException(status_code=409, detail="Manual tags/count exist; use force to overwrite")

    existing_people_names = tag_row.get("people_names") if tag_row else None
    existing_people_ids = tag_row.get("people_ids") if tag_row else None
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    allow_identity_assignment = _is_trr_show_eligible(db, metadata=metadata)
    owner_person_id = _normalize_person_id(row.get("person_id"))
    owner_person_name = _resolve_owner_person_name(
        owner_person_id=owner_person_id,
        owner_person_name=None,
        tagged_people_ids=existing_people_ids,
        tagged_people_names=existing_people_names,
    )
    candidate_person_ids = _build_identity_candidate_person_ids(
        db=db,
        allow_identity_assignment=allow_identity_assignment,
        owner_person_id=owner_person_id,
        tagged_people_ids=existing_people_ids,
        tagged_people_names=existing_people_names,
    )
    result = None
    selected_image_url: str | None = None
    last_error: ScreenalyticsClientError | None = None
    for image_url in image_urls:
        try:
            if candidate_person_ids:
                result = count_people(image_url, candidate_person_ids=candidate_person_ids)
            else:
                result = count_people(image_url)
            selected_image_url = image_url
            break
        except TypeError:
            result = count_people(image_url)
            selected_image_url = image_url
            break
        except ScreenalyticsClientError as exc:
            last_error = exc
    if result is None:
        raise HTTPException(status_code=502, detail=str(last_error or "Failed to auto-count cast photo"))
    face_boxes = _build_detection_boxes(
        result,
        tagged_people_ids=existing_people_ids,
        tagged_people_names=existing_people_names,
        owner_person_id=owner_person_id,
        owner_person_name=owner_person_name,
        allow_identity_assignment=allow_identity_assignment,
    )
    auto_people_ids, auto_people_names = _auto_people_from_face_boxes(face_boxes)
    upsert_cast_photo_tags(
        db,
        cast_photo_id=str(photo_id),
        people_names=existing_people_names if existing_people_names else (auto_people_names or None),
        people_ids=existing_people_ids if existing_people_ids else (auto_people_ids or None),
        people_count=result.people_count,
        people_count_source="auto",
        detector=result.detector,
        updated_by_firebase_uid="system:auto",
    )
    generated_crop = auto_thumbnail_crop(result)
    centroid = face_centroid(result)
    metadata = dict(row.get("metadata") or {})
    metadata_changed = False
    latest_crop_payload: dict[str, Any] | None = None
    face_crops: list[dict[str, Any]] = []

    if selected_image_url and face_boxes:
        face_crops = generate_and_upload_face_crops(
            entity_kind="cast_photo",
            entity_id=str(photo_id),
            image_url=selected_image_url,
            face_boxes=face_boxes,
            size=256,
        )
    if metadata.get("face_crops") != face_crops:
        metadata["face_crops"] = face_crops
        metadata_changed = True

    if metadata.get("face_boxes") != face_boxes:
        metadata["face_boxes"] = face_boxes
        metadata_changed = True

    if generated_crop is not None or centroid is not None:
        existing_crop = metadata.get("thumbnail_crop")
        if not (isinstance(existing_crop, dict) and existing_crop.get("mode") == "manual"):
            generated_at = datetime.now(UTC).isoformat()
            if generated_crop is not None:
                metadata["thumbnail_crop"] = {
                    **generated_crop,
                    "generated_at": generated_at,
                }
                latest_crop_payload = metadata["thumbnail_crop"]
                metadata_changed = True
            elif centroid is not None:
                cx, cy = centroid
                metadata["thumbnail_crop"] = {
                    "x": cx,
                    "y": cy,
                    "zoom": 1,
                    "mode": "auto",
                    "strategy": "face_centroid_v1",
                    "generated_at": generated_at,
                }
                latest_crop_payload = metadata["thumbnail_crop"]
                metadata_changed = True

    if metadata_changed:
        try:
            db.schema("core").table("cast_photos").update({"metadata": metadata}).eq("id", str(photo_id)).execute()
        except Exception:
            # Best effort: count should still succeed if metadata write fails.
            pass

    resolved_crop_payload = _normalize_thumbnail_crop_payload(
        latest_crop_payload
        or (metadata.get("thumbnail_crop") if isinstance(metadata.get("thumbnail_crop"), dict) else None)
    )

    return AutoCountResponse(
        people_count=result.people_count,
        face_count=result.face_count,
        detector=result.detector,
        model=result.model,
        face_boxes=face_boxes,
        face_crops=face_crops,
        thumbnail_crop=resolved_crop_payload,
    )


@router.post("/media-assets/{asset_id}/auto-count", response_model=AutoCountResponse)
def auto_count_media_asset(
    asset_id: UUID,
    force: bool = Query(default=False),
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> AutoCountResponse:
    response = (
        db.schema("core")
        .table("media_assets")
        .select("id, hosted_url, source_url, metadata")
        .eq("id", str(asset_id))
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail="Database error fetching media asset")
    if not response.data:
        raise HTTPException(status_code=404, detail="Media asset not found")

    row = response.data[0]
    image_urls = _build_media_asset_count_urls(row)
    if not image_urls:
        raise HTTPException(
            status_code=409,
            detail="Media asset has no valid image URL to analyze",
        )

    links = list_person_links_by_asset_id(db, str(asset_id))
    if not links:
        raise HTTPException(status_code=404, detail="No person links found for asset")
    if any(has_manual_people_tags(link.get("context")) for link in links) and not force:
        raise HTTPException(status_code=409, detail="Manual tags/count exist; use force to overwrite")

    tagged_people_ids = next(
        (
            link.get("context", {}).get("people_ids")
            for link in links
            if isinstance(link.get("context"), dict) and isinstance(link.get("context", {}).get("people_ids"), list)
        ),
        None,
    )
    tagged_people_names = next(
        (
            link.get("context", {}).get("people_names")
            for link in links
            if isinstance(link.get("context"), dict) and isinstance(link.get("context", {}).get("people_names"), list)
        ),
        None,
    )
    show_exists_cache: dict[str, bool] = {}
    show_name_cache: dict[str, str | None] = {}
    allow_identity_assignment = any(
        _is_trr_show_eligible(
            db,
            metadata=(link.get("context") if isinstance(link.get("context"), dict) else {}),
            show_exists_cache=show_exists_cache,
            show_name_cache=show_name_cache,
        )
        for link in links
    )
    candidate_person_ids = _build_identity_candidate_person_ids(
        db=db,
        allow_identity_assignment=allow_identity_assignment,
        owner_person_id=None,
        tagged_people_ids=tagged_people_ids,
        tagged_people_names=tagged_people_names,
    )
    result = None
    selected_image_url: str | None = None
    last_error: ScreenalyticsClientError | None = None
    for image_url in image_urls:
        try:
            if candidate_person_ids:
                result = count_people(image_url, candidate_person_ids=candidate_person_ids)
            else:
                result = count_people(image_url)
            selected_image_url = image_url
            break
        except TypeError:
            result = count_people(image_url)
            selected_image_url = image_url
            break
        except ScreenalyticsClientError as exc:
            last_error = exc
    if result is None:
        raise HTTPException(status_code=502, detail=str(last_error or "Failed to auto-count media asset"))
    face_boxes = _build_detection_boxes(
        result,
        tagged_people_ids=tagged_people_ids,
        tagged_people_names=tagged_people_names,
        owner_person_id=None,
        owner_person_name=None,
        allow_identity_assignment=allow_identity_assignment,
    )
    auto_people_ids, auto_people_names = _auto_people_from_face_boxes(face_boxes)
    face_crops: list[dict[str, Any]] = []
    if selected_image_url and face_boxes:
        face_crops = generate_and_upload_face_crops(
            entity_kind="media_asset",
            entity_id=str(asset_id),
            image_url=selected_image_url,
            face_boxes=face_boxes,
            size=256,
        )
    context_auto_update = {
        "people_count": result.people_count,
        "people_count_source": "auto",
        "people_count_detector": result.detector,
        "face_boxes": face_boxes,
        "face_crops": face_crops,
        **({"people_ids": auto_people_ids} if auto_people_ids else {}),
        **({"people_names": auto_people_names} if auto_people_names else {}),
    }
    update_person_links_context(
        db,
        links,
        context_auto_update,
    )
    generated_crop = auto_thumbnail_crop(result)
    centroid = face_centroid(result)
    latest_crop_payload: dict[str, Any] | None = None
    if generated_crop is not None or centroid is not None:
        now = datetime.now(UTC).isoformat()
        for link in links:
            context = {**dict(link.get("context") or {}), **context_auto_update}
            existing_crop = context.get("thumbnail_crop")
            if isinstance(existing_crop, dict) and existing_crop.get("mode") == "manual":
                continue
            if generated_crop is not None:
                context["thumbnail_crop"] = {
                    **generated_crop,
                    "generated_at": now,
                }
                latest_crop_payload = context["thumbnail_crop"]
            elif centroid is not None:
                cx, cy = centroid
                context["thumbnail_crop"] = {
                    "x": cx,
                    "y": cy,
                    "zoom": 1,
                    "mode": "auto",
                    "strategy": "face_centroid_v1",
                    "generated_at": now,
                }
                latest_crop_payload = context["thumbnail_crop"]
            try:
                db.schema("core").table("media_links").update({"context": context, "updated_at": now}).eq(
                    "id", link["id"]
                ).execute()
            except Exception:
                # Best effort: count should still succeed if crop write fails.
                continue

    if latest_crop_payload is not None:
        try:
            generate_media_asset_variants(
                db,
                asset_id=str(asset_id),
                crop=latest_crop_payload,
                force=False,
            )
        except Exception:
            # Best effort: auto-count should still succeed if variant generation fails.
            pass

    resolved_crop_payload = _normalize_thumbnail_crop_payload(latest_crop_payload)
    if resolved_crop_payload is None:
        for link in links:
            context = link.get("context") if isinstance(link.get("context"), dict) else {}
            resolved_crop_payload = _normalize_thumbnail_crop_payload(context.get("thumbnail_crop"))
            if resolved_crop_payload is not None:
                break
    if resolved_crop_payload is None:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        resolved_crop_payload = _normalize_thumbnail_crop_payload(metadata.get("thumbnail_crop"))

    return AutoCountResponse(
        people_count=result.people_count,
        face_count=result.face_count,
        detector=result.detector,
        model=result.model,
        face_boxes=face_boxes,
        face_crops=face_crops,
        thumbnail_crop=resolved_crop_payload,
    )


@router.post("/shows/{show_id}/auto-count-images", response_model=AutoCountShowImagesResponse)
def auto_count_show_images(
    show_id: UUID,
    payload: AutoCountShowImagesRequest,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> AutoCountShowImagesResponse:
    show_id_str = str(show_id)

    # Fetch seasons for the show (optionally filter by season_number)
    season_query = db.schema("core").table("seasons").select("id, season_number").eq("show_id", show_id_str)
    if payload.season_number is not None:
        season_query = season_query.eq("season_number", payload.season_number)
    season_response = season_query.execute()
    if hasattr(season_response, "error") and season_response.error:
        raise HTTPException(status_code=502, detail="Database error fetching seasons")

    seasons = season_response.data or []
    season_ids = [row["id"] for row in seasons]

    if payload.season_number is not None and not season_ids:
        return AutoCountShowImagesResponse(
            assets_total=0,
            assets_counted=0,
            assets_skipped=0,
            assets_failed=0,
        )

    # Fetch episodes for the show (optionally filter by season)
    episode_query = db.schema("core").table("episodes").select("id, season_id").eq("show_id", show_id_str)
    if season_ids:
        episode_query = episode_query.in_("season_id", season_ids)
    episode_response = episode_query.execute()
    if hasattr(episode_response, "error") and episode_response.error:
        raise HTTPException(status_code=502, detail="Database error fetching episodes")

    episode_ids = [row["id"] for row in (episode_response.data or [])]

    # Fetch media_links for show/season/episode entities
    links: list[dict[str, Any]] = []
    try:
        show_links = (
            db.schema("core")
            .table("media_links")
            .select("id, media_asset_id, context")
            .eq("entity_type", "show")
            .eq("entity_id", show_id_str)
            .execute()
        )
        if not (hasattr(show_links, "error") and show_links.error):
            links.extend(show_links.data or [])
    except Exception:
        pass

    if season_ids:
        season_links = (
            db.schema("core")
            .table("media_links")
            .select("id, media_asset_id, context")
            .eq("entity_type", "season")
            .in_("entity_id", season_ids)
            .execute()
        )
        if hasattr(season_links, "error") and season_links.error:
            raise HTTPException(status_code=502, detail="Database error fetching season links")
        links.extend(season_links.data or [])

    if episode_ids:
        episode_links = (
            db.schema("core")
            .table("media_links")
            .select("id, media_asset_id, context")
            .eq("entity_type", "episode")
            .in_("entity_id", episode_ids)
            .execute()
        )
        if hasattr(episode_links, "error") and episode_links.error:
            raise HTTPException(status_code=502, detail="Database error fetching episode links")
        links.extend(episode_links.data or [])

    # Group links by media_asset_id
    links_by_asset: dict[str, list[dict[str, Any]]] = {}
    for link in links:
        media_asset_id = link.get("media_asset_id")
        if not media_asset_id:
            continue
        links_by_asset.setdefault(media_asset_id, []).append(link)

    asset_ids = list(links_by_asset.keys())
    if not asset_ids:
        return AutoCountShowImagesResponse(
            assets_total=0,
            assets_counted=0,
            assets_skipped=0,
            assets_failed=0,
        )

    assets_response = (
        db.schema("core")
        .table("media_assets")
        .select("id, hosted_url, source_url, metadata")
        .in_("id", asset_ids)
        .execute()
    )
    if hasattr(assets_response, "error") and assets_response.error:
        raise HTTPException(status_code=502, detail="Database error fetching media assets")

    assets = {row["id"]: row for row in (assets_response.data or [])}

    assets_total = len(asset_ids)
    assets_counted = 0
    assets_skipped = 0
    assets_failed = 0
    show_exists_cache: dict[str, bool] = {}
    show_name_cache: dict[str, str | None] = {}

    for asset_id in asset_ids:
        links_for_asset = links_by_asset.get(asset_id, [])
        if not links_for_asset:
            assets_skipped += 1
            continue

        if not payload.force:
            if any(has_manual_people_tags(link.get("context")) for link in links_for_asset):
                assets_skipped += 1
                continue
            has_any_people_count = any(has_people_count(link.get("context")) for link in links_for_asset)
            has_any_backfill_needed = any(
                _has_face_metadata_backfill_needed(link.get("context")) for link in links_for_asset
            )
            if has_any_people_count and not has_any_backfill_needed:
                assets_skipped += 1
                continue

        asset = assets.get(asset_id)
        if not asset:
            assets_skipped += 1
            continue

        image_urls = _build_media_asset_count_urls(asset)
        if not image_urls:
            assets_skipped += 1
            continue

        tagged_people_ids = next(
            (
                link.get("context", {}).get("people_ids")
                for link in links_for_asset
                if isinstance(link.get("context"), dict)
                and isinstance(link.get("context", {}).get("people_ids"), list)
            ),
            None,
        )
        tagged_people_names = next(
            (
                link.get("context", {}).get("people_names")
                for link in links_for_asset
                if isinstance(link.get("context"), dict)
                and isinstance(link.get("context", {}).get("people_names"), list)
            ),
            None,
        )
        allow_identity_assignment = any(
            _is_trr_show_eligible(
                db,
                metadata=(link.get("context") if isinstance(link.get("context"), dict) else {}),
                show_exists_cache=show_exists_cache,
                show_name_cache=show_name_cache,
            )
            for link in links_for_asset
        )
        candidate_person_ids = _build_identity_candidate_person_ids(
            db=db,
            allow_identity_assignment=allow_identity_assignment,
            owner_person_id=None,
            tagged_people_ids=tagged_people_ids,
            tagged_people_names=tagged_people_names,
        )
        result = None
        selected_image_url: str | None = None
        try:
            for image_url in image_urls:
                try:
                    if candidate_person_ids:
                        result = count_people(image_url, candidate_person_ids=candidate_person_ids)
                    else:
                        result = count_people(image_url)
                    selected_image_url = image_url
                    break
                except TypeError:
                    result = count_people(image_url)
                    selected_image_url = image_url
                    break
                except ScreenalyticsClientError:
                    continue
        except Exception:
            result = None

        if result is None:
            assets_failed += 1
            continue
        face_boxes = _build_detection_boxes(
            result,
            tagged_people_ids=tagged_people_ids,
            tagged_people_names=tagged_people_names,
            owner_person_id=None,
            owner_person_name=None,
            allow_identity_assignment=allow_identity_assignment,
        )
        auto_people_ids, auto_people_names = _auto_people_from_face_boxes(face_boxes)
        face_crops: list[dict[str, Any]] = []
        if selected_image_url and face_boxes:
            face_crops = generate_and_upload_face_crops(
                entity_kind="media_asset",
                entity_id=str(asset_id),
                image_url=selected_image_url,
                face_boxes=face_boxes,
                size=256,
            )
        context_auto_update = {
            "people_count": result.people_count,
            "people_count_source": "auto",
            "people_count_detector": result.detector,
            "face_boxes": face_boxes,
            "face_crops": face_crops,
            **({"people_ids": auto_people_ids} if auto_people_ids else {}),
            **({"people_names": auto_people_names} if auto_people_names else {}),
        }
        update_person_links_context(
            db,
            links_for_asset,
            context_auto_update,
        )

        generated_crop = auto_thumbnail_crop(result)
        centroid = face_centroid(result)
        latest_crop_payload: dict[str, Any] | None = None
        if generated_crop is not None or centroid is not None:
            now = datetime.now(UTC).isoformat()
            for link in links_for_asset:
                context = {**dict(link.get("context") or {}), **context_auto_update}
                existing_crop = context.get("thumbnail_crop")
                if isinstance(existing_crop, dict) and existing_crop.get("mode") == "manual":
                    continue
                if generated_crop is not None:
                    context["thumbnail_crop"] = {
                        **generated_crop,
                        "generated_at": now,
                    }
                    latest_crop_payload = context["thumbnail_crop"]
                elif centroid is not None:
                    cx, cy = centroid
                    context["thumbnail_crop"] = {
                        "x": cx,
                        "y": cy,
                        "zoom": 1,
                        "mode": "auto",
                        "strategy": "face_centroid_v1",
                        "generated_at": now,
                    }
                    latest_crop_payload = context["thumbnail_crop"]
                try:
                    db.schema("core").table("media_links").update({"context": context, "updated_at": now}).eq(
                        "id", link["id"]
                    ).execute()
                except Exception:
                    continue

        if latest_crop_payload is not None:
            try:
                generate_media_asset_variants(
                    db,
                    asset_id=str(asset_id),
                    crop=latest_crop_payload,
                    force=False,
                )
            except Exception:
                pass
        assets_counted += 1

    return AutoCountShowImagesResponse(
        assets_total=assets_total,
        assets_counted=assets_counted,
        assets_skipped=assets_skipped,
        assets_failed=assets_failed,
    )
