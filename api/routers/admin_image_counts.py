"""Admin endpoints for auto-counting people in images."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

if TYPE_CHECKING:
    from collections.abc import Mapping

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.auth import InternalAdminUser
from api.deps import SupabaseAdminClient
from trr_backend.media.face_crops import generate_and_upload_face_crops
from trr_backend.media.image_variants import generate_media_asset_variants
from trr_backend.media.s3_mirror import normalize_fandom_file_url
from trr_backend.repositories.cast_photo_tags import (
    get_tags_by_photo_ids,
    has_manual_tags,
    upsert_cast_photo_tags,
)
from trr_backend.repositories.identity_assignment import (
    build_assignment_tagged_people as build_assignment_tagged_people_shared,
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
from trr_backend.repositories.tagging_references import (
    build_owner_tagging_reference_profile,
    sync_owner_tagging_reference_usage,
)
from trr_backend.vision.people_count_service import (
    PeopleCountServiceError,
    auto_thumbnail_crop,
    count_people,
    face_centroid,
)

router = APIRouter(prefix="/admin", tags=["admin-images"])
logger = logging.getLogger(__name__)
OWNER_FACE_MATCH_SIMILARITY_MIN_DEFAULT = 0.50
OWNER_FALLBACK_CROP_MIN_CONFIDENCE_DEFAULT = 0.80
FACE_MATCH_CROSS_FACE_LEAD_MIN = 0.45
FACE_MATCH_CROSS_FACE_LEAD_MIN_SIMILARITY = 0.30
FACE_MATCH_SCORE_EVIDENCE_MIN = 1e-6


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


def _owner_face_match_similarity_min() -> float:
    raw = str(os.getenv("OWNER_FACE_MATCH_SIMILARITY_MIN") or "").strip()
    if not raw:
        return OWNER_FACE_MATCH_SIMILARITY_MIN_DEFAULT
    try:
        value = float(raw)
    except ValueError:
        return OWNER_FACE_MATCH_SIMILARITY_MIN_DEFAULT
    return max(0.0, min(1.0, value))


def _owner_fallback_crop_min_confidence() -> float:
    raw = str(os.getenv("OWNER_FALLBACK_CROP_MIN_CONFIDENCE") or "").strip()
    if not raw:
        return OWNER_FALLBACK_CROP_MIN_CONFIDENCE_DEFAULT
    try:
        value = float(raw)
    except ValueError:
        return OWNER_FALLBACK_CROP_MIN_CONFIDENCE_DEFAULT
    return max(0.0, min(1.0, value))


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
    metadata_signals: list[Any] | None = None,
    person_name_id_cache: dict[str, str | None] | None = None,
) -> list[str]:
    return build_identity_candidate_person_ids_shared(
        db=db,
        allow_identity_assignment=allow_identity_assignment,
        owner_person_id=owner_person_id,
        tagged_people_ids=tagged_people_ids,
        tagged_people_names=tagged_people_names,
        metadata_signals=metadata_signals,
        person_name_id_cache=person_name_id_cache,
    )


def _build_assignment_tagged_people(
    *,
    db: SupabaseAdminClient | None,
    owner_person_id: str | None,
    owner_person_name: str | None = None,
    tagged_people_ids: Any = None,
    tagged_people_names: Any = None,
    row_people_ids: Any = None,
    row_people_names: Any = None,
    metadata_signals: list[Any] | None = None,
    person_name_id_cache: dict[str, str | None] | None = None,
    person_id_name_cache: dict[str, str | None] | None = None,
) -> tuple[list[str], list[str]]:
    return build_assignment_tagged_people_shared(
        db=db,
        owner_person_id=owner_person_id,
        owner_person_name=owner_person_name,
        tagged_people_ids=tagged_people_ids,
        tagged_people_names=tagged_people_names,
        row_people_ids=row_people_ids,
        row_people_names=row_people_names,
        metadata_signals=metadata_signals,
        person_name_id_cache=person_name_id_cache,
        person_id_name_cache=person_id_name_cache,
    )


def _resolve_person_name_by_id(
    db: SupabaseAdminClient | None,
    person_id: str | None,
    *,
    person_id_name_cache: dict[str, str | None] | None = None,
) -> str | None:
    normalized_person_id = _normalize_person_id(person_id)
    if not normalized_person_id:
        return None
    if person_id_name_cache is None:
        person_id_name_cache = {}
    if normalized_person_id in person_id_name_cache:
        return person_id_name_cache[normalized_person_id]
    if db is None:
        person_id_name_cache[normalized_person_id] = None
        return None

    resolved_name: str | None = None
    try:
        response = (
            db.schema("core").table("people").select("full_name").eq("id", normalized_person_id).limit(1).execute()
        )
        rows = response.data if isinstance(getattr(response, "data", None), list) else []
        if rows and isinstance(rows[0], dict):
            candidate = rows[0].get("full_name")
            if isinstance(candidate, str) and candidate.strip():
                resolved_name = candidate.strip()
    except Exception:  # noqa: BLE001
        resolved_name = None

    person_id_name_cache[normalized_person_id] = resolved_name
    return resolved_name


def _resolve_runtime_person_reference_pools(
    db: SupabaseAdminClient,
    *,
    candidate_person_ids: list[str] | None,
    request_show_id: UUID | None,
    request_show_name: str | None,
    reference_cache: dict[str, list[dict[str, Any]]],
    person_id_name_cache: dict[str, str | None] | None = None,
) -> list[dict[str, Any]]:
    pools: list[dict[str, Any]] = []
    if not candidate_person_ids:
        return pools
    for raw_person_id in candidate_person_ids:
        person_id = str(raw_person_id or "").strip()
        if not person_id:
            continue
        if person_id not in reference_cache:
            references: list[dict[str, Any]] = []
            try:
                profile = build_owner_tagging_reference_profile(
                    db,
                    person_id,
                    show_id=request_show_id,
                    show_name=request_show_name,
                )
                used_raw = profile.get("used")
                if isinstance(used_raw, list):
                    references = cast("list[dict[str, Any]]", [entry for entry in used_raw if isinstance(entry, dict)])
                if references:
                    references = cast(
                        "list[dict[str, Any]]",
                        sync_owner_tagging_reference_usage(
                            db,
                            person_id,
                            used_references=references,
                        ),
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to resolve runtime tagging references person_id=%s error=%s",
                    person_id,
                    exc,
                )
                references = []
            reference_cache[person_id] = references
        references = reference_cache.get(person_id) or []
        if references:
            pool_payload: dict[str, Any] = {"person_id": person_id, "references": references}
            resolved_name = _resolve_person_name_by_id(
                db,
                person_id,
                person_id_name_cache=person_id_name_cache,
            )
            if isinstance(resolved_name, str) and resolved_name:
                pool_payload["person_name"] = resolved_name
            pools.append(pool_payload)
    return pools


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

    owner_indexes = []
    for idx, box in enumerate(boxes):
        matches_owner_id = owner_id and _normalize_person_id(box.get("person_id")) == owner_id
        matches_owner_name = owner_name_key and _person_name_key(box.get("person_name")) == owner_name_key
        if matches_owner_id or matches_owner_name:
            owner_indexes.append(idx)
    if len(owner_indexes) <= 1:
        return

    winner_idx = max(
        owner_indexes,
        key=lambda idx: (
            1 if str(boxes[idx].get("match_status") or "").strip().lower() == "matched" else 0,
            float(boxes[idx].get("match_similarity") or 0.0),
            float(boxes[idx].get("confidence") or 0.0),
            float(boxes[idx].get("width") or 0.0) * float(boxes[idx].get("height") or 0.0),
        ),
    )

    for idx in owner_indexes:
        box = boxes[idx]
        if idx == winner_idx:
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


def _face_similarity_for_tagged_person(
    box: dict[str, Any],
    *,
    person_id: str | None,
    person_name_key: str | None,
) -> float | None:
    if not person_id and not person_name_key:
        return None
    best_similarity: float | None = None

    match_similarity = box.get("match_similarity")
    if isinstance(match_similarity, (int, float)):
        box_person_id = _normalize_person_id(box.get("person_id"))
        box_person_name_key = _person_name_key(box.get("person_name"))
        if (person_id and box_person_id == person_id) or (person_name_key and box_person_name_key == person_name_key):
            best_similarity = float(match_similarity)

    match_candidates = box.get("match_candidates")
    if isinstance(match_candidates, list):
        for candidate in match_candidates:
            if not isinstance(candidate, dict):
                continue
            similarity = candidate.get("similarity")
            if not isinstance(similarity, (int, float)):
                continue
            candidate_person_id = _normalize_person_id(candidate.get("person_id"))
            candidate_person_name_key = _person_name_key(candidate.get("person_name"))
            if not (
                (person_id and candidate_person_id == person_id)
                or (person_name_key and candidate_person_name_key == person_name_key)
            ):
                continue
            similarity_value = float(similarity)
            if best_similarity is None or similarity_value > best_similarity:
                best_similarity = similarity_value

    if best_similarity is None:
        return None
    return max(0.0, min(1.0, best_similarity))


def _tagged_person_has_similarity_evidence(
    tagged: dict[str, str | None],
    boxes: list[dict[str, Any]],
) -> bool:
    tagged_id = _normalize_person_id(tagged.get("person_id"))
    tagged_name_key = _person_name_key(tagged.get("person_name"))
    if not tagged_id and not tagged_name_key:
        return False
    for box in boxes:
        similarity = _face_similarity_for_tagged_person(
            box,
            person_id=tagged_id,
            person_name_key=tagged_name_key,
        )
        if similarity is None:
            continue
        if similarity > FACE_MATCH_SCORE_EVIDENCE_MIN:
            return True
    return False


def _apply_similarity_lead_assignments(
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

    claims: list[dict[str, Any]] = []
    for tagged in tagged_people:
        tagged_id = _normalize_person_id(tagged.get("person_id"))
        tagged_name = str(tagged.get("person_name") or "").strip() or None
        tagged_name_key = _person_name_key(tagged_name)
        if not tagged_id and not tagged_name_key:
            continue

        ranked_faces: list[tuple[int, float, float, float]] = []
        for index, box in enumerate(boxes):
            similarity = _face_similarity_for_tagged_person(
                box,
                person_id=tagged_id,
                person_name_key=tagged_name_key,
            )
            if similarity is None:
                continue
            ranked_faces.append(
                (
                    index,
                    similarity,
                    float(box.get("confidence") or 0.0),
                    float(box.get("width") or 0.0) * float(box.get("height") or 0.0),
                )
            )

        if not ranked_faces:
            continue
        ranked_faces.sort(key=lambda item: (item[1], item[2], item[3]), reverse=True)
        best_index, best_similarity, best_confidence, best_area = ranked_faces[0]
        second_similarity = ranked_faces[1][1] if len(ranked_faces) > 1 else 0.0
        if best_similarity < FACE_MATCH_CROSS_FACE_LEAD_MIN_SIMILARITY:
            continue
        if (best_similarity - second_similarity) < FACE_MATCH_CROSS_FACE_LEAD_MIN:
            continue
        claims.append(
            {
                "index": best_index,
                "person_id": tagged_id,
                "person_name": tagged_name,
                "person_name_key": tagged_name_key,
                "similarity": best_similarity,
                "confidence": best_confidence,
                "area": best_area,
            }
        )

    if not claims:
        return
    claims.sort(key=lambda item: (item["similarity"], item["confidence"], item["area"]), reverse=True)
    claimed_faces: set[int] = set()
    claimed_people: set[str] = set()

    for claim in claims:
        box_index = int(claim["index"])
        if box_index in claimed_faces:
            continue
        person_id = claim.get("person_id")
        person_name = claim.get("person_name")
        person_name_key = claim.get("person_name_key")
        person_key = str(person_id or f"name:{person_name_key or ''}").strip()
        if not person_key or person_key in claimed_people:
            continue

        box = boxes[box_index]
        existing_label_source = str(box.get("label_source") or "").strip().lower()
        existing_person_id = _normalize_person_id(box.get("person_id"))
        existing_person_name_key = _person_name_key(box.get("person_name"))
        same_person = bool(
            (person_id and existing_person_id == person_id)
            or (person_name_key and existing_person_name_key == person_name_key)
        )
        if existing_label_source in {"identity_match", "owner_similarity_seed", "lead_override"} and not same_person:
            continue

        if person_id:
            box["person_id"] = person_id
        if isinstance(person_name, str) and person_name:
            box["person_name"] = person_name
            box["label"] = person_name
        box["label_source"] = "lead_override"
        box["match_status"] = "matched"
        box["match_reason"] = "cross_face_lead_override"
        box["match_similarity"] = round(float(claim["similarity"]), 4)
        claimed_faces.add(box_index)
        claimed_people.add(person_key)


def _has_any_similarity_evidence(boxes: list[dict[str, Any]]) -> bool:
    for box in boxes:
        match_similarity = box.get("match_similarity")
        if isinstance(match_similarity, (int, float)) and float(match_similarity) > FACE_MATCH_SCORE_EVIDENCE_MIN:
            return True
        raw_candidates = box.get("match_candidates")
        if not isinstance(raw_candidates, list):
            continue
        for candidate in raw_candidates:
            if not isinstance(candidate, dict):
                continue
            similarity = candidate.get("similarity")
            if isinstance(similarity, (int, float)) and float(similarity) > FACE_MATCH_SCORE_EVIDENCE_MIN:
                return True
    return False


def _apply_owner_only_fallback_assignment(
    boxes: list[dict[str, Any]],
    *,
    owner_person_id: str | None = None,
    owner_person_name: str | None = None,
) -> bool:
    owner_id = _normalize_person_id(owner_person_id)
    owner_name = str(owner_person_name or "").strip() or None
    if not owner_id and not owner_name:
        return False
    protected_label_sources = {"identity_match", "owner_similarity_seed", "lead_override", "owner_fallback_map"}
    owner_name_key = _person_name_key(owner_name)

    for box in boxes:
        box_person_id = _normalize_person_id(box.get("person_id"))
        box_person_name_key = _person_name_key(box.get("person_name"))
        if (owner_id and box_person_id == owner_id) or (owner_name_key and box_person_name_key == owner_name_key):
            box["label_source"] = "owner_fallback_map"
            box["match_status"] = "matched"
            box["match_reason"] = "owner_fallback_map"
            return True

    candidate_indexes = [
        idx
        for idx, box in enumerate(boxes)
        if str(box.get("label_source") or "").strip().lower() not in protected_label_sources
    ]
    if not candidate_indexes:
        candidate_indexes = list(range(len(boxes)))

    if not candidate_indexes:
        return False

    best_index = max(
        candidate_indexes,
        key=lambda idx: (
            float(boxes[idx].get("confidence") or 0.0),
            float(boxes[idx].get("width") or 0.0) * float(boxes[idx].get("height") or 0.0),
            -float(boxes[idx].get("x") or 0.0),
        ),
    )
    best_box = boxes[best_index]
    if owner_id:
        best_box["person_id"] = owner_id
    if owner_name:
        best_box["person_name"] = owner_name
        best_box["label"] = owner_name
    best_box["label_source"] = "owner_fallback_map"
    best_box["match_status"] = "matched"
    best_box["match_reason"] = "owner_fallback_map"
    return True


def _apply_tagged_people_assignments(
    boxes: list[dict[str, Any]],
    *,
    tagged_people_ids: Any,
    tagged_people_names: Any,
    owner_person_id: str | None = None,
    owner_person_name: str | None = None,
) -> None:
    if not boxes:
        return
    if len(boxes) == 1 and not _has_any_similarity_evidence(boxes):
        if _apply_owner_only_fallback_assignment(
            boxes,
            owner_person_id=owner_person_id,
            owner_person_name=owner_person_name,
        ):
            return
    if len(boxes) > 1 and not _has_any_similarity_evidence(boxes):
        if _apply_owner_only_fallback_assignment(
            boxes,
            owner_person_id=owner_person_id,
            owner_person_name=owner_person_name,
        ):
            return
    tagged_people = _build_tagged_people(tagged_people_ids, tagged_people_names)
    if not tagged_people:
        if len(boxes) == 1:
            _apply_owner_only_fallback_assignment(
                boxes,
                owner_person_id=owner_person_id,
                owner_person_name=owner_person_name,
            )
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

    scored_remaining_tags = [
        tagged for tagged in remaining_tags if _tagged_person_has_similarity_evidence(tagged, boxes)
    ]
    if scored_remaining_tags:
        remaining_tags = [
            tagged for tagged in remaining_tags if not _tagged_person_has_similarity_evidence(tagged, boxes)
        ]

    protected_label_sources = {"identity_match", "owner_similarity_seed", "lead_override", "owner_fallback_map"}
    unassigned_indexes = [
        idx
        for idx, box in enumerate(boxes)
        if not (
            (isinstance(box.get("person_id"), str) and str(box.get("person_id")).strip())
            or (isinstance(box.get("person_name"), str) and str(box.get("person_name")).strip())
        )
        and str(box.get("label_source") or "").strip().lower() not in protected_label_sources
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
        tagged_name_key = _person_name_key(tagged_name)
        if tagged_id:
            boxes[idx]["person_id"] = tagged_id
        if tagged_name:
            boxes[idx]["person_name"] = tagged_name
            boxes[idx]["label"] = tagged_name
        label_source = (
            "deterministic_tag_map" if len(remaining_tags) == len(sorted_unassigned) else "best_effort_tag_map"
        )
        boxes[idx]["label_source"] = label_source
        boxes[idx]["match_status"] = "matched"
        boxes[idx]["match_reason"] = label_source
        similarity = _face_similarity_for_tagged_person(
            boxes[idx],
            person_id=_normalize_person_id(tagged_id),
            person_name_key=tagged_name_key,
        )
        if similarity is not None:
            boxes[idx]["match_similarity"] = round(float(similarity), 4)


def _backfill_assigned_person_names(
    boxes: list[dict[str, Any]],
    *,
    person_name_lookup_by_id: dict[str, str],
) -> None:
    if not boxes or not person_name_lookup_by_id:
        return
    for box in boxes:
        person_id = _normalize_person_id(box.get("person_id"))
        if not person_id:
            continue
        has_person_name = isinstance(box.get("person_name"), str) and str(box.get("person_name")).strip()
        if has_person_name:
            continue
        resolved_name = person_name_lookup_by_id.get(person_id)
        if not isinstance(resolved_name, str) or not resolved_name:
            continue
        box["person_name"] = resolved_name
        box["label"] = resolved_name


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
        match_reason_raw = getattr(det, "match_reason", None)
        match_candidates_raw = getattr(det, "match_candidates", None)
        filter_decision_raw = getattr(det, "filter_decision", None)
        filter_metrics_raw = getattr(det, "filter_metrics", None)
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
        match_reason = (
            str(match_reason_raw).strip().lower()
            if isinstance(match_reason_raw, str) and match_reason_raw.strip()
            else None
        )
        filter_decision = (
            str(filter_decision_raw).strip().lower()
            if isinstance(filter_decision_raw, str) and str(filter_decision_raw).strip()
            else None
        )
        filter_metrics: dict[str, float] | None = None
        if isinstance(filter_metrics_raw, dict):
            candidate_metrics: dict[str, float] = {}
            for key in ("face_w", "face_h", "face_area_ratio"):
                raw_metric = filter_metrics_raw.get(key)
                if isinstance(raw_metric, (int, float)):
                    candidate_metrics[key] = float(raw_metric)
            if candidate_metrics:
                filter_metrics = candidate_metrics
        match_candidates: list[dict[str, Any]] = []
        if isinstance(match_candidates_raw, list):
            for candidate in match_candidates_raw:
                if not isinstance(candidate, dict):
                    continue
                similarity_raw = candidate.get("similarity")
                if not isinstance(similarity_raw, (int, float)):
                    continue
                normalized_candidate: dict[str, Any] = {
                    "similarity": round(max(0.0, min(1.0, float(similarity_raw))), 4)
                }
                person_id_candidate = candidate.get("person_id")
                if isinstance(person_id_candidate, str) and person_id_candidate.strip():
                    normalized_candidate["person_id"] = person_id_candidate.strip()
                person_name_candidate = candidate.get("person_name")
                if isinstance(person_name_candidate, str) and person_name_candidate.strip():
                    normalized_candidate["person_name"] = person_name_candidate.strip()
                match_candidates.append(normalized_candidate)
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
                **({"match_reason": match_reason} if match_reason else {}),
                **({"match_candidates": match_candidates} if match_candidates else {}),
                **({"filter_decision": filter_decision} if filter_decision else {}),
                **({"filter_metrics": filter_metrics} if filter_metrics else {}),
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
    explicit_owner_name = str(owner_person_name or "").strip() or None
    tagged_people_lookup = _build_tagged_people(tagged_people_ids, tagged_people_names)
    person_name_lookup_by_id: dict[str, str] = {}
    for tagged in tagged_people_lookup:
        tagged_id = _normalize_person_id(tagged.get("person_id"))
        tagged_name = str(tagged.get("person_name") or "").strip()
        if tagged_id and tagged_name and tagged_id not in person_name_lookup_by_id:
            person_name_lookup_by_id[tagged_id] = tagged_name
    normalized_owner_id = _normalize_person_id(owner_person_id)
    normalized_owner_name = explicit_owner_name or ""
    if normalized_owner_id and normalized_owner_name and normalized_owner_id not in person_name_lookup_by_id:
        person_name_lookup_by_id[normalized_owner_id] = normalized_owner_name

    if face_boxes:
        out: list[dict[str, Any]] = []
        for idx, box in enumerate(face_boxes, start=1):
            if allow_identity_assignment:
                box_person_id = _normalize_person_id(box.get("person_id"))
                if box_person_id and not str(box.get("person_name") or "").strip():
                    resolved_name = person_name_lookup_by_id.get(box_person_id)
                    if isinstance(resolved_name, str) and resolved_name:
                        box["person_name"] = resolved_name
                        if not str(box.get("label") or "").strip():
                            box["label"] = resolved_name
                if isinstance(box.get("match_candidates"), list) and person_name_lookup_by_id:
                    enriched_candidates: list[dict[str, Any]] = []
                    for raw_candidate in box.get("match_candidates") or []:
                        if not isinstance(raw_candidate, dict):
                            continue
                        candidate = dict(raw_candidate)
                        candidate_person_id = _normalize_person_id(candidate.get("person_id"))
                        has_name = bool(
                            isinstance(candidate.get("person_name"), str) and str(candidate.get("person_name")).strip()
                        )
                        if candidate_person_id and not has_name:
                            resolved_candidate_name = person_name_lookup_by_id.get(candidate_person_id)
                            if isinstance(resolved_candidate_name, str) and resolved_candidate_name:
                                candidate["person_name"] = resolved_candidate_name
                        enriched_candidates.append(candidate)
                    box["match_candidates"] = enriched_candidates
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
                    **(
                        {"match_reason": box.get("match_reason")}
                        if allow_identity_assignment and box.get("match_reason")
                        else {}
                    ),
                    **(
                        {"match_candidates": box.get("match_candidates")}
                        if allow_identity_assignment and isinstance(box.get("match_candidates"), list)
                        else {}
                    ),
                    **({"filter_decision": box.get("filter_decision")} if box.get("filter_decision") else {}),
                    **(
                        {"filter_metrics": box.get("filter_metrics")}
                        if isinstance(box.get("filter_metrics"), dict)
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
            owner_name_for_assignment = resolved_owner_name if (normalized_owner_id or explicit_owner_name) else None
            _promote_owner_similarity_assignment(
                out,
                owner_person_id=owner_person_id,
                owner_person_name=owner_name_for_assignment,
                allow_identity_assignment=allow_identity_assignment,
            )
            _apply_similarity_lead_assignments(
                out,
                tagged_people_ids=tagged_people_ids,
                tagged_people_names=tagged_people_names,
            )
            _apply_tagged_people_assignments(
                out,
                tagged_people_ids=tagged_people_ids,
                tagged_people_names=tagged_people_names,
                owner_person_id=owner_person_id,
                owner_person_name=owner_name_for_assignment,
            )
            _backfill_assigned_person_names(
                out,
                person_name_lookup_by_id=person_name_lookup_by_id,
            )
        return out

    person_boxes = _extract_detection_boxes(result, kind="person")
    if not person_boxes:
        return []

    person_boxes = sorted(
        person_boxes,
        key=lambda box: (
            -(cast("float", box.get("confidence")) if isinstance(box.get("confidence"), (int, float)) else 0.0),
            float(box.get("x") or 0.0),
            float(box.get("y") or 0.0),
        ),
    )
    tagged_people = _build_tagged_people(tagged_people_ids, tagged_people_names)
    deterministic_assignments: dict[int, dict[str, str | None]] = {}
    if allow_identity_assignment:
        resolved_owner_name = _resolve_owner_person_name(
            owner_person_id=owner_person_id,
            owner_person_name=owner_person_name,
            tagged_people_ids=tagged_people_ids,
            tagged_people_names=tagged_people_names,
        )
        owner_name_for_assignment = resolved_owner_name if (normalized_owner_id or explicit_owner_name) else None
        has_owner_context = bool(normalized_owner_id or owner_name_for_assignment)
        if len(person_boxes) == 1 and has_owner_context:
            deterministic_assignments[0] = {
                "person_id": normalized_owner_id,
                "person_name": owner_name_for_assignment,
            }
        elif len(person_boxes) > 1 and has_owner_context:
            deterministic_assignments[0] = {
                "person_id": normalized_owner_id,
                "person_name": owner_name_for_assignment,
            }
        elif len(tagged_people) == 1:
            deterministic_assignments[0] = tagged_people[0]
        elif len(tagged_people) == len(person_boxes):
            for idx, tagged in enumerate(tagged_people):
                deterministic_assignments[idx] = tagged

    out: list[dict[str, Any]] = []
    for idx, box in enumerate(person_boxes, start=1):
        assignment = deterministic_assignments.get(idx - 1)
        assigned_person_id = assignment.get("person_id") if assignment else None
        assigned_person_name = assignment.get("person_name") if assignment else None
        is_owner_assignment = bool(
            assignment
            and (
                (normalized_owner_id and _normalize_person_id(assigned_person_id) == normalized_owner_id)
                or (
                    normalized_owner_name
                    and _person_name_key(assigned_person_name) == _person_name_key(normalized_owner_name)
                )
            )
        )
        assignment_reason = (
            "owner_fallback_map"
            if is_owner_assignment and len(person_boxes) != len(tagged_people)
            else "deterministic_tag_map"
        )
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
                "label_source": assignment_reason if assignment else "generic",
                "fallback_reason": "no_faces_detected",
                "label": assigned_person_name or fallback_label,
                **({"person_id": assigned_person_id} if assigned_person_id else {}),
                **({"person_name": assigned_person_name} if assigned_person_name else {}),
                **({"match_status": "matched"} if assignment else {}),
                **({"match_reason": assignment_reason} if assignment else {}),
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


def _build_face_filter_diagnostics(result: Any) -> dict[str, Any] | None:
    face_count_raw = getattr(result, "face_count_raw", None)
    face_count_filtered = getattr(result, "face_count_filtered", None)
    raw_thresholds = getattr(result, "face_filter_thresholds", None)
    payload: dict[str, Any] = {}
    if isinstance(face_count_raw, int) and face_count_raw >= 0:
        payload["raw"] = face_count_raw
    if isinstance(face_count_filtered, int) and face_count_filtered >= 0:
        payload["filtered"] = face_count_filtered
    if isinstance(raw_thresholds, dict):
        min_side_px = raw_thresholds.get("min_side_px")
        min_area_ratio = raw_thresholds.get("min_area_ratio")
        thresholds: dict[str, Any] = {}
        if isinstance(min_side_px, int) and min_side_px > 0:
            thresholds["min_side_px"] = min_side_px
        if isinstance(min_area_ratio, (int, float)) and float(min_area_ratio) >= 0:
            thresholds["min_area_ratio"] = float(min_area_ratio)
        if thresholds:
            payload["thresholds"] = thresholds
    return payload or None


def _normalize_thumbnail_crop_payload(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    try:
        x = float(cast("Any", value.get("x")))
        y = float(cast("Any", value.get("y")))
        zoom = float(cast("Any", value.get("zoom")))
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


def _owner_face_crop_payload(
    face_boxes: list[dict[str, Any]],
    *,
    owner_person_id: str | None = None,
    owner_person_name: str | None = None,
) -> dict[str, Any] | None:
    if not face_boxes:
        return None
    owner_id = str(owner_person_id or "").strip()
    owner_name_key = _person_name_key(owner_person_name)

    candidates: list[dict[str, Any]] = []
    for box in face_boxes:
        box_person_id = str(box.get("person_id") or "").strip()
        box_person_name_key = _person_name_key(box.get("person_name"))
        if owner_id and box_person_id == owner_id:
            candidates.append(box)
            continue
        if owner_name_key and box_person_name_key == owner_name_key:
            candidates.append(box)

    if not candidates:
        return None

    min_similarity = _owner_face_match_similarity_min()
    min_fallback_confidence = _owner_fallback_crop_min_confidence()
    qualified_candidates: list[dict[str, Any]] = []
    for box in candidates:
        match_status = str(box.get("match_status") or "").strip().lower()
        match_similarity = box.get("match_similarity")
        if match_status != "matched":
            continue
        if isinstance(match_similarity, (int, float)) and float(match_similarity) >= min_similarity:
            qualified_candidates.append(box)
            continue
        label_source = str(box.get("label_source") or "").strip().lower()
        confidence = float(box.get("confidence") or 0.0)
        if label_source == "owner_fallback_map" and confidence >= min_fallback_confidence:
            qualified_candidates.append(box)
    if not qualified_candidates:
        return None

    best = max(
        qualified_candidates,
        key=lambda item: (
            float(item.get("match_similarity") or 0.0),
            float(item.get("confidence") or 0.0),
            float(item.get("width") or 0.0) * float(item.get("height") or 0.0),
        ),
    )
    # Defaults for static analysis; every runtime path below overwrites them.
    cx: float = 0.0
    cy: float = 0.0
    target_span: float = 1.0
    # Prefer square_crop_bbox from vision API (includes proper padding)
    scb = best.get("square_crop_bbox")
    if isinstance(scb, list) and len(scb) >= 4:
        try:
            scb_x1, scb_y1, scb_x2, scb_y2 = [float(v) for v in scb[:4]]
            scb_height = max(scb_y2 - scb_y1, 1e-4)
            cx = max(0.0, min(1.0, (scb_x1 + scb_x2) / 2.0))
            cy = max(0.0, min(1.0, scb_y1 + (scb_height * 0.45)))
            target_span = max(0.34, min(0.72, scb_height * 1.5))
        except (TypeError, ValueError):
            scb = None

    if not (isinstance(scb, list) and len(scb) >= 4):
        x = float(best.get("x") or 0.0)
        y = float(best.get("y") or 0.0)
        width = max(float(best.get("width") or 0.0), 1e-4)
        height = max(float(best.get("height") or 0.0), 1e-4)
        cx = max(0.0, min(1.0, x + (width / 2.0)))
        cy = max(0.0, min(1.0, y + (height * 0.62)))
        target_span = max(0.34, min(0.72, height * 2.8))

    zoom = max(1.05, min(1.9, 0.8 / target_span))
    return {
        "x": round(cx * 100.0, 1),
        "y": round(cy * 100.0, 1),
        "zoom": round(zoom, 2),
        "mode": "auto",
        "strategy": "owner_face_box_v1",
        "generated_at": datetime.now(UTC).isoformat(),
    }


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
    match_reason: str | None = None
    match_candidates: list[dict[str, Any]] | None = None
    filter_decision: str | None = None
    filter_metrics: dict[str, float] | None = None
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
    references_used: list[dict[str, Any]] | None = None


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
    db: SupabaseAdminClient = cast("SupabaseAdminClient", None),
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> AutoCountResponse:
    response = (
        db.schema("core")
        .table("cast_photos")
        .select(
            "id, person_id, hosted_url, url, image_url, thumb_url, source, source_page_url, "
            "people_names, title_names, caption, metadata"
        )
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
    allow_identity_assignment = bool(force) or _is_trr_show_eligible(db, metadata=metadata)
    owner_person_id = _normalize_person_id(row.get("person_id"))
    owner_person_name = _resolve_owner_person_name(
        owner_person_id=owner_person_id,
        owner_person_name=None,
        tagged_people_ids=existing_people_ids,
        tagged_people_names=existing_people_names,
    )
    person_name_id_cache: dict[str, str | None] = {}
    person_id_name_cache: dict[str, str | None] = {}
    assignment_people_ids, assignment_people_names = _build_assignment_tagged_people(
        db=db,
        owner_person_id=owner_person_id,
        tagged_people_ids=existing_people_ids,
        tagged_people_names=existing_people_names,
        row_people_names=row.get("people_names"),
        metadata_signals=[
            existing_people_names,
            row.get("people_names"),
            row.get("title_names"),
            row.get("caption"),
            row.get("source_page_url"),
            metadata,
        ],
        person_name_id_cache=person_name_id_cache,
        person_id_name_cache=person_id_name_cache,
    )
    candidate_person_ids = _build_identity_candidate_person_ids(
        db=db,
        allow_identity_assignment=allow_identity_assignment,
        owner_person_id=owner_person_id,
        tagged_people_ids=assignment_people_ids,
        tagged_people_names=assignment_people_names,
        metadata_signals=[
            assignment_people_names,
            row.get("title_names"),
            row.get("caption"),
            row.get("source_page_url"),
            metadata,
        ],
    )
    request_show_id_raw = metadata.get("show_id")
    request_show_id = None
    if isinstance(request_show_id_raw, UUID):
        request_show_id = request_show_id_raw
    elif isinstance(request_show_id_raw, str) and request_show_id_raw.strip():
        try:
            request_show_id = UUID(request_show_id_raw.strip())
        except ValueError:
            request_show_id = None
    reference_cache: dict[str, list[dict[str, Any]]] = {}
    person_reference_images = _resolve_runtime_person_reference_pools(
        db,
        candidate_person_ids=candidate_person_ids,
        request_show_id=request_show_id,
        request_show_name=str(metadata.get("show_name") or "").strip() or None,
        reference_cache=reference_cache,
        person_id_name_cache=person_id_name_cache,
    )
    owner_reference_profile: dict[str, Any] | None = None
    owner_reference_images: list[dict[str, Any]] = []
    if owner_person_id:
        try:
            owner_reference_profile = cast(
                "dict[str, Any]",
                build_owner_tagging_reference_profile(
                    db,
                    owner_person_id,
                    show_id=metadata.get("show_id"),
                    show_name=str(metadata.get("show_name") or "").strip() or None,
                ),
            )
            raw_used = owner_reference_profile.get("used")
            if isinstance(raw_used, list):
                owner_reference_images = [entry for entry in raw_used if isinstance(entry, dict)]
            if owner_reference_images:
                owner_reference_images = cast(
                    "list[dict[str, Any]]",
                    sync_owner_tagging_reference_usage(
                        db,
                        owner_person_id,
                        used_references=owner_reference_images,
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            owner_reference_profile = None
            owner_reference_images = []
            logger.warning(
                "Failed to build owner tagging references for cast photo %s: %s",
                photo_id,
                exc,
            )
    references_used: list[dict[str, Any]] = []
    result = None
    selected_image_url: str | None = None
    last_error: PeopleCountServiceError | None = None
    for image_url in image_urls:
        try:
            if candidate_person_ids:
                result = count_people(
                    image_url,
                    candidate_person_ids=candidate_person_ids,
                    owner_person_id=owner_person_id,
                    owner_reference_images=owner_reference_images or None,
                    person_reference_images=person_reference_images or None,
                )
            else:
                result = count_people(
                    image_url,
                    owner_person_id=owner_person_id,
                    owner_reference_images=owner_reference_images or None,
                    person_reference_images=person_reference_images or None,
                )
            selected_image_url = image_url
            break
        except TypeError:
            try:
                if candidate_person_ids:
                    result = count_people(image_url, candidate_person_ids=candidate_person_ids)
                else:
                    result = count_people(image_url)
                selected_image_url = image_url
                break
            except TypeError:
                try:
                    result = count_people(image_url)
                    selected_image_url = image_url
                    break
                except PeopleCountServiceError as exc:
                    last_error = exc
                    continue
            except PeopleCountServiceError as exc:
                last_error = exc
                continue
        except PeopleCountServiceError as exc:
            last_error = exc
    if result is None:
        raise HTTPException(status_code=502, detail=str(last_error or "Failed to auto-count cast photo"))
    face_boxes = _build_detection_boxes(
        result,
        tagged_people_ids=assignment_people_ids,
        tagged_people_names=assignment_people_names,
        owner_person_id=owner_person_id,
        owner_person_name=owner_person_name,
        allow_identity_assignment=allow_identity_assignment,
    )
    auto_people_ids, auto_people_names = _auto_people_from_face_boxes(face_boxes)
    face_filter_diagnostics = _build_face_filter_diagnostics(result)
    reference_profile_raw = getattr(result, "reference_profile", None)
    reference_profile_result = reference_profile_raw if isinstance(reference_profile_raw, dict) else None
    reference_profile_used = reference_profile_result.get("used") if reference_profile_result else None
    if owner_reference_images:
        references_used = owner_reference_images
    elif isinstance(reference_profile_used, list):
        references_used = [entry for entry in reference_profile_used if isinstance(entry, dict)]
    if owner_person_id:
        try:
            references_used = cast(
                "list[dict[str, Any]]",
                sync_owner_tagging_reference_usage(
                    db,
                    owner_person_id,
                    used_references=references_used,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to sync owner tagging references for cast photo %s: %s", photo_id, exc)
    if force:
        resolved_people_names = auto_people_names or assignment_people_names or existing_people_names or None
        resolved_people_ids = auto_people_ids or assignment_people_ids or existing_people_ids or None
    else:
        resolved_people_names = existing_people_names if existing_people_names else (auto_people_names or None)
        resolved_people_ids = existing_people_ids if existing_people_ids else (auto_people_ids or None)
    upsert_cast_photo_tags(
        db,
        cast_photo_id=str(photo_id),
        people_names=resolved_people_names,
        people_ids=resolved_people_ids,
        people_count=result.people_count,
        people_count_source="auto",
        detector=result.detector,
        updated_by_firebase_uid="system:auto",
    )
    metadata = dict(row.get("metadata") or {})
    metadata_changed = False
    latest_crop_payload: dict[str, Any] | None = None
    face_crops: list[dict[str, Any]] = []

    if selected_image_url and face_boxes:
        face_crops = generate_and_upload_face_crops(
            entity_kind="cast_photo",
            entity_id=str(photo_id),
            image_url=selected_image_url,
            face_boxes=cast("list[Mapping[str, Any]]", face_boxes),
            size=256,
        )
    if metadata.get("face_crops") != face_crops:
        metadata["face_crops"] = face_crops
        metadata_changed = True

    if metadata.get("face_boxes") != face_boxes:
        metadata["face_boxes"] = face_boxes
        metadata_changed = True
    if face_filter_diagnostics is not None:
        if metadata.get("face_detection_diagnostics") != face_filter_diagnostics:
            metadata["face_detection_diagnostics"] = face_filter_diagnostics
            metadata_changed = True

    owner_crop_payload = _owner_face_crop_payload(
        face_boxes,
        owner_person_id=owner_person_id,
        owner_person_name=owner_person_name,
    )
    if owner_crop_payload is not None:
        existing_crop = metadata.get("thumbnail_crop")
        if not (isinstance(existing_crop, dict) and existing_crop.get("mode") == "manual"):
            metadata["thumbnail_crop"] = owner_crop_payload
            latest_crop_payload = owner_crop_payload
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
        face_boxes=cast("list[FaceBox]", face_boxes),
        face_crops=cast("list[FaceCrop]", face_crops),
        thumbnail_crop=resolved_crop_payload,
        references_used=references_used or None,
    )


@router.post("/media-assets/{asset_id}/auto-count", response_model=AutoCountResponse)
def auto_count_media_asset(
    asset_id: UUID,
    force: bool = Query(default=False),
    db: SupabaseAdminClient = cast("SupabaseAdminClient", None),
    _: InternalAdminUser = cast("InternalAdminUser", None),
) -> AutoCountResponse:
    response = (
        db.schema("core")
        .table("media_assets")
        .select("id, hosted_url, source_url, caption, metadata")
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
    owner_person_ids = {
        normalized
        for normalized in (
            _normalize_person_id(link.get("entity_id"))
            if str(link.get("entity_type") or "").strip() == "person"
            and str(link.get("kind") or "").strip() == "gallery"
            else None
            for link in links
        )
        if normalized
    }
    owner_person_id = next(iter(owner_person_ids)) if len(owner_person_ids) == 1 else None
    owner_person_name = _resolve_owner_person_name(
        owner_person_id=owner_person_id,
        owner_person_name=None,
        tagged_people_ids=tagged_people_ids,
        tagged_people_names=tagged_people_names,
    )
    show_exists_cache: dict[str, bool] = {}
    show_name_cache: dict[str, str | None] = {}
    allow_identity_assignment = bool(force) or any(
        _is_trr_show_eligible(
            db,
            metadata=(link.get("context") if isinstance(link.get("context"), dict) else {}),
            show_exists_cache=show_exists_cache,
            show_name_cache=show_name_cache,
        )
        for link in links
    )
    primary_context_for_refs = next(
        (link.get("context") for link in links if isinstance(link.get("context"), dict)),
        None,
    )
    context_for_refs = primary_context_for_refs if isinstance(primary_context_for_refs, dict) else {}
    person_name_id_cache: dict[str, str | None] = {}
    person_id_name_cache: dict[str, str | None] = {}
    assignment_people_ids, assignment_people_names = _build_assignment_tagged_people(
        db=db,
        owner_person_id=owner_person_id,
        owner_person_name=owner_person_name,
        tagged_people_ids=tagged_people_ids,
        tagged_people_names=tagged_people_names,
        metadata_signals=[
            tagged_people_names,
            context_for_refs.get("titles"),
            context_for_refs.get("caption"),
            context_for_refs.get("name"),
            context_for_refs.get("title"),
            context_for_refs.get("episode"),
            context_for_refs.get("original_source_page"),
            context_for_refs,
            row.get("caption"),
            row.get("metadata"),
        ],
        person_name_id_cache=person_name_id_cache,
        person_id_name_cache=person_id_name_cache,
    )
    candidate_person_ids = _build_identity_candidate_person_ids(
        db=db,
        allow_identity_assignment=allow_identity_assignment,
        owner_person_id=owner_person_id,
        tagged_people_ids=assignment_people_ids,
        tagged_people_names=assignment_people_names,
        metadata_signals=[
            assignment_people_names,
            context_for_refs.get("titles"),
            context_for_refs.get("caption"),
            context_for_refs.get("name"),
            context_for_refs.get("title"),
            context_for_refs.get("episode"),
            context_for_refs.get("original_source_page"),
            context_for_refs,
            row.get("caption"),
            row.get("metadata"),
        ],
    )
    request_show_id_raw = context_for_refs.get("show_id")
    request_show_id = None
    if isinstance(request_show_id_raw, UUID):
        request_show_id = request_show_id_raw
    elif isinstance(request_show_id_raw, str) and request_show_id_raw.strip():
        try:
            request_show_id = UUID(request_show_id_raw.strip())
        except ValueError:
            request_show_id = None
    reference_cache: dict[str, list[dict[str, Any]]] = {}
    person_reference_images = _resolve_runtime_person_reference_pools(
        db,
        candidate_person_ids=candidate_person_ids,
        request_show_id=request_show_id,
        request_show_name=str(context_for_refs.get("show_name") or "").strip() or None,
        reference_cache=reference_cache,
        person_id_name_cache=person_id_name_cache,
    )
    owner_reference_profile: dict[str, Any] | None = None
    owner_reference_images: list[dict[str, Any]] = []
    if owner_person_id:
        try:
            primary_context = next(
                (
                    link.get("context")
                    for link in links
                    if isinstance(link.get("context"), dict)
                    and str(link.get("entity_id") or "").strip() == owner_person_id
                ),
                None,
            )
            context_obj = primary_context if isinstance(primary_context, dict) else {}
            owner_reference_profile = cast(
                "dict[str, Any]",
                build_owner_tagging_reference_profile(
                    db,
                    owner_person_id,
                    show_id=context_obj.get("show_id"),
                    show_name=str(context_obj.get("show_name") or "").strip() or None,
                ),
            )
            raw_used = owner_reference_profile.get("used")
            if isinstance(raw_used, list):
                owner_reference_images = [entry for entry in raw_used if isinstance(entry, dict)]
            if owner_reference_images:
                owner_reference_images = cast(
                    "list[dict[str, Any]]",
                    sync_owner_tagging_reference_usage(
                        db,
                        owner_person_id,
                        used_references=owner_reference_images,
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            owner_reference_profile = None
            owner_reference_images = []
            logger.warning(
                "Failed to build owner tagging references for media asset %s: %s",
                asset_id,
                exc,
            )
    references_used: list[dict[str, Any]] = []
    result = None
    selected_image_url: str | None = None
    last_error: PeopleCountServiceError | None = None
    for image_url in image_urls:
        try:
            if candidate_person_ids:
                result = count_people(
                    image_url,
                    candidate_person_ids=candidate_person_ids,
                    owner_person_id=owner_person_id,
                    owner_reference_images=owner_reference_images or None,
                    person_reference_images=person_reference_images or None,
                )
            else:
                result = count_people(
                    image_url,
                    owner_person_id=owner_person_id,
                    owner_reference_images=owner_reference_images or None,
                    person_reference_images=person_reference_images or None,
                )
            selected_image_url = image_url
            break
        except TypeError:
            try:
                if candidate_person_ids:
                    result = count_people(image_url, candidate_person_ids=candidate_person_ids)
                else:
                    result = count_people(image_url)
                selected_image_url = image_url
                break
            except TypeError:
                try:
                    result = count_people(image_url)
                    selected_image_url = image_url
                    break
                except PeopleCountServiceError as exc:
                    last_error = exc
                    continue
            except PeopleCountServiceError as exc:
                last_error = exc
                continue
        except PeopleCountServiceError as exc:
            last_error = exc
    if result is None:
        raise HTTPException(status_code=502, detail=str(last_error or "Failed to auto-count media asset"))
    face_boxes = _build_detection_boxes(
        result,
        tagged_people_ids=assignment_people_ids,
        tagged_people_names=assignment_people_names,
        owner_person_id=owner_person_id,
        owner_person_name=owner_person_name,
        allow_identity_assignment=allow_identity_assignment,
    )
    auto_people_ids, auto_people_names = _auto_people_from_face_boxes(face_boxes)
    face_filter_diagnostics = _build_face_filter_diagnostics(result)
    reference_profile_raw = getattr(result, "reference_profile", None)
    reference_profile_result = reference_profile_raw if isinstance(reference_profile_raw, dict) else None
    reference_profile_used = reference_profile_result.get("used") if reference_profile_result else None
    if owner_reference_images:
        references_used = owner_reference_images
    elif isinstance(reference_profile_used, list):
        references_used = [entry for entry in reference_profile_used if isinstance(entry, dict)]
    if owner_person_id:
        try:
            references_used = cast(
                "list[dict[str, Any]]",
                sync_owner_tagging_reference_usage(
                    db,
                    owner_person_id,
                    used_references=references_used,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to sync owner tagging references for media asset %s: %s", asset_id, exc)
    face_crops: list[dict[str, Any]] = []
    if selected_image_url and face_boxes:
        face_crops = generate_and_upload_face_crops(
            entity_kind="media_asset",
            entity_id=str(asset_id),
            image_url=selected_image_url,
            face_boxes=cast("list[Mapping[str, Any]]", face_boxes),
            size=256,
        )
    context_auto_update = {
        "people_count": result.people_count,
        "people_count_source": "auto",
        "people_count_detector": result.detector,
        "face_boxes": face_boxes,
        "face_crops": face_crops,
        **({"face_detection_diagnostics": face_filter_diagnostics} if face_filter_diagnostics else {}),
    }
    if force:
        if auto_people_ids:
            context_auto_update["people_ids"] = auto_people_ids
        elif assignment_people_ids:
            context_auto_update["people_ids"] = assignment_people_ids
        if auto_people_names:
            context_auto_update["people_names"] = auto_people_names
        elif assignment_people_names:
            context_auto_update["people_names"] = assignment_people_names
    else:
        if auto_people_ids:
            context_auto_update["people_ids"] = auto_people_ids
        if auto_people_names:
            context_auto_update["people_names"] = auto_people_names
    update_person_links_context(
        db,
        links,
        context_auto_update,
    )
    owner_crop_payload = _owner_face_crop_payload(
        face_boxes,
        owner_person_id=owner_person_id,
        owner_person_name=owner_person_name,
    )
    latest_crop_payload: dict[str, Any] | None = None
    if owner_crop_payload is not None:
        now = datetime.now(UTC).isoformat()
        for link in links:
            context = {**dict(link.get("context") or {}), **context_auto_update}
            existing_crop = context.get("thumbnail_crop")
            if isinstance(existing_crop, dict) and existing_crop.get("mode") == "manual":
                continue
            context["thumbnail_crop"] = owner_crop_payload
            latest_crop_payload = owner_crop_payload
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
            context = cast("dict[str, Any]", link.get("context")) if isinstance(link.get("context"), dict) else {}
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
        face_boxes=cast("list[FaceBox]", face_boxes),
        face_crops=cast("list[FaceCrop]", face_crops),
        thumbnail_crop=resolved_crop_payload,
        references_used=references_used or None,
    )


@router.post("/shows/{show_id}/auto-count-images", response_model=AutoCountShowImagesResponse)
def auto_count_show_images(
    show_id: UUID,
    payload: AutoCountShowImagesRequest,
    db: SupabaseAdminClient = cast("SupabaseAdminClient", None),
    _: InternalAdminUser = cast("InternalAdminUser", None),
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
        .select("id, hosted_url, source_url, caption, metadata")
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
                if isinstance(link.get("context"), dict) and isinstance(link.get("context", {}).get("people_ids"), list)
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
        allow_identity_assignment = bool(payload.force) or any(
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
            metadata_signals=[
                tagged_people_names,
                *[link.get("context") for link in links_for_asset if isinstance(link.get("context"), dict)],
                asset.get("caption"),
                asset.get("metadata"),
            ],
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
                except PeopleCountServiceError:
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
        face_filter_diagnostics = _build_face_filter_diagnostics(result)
        face_crops: list[dict[str, Any]] = []
        if selected_image_url and face_boxes:
            face_crops = generate_and_upload_face_crops(
                entity_kind="media_asset",
                entity_id=str(asset_id),
                image_url=selected_image_url,
                face_boxes=cast("list[Mapping[str, Any]]", face_boxes),
                size=256,
            )
        context_auto_update = {
            "people_count": result.people_count,
            "people_count_source": "auto",
            "people_count_detector": result.detector,
            "face_boxes": face_boxes,
            "face_crops": face_crops,
            **({"face_detection_diagnostics": face_filter_diagnostics} if face_filter_diagnostics else {}),
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
