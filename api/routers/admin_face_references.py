"""Admin face-reference governance routes."""

from __future__ import annotations

from typing import Any, Literal, cast
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from api.auth import InternalAdminUser
from trr_backend.repositories import face_references
from trr_backend.services import face_reference_embeddings

router = APIRouter(prefix="/admin", tags=["admin-face-references"])


class FaceReferenceImageItem(BaseModel):
    id: str
    person_id: str
    person_name: str | None = None
    media_link_id: str
    media_asset_id: str
    legacy_screenalytics_face_bank_image_id: str | None = None
    is_active: bool
    approved: bool
    review_status: str
    review_notes: dict[str, Any] = Field(default_factory=dict)
    reviewed_at: str | None = None
    reviewed_by: str | None = None
    duplicate_of_reference_image_id: str | None = None
    embedding_status: str
    source_url: str | None = None
    hosted_url: str | None = None
    hosted_sha256: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    last_enqueued_at: str | None = None
    deactivated_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class FaceReferenceListResponse(BaseModel):
    person_id: str
    items: list[FaceReferenceImageItem]


class FaceReferenceReviewQueueResponse(BaseModel):
    items: list[FaceReferenceImageItem]


class ReviewReferenceRequest(BaseModel):
    review_status: Literal["pending_review", "approved", "rejected", "duplicate"]
    review_notes: dict[str, Any] = Field(default_factory=dict)
    duplicate_of_reference_image_id: UUID | None = None


class SearchReferencesRequest(BaseModel):
    reference_image_id: UUID | None = None
    image_source: str | None = None
    person_id: UUID | None = None
    limit: int = Field(default=5, ge=1, le=25)


class VerifyReferencesRequest(BaseModel):
    left_reference_image_id: UUID | None = None
    right_reference_image_id: UUID | None = None
    left_image_source: str | None = None
    right_image_source: str | None = None


class ReembedReferenceRequest(BaseModel):
    image_source: str | None = None
    selected_face_index: int | None = Field(default=None, ge=0)


def _row_to_item(row: dict[str, Any]) -> FaceReferenceImageItem:
    payload = dict(row)
    for key in ("review_notes", "metadata"):
        if not isinstance(payload.get(key), dict):
            payload[key] = {}
    for key in ("reviewed_at", "last_enqueued_at", "deactivated_at", "created_at", "updated_at"):
        if payload.get(key) is not None:
            payload[key] = str(payload[key])
    return FaceReferenceImageItem.model_validate(payload)


def _image_source_for_reference(reference_image_id: str) -> tuple[dict[str, Any], str]:
    row = face_references.resolve_face_reference_image(reference_image_id=reference_image_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Face reference image not found")
    image_source = str(row.get("hosted_url") or row.get("source_url") or "").strip()
    if not image_source:
        raise HTTPException(status_code=409, detail="Face reference image does not have a usable source")
    return row, image_source


@router.get("/face-references/people/{person_id}", response_model=FaceReferenceListResponse)
def list_face_references(
    person_id: UUID, _: InternalAdminUser = cast(InternalAdminUser, None)
) -> FaceReferenceListResponse:
    rows = face_references.list_face_reference_images(person_id=str(person_id), include_inactive=True)
    return FaceReferenceListResponse(person_id=str(person_id), items=[_row_to_item(row) for row in rows])


@router.get("/face-references/review-queue", response_model=FaceReferenceReviewQueueResponse)
def list_face_reference_review_queue(
    limit: int = 50,
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> FaceReferenceReviewQueueResponse:
    rows = face_references.list_face_reference_builder_review_queue(limit=limit)
    return FaceReferenceReviewQueueResponse(items=[_row_to_item(row) for row in rows])


@router.post("/face-references/{reference_image_id}/review", response_model=FaceReferenceImageItem)
def review_face_reference(
    reference_image_id: UUID,
    payload: ReviewReferenceRequest,
    admin_user: InternalAdminUser = cast(InternalAdminUser, None),
) -> FaceReferenceImageItem:
    updated = face_references.set_face_reference_review_status(
        reference_image_id=str(reference_image_id),
        review_status=payload.review_status,
        reviewed_by=str((admin_user or {}).get("email") or (admin_user or {}).get("id") or "admin"),
        review_notes=payload.review_notes,
        duplicate_of_reference_image_id=(
            str(payload.duplicate_of_reference_image_id) if payload.duplicate_of_reference_image_id else None
        ),
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="Face reference image not found")
    return _row_to_item(updated)


@router.post("/face-references/search")
def search_face_references(
    payload: SearchReferencesRequest, _: InternalAdminUser = cast(InternalAdminUser, None)
) -> dict[str, Any]:
    image_source = payload.image_source
    if payload.reference_image_id:
        reference_row, derived_image_source = _image_source_for_reference(str(payload.reference_image_id))
        ready_embedding = face_references.get_ready_face_reference_embedding(
            reference_image_id=str(payload.reference_image_id),
            contract_key=face_reference_embeddings.FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
        )
        if ready_embedding and ready_embedding.get("embedding") is not None:
            response = face_reference_embeddings.search_reference_matches(
                embedding=ready_embedding["embedding"],
                limit=payload.limit,
                person_id=str(payload.person_id) if payload.person_id else None,
            )
            response["query_reference_image_id"] = str(payload.reference_image_id)
            response["query_metadata"] = reference_row.get("metadata")
            return response
        image_source = image_source or derived_image_source
    if not image_source:
        raise HTTPException(status_code=400, detail="reference_image_id or image_source is required")
    return face_reference_embeddings.search_reference_matches(
        image_source=image_source,
        limit=payload.limit,
        person_id=str(payload.person_id) if payload.person_id else None,
    )


@router.post("/face-references/verify")
def verify_face_reference_pair(
    payload: VerifyReferencesRequest, _: InternalAdminUser = cast(InternalAdminUser, None)
) -> dict[str, Any]:
    left_image = payload.left_image_source
    right_image = payload.right_image_source
    if payload.left_reference_image_id:
        _, left_image = _image_source_for_reference(str(payload.left_reference_image_id))
    if payload.right_reference_image_id:
        _, right_image = _image_source_for_reference(str(payload.right_reference_image_id))
    if not left_image or not right_image:
        raise HTTPException(status_code=400, detail="Both left and right image sources are required")
    return face_reference_embeddings.verify_reference_pair(left_image=left_image, right_image=right_image)


@router.post("/face-references/{reference_image_id}/reembed")
def reembed_face_reference(
    reference_image_id: UUID,
    payload: ReembedReferenceRequest,
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    reference_row, derived_image_source = _image_source_for_reference(str(reference_image_id))
    image_source = payload.image_source or derived_image_source
    return face_reference_embeddings.register_reference_image(
        reference_image_id=str(reference_image_id),
        image_source=image_source,
        assigned_person_id=str((reference_row or {}).get("person_id") or "") or None,
        selected_face_index=payload.selected_face_index,
    )
