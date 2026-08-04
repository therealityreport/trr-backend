"""Strict API v2 contracts for admin core people reads."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class AdminPersonSummaryV2(_StrictModel):
    id: UUID
    full_name: str = Field(min_length=1)
    known_for: str | None = None
    external_ids: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class AdminPersonV2(AdminPersonSummaryV2):
    birthday: dict[str, Any] | None = None
    gender: dict[str, Any] | None = None
    biography: dict[str, Any] | None = None
    place_of_birth: dict[str, Any] | None = None
    homepage: dict[str, Any] | None = None
    profile_image_url: dict[str, Any] | None = None
    alternative_names: dict[str, list[str]] | None = None


class AdminPeopleListResponseV2(_StrictModel):
    people: list[AdminPersonSummaryV2]
    limit: int = Field(ge=1, le=500)
    offset: int = Field(ge=0)
    count: int = Field(ge=0)
    total_count: int | None = Field(default=None, ge=0)
    has_more: bool


class AdminPersonResponseV2(_StrictModel):
    person: AdminPersonV2


RelationshipLabel = Literal["Mom", "Dad", "Parent", "Sister", "Brother", "Sibling"]


class AdminPersonRelationshipsResponseV2(_StrictModel):
    person_id: UUID
    show_id: UUID | None = None
    relationships: dict[str, RelationshipLabel]


class AdminPeopleReadProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class AdminPeopleReadProblemResponseV2(_StrictModel):
    detail: AdminPeopleReadProblemDetailV2
