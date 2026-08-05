"""Typed public identity resolver contracts for API v2."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class IdentityMatchKind(str, Enum):
    canonical = "canonical"
    alias = "alias"


class _StrictResponseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ShowIdentityResponse(_StrictResponseModel):
    resource_type: Literal["show"]
    show_id: UUID
    show_name: str
    requested_slug: str
    canonical_slug: str
    match_kind: IdentityMatchKind
    canonical_path: str


class SeasonIdentityResponse(_StrictResponseModel):
    resource_type: Literal["season"]
    season_id: UUID
    show_id: UUID
    show_name: str
    season_number: int = Field(ge=0)
    season_title: str | None = None
    requested_show_slug: str
    canonical_show_slug: str
    show_match_kind: IdentityMatchKind
    canonical_path: str


class PersonShowContext(_StrictResponseModel):
    show_id: UUID
    show_name: str
    canonical_slug: str


class PersonIdentityResponse(_StrictResponseModel):
    resource_type: Literal["person"]
    person_id: UUID
    full_name: str
    requested_slug: str
    canonical_slug: str
    match_kind: IdentityMatchKind
    canonical_path: str
    show_context: PersonShowContext | None = None


class IdentityProblemDetail(_StrictResponseModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class IdentityProblemResponse(_StrictResponseModel):
    detail: IdentityProblemDetail
