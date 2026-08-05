"""Strict API v2 contracts for recent-people admin endpoints."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, populate_by_name=True)


class RecentPersonV2(_StrictModel):
    person_id: UUID
    full_name: str | None
    known_for: str | None
    photo_url: str | None
    show_context: str | None
    view_count: int = Field(ge=0)
    first_viewed_at: str
    last_viewed_at: str


class RecentPeoplePaginationV2(_StrictModel):
    limit: int = Field(ge=1, le=50)
    count: int = Field(ge=0)


class RecentPeopleListResponseV2(_StrictModel):
    people: list[RecentPersonV2]
    pagination: RecentPeoplePaginationV2


class RecordRecentPersonRequestV2(_StrictModel):
    person_id: UUID = Field(alias="personId")
    show_id: str | None = Field(default=None, alias="showId")


class RecordRecentPersonResponseV2(_StrictModel):
    ok: bool


class RecentPeopleProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class RecentPeopleProblemResponseV2(_StrictModel):
    detail: RecentPeopleProblemDetailV2
