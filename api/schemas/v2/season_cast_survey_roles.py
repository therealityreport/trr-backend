"""Strict API v2 contracts for season cast survey roles."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

SeasonSurveyCastRole = Literal["main", "friend_of"]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class SeasonCastSurveyRoleV2(_StrictModel):
    id: UUID
    trr_show_id: UUID
    season_number: int = Field(ge=1)
    person_id: UUID
    role: SeasonSurveyCastRole
    created_at: datetime
    updated_at: datetime


class SeasonCastSurveyRoleListResponseV2(_StrictModel):
    roles: list[SeasonCastSurveyRoleV2]


class UpsertSeasonCastSurveyRoleRequestV2(_StrictModel):
    person_id: UUID
    role: SeasonSurveyCastRole


class UpsertSeasonCastSurveyRoleResponseV2(_StrictModel):
    role: SeasonCastSurveyRoleV2


class ReplaceSeasonCastSurveyRoleEntryV2(_StrictModel):
    person_id: UUID
    role: SeasonSurveyCastRole


class ReplaceSeasonCastSurveyRolesRequestV2(_StrictModel):
    roles: list[ReplaceSeasonCastSurveyRoleEntryV2] = Field(max_length=500)

    @field_validator("roles")
    @classmethod
    def ensure_unique_people(
        cls, roles: list[ReplaceSeasonCastSurveyRoleEntryV2]
    ) -> list[ReplaceSeasonCastSurveyRoleEntryV2]:
        person_ids = [str(role.person_id).lower() for role in roles]
        if len(person_ids) != len(set(person_ids)):
            raise ValueError("roles must not contain duplicate person_id entries")
        return roles


class DeleteSeasonCastSurveyRoleRequestV2(_StrictModel):
    person_id: UUID


class DeleteSeasonCastSurveyRoleResponseV2(_StrictModel):
    success: Literal[True]
    removed: bool


class SeasonCastSurveyRoleProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class SeasonCastSurveyRoleProblemResponseV2(_StrictModel):
    detail: SeasonCastSurveyRoleProblemDetailV2
