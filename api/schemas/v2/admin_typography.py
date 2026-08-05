"""Strict API v2 contracts for authenticated typography administration."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, str_strip_whitespace=True)


TypographyAreaV2 = Literal["user-frontend", "surveys", "admin"]


class TypographyRoleStyleV2(_StrictModel):
    font_family: str = Field(min_length=1, validation_alias="fontFamily", serialization_alias="fontFamily")
    font_size: str = Field(min_length=1, validation_alias="fontSize", serialization_alias="fontSize")
    font_weight: str = Field(min_length=1, validation_alias="fontWeight", serialization_alias="fontWeight")
    line_height: str = Field(min_length=1, validation_alias="lineHeight", serialization_alias="lineHeight")
    letter_spacing: str = Field(min_length=1, validation_alias="letterSpacing", serialization_alias="letterSpacing")
    text_transform: str | None = Field(
        default=None,
        min_length=1,
        validation_alias="textTransform",
        serialization_alias="textTransform",
    )


class TypographyRoleConfigV2(_StrictModel):
    mobile: TypographyRoleStyleV2
    desktop: TypographyRoleStyleV2


class TypographySetV2(_StrictModel):
    id: str = Field(min_length=1)
    slug: str = Field(min_length=1)
    name: str = Field(min_length=1)
    area: TypographyAreaV2
    seed_source: str = Field(min_length=1)
    roles: dict[str, TypographyRoleConfigV2] = Field(min_length=1)
    created_at: datetime | Literal[""]
    updated_at: datetime | Literal[""]


class TypographyAssignmentV2(_StrictModel):
    id: str = Field(min_length=1)
    area: TypographyAreaV2
    page_key: str | None = None
    instance_key: str | None = None
    set_id: str = Field(min_length=1)
    source_path: str = Field(min_length=1)
    notes: str | None = None
    created_at: datetime | Literal[""]
    updated_at: datetime | Literal[""]


class AdminTypographyStateResponseV2(_StrictModel):
    sets: list[TypographySetV2]
    assignments: list[TypographyAssignmentV2]


class CreateTypographySetRequestV2(_StrictModel):
    slug: str | None = None
    name: str = Field(min_length=1)
    area: TypographyAreaV2
    seed_source: str = Field(min_length=1)
    roles: dict[str, TypographyRoleConfigV2] = Field(min_length=1)


class UpdateTypographySetRequestV2(_StrictModel):
    name: str | None = Field(default=None, min_length=1)
    area: TypographyAreaV2 | None = None
    seed_source: str | None = Field(default=None, min_length=1)
    roles: dict[str, TypographyRoleConfigV2] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def reject_explicit_nulls(self) -> UpdateTypographySetRequestV2:
        if any(getattr(self, field) is None for field in self.model_fields_set):
            raise ValueError("Typography set update fields cannot be null.")
        return self


class UpsertTypographyAssignmentRequestV2(_StrictModel):
    area: TypographyAreaV2
    page_key: str | None = None
    instance_key: str | None = None
    set_id: str = Field(min_length=1)
    source_path: str = Field(min_length=1)
    notes: str | None = None


class TypographySetResponseV2(_StrictModel):
    set: TypographySetV2


class TypographyAssignmentResponseV2(_StrictModel):
    assignment: TypographyAssignmentV2


class TypographyDeleteResponseV2(_StrictModel):
    ok: Literal[True]


class AdminTypographyProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class AdminTypographyProblemResponseV2(_StrictModel):
    detail: AdminTypographyProblemDetailV2
