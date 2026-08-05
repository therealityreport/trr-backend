"""Strict API v2 contracts for admin external-ID reads."""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

MAX_EXTERNAL_ID_BATCH_SIZE = 200
MAX_PERSON_EXTERNAL_IDS = 12

PersonExternalIdSourceV2 = Literal[
    "imdb",
    "tmdb",
    "wikidata",
    "tvdb",
    "tvrage",
    "fandom",
    "facebook",
    "instagram",
    "threads",
    "twitter",
    "tiktok",
    "youtube",
]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


def _dedupe_ids(values: list[UUID]) -> list[UUID]:
    seen: set[UUID] = set()
    unique: list[UUID] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return unique


class PersonExternalIdV2(_StrictModel):
    id: int | None = Field(ge=1)
    source_id: PersonExternalIdSourceV2
    external_id: str = Field(min_length=1, max_length=2048)
    is_primary: bool
    valid_from: str | None
    valid_to: str | None
    observed_at: str | None
    created_at: str | None
    updated_at: str | None


class PersonExternalIdsResponseV2(_StrictModel):
    person_id: UUID
    external_ids: list[PersonExternalIdV2] = Field(max_length=MAX_PERSON_EXTERNAL_IDS)


class PersonExternalIdsBatchRequestV2(_StrictModel):
    person_ids: list[UUID] = Field(min_length=1, max_length=MAX_EXTERNAL_ID_BATCH_SIZE)
    include_inactive: bool = False

    @field_validator("person_ids")
    @classmethod
    def dedupe_person_ids(cls, value: list[UUID]) -> list[UUID]:
        return _dedupe_ids(value)


class PersonExternalIdsBatchResponseV2(_StrictModel):
    people: list[PersonExternalIdsResponseV2] = Field(max_length=MAX_EXTERNAL_ID_BATCH_SIZE)


class ShowExternalIdsV2(_StrictModel):
    show_id: UUID
    external_ids: dict[str, JsonValue] | None


class ShowExternalIdsBatchRequestV2(_StrictModel):
    show_ids: list[UUID] = Field(min_length=1, max_length=MAX_EXTERNAL_ID_BATCH_SIZE)

    @field_validator("show_ids")
    @classmethod
    def dedupe_show_ids(cls, value: list[UUID]) -> list[UUID]:
        return _dedupe_ids(value)


class ShowExternalIdsBatchResponseV2(_StrictModel):
    shows: list[ShowExternalIdsV2] = Field(max_length=MAX_EXTERNAL_ID_BATCH_SIZE)


class ExternalIdsProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class ExternalIdsProblemResponseV2(_StrictModel):
    detail: ExternalIdsProblemDetailV2
