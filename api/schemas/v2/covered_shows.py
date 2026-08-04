"""Strict API v2 contracts for the covered-shows admin surface."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class CoveredShowV2(_StrictModel):
    id: UUID
    trr_show_id: UUID
    show_name: str = Field(min_length=1)
    canonical_slug: str | None
    alternative_names: list[str] | None
    show_total_episodes: int | None = Field(ge=0)
    poster_url: str | None


class CreateCoveredShowV2(_StrictModel):
    trr_show_id: UUID
    show_name: str = Field(min_length=1)


class CoveredShowListResponseV2(_StrictModel):
    shows: list[CoveredShowV2]


class CoveredShowResponseV2(_StrictModel):
    show: CoveredShowV2


class CoveredShowDeleteResponseV2(_StrictModel):
    success: bool


class CoveredShowProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class CoveredShowProblemResponseV2(_StrictModel):
    detail: CoveredShowProblemDetailV2
