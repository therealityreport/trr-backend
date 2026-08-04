"""Strict API v2 contracts for an exact admin show-slug read."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ExactShowSlugV2(_StrictModel):
    id: UUID
    name: str = Field(min_length=1)
    slug: str = Field(min_length=1, max_length=160)


class ExactShowSlugResponseV2(_StrictModel):
    show: ExactShowSlugV2


class ShowSlugProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class ShowSlugProblemResponseV2(_StrictModel):
    detail: ShowSlugProblemDetailV2
