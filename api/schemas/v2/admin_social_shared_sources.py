"""Strict v2 models for shared social account source administration."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt

SocialPlatformV2 = Literal["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"]
SourceScopeV2 = Literal["network", "creator", "community", "news"]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class SharedAccountSourceInputV2(_StrictModel):
    platform: SocialPlatformV2
    account_handle: str = Field(min_length=1, max_length=128)
    is_active: StrictBool = True
    scrape_priority: StrictInt = Field(default=100, ge=1, le=10_000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class PutSharedAccountSourcesRequestV2(_StrictModel):
    source_scope: SourceScopeV2 = "network"
    sources: list[SharedAccountSourceInputV2]


class SharedAccountSourceV2(_StrictModel):
    id: str
    platform: SocialPlatformV2
    source_scope: SourceScopeV2
    account_handle: str
    is_active: bool
    scrape_priority: int
    metadata: dict[str, Any]
    last_scrape_status: str | None = None
    last_scrape_run_id: str | None = None
    last_scrape_job_id: str | None = None
    last_scrape_at: datetime | None = None
    last_classified_at: datetime | None = None
    updated_by: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    is_default: bool
    profile_kind: str
    network_name: str
    assignment_mode: str
    assignment_rules: dict[str, Any]


class SharedAccountSourcesResponseV2(_StrictModel):
    source_scope: SourceScopeV2
    sources: list[SharedAccountSourceV2]
    using_defaults: bool


class SharedAccountSourcesProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class SharedAccountSourcesProblemResponseV2(_StrictModel):
    detail: SharedAccountSourcesProblemDetailV2
