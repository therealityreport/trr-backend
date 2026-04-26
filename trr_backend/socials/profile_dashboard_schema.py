from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class SocialAccountDashboardFreshness(BaseModel):
    status: Literal["fresh", "stale", "missing", "error"]
    source: Literal["live", "cache", "materialized"]
    generated_at: str | None = None
    age_seconds: int | None = Field(default=None, ge=0)


class SocialAccountDashboardData(BaseModel):
    model_config = ConfigDict(extra="allow")

    summary: dict[str, Any]
    catalog_run_progress: dict[str, Any] | None = None


class SocialAccountDashboardPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    data: SocialAccountDashboardData
    freshness: SocialAccountDashboardFreshness
    operational_alerts: list[dict[str, Any]] = Field(default_factory=list)
