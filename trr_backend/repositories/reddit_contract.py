"""Canonical versioned Reddit capture response types shared with generated app types."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

RefreshPolicy = Literal["missing", "stale", "force"]
TargetState = Literal["pending", "running", "retry_wait", "committed", "source_unavailable", "failed", "cancelled"]
CapturePhase = Literal["discovery", "comments", "media"]


class RedditCoverageEvidence(BaseModel):
    model_config = ConfigDict(extra="allow")
    state: Literal["unknown", "partial", "complete", "not_requested"]


class RedditPhaseOutcome(BaseModel):
    requested: int = Field(ge=0)
    satisfied: int = Field(ge=0)
    unresolved: int = Field(ge=0)
    states: dict[TargetState, int]


class RedditRunCaptureContract(BaseModel):
    contract_version: Literal[2] = 2
    control_state: Literal["active", "paused", "cancel_requested", "cancelled"]
    requested: int = Field(ge=0)
    satisfied: int = Field(ge=0)
    unresolved: int = Field(ge=0)
    outcomes: dict[CapturePhase, RedditPhaseOutcome]
    coverage: RedditCoverageEvidence
    retry_at: datetime | None = None
    requested_phases: list[CapturePhase]


class RedditWorkTarget(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: str | None = None
    kind: CapturePhase
    target_key: str
    reddit_post_id: str | None = None
    state: TargetState
    attempt_count: int = 0
    error: str | None = None
    checkpoint: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] = Field(default_factory=dict)
    next_retry_at: datetime | None = None


class RedditCaptureEvidence(BaseModel):
    model_config = ConfigDict(extra="allow")
    state: TargetState
    successful_at: datetime | None = None
    attempted_at: datetime | None = None
    next_retry_at: datetime | None = None
    evidence: dict[str, Any] = Field(default_factory=dict)


class RedditPostCapture(BaseModel):
    reddit_post_id: str
    capture_state: dict[str, RedditCaptureEvidence]
