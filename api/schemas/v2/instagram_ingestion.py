"""Strict contracts for the rebuilt Instagram path (routing initially disabled)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from trr_backend.ingestion.domain import StageCounts

Task = Literal["post_details", "comments", "media"]
SourceScope = Literal["bravo", "network", "creator", "community", "news"]
RunState = Literal["queued", "running", "blocked", "cancelled", "completed", "failed"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class InstagramIngestionLaunchV2(StrictModel):
    action: Literal["backfill", "sync_recent", "sync_newer", "resume_tail"] = "backfill"
    source_scope: SourceScope = "network"
    date_start: datetime | None = None
    date_end: datetime | None = None
    lookback_days: int = Field(default=1, ge=1, le=30)
    backfill_scope: Literal["full_history", "bounded_window"] = "full_history"
    selected_tasks: list[Task] = Field(default_factory=lambda: ["post_details", "comments", "media"], min_length=1)
    detail_worker_count: int | None = Field(default=None, ge=1, le=12)
    comments_worker_count: int | None = Field(default=None, ge=1, le=24)
    comments_enable_media_followups: bool | None = None
    force_catalog_rediscovery: bool = False
    enable_cap4_canary: bool = False
    apply_run_id: UUID | None = None
    operator_confirmation: str | None = None

    @field_validator("source_scope", mode="before")
    @classmethod
    def normalize_source_scope(cls, value):
        return "network" if value == "bravo" else value

    @model_validator(mode="after")
    def canonical_tasks(self):
        self.selected_tasks = [task for task in ("post_details", "comments", "media") if task in self.selected_tasks]
        if self.action == "backfill" and self.backfill_scope == "bounded_window":
            if self.date_start is None or self.date_end is None:
                raise ValueError("Bounded backfill requires date_start and date_end")
            start = self.date_start.replace(tzinfo=UTC) if self.date_start.tzinfo is None else self.date_start
            end = self.date_end.replace(tzinfo=UTC) if self.date_end.tzinfo is None else self.date_end
            if start >= end:
                raise ValueError("date_start must precede date_end")
        return self


class InstagramIngestionCommandV2(StrictModel):
    run_id: UUID
    state: RunState
    engine: Literal["dbos", "temporal"]
    workflow_version: str = Field(min_length=1)
    generation: int = Field(ge=1)
    execution_id: str | None = None


class InstagramStageProgressV2(StrictModel):
    stage: Literal["listing", "post_details", "comments", "media"]
    discovery_complete: bool
    sealed_at: datetime | None
    total: int = Field(ge=0)
    committed: int = Field(ge=0)
    cached_satisfied: int = Field(ge=0)
    source_unavailable: int = Field(ge=0)
    unresolved: int = Field(ge=0)
    attempts: int = Field(ge=0)
    requests: int = Field(ge=0)
    retry_at: datetime | None
    blocked_reasons: list[str]
    last_committed_at: datetime | None
    complete: bool

    @model_validator(mode="after")
    def balanced_outcomes(self):
        counts = StageCounts(
            self.total,
            self.committed,
            self.cached_satisfied,
            self.source_unavailable,
            self.unresolved,
            self.discovery_complete,
        )
        if self.complete != counts.complete or self.discovery_complete != (self.sealed_at is not None):
            raise ValueError("Stage completion does not match persisted outcomes and discovery")
        return self


class InstagramRunIntentV2(StrictModel):
    platform: Literal["instagram"]
    account: str = Field(min_length=1, max_length=30)
    action: Literal["backfill", "sync_recent", "sync_newer", "resume_tail"]
    source_scope: SourceScope
    date_start: datetime | None
    date_end: datetime | None
    selected_tasks: list[Task] = Field(min_length=1)
    scope: Literal["full_history", "bounded_window", "recent_window", "head_gap", "frontier_resume"]
    metadata_ttl_days: int = Field(ge=1, le=365)
    metrics_ttl_hours: Literal[24]
    force: bool
    policy_version: Literal[1]
    detail_worker_count: int | None = Field(default=None, ge=1, le=12)
    comments_worker_count: int | None = Field(default=None, ge=1, le=24)
    comments_enable_media_followups: bool = True
    enable_cap4_canary: bool = False
    listing_max_attempts: int = Field(default=12, ge=1)
    comments_max_attempts: int = Field(default=12, ge=1, le=12)
    media_max_attempts: Literal[3] = 3


class InstagramIngestionProgressV2(StrictModel):
    execution_placement: Literal["modal", "local"] = "modal"
    run_id: UUID
    account: str
    intent: InstagramRunIntentV2
    intent_hash: str
    engine: Literal["dbos", "temporal"]
    workflow_version: str
    state: RunState
    generation: int = Field(ge=1)
    last_committed_at: datetime | None
    created_at: datetime
    updated_at: datetime
    stages: list[InstagramStageProgressV2]
    pinned_evidence_count: int = Field(ge=0)
    complete: bool
    allowed_actions: list[Literal["cancel", "resume", "retry_targets", "replay"]]

    @model_validator(mode="after")
    def complete_requires_all_selected_stages(self):
        stages = {stage.stage: stage for stage in self.stages}
        if len(stages) != len(self.stages):
            raise ValueError("Duplicate stage progress")
        complete = all(stage in stages and stages[stage].complete for stage in ("listing", *self.intent.selected_tasks))
        if self.complete != complete or (self.state == "completed" and not complete):
            raise ValueError("Run completion does not match stage outcomes")
        return self


class InstagramIngestionControlV2(StrictModel):
    """Optional empty command body; supplied unknown fields remain invalid."""


class InstagramRetryTargetsV2(StrictModel):
    stage: Task
    target_keys: list[str] = Field(min_length=1, max_length=100)


class InstagramReplayV2(StrictModel):
    evidence_id: UUID
    apply: bool = False
    parser_version: str = Field(min_length=1)


class InstagramIngestionProblemV2(StrictModel):
    code: str
    status: int = Field(ge=400, le=599)
    message: str
    retryable: bool = False
    retry_after_ms: int | None = Field(default=None, ge=0)
    request_id: str | None = None
    trace_id: str | None = None


class InstagramIngestionProblemResponseV2(StrictModel):
    detail: InstagramIngestionProblemV2


class InstagramIngestionCapabilitiesV2(StrictModel):
    enabled: bool
    account: str
    engine: Literal["dbos", "temporal"] | None = None
    workflow_version: str | None = None
    supported_actions: list[Literal["backfill", "sync_recent", "sync_newer", "resume_tail"]] = Field(
        default_factory=list
    )
    replay_apply_enabled: bool = False
    disabled_reason: str | None = None


class InstagramRecentRunV2(StrictModel):
    execution_placement: Literal["modal", "local"] = "modal"
    run_id: UUID
    account: str
    state: RunState
    intent: InstagramRunIntentV2
    engine: Literal["dbos", "temporal"]
    workflow_version: str
    created_at: datetime
    last_committed_at: datetime | None


class InstagramRecentRunsQueryV2(StrictModel):
    source_scope: SourceScope = "network"
    limit: int = Field(default=20, ge=1, le=100)

    @field_validator("source_scope", mode="before")
    @classmethod
    def normalize_source_scope(cls, value):
        return "network" if value == "bravo" else value


class InstagramDiagnosticsQueryV2(StrictModel):
    limit: int = Field(default=100, ge=1, le=100)


class InstagramRecentRunsV2(StrictModel):
    runs: list[InstagramRecentRunV2] = Field(max_length=100)


class InstagramTargetDiagnosticV2(StrictModel):
    stage: Literal["listing", "post_details", "comments", "media"]
    target_key: str
    outcome: Literal["committed", "cached_satisfied", "source_unavailable", "unresolved"]
    attempts: int = Field(ge=0)
    requests: int = Field(ge=0)
    max_attempts: int = Field(ge=1)
    retry_at: datetime | None
    blocked_reason: str | None
    last_error_code: str | None = None


class InstagramEvidenceDiagnosticV2(StrictModel):
    evidence_id: UUID
    stage: Literal["listing", "post_details", "comments", "media"]
    target_key: str
    source_identity: str
    captured_at: datetime
    content_hash: str
    available: bool
    expires_at: datetime
    pinned: bool
    parser_version: str
    schema_version: int = Field(ge=1)


class InstagramIngestionDiagnosticsV2(StrictModel):
    run_id: UUID
    account: str
    targets: list[InstagramTargetDiagnosticV2] = Field(max_length=100)
    evidence: list[InstagramEvidenceDiagnosticV2] = Field(max_length=100)
    targets_truncated: bool
    evidence_truncated: bool


class InstagramReplayComparisonV2(StrictModel):
    previous: JsonValue
    current: JsonValue
    changed: bool


class InstagramReplayResultV2(StrictModel):
    mode: Literal["dry_run", "apply"]
    evidence_id: UUID
    run_id: UUID
    stage: Literal["listing", "post_details", "comments", "media"]
    target_key: str
    captured_at: datetime
    content_hash: str
    capture_parser_version: str
    parser_version: str
    schema_version: int = Field(ge=1)
    normalized: JsonValue
    comparison: InstagramReplayComparisonV2
    writes: int = Field(ge=0)
    source_requests: Literal[0] = 0
    receipt_id: UUID | None = None

    @model_validator(mode="after")
    def replay_side_effects(self):
        if self.mode == "dry_run" and (self.writes != 0 or self.receipt_id is not None):
            raise ValueError("Dry-run replay cannot write or produce a commit receipt")
        if self.mode == "apply" and self.receipt_id is None:
            raise ValueError("Apply replay requires a commit receipt")
        return self
