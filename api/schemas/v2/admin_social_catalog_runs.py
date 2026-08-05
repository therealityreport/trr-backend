"""Strict API v2 contracts for recent social-account catalog runs."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class AdminSocialCatalogRunCommentsFollowupV2(_StrictModel):
    run_id: str | None = None
    status: str | None = None
    state: str | None = None
    source: str | None = None
    error_message: str | None = None
    failed_at: str | None = None
    retryable: bool | None = None


class AdminSocialCatalogRunMediaFollowupV2(_StrictModel):
    attachment_id: str | None = None
    status: str | None = None
    state: str | None = None
    source: str | None = None
    enqueued_job_ids: list[str] = Field(default_factory=list)
    enqueued_job_count: int = Field(ge=0)


class AdminSocialCatalogRunFollowupsV2(_StrictModel):
    comments: AdminSocialCatalogRunCommentsFollowupV2 | None = None
    media: AdminSocialCatalogRunMediaFollowupV2 | None = None


class AdminSocialCatalogRunV2(_StrictModel):
    job_id: str
    run_id: str = Field(min_length=1)
    status: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    error_message: str | None = None
    catalog_action: str | None = None
    catalog_action_scope: str | None = None
    date_start: str | None = None
    date_end: str | None = None
    launch_group_id: str | None = None
    launch_state: str | None = None
    selected_tasks: list[str] = Field(default_factory=list)
    effective_selected_tasks: list[str] = Field(default_factory=list)
    comments_run_id: str | None = None
    attached_followups: AdminSocialCatalogRunFollowupsV2 = Field(default_factory=AdminSocialCatalogRunFollowupsV2)


class AdminSocialCatalogRecentRunsResponseV2(_StrictModel):
    platform: str = Field(min_length=1)
    handle: str = Field(min_length=1)
    catalog_recent_runs: list[AdminSocialCatalogRunV2] = Field(default_factory=list)


class AdminSocialCatalogRunsProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class AdminSocialCatalogRunsProblemResponseV2(_StrictModel):
    detail: AdminSocialCatalogRunsProblemDetailV2
