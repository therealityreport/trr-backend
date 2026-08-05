"""Strict API v2 contracts for admin Reddit post reads."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class AdminRedditPostDetailV2(_StrictModel):
    reddit_post_id: str = Field(min_length=1)
    subreddit: str | None = None
    title: str | None = None
    text: str | None = None
    url: str | None = None
    permalink: str | None = None
    author: str | None = None
    score: int
    num_comments: int
    posted_at: str | None = None
    link_flair_text: str | None = None
    canonical_flair_key: str | None = None
    upvote_ratio: float | None = None
    is_self: bool | None = None
    post_type: str | None = None
    thumbnail: str | None = None
    content_url: str | None = None
    is_nsfw: bool | None = None
    is_spoiler: bool | None = None
    author_flair_text: str | None = None
    detail_scraped_at: str | None = None
    source_sorts: list[str] = Field(default_factory=list)
    media_metadata: dict[str, Any] | None = None
    poll_data: dict[str, Any] | None = None
    matches: list[dict[str, Any]] = Field(default_factory=list)
    comments: list[dict[str, Any]] = Field(default_factory=list)
    comment_summary: dict[str, Any]
    media: list[dict[str, Any]] = Field(default_factory=list)
    media_summary: dict[str, Any]
    assigned_threads: list[dict[str, Any]] = Field(default_factory=list)


class AdminRedditPostDetailResponseV2(_StrictModel):
    post: AdminRedditPostDetailV2


class AdminRedditResolvedPostV2(_StrictModel):
    title: str | None = None
    author: str | None = None
    posted_at: str | None = None
    url: str | None = None
    permalink: str | None = None


class AdminRedditPostResolveResponseV2(_StrictModel):
    reddit_post_id: str = Field(min_length=1)
    detail_slug: str = Field(min_length=1)
    collision: bool
    post: AdminRedditResolvedPostV2


class AdminRedditFlairContainerCountV2(_StrictModel):
    container_key: str = Field(min_length=1)
    post_count: int = Field(ge=0)


class AdminRedditTrackedFlairCountV2(_StrictModel):
    flair_key: str
    flair_label: str = Field(min_length=1)
    post_count: int = Field(ge=0)
    container_counts: list[AdminRedditFlairContainerCountV2] = Field(default_factory=list)


class AdminRedditPendingTrackedFlairCountV2(_StrictModel):
    container_key: str = Field(min_length=1)
    flair_key: str
    flair_label: str = Field(min_length=1)
    post_count: int = Field(ge=0)


class AdminRedditFlairCountV2(_StrictModel):
    flair: str = Field(min_length=1)
    post_count: int = Field(ge=0)


class AdminRedditPostWindowCountsResponseV2(_StrictModel):
    counts: dict[str, int]
    total_posts: int = Field(ge=0)
    tracked_total_posts: int = Field(ge=0)
    tracked_flair_counts: list[AdminRedditTrackedFlairCountV2] = Field(default_factory=list)
    pending_tracked_flair_counts: list[AdminRedditPendingTrackedFlairCountV2] = Field(default_factory=list)
    flair_counts: list[AdminRedditFlairCountV2] = Field(default_factory=list)


class AdminRedditPostWindowPaginationV2(_StrictModel):
    page: int = Field(ge=1)
    per_page: int = Field(ge=1, le=200)
    total_count: int = Field(ge=0)


class AdminRedditPostWindowPostV2(_StrictModel):
    reddit_post_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    text: str | None = None
    url: str
    permalink: str | None = None
    author: str | None = None
    score: int = Field(ge=0)
    num_comments: int = Field(ge=0)
    posted_at: str | None = None
    link_flair_text: str | None = None
    is_show_match: bool
    passes_flair_filter: bool
    match_score: int | None = None
    match_type: str = Field(min_length=1)


class AdminRedditPostWindowResponseV2(_StrictModel):
    pagination: AdminRedditPostWindowPaginationV2
    posts: list[AdminRedditPostWindowPostV2] = Field(default_factory=list)


class AdminRedditReadProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class AdminRedditReadProblemResponseV2(_StrictModel):
    detail: AdminRedditReadProblemDetailV2
