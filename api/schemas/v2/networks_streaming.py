"""Strict API v2 contracts for networks/streaming admin reads."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

NetworkStreamingEntityTypeV2 = Literal["network", "streaming", "production"]
NetworkStreamingResolutionStatusV2 = Literal["resolved", "manual_required", "failed"]
NetworkStreamingLogoMirrorStatusV2 = Literal["mirrored", "skipped", "failed"]
NetworkStreamingFamilyEntityTypeV2 = Literal["network", "streaming"]
NetworkStreamingLinkGroupV2 = Literal[
    "official",
    "social",
    "knowledge",
    "cast_announcements",
    "other",
]
NetworkStreamingCoverageTypeV2 = Literal[
    "family_all_shows",
    "family_network_shows",
    "family_streaming_shows",
    "franchise_rule",
    "show_wikidata_exact",
    "show_name_contains",
]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class NetworkStreamingSummaryTotalsV2(_StrictModel):
    total_available_shows: int = Field(ge=0)
    total_added_shows: int = Field(ge=0)


class NetworkStreamingSummaryRowV2(_StrictModel):
    type: NetworkStreamingEntityTypeV2
    name: str = Field(min_length=1)
    available_show_count: int = Field(ge=0)
    added_show_count: int = Field(ge=0)
    hosted_logo_url: str | None
    hosted_logo_black_url: str | None
    hosted_logo_white_url: str | None
    wikidata_id: str | None
    wikipedia_url: str | None
    tmdb_entity_id: str | None
    homepage_url: str | None
    resolution_status: NetworkStreamingResolutionStatusV2 | None
    resolution_reason: str | None
    last_attempt_at: datetime | None
    has_logo: bool
    has_bw_variants: bool
    has_links: bool


class NetworkStreamingSummaryResponseV2(_StrictModel):
    totals: NetworkStreamingSummaryTotalsV2
    rows: list[NetworkStreamingSummaryRowV2]
    generated_at: datetime


class NetworkStreamingCoreDetailV2(_StrictModel):
    entity_id: str | None
    origin_country: str | None
    display_priority: int | None
    tmdb_logo_path: str | None
    logo_path: str | None
    hosted_logo_key: str | None
    hosted_logo_url: str | None
    hosted_logo_black_url: str | None
    hosted_logo_white_url: str | None
    wikidata_id: str | None
    wikipedia_url: str | None
    wikimedia_logo_file: str | None
    link_enriched_at: datetime | None
    link_enrichment_source: str | None
    facebook_id: str | None
    instagram_id: str | None
    twitter_id: str | None
    tiktok_id: str | None


class NetworkStreamingOverrideDetailV2(_StrictModel):
    id: str | None
    display_name_override: str | None
    wikidata_id_override: str | None
    wikipedia_url_override: str | None
    logo_source_urls_override: list[str]
    source_priority_override: list[str]
    aliases_override: list[str]
    notes: str | None
    is_active: bool
    updated_by: str | None
    updated_at: datetime | None


class NetworkStreamingCompletionDetailV2(_StrictModel):
    resolution_status: NetworkStreamingResolutionStatusV2 | None
    resolution_reason: str | None
    last_attempt_at: datetime | None


class NetworkStreamingLogoAssetV2(_StrictModel):
    id: str = Field(min_length=1)
    source: str = Field(min_length=1)
    source_url: str = Field(min_length=1)
    source_rank: int = Field(ge=0)
    hosted_logo_url: str | None
    hosted_logo_content_type: str | None
    base_logo_format: str = Field(min_length=1)
    pixel_width: int | None = Field(ge=0)
    pixel_height: int | None = Field(ge=0)
    mirror_status: NetworkStreamingLogoMirrorStatusV2
    failure_reason: str | None
    is_primary: bool
    updated_at: datetime | None


class NetworkStreamingShowV2(_StrictModel):
    trr_show_id: str = Field(min_length=1)
    show_name: str = Field(min_length=1)
    canonical_slug: str | None
    poster_url: str | None


class NetworkStreamingFamilyMemberV2(_StrictModel):
    id: str = Field(min_length=1)
    family_id: str = Field(min_length=1)
    entity_type: NetworkStreamingFamilyEntityTypeV2
    entity_key: str = Field(min_length=1)
    entity_display_name: str = Field(min_length=1)
    source: str = Field(min_length=1)
    confidence: float | None = Field(ge=0, le=1)
    metadata: dict[str, Any]
    created_by: str | None
    updated_by: str | None
    created_at: datetime | None
    updated_at: datetime | None


class NetworkStreamingFamilyV2(_StrictModel):
    id: str = Field(min_length=1)
    family_key: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    owner_wikidata_id: str | None
    owner_label: str | None
    is_active: bool
    notes: str | None
    metadata: dict[str, Any]
    created_by: str | None
    updated_by: str | None
    created_at: datetime | None
    updated_at: datetime | None
    members: list[NetworkStreamingFamilyMemberV2]


class NetworkStreamingFamilySuggestionEntityV2(_StrictModel):
    entity_type: NetworkStreamingEntityTypeV2
    entity_key: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    updated_at: datetime


class NetworkStreamingFamilySuggestionV2(_StrictModel):
    owner_wikidata_id: str = Field(min_length=1)
    owner_label: str = Field(min_length=1)
    entity_count: int = Field(ge=2)
    entities: list[NetworkStreamingFamilySuggestionEntityV2]


class NetworkStreamingSharedLinkV2(_StrictModel):
    id: str = Field(min_length=1)
    family_id: str = Field(min_length=1)
    link_group: NetworkStreamingLinkGroupV2
    link_kind: str = Field(min_length=1)
    label: str | None
    url: str = Field(min_length=1)
    url_key: str = Field(min_length=1)
    coverage_type: NetworkStreamingCoverageTypeV2
    coverage_value: str | None
    source: str = Field(min_length=1)
    priority: int
    auto_apply: bool
    is_active: bool
    metadata: dict[str, Any]
    created_at: datetime | None
    updated_at: datetime | None
    created_by: str | None
    updated_by: str | None


class NetworkStreamingWikipediaShowUrlV2(_StrictModel):
    id: str = Field(min_length=1)
    family_id: str = Field(min_length=1)
    entity_type: NetworkStreamingFamilyEntityTypeV2
    entity_key: str = Field(min_length=1)
    brand_wikipedia_url: str | None
    show_url: str = Field(min_length=1)
    show_url_key: str = Field(min_length=1)
    show_title: str | None
    wikidata_id: str | None
    matched_show_id: str | None
    match_method: str | None
    import_source: str = Field(min_length=1)
    is_applied: bool
    metadata: dict[str, Any]
    last_seen_at: datetime | None
    created_at: datetime | None
    updated_at: datetime | None


class NetworkStreamingDetailResponseV2(_StrictModel):
    entity_type: NetworkStreamingEntityTypeV2
    entity_key: str = Field(min_length=1)
    entity_slug: str = Field(min_length=1, pattern=r"^[a-z0-9]+(?:-+[a-z0-9]+)*$")
    display_name: str = Field(min_length=1)
    available_show_count: int = Field(ge=0)
    added_show_count: int = Field(ge=0)
    core: NetworkStreamingCoreDetailV2
    override: NetworkStreamingOverrideDetailV2
    completion: NetworkStreamingCompletionDetailV2
    logo_assets: list[NetworkStreamingLogoAssetV2]
    shows: list[NetworkStreamingShowV2]
    family: NetworkStreamingFamilyV2 | None
    family_suggestions: list[NetworkStreamingFamilySuggestionV2]
    shared_links: list[NetworkStreamingSharedLinkV2]
    wikipedia_show_urls: list[NetworkStreamingWikipediaShowUrlV2]


class NetworkStreamingSuggestionV2(_StrictModel):
    entity_type: NetworkStreamingEntityTypeV2
    name: str = Field(min_length=1)
    entity_slug: str = Field(min_length=1, pattern=r"^[a-z0-9]+(?:-+[a-z0-9]+)*$")
    available_show_count: int = Field(ge=0)
    added_show_count: int = Field(ge=0)


class NetworkStreamingProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class NetworkStreamingProblemResponseV2(_StrictModel):
    detail: NetworkStreamingProblemDetailV2


class NetworkStreamingDetailNotFoundProblemDetailV2(NetworkStreamingProblemDetailV2):
    suggestions: list[NetworkStreamingSuggestionV2]


class NetworkStreamingDetailNotFoundProblemResponseV2(_StrictModel):
    detail: NetworkStreamingDetailNotFoundProblemDetailV2
