"""
Admin endpoints for syncing and refreshing show metadata.

This router exposes "one-button" admin actions that wrap existing ingestion/sync logic:
- Sync from IMDb/TMDb lists (import + enrich, no images)
- Refresh a single show by target area (details, seasons+episodes, photos, cast/credits)
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import re
import time
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from threading import Thread
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

import scripts.sync.sync_episode_appearances as sync_episode_appearances
import scripts.sync.sync_episodes as sync_episodes
import scripts.sync.sync_networks_streaming_links as sync_networks_streaming_links
import scripts.sync.sync_season_episode_images as sync_season_episode_images
import scripts.sync.sync_seasons as sync_seasons
import scripts.sync.sync_seasons_episodes as sync_seasons_episodes
import scripts.sync.sync_show_cast as sync_show_cast
import scripts.sync.sync_show_images as sync_show_images
import scripts.sync.sync_show_logos as sync_show_logos
import scripts.sync.sync_shows as sync_shows
import scripts.sync.sync_shows_all as sync_shows_all
import scripts.sync.sync_tmdb_show_entities as sync_tmdb_show_entities
import scripts.sync.sync_tmdb_watch_providers as sync_tmdb_watch_providers
from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from trr_backend.ingestion.show_importer import (
    collect_candidates_from_lists,
    parse_imdb_headers_json_env,
    upsert_candidates_into_supabase,
)
from trr_backend.integrations.tmdb.client import resolve_api_key
from trr_backend.media.s3_mirror import (
    build_hosted_url,
    build_logo_s3_key,
    build_show_image_s3_key,
    download_image,
    get_s3_bucket,
    get_s3_client,
    guess_ext_from_content_type,
    mirror_external_logo_row,
    mirror_logo_monochrome_variants_row,
    upload_bytes_to_s3,
)
from trr_backend.pipeline.admin_operations import operation_stream_response, start_operation_for_stream
from trr_backend.repositories import brand_families
from trr_backend.repositories.media_assets import update_asset_with_mirror_result
from trr_backend.repositories.web_scrape_images import (
    create_media_asset_from_scrape,
    create_media_link_for_entity,
    find_asset_by_sha256,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/shows", tags=["admin-show-sync"])
STREAM_HEARTBEAT_INTERVAL_SECONDS = 10


def _split_env_list(var_name: str) -> list[str]:
    raw = (os.getenv(var_name) or "").strip()
    if not raw:
        return []
    parts = []
    for chunk in raw.split(","):
        item = chunk.strip()
        if item:
            parts.append(item)
    return parts


def _field_provided(model: BaseModel, field_name: str) -> bool:
    fields_set = getattr(model, "model_fields_set", None)
    if fields_set is None:
        fields_set = getattr(model, "__fields_set__", set())
    return field_name in fields_set


def _is_internal_raw_stream_request(request: Request | Any | None) -> bool:
    headers = getattr(request, "headers", None)
    if headers is None:
        return False
    try:
        value = headers.get("x-trr-internal-raw-stream")
    except Exception:  # noqa: BLE001
        return False
    return str(value or "").strip() == "1"


def _is_real_housewives_show(show_name: str | None) -> bool:
    if not isinstance(show_name, str):
        return False
    normalized = show_name.strip().lower()
    return bool(normalized) and "real housewives" in normalized


def _count_mirrored_cast_photos_for_source(
    db: SupabaseAdminClient,
    person_id: str,
    source: Literal["imdb", "tmdb"],
) -> int:
    try:
        response = (
            db.schema("core")
            .table("cast_photos")
            .select("id", count="exact")
            .eq("person_id", person_id)
            .eq("source", source)
            .not_.is_("hosted_url", "null")
            .execute()
        )
        if hasattr(response, "count") and response.count is not None:
            return int(response.count)
        data = response.data or []
        return len(data) if isinstance(data, list) else 0
    except Exception:  # noqa: BLE001
        return 0


def _get_known_person_source_total(
    source: Literal["imdb", "tmdb"],
    imdb_person_id: str | None,
    tmdb_person_id: int | None,
) -> int | None:
    if source == "imdb" and imdb_person_id:
        try:
            from trr_backend.integrations.imdb.person_gallery import (
                fetch_imdb_person_mediaindex_html,
                parse_imdb_person_mediaindex_images,
            )

            html = fetch_imdb_person_mediaindex_html(imdb_person_id, session=None)
            images = parse_imdb_person_mediaindex_images(html, imdb_person_id)
            return len(images) if isinstance(images, list) else None
        except Exception:  # noqa: BLE001
            return None
    if source == "tmdb" and tmdb_person_id:
        try:
            from trr_backend.integrations.tmdb.client import fetch_person_images

            payload = fetch_person_images(int(tmdb_person_id), session=None)
            profiles = payload.get("profiles") if isinstance(payload, dict) else None
            return len(profiles) if isinstance(profiles, list) else None
        except Exception:  # noqa: BLE001
            return None
    return None


def _extract_tag_people_titles(metadata: dict[str, Any]) -> tuple[list[str], list[str], list[str], list[str]]:
    tags = metadata.get("tags") if isinstance(metadata.get("tags"), dict) else {}
    people = tags.get("people") if isinstance(tags.get("people"), list) else []
    titles = tags.get("titles") if isinstance(tags.get("titles"), list) else []

    people_names: list[str] = []
    people_ids: list[str] = []
    for item in people:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        imdb_id = str(item.get("imdb_id") or "").strip()
        if name:
            people_names.append(name)
        if imdb_id:
            people_ids.append(imdb_id)

    title_names: list[str] = []
    title_ids: list[str] = []
    for item in titles:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        imdb_id = str(item.get("imdb_id") or "").strip()
        if title:
            title_names.append(title)
        if imdb_id:
            title_ids.append(imdb_id)

    return (
        list(dict.fromkeys(people_names)),
        list(dict.fromkeys(people_ids)),
        list(dict.fromkeys(title_names)),
        list(dict.fromkeys(title_ids)),
    )


def _normalize_imdb_media_kind(image_type: str | None, fallback_kind: str = "media") -> str:
    normalized = str(image_type or "").strip().lower()
    if not normalized:
        return fallback_kind
    if "still" in normalized or "frame" in normalized:
        return "episode_still"
    if "poster" in normalized:
        return "poster"
    if "publicity" in normalized:
        return "promo"
    return fallback_kind


def _enrich_imdb_mediaindex_rows_with_episode_context(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    show_name: str | None,
    show_imdb_id: str | None,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return

    imdb_title_ids: list[str] = []
    for row in rows:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        _, _, _, title_ids = _extract_tag_people_titles(metadata)
        imdb_title_ids.extend(title_ids)
    imdb_title_ids = list(dict.fromkeys([value for value in imdb_title_ids if value]))

    episodes_by_imdb: dict[str, dict[str, Any]] = {}
    if imdb_title_ids:
        for i in range(0, len(imdb_title_ids), 100):
            chunk = imdb_title_ids[i : i + 100]
            response = (
                db.schema("core")
                .table("episodes")
                .select("id,imdb_episode_id,title,episode_number,season_number,air_date,show_id,show_name")
                .eq("show_id", show_id)
                .in_("imdb_episode_id", chunk)
                .execute()
            )
            if hasattr(response, "error") and response.error:
                logger.warning("IMDb mediaindex episode lookup failed for show %s: %s", show_id, response.error)
                continue
            for episode in response.data or []:
                imdb_episode_id = str(episode.get("imdb_episode_id") or "").strip()
                if imdb_episode_id:
                    episodes_by_imdb[imdb_episode_id] = episode

    for row in rows:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        people_names, people_ids, title_names, title_ids = _extract_tag_people_titles(metadata)
        if people_names:
            metadata["people_names"] = people_names
        if people_ids:
            metadata["people_imdb_ids"] = people_ids
        if title_names:
            metadata["title_names"] = title_names
        if title_ids:
            metadata["title_imdb_ids"] = title_ids

        image_type = str(metadata.get("imdb_image_type") or row.get("image_type") or "").strip() or None
        if image_type:
            metadata["imdb_image_type"] = image_type
            row["image_type"] = image_type
            row["kind"] = _normalize_imdb_media_kind(image_type, str(row.get("kind") or "media"))

        metadata.setdefault("show_id", show_id)
        if show_name:
            metadata.setdefault("show_name", show_name)
        if show_imdb_id:
            metadata.setdefault("show_imdb_id", show_imdb_id)

        for title_id in title_ids:
            episode = episodes_by_imdb.get(title_id)
            if not episode:
                continue
            metadata["episode_id"] = episode.get("id")
            metadata["episode_imdb_id"] = episode.get("imdb_episode_id")
            metadata["episode_title"] = episode.get("title")
            metadata["episode_number"] = episode.get("episode_number")
            metadata["season_number"] = episode.get("season_number")
            metadata["episode_air_date"] = episode.get("air_date")
            row["kind"] = "episode_still"
            break

        row["metadata"] = metadata


class SyncFromListsRequest(BaseModel):
    imdb_lists: list[str] | None = Field(default=None, description="IMDb list URLs.")
    tmdb_lists: list[str] | None = Field(default=None, description="TMDb list ids or URLs.")
    region: str = Field(default="US", description="Region code for enrichment (TMDb watch providers).")
    concurrency: int = Field(default=5, ge=1, le=20, description="Parallelism for metadata enrichment.")
    force_refresh: bool = Field(
        default=False,
        description="Force refetch enrichment even if show_meta appears fresh.",
    )


class SyncFromListsResponse(BaseModel):
    imdb_lists_used: list[str]
    tmdb_lists_used: list[str]
    candidates_collected: int
    created: int
    updated: int
    skipped: int
    duration_ms: int


class SyncNetworksStreamingRequest(BaseModel):
    force: bool = False
    skip_s3: bool = False
    dry_run: bool = False
    unresolved_only: bool = False
    refresh_external_sources: bool = False
    batch_size: int = Field(default=25, ge=1, le=500)
    max_runtime_sec: int = Field(default=840, ge=60, le=3600)
    resume_run_id: str | None = None
    entity_type: Literal["network", "streaming", "production"] | None = None
    entity_keys: list[str] | None = Field(default=None, min_length=1, max_length=200)
    limit: int | None = Field(default=None, ge=1, le=5000)
    verbose: bool = False


class SyncNetworksStreamingStepResult(BaseModel):
    status: Literal["success", "failed"]
    duration_ms: int
    exit_code: int | None = None
    error: str | None = None
    metrics: dict[str, int] = Field(default_factory=dict)


class SyncNetworksStreamingUnresolvedLogo(BaseModel):
    type: Literal["network", "streaming", "production"]
    id: str
    name: str
    reason: str


class SyncNetworksStreamingMissingColumn(BaseModel):
    table: str
    column: str


class SyncNetworksStreamingResumeCursor(BaseModel):
    entity_type: Literal["network", "streaming", "production"]
    entity_key: str


class SyncNetworksStreamingResponse(BaseModel):
    run_id: str
    status: Literal["completed", "stopped", "failed"]
    resume_cursor: SyncNetworksStreamingResumeCursor | None = None
    entities_synced: int
    providers_synced: int
    links_enriched: int
    logos_mirrored: int
    variants_black_mirrored: int
    variants_white_mirrored: int
    logo_assets_discovered: int
    logo_assets_mirrored: int
    logo_assets_skipped: int
    logo_assets_failed: int
    show_logos_discovered: int
    show_logos_imported: int
    show_logos_skipped: int
    show_logo_failures: int
    completion_total: int
    completion_resolved: int
    completion_unresolved: int
    completion_unresolved_total: int
    completion_unresolved_network: int
    completion_unresolved_streaming: int
    completion_unresolved_production: int
    production_missing_logos: int
    production_missing_bw_variants: int
    completion_percent: float
    completion_gate_passed: bool
    missing_columns: list[SyncNetworksStreamingMissingColumn] = Field(default_factory=list)
    unresolved_logos_count: int
    unresolved_logos_truncated: bool
    unresolved_logos: list[SyncNetworksStreamingUnresolvedLogo] = Field(default_factory=list)
    failures: int
    steps: dict[str, SyncNetworksStreamingStepResult]


class NetworksStreamingOverride(BaseModel):
    id: str
    entity_type: Literal["network", "streaming", "production"]
    entity_key: str
    display_name_override: str | None = None
    wikidata_id_override: str | None = None
    wikipedia_url_override: str | None = None
    logo_source_urls_override: dict | list | None = None
    source_priority_override: list[str] = Field(default_factory=list)
    aliases_override: list[str] = Field(default_factory=list)
    notes: str | None = None
    is_active: bool
    updated_by: str | None = None
    updated_at: str | None = None
    created_at: str | None = None


class UpsertNetworksStreamingOverrideRequest(BaseModel):
    entity_type: Literal["network", "streaming", "production"]
    entity_key: str
    display_name_override: str | None = None
    wikidata_id_override: str | None = None
    wikipedia_url_override: str | None = None
    logo_source_urls_override: dict | list | None = None
    source_priority_override: list[str] | None = None
    aliases_override: list[str] | None = None
    notes: str | None = None
    is_active: bool = True


class PatchNetworksStreamingOverrideRequest(BaseModel):
    entity_type: Literal["network", "streaming", "production"] | None = None
    entity_key: str | None = None
    display_name_override: str | None = None
    wikidata_id_override: str | None = None
    wikipedia_url_override: str | None = None
    logo_source_urls_override: dict | list | None = None
    source_priority_override: list[str] | None = None
    aliases_override: list[str] | None = None
    notes: str | None = None
    is_active: bool | None = None


LogoImportTargetType = Literal["show", "network", "streaming", "production"]


class LogoImportUrlRequest(BaseModel):
    target_type: LogoImportTargetType
    source_url: str
    set_primary: bool = False
    show_id: UUID | None = None
    network_id: int | None = None
    provider_id: int | None = None
    production_company_id: int | None = None
    entity_key: str | None = None


class LogoSetPrimaryRequest(BaseModel):
    target_type: LogoImportTargetType
    asset_id: str
    show_id: UUID | None = None
    network_id: int | None = None
    provider_id: int | None = None
    production_company_id: int | None = None
    entity_key: str | None = None


class LogoImportResponse(BaseModel):
    status: Literal["imported", "skipped", "failed"]
    target_type: LogoImportTargetType
    target_id: str
    target_key: str | None = None
    asset_id: str | None = None
    hosted_logo_url: str | None = None
    set_primary: bool = False
    message: str | None = None


@router.post("/sync-from-lists", response_model=SyncFromListsResponse)
def sync_from_lists(
    payload: SyncFromListsRequest | None = None,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> SyncFromListsResponse:
    """
    Sync shows from IMDb/TMDb lists (Stage 1) + enrich show metadata (Stage 2).

    Defaults list sources from env:
    - IMDB_LIST_URL (comma-separated)
    - TMDB_LIST_ID (comma-separated)

    Note: This does NOT fetch TMDb show images during list sync.
    """

    payload = payload or SyncFromListsRequest()

    imdb_lists = [s.strip() for s in (payload.imdb_lists or []) if isinstance(s, str) and s.strip()]
    tmdb_lists = [s.strip() for s in (payload.tmdb_lists or []) if isinstance(s, str) and s.strip()]

    if not _field_provided(payload, "imdb_lists"):
        imdb_lists = _split_env_list("IMDB_LIST_URL")
    if not _field_provided(payload, "tmdb_lists"):
        tmdb_lists = _split_env_list("TMDB_LIST_ID")

    if not imdb_lists and not tmdb_lists:
        raise HTTPException(
            status_code=400,
            detail="No list sources provided. Provide imdb_lists/tmdb_lists or set IMDB_LIST_URL/TMDB_LIST_ID.",
        )

    tmdb_api_key = resolve_api_key() or None
    if tmdb_lists and not tmdb_api_key:
        raise HTTPException(
            status_code=400,
            detail="TMDB_API_KEY is required when tmdb_lists is provided.",
        )

    imdb_extra_headers = parse_imdb_headers_json_env()

    started = time.perf_counter()
    try:
        candidates = collect_candidates_from_lists(
            imdb_list_urls=imdb_lists,
            tmdb_lists=tmdb_lists,
            tmdb_api_key=tmdb_api_key,
            resolve_tmdb_external_ids=True,
            imdb_use_graphql=True,
            imdb_extra_headers=imdb_extra_headers,
        )
        result = upsert_candidates_into_supabase(
            candidates,
            dry_run=False,
            annotate_imdb_episodic=False,
            tmdb_fetch_details=True,
            tmdb_fetch_images=False,
            tmdb_fetch_seasons=False,
            imdb_fetch_episodes=False,
            imdb_fetch_cast=False,
            enrich_show_metadata=True,
            enrich_region=str(payload.region or "US").upper(),
            enrich_concurrency=int(payload.concurrency or 5),
            enrich_force_refresh=bool(payload.force_refresh),
            supabase_client=db,
            imdb_episodic_extra_headers=imdb_extra_headers,
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Sync from lists failed")
        raise HTTPException(status_code=500, detail=f"Sync failed: {exc}") from exc
    duration_ms = int((time.perf_counter() - started) * 1000)

    return SyncFromListsResponse(
        imdb_lists_used=imdb_lists,
        tmdb_lists_used=tmdb_lists,
        candidates_collected=len(candidates),
        created=int(result.created),
        updated=int(result.updated),
        skipped=int(result.skipped),
        duration_ms=duration_ms,
    )


def _extract_metric_int(output: str, key: str) -> int:
    match = re.search(rf"^{re.escape(key)}=(\d+)\s*$", output, flags=re.MULTILINE)
    if not match or not match.group(1):
        return 0
    return int(match.group(1))


def _extract_metric_float(output: str, key: str) -> float:
    match = re.search(rf"^{re.escape(key)}=([0-9]+(?:\.[0-9]+)?)\s*$", output, flags=re.MULTILINE)
    if not match or not match.group(1):
        return 0.0
    return float(match.group(1))


def _extract_metric_text(output: str, key: str) -> str | None:
    match = re.search(rf"^{re.escape(key)}=(.*)\s*$", output, flags=re.MULTILINE)
    if not match:
        return None
    value = match.group(1).strip()
    return value or None


def _extract_unresolved_logos(output: str) -> list[SyncNetworksStreamingUnresolvedLogo]:
    rows: list[SyncNetworksStreamingUnresolvedLogo] = []
    for line in output.splitlines():
        text = str(line or "").strip()
        if not text.startswith("unresolved_logo="):
            continue
        payload_raw = text.partition("=")[2].strip()
        if not payload_raw:
            continue
        try:
            payload = json.loads(payload_raw)
            if isinstance(payload, dict):
                rows.append(SyncNetworksStreamingUnresolvedLogo(**payload))
        except Exception:  # noqa: BLE001
            continue
    return rows


def _schema_requirements() -> dict[tuple[str, str], set[str]]:
    return {
        ("core", "networks"): {
            "id",
            "name",
            "logo_path",
            "hosted_logo_key",
            "hosted_logo_url",
            "hosted_logo_sha256",
            "hosted_logo_black_key",
            "hosted_logo_black_url",
            "hosted_logo_black_sha256",
            "hosted_logo_white_key",
            "hosted_logo_white_url",
            "hosted_logo_white_sha256",
            "wikidata_id",
            "wikipedia_url",
            "wikimedia_logo_file",
            "link_enriched_at",
            "link_enrichment_source",
        },
        ("core", "watch_providers"): {
            "provider_id",
            "provider_name",
            "logo_path",
            "hosted_logo_key",
            "hosted_logo_url",
            "hosted_logo_sha256",
            "hosted_logo_black_key",
            "hosted_logo_black_url",
            "hosted_logo_black_sha256",
            "hosted_logo_white_key",
            "hosted_logo_white_url",
            "hosted_logo_white_sha256",
            "wikidata_id",
            "wikipedia_url",
            "wikimedia_logo_file",
            "link_enriched_at",
            "link_enrichment_source",
        },
        ("core", "production_companies"): {
            "id",
            "name",
            "logo_path",
            "hosted_logo_key",
            "hosted_logo_url",
            "hosted_logo_sha256",
            "hosted_logo_black_key",
            "hosted_logo_black_url",
            "hosted_logo_black_sha256",
            "hosted_logo_white_key",
            "hosted_logo_white_url",
            "hosted_logo_white_sha256",
            "wikidata_id",
            "wikipedia_url",
            "wikimedia_logo_file",
            "link_enriched_at",
            "link_enrichment_source",
        },
        ("admin", "covered_shows"): {
            "trr_show_id",
        },
        ("admin", "network_streaming_overrides"): {
            "id",
            "entity_type",
            "entity_key",
            "logo_source_urls_override",
            "source_priority_override",
            "aliases_override",
            "is_active",
            "updated_at",
        },
        ("admin", "network_streaming_completion"): {
            "id",
            "entity_type",
            "entity_key",
            "resolution_status",
            "resolution_reason",
            "resolution_policy",
            "logo_required",
            "last_run_id",
            "last_attempt_at",
        },
        ("admin", "network_streaming_completion_attempts"): {
            "id",
            "completion_id",
            "run_id",
            "entity_type",
            "entity_key",
            "source",
            "outcome",
            "attempted_at",
        },
        ("admin", "network_streaming_logo_assets"): {
            "id",
            "entity_type",
            "entity_key",
            "source",
            "source_url",
            "source_rank",
            "run_id",
            "hosted_logo_url",
            "hosted_logo_sha256",
            "hosted_logo_content_type",
            "hosted_logo_bytes",
            "hosted_logo_etag",
            "base_logo_format",
            "mirror_status",
            "is_primary",
            "updated_at",
        },
        ("admin", "network_streaming_discovery_state"): {
            "entity_type",
            "entity_key",
            "source",
            "last_outcome",
            "attempt_count",
            "last_attempt_at",
            "lock_until",
            "cached_candidate_count",
            "updated_at",
        },
        ("admin", "network_streaming_sync_runs"): {
            "run_id",
            "status",
            "started_at",
            "finished_at",
            "cursor_entity_type",
            "cursor_entity_key",
            "processed",
            "links_enriched",
            "wikidata_linked",
            "wikipedia_linked",
            "logos_mirrored",
            "variants_black_mirrored",
            "variants_white_mirrored",
            "logo_assets_discovered",
            "logo_assets_mirrored",
            "logo_assets_skipped",
            "logo_assets_failed",
            "completion_total",
            "completion_resolved",
            "completion_unresolved",
            "completion_percent",
            "failures",
            "error_message",
        },
    }


def _schema_preflight_missing_columns(db: SupabaseAdminClient) -> list[SyncNetworksStreamingMissingColumn]:
    expected = _schema_requirements()
    table_schemas = sorted({schema for schema, _ in expected.keys()})
    table_names = sorted({table for _, table in expected.keys()})

    response = (
        db.schema("information_schema")
        .table("columns")
        .select("table_schema,table_name,column_name")
        .in_("table_schema", table_schemas)
        .in_("table_name", table_names)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"schema preflight query failed: {response.error}")

    seen: dict[tuple[str, str], set[str]] = {}
    for row in response.data or []:
        schema_name = str((row or {}).get("table_schema") or "")
        table_name = str((row or {}).get("table_name") or "")
        column_name = str((row or {}).get("column_name") or "")
        if not schema_name or not table_name or not column_name:
            continue
        seen.setdefault((schema_name, table_name), set()).add(column_name)

    missing: list[SyncNetworksStreamingMissingColumn] = []
    for table_key, required_columns in expected.items():
        present = seen.get(table_key, set())
        schema_name, table_name = table_key
        for column_name in sorted(required_columns - present):
            missing.append(
                SyncNetworksStreamingMissingColumn(
                    table=f"{schema_name}.{table_name}",
                    column=column_name,
                )
            )
    return missing


def _sanitize_override_payload(raw: dict, *, partial: bool) -> dict:
    payload: dict = {}

    if not partial or "entity_type" in raw:
        entity_type = str(raw.get("entity_type") or "").strip().lower()
        if entity_type:
            payload["entity_type"] = entity_type
    if not partial or "entity_key" in raw:
        entity_key = str(raw.get("entity_key") or "").strip().casefold()
        if entity_key:
            payload["entity_key"] = entity_key

    nullable_text_fields = [
        "display_name_override",
        "wikidata_id_override",
        "wikipedia_url_override",
        "notes",
        "updated_by",
    ]
    for field_name in nullable_text_fields:
        if partial and field_name not in raw:
            continue
        value = raw.get(field_name)
        if value is None:
            payload[field_name] = None
            continue
        text_value = str(value).strip()
        payload[field_name] = text_value or None

    if (not partial) or ("logo_source_urls_override" in raw):
        value = raw.get("logo_source_urls_override")
        payload["logo_source_urls_override"] = value if value is not None else []

    if (not partial) or ("source_priority_override" in raw):
        raw_values = raw.get("source_priority_override")
        values: list[str] = []
        if isinstance(raw_values, list):
            for item in raw_values:
                text = str(item or "").strip().lower()
                if text:
                    values.append(text)
        payload["source_priority_override"] = values

    if (not partial) or ("aliases_override" in raw):
        raw_values = raw.get("aliases_override")
        values: list[str] = []
        if isinstance(raw_values, list):
            for item in raw_values:
                text = str(item or "").strip()
                if text:
                    values.append(text)
        payload["aliases_override"] = values

    if (not partial) or ("is_active" in raw):
        if "is_active" in raw:
            payload["is_active"] = bool(raw.get("is_active"))
        elif not partial:
            payload["is_active"] = True

    payload["updated_at"] = datetime.now(tz=UTC).isoformat()
    if not partial:
        payload.setdefault("created_at", payload["updated_at"])
    return payload


def _text(value: Any) -> str:
    return str(value or "").strip()


def _detect_base_logo_format(*, source_url: str | None = None, content_type: str | None = None) -> str:
    source = _text(source_url).lower()
    ct = _text(content_type).lower()
    if ".svg" in source or ct == "image/svg+xml":
        return "svg"
    if ".png" in source or ct == "image/png":
        return "png"
    if ".webp" in source or ct == "image/webp":
        return "webp"
    if any(ext in source for ext in (".jpg", ".jpeg")) or ct in {"image/jpeg", "image/jpg"}:
        return "jpg"
    return "unknown"


def _upsert_logo_import_audit(
    db: SupabaseAdminClient,
    *,
    target_type: LogoImportTargetType,
    target_id: str,
    target_key: str | None,
    source_type: Literal["url", "file"],
    source_url: str | None,
    uploaded_filename: str | None,
    hosted_logo_url: str | None,
    hosted_logo_sha256: str | None,
    status: Literal["imported", "skipped", "failed"],
    failure_reason: str | None,
    created_by: str | None,
) -> None:
    payload = {
        "target_type": target_type,
        "target_id": target_id,
        "target_key": target_key,
        "source_type": source_type,
        "source_url": source_url,
        "uploaded_filename": uploaded_filename,
        "hosted_logo_url": hosted_logo_url,
        "hosted_logo_sha256": hosted_logo_sha256,
        "status": status,
        "failure_reason": failure_reason,
        "created_by": created_by,
    }
    response = db.schema("admin").table("entity_logo_imports").insert(payload).execute()
    if hasattr(response, "error") and response.error:
        logger.warning("Failed to write admin.entity_logo_imports row: %s", response.error)


def _dimension_target_config(target_type: Literal["network", "streaming", "production"]) -> dict[str, str]:
    if target_type == "network":
        return {
            "table": "networks",
            "id_field": "id",
            "name_field": "name",
            "entity_type": "network",
            "logo_kind": "networks",
        }
    if target_type == "streaming":
        return {
            "table": "watch_providers",
            "id_field": "provider_id",
            "name_field": "provider_name",
            "entity_type": "streaming",
            "logo_kind": "watch-providers",
        }
    return {
        "table": "production_companies",
        "id_field": "id",
        "name_field": "name",
        "entity_type": "production",
        "logo_kind": "production-companies",
    }


def _resolve_dimension_target(
    db: SupabaseAdminClient,
    *,
    target_type: Literal["network", "streaming", "production"],
    explicit_id: int | None,
    entity_key: str | None,
) -> tuple[dict[str, Any], dict[str, str]]:
    config = _dimension_target_config(target_type)
    table = config["table"]
    id_field = config["id_field"]
    name_field = config["name_field"]

    query = db.schema("core").table(table).select("*")
    if explicit_id is not None:
        response = query.eq(id_field, explicit_id).limit(1).execute()
    else:
        key = _text(entity_key).casefold()
        if not key:
            raise HTTPException(status_code=400, detail="Missing target identifier")
        response = query.ilike(name_field, key).limit(1).execute()

    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to fetch {table} row: {response.error}")
    rows = response.data or []
    if not rows:
        raise HTTPException(status_code=404, detail=f"{target_type} target not found")
    row = rows[0]
    if not isinstance(row, dict):
        raise HTTPException(status_code=404, detail=f"{target_type} target not found")
    return row, config


def _upsert_dimension_logo_asset_row(
    db: SupabaseAdminClient,
    *,
    entity_type: Literal["network", "streaming", "production"],
    entity_key: str,
    entity_id: str,
    display_name: str,
    source_url: str,
    source_rank: int,
    mirror_status: Literal["mirrored", "skipped", "failed"],
    failure_reason: str | None,
    patch: dict[str, Any],
    is_primary: bool,
) -> None:
    payload = {
        "entity_type": entity_type,
        "entity_key": entity_key,
        "entity_id": entity_id or None,
        "display_name": display_name,
        "source": "override",
        "source_url": source_url,
        "source_rank": source_rank,
        "run_id": f"manual-{datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%SZ')}",
        "hosted_logo_key": patch.get("hosted_logo_key"),
        "hosted_logo_url": patch.get("hosted_logo_url"),
        "hosted_logo_sha256": patch.get("hosted_logo_sha256"),
        "hosted_logo_content_type": patch.get("hosted_logo_content_type"),
        "hosted_logo_bytes": patch.get("hosted_logo_bytes"),
        "hosted_logo_etag": patch.get("hosted_logo_etag"),
        "base_logo_format": _detect_base_logo_format(
            source_url=source_url,
            content_type=_text(patch.get("hosted_logo_content_type")) or None,
        ),
        "mirror_status": mirror_status,
        "failure_reason": failure_reason,
        "is_primary": is_primary,
        "updated_at": datetime.now(tz=UTC).isoformat(),
    }
    response = (
        db.schema("admin")
        .table("network_streaming_logo_assets")
        .upsert(payload, on_conflict="entity_type,entity_key,source,source_url")
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to upsert logo asset row: {response.error}")


def _set_dimension_asset_primary_flag(
    db: SupabaseAdminClient,
    *,
    entity_type: Literal["network", "streaming", "production"],
    entity_key: str,
    source_url: str,
) -> None:
    reset = (
        db.schema("admin")
        .table("network_streaming_logo_assets")
        .update({"is_primary": False, "updated_at": datetime.now(tz=UTC).isoformat()})
        .eq("entity_type", entity_type)
        .eq("entity_key", entity_key)
        .execute()
    )
    if hasattr(reset, "error") and reset.error:
        raise HTTPException(status_code=502, detail=f"Failed to reset logo asset primary flags: {reset.error}")

    set_primary = (
        db.schema("admin")
        .table("network_streaming_logo_assets")
        .update({"is_primary": True, "updated_at": datetime.now(tz=UTC).isoformat()})
        .eq("entity_type", entity_type)
        .eq("entity_key", entity_key)
        .eq("source", "override")
        .eq("source_url", source_url)
        .execute()
    )
    if hasattr(set_primary, "error") and set_primary.error:
        raise HTTPException(status_code=502, detail=f"Failed to set logo asset primary flag: {set_primary.error}")


def _set_show_logo_primary(db: SupabaseAdminClient, *, show_id: str, media_asset_id: str) -> None:
    reset = (
        db.schema("core")
        .table("media_links")
        .update({"is_primary": False, "updated_at": datetime.now(tz=UTC).isoformat()})
        .eq("entity_type", "show")
        .eq("entity_id", show_id)
        .eq("kind", "logo")
        .execute()
    )
    if hasattr(reset, "error") and reset.error:
        raise HTTPException(status_code=502, detail=f"Failed to reset show logo primary flags: {reset.error}")

    set_primary = (
        db.schema("core")
        .table("media_links")
        .update({"is_primary": True, "updated_at": datetime.now(tz=UTC).isoformat()})
        .eq("entity_type", "show")
        .eq("entity_id", show_id)
        .eq("kind", "logo")
        .eq("media_asset_id", media_asset_id)
        .execute()
    )
    if hasattr(set_primary, "error") and set_primary.error:
        raise HTTPException(status_code=502, detail=f"Failed to set show logo primary flag: {set_primary.error}")


def _import_show_logo_bytes(
    db: SupabaseAdminClient,
    *,
    show_row: dict[str, Any],
    image_bytes: bytes,
    content_type: str,
    source_url: str,
    set_primary: bool,
) -> LogoImportResponse:
    show_id = _text(show_row.get("id"))
    if not show_id:
        raise HTTPException(status_code=404, detail="Show target not found")

    sha256 = hashlib.sha256(image_bytes).hexdigest()
    existing_asset = find_asset_by_sha256(db, sha256)

    if existing_asset and _text(existing_asset.get("id")):
        asset_id = _text(existing_asset.get("id"))
        hosted_url = _text(existing_asset.get("hosted_url"))

        if not hosted_url:
            s3_client = get_s3_client()
            show_identifier = _text(show_row.get("imdb_id")) or show_id
            ext = guess_ext_from_content_type(content_type)
            key = build_show_image_s3_key(
                show_identifier=show_identifier,
                kind="logo",
                source="manual-logo-import",
                sha256=sha256,
                ext=ext,
            )
            bucket = get_s3_bucket()
            etag, file_size = upload_bytes_to_s3(
                s3_client,
                bucket=bucket,
                key=key,
                data=image_bytes,
                content_type=content_type,
            )
            hosted_url = build_hosted_url(key)
            update_asset_with_mirror_result(
                db,
                asset_id=asset_id,
                sha256=sha256,
                hosted_bucket=bucket,
                hosted_key=key,
                hosted_url=hosted_url,
                hosted_bytes=file_size,
                hosted_content_type=content_type,
                hosted_etag=etag,
                completed_at=datetime.now(tz=UTC).isoformat(),
            )

        create_media_link_for_entity(
            db,
            entity_type="show",
            entity_id=show_id,
            media_asset_id=asset_id,
            kind="logo",
            position=0,
            context={
                "source": "manual_logo_import",
                "source_url": source_url,
            },
        )
        if set_primary:
            _set_show_logo_primary(db, show_id=show_id, media_asset_id=asset_id)

        return LogoImportResponse(
            status="skipped",
            target_type="show",
            target_id=show_id,
            target_key=_text(show_row.get("canonical_slug")) or None,
            asset_id=asset_id,
            hosted_logo_url=hosted_url or None,
            set_primary=set_primary,
            message="Existing asset reused",
        )

    s3_client = get_s3_client()
    show_identifier = _text(show_row.get("imdb_id")) or show_id
    ext = guess_ext_from_content_type(content_type)
    key = build_show_image_s3_key(
        show_identifier=show_identifier,
        kind="logo",
        source="manual-logo-import",
        sha256=sha256,
        ext=ext,
    )
    bucket = get_s3_bucket()
    etag, file_size = upload_bytes_to_s3(
        s3_client,
        bucket=bucket,
        key=key,
        data=image_bytes,
        content_type=content_type,
    )
    hosted_url = build_hosted_url(key)

    asset = create_media_asset_from_scrape(
        db,
        source="manual_logo_import",
        source_url=source_url,
        sha256=sha256,
        hosted_bucket=bucket,
        hosted_key=key,
        hosted_url=hosted_url,
        hosted_bytes=file_size,
        hosted_etag=etag,
        content_type=content_type,
        width=None,
        height=None,
        caption=None,
        metadata={"kind": "logo", "source_url": source_url},
    )
    asset_id = _text(asset.get("id"))
    if not asset_id:
        raise HTTPException(status_code=500, detail="Failed to create media asset")

    create_media_link_for_entity(
        db,
        entity_type="show",
        entity_id=show_id,
        media_asset_id=asset_id,
        kind="logo",
        position=0,
        context={
            "source": "manual_logo_import",
            "source_url": source_url,
        },
    )
    if set_primary:
        _set_show_logo_primary(db, show_id=show_id, media_asset_id=asset_id)

    return LogoImportResponse(
        status="imported",
        target_type="show",
        target_id=show_id,
        target_key=_text(show_row.get("canonical_slug")) or None,
        asset_id=asset_id,
        hosted_logo_url=hosted_url,
        set_primary=set_primary,
    )


def _run_script_step_with_metrics(
    name: str,
    fn: Callable[[list[str] | None], int],
    argv: list[str],
    metric_keys: list[str],
) -> tuple[SyncNetworksStreamingStepResult, str]:
    started = time.perf_counter()
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    try:
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            code = fn(list(argv))
    except BaseException as exc:  # noqa: BLE001
        duration_ms = int((time.perf_counter() - started) * 1000)
        logger.exception("admin sync step failed: %s", name)
        exit_code = None
        if isinstance(exc, SystemExit):
            raw_code = exc.code
            if isinstance(raw_code, int):
                exit_code = raw_code
            elif isinstance(raw_code, str) and raw_code.isdigit():
                exit_code = int(raw_code)
        return (
            SyncNetworksStreamingStepResult(
                status="failed",
                duration_ms=duration_ms,
                exit_code=exit_code,
                error=str(exc),
                metrics=dict.fromkeys(metric_keys, 0),
            ),
            "",
        )

    output = "\n".join([stdout_buffer.getvalue().strip(), stderr_buffer.getvalue().strip()]).strip()
    metrics = {key: _extract_metric_int(output, key) for key in metric_keys}
    duration_ms = int((time.perf_counter() - started) * 1000)
    if int(code) == 0:
        return (
            SyncNetworksStreamingStepResult(
                status="success",
                duration_ms=duration_ms,
                exit_code=0,
                metrics=metrics,
            ),
            output,
        )
    return (
        SyncNetworksStreamingStepResult(
            status="failed",
            duration_ms=duration_ms,
            exit_code=int(code),
            error=f"non-zero exit code: {code}",
            metrics=metrics,
        ),
        output,
    )


def _run_brand_family_wikipedia_import_step(*, dry_run: bool) -> SyncNetworksStreamingStepResult:
    started = time.perf_counter()
    try:
        families = brand_families.list_families(active_only=True).get("rows", [])
        if dry_run:
            return SyncNetworksStreamingStepResult(
                status="success",
                duration_ms=int((time.perf_counter() - started) * 1000),
                exit_code=0,
                metrics={
                    "families_total": len(families),
                    "families_processed": 0,
                    "wikipedia_rows_imported": 0,
                    "wikipedia_rows_matched": 0,
                    "rules_upserted": 0,
                    "fetch_errors": 0,
                    "skipped_dry_run": 1,
                },
            )

        families_processed = 0
        wikipedia_rows_imported = 0
        wikipedia_rows_matched = 0
        rules_upserted = 0
        fetch_errors = 0

        for family in families:
            family_id = str(family.get("id") or "").strip()
            if not family_id:
                continue
            result = brand_families.import_family_wikipedia_show_links(
                family_id=family_id,
                actor="sync_networks_streaming",
                apply_matched=True,
                import_source="sync_networks_streaming",
            )
            families_processed += 1
            wikipedia_rows_imported += int(result.get("imported_count") or 0)
            wikipedia_rows_matched += int(result.get("matched_count") or 0)
            rules_upserted += int(result.get("rules_upserted") or 0)
            fetch_errors += len(result.get("fetch_errors") or [])

        return SyncNetworksStreamingStepResult(
            status="success",
            duration_ms=int((time.perf_counter() - started) * 1000),
            exit_code=0,
            metrics={
                "families_total": len(families),
                "families_processed": families_processed,
                "wikipedia_rows_imported": wikipedia_rows_imported,
                "wikipedia_rows_matched": wikipedia_rows_matched,
                "rules_upserted": rules_upserted,
                "fetch_errors": fetch_errors,
                "skipped_dry_run": 0,
            },
        )
    except Exception as exc:  # noqa: BLE001
        return SyncNetworksStreamingStepResult(
            status="success",
            duration_ms=int((time.perf_counter() - started) * 1000),
            exit_code=None,
            error=f"non_blocking_wikipedia_import_error: {exc}",
            metrics={
                "families_total": 0,
                "families_processed": 0,
                "wikipedia_rows_imported": 0,
                "wikipedia_rows_matched": 0,
                "rules_upserted": 0,
                "fetch_errors": 1,
                "skipped_dry_run": 0,
            },
        )


@router.post("/sync-networks-streaming", response_model=SyncNetworksStreamingResponse)
def sync_networks_streaming(
    payload: SyncNetworksStreamingRequest | None = None,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> SyncNetworksStreamingResponse:
    payload = payload or SyncNetworksStreamingRequest()

    try:
        missing_columns = _schema_preflight_missing_columns(db)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Schema preflight failed: {exc}") from exc
    if missing_columns:
        return SyncNetworksStreamingResponse(
            run_id="schema-preflight",
            status="failed",
            resume_cursor=None,
            entities_synced=0,
            providers_synced=0,
            links_enriched=0,
            logos_mirrored=0,
            variants_black_mirrored=0,
            variants_white_mirrored=0,
            logo_assets_discovered=0,
            logo_assets_mirrored=0,
            logo_assets_skipped=0,
            logo_assets_failed=0,
            show_logos_discovered=0,
            show_logos_imported=0,
            show_logos_skipped=0,
            show_logo_failures=0,
            completion_total=0,
            completion_resolved=0,
            completion_unresolved=0,
            completion_unresolved_total=0,
            completion_unresolved_network=0,
            completion_unresolved_streaming=0,
            completion_unresolved_production=0,
            production_missing_logos=0,
            production_missing_bw_variants=0,
            completion_percent=0.0,
            completion_gate_passed=False,
            missing_columns=missing_columns,
            unresolved_logos_count=0,
            unresolved_logos_truncated=False,
            unresolved_logos=[],
            failures=max(1, len(missing_columns)),
            steps={},
        )

    common_args = ["--all"]
    if payload.force:
        common_args.append("--force")
    if payload.dry_run:
        common_args.append("--dry-run")
    if payload.verbose:
        common_args.append("--verbose")
    if payload.limit is not None:
        common_args.extend(["--limit", str(int(payload.limit))])

    entities_args = list(common_args)
    if payload.skip_s3:
        entities_args.append("--skip-s3")

    providers_args = list(common_args)
    if payload.skip_s3:
        providers_args.append("--skip-s3")

    links_args = list(common_args)
    if payload.unresolved_only:
        links_args.append("--unresolved-only")
    if payload.refresh_external_sources:
        links_args.append("--refresh-external-sources")
    if payload.resume_run_id:
        links_args.extend(["--resume-run-id", str(payload.resume_run_id)])
    if payload.entity_type:
        links_args.extend(["--entity-type", payload.entity_type])
    if payload.entity_keys:
        for key in payload.entity_keys:
            normalized_key = str(key or "").strip().casefold()
            if normalized_key:
                links_args.extend(["--entity-key", normalized_key])
    if payload.batch_size is not None:
        links_args.extend(["--batch-size", str(int(payload.batch_size))])
    if payload.max_runtime_sec is not None:
        links_args.extend(["--max-runtime-sec", str(int(payload.max_runtime_sec))])
    if payload.skip_s3:
        links_args.append("--skip-s3")

    show_logos_args = list(common_args)
    if payload.skip_s3:
        show_logos_args.append("--skip-s3")

    tmdb_entities, _ = _run_script_step_with_metrics(
        "tmdb_show_entities",
        sync_tmdb_show_entities.main,
        entities_args,
        ["networks_upserted", "production_companies_upserted", "logos_mirrored", "failures"],
    )
    tmdb_watch_providers, _ = _run_script_step_with_metrics(
        "tmdb_watch_providers",
        sync_tmdb_watch_providers.main,
        providers_args,
        ["providers_upserted", "show_watch_providers_upserted", "logos_mirrored", "failures"],
    )
    network_streaming_links, network_streaming_links_output = _run_script_step_with_metrics(
        "network_streaming_links",
        sync_networks_streaming_links.main,
        links_args,
        [
            "processed",
            "links_enriched",
            "wikidata_linked",
            "wikipedia_linked",
            "logos_mirrored",
            "variants_black_mirrored",
            "variants_white_mirrored",
            "logo_assets_discovered",
            "logo_assets_mirrored",
            "logo_assets_skipped",
            "logo_assets_failed",
            "completion_total",
            "completion_resolved",
            "completion_unresolved",
            "completion_unresolved_total",
            "completion_unresolved_network",
            "completion_unresolved_streaming",
            "completion_unresolved_production",
            "production_missing_logos",
            "production_missing_bw_variants",
            "completion_percent",
            "unresolved_logos",
            "failures",
        ],
    )
    show_logos, _ = _run_script_step_with_metrics(
        "show_logos",
        sync_show_logos.main,
        show_logos_args,
        [
            "show_logos_discovered",
            "show_logos_imported",
            "show_logos_skipped",
            "show_logo_failures",
            "failures",
        ],
    )
    brand_family_wikipedia_import = _run_brand_family_wikipedia_import_step(dry_run=payload.dry_run)

    steps = {
        "tmdb_show_entities": tmdb_entities,
        "tmdb_watch_providers": tmdb_watch_providers,
        "network_streaming_links": network_streaming_links,
        "show_logos": show_logos,
        "brand_family_wikipedia_import": brand_family_wikipedia_import,
    }

    entities_synced = tmdb_entities.metrics.get("networks_upserted", 0) + tmdb_entities.metrics.get(
        "production_companies_upserted", 0
    )
    providers_synced = tmdb_watch_providers.metrics.get("providers_upserted", 0)
    links_enriched = network_streaming_links.metrics.get("links_enriched", 0)
    logos_mirrored = (
        tmdb_entities.metrics.get("logos_mirrored", 0)
        + tmdb_watch_providers.metrics.get("logos_mirrored", 0)
        + network_streaming_links.metrics.get("logos_mirrored", 0)
    )
    variants_black_mirrored = network_streaming_links.metrics.get("variants_black_mirrored", 0)
    variants_white_mirrored = network_streaming_links.metrics.get("variants_white_mirrored", 0)
    logo_assets_discovered = network_streaming_links.metrics.get("logo_assets_discovered", 0)
    logo_assets_mirrored = network_streaming_links.metrics.get("logo_assets_mirrored", 0)
    logo_assets_skipped = network_streaming_links.metrics.get("logo_assets_skipped", 0)
    logo_assets_failed = network_streaming_links.metrics.get("logo_assets_failed", 0)
    show_logos_discovered = show_logos.metrics.get("show_logos_discovered", 0)
    show_logos_imported = show_logos.metrics.get("show_logos_imported", 0)
    show_logos_skipped = show_logos.metrics.get("show_logos_skipped", 0)
    show_logo_failures = show_logos.metrics.get("show_logo_failures", 0)
    completion_total = network_streaming_links.metrics.get("completion_total", 0)
    completion_resolved = network_streaming_links.metrics.get("completion_resolved", 0)
    completion_unresolved = network_streaming_links.metrics.get("completion_unresolved", 0)
    completion_unresolved_total = network_streaming_links.metrics.get(
        "completion_unresolved_total",
        completion_unresolved,
    )
    completion_unresolved_network = network_streaming_links.metrics.get("completion_unresolved_network", 0)
    completion_unresolved_streaming = network_streaming_links.metrics.get("completion_unresolved_streaming", 0)
    completion_unresolved_production = network_streaming_links.metrics.get("completion_unresolved_production", 0)
    production_missing_logos = network_streaming_links.metrics.get("production_missing_logos", 0)
    production_missing_bw_variants = network_streaming_links.metrics.get("production_missing_bw_variants", 0)
    completion_percent = _extract_metric_float(network_streaming_links_output, "completion_percent")
    run_id = _extract_metric_text(network_streaming_links_output, "run_id") or "network-streaming-unknown"
    run_status_raw = (_extract_metric_text(network_streaming_links_output, "run_status") or "").lower()
    if network_streaming_links.status == "failed":
        run_status: Literal["completed", "stopped", "failed"] = "failed"
    elif run_status_raw in {"completed", "stopped", "failed"}:
        run_status = run_status_raw  # type: ignore[assignment]
    else:
        run_status = "completed"
    if any(step.status == "failed" for step in steps.values()):
        run_status = "failed"
    cursor_entity_type = (
        _extract_metric_text(network_streaming_links_output, "resume_cursor_entity_type") or ""
    ).lower()
    cursor_entity_key = _extract_metric_text(network_streaming_links_output, "resume_cursor_entity_key") or ""
    resume_cursor = None
    if run_status == "stopped" and cursor_entity_type in {"network", "streaming", "production"} and cursor_entity_key:
        resume_cursor = SyncNetworksStreamingResumeCursor(
            entity_type=cursor_entity_type,  # type: ignore[arg-type]
            entity_key=cursor_entity_key,
        )
    unresolved_logos = _extract_unresolved_logos(network_streaming_links_output)
    unresolved_logos_count = network_streaming_links.metrics.get("unresolved_logos", len(unresolved_logos))
    unresolved_cap = 300
    unresolved_logos_truncated = len(unresolved_logos) > unresolved_cap
    unresolved_payload = unresolved_logos[:unresolved_cap]
    failures = (
        tmdb_entities.metrics.get("failures", 0)
        + tmdb_watch_providers.metrics.get("failures", 0)
        + network_streaming_links.metrics.get("failures", 0)
    )
    completion_gate_passed = completion_unresolved_total == 0 and run_status != "failed"
    if not completion_gate_passed and run_status != "failed":
        failures += 1

    return SyncNetworksStreamingResponse(
        run_id=run_id,
        status=run_status,
        resume_cursor=resume_cursor,
        entities_synced=entities_synced,
        providers_synced=providers_synced,
        links_enriched=links_enriched,
        logos_mirrored=logos_mirrored,
        variants_black_mirrored=variants_black_mirrored,
        variants_white_mirrored=variants_white_mirrored,
        logo_assets_discovered=logo_assets_discovered,
        logo_assets_mirrored=logo_assets_mirrored,
        logo_assets_skipped=logo_assets_skipped,
        logo_assets_failed=logo_assets_failed,
        show_logos_discovered=show_logos_discovered,
        show_logos_imported=show_logos_imported,
        show_logos_skipped=show_logos_skipped,
        show_logo_failures=show_logo_failures,
        completion_total=completion_total,
        completion_resolved=completion_resolved,
        completion_unresolved=completion_unresolved,
        completion_unresolved_total=completion_unresolved_total,
        completion_unresolved_network=completion_unresolved_network,
        completion_unresolved_streaming=completion_unresolved_streaming,
        completion_unresolved_production=completion_unresolved_production,
        production_missing_logos=production_missing_logos,
        production_missing_bw_variants=production_missing_bw_variants,
        completion_percent=completion_percent,
        completion_gate_passed=completion_gate_passed,
        missing_columns=[],
        unresolved_logos_count=unresolved_logos_count,
        unresolved_logos_truncated=unresolved_logos_truncated,
        unresolved_logos=unresolved_payload,
        failures=failures,
        steps=steps,
    )


@router.get("/networks-streaming/overrides", response_model=list[NetworksStreamingOverride])
def list_networks_streaming_overrides(
    entity_type: Literal["network", "streaming", "production"] | None = None,
    active_only: bool = True,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> list[NetworksStreamingOverride]:
    query = db.schema("admin").table("network_streaming_overrides").select("*").order("updated_at", desc=True)
    if entity_type:
        query = query.eq("entity_type", entity_type)
    if active_only:
        query = query.eq("is_active", True)
    response = query.execute()
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to fetch overrides: {response.error}")
    rows = response.data or []
    return [NetworksStreamingOverride(**row) for row in rows if isinstance(row, dict)]


@router.post("/networks-streaming/overrides", response_model=NetworksStreamingOverride)
def create_networks_streaming_override(
    payload: UpsertNetworksStreamingOverrideRequest,
    db: SupabaseAdminClient = None,
    admin: AdminUser = None,
) -> NetworksStreamingOverride:
    raw = payload.model_dump()
    raw["updated_by"] = str(getattr(admin, "email", "") or "")
    patch = _sanitize_override_payload(raw, partial=False)
    if not patch.get("entity_type") or not patch.get("entity_key"):
        raise HTTPException(status_code=400, detail="entity_type and entity_key are required")

    response = (
        db.schema("admin")
        .table("network_streaming_overrides")
        .upsert(
            patch,
            on_conflict="entity_type,entity_key",
        )
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to upsert override: {response.error}")
    rows = response.data or []
    if not rows:
        raise HTTPException(status_code=500, detail="No override row returned")
    return NetworksStreamingOverride(**rows[0])


@router.patch("/networks-streaming/overrides/{override_id}", response_model=NetworksStreamingOverride)
def patch_networks_streaming_override(
    override_id: UUID,
    payload: PatchNetworksStreamingOverrideRequest,
    db: SupabaseAdminClient = None,
    admin: AdminUser = None,
) -> NetworksStreamingOverride:
    patch = _sanitize_override_payload(payload.model_dump(exclude_unset=True), partial=True)
    patch["updated_by"] = str(getattr(admin, "email", "") or "")
    patch["updated_at"] = datetime.now(tz=UTC).isoformat()
    response = (
        db.schema("admin").table("network_streaming_overrides").update(patch).eq("id", str(override_id)).execute()
    )
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to update override: {response.error}")
    rows = response.data or []
    if not rows:
        raise HTTPException(status_code=404, detail=f"Override {override_id} not found")
    return NetworksStreamingOverride(**rows[0])


@router.delete("/networks-streaming/overrides/{override_id}")
def delete_networks_streaming_override(
    override_id: UUID,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> dict[str, str]:
    response = db.schema("admin").table("network_streaming_overrides").delete().eq("id", str(override_id)).execute()
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to delete override: {response.error}")
    if not (response.data or []):
        raise HTTPException(status_code=404, detail=f"Override {override_id} not found")
    return {"status": "deleted", "id": str(override_id)}


@router.post("/logos/import-url", response_model=LogoImportResponse)
def import_logo_url(
    payload: LogoImportUrlRequest,
    db: SupabaseAdminClient = None,
    admin: AdminUser = None,
) -> LogoImportResponse:
    source_url = _text(payload.source_url)
    if not source_url.startswith("http://") and not source_url.startswith("https://"):
        raise HTTPException(status_code=400, detail="source_url must be an absolute http(s) URL")

    admin_email = _text(getattr(admin, "email", "")) or None

    if payload.target_type == "show":
        show_id = str(payload.show_id) if payload.show_id else ""
        if not show_id:
            raise HTTPException(status_code=400, detail="show_id is required for target_type=show")
        show_response = (
            db.schema("core").table("shows").select("id,imdb_id,canonical_slug").eq("id", show_id).limit(1).execute()
        )
        if hasattr(show_response, "error") and show_response.error:
            raise HTTPException(status_code=502, detail=f"Failed to fetch show row: {show_response.error}")
        rows = show_response.data or []
        if not rows:
            raise HTTPException(status_code=404, detail="Show target not found")
        show_row = rows[0]

        try:
            image_bytes, content_type = download_image(source_url, source="manual_logo_import", referer=source_url)
            response = _import_show_logo_bytes(
                db,
                show_row=show_row,
                image_bytes=image_bytes,
                content_type=content_type or "application/octet-stream",
                source_url=source_url,
                set_primary=bool(payload.set_primary),
            )
            _upsert_logo_import_audit(
                db,
                target_type="show",
                target_id=show_id,
                target_key=_text(show_row.get("canonical_slug")) or None,
                source_type="url",
                source_url=source_url,
                uploaded_filename=None,
                hosted_logo_url=response.hosted_logo_url,
                hosted_logo_sha256=None,
                status=response.status,
                failure_reason=None,
                created_by=admin_email,
            )
            return response
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            _upsert_logo_import_audit(
                db,
                target_type="show",
                target_id=show_id,
                target_key=None,
                source_type="url",
                source_url=source_url,
                uploaded_filename=None,
                hosted_logo_url=None,
                hosted_logo_sha256=None,
                status="failed",
                failure_reason=str(exc),
                created_by=admin_email,
            )
            raise HTTPException(status_code=502, detail=f"Logo import failed: {exc}") from exc

    explicit_id = payload.network_id
    if payload.target_type == "streaming":
        explicit_id = payload.provider_id
    if payload.target_type == "production":
        explicit_id = payload.production_company_id

    target_row, config = _resolve_dimension_target(
        db,
        target_type=payload.target_type,  # type: ignore[arg-type]
        explicit_id=explicit_id,
        entity_key=payload.entity_key,
    )
    table = config["table"]
    id_field = config["id_field"]
    name_field = config["name_field"]
    entity_type = config["entity_type"]  # type: ignore[assignment]
    logo_kind = config["logo_kind"]
    entity_id = _text(target_row.get(id_field))
    display_name = _text(target_row.get(name_field))
    entity_key = display_name.casefold()

    try:
        patch = (
            mirror_external_logo_row(
                {
                    id_field: target_row.get(id_field),
                    "hosted_logo_url": None,
                    "hosted_logo_key": None,
                    "hosted_logo_sha256": None,
                },
                kind=logo_kind,
                id_field=id_field,
                source_url=source_url,
                force=bool(payload.set_primary),
                s3_client=get_s3_client(),
                source="override",
            )
            or {}
        )
        mirror_status: Literal["mirrored", "skipped", "failed"] = "mirrored" if patch else "failed"
        failure_reason = None if patch else "logo_decode_failed"

        if payload.set_primary and patch:
            update_patch = dict(patch)
            variant_patch = mirror_logo_monochrome_variants_row(
                {**target_row, **patch},
                kind=logo_kind,
                id_field=id_field,
                source_url=_text(patch.get("hosted_logo_url")) or source_url,
                force=True,
                s3_client=get_s3_client(),
                source="override",
            )
            if variant_patch and isinstance(variant_patch.patch, dict):
                update_patch.update(variant_patch.patch)
            core_update = (
                db.schema("core").table(table).update(update_patch).eq(id_field, target_row.get(id_field)).execute()
            )
            if hasattr(core_update, "error") and core_update.error:
                raise HTTPException(status_code=502, detail=f"Failed to update {table} row: {core_update.error}")

        _upsert_dimension_logo_asset_row(
            db,
            entity_type=entity_type,  # type: ignore[arg-type]
            entity_key=entity_key,
            entity_id=entity_id,
            display_name=display_name,
            source_url=source_url,
            source_rank=0,
            mirror_status=mirror_status,
            failure_reason=failure_reason,
            patch=patch,
            is_primary=bool(payload.set_primary and patch),
        )
        if payload.set_primary and patch:
            _set_dimension_asset_primary_flag(
                db,
                entity_type=entity_type,  # type: ignore[arg-type]
                entity_key=entity_key,
                source_url=source_url,
            )

        status: Literal["imported", "skipped", "failed"] = "imported" if patch else "failed"
        response = LogoImportResponse(
            status=status,
            target_type=payload.target_type,
            target_id=entity_id,
            target_key=entity_key,
            asset_id=None,
            hosted_logo_url=_text(patch.get("hosted_logo_url")) or None,
            set_primary=bool(payload.set_primary),
            message=None if patch else "Unable to mirror source URL",
        )
        _upsert_logo_import_audit(
            db,
            target_type=payload.target_type,
            target_id=entity_id,
            target_key=entity_key,
            source_type="url",
            source_url=source_url,
            uploaded_filename=None,
            hosted_logo_url=response.hosted_logo_url,
            hosted_logo_sha256=_text(patch.get("hosted_logo_sha256")) or None,
            status=response.status,
            failure_reason=response.message,
            created_by=admin_email,
        )
        return response
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        _upsert_logo_import_audit(
            db,
            target_type=payload.target_type,
            target_id=entity_id,
            target_key=entity_key,
            source_type="url",
            source_url=source_url,
            uploaded_filename=None,
            hosted_logo_url=None,
            hosted_logo_sha256=None,
            status="failed",
            failure_reason=str(exc),
            created_by=admin_email,
        )
        raise HTTPException(status_code=502, detail=f"Logo import failed: {exc}") from exc


@router.post("/logos/import-file", response_model=LogoImportResponse)
async def import_logo_file(
    target_type: LogoImportTargetType = Form(...),
    set_primary: bool = Form(False),
    show_id: str | None = Form(None),
    network_id: int | None = Form(None),
    provider_id: int | None = Form(None),
    production_company_id: int | None = Form(None),
    entity_key: str | None = Form(None),
    file: UploadFile = File(...),
    db: SupabaseAdminClient = None,
    admin: AdminUser = None,
) -> LogoImportResponse:
    filename = _text(file.filename) or "upload"
    content_type = _text(file.content_type) or "application/octet-stream"
    if not content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Uploaded file must be an image")

    image_bytes = await file.read()
    if not image_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    admin_email = _text(getattr(admin, "email", "")) or None
    source_url = f"upload://{filename}"

    if target_type == "show":
        if not show_id:
            raise HTTPException(status_code=400, detail="show_id is required for target_type=show")
        show_response = (
            db.schema("core").table("shows").select("id,imdb_id,canonical_slug").eq("id", show_id).limit(1).execute()
        )
        if hasattr(show_response, "error") and show_response.error:
            raise HTTPException(status_code=502, detail=f"Failed to fetch show row: {show_response.error}")
        rows = show_response.data or []
        if not rows:
            raise HTTPException(status_code=404, detail="Show target not found")
        show_row = rows[0]
        response = _import_show_logo_bytes(
            db,
            show_row=show_row,
            image_bytes=image_bytes,
            content_type=content_type,
            source_url=source_url,
            set_primary=set_primary,
        )
        _upsert_logo_import_audit(
            db,
            target_type="show",
            target_id=show_id,
            target_key=_text(show_row.get("canonical_slug")) or None,
            source_type="file",
            source_url=None,
            uploaded_filename=filename,
            hosted_logo_url=response.hosted_logo_url,
            hosted_logo_sha256=None,
            status=response.status,
            failure_reason=response.message,
            created_by=admin_email,
        )
        return response

    explicit_id = network_id
    if target_type == "streaming":
        explicit_id = provider_id
    if target_type == "production":
        explicit_id = production_company_id

    target_row, config = _resolve_dimension_target(
        db,
        target_type=target_type,  # type: ignore[arg-type]
        explicit_id=explicit_id,
        entity_key=entity_key,
    )
    table = config["table"]
    id_field = config["id_field"]
    name_field = config["name_field"]
    entity_type = config["entity_type"]  # type: ignore[assignment]
    logo_kind = config["logo_kind"]
    entity_id = _text(target_row.get(id_field))
    display_name = _text(target_row.get(name_field))
    entity_key_value = display_name.casefold()

    sha256 = hashlib.sha256(image_bytes).hexdigest()
    ext = guess_ext_from_content_type(content_type)
    key = build_logo_s3_key(kind=logo_kind, entity_id=entity_id, sha256=sha256, ext=ext)
    bucket = get_s3_bucket()
    etag, file_size = upload_bytes_to_s3(
        get_s3_client(),
        bucket=bucket,
        key=key,
        data=image_bytes,
        content_type=content_type,
    )
    hosted_url = build_hosted_url(key)
    patch = {
        "hosted_logo_key": key,
        "hosted_logo_url": hosted_url,
        "hosted_logo_sha256": sha256,
        "hosted_logo_content_type": content_type,
        "hosted_logo_bytes": file_size,
        "hosted_logo_etag": etag,
        "hosted_logo_at": datetime.now(tz=UTC).isoformat(),
    }

    if set_primary:
        variant_patch = mirror_logo_monochrome_variants_row(
            {**target_row, **patch},
            kind=logo_kind,
            id_field=id_field,
            source_url=hosted_url,
            force=True,
            s3_client=get_s3_client(),
            source="override",
        )
        if variant_patch and isinstance(variant_patch.patch, dict):
            patch.update(variant_patch.patch)
        core_update = db.schema("core").table(table).update(patch).eq(id_field, target_row.get(id_field)).execute()
        if hasattr(core_update, "error") and core_update.error:
            raise HTTPException(status_code=502, detail=f"Failed to update {table} row: {core_update.error}")

    _upsert_dimension_logo_asset_row(
        db,
        entity_type=entity_type,  # type: ignore[arg-type]
        entity_key=entity_key_value,
        entity_id=entity_id,
        display_name=display_name,
        source_url=source_url,
        source_rank=0,
        mirror_status="mirrored",
        failure_reason=None,
        patch=patch,
        is_primary=bool(set_primary),
    )
    if set_primary:
        _set_dimension_asset_primary_flag(
            db,
            entity_type=entity_type,  # type: ignore[arg-type]
            entity_key=entity_key_value,
            source_url=source_url,
        )

    response = LogoImportResponse(
        status="imported",
        target_type=target_type,
        target_id=entity_id,
        target_key=entity_key_value,
        hosted_logo_url=hosted_url,
        set_primary=set_primary,
    )
    _upsert_logo_import_audit(
        db,
        target_type=target_type,
        target_id=entity_id,
        target_key=entity_key_value,
        source_type="file",
        source_url=None,
        uploaded_filename=filename,
        hosted_logo_url=hosted_url,
        hosted_logo_sha256=sha256,
        status=response.status,
        failure_reason=None,
        created_by=admin_email,
    )
    return response


@router.post("/logos/set-primary", response_model=LogoImportResponse)
def set_logo_primary(
    payload: LogoSetPrimaryRequest,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> LogoImportResponse:
    if payload.target_type == "show":
        show_id = str(payload.show_id) if payload.show_id else ""
        if not show_id:
            raise HTTPException(status_code=400, detail="show_id is required for target_type=show")
        asset_id = _text(payload.asset_id)
        if not asset_id:
            raise HTTPException(status_code=400, detail="asset_id is required")
        _set_show_logo_primary(db, show_id=show_id, media_asset_id=asset_id)
        return LogoImportResponse(
            status="imported",
            target_type="show",
            target_id=show_id,
            asset_id=asset_id,
            set_primary=True,
        )

    target_type = payload.target_type
    if target_type not in {"network", "streaming", "production"}:
        raise HTTPException(status_code=400, detail="Unsupported target_type")

    explicit_id = payload.network_id
    if target_type == "streaming":
        explicit_id = payload.provider_id
    if target_type == "production":
        explicit_id = payload.production_company_id

    target_row, config = _resolve_dimension_target(
        db,
        target_type=target_type,
        explicit_id=explicit_id,
        entity_key=payload.entity_key,
    )
    id_field = config["id_field"]
    name_field = config["name_field"]
    entity_id = _text(target_row.get(id_field))
    entity_key = _text(target_row.get(name_field)).casefold()
    logo_kind = config["logo_kind"]
    table = config["table"]

    asset_response = (
        db.schema("admin")
        .table("network_streaming_logo_assets")
        .select("*")
        .eq("id", _text(payload.asset_id))
        .limit(1)
        .execute()
    )
    if hasattr(asset_response, "error") and asset_response.error:
        raise HTTPException(status_code=502, detail=f"Failed to fetch logo asset row: {asset_response.error}")
    asset_rows = asset_response.data or []
    if not asset_rows:
        raise HTTPException(status_code=404, detail="Logo asset row not found")
    asset_row = asset_rows[0]
    if _text(asset_row.get("entity_type")) != target_type or _text(asset_row.get("entity_key")) != entity_key:
        raise HTTPException(status_code=400, detail="Logo asset does not belong to target entity")

    patch = {
        "hosted_logo_key": asset_row.get("hosted_logo_key"),
        "hosted_logo_url": asset_row.get("hosted_logo_url"),
        "hosted_logo_sha256": asset_row.get("hosted_logo_sha256"),
        "hosted_logo_content_type": asset_row.get("hosted_logo_content_type"),
        "hosted_logo_bytes": asset_row.get("hosted_logo_bytes"),
        "hosted_logo_etag": asset_row.get("hosted_logo_etag"),
        "hosted_logo_at": datetime.now(tz=UTC).isoformat(),
    }
    hosted_logo_url = _text(asset_row.get("hosted_logo_url"))
    if hosted_logo_url:
        variant_patch = mirror_logo_monochrome_variants_row(
            {**target_row, **patch},
            kind=logo_kind,
            id_field=id_field,
            source_url=hosted_logo_url,
            force=False,
            s3_client=get_s3_client(),
            source="override",
        )
        if variant_patch and isinstance(variant_patch.patch, dict):
            patch.update(variant_patch.patch)

    core_update = db.schema("core").table(table).update(patch).eq(id_field, target_row.get(id_field)).execute()
    if hasattr(core_update, "error") and core_update.error:
        raise HTTPException(status_code=502, detail=f"Failed to update {table} row: {core_update.error}")

    _set_dimension_asset_primary_flag(
        db,
        entity_type=target_type,
        entity_key=entity_key,
        source_url=_text(asset_row.get("source_url")),
    )
    return LogoImportResponse(
        status="imported",
        target_type=target_type,
        target_id=entity_id,
        target_key=entity_key,
        hosted_logo_url=hosted_logo_url or None,
        set_primary=True,
    )


ShowRefreshTarget = Literal["details", "seasons_episodes", "photos", "cast_credits"]


class ShowRefreshRequest(BaseModel):
    targets: list[ShowRefreshTarget] = Field(..., min_length=1)
    skip_s3: bool = False
    verbose: bool = False
    reload_schema_cache: bool = False


class RefreshStepResult(BaseModel):
    status: Literal["success", "failed"]
    duration_ms: int
    exit_code: int | None = None
    error: str | None = None


class ShowRefreshResponse(BaseModel):
    show_id: str
    targets: list[ShowRefreshTarget]
    results: dict[str, RefreshStepResult]


class RefreshShowPhotosRequest(BaseModel):
    """High-fidelity gallery refresh with live progress updates."""

    limit_per_source: int = Field(default=50, ge=1, le=200)
    imdb_mediaindex_max_pages: int = Field(default=25, ge=1, le=100)
    imdb_mediaindex_max_images: int | None = Field(default=None, ge=1, le=5000)
    skip_s3: bool = False
    skip_prune: bool = False
    skip_auto_count: bool = False
    skip_word_detection: bool = False
    force_mirror: bool = False
    verbose: bool = False
    season_number: int | None = Field(default=None, ge=0)


class RefreshShowPhotosResponse(BaseModel):
    show_id: str
    show_name: str | None = None
    sources_used: list[str]
    show_images_upserted: int = 0
    show_images_mirrored: int = 0
    season_images_upserted: int = 0
    season_images_mirrored: int = 0
    episode_images_upserted: int = 0
    episode_images_mirrored: int = 0
    cast_photos_fetched: int = 0
    cast_photos_upserted: int = 0
    cast_photos_mirrored: int = 0
    cast_photos_failed: int = 0
    cast_photos_pruned: int = 0
    auto_counts_attempted: int = 0
    auto_counts_succeeded: int = 0
    auto_counts_failed: int = 0
    text_overlay_attempted: int = 0
    text_overlay_succeeded: int = 0
    text_overlay_failed: int = 0
    duration_ms: int
    errors: list[str] = Field(default_factory=list)


def _run_script_step(
    name: str,
    fn: Callable[[list[str] | None], int],
    argv: list[str],
) -> RefreshStepResult:
    started = time.perf_counter()
    try:
        code = fn(list(argv))
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((time.perf_counter() - started) * 1000)
        logger.exception("admin show refresh step failed: %s", name)
        return RefreshStepResult(status="failed", duration_ms=duration_ms, error=str(exc))

    duration_ms = int((time.perf_counter() - started) * 1000)
    if int(code) == 0:
        return RefreshStepResult(status="success", duration_ms=duration_ms, exit_code=0)
    return RefreshStepResult(
        status="failed",
        duration_ms=duration_ms,
        exit_code=int(code),
        error=f"non-zero exit code: {code}",
    )


def _combine_step_results(results: list[tuple[str, RefreshStepResult]]) -> RefreshStepResult:
    total_ms = sum(r.duration_ms for _, r in results)
    failures = [(name, r) for name, r in results if r.status != "success"]
    if not failures:
        return RefreshStepResult(status="success", duration_ms=total_ms, exit_code=0)

    first = failures[0][1]
    parts = []
    for step_name, step_result in failures:
        msg = step_result.error or "failed"
        parts.append(f"{step_name}: {msg}")
    return RefreshStepResult(
        status="failed",
        duration_ms=total_ms,
        exit_code=first.exit_code,
        error="; ".join(parts),
    )


def _resolve_show_imdb_id(show_row: dict[str, Any] | None) -> str | None:
    if not isinstance(show_row, dict):
        return None
    external_ids = show_row.get("external_ids") if isinstance(show_row.get("external_ids"), dict) else {}
    return str(show_row.get("imdb_id") or external_ids.get("imdb_id") or external_ids.get("imdb") or "").strip() or None


def _ensure_cast_refresh_imdb_id(show_row: dict[str, Any] | None, show_id: str) -> None:
    if _resolve_show_imdb_id(show_row):
        return
    raise HTTPException(
        status_code=409,
        detail=(
            f"Show {show_id} is missing an IMDb ID. "
            "Cast refresh requires imdb_id or external_ids.imdb/external_ids.imdb_id."
        ),
    )


@router.post("/{show_id}/refresh", response_model=ShowRefreshResponse)
def refresh_show(
    show_id: UUID,
    payload: ShowRefreshRequest,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> ShowRefreshResponse:
    """
    Refresh a single show for one or more target areas by invoking existing sync scripts.
    """

    show_id_str = str(show_id)
    # Preflight: ensure show exists
    show_resp = (
        db.schema("core").table("shows").select("id,imdb_id,external_ids").eq("id", show_id_str).limit(1).execute()
    )
    if hasattr(show_resp, "error") and show_resp.error:
        raise HTTPException(status_code=502, detail="Database error fetching show")
    if not show_resp.data:
        raise HTTPException(status_code=404, detail=f"Show {show_id_str} not found")
    show_row = show_resp.data[0] or {}

    # De-dupe targets while preserving order
    ordered: list[ShowRefreshTarget] = []
    seen: set[str] = set()
    for target in payload.targets:
        if target in seen:
            continue
        seen.add(target)
        ordered.append(target)

    if "cast_credits" in ordered:
        _ensure_cast_refresh_imdb_id(show_row, show_id_str)

    results: dict[str, RefreshStepResult] = {}

    for target in ordered:
        if target == "details":
            argv = ["--show-id", show_id_str, "--force"]
            if payload.skip_s3:
                argv.append("--skip-s3")
            if payload.verbose:
                argv.append("--verbose")
            # Default to no schema cache reload unless explicitly requested.
            argv.append("--reload-schema-cache" if payload.reload_schema_cache else "--no-reload-schema-cache")
            step = _run_script_step("details", sync_shows_all.main, argv)
            results["details"] = step
            continue

        if target == "seasons_episodes":
            argv = ["--show-id", show_id_str, "--force"]
            if payload.verbose:
                argv.append("--verbose")
            step = _run_script_step("seasons_episodes", sync_seasons_episodes.main, argv)
            results["seasons_episodes"] = step
            continue

        if target == "photos":
            step_results: list[tuple[str, RefreshStepResult]] = []

            argv = ["--show-id", show_id_str, "--force"]
            if payload.skip_s3:
                argv.append("--no-s3")
            if payload.verbose:
                argv.append("--verbose")
            show_images = _run_script_step("photos_show_images", sync_show_images.main, argv)
            step_results.append(("show_images", show_images))
            results["photos_show_images"] = show_images

            argv2 = ["--show-id", show_id_str, "--force"]
            if payload.skip_s3:
                argv2.append("--no-s3")
            if payload.verbose:
                argv2.append("--verbose")
            season_images = _run_script_step("photos_season_episode_images", sync_season_episode_images.main, argv2)
            step_results.append(("season_episode_images", season_images))
            results["photos_season_episode_images"] = season_images

            results["photos"] = _combine_step_results(step_results)
            continue

        if target == "cast_credits":
            step_results = []

            argv = ["--show-id", show_id_str, "--force"]
            if payload.verbose:
                argv.append("--verbose")
            show_cast = _run_script_step("cast_credits_show_cast", sync_show_cast.main, argv)
            step_results.append(("show_cast", show_cast))
            results["cast_credits_show_cast"] = show_cast

            argv2 = ["--show-id", show_id_str, "--force"]
            if payload.verbose:
                argv2.append("--verbose")
            occurrences = _run_script_step(
                "cast_credits_episode_appearances",
                sync_episode_appearances.main,
                argv2,
            )
            step_results.append(("episode_appearances", occurrences))
            results["cast_credits_episode_appearances"] = occurrences

            results["cast_credits"] = _combine_step_results(step_results)
            continue

        raise HTTPException(status_code=400, detail=f"Unknown refresh target: {target}")

    return ShowRefreshResponse(show_id=show_id_str, targets=ordered, results=results)


@router.post("/{show_id}/refresh/stream")
def refresh_show_stream(
    show_id: UUID,
    payload: ShowRefreshRequest,
    request: Request,
    db: SupabaseAdminClient = None,
    admin: AdminUser = None,
) -> StreamingResponse:
    """
    Stream refresh progress for one or more targets as SSE.

    Progress is step-based and accurate (current/total). This is intended for UI progress bars.
    """

    show_id_str = str(show_id)
    request_id = str(request.headers.get("x-trr-request-id") or "").strip() or None
    show_resp = (
        db.schema("core").table("shows").select("id,imdb_id,external_ids").eq("id", show_id_str).limit(1).execute()
    )
    if hasattr(show_resp, "error") and show_resp.error:
        raise HTTPException(status_code=502, detail="Database error fetching show")
    if not show_resp.data:
        raise HTTPException(status_code=404, detail=f"Show {show_id_str} not found")
    show_row = show_resp.data[0] or {}

    ordered: list[ShowRefreshTarget] = []
    seen: set[str] = set()
    for target in payload.targets:
        if target in seen:
            continue
        seen.add(target)
        ordered.append(target)

    if "cast_credits" in ordered:
        _ensure_cast_refresh_imdb_id(show_row, show_id_str)

    # Expand targets into concrete steps so the progress bar can update while work runs.
    # Keys are stored in results to match the non-stream endpoint's structure where possible.
    steps: list[tuple[str, str, Callable[[list[str] | None], int], list[str], str | None, str | None]] = []
    for target in ordered:
        if target == "details":
            common = ["--show-id", show_id_str, "--force"]
            if payload.verbose:
                common.append("--verbose")

            steps.append(("details", "details_sync_shows", sync_shows.main, list(common), "shows", "mixed"))

            entity_args = list(common)
            if payload.skip_s3:
                entity_args.append("--skip-s3")
            steps.append(
                ("details", "details_tmdb_show_entities", sync_tmdb_show_entities.main, entity_args, "shows", "tmdb")
            )

            watch_args = list(common)
            if payload.skip_s3:
                watch_args.append("--skip-s3")
            steps.append(
                (
                    "details",
                    "details_tmdb_watch_providers",
                    sync_tmdb_watch_providers.main,
                    watch_args,
                    "shows",
                    "tmdb",
                )
            )
            continue

        if target == "seasons_episodes":
            common = ["--show-id", show_id_str, "--force"]
            if payload.verbose:
                common.append("--verbose")
            steps.append(
                ("seasons_episodes", "seasons_episodes_episodes", sync_episodes.main, list(common), "episodes", "imdb")
            )
            steps.append(
                ("seasons_episodes", "seasons_episodes_seasons", sync_seasons.main, list(common), "seasons", "tmdb")
            )
            continue

        if target == "photos":
            argv = ["--show-id", show_id_str, "--force"]
            if payload.skip_s3:
                argv.append("--no-s3")
            if payload.verbose:
                argv.append("--verbose")
            steps.append(("photos", "photos_show_images", sync_show_images.main, list(argv), "media", "mixed"))

            argv2 = ["--show-id", show_id_str, "--force"]
            if payload.skip_s3:
                argv2.append("--no-s3")
            if payload.verbose:
                argv2.append("--verbose")
            steps.append(
                (
                    "photos",
                    "photos_season_episode_images",
                    sync_season_episode_images.main,
                    argv2,
                    "media",
                    "tmdb",
                )
            )
            continue

        if target == "cast_credits":
            argv = ["--show-id", show_id_str, "--force"]
            if payload.verbose:
                argv.append("--verbose")
            steps.append(("cast_credits", "cast_credits_show_cast", sync_show_cast.main, list(argv), "people", "imdb"))

            argv2 = ["--show-id", show_id_str, "--force"]
            if payload.verbose:
                argv2.append("--verbose")
            steps.append(
                (
                    "cast_credits",
                    "cast_credits_episode_appearances",
                    sync_episode_appearances.main,
                    argv2,
                    "episodes",
                    "imdb",
                )
            )
            continue

        raise HTTPException(status_code=400, detail=f"Unknown refresh target: {target}")

    total_steps = len(steps)
    actor = str((admin or {}).get("email") or (admin or {}).get("id") or "admin")
    request_payload = {
        "show_id": show_id_str,
        "payload": payload.model_dump(mode="json"),
        "request_id": request_id,
        "initiated_by": actor,
    }

    def _yield_event(event: str, data: dict) -> str:
        return f"event: {event}\ndata: {json.dumps(data)}\n\n"

    def _with_request_id(data: dict[str, object]) -> dict[str, object]:
        if request_id:
            data["request_id"] = request_id
        return data

    def event_generator():
        results: dict[str, RefreshStepResult] = {}
        current = 0
        current_target: str | None = None
        current_step_key: str | None = None

        try:
            yield _yield_event(
                "progress",
                _with_request_id(
                    {
                        "show_id": show_id_str,
                        "current": current,
                        "total": total_steps,
                        "message": "Starting refresh...",
                    }
                ),
            )

            # Run expanded steps sequentially.
            for target, step_key, fn, argv, topic, provider in steps:
                current_target = target
                current_step_key = step_key
                step_started_at = time.perf_counter()
                yield _yield_event(
                    "progress",
                    _with_request_id(
                        {
                            "show_id": show_id_str,
                            "target": target,
                            "step": step_key,
                            "stage_key": step_key,
                            "current": current,
                            "total": total_steps,
                            "step_status": "running",
                            "message": f"{step_key.replace('_', ' ')}: running",
                            "elapsed_ms": 0,
                        }
                    ),
                )

                step_result_holder: dict[str, RefreshStepResult | None] = {"result": None}
                step_error_holder: dict[str, Exception | None] = {"error": None}

                def _run_step_in_thread(
                    result_holder: dict[str, RefreshStepResult | None],
                    error_holder: dict[str, Exception | None],
                    step_key_for_thread: str,
                    fn_for_thread: Callable[[list[str] | None], int],
                    argv_for_thread: list[str],
                ) -> None:
                    try:
                        result_holder["result"] = _run_script_step(
                            step_key_for_thread,
                            fn_for_thread,
                            argv_for_thread,
                        )
                    except Exception as exc:  # noqa: BLE001
                        error_holder["error"] = exc

                step_thread = Thread(
                    target=_run_step_in_thread,
                    args=(
                        step_result_holder,
                        step_error_holder,
                        step_key,
                        fn,
                        list(argv),
                    ),
                    daemon=True,
                )
                step_thread.start()
                while step_thread.is_alive():
                    step_thread.join(timeout=STREAM_HEARTBEAT_INTERVAL_SECONDS)
                    if step_thread.is_alive():
                        yield _yield_event(
                            "progress",
                            _with_request_id(
                                {
                                    "show_id": show_id_str,
                                    "target": target,
                                    "step": step_key,
                                    "stage_key": step_key,
                                    "current": current,
                                    "total": total_steps,
                                    "step_status": "running",
                                    "message": f"{step_key.replace('_', ' ')}: running",
                                    "heartbeat": True,
                                    "elapsed_ms": int((time.perf_counter() - step_started_at) * 1000),
                                }
                            ),
                        )
                step_thread.join()
                if step_error_holder["error"] is not None:
                    raise RuntimeError(f"Step {step_key} failed: {step_error_holder['error']}") from step_error_holder[
                        "error"
                    ]
                step_result = step_result_holder["result"]
                if step_result is None:
                    raise RuntimeError(f"Step {step_key} returned no result")

                results[step_key] = step_result
                current += 1

                payload_data: dict[str, object] = {
                    "show_id": show_id_str,
                    "target": target,
                    "step": step_key,
                    "stage_key": step_key,
                    "current": current,
                    "total": total_steps,
                    "step_status": step_result.status,
                    "message": f"{step_key.replace('_', ' ')}: {step_result.status}",
                    "elapsed_ms": int((time.perf_counter() - step_started_at) * 1000),
                }
                if topic:
                    payload_data["topic"] = topic
                if provider:
                    payload_data["provider"] = provider
                if step_result.error:
                    payload_data["error"] = step_result.error

                yield _yield_event("progress", _with_request_id(payload_data))

            # Combine step results into target-level results to match the non-stream endpoint's shape.
            for target in ordered:
                if target == "details":
                    results["details"] = _combine_step_results(
                        [
                            ("sync_shows", results["details_sync_shows"]),
                            ("tmdb_show_entities", results["details_tmdb_show_entities"]),
                            ("tmdb_watch_providers", results["details_tmdb_watch_providers"]),
                        ]
                    )
                    continue

                if target == "seasons_episodes":
                    results["seasons_episodes"] = _combine_step_results(
                        [
                            ("seasons", results["seasons_episodes_seasons"]),
                            ("episodes", results["seasons_episodes_episodes"]),
                        ]
                    )
                    continue

                if target == "photos":
                    results["photos"] = _combine_step_results(
                        [
                            ("show_images", results["photos_show_images"]),
                            ("season_episode_images", results["photos_season_episode_images"]),
                        ]
                    )
                    continue

                if target == "cast_credits":
                    results["cast_credits"] = _combine_step_results(
                        [
                            ("show_cast", results["cast_credits_show_cast"]),
                            ("episode_appearances", results["cast_credits_episode_appearances"]),
                        ]
                    )
                    continue

            out = ShowRefreshResponse(show_id=show_id_str, targets=ordered, results=results)
            yield _yield_event("complete", _with_request_id(out.model_dump()))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Show refresh stream failed for show_id=%s", show_id_str)
            error_payload: dict[str, object] = {
                "show_id": show_id_str,
                "error": "Show refresh stream failed",
                "detail": str(exc),
                "current": current,
                "total": total_steps,
            }
            if current_target:
                error_payload["target"] = current_target
            if current_step_key:
                error_payload["step"] = current_step_key
                error_payload["stage_key"] = current_step_key
            yield _yield_event("error", _with_request_id(error_payload))

    operation = start_operation_for_stream(
        operation_type="admin_show_refresh",
        producer=event_generator,
        request_payload=request_payload,
        initiated_by=actor,
        request=request,
    )
    return operation_stream_response(str(operation.get("id")), request=request)


def build_show_refresh_operation_producer(
    *,
    request_payload: dict[str, Any],
    db: SupabaseAdminClient | None = None,
):
    from trr_backend.db.admin import create_supabase_admin_client

    show_id_str = str(request_payload.get("show_id") or "").strip()
    if not show_id_str:
        raise ValueError("request_payload.show_id is required")
    payload_data = request_payload.get("payload") if isinstance(request_payload.get("payload"), dict) else {}
    payload = ShowRefreshRequest.model_validate(payload_data)
    initiated_by = str(request_payload.get("initiated_by") or "admin")
    request_id = str(request_payload.get("request_id") or "").strip() or None

    def _producer():
        local_db = db or create_supabase_admin_client()
        start_payload: dict[str, Any] = {
            "show_id": show_id_str,
            "stage": "starting",
            "message": "Starting refresh...",
            "actor": initiated_by,
        }
        if request_id:
            start_payload["request_id"] = request_id
        yield f"event: progress\ndata: {json.dumps(start_payload)}\n\n"
        try:
            result = refresh_show(
                show_id=UUID(show_id_str),
                payload=payload,
                db=local_db,
                _={"id": initiated_by},
            )
            complete_payload = result.model_dump() if isinstance(result, BaseModel) else dict(result or {})
            if request_id:
                complete_payload["request_id"] = request_id
            yield f"event: complete\ndata: {json.dumps(complete_payload)}\n\n"
        except HTTPException as exc:
            error_payload: dict[str, Any] = {
                "show_id": show_id_str,
                "error": "Show refresh failed",
                "detail": str(exc.detail),
                "status": int(exc.status_code),
            }
            if request_id:
                error_payload["request_id"] = request_id
            yield f"event: error\ndata: {json.dumps(error_payload)}\n\n"
        except Exception as exc:  # noqa: BLE001
            error_payload = {
                "show_id": show_id_str,
                "error": "Show refresh failed",
                "detail": str(exc),
            }
            if request_id:
                error_payload["request_id"] = request_id
            yield f"event: error\ndata: {json.dumps(error_payload)}\n\n"

    return _producer


@router.post("/{show_id}/refresh-photos/stream")
def refresh_show_photos_stream(
    show_id: UUID,
    request: Request,
    payload: RefreshShowPhotosRequest | None = None,
    db: SupabaseAdminClient = None,
    admin_user: AdminUser = None,
) -> StreamingResponse:
    """
    High-fidelity gallery refresh for a show with live SSE progress updates.

    Includes:
    - Show images (TMDb + IMDb section-images + IMDb mediaindex gallery)
    - Season posters + episode stills (TMDb)
    - Cast photos (IMDb + TMDb + Fandom + Fandom gallery)
    - S3 mirroring for all of the above (unless skip_s3)
    - Auto-count people (cast photos; best-effort)
    - Text overlay detection (cast photos; best-effort)
    """

    payload = payload or RefreshShowPhotosRequest()
    show_id_str = str(show_id)
    request_id = str(request.headers.get("x-trr-request-id") or "").strip() or None

    show_resp = (
        db.schema("core")
        .table("shows")
        .select("id,name,imdb_id,tmdb_id,external_ids")
        .eq("id", show_id_str)
        .limit(1)
        .execute()
    )
    if hasattr(show_resp, "error") and show_resp.error:
        raise HTTPException(status_code=502, detail="Database error fetching show")
    if not show_resp.data:
        raise HTTPException(status_code=404, detail=f"Show {show_id_str} not found")
    show_row = show_resp.data[0] or {}
    show_name = str(show_row.get("name") or "").strip() or None
    external_ids = show_row.get("external_ids") if isinstance(show_row.get("external_ids"), dict) else {}
    show_imdb_id = (
        str(show_row.get("imdb_id") or external_ids.get("imdb_id") or external_ids.get("imdb") or "").strip() or None
    )
    show_tmdb_id = show_row.get("tmdb_id")
    if not isinstance(show_tmdb_id, int):
        # Some schemas store TMDb ID in external_ids.
        try:
            show_tmdb_id = int(external_ids.get("tmdb_id") or external_ids.get("tmdb") or 0) or None
        except Exception:
            show_tmdb_id = None

    def _yield_event(event: str, data: dict) -> str:
        return f"event: {event}\ndata: {json.dumps(data)}\n\n"

    def _with_request_id(data: dict[str, object]) -> dict[str, object]:
        if request_id:
            data["request_id"] = request_id
        return data

    def event_generator():
        from uuid import UUID as _UUID

        from trr_backend.clients.screenalytics import (
            ScreenalyticsClientError,
            ScreenalyticsUnavailableError,
            count_people,
            get_screenalytics_unavailable_state,
            is_screenalytics_configured,
        )
        from trr_backend.ingestion.cast_photo_sources import (
            fetch_fandom_gallery_cast_photos,
            fetch_fandom_person_cast_photos,
            fetch_imdb_cast_photos,
            fetch_tmdb_cast_photos,
        )
        from trr_backend.ingestion.imdb_show_mediaindex import fetch_imdb_show_mediaindex_rows
        from trr_backend.ingestion.show_metadata_enricher import enrich_shows_after_upsert
        from trr_backend.integrations.tmdb.client import (
            TmdbClientError,
            fetch_tv_episode_images,
            fetch_tv_season_images,
            resolve_api_key,
        )
        from trr_backend.media.s3_mirror import (
            get_cdn_base_url,
            get_s3_client,
            mirror_cast_photo_row,
            mirror_episode_image_row,
            mirror_season_image_row,
            mirror_show_image_row,
            prune_orphaned_cast_photo_objects,
            prune_orphaned_show_image_objects,
        )
        from trr_backend.models.shows import ShowRecord
        from trr_backend.repositories.cast_photo_tags import (
            get_tags_by_photo_ids,
            has_manual_tags,
            upsert_cast_photo_tags,
        )
        from trr_backend.repositories.cast_photos import (
            fetch_cast_photos_missing_hosted,
            update_cast_photo_hosted_fields,
            upsert_cast_photos,
        )
        from trr_backend.repositories.cast_tmdb import get_cast_tmdb_by_person_id
        from trr_backend.repositories.episode_images import (
            assert_core_episode_images_table_exists,
            fetch_episode_images_missing_hosted,
            update_episode_image_hosted_fields,
            upsert_episode_images,
        )
        from trr_backend.repositories.season_images import (
            assert_core_season_images_table_exists,
            fetch_season_images_missing_hosted,
            update_season_image_hosted_fields,
            upsert_season_images,
        )
        from trr_backend.repositories.show_images import (
            assert_core_show_images_table_exists,
            fetch_show_images_missing_hosted,
            update_show_image_hosted_fields,
            upsert_show_images,
        )

        errors: list[str] = []
        started = time.perf_counter()

        # Global progress is dynamic: we increment total as we discover work to do.
        current = 0
        total = 0
        show_images_upserted = 0
        show_images_mirrored = 0
        season_images_upserted = 0
        season_images_mirrored = 0
        episode_images_upserted = 0
        episode_images_mirrored = 0
        cast_photos_upserted = 0
        cast_photos_mirrored = 0
        auto_counts_succeeded = 0
        text_overlay_succeeded = 0

        def build_live_counts() -> dict[str, int]:
            return {
                "synced": int(
                    show_images_upserted + season_images_upserted + episode_images_upserted + cast_photos_upserted
                ),
                "mirrored": int(
                    show_images_mirrored + season_images_mirrored + episode_images_mirrored + cast_photos_mirrored
                ),
                "counted": int(auto_counts_succeeded),
                "cropped": 0,
                "id_text": int(text_overlay_succeeded),
                "resized": 0,
            }

        def bump_total(amount: int) -> None:
            nonlocal total
            total += max(0, int(amount))

        def bump_current(amount: int = 1) -> None:
            nonlocal current
            current += max(0, int(amount))

        def progress(
            *,
            stage: str,
            message: str | None = None,
            stage_current: int | None = None,
            stage_total: int | None = None,
            extra: dict | None = None,
        ):
            data: dict[str, object] = {
                "show_id": show_id_str,
                "stage": stage,
                "message": message,
                "current": current,
                "total": total,
                "live_counts": build_live_counts(),
            }
            if stage_current is not None:
                data["stage_current"] = stage_current
            if stage_total is not None:
                data["stage_total"] = stage_total
            if extra:
                data.update(extra)
            return _yield_event("progress", _with_request_id(data))

        yield progress(stage="starting", message="Starting refresh...")

        sources_used: set[str] = set()
        season_scope = int(payload.season_number) if isinstance(payload.season_number, int) else None

        # ------------------------------------------------------------------
        # Stage 1: Show images (TMDb + IMDb)
        # ------------------------------------------------------------------
        if season_scope is None:
            assert_core_show_images_table_exists(db)
            yield progress(stage="sync_show_images", message="Syncing TMDb/IMDb show images...")
            try:
                record = ShowRecord(
                    id=_UUID(show_id_str),
                    name=str(show_name or ""),
                    description=None,
                    premiere_date=None,
                    imdb_id=str(show_imdb_id) if show_imdb_id else None,
                    tmdb_id=int(show_tmdb_id) if isinstance(show_tmdb_id, int) else None,
                )
                summary = enrich_shows_after_upsert([record], force_refresh=True, dry_run=False)
                for patch in summary.patches:
                    if patch.show_images_rows:
                        upsert_show_images(db, patch.show_images_rows)
                        show_images_upserted += len(patch.show_images_rows)
                        sources_used.update(["tmdb", "imdb"])
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Show images enrich: {exc}")

            if show_imdb_id:
                yield progress(stage="sync_imdb_mediaindex", message="Syncing IMDb mediaindex gallery...")
                try:
                    rows = fetch_imdb_show_mediaindex_rows(
                        show_imdb_id,
                        show_id=show_id_str,
                        max_pages=int(payload.imdb_mediaindex_max_pages or 25),
                        max_images=(
                            int(payload.imdb_mediaindex_max_images)
                            if isinstance(payload.imdb_mediaindex_max_images, int)
                            else None
                        ),
                        sleep_ms=0,
                        include_tags=True,
                    )
                    if rows:
                        _enrich_imdb_mediaindex_rows_with_episode_context(
                            db,
                            show_id=show_id_str,
                            show_name=show_name,
                            show_imdb_id=show_imdb_id,
                            rows=rows,
                        )
                        upsert_show_images(db, rows)
                        show_images_upserted += len(rows)
                        sources_used.add("imdb")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"IMDb mediaindex: {exc}")

            bump_total(1)
            bump_current(1)
            yield progress(stage="sync_show_images", message="Show images synced.")
        else:
            yield progress(
                stage="sync_show_images",
                message=f"Skipping show-level images for season-scoped refresh (season {season_scope}).",
            )

        # ------------------------------------------------------------------
        # Stage 2: Season posters + episode stills (TMDb)
        # ------------------------------------------------------------------
        try:
            assert_core_season_images_table_exists(db)
        except Exception as exc:
            errors.append(f"Season images table missing: {exc}")

        episode_images_enabled = True
        try:
            assert_core_episode_images_table_exists(db)
        except Exception:
            episode_images_enabled = False

        if isinstance(show_tmdb_id, int) and show_tmdb_id > 0 and resolve_api_key():
            yield progress(stage="sync_tmdb_seasons", message="Syncing TMDb season/episode images...")
            seasons_query = db.schema("core").table("seasons").select("id,season_number").eq("show_id", show_id_str)
            if season_scope is not None:
                seasons_query = seasons_query.eq("season_number", season_scope)
            seasons_resp = seasons_query.order("season_number").execute()
            seasons = seasons_resp.data or []
            if not isinstance(seasons, list):
                seasons = []

            season_stage_total = len(seasons)
            season_stage_current = 0
            if season_stage_total:
                bump_total(season_stage_total)

            for season in seasons:
                season_stage_current += 1
                season_number = season.get("season_number")
                season_id_val = season.get("id")
                if not isinstance(season_number, int) or not season_id_val:
                    bump_current(1)
                    continue

                try:
                    season_payload = fetch_tv_season_images(int(show_tmdb_id), int(season_number))
                    posters = season_payload.get("posters") if isinstance(season_payload, dict) else None
                    rows: list[dict] = []
                    if isinstance(posters, list):
                        fetched_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                        shared_metadata = {"image_roles": ["poster"], "season_backdrop": False}
                        for poster in posters:
                            if not isinstance(poster, dict):
                                continue
                            file_path = poster.get("file_path")
                            width = poster.get("width")
                            height = poster.get("height")
                            if not isinstance(file_path, str) or not file_path.strip():
                                continue
                            if not isinstance(width, int) or not isinstance(height, int):
                                continue
                            aspect_ratio = poster.get("aspect_ratio")
                            try:
                                if isinstance(aspect_ratio, (int, float)):
                                    aspect_ratio_val = float(aspect_ratio)
                                else:
                                    aspect_ratio_val = float(width) / float(height) if height else 0.0
                            except Exception:
                                aspect_ratio_val = 0.0
                            iso_639_1 = poster.get("iso_639_1") if isinstance(poster.get("iso_639_1"), str) else None
                            rows.append(
                                {
                                    "show_id": show_id_str,
                                    "season_id": str(season_id_val),
                                    "tmdb_series_id": int(show_tmdb_id),
                                    "season_number": int(season_number),
                                    "source": "tmdb",
                                    "kind": "poster",
                                    "iso_639_1": iso_639_1,
                                    "file_path": file_path,
                                    "source_image_id": file_path,
                                    "url": f"https://image.tmdb.org/t/p/original{file_path}",
                                    "url_original": f"https://image.tmdb.org/t/p/original{file_path}",
                                    "width": int(width),
                                    "height": int(height),
                                    "aspect_ratio": aspect_ratio_val,
                                    "fetched_at": fetched_at,
                                    "metadata": dict(shared_metadata),
                                }
                            )
                    if rows:
                        upsert_season_images(db, rows)
                        season_images_upserted += len(rows)
                        sources_used.add("tmdb")
                except TmdbClientError as exc:
                    errors.append(f"TMDb season images s{season_number}: {exc}")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"TMDb season images s{season_number}: {exc}")

                if episode_images_enabled:
                    episodes_resp = (
                        db.schema("core")
                        .table("episodes")
                        .select("id,episode_number")
                        .eq("season_id", str(season_id_val))
                        .order("episode_number")
                        .execute()
                    )
                    episodes = episodes_resp.data or []
                    if not isinstance(episodes, list):
                        episodes = []
                    # Episode stills: count each episode as a unit of work (we don't know still count until fetched).
                    bump_total(len(episodes))
                    ep_idx = 0
                    for ep in episodes:
                        ep_idx += 1
                        ep_number = ep.get("episode_number")
                        ep_id_val = ep.get("id")
                        if not isinstance(ep_number, int) or not ep_id_val:
                            bump_current(1)
                            continue
                        try:
                            ep_payload = fetch_tv_episode_images(int(show_tmdb_id), int(season_number), int(ep_number))
                            stills = ep_payload.get("stills") if isinstance(ep_payload, dict) else None
                            still_rows: list[dict] = []
                            if isinstance(stills, list):
                                fetched_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                                for still in stills:
                                    if not isinstance(still, dict):
                                        continue
                                    file_path = still.get("file_path")
                                    width = still.get("width")
                                    height = still.get("height")
                                    if not isinstance(file_path, str) or not file_path.strip():
                                        continue
                                    if not isinstance(width, int) or not isinstance(height, int):
                                        continue
                                    aspect_ratio = still.get("aspect_ratio")
                                    try:
                                        if isinstance(aspect_ratio, (int, float)):
                                            aspect_ratio_val = float(aspect_ratio)
                                        else:
                                            aspect_ratio_val = float(width) / float(height) if height else 0.0
                                    except Exception:
                                        aspect_ratio_val = 0.0
                                    iso_639_1 = (
                                        still.get("iso_639_1") if isinstance(still.get("iso_639_1"), str) else None
                                    )
                                    still_rows.append(
                                        {
                                            "show_id": show_id_str,
                                            "season_id": str(season_id_val),
                                            "episode_id": str(ep_id_val),
                                            "tmdb_series_id": int(show_tmdb_id),
                                            "season_number": int(season_number),
                                            "episode_number": int(ep_number),
                                            "source": "tmdb",
                                            "kind": "still",
                                            "iso_639_1": iso_639_1,
                                            "file_path": file_path,
                                            "source_image_id": file_path,
                                            "url": f"https://image.tmdb.org/t/p/original{file_path}",
                                            "url_original": f"https://image.tmdb.org/t/p/original{file_path}",
                                            "width": int(width),
                                            "height": int(height),
                                            "aspect_ratio": aspect_ratio_val,
                                            "fetched_at": fetched_at,
                                        }
                                    )
                            if still_rows:
                                upsert_episode_images(db, still_rows)
                                episode_images_upserted += len(still_rows)
                                sources_used.add("tmdb")
                        except Exception as exc:  # noqa: BLE001
                            errors.append(f"TMDb episode images s{season_number}e{ep_number}: {exc}")
                        bump_current(1)
                        if ep_idx % 10 == 0:
                            yield progress(
                                stage="sync_tmdb_episodes",
                                message=f"Syncing TMDb episode stills (S{season_number})...",
                                stage_current=ep_idx,
                                stage_total=len(episodes),
                            )

                bump_current(1)
                yield progress(
                    stage="sync_tmdb_seasons",
                    message="Syncing TMDb season/episode images...",
                    stage_current=season_stage_current,
                    stage_total=season_stage_total,
                )
        else:
            yield progress(
                stage="sync_tmdb_seasons",
                message="Skipping TMDb season/episode images (missing TMDb ID or TMDB_API_KEY).",
            )

        # ------------------------------------------------------------------
        # Stage 3: Mirror legacy show/season/episode images to S3
        # ------------------------------------------------------------------
        show_images_mirror_failed = 0
        season_images_mirror_failed = 0
        episode_images_mirror_failed = 0

        if not payload.skip_s3:
            s3_client = get_s3_client()
            cdn_base_url = None if payload.force_mirror else get_cdn_base_url()

            # 3.1 show_images
            if season_scope is None:
                try:
                    rows = fetch_show_images_missing_hosted(
                        db,
                        source="all",
                        show_id=show_id_str,
                        kind=None,
                        limit=None,
                        include_hosted=bool(payload.force_mirror),
                        cdn_base_url=cdn_base_url,
                    )
                except Exception as exc:  # noqa: BLE001
                    rows = []
                    errors.append(f"Mirror show_images list: {exc}")

                bump_total(len(rows))
                yield progress(
                    stage="mirror_show_images",
                    message="Mirroring show images to S3...",
                    stage_total=len(rows),
                )
                for idx, row in enumerate(rows):
                    try:
                        patch = mirror_show_image_row(row, force=bool(payload.force_mirror), s3_client=s3_client)
                        if patch:
                            update_show_image_hosted_fields(db, str(row.get("id")), patch)
                            show_images_mirrored += 1
                    except Exception:  # noqa: BLE001
                        show_images_mirror_failed += 1
                    bump_current(1)
                    if (idx + 1) % 10 == 0:
                        yield progress(
                            stage="mirror_show_images",
                            message="Mirroring show images to S3...",
                            stage_current=idx + 1,
                            stage_total=len(rows),
                        )
            else:
                yield progress(
                    stage="mirror_show_images",
                    message=f"Skipping show-level mirroring for season-scoped refresh (season {season_scope}).",
                )

            # 3.2 season_images
            try:
                rows = fetch_season_images_missing_hosted(
                    db,
                    show_id=show_id_str,
                    tmdb_id=int(show_tmdb_id) if isinstance(show_tmdb_id, int) else None,
                    season_number=season_scope,
                    limit=None,
                    include_hosted=bool(payload.force_mirror),
                    cdn_base_url=cdn_base_url,
                )
            except Exception as exc:  # noqa: BLE001
                rows = []
                errors.append(f"Mirror season_images list: {exc}")

            bump_total(len(rows))
            yield progress(
                stage="mirror_season_images",
                message="Mirroring season images to S3...",
                stage_total=len(rows),
            )
            for idx, row in enumerate(rows):
                try:
                    patch = mirror_season_image_row(row, force=bool(payload.force_mirror), s3_client=s3_client)
                    if patch:
                        update_season_image_hosted_fields(db, str(row.get("id")), patch)
                        season_images_mirrored += 1
                except Exception:  # noqa: BLE001
                    season_images_mirror_failed += 1
                bump_current(1)
                if (idx + 1) % 10 == 0:
                    yield progress(
                        stage="mirror_season_images",
                        message="Mirroring season images to S3...",
                        stage_current=idx + 1,
                        stage_total=len(rows),
                    )

            # 3.3 episode_images
            if episode_images_enabled:
                try:
                    rows = fetch_episode_images_missing_hosted(
                        db,
                        show_id=show_id_str,
                        tmdb_id=int(show_tmdb_id) if isinstance(show_tmdb_id, int) else None,
                        season_number=season_scope,
                        episode_number=None,
                        limit=None,
                        include_hosted=bool(payload.force_mirror),
                        cdn_base_url=cdn_base_url,
                    )
                except Exception as exc:  # noqa: BLE001
                    rows = []
                    errors.append(f"Mirror episode_images list: {exc}")

                bump_total(len(rows))
                yield progress(
                    stage="mirror_episode_images",
                    message="Mirroring episode images to S3...",
                    stage_total=len(rows),
                )
                for idx, row in enumerate(rows):
                    try:
                        patch = mirror_episode_image_row(row, force=bool(payload.force_mirror), s3_client=s3_client)
                        if patch:
                            update_episode_image_hosted_fields(db, str(row.get("id")), patch)
                            episode_images_mirrored += 1
                    except Exception:  # noqa: BLE001
                        episode_images_mirror_failed += 1
                    bump_current(1)
                    if (idx + 1) % 10 == 0:
                        yield progress(
                            stage="mirror_episode_images",
                            message="Mirroring episode images to S3...",
                            stage_current=idx + 1,
                            stage_total=len(rows),
                        )

            if not payload.skip_prune and season_scope is None:
                try:
                    prune_orphaned_show_image_objects(
                        db,
                        show_imdb_id or show_id_str,
                        show_id=show_id_str,
                        dry_run=False,
                        verbose=bool(payload.verbose),
                        s3_client=s3_client,
                    )
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"Prune show images: {exc}")
            elif not payload.skip_prune and season_scope is not None:
                yield progress(
                    stage="prune_show_images",
                    message=f"Skipping show-level prune for season-scoped refresh (season {season_scope}).",
                )

        else:
            yield progress(stage="mirror", message="Skipping S3 mirroring (skip_s3=true).")
            season_images_mirrored = 0
            episode_images_mirrored = 0

        # ------------------------------------------------------------------
        # Stage 4: Cast photos (IMDb + TMDb, and Fandom only for Real Housewives)
        # ------------------------------------------------------------------
        cast_photos_fetched = 0
        cast_photos_failed = 0
        cast_photos_pruned = 0
        source_skip_details: list[dict[str, object]] = []
        allow_fandom_sources = _is_real_housewives_show(show_name)

        # Determine cast people ids for show.
        def _fetch_person_ids_for_show(season_number: int | None = None) -> list[str]:
            person_ids: set[str] = set()
            if season_number is None:
                for table in ("episode_appearances", "show_cast"):
                    try:
                        resp = db.schema("core").table(table).select("person_id").eq("show_id", show_id_str).execute()
                    except Exception:  # noqa: BLE001
                        continue
                    if hasattr(resp, "error") and resp.error:
                        continue
                    for row in resp.data or []:
                        pid = row.get("person_id")
                        if pid:
                            person_ids.add(str(pid))
                return list(person_ids)

            try:
                resp = (
                    db.schema("core")
                    .table("episode_appearances")
                    .select("person_id,seasons")
                    .eq("show_id", show_id_str)
                    .execute()
                )
            except Exception:  # noqa: BLE001
                return []
            if hasattr(resp, "error") and resp.error:
                return []
            for row in resp.data or []:
                pid = row.get("person_id")
                seasons = row.get("seasons")
                if not pid or not isinstance(seasons, list):
                    continue
                season_values: set[int] = set()
                for value in seasons:
                    try:
                        season_values.add(int(value))
                    except Exception:
                        continue
                if int(season_number) in season_values:
                    person_ids.add(str(pid))
            return list(person_ids)

        person_ids = _fetch_person_ids_for_show(season_scope)
        if not person_ids:
            if season_scope is not None:
                yield progress(
                    stage="sync_cast_photos",
                    message=f"No cast members found for season {season_scope}; skipping cast photos.",
                )
            else:
                yield progress(stage="sync_cast_photos", message="No cast members found; skipping cast photos.")
        else:
            people_rows: list[dict] = []
            for i in range(0, len(person_ids), 200):
                chunk = person_ids[i : i + 200]
                try:
                    resp = (
                        db.schema("core").table("people").select("id,full_name,external_ids").in_("id", chunk).execute()
                    )
                except Exception:  # noqa: BLE001
                    errors.append("People lookup: query failed")
                    continue
                if hasattr(resp, "error") and resp.error:
                    errors.append(f"People lookup: {resp.error}")
                    continue
                if isinstance(resp.data, list):
                    people_rows.extend(resp.data)

            # Build inputs per person.
            targets: list[dict[str, object]] = []
            for person in people_rows:
                pid = str(person.get("id") or "").strip()
                if not pid:
                    continue
                full_name = str(person.get("full_name") or "").strip() or None
                external_ids = person.get("external_ids") if isinstance(person.get("external_ids"), dict) else {}

                imdb_person_id = str(external_ids.get("imdb") or "").strip() or None
                tmdb_person_id = None
                try:
                    if external_ids.get("tmdb_id") or external_ids.get("tmdb"):
                        tmdb_person_id = int(external_ids.get("tmdb_id") or external_ids.get("tmdb"))
                except Exception:
                    tmdb_person_id = None

                try:
                    tmdb_row = get_cast_tmdb_by_person_id(db, pid)
                except Exception:
                    tmdb_row = None
                if tmdb_row:
                    if not imdb_person_id:
                        imdb_person_id = str(tmdb_row.get("imdb_id") or "").strip() or imdb_person_id
                    if tmdb_person_id is None and tmdb_row.get("tmdb_id"):
                        try:
                            tmdb_person_id = int(tmdb_row.get("tmdb_id"))
                        except Exception:
                            tmdb_person_id = tmdb_person_id

                targets.append(
                    {
                        "person_id": pid,
                        "person_name": full_name,
                        "imdb_person_id": imdb_person_id,
                        "tmdb_person_id": tmdb_person_id,
                    }
                )

            # Compute fetch work (only count source fetches that have required ids).
            fetch_units = 0
            for t in targets:
                if t.get("imdb_person_id"):
                    fetch_units += 1  # IMDb
                if t.get("tmdb_person_id"):
                    fetch_units += 1  # TMDb
                if allow_fandom_sources and t.get("person_name"):
                    fetch_units += 2  # Fandom person + Fandom gallery

            bump_total(fetch_units)
            yield progress(
                stage="sync_cast_photos",
                message=(
                    "Syncing cast photos (IMDb/TMDb/Fandom)..."
                    if allow_fandom_sources
                    else "Syncing cast photos (IMDb/TMDb)..."
                ),
                stage_total=fetch_units,
            )
            if not allow_fandom_sources:
                yield progress(
                    stage="sync_fandom",
                    message="Skipping Fandom cast photos for non-Real Housewives shows.",
                )

            stage_done = 0
            for t in targets:
                pid = str(t.get("person_id"))
                pname = t.get("person_name")
                imdb_pid = t.get("imdb_person_id")
                tmdb_pid = t.get("tmdb_person_id")

                def _tag_show_context(rows: list[dict]) -> None:
                    for row in rows:
                        meta = dict(row.get("metadata") or {})
                        meta.setdefault("show_id", show_id_str)
                        if show_name:
                            meta.setdefault("show_name", show_name)
                        row["metadata"] = meta

                # IMDb
                if imdb_pid:
                    source_total = _get_known_person_source_total("imdb", str(imdb_pid), None)
                    mirrored_count = (
                        _count_mirrored_cast_photos_for_source(db, pid, "imdb") if source_total is not None else None
                    )
                    if (
                        source_total is not None
                        and mirrored_count is not None
                        and mirrored_count >= source_total
                        and not payload.force_mirror
                    ):
                        stage_done += 1
                        source_skip_details.append(
                            {
                                "source": "imdb",
                                "reason": "already_mirrored",
                                "source_total": source_total,
                                "mirrored_count": mirrored_count,
                            }
                        )
                        yield progress(
                            stage="sync_imdb",
                            message=(
                                f"Skipping IMDb cast photos ({pid}): already mirrored {mirrored_count}/{source_total}."
                            ),
                            stage_current=stage_done,
                            stage_total=fetch_units,
                            extra={
                                "source": "imdb",
                                "skip_reason": "already_mirrored",
                                "source_total": source_total,
                                "mirrored_count": mirrored_count,
                            },
                        )
                        bump_current(1)
                    else:
                        stage_done += 1
                        yield progress(
                            stage="sync_imdb",
                            message=f"Syncing IMDb cast photos ({pid})...",
                            stage_current=stage_done,
                            stage_total=fetch_units,
                            extra={
                                "source": "imdb",
                                "source_total": source_total,
                                "mirrored_count": mirrored_count,
                                "heartbeat": True,
                                "elapsed_ms": 0,
                            },
                        )
                        started_at = time.perf_counter()
                        fetch_result: dict[str, Any] = {"rows": [], "error": None}
                        imdb_pid_for_stage = int(imdb_pid)
                        person_id_for_stage = pid
                        stage_limit = int(payload.limit_per_source)

                        def run_imdb_fetch(
                            *,
                            result: dict[str, Any] = fetch_result,
                            imdb_id: int = imdb_pid_for_stage,
                            person_id: str = person_id_for_stage,
                            limit: int = stage_limit,
                        ) -> None:
                            try:
                                result["rows"] = fetch_imdb_cast_photos(
                                    str(imdb_id),
                                    person_id,
                                    limit=limit,
                                )
                            except Exception as exc:  # noqa: BLE001
                                result["error"] = exc

                        fetch_thread = Thread(target=run_imdb_fetch, daemon=True)
                        fetch_thread.start()
                        while fetch_thread.is_alive():
                            fetch_thread.join(timeout=10)
                            if fetch_thread.is_alive():
                                yield progress(
                                    stage="sync_imdb",
                                    message=f"Syncing IMDb cast photos ({pid})...",
                                    stage_current=stage_done,
                                    stage_total=fetch_units,
                                    extra={
                                        "source": "imdb",
                                        "heartbeat": True,
                                        "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
                                    },
                                )
                        fetch_thread.join()
                        try:
                            if fetch_result["error"] is not None:
                                raise fetch_result["error"]
                            rows = fetch_result["rows"] if isinstance(fetch_result["rows"], list) else []
                            _tag_show_context(rows)
                            cast_photos_fetched += len(rows)
                            if rows:
                                upserted = upsert_cast_photos(db, rows, dedupe_on="source_image_id")
                                cast_photos_upserted += len(upserted)
                            sources_used.add("imdb")
                        except Exception as exc:  # noqa: BLE001
                            errors.append(f"IMDb cast photos {pid}: {exc}")
                        bump_current(1)
                        yield progress(
                            stage="sync_imdb",
                            message=f"Synced IMDb cast photos ({pid}).",
                            stage_current=stage_done,
                            stage_total=fetch_units,
                            extra={
                                "source": "imdb",
                                "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
                            },
                        )

                # TMDb
                if tmdb_pid:
                    source_total = _get_known_person_source_total("tmdb", None, int(tmdb_pid))
                    mirrored_count = (
                        _count_mirrored_cast_photos_for_source(db, pid, "tmdb") if source_total is not None else None
                    )
                    if (
                        source_total is not None
                        and mirrored_count is not None
                        and mirrored_count >= source_total
                        and not payload.force_mirror
                    ):
                        stage_done += 1
                        source_skip_details.append(
                            {
                                "source": "tmdb",
                                "reason": "already_mirrored",
                                "source_total": source_total,
                                "mirrored_count": mirrored_count,
                            }
                        )
                        yield progress(
                            stage="sync_tmdb",
                            message=(
                                f"Skipping TMDb cast photos ({pid}): already mirrored {mirrored_count}/{source_total}."
                            ),
                            stage_current=stage_done,
                            stage_total=fetch_units,
                            extra={
                                "source": "tmdb",
                                "skip_reason": "already_mirrored",
                                "source_total": source_total,
                                "mirrored_count": mirrored_count,
                            },
                        )
                        bump_current(1)
                    else:
                        stage_done += 1
                        yield progress(
                            stage="sync_tmdb",
                            message=f"Syncing TMDb cast photos ({pid})...",
                            stage_current=stage_done,
                            stage_total=fetch_units,
                            extra={
                                "source": "tmdb",
                                "source_total": source_total,
                                "mirrored_count": mirrored_count,
                                "heartbeat": True,
                                "elapsed_ms": 0,
                            },
                        )
                        started_at = time.perf_counter()
                        fetch_result: dict[str, Any] = {"rows": [], "error": None}
                        tmdb_pid_for_stage = int(tmdb_pid)
                        person_id_for_stage = pid
                        stage_limit = int(payload.limit_per_source)

                        def run_tmdb_fetch(
                            *,
                            result: dict[str, Any] = fetch_result,
                            tmdb_id: int = tmdb_pid_for_stage,
                            person_id: str = person_id_for_stage,
                            limit: int = stage_limit,
                        ) -> None:
                            try:
                                result["rows"] = fetch_tmdb_cast_photos(
                                    tmdb_id,
                                    person_id,
                                    limit=limit,
                                )
                            except Exception as exc:  # noqa: BLE001
                                result["error"] = exc

                        fetch_thread = Thread(target=run_tmdb_fetch, daemon=True)
                        fetch_thread.start()
                        while fetch_thread.is_alive():
                            fetch_thread.join(timeout=10)
                            if fetch_thread.is_alive():
                                yield progress(
                                    stage="sync_tmdb",
                                    message=f"Syncing TMDb cast photos ({pid})...",
                                    stage_current=stage_done,
                                    stage_total=fetch_units,
                                    extra={
                                        "source": "tmdb",
                                        "heartbeat": True,
                                        "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
                                    },
                                )
                        fetch_thread.join()
                        try:
                            if fetch_result["error"] is not None:
                                raise fetch_result["error"]
                            rows = fetch_result["rows"] if isinstance(fetch_result["rows"], list) else []
                            _tag_show_context(rows)
                            cast_photos_fetched += len(rows)
                            if rows:
                                upserted = upsert_cast_photos(db, rows, dedupe_on="image_url_canonical")
                                cast_photos_upserted += len(upserted)
                            sources_used.add("tmdb")
                        except Exception as exc:  # noqa: BLE001
                            errors.append(f"TMDb cast photos {pid}: {exc}")
                        bump_current(1)
                        yield progress(
                            stage="sync_tmdb",
                            message=f"Synced TMDb cast photos ({pid}).",
                            stage_current=stage_done,
                            stage_total=fetch_units,
                            extra={
                                "source": "tmdb",
                                "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
                            },
                        )

                # Fandom person + gallery (Real Housewives only)
                if allow_fandom_sources and isinstance(pname, str) and pname.strip():
                    stage_done += 1
                    yield progress(
                        stage="sync_fandom",
                        message=f"Syncing Fandom cast photos ({pname})...",
                        stage_current=stage_done,
                        stage_total=fetch_units,
                    )
                    try:
                        rows = fetch_fandom_person_cast_photos(
                            str(pname),
                            pid,
                            imdb_person_id=str(imdb_pid) if imdb_pid else None,
                            limit=int(payload.limit_per_source),
                        )
                        _tag_show_context(rows)
                        cast_photos_fetched += len(rows)
                        if rows:
                            upserted = upsert_cast_photos(db, rows, dedupe_on="image_url_canonical")
                            cast_photos_upserted += len(upserted)
                        sources_used.add("fandom")
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"Fandom cast photos {pid}: {exc}")
                    bump_current(1)

                    stage_done += 1
                    yield progress(
                        stage="sync_fandom_gallery",
                        message=f"Syncing Fandom gallery ({pname})...",
                        stage_current=stage_done,
                        stage_total=fetch_units,
                    )
                    try:
                        rows = fetch_fandom_gallery_cast_photos(
                            str(pname),
                            pid,
                            imdb_person_id=str(imdb_pid) if imdb_pid else None,
                            limit=int(payload.limit_per_source),
                        )
                        _tag_show_context(rows)
                        cast_photos_fetched += len(rows)
                        if rows:
                            upserted = upsert_cast_photos(db, rows, dedupe_on="image_url_canonical")
                            cast_photos_upserted += len(upserted)
                        sources_used.add("fandom")
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"Fandom gallery {pid}: {exc}")
                    bump_current(1)

            # Mirror cast photos
            if not payload.skip_s3:
                yield progress(stage="mirror_cast_photos", message="Mirroring cast photos to S3...")
                try:
                    cdn_base_url = None if payload.force_mirror else get_cdn_base_url()
                    rows = fetch_cast_photos_missing_hosted(
                        db,
                        person_ids=[str(t.get("person_id")) for t in targets],
                        cdn_base_url=cdn_base_url,
                        include_hosted=bool(payload.force_mirror),
                    )
                except Exception as exc:  # noqa: BLE001
                    rows = []
                    errors.append(f"Mirror cast photos list: {exc}")

                bump_total(len(rows))
                for idx, row in enumerate(rows):
                    try:
                        patch = mirror_cast_photo_row(row, force=bool(payload.force_mirror))
                        if patch:
                            update_cast_photo_hosted_fields(db, str(row.get("id")), patch)
                            cast_photos_mirrored += 1
                    except Exception:  # noqa: BLE001
                        cast_photos_failed += 1
                    bump_current(1)
                    if (idx + 1) % 10 == 0:
                        yield progress(
                            stage="mirror_cast_photos",
                            message="Mirroring cast photos to S3...",
                            stage_current=idx + 1,
                            stage_total=len(rows),
                        )

                if not payload.skip_prune:
                    prune_stage_total = len(targets)
                    bump_total(prune_stage_total)
                    yield progress(
                        stage="prune_cast_photos",
                        message="Pruning orphaned cast photo objects...",
                        stage_total=prune_stage_total,
                    )
                    for idx, t in enumerate(targets):
                        identifier = str(t.get("imdb_person_id") or t.get("person_id") or "").strip()
                        if not identifier:
                            bump_current(1)
                            continue
                        try:
                            cast_photos_pruned += len(
                                prune_orphaned_cast_photo_objects(
                                    db,
                                    identifier,
                                    dry_run=False,
                                    verbose=bool(payload.verbose),
                                )
                            )
                        except Exception as exc:  # noqa: BLE001
                            errors.append(f"Prune cast photos {identifier}: {exc}")
                        bump_current(1)
                        if (idx + 1) % 10 == 0:
                            yield progress(
                                stage="prune_cast_photos",
                                message="Pruning orphaned cast photo objects...",
                                stage_current=idx + 1,
                                stage_total=prune_stage_total,
                            )

        # ------------------------------------------------------------------
        # Stage 5: Auto-count people (cast photos)
        # ------------------------------------------------------------------
        auto_counts_attempted = 0
        auto_counts_succeeded = 0
        auto_counts_failed = 0
        if payload.skip_auto_count:
            yield progress(stage="auto_count", message="Skipping auto-count (fast mode).")
        elif is_screenalytics_configured() and person_ids and not payload.skip_s3:
            unavailable, retry_after_s, unavailable_reason = get_screenalytics_unavailable_state()
            if unavailable:
                yield progress(
                    stage="auto_count",
                    message="Skipping auto-count (vision unavailable).",
                    extra={
                        "skip_reason": "service_unavailable",
                        "service_unavailable": True,
                        "retry_after_s": retry_after_s,
                        "detail": unavailable_reason,
                    },
                )
            else:
                try:
                    resp = (
                        db.schema("core")
                        .table("cast_photos")
                        .select("id,hosted_url,url,image_url,thumb_url,people_names,source,metadata")
                        .in_("person_id", person_ids)
                        .execute()
                    )
                    rows = resp.data or []
                    if not isinstance(rows, list):
                        rows = []
                except Exception as exc:  # noqa: BLE001
                    rows = []
                    errors.append(f"Auto-count query: {exc}")

                tag_rows = get_tags_by_photo_ids(db, [str(r.get("id")) for r in rows if r.get("id")])

                to_process: list[dict] = []
                for row in rows:
                    if row.get("people_names"):
                        continue
                    tag_row = tag_rows.get(str(row.get("id")))
                    if has_manual_tags(tag_row):
                        continue
                    if tag_row and tag_row.get("people_count") is not None:
                        continue
                    to_process.append(row)

                bump_total(len(to_process))
                yield progress(
                    stage="auto_count",
                    message="Auto-counting people in images...",
                    stage_total=len(to_process),
                )
                for idx, row in enumerate(to_process):
                    url = row.get("hosted_url") or row.get("url") or row.get("image_url") or row.get("thumb_url")
                    if not url:
                        bump_current(1)
                        continue
                    try:
                        result = count_people(str(url))
                        upsert_cast_photo_tags(
                            db,
                            cast_photo_id=str(row.get("id")),
                            people_names=None,
                            people_ids=None,
                            people_count=result.people_count,
                            people_count_source="auto",
                            detector=result.detector,
                            updated_by_firebase_uid="system:auto",
                        )
                        auto_counts_succeeded += 1
                    except ScreenalyticsUnavailableError as exc:
                        auto_counts_failed += 1
                        detail = str(exc) or "Vision unavailable"
                        errors.append(f"Auto-count service unavailable: {detail}")
                        yield progress(
                            stage="auto_count",
                            message="Auto-count paused (vision unavailable).",
                            stage_current=idx,
                            stage_total=len(to_process),
                            extra={
                                "skip_reason": "service_unavailable",
                                "service_unavailable": True,
                                "retry_after_s": max(int(exc.retry_after_s), 1),
                                "detail": detail,
                            },
                        )
                        break
                    except ScreenalyticsClientError as exc:
                        auto_counts_failed += 1
                        errors.append(f"Auto-count {row.get('id')}: {exc}")
                    auto_counts_attempted += 1
                    bump_current(1)
                    if (idx + 1) % 10 == 0:
                        yield progress(
                            stage="auto_count",
                            message="Auto-counting people in images...",
                            stage_current=idx + 1,
                            stage_total=len(to_process),
                        )
        else:
            yield progress(
                stage="auto_count",
                message="Skipping auto-count (not configured).",
                extra={"skip_reason": "not_configured"},
            )

        # ------------------------------------------------------------------
        # Stage 6: Text overlay detection (cast photos)
        # ------------------------------------------------------------------
        text_overlay_attempted = 0
        text_overlay_succeeded = 0
        text_overlay_failed = 0
        if payload.skip_word_detection:
            yield progress(stage="word_id", message="Skipping word detection (fast mode).")
        else:
            try:
                from trr_backend.vision.text_overlay import (
                    TextOverlayDetectionNotConfiguredError,
                    detect_and_update_cast_photo_text_overlay,
                    is_text_overlay_detection_configured,
                )

                if is_text_overlay_detection_configured() and person_ids:
                    resp = (
                        db.schema("core")
                        .table("cast_photos")
                        .select("id,metadata")
                        .in_("person_id", person_ids)
                        .limit(500)
                        .execute()
                    )
                    rows = resp.data or []
                    if not isinstance(rows, list):
                        rows = []
                    candidates: list[str] = []
                    for row in rows:
                        meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                        if "has_text_overlay" in (meta or {}):
                            continue
                        pid = row.get("id")
                        if pid:
                            candidates.append(str(pid))

                    bump_total(len(candidates))
                    yield progress(
                        stage="word_id",
                        message="Detecting words/text overlays...",
                        stage_total=len(candidates),
                    )
                    for idx, photo_id in enumerate(candidates):
                        try:
                            detect_and_update_cast_photo_text_overlay(db, photo_id, force=False)
                            text_overlay_succeeded += 1
                        except TextOverlayDetectionNotConfiguredError:
                            break
                        except Exception as exc:  # noqa: BLE001
                            text_overlay_failed += 1
                            errors.append(f"Word ID {photo_id}: {exc}")
                        text_overlay_attempted += 1
                        bump_current(1)
                        if (idx + 1) % 10 == 0:
                            yield progress(
                                stage="word_id",
                                message="Detecting words/text overlays...",
                                stage_current=idx + 1,
                                stage_total=len(candidates),
                            )
                else:
                    yield progress(stage="word_id", message="Skipping word detection (not configured).")
            except Exception:
                yield progress(stage="word_id", message="Skipping word detection (module not available).")

        duration_ms = int((time.perf_counter() - started) * 1000)
        complete = RefreshShowPhotosResponse(
            show_id=show_id_str,
            show_name=show_name,
            sources_used=sorted(sources_used),
            show_images_upserted=show_images_upserted,
            show_images_mirrored=show_images_mirrored,
            season_images_upserted=season_images_upserted,
            season_images_mirrored=season_images_mirrored,
            episode_images_upserted=episode_images_upserted,
            episode_images_mirrored=episode_images_mirrored,
            cast_photos_fetched=cast_photos_fetched,
            cast_photos_upserted=cast_photos_upserted,
            cast_photos_mirrored=cast_photos_mirrored,
            cast_photos_failed=cast_photos_failed,
            cast_photos_pruned=cast_photos_pruned,
            auto_counts_attempted=auto_counts_attempted,
            auto_counts_succeeded=auto_counts_succeeded,
            auto_counts_failed=auto_counts_failed,
            text_overlay_attempted=text_overlay_attempted,
            text_overlay_succeeded=text_overlay_succeeded,
            text_overlay_failed=text_overlay_failed,
            duration_ms=duration_ms,
            errors=errors,
        )
        complete_payload = complete.model_dump()
        complete_payload["season_number"] = season_scope
        complete_payload["sources_skipped"] = len(source_skip_details)
        complete_payload["source_skip_details"] = source_skip_details
        complete_payload["live_counts"] = build_live_counts()
        yield _yield_event("complete", _with_request_id(complete_payload))

    stream_response = StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
    if _is_internal_raw_stream_request(request):
        return stream_response

    actor = str((admin_user or {}).get("email") or (admin_user or {}).get("id") or "admin")
    request_payload = {
        "show_id": show_id_str,
        "payload": payload.model_dump(mode="json"),
        "request_id": request_id,
        "initiated_by": actor,
    }
    operation = start_operation_for_stream(
        operation_type="admin_show_refresh_photos",
        producer=event_generator,
        request_payload=request_payload,
        initiated_by=actor,
        request=request,
    )
    return operation_stream_response(str(operation.get("id")), request=request)


def build_show_refresh_photos_operation_producer(
    *,
    request_payload: dict[str, Any],
    db: SupabaseAdminClient | None = None,
):
    from trr_backend.db.admin import create_supabase_admin_client

    show_id_str = str(request_payload.get("show_id") or "").strip()
    if not show_id_str:
        raise ValueError("request_payload.show_id is required")

    payload_data = request_payload.get("payload") if isinstance(request_payload.get("payload"), dict) else {}
    payload = RefreshShowPhotosRequest.model_validate(payload_data)
    request_id = str(request_payload.get("request_id") or "").strip() or None
    initiated_by = str(request_payload.get("initiated_by") or "admin")

    class _InternalStreamRequest:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {"x-trr-internal-raw-stream": "1"}
            if request_id:
                self.headers["x-trr-request-id"] = request_id

        async def is_disconnected(self) -> bool:
            return False

    def _producer():
        local_db = db or create_supabase_admin_client()
        response = refresh_show_photos_stream(
            show_id=UUID(show_id_str),
            request=_InternalStreamRequest(),
            payload=payload,
            db=local_db,
            admin_user={"id": initiated_by},
        )
        body_iterator = getattr(response, "body_iterator", None)
        if body_iterator is None:
            return []
        return body_iterator

    return _producer
