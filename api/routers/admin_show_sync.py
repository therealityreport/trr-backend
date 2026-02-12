"""
Admin endpoints for syncing and refreshing show metadata.

This router exposes "one-button" admin actions that wrap existing ingestion/sync logic:
- Sync from IMDb/TMDb lists (import + enrich, no images)
- Refresh a single show by target area (details, seasons+episodes, photos, cast/credits)
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

import scripts.sync.sync_episode_appearances as sync_episode_appearances
import scripts.sync.sync_episodes as sync_episodes
import scripts.sync.sync_season_episode_images as sync_season_episode_images
import scripts.sync.sync_seasons as sync_seasons
import scripts.sync.sync_seasons_episodes as sync_seasons_episodes
import scripts.sync.sync_show_cast as sync_show_cast
import scripts.sync.sync_show_images as sync_show_images
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

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/shows", tags=["admin-show-sync"])


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


def _is_real_housewives_show(show_name: str | None) -> bool:
    if not isinstance(show_name, str):
        return False
    normalized = show_name.strip().lower()
    return bool(normalized) and "real housewives" in normalized


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

    if not imdb_lists:
        imdb_lists = _split_env_list("IMDB_LIST_URL")
    if not tmdb_lists:
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
    show_resp = db.schema("core").table("shows").select("id").eq("id", show_id_str).limit(1).execute()
    if hasattr(show_resp, "error") and show_resp.error:
        raise HTTPException(status_code=502, detail="Database error fetching show")
    if not show_resp.data:
        raise HTTPException(status_code=404, detail=f"Show {show_id_str} not found")

    # De-dupe targets while preserving order
    ordered: list[ShowRefreshTarget] = []
    seen: set[str] = set()
    for target in payload.targets:
        if target in seen:
            continue
        seen.add(target)
        ordered.append(target)

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
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> StreamingResponse:
    """
    Stream refresh progress for one or more targets as SSE.

    Progress is step-based and accurate (current/total). This is intended for UI progress bars.
    """

    show_id_str = str(show_id)
    show_resp = db.schema("core").table("shows").select("id").eq("id", show_id_str).limit(1).execute()
    if hasattr(show_resp, "error") and show_resp.error:
        raise HTTPException(status_code=502, detail="Database error fetching show")
    if not show_resp.data:
        raise HTTPException(status_code=404, detail=f"Show {show_id_str} not found")

    ordered: list[ShowRefreshTarget] = []
    seen: set[str] = set()
    for target in payload.targets:
        if target in seen:
            continue
        seen.add(target)
        ordered.append(target)

    # Expand targets into concrete steps so the progress bar can update while work runs.
    # Keys are stored in results to match the non-stream endpoint's structure where possible.
    steps: list[tuple[str, str, Callable[[list[str] | None], int], list[str]]] = []
    for target in ordered:
        if target == "details":
            common = ["--show-id", show_id_str, "--force"]
            if payload.verbose:
                common.append("--verbose")

            steps.append(("details", "details_sync_shows", sync_shows.main, list(common)))

            entity_args = list(common)
            if payload.skip_s3:
                entity_args.append("--skip-s3")
            steps.append(("details", "details_tmdb_show_entities", sync_tmdb_show_entities.main, entity_args))

            watch_args = list(common)
            if payload.skip_s3:
                watch_args.append("--skip-s3")
            steps.append(("details", "details_tmdb_watch_providers", sync_tmdb_watch_providers.main, watch_args))
            continue

        if target == "seasons_episodes":
            common = ["--show-id", show_id_str, "--force"]
            if payload.verbose:
                common.append("--verbose")
            steps.append(("seasons_episodes", "seasons_episodes_seasons", sync_seasons.main, list(common)))
            steps.append(("seasons_episodes", "seasons_episodes_episodes", sync_episodes.main, list(common)))
            continue

        if target == "photos":
            argv = ["--show-id", show_id_str, "--force"]
            if payload.skip_s3:
                argv.append("--no-s3")
            if payload.verbose:
                argv.append("--verbose")
            steps.append(("photos", "photos_show_images", sync_show_images.main, list(argv)))

            argv2 = ["--show-id", show_id_str, "--force"]
            if payload.skip_s3:
                argv2.append("--no-s3")
            if payload.verbose:
                argv2.append("--verbose")
            steps.append(("photos", "photos_season_episode_images", sync_season_episode_images.main, argv2))
            continue

        if target == "cast_credits":
            argv = ["--show-id", show_id_str, "--force"]
            if payload.verbose:
                argv.append("--verbose")
            steps.append(("cast_credits", "cast_credits_show_cast", sync_show_cast.main, list(argv)))

            argv2 = ["--show-id", show_id_str, "--force"]
            if payload.verbose:
                argv2.append("--verbose")
            steps.append(("cast_credits", "cast_credits_episode_appearances", sync_episode_appearances.main, argv2))
            continue

        raise HTTPException(status_code=400, detail=f"Unknown refresh target: {target}")

    total_steps = len(steps)

    def _yield_event(event: str, data: dict) -> str:
        return f"event: {event}\ndata: {json.dumps(data)}\n\n"

    def event_generator():
        results: dict[str, RefreshStepResult] = {}
        current = 0

        yield _yield_event(
            "progress",
            {
                "show_id": show_id_str,
                "current": current,
                "total": total_steps,
                "message": "Starting refresh...",
            },
        )

        # Run expanded steps sequentially.
        for target, step_key, fn, argv in steps:
            step_result = _run_script_step(step_key, fn, argv)
            results[step_key] = step_result
            current += 1

            payload_data: dict[str, object] = {
                "show_id": show_id_str,
                "target": target,
                "step": step_key,
                "current": current,
                "total": total_steps,
                "step_status": step_result.status,
                "message": f"{step_key.replace('_', ' ')}: {step_result.status}",
            }
            if step_result.error:
                payload_data["error"] = step_result.error

            yield _yield_event("progress", payload_data)

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
        yield _yield_event("complete", out.model_dump())

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/{show_id}/refresh-photos/stream")
def refresh_show_photos_stream(
    show_id: UUID,
    payload: RefreshShowPhotosRequest | None = None,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
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

    def event_generator():
        from uuid import UUID as _UUID

        from trr_backend.clients.screenalytics import (
            ScreenalyticsClientError,
            count_people,
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
            }
            if stage_current is not None:
                data["stage_current"] = stage_current
            if stage_total is not None:
                data["stage_total"] = stage_total
            if extra:
                data.update(extra)
            return _yield_event("progress", data)

        yield progress(stage="starting", message="Starting refresh...")

        sources_used: set[str] = set()

        # ------------------------------------------------------------------
        # Stage 1: Show images (TMDb + IMDb)
        # ------------------------------------------------------------------
        assert_core_show_images_table_exists(db)
        show_images_upserted = 0
        show_images_mirrored = 0

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
                    upsert_show_images(db, rows)
                    show_images_upserted += len(rows)
                    sources_used.add("imdb")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"IMDb mediaindex: {exc}")

        bump_total(1)
        bump_current(1)
        yield progress(stage="sync_show_images", message="Show images synced.")

        # ------------------------------------------------------------------
        # Stage 2: Season posters + episode stills (TMDb)
        # ------------------------------------------------------------------
        season_images_upserted = 0
        episode_images_upserted = 0
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
            seasons_resp = (
                db.schema("core")
                .table("seasons")
                .select("id,season_number")
                .eq("show_id", show_id_str)
                .order("season_number")
                .execute()
            )
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
        season_images_mirrored = 0
        episode_images_mirrored = 0
        show_images_mirror_failed = 0
        season_images_mirror_failed = 0
        episode_images_mirror_failed = 0

        if not payload.skip_s3:
            s3_client = get_s3_client()
            cdn_base_url = None if payload.force_mirror else get_cdn_base_url()

            # 3.1 show_images
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
            yield progress(stage="mirror_show_images", message="Mirroring show images to S3...", stage_total=len(rows))
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

            # 3.2 season_images
            try:
                rows = fetch_season_images_missing_hosted(
                    db,
                    show_id=show_id_str,
                    tmdb_id=int(show_tmdb_id) if isinstance(show_tmdb_id, int) else None,
                    season_number=None,
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
                        season_number=None,
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

            if not payload.skip_prune:
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

        else:
            yield progress(stage="mirror", message="Skipping S3 mirroring (skip_s3=true).")
            season_images_mirrored = 0
            episode_images_mirrored = 0

        # ------------------------------------------------------------------
        # Stage 4: Cast photos (IMDb + TMDb, and Fandom only for Real Housewives)
        # ------------------------------------------------------------------
        cast_photos_fetched = 0
        cast_photos_upserted = 0
        cast_photos_mirrored = 0
        cast_photos_failed = 0
        cast_photos_pruned = 0
        allow_fandom_sources = _is_real_housewives_show(show_name)

        # Determine cast people ids for show.
        def _fetch_person_ids_for_show() -> list[str]:
            person_ids: set[str] = set()
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

        person_ids = _fetch_person_ids_for_show()
        if not person_ids:
            yield progress(stage="sync_cast_photos", message="No cast members found; skipping cast photos.")
        else:
            people_rows: list[dict] = []
            for i in range(0, len(person_ids), 200):
                chunk = person_ids[i : i + 200]
                resp = db.schema("core").table("people").select("id,full_name,external_ids").in_("id", chunk).execute()
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
                    stage_done += 1
                    yield progress(
                        stage="sync_imdb",
                        message=f"Syncing IMDb cast photos ({pid})...",
                        stage_current=stage_done,
                        stage_total=fetch_units,
                    )
                    try:
                        rows = fetch_imdb_cast_photos(str(imdb_pid), pid, limit=int(payload.limit_per_source))
                        _tag_show_context(rows)
                        cast_photos_fetched += len(rows)
                        if rows:
                            upserted = upsert_cast_photos(db, rows, dedupe_on="source_image_id")
                            cast_photos_upserted += len(upserted)
                        sources_used.add("imdb")
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"IMDb cast photos {pid}: {exc}")
                    bump_current(1)

                # TMDb
                if tmdb_pid:
                    stage_done += 1
                    yield progress(
                        stage="sync_tmdb",
                        message=f"Syncing TMDb cast photos ({pid})...",
                        stage_current=stage_done,
                        stage_total=fetch_units,
                    )
                    try:
                        rows = fetch_tmdb_cast_photos(int(tmdb_pid), pid, limit=int(payload.limit_per_source))
                        _tag_show_context(rows)
                        cast_photos_fetched += len(rows)
                        if rows:
                            upserted = upsert_cast_photos(db, rows, dedupe_on="image_url_canonical")
                            cast_photos_upserted += len(upserted)
                        sources_used.add("tmdb")
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"TMDb cast photos {pid}: {exc}")
                    bump_current(1)

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
            yield progress(stage="auto_count", message="Auto-counting people in images...", stage_total=len(to_process))
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
            yield progress(stage="auto_count", message="Skipping auto-count (not configured).")

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
        yield _yield_event("complete", complete.model_dump())

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
