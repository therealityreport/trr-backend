"""Shared source-policy and progress helpers for person image workflows."""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Literal, TypeVar, cast
from urllib.parse import quote

TChunk = TypeVar("TChunk")
SourceType = Literal["imdb", "tmdb", "fandom", "fandom-gallery", "nbcumv", "getty", "bravotv"]
SourceProgressStatus = Literal["pending", "running", "completed", "warning", "skipped", "failed"]


def normalize_scope_ids(values: list[str] | None) -> list[str] | None:
    if values is None:
        return None
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        cleaned = str(value).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


def build_google_reverse_image_search_url(image_url: str | None) -> str | None:
    cleaned = str(image_url or "").strip()
    if not cleaned:
        return None
    return f"https://www.google.com/searchbyimage?image_url={quote(cleaned, safe='')}"


def normalize_source_progress_key(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower().replace("-", "_")
    if not normalized:
        return None
    if normalized in {"nbcumv", "getty"}:
        return "getty_nbcumv"
    return normalized


def canonicalize_refresh_source(value: str | None) -> SourceType | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    if cleaned.lower() == "getty":
        return "nbcumv"
    return cast(SourceType, cleaned)


def canonicalize_refresh_sources(values: list[str] | None) -> list[SourceType]:
    if values is None:
        return []
    seen: set[str] = set()
    normalized: list[SourceType] = []
    for value in values:
        canonical = canonicalize_refresh_source(value)
        if canonical is None:
            continue
        dedupe_key = canonical.lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized.append(canonical)
    return normalized


def allow_nbcumv_only_supplement_for_requested_sources(values: list[str] | tuple[str, ...] | None) -> bool:
    if not values:
        return True
    normalized_sources = {str(value or "").strip().lower() for value in values if str(value or "").strip()}
    return "nbcumv" in normalized_sources


def resolve_requested_source_labels(
    *,
    requested_sources: list[str] | None,
    operational_sources: list[SourceType],
) -> list[str]:
    if not requested_sources:
        return list(operational_sources)

    enabled_progress_keys = {
        progress_key
        for source in operational_sources
        if (progress_key := normalize_source_progress_key(source)) is not None
    }
    labels: list[str] = []
    for source in requested_sources:
        label = str(source or "").strip().lower()
        progress_key = normalize_source_progress_key(label)
        if not label or progress_key is None or progress_key not in enabled_progress_keys:
            continue
        if label not in labels:
            labels.append(label)
    return labels or list(operational_sources)


def empty_source_progress_entry() -> dict[str, Any]:
    return {
        "discovered_total": None,
        "scraped_current": 0,
        "saved_current": 0,
        "covered_existing": 0,
        "upgraded_existing": 0,
        "failed_current": 0,
        "skipped_current": 0,
        "remaining": None,
        "status": "pending",
        "message": None,
    }


def status_with_warning(
    *,
    imported: int = 0,
    covered_existing: int = 0,
    failed: int = 0,
    skipped: int = 0,
    cancelled: bool = False,
) -> SourceProgressStatus:
    if cancelled:
        return "failed"
    successful = max(0, int(imported)) + max(0, int(covered_existing))
    failed_count = max(0, int(failed))
    skipped_count = max(0, int(skipped))
    if failed_count <= 0:
        return "completed"
    if successful > 0 or skipped_count > 0:
        return "warning"
    return "failed"


def getty_progress_status_with_warning(
    *,
    hosted: int = 0,
    covered_existing: int = 0,
    failed: int = 0,
) -> str:
    successful = max(0, int(hosted)) + max(0, int(covered_existing))
    failed_count = max(0, int(failed))
    if failed_count <= 0:
        return "completed"
    if successful > 0:
        return "warning"
    return "failed"


def ordered_source_progress_snapshot(
    source_progress: dict[str, dict[str, Any]],
    key_order: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    def _sort_key(item: tuple[str, dict[str, Any]]) -> tuple[int, str]:
        key = item[0]
        try:
            return key_order.index(key), key
        except ValueError:
            return len(key_order), key

    return dict(sorted(source_progress.items(), key=_sort_key))


def empty_getty_progress_subtask(
    task_id: str,
    subtask_labels: dict[str, str],
) -> dict[str, Any]:
    return {
        "id": task_id,
        "label": subtask_labels.get(task_id, task_id.replace("_", " ")),
        "status": "pending",
        "query": None,
        "query_url": None,
        "candidates_found": 0,
        "site_image_total": None,
        "site_event_total": None,
        "site_video_total": None,
        "usable_after_dedupe_total": 0,
        "overlap_count": 0,
        "current": 0,
        "total": 0,
        "message": None,
    }


def empty_getty_progress(
    subtask_order: tuple[str, ...],
    subtask_labels: dict[str, str],
) -> dict[str, Any]:
    return {
        "status": "pending",
        "phase": "searching",
        "auth_mode": None,
        "subtasks": {task_id: empty_getty_progress_subtask(task_id, subtask_labels) for task_id in subtask_order},
        "breakdown": {
            "raw_getty_candidates": 0,
            "unique_discovered": 0,
            "getty_query_image_total": 0,
            "getty_query_event_total": 0,
            "getty_query_page_total": 0,
            "getty_pages_completed": 0,
            "getty_pages_total": 0,
            "getty_discovered_total": 0,
            "getty_usable_total": 0,
            "getty_existing_shared_total": 0,
            "getty_existing_getty_total": 0,
            "getty_to_import_total": 0,
            "getty_skipped_existing_total": 0,
            "getty_deferred_resolution_total": 0,
            "matched_via_nbcumv": 0,
            "matched_via_bravotv_json": 0,
            "matched_via_image_search": 0,
            "unmatched_getty": 0,
            "getty_only_imported": 0,
            "nbcumv_only_imported": 0,
            "bravotv_only_imported": 0,
            "covered_existing": 0,
            "upgraded_existing": 0,
            "skipped": 0,
            "failed": 0,
            "mirrored_hosted": 0,
            "mirrored_failed": 0,
        },
    }


def ordered_getty_progress_snapshot(
    getty_progress: dict[str, Any] | None,
    subtask_order: tuple[str, ...],
    subtask_labels: dict[str, str],
) -> dict[str, Any] | None:
    if not isinstance(getty_progress, dict):
        return None
    subtasks_raw = getty_progress.get("subtasks")
    subtasks_by_id = subtasks_raw if isinstance(subtasks_raw, dict) else {}
    ordered_subtasks = [
        dict(subtasks_by_id.get(task_id) or empty_getty_progress_subtask(task_id, subtask_labels))
        for task_id in subtask_order
    ]
    return {
        "status": str(getty_progress.get("status") or "pending").strip().lower() or "pending",
        "phase": str(getty_progress.get("phase") or "searching").strip().lower() or "searching",
        "auth_mode": (str(getty_progress.get("auth_mode") or "").strip() or None),
        "subtasks": ordered_subtasks,
        "breakdown": dict(getty_progress.get("breakdown") or {}),
    }


def resolve_execution_profile(profile: str | None) -> Literal["speed", "balanced", "safe"]:
    normalized = str(profile or "").strip().lower()
    if normalized in {"speed", "balanced", "safe"}:
        return cast(Literal["speed", "balanced", "safe"], normalized)
    return "speed"


def profile_default_parallelism(
    profile: Literal["speed", "balanced", "safe"],
    stage: Literal["sync", "mirror", "tagging", "crop"],
) -> int:
    defaults: dict[str, dict[str, int]] = {
        "speed": {"sync": 3, "mirror": 12, "tagging": 8, "crop": 8},
        "balanced": {"sync": 2, "mirror": 8, "tagging": 4, "crop": 4},
        "safe": {"sync": 1, "mirror": 4, "tagging": 2, "crop": 2},
    }
    return defaults[profile][stage]


def profile_default_batch_size(
    profile: Literal["speed", "balanced", "safe"],
    stage: Literal["tagging", "mirror", "crop"],
) -> int:
    defaults: dict[str, dict[str, int]] = {
        "speed": {"tagging": 32, "mirror": 200, "crop": 64},
        "balanced": {"tagging": 24, "mirror": 120, "crop": 48},
        "safe": {"tagging": 16, "mirror": 80, "crop": 24},
    }
    return defaults[profile][stage]


def resolve_stage_parallelism(
    *,
    request_overrides: dict[str, int] | None,
    stage: Literal["sync", "mirror", "tagging", "crop"],
    default: int,
) -> int:
    if isinstance(request_overrides, dict):
        candidate = request_overrides.get(stage)
        if isinstance(candidate, int) and candidate > 0:
            return candidate
    env_map = {
        "sync": "SYNC_MAX_PARALLEL",
        "mirror": "MIRROR_MAX_PARALLEL",
        "tagging": "TAGGING_MAX_PARALLEL",
        "crop": "CROP_MAX_PARALLEL",
    }
    return read_positive_int_env(env_map[stage], default)


def resolve_stage_batch_size(
    *,
    request_overrides: dict[str, int] | None,
    stage: Literal["tagging", "mirror", "crop"],
    default: int,
) -> int:
    if isinstance(request_overrides, dict):
        candidate = request_overrides.get(stage)
        if isinstance(candidate, int) and candidate > 0:
            return candidate
    env_map = {
        "tagging": "TAGGING_BATCH_SIZE",
        "mirror": "MIRROR_BATCH_SIZE",
        "crop": "CROP_BATCH_SIZE",
    }
    return read_positive_int_env(env_map[stage], default)


def read_positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return max(0.001, float(default))
    try:
        parsed = float(str(raw).strip())
    except (TypeError, ValueError):
        return max(0.001, float(default))
    return max(0.001, parsed)


def resolve_resize_variant_job_timeout_seconds() -> float:
    return read_positive_float_env("TRR_RESIZE_VARIANT_JOB_TIMEOUT_S", 120.0)


def resolve_nbcumv_import_item_timeout_seconds() -> float:
    return read_positive_float_env("TRR_NBCUMV_IMPORT_ITEM_TIMEOUT_S", 120.0)


def resolve_getty_only_upsert_batch_size() -> int:
    return read_positive_int_env("TRR_GETTY_ONLY_UPSERT_BATCH_SIZE", 10)


def chunked(items: list[TChunk], size: int) -> list[list[TChunk]]:
    if size <= 0:
        return [items]
    return [items[index : index + size] for index in range(0, len(items), size)]


def snapshot_payload_sha(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def is_transient_stage_error(exc: Exception) -> bool:
    lowered = str(exc).lower()
    return any(
        marker in lowered
        for marker in (
            "timeout",
            "timed out",
            "temporarily unavailable",
            "connection reset",
            "broken pipe",
            "network",
            "socket",
        )
    )


def is_real_housewives_show(show_name: str | None) -> bool:
    if not isinstance(show_name, str):
        return False
    normalized = show_name.strip().lower()
    return bool(normalized) and "real housewives" in normalized


def apply_show_source_policy(
    *,
    show_name: str | None,
    sources: list[SourceType],
) -> tuple[list[SourceType], bool]:
    if show_name is None or is_real_housewives_show(show_name):
        return sources, False

    blocked = {"fandom", "fandom-gallery"}
    filtered_sources: list[SourceType] = [source for source in sources if source not in blocked]
    fandom_skipped = len(filtered_sources) != len(sources)
    return filtered_sources, fandom_skipped


def resolve_refresh_sources(
    *,
    requested_sources: list[str] | None,
    enforce_show_source_policy: bool,
    show_name: str | None,
) -> tuple[list[SourceType], bool]:
    normalized_sources = canonicalize_refresh_sources(requested_sources)
    if not enforce_show_source_policy:
        return normalized_sources, False
    return apply_show_source_policy(show_name=show_name, sources=normalized_sources)


def normalize_operational_refresh_sources(
    *,
    sources: list[SourceType],
    requested_sources: list[str] | None,
    has_getty_prefetched_assets: bool,
    has_getty_prefetched_events: bool,
    has_getty_prefetched_queries: bool,
) -> list[SourceType]:
    normalized: list[SourceType] = [source for source in sources if source != "getty"]
    requested = {str(source or "").strip().lower() for source in (requested_sources or []) if str(source or "").strip()}
    wants_getty_pipeline = (
        "nbcumv" in requested
        or "getty" in requested
        or has_getty_prefetched_assets
        or has_getty_prefetched_events
        or has_getty_prefetched_queries
    )
    if wants_getty_pipeline and "nbcumv" not in normalized:
        normalized.append("nbcumv")
    return normalized


def read_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return max(1, default)
    try:
        parsed = int(str(raw).strip())
    except (TypeError, ValueError):
        return max(1, default)
    return max(1, parsed)
