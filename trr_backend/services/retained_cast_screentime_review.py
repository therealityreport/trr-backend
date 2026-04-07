"""Canonical review and publication helpers for retained cast screentime runs."""

from __future__ import annotations

from typing import Any

from trr_backend.repositories import cast_screentime


def publication_mode_for_media_type(media_type: str | None) -> str:
    return "canonical_episode" if str(media_type or "").strip() == "episode" else "supplementary_reference"


def is_canonical_publication(media_type: str | None) -> bool:
    return publication_mode_for_media_type(media_type) == "canonical_episode"


def build_review_summary(run: dict[str, Any]) -> dict[str, Any]:
    run_id = str(run["id"])
    media_type = str(run.get("media_type") or "").strip() or None
    publication_mode = publication_mode_for_media_type(media_type)
    canonical_publication = is_canonical_publication(media_type)

    raw_leaderboard = cast_screentime.list_leaderboard(run_id)
    segments = cast_screentime.list_segments(run_id)
    excluded_sections = cast_screentime.list_excluded_sections(run_id)

    reviewed_leaderboard, excluded_overlap_ms = _build_reviewed_leaderboard(
        segments,
        excluded_sections,
    )
    if not reviewed_leaderboard:
        reviewed_leaderboard = [
            _normalize_leaderboard_entry(entry) for entry in raw_leaderboard if isinstance(entry, dict)
        ]
        reviewed_totals_source = "raw_metrics_fallback"
    else:
        reviewed_totals_source = "segments_and_exclusions"

    show_id = str(run.get("show_id") or "").strip()
    season_id = str(run.get("season_id") or "").strip() or None
    episode_id = str(run.get("episode_id") or "").strip() or None
    suggestion_decisions = (
        cast_screentime.list_suggestion_decisions_for_context(
            show_id=show_id,
            season_id=season_id,
            episode_id=episode_id,
        )
        if show_id
        else []
    )
    unknown_review_state = (
        cast_screentime.list_unknown_review_state_for_context(
            show_id=show_id,
            season_id=season_id,
            episode_id=episode_id,
        )
        if show_id
        else []
    )
    rerun_required = bool(suggestion_decisions or unknown_review_state)
    current_publish_version = run.get("current_publish_version")
    if current_publish_version is None:
        try:
            current_publish_version = cast_screentime.get_publish_version_for_run(run_id)
        except Exception:
            current_publish_version = None

    return {
        "run_id": run_id,
        "video_asset_id": str(run.get("video_asset_id") or ""),
        "show_id": show_id or None,
        "season_id": season_id,
        "episode_id": episode_id,
        "publication_mode": publication_mode,
        "is_canonical_publication": canonical_publication,
        "raw_leaderboard": [
            _normalize_leaderboard_entry(entry) for entry in raw_leaderboard if isinstance(entry, dict)
        ],
        "reviewed_leaderboard": reviewed_leaderboard,
        "reviewed_totals_source": reviewed_totals_source,
        "excluded_section_count": len(excluded_sections),
        "excluded_overlap_ms": excluded_overlap_ms,
        "decision_counts": {
            "suggestion_decisions": len(suggestion_decisions),
            "unknown_review_state": len(unknown_review_state),
        },
        "rerun_required_for_identity_changes": rerun_required,
        "decision_effect_summary": (
            "Accepted suggestions and unknown-review decisions are stored as "
            "review overlays and require a rerun before identity-driven metrics change."
            if rerun_required
            else "Reviewed totals are regenerated from immutable segments plus "
            "excluded-section review state without mutating raw run artifacts."
        ),
        "current_publish_version": current_publish_version,
    }


def _build_reviewed_leaderboard(
    segments: list[dict[str, Any]],
    excluded_sections: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    merged_ranges = _merge_ranges(
        [
            (int(section.get("start_ms") or 0), int(section.get("end_ms") or 0))
            for section in excluded_sections
            if isinstance(section, dict)
        ]
    )
    totals: dict[str, dict[str, Any]] = {}
    excluded_overlap_ms = 0
    for segment in segments:
        if not isinstance(segment, dict):
            continue
        person_id = str(segment.get("person_id") or "").strip()
        if not person_id or segment.get("is_counted") is False:
            continue
        start_ms = int(segment.get("start_ms") or 0)
        end_ms = int(segment.get("end_ms") or start_ms)
        if end_ms <= start_ms:
            continue
        overlap_ms = _range_overlap_ms(start_ms, end_ms, merged_ranges)
        effective_ms = max(end_ms - start_ms - overlap_ms, 0)
        if effective_ms <= 0:
            excluded_overlap_ms += overlap_ms
            continue
        excluded_overlap_ms += overlap_ms
        state = totals.setdefault(
            person_id,
            {
                "person_id": person_id,
                "display_name": _segment_display_name(segment),
                "screen_time_seconds": 0.0,
                "frame_count": 0,
                "confidence_avg": segment.get("confidence_score"),
                "_confidence_weight": 0,
            },
        )
        state["screen_time_seconds"] += effective_ms / 1000.0
        state["frame_count"] += int(segment.get("frame_count") or 0)
        confidence = segment.get("confidence_score")
        if confidence is not None:
            weighted = int(segment.get("frame_count") or 1) or 1
            state["_confidence_weight"] += weighted
            previous_weight = max(state["_confidence_weight"] - weighted, 0)
            previous_value = float(state.get("confidence_avg") or 0.0) * previous_weight
            state["confidence_avg"] = round(
                (previous_value + (float(confidence) * weighted)) / state["_confidence_weight"], 4
            )
        if not state.get("display_name"):
            state["display_name"] = _segment_display_name(segment)
    leaderboard = sorted(
        [
            {
                "person_id": item["person_id"],
                "display_name": item.get("display_name"),
                "screen_time_seconds": round(float(item["screen_time_seconds"]), 3),
                "frame_count": int(item["frame_count"]),
                "confidence_avg": item.get("confidence_avg"),
            }
            for item in totals.values()
        ],
        key=lambda item: (float(item["screen_time_seconds"]), int(item["frame_count"])),
        reverse=True,
    )
    return leaderboard, excluded_overlap_ms


def _segment_display_name(segment: dict[str, Any]) -> str | None:
    metadata = segment.get("metadata")
    if isinstance(metadata, dict):
        candidate = str(metadata.get("display_name") or "").strip()
        if candidate:
            return candidate
    candidate = str(segment.get("display_name") or "").strip()
    return candidate or None


def _merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    normalized = sorted((max(start, 0), max(end, 0)) for start, end in ranges if end > start)
    merged: list[tuple[int, int]] = []
    for start, end in normalized:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
            continue
        merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def _range_overlap_ms(start_ms: int, end_ms: int, ranges: list[tuple[int, int]]) -> int:
    overlap = 0
    for range_start, range_end in ranges:
        if range_end <= start_ms:
            continue
        if range_start >= end_ms:
            break
        overlap += max(min(end_ms, range_end) - max(start_ms, range_start), 0)
    return overlap


def _normalize_leaderboard_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "person_id": str(entry.get("person_id") or "").strip(),
        "display_name": entry.get("display_name"),
        "screen_time_seconds": round(float(entry.get("screen_time_seconds") or 0.0), 3),
        "frame_count": int(entry.get("frame_count") or 0),
        "confidence_avg": entry.get("confidence_avg"),
    }
