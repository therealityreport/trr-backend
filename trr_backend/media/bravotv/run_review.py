from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any

REVIEW_REASON_LABELS = {
    "unmatched_source_row": "Unmatched source row",
    "ambiguous_people_match": "Ambiguous people match",
    "ambiguous_episode_match": "Ambiguous episode match",
    "ambiguous_season_match": "Ambiguous season match",
    "source_mismatch": "Source mismatch",
    "duplicate_candidate": "Duplicate candidate",
    "low_resolution": "Low resolution",
    "metadata_only": "Metadata only",
    "replacement_pending": "Replacement pending",
    "failed_download": "Failed download",
    "retryable_acquisition_failure": "Retryable acquisition failure",
    "missing_entity_link": "Missing entity link",
}


def _safe_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _safe_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def load_run_artifacts(output_root: Path) -> dict[str, Any]:
    artifacts: dict[str, Any] = {}
    for path in sorted(output_root.rglob("*.json")):
        rel = path.relative_to(output_root).as_posix()
        name = rel[:-5] if rel.endswith(".json") else rel
        artifacts[name] = _load_json(path)
    return artifacts


def _artifact_list(artifacts: Mapping[str, Any], name: str) -> list[Any]:
    return _safe_list(artifacts.get(name))


def _count_sources(artifacts: Mapping[str, Any], merged_catalog: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for name, value in artifacts.items():
        if not name.startswith("raw/") or not isinstance(value, list):
            continue
        counts[name.removeprefix("raw/")] += len(value)
    if counts:
        return dict(sorted(counts.items()))
    for row in merged_catalog:
        for source in _safe_list(row.get("sources")):
            counts[str(source)] += 1
    return dict(sorted(counts.items()))


def _bridge_strategy_counts(bridge_rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in bridge_rows:
        counts[str(row.get("strategy") or "unknown")] += 1
    return dict(sorted(counts.items()))


def _bravo_gallery_source_breakdown(
    raw_bravo_rows: list[dict[str, Any]],
    merged_catalog: list[dict[str, Any]],
) -> dict[str, Any]:
    branch_counts: Counter[str] = Counter()
    extraction_counts: Counter[str] = Counter()
    profile_field_counts: Counter[str] = Counter()
    merged_branch_counts: Counter[str] = Counter()

    for row in raw_bravo_rows:
        branch = str(row.get("bravotv_collection_branch") or "").strip() or "unknown"
        branch_counts[branch] += 1
        if row.get("bravotv_person_image_field"):
            extraction_counts["person_profile_image"] += 1
            profile_field_counts[str(row.get("bravotv_person_image_field"))] += 1
        elif row.get("bravotv_html_original_url"):
            extraction_counts["legacy_html_original_url"] += 1
        elif row.get("bravotv_html_fallback"):
            extraction_counts["html_fallback"] += 1
        else:
            extraction_counts["jsonapi_media_item"] += 1

    for record in merged_catalog:
        bravo_record = _safe_dict(_safe_dict(record.get("per_source")).get("bravo"))
        if not bravo_record:
            continue
        raw_bravo = _safe_dict(bravo_record.get("raw"))
        branch = (
            str(raw_bravo.get("bravotv_collection_branch") or "").strip()
            or str(bravo_record.get("bravotv_collection_branch") or "").strip()
            or "unknown"
        )
        merged_branch_counts[branch] += 1

    return {
        "total_raw_bravo_rows": len(raw_bravo_rows),
        "total_merged_bravo_rows": sum(merged_branch_counts.values()),
        "by_collection_branch": dict(sorted(branch_counts.items())),
        "by_extraction_method": dict(sorted(extraction_counts.items())),
        "by_profile_image_field": dict(sorted(profile_field_counts.items())),
        "merged_by_collection_branch": dict(sorted(merged_branch_counts.items())),
    }


def _canonical_review_reason(raw_reason: Any) -> str:
    reason = str(raw_reason or "").strip().lower()
    if reason == "caption_match_ambiguous":
        return "source_mismatch"
    if reason == "target_person_not_deterministic":
        return "ambiguous_people_match"
    if reason == "person_assignment_needs_review":
        return "ambiguous_people_match"
    return reason or "unmatched_source_row"


def canonical_review_reason(raw_reason: Any) -> str:
    return _canonical_review_reason(raw_reason)


def _review_reason_counts(
    review_candidates: list[dict[str, Any]],
    bridge_rows: list[dict[str, Any]],
    replacement_candidates: list[dict[str, Any]],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in review_candidates:
        counts[_canonical_review_reason(row.get("reason"))] += 1
    for row in bridge_rows:
        if str(row.get("strategy") or "") == "manual_review":
            counts[_canonical_review_reason(row.get("reason"))] += 1
    if replacement_candidates:
        counts["replacement_pending"] += len(replacement_candidates)
    return dict(sorted(counts.items()))


def _entity_link_counts(imported_records: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in imported_records:
        for target in _safe_list(row.get("link_targets")):
            entity_type = str(target).split(":", 1)[0] or "unknown"
            counts[entity_type] += 1
        if not _safe_list(row.get("link_targets")):
            counts["missing"] += 1
    return dict(sorted(counts.items()))


def _quality_buckets(merged_catalog: list[dict[str, Any]], imported_records: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    imported_by_group = {str(row.get("group_id")): row for row in imported_records}
    for row in merged_catalog:
        group_id = str(row.get("id") or "")
        imported = _safe_dict(imported_by_group.get(group_id))
        if imported.get("replacement_pending"):
            counts["referenced_only"] += 1
        per_source = _safe_dict(row.get("per_source"))
        widths = []
        heights = []
        for source_row in per_source.values():
            source_dict = _safe_dict(source_row)
            if isinstance(source_dict.get("width"), int):
                widths.append(int(source_dict["width"]))
            if isinstance(source_dict.get("height"), int):
                heights.append(int(source_dict["height"]))
        if not widths or not heights:
            counts["missing_dimensions"] += 1
        elif max(widths) < 1000 or max(heights) < 667:
            counts["low_resolution"] += 1
        else:
            counts["usable_dimensions"] += 1
    return dict(sorted(counts.items()))


def _duplicate_groups(
    merged_catalog: list[dict[str, Any]], imported_records: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    keys: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in merged_catalog:
        group_id = str(row.get("id") or "")
        for source_name, source_row in _safe_dict(row.get("per_source")).items():
            source_dict = _safe_dict(source_row)
            for key_type, value in (
                ("source_asset_id", source_dict.get("source_id")),
                ("source_url", source_dict.get("source_url")),
                ("nup_key", source_dict.get("bridge_key") or source_dict.get("nup_filename")),
            ):
                cleaned = str(value or "").strip()
                if cleaned:
                    keys[(key_type, f"{source_name}:{cleaned}")].append(group_id)
    for row in imported_records:
        media_asset_id = str(row.get("media_asset_id") or "").strip()
        if media_asset_id:
            for target in _safe_list(row.get("link_targets")):
                keys[("media_link", f"{target}:{media_asset_id}")].append(str(row.get("group_id") or ""))
    duplicates = []
    for (key_type, key_value), group_ids in sorted(keys.items()):
        unique_group_ids = sorted({group_id for group_id in group_ids if group_id})
        if len(unique_group_ids) > 1:
            duplicates.append({"key_type": key_type, "key": key_value, "group_ids": unique_group_ids})
    return duplicates


def _summarize_nup_lookup_report(report: Mapping[str, Any]) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    for section in ("getty_from_nup_sources", "nbcumv_from_getty_nup"):
        counts: Counter[str] = Counter()
        for row in _safe_list(report.get(section)):
            status = str(_safe_dict(row).get("status") or "unknown").strip().lower() or "unknown"
            counts[status] += 1
        summary[section] = {
            "attempted": sum(counts.values()),
            "added": counts.get("added", 0),
            "missed": counts.get("missed", 0),
            "duplicate": counts.get("duplicate", 0),
        }
    return summary


def build_run_review(artifacts: Mapping[str, Any]) -> dict[str, Any]:
    merged_catalog = [_safe_dict(row) for row in _artifact_list(artifacts, "merged_catalog")]
    bridge_rows = [
        _safe_dict(row)
        for row in (_artifact_list(artifacts, "bridge_rows") or _artifact_list(artifacts, "bridge_table"))
    ]
    imported_records = [_safe_dict(row) for row in _artifact_list(artifacts, "imported_records")]
    review_candidates = [_safe_dict(row) for row in _artifact_list(artifacts, "review_candidates")]
    replacement_candidates = [_safe_dict(row) for row in _artifact_list(artifacts, "replacement_candidates")]
    raw_bravo_rows = [_safe_dict(row) for row in _artifact_list(artifacts, "raw/bravo")]
    nup_lookup_report = _safe_dict(artifacts.get("nup_lookup_report"))
    missing_nup_lookups = [_safe_dict(row) for row in _artifact_list(artifacts, "missing_nup_lookups")]
    review_reason_counts = _review_reason_counts(review_candidates, bridge_rows, replacement_candidates)
    duplicate_groups = _duplicate_groups(merged_catalog, imported_records)
    if duplicate_groups:
        review_reason_counts["duplicate_candidate"] = review_reason_counts.get("duplicate_candidate", 0) + len(
            duplicate_groups
        )
    unmatched_rows = [
        row
        for row in bridge_rows
        if str(row.get("strategy") or "") in {"source_only", "manual_review"} and not row.get("group_id")
    ]
    failed_acquisitions = [
        row for row in merged_catalog if str(_safe_dict(row.get("acquisition")).get("status") or "").lower() == "failed"
    ]
    if failed_acquisitions:
        review_reason_counts["failed_download"] = review_reason_counts.get("failed_download", 0) + len(
            failed_acquisitions
        )
    entity_link_counts = _entity_link_counts(imported_records)
    if entity_link_counts.get("missing"):
        review_reason_counts["missing_entity_link"] = (
            review_reason_counts.get("missing_entity_link", 0) + entity_link_counts["missing"]
        )

    summary = {
        "total_merged_records": len(merged_catalog),
        "total_imported_records": len(imported_records),
        "total_review_candidates": len(review_candidates),
        "total_replacement_pending": len(replacement_candidates),
        "total_duplicate_groups": len(duplicate_groups),
        "total_failed_acquisitions": len(failed_acquisitions),
        "total_missing_nup_lookups": len(missing_nup_lookups),
    }
    return {
        "summary": summary,
        "source_counts": _count_sources(artifacts, merged_catalog),
        "getty_family_backfill": _summarize_nup_lookup_report(nup_lookup_report),
        "bravo_gallery_source_breakdown": _bravo_gallery_source_breakdown(raw_bravo_rows, merged_catalog),
        "missing_nup_lookups": missing_nup_lookups,
        "bridge_strategy_counts": _bridge_strategy_counts(bridge_rows),
        "review_reason_counts": dict(sorted(review_reason_counts.items())),
        "review_reason_labels": REVIEW_REASON_LABELS,
        "entity_link_counts": entity_link_counts,
        "quality_buckets": _quality_buckets(merged_catalog, imported_records),
        "duplicate_groups": duplicate_groups,
        "unmatched_rows": unmatched_rows,
        "replacement_pending": replacement_candidates,
        "failed_acquisitions": failed_acquisitions,
        "recommended_next_actions": _recommended_next_actions(review_reason_counts),
    }


def build_run_review_from_dir(output_root: Path) -> dict[str, Any]:
    return build_run_review(load_run_artifacts(output_root))


def _recommended_next_actions(review_reason_counts: Mapping[str, int]) -> list[str]:
    actions: list[str] = []
    if review_reason_counts.get("ambiguous_people_match"):
        actions.append("Resolve ambiguous people matches before marking gallery rows complete.")
    if review_reason_counts.get("replacement_pending"):
        actions.append("Find approved non-watermarked replacements for Getty reference-only rows.")
    if review_reason_counts.get("duplicate_candidate"):
        actions.append("Review duplicate candidate groups before changing media identity rules.")
    if review_reason_counts.get("failed_download"):
        actions.append("Retry failed downloads after checking source availability.")
    if review_reason_counts.get("missing_entity_link"):
        actions.append("Assign missing entity links or leave records as source-only review rows.")
    return actions
