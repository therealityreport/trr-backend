#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

REQUIRED_COLUMNS = {
    "workload",
    "owner",
    "schema",
    "table",
    "index",
    "current_review_status",
    "current_idx_scan",
    "idx_tup_read",
    "idx_tup_fetch",
    "index_size",
    "table_size",
    "advisor_reported",
    "exclude_reasons",
    "migration_path",
    "live_indexdef",
    "constraint_status",
    "uniqueness_status",
    "fk_hardening_status",
    "recent_migration_status",
    "app_or_job_references_found",
    "reviewed_routes_or_jobs",
    "query_pattern_labels",
    "query_pattern_supported",
    "decision",
    "decision_reason",
    "approved_to_drop",
    "approved_by",
    "approval_reason",
    "stats_window_checked_at",
    "rollback_sql",
    "drop_sql_if_approved",
    "replacement_index_sql_if_needed",
    "risk_level",
    "phase3_batch_recommendation",
    "notes",
}

APPROVED_REQUIRED_COLUMNS = {
    "approved_by",
    "approval_reason",
    "reviewed_routes_or_jobs",
    "stats_window_checked_at",
    "rollback_sql",
    "drop_sql_if_approved",
    "risk_level",
    "phase3_batch_recommendation",
}

ALLOWED_LABELS = {
    "integrity_constraint",
    "fk_hardening",
    "dedupe_unique",
    "primary_key_lookup",
    "public_page_read",
    "admin_tooling",
    "backfill_ingest",
    "worker_claim_hotpath",
    "worker_heartbeat",
    "media_mirror_queue",
    "comment_thread_lookup",
    "post_feed_lookup",
    "hashtag_search",
    "text_search",
    "handle_search",
    "leaderboard_candidate",
    "survey_response_flow",
    "ml_review_flow",
    "catalog_media_lookup",
    "recent_migration",
    "unknown_needs_manual_review",
}

SOCIAL_SEARCH_PATTERNS = (
    "_search_hashtags_idx",
    "_search_text_trgm_idx",
    "_search_handles_idx",
    "_search_handle_identities_idx",
)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _labels(row: dict[str, str]) -> set[str]:
    raw = row.get("query_pattern_labels", "")
    return {part.strip() for part in raw.replace(";", ",").split(",") if part.strip()}


def _architecture_unresolved(path: Path | None) -> bool:
    if path is None:
        return True
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    return "Status: unresolved" in text or "unresolved / stub" in text or not text.strip()


def _stats_window_is_sufficient(path: Path | None) -> bool:
    if path is None:
        return False
    data = json.loads(path.read_text(encoding="utf-8"))
    return bool(data.get("stats_window_is_7_days_or_more"))


def _row_key(row: dict[str, str]) -> tuple[str, str, str]:
    return (row.get("schema", ""), row.get("table", ""), row.get("index", ""))


def validate(args: argparse.Namespace) -> list[str]:
    errors: list[str] = []
    rows = _read_csv(args.matrix)
    source_rows = _read_csv(args.source)
    fieldnames = set(rows[0].keys()) if rows else set()
    missing_columns = sorted(REQUIRED_COLUMNS - fieldnames)
    if missing_columns:
        errors.append(f"missing required columns: {', '.join(missing_columns)}")
        return errors

    expected_count = args.expected_row_count if args.expected_row_count is not None else len(source_rows)
    if len(rows) != expected_count:
        errors.append(f"matrix row count {len(rows)} does not match expected {expected_count}")
    if len(rows) != len(source_rows):
        errors.append(f"matrix row count {len(rows)} does not match source row count {len(source_rows)}")

    if [_row_key(row) for row in rows] != [_row_key(row) for row in source_rows]:
        errors.append("matrix row order/identity does not match source rows")

    stats_sufficient = _stats_window_is_sufficient(args.stats_window)
    architecture_unresolved = _architecture_unresolved(args.architecture_stub)

    for line_no, row in enumerate(rows, start=2):
        prefix = f"row {line_no} {row.get('schema')}.{row.get('table')}.{row.get('index')}: "
        labels = _labels(row)
        if not row.get("decision", "").strip():
            errors.append(prefix + "missing decision")
        if not labels:
            errors.append(prefix + "missing query_pattern_labels")
        invalid_labels = sorted(labels - ALLOWED_LABELS)
        if invalid_labels:
            errors.append(prefix + f"invalid query_pattern_labels {invalid_labels}")
        approved = row.get("approved_to_drop", "").strip().lower() == "yes"
        status = row.get("current_review_status", "").strip()
        decision = row.get("decision", "").strip()
        idx_scan = int(row.get("current_idx_scan") or 0)
        exclude_reasons = row.get("exclude_reasons", "")
        notes = row.get("notes", "")

        if approved:
            missing = sorted(col for col in APPROVED_REQUIRED_COLUMNS if not row.get(col, "").strip())
            if missing:
                errors.append(prefix + f"approved_to_drop=yes missing {missing}")
            if not row.get("drop_sql_if_approved", "").startswith("DROP INDEX CONCURRENTLY IF EXISTS"):
                errors.append(prefix + "approved drop SQL must use DROP INDEX CONCURRENTLY IF EXISTS")
            if idx_scan == 0 and not stats_sufficient and "owner_canary_risk_accepted" not in notes:
                errors.append(
                    prefix + "zero-scan approval requires 7-day stats window or owner_canary_risk_accepted note"
                )

        if status == "excluded" and approved:
            if (
                not {"integrity_replacement_proof", "replacement_verified"} & labels
                and "explicit_integrity_proof" not in notes
            ):
                errors.append(prefix + "excluded rows cannot be approved without explicit replacement/integrity proof")
        if status == "defer:idx_scan_nonzero" and decision not in {
            "keep_because_nonzero_usage",
            "keep_current_index",
            "replace_with_better_index",
        }:
            if "nonzero_exception_evidence" not in notes:
                errors.append(prefix + "nonzero-usage row has unsupported decision without exception evidence")
        if status == "defer:idx_scan_nonzero" and approved and "nonzero_exception_evidence" not in notes:
            errors.append(prefix + "nonzero-usage row cannot be approved without exception evidence")
        if row.get("schema") == "social" and any(pattern in row.get("index", "") for pattern in SOCIAL_SEARCH_PATTERNS):
            if approved and architecture_unresolved and "confirmed_unrelated_to_hashtag_architecture" not in notes:
                errors.append(prefix + "social hashtag/search index approved while architecture is unresolved")
        if "hashtag_search" in labels or "text_search" in labels or "handle_search" in labels:
            if approved and architecture_unresolved and "confirmed_unrelated_to_hashtag_architecture" not in notes:
                errors.append(prefix + "search-labeled row approved while architecture is unresolved")
        if "primary-key" in exclude_reasons and approved:
            errors.append(prefix + "primary-key exclusion must not be approved")

    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the unused-index decision matrix.")
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--stats-window", type=Path, required=True)
    parser.add_argument("--architecture-stub", type=Path, required=True)
    parser.add_argument("--expected-row-count", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    errors = validate(parse_args())
    if errors:
        for error in errors:
            print(f"[unused-index-matrix] ERROR: {error}", file=sys.stderr)
        return 1
    print("[unused-index-matrix] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
