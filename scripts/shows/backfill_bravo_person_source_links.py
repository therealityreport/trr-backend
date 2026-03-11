#!/usr/bin/env python3
"""
Backfill validated person source links for shows with IMDb-backed cast people.

Sequence per show:
1. Cleanup invalid person source links.
2. Rediscover and upsert links with source-driven status/confidence.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    from api.routers import admin_show_links
    from trr_backend.db import pg
    from trr_backend.db.admin import create_supabase_admin_client
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:
    # Allow direct script execution without requiring PYTHONPATH=. from the repo root.
    REPO_ROOT = Path(__file__).resolve().parents[2]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from api.routers import admin_show_links
    from trr_backend.db import pg
    from trr_backend.db.admin import create_supabase_admin_client
    from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_bravo_person_source_links",
        description="Run cleanup + rediscovery for person source links across impacted shows.",
    )
    parser.add_argument(
        "--show-id",
        action="append",
        default=[],
        help="Optional show UUID(s). If omitted, impacted shows are auto-selected.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Optional maximum number of shows to process.")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Default is dry-run.")
    parser.add_argument("--actor", default="backfill_script", help="Audit actor value for upserted rows.")
    parser.add_argument(
        "--json-summary",
        default="",
        help="Optional file path for JSON summary output ('-' prints to stdout).",
    )
    parser.add_argument(
        "--warn-fetch-errors",
        type=int,
        default=None,
        help="Optional warning threshold for cleanup fetch errors.",
    )
    parser.add_argument(
        "--fail-fetch-errors",
        type=int,
        default=None,
        help="Optional failure threshold for cleanup fetch errors (exit code 2 when exceeded).",
    )
    parser.add_argument(
        "--warn-pending-person-sources",
        type=int,
        default=None,
        help="Optional warning threshold for pending person source links after run.",
    )
    parser.add_argument(
        "--fail-pending-person-sources",
        type=int,
        default=None,
        help="Optional failure threshold for pending person source links after run (exit code 2 when exceeded).",
    )
    parser.add_argument(
        "--diagnose-missing-person-sources",
        action="store_true",
        help="Emit diagnostics for cast people still missing approved IMDb/TMDb links.",
    )
    parser.add_argument(
        "--diagnose-name",
        action="append",
        default=[],
        help="Optional person name filter for diagnostics. Can be provided multiple times.",
    )
    parser.add_argument(
        "--diagnostics-json",
        default="",
        help="Optional file path for diagnostics output ('-' prints to stdout).",
    )
    return parser.parse_args(argv)


def _list_impacted_show_ids() -> list[str]:
    rows = pg.fetch_all(
        """
        SELECT DISTINCT sc.show_id::text AS id
        FROM core.v_show_cast sc
        JOIN core.people p ON p.id = sc.person_id
        LEFT JOIN core.cast_tmdb ct ON ct.person_id = p.id
        WHERE COALESCE(
            NULLIF(trim(p.external_ids ->> 'imdb'), ''),
            NULLIF(trim(p.external_ids ->> 'imdb_id'), ''),
            NULLIF(trim(ct.imdb_id), '')
        ) IS NOT NULL
        ORDER BY id
        """
    )
    return [str(row.get("id") or "").strip() for row in rows if row.get("id")]


def _upsert_discovered_links(*, db: Any, show_id: str, actor: str) -> dict[str, int]:
    discovered = admin_show_links._discover_show_links(show_id)
    discovered.extend(admin_show_links._discover_season_links(show_id))
    discovered.extend(admin_show_links._discover_people_links(show_id))

    upserted = 0
    skipped_non_http = 0
    skipped_person_source_non_approved = 0
    skipped_duplicate = 0
    for row in discovered:
        url = str(row.get("url") or "").strip()
        parsed = urlparse(url)
        if not url or not parsed.scheme.startswith("http"):
            skipped_non_http += 1
            continue

        entity_type = str(row.get("entity_type") or "show").strip().lower()
        link_kind = admin_show_links._normalize_link_kind(str(row.get("link_kind") or "other").strip().lower())
        status = str(row.get("status") or "pending").strip().lower()
        is_person_source = entity_type == "person" and link_kind in admin_show_links._PERSON_SOURCE_LINK_KINDS
        if is_person_source and status != "approved":
            skipped_person_source_non_approved += 1
            continue
        if status not in {"pending", "approved", "rejected"}:
            status = "pending"
        if is_person_source:
            status = "approved"
        confidence_raw = row.get("confidence")
        if isinstance(confidence_raw, (int, float)):
            confidence = float(confidence_raw)
        else:
            confidence = 0.95 if status == "approved" else 0.65

        try:
            admin_show_links._upsert_link(
                db,
                show_id=show_id,
                entity_type=entity_type,
                entity_id=str(row.get("entity_id") or show_id),
                link_group=str(row.get("link_group") or "other"),
                link_kind=link_kind,
                url=url,
                label=(str(row.get("label")) if row.get("label") else None),
                season_number=int(row.get("season_number") or 0),
                status=status,
                confidence=confidence,
                source=(str(row.get("source")) if row.get("source") else None),
                discovered_by="backfill_script",
                metadata=(row.get("metadata") if isinstance(row.get("metadata"), dict) else {}),
                actor=actor,
            )
        except Exception as exc:  # noqa: BLE001
            if admin_show_links._is_duplicate_violation(exc, constraint="entity_links_unique_active"):
                skipped_duplicate += 1
                continue
            raise
        upserted += 1
    return {
        "upserted": upserted,
        "skipped_non_http": skipped_non_http,
        "skipped_person_source_non_approved": skipped_person_source_non_approved,
        "skipped_duplicate": skipped_duplicate,
    }


def _invalid_reason_counts(scan: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in scan.get("invalid_rows") or []:
        if not isinstance(row, dict):
            continue
        reason = str(row.get("reason") or "unknown").strip() or "unknown"
        counts[reason] = counts.get(reason, 0) + 1
    return counts


def _person_source_link_kinds() -> list[str]:
    return sorted(admin_show_links._PERSON_SOURCE_LINK_KINDS)


def _count_pending_person_source_links(show_ids: list[str]) -> int:
    filtered_ids = [str(show_id).strip() for show_id in show_ids if str(show_id).strip()]
    if not filtered_ids:
        return 0
    rows = pg.fetch_all(
        """
        SELECT COUNT(*)::int AS pending_count
        FROM core.entity_links
        WHERE show_id = ANY(%s::uuid[])
          AND entity_type = 'person'
          AND link_kind = ANY(%s::text[])
          AND status = 'pending'
        """,
        [filtered_ids, _person_source_link_kinds()],
    )
    return int((rows[0] or {}).get("pending_count") or 0) if rows else 0


def _load_show_cast_people_for_diagnostics(show_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT DISTINCT
          p.id::text AS person_id,
          p.full_name AS person_name,
          p.external_ids,
          ct.imdb_id AS cast_tmdb_imdb_id,
          ct.tmdb_id AS cast_tmdb_tmdb_id,
          ct.wikidata_id AS cast_tmdb_wikidata_id
        FROM core.v_show_cast sc
        JOIN core.people p ON p.id = sc.person_id
        LEFT JOIN core.cast_tmdb ct ON ct.person_id = p.id
        WHERE sc.show_id = %s
        ORDER BY person_name NULLS LAST, person_id
        """,
        [show_id],
    )


def _load_person_link_state_by_person_id(show_id: str) -> dict[str, dict[str, dict[str, str | None]]]:
    rows = pg.fetch_all(
        """
        SELECT DISTINCT ON (entity_id, link_kind)
          entity_id::text AS person_id,
          link_kind,
          status,
          url
        FROM core.entity_links
        WHERE show_id = %s
          AND entity_type = 'person'
          AND link_kind = ANY(%s::text[])
        ORDER BY entity_id, link_kind, (status = 'approved') DESC, updated_at DESC
        """,
        [show_id, ["imdb", "tmdb"]],
    )
    by_person: dict[str, dict[str, dict[str, str | None]]] = {}
    for row in rows:
        person_id = str(row.get("person_id") or "").strip()
        link_kind = admin_show_links._normalize_link_kind(str(row.get("link_kind") or "").strip().lower())
        if not person_id or link_kind not in {"imdb", "tmdb"}:
            continue
        by_person.setdefault(person_id, {})[link_kind] = {
            "status": str(row.get("status") or "").strip().lower() or None,
            "url": str(row.get("url") or "").strip() or None,
        }
    return by_person


def _owner_signal_for_candidate(*, kind: str, candidate_url: str, expected_name: str) -> bool | None:
    status_code, html, final_url, _ = admin_show_links._fetch_html_with_status(
        candidate_url,
        timeout=admin_show_links._source_timeout_seconds(kind),
    )
    if status_code is None or not html:
        return None
    resolved_url = final_url or candidate_url
    return bool(admin_show_links._person_page_matches_expected_name(expected_name, html, resolved_url))


def _build_missing_source_reason(*, identifier: str | None, outcome: str) -> str:
    if not identifier:
        return "missing_external_id"
    if outcome == "valid":
        return "valid_but_not_persisted"
    if outcome == "fetch_error":
        return "unverifiable_fetch_error"
    return "invalid_or_owner_mismatch"


def _diagnose_missing_person_sources(*, show_ids: list[str], names: list[str] | None = None) -> list[dict[str, Any]]:
    filtered_show_ids = [str(show_id).strip() for show_id in show_ids if str(show_id).strip()]
    name_filters = {str(name).strip().casefold() for name in (names or []) if str(name).strip()}
    diagnostics: list[dict[str, Any]] = []

    for show_id in filtered_show_ids:
        people_rows = _load_show_cast_people_for_diagnostics(show_id)
        link_state_by_person = _load_person_link_state_by_person_id(show_id)
        for row in people_rows:
            person_id = str(row.get("person_id") or "").strip()
            person_name = str(row.get("person_name") or "").strip()
            if not person_id or not person_name:
                continue
            if name_filters and person_name.casefold() not in name_filters:
                continue

            state = link_state_by_person.get(person_id, {})
            imdb_state = state.get("imdb") or {}
            tmdb_state = state.get("tmdb") or {}
            has_approved_imdb = imdb_state.get("status") == "approved"
            has_approved_tmdb = tmdb_state.get("status") == "approved"
            if has_approved_imdb and has_approved_tmdb:
                continue

            external_ids = row.get("external_ids") if isinstance(row.get("external_ids"), dict) else {}
            imdb_id, imdb_id_source = admin_show_links._resolve_person_external_identifier(
                external_ids,
                keys=("imdb", "imdb_id"),
                fallback_value=row.get("cast_tmdb_imdb_id"),
                extractor=admin_show_links._extract_imdb_person_id,
            )
            tmdb_id, tmdb_id_source = admin_show_links._resolve_person_external_identifier(
                external_ids,
                keys=("tmdb", "tmdb_id"),
                fallback_value=row.get("cast_tmdb_tmdb_id"),
                extractor=admin_show_links._extract_tmdb_person_id,
            )

            imdb_candidate = f"https://www.imdb.com/name/{imdb_id}/" if imdb_id else None
            tmdb_candidate = f"https://www.themoviedb.org/person/{tmdb_id}" if tmdb_id else None

            imdb_outcome = "not_checked"
            imdb_resolved = None
            imdb_owner_match = None
            if imdb_candidate and not has_approved_imdb:
                imdb_resolved, imdb_outcome = admin_show_links._validate_person_knowledge_url(
                    imdb_candidate,
                    kind="imdb",
                    expected_name=person_name,
                )
                imdb_owner_match = _owner_signal_for_candidate(
                    kind="imdb",
                    candidate_url=imdb_candidate,
                    expected_name=person_name,
                )

            tmdb_outcome = "not_checked"
            tmdb_resolved = None
            tmdb_owner_match = None
            if tmdb_candidate and not has_approved_tmdb:
                tmdb_resolved, tmdb_outcome = admin_show_links._validate_person_knowledge_url(
                    tmdb_candidate,
                    kind="tmdb",
                    expected_name=person_name,
                )
                tmdb_owner_match = _owner_signal_for_candidate(
                    kind="tmdb",
                    candidate_url=tmdb_candidate,
                    expected_name=person_name,
                )

            diagnostics.append(
                {
                    "show_id": show_id,
                    "person_id": person_id,
                    "person_name": person_name,
                    "has_approved_imdb": has_approved_imdb,
                    "has_approved_tmdb": has_approved_tmdb,
                    "existing_imdb_status": imdb_state.get("status"),
                    "existing_imdb_url": imdb_state.get("url"),
                    "existing_tmdb_status": tmdb_state.get("status"),
                    "existing_tmdb_url": tmdb_state.get("url"),
                    "imdb_id": imdb_id,
                    "imdb_id_source": imdb_id_source,
                    "tmdb_id": tmdb_id,
                    "tmdb_id_source": tmdb_id_source,
                    "imdb_candidate_url": imdb_candidate,
                    "tmdb_candidate_url": tmdb_candidate,
                    "imdb_validation_outcome": imdb_outcome,
                    "imdb_validation_resolved_url": imdb_resolved,
                    "imdb_owner_match_signal": imdb_owner_match,
                    "imdb_missing_reason": (
                        None
                        if has_approved_imdb
                        else _build_missing_source_reason(
                            identifier=imdb_id,
                            outcome=imdb_outcome,
                        )
                    ),
                    "tmdb_validation_outcome": tmdb_outcome,
                    "tmdb_validation_resolved_url": tmdb_resolved,
                    "tmdb_owner_match_signal": tmdb_owner_match,
                    "tmdb_missing_reason": (
                        None
                        if has_approved_tmdb
                        else _build_missing_source_reason(
                            identifier=tmdb_id,
                            outcome=tmdb_outcome,
                        )
                    ),
                }
            )
    diagnostics.sort(key=lambda row: (str(row.get("person_name") or ""), str(row.get("show_id") or "")))
    return diagnostics


def _write_json_payload(path: str, payload: Any) -> None:
    summary_json = json.dumps(payload, indent=2, sort_keys=True)
    if path.strip() == "-":
        print(summary_json)
        return
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(summary_json)
    print(f"json_written={path}")


def _threshold_warning(*, value: int, threshold: int | None, label: str) -> str | None:
    if threshold is None:
        return None
    if value > int(threshold):
        return f"warning: {label}={value} exceeds threshold={int(threshold)}"
    return None


def _threshold_failure(*, value: int, threshold: int | None, label: str) -> str | None:
    if threshold is None:
        return None
    if value > int(threshold):
        return f"error: {label}={value} exceeds threshold={int(threshold)}"
    return None


def _run_show(*, db: Any, show_id: str, actor: str, apply: bool) -> dict[str, Any]:
    if apply:
        cleanup = admin_show_links._cleanup_invalid_person_knowledge_links(show_id)
        discovered = _upsert_discovered_links(db=db, show_id=show_id, actor=actor)
        return {
            "cleanup_scanned": int(cleanup.get("scanned") or 0),
            "cleanup_invalid": int(cleanup.get("invalid") or 0),
            "cleanup_promoted": int(cleanup.get("promoted") or 0),
            "cleanup_deleted": int(cleanup.get("deleted") or 0),
            "cleanup_fetch_errors": int(cleanup.get("validation_failures") or 0),
            "discovered_upserted": int(discovered.get("upserted") or 0),
            "discovery_skipped_non_http": int(discovered.get("skipped_non_http") or 0),
            "discovery_skipped_person_source_non_approved": int(
                discovered.get("skipped_person_source_non_approved") or 0
            ),
            "discovery_skipped_duplicate": int(discovered.get("skipped_duplicate") or 0),
            "invalid_reason_counts": _invalid_reason_counts({"invalid_rows": []}),
        }

    scan = admin_show_links._scan_invalid_person_knowledge_links(show_id)
    discovered = admin_show_links._discover_show_links(show_id)
    discovered.extend(admin_show_links._discover_season_links(show_id))
    discovered.extend(admin_show_links._discover_people_links(show_id))
    discovered_upserted = 0
    discovery_skipped_non_http = 0
    discovery_skipped_person_source_non_approved = 0
    for row in discovered:
        url = str(row.get("url") or "").strip()
        if not url or not urlparse(url).scheme.startswith("http"):
            discovery_skipped_non_http += 1
            continue
        entity_type = str(row.get("entity_type") or "").strip().lower()
        link_kind = admin_show_links._normalize_link_kind(str(row.get("link_kind") or "").strip().lower())
        status = str(row.get("status") or "pending").strip().lower()
        if entity_type == "person" and link_kind in admin_show_links._PERSON_SOURCE_LINK_KINDS and status != "approved":
            discovery_skipped_person_source_non_approved += 1
            continue
        discovered_upserted += 1
    return {
        "cleanup_scanned": int(scan.get("scanned") or 0),
        "cleanup_invalid": len(scan.get("invalid_rows") or []),
        "cleanup_promoted": len(scan.get("pending_promotions") or []),
        "cleanup_deleted": 0,
        "cleanup_fetch_errors": int(scan.get("validation_failures") or 0),
        "discovered_upserted": discovered_upserted,
        "discovery_skipped_non_http": discovery_skipped_non_http,
        "discovery_skipped_person_source_non_approved": discovery_skipped_person_source_non_approved,
        "discovery_skipped_duplicate": 0,
        "invalid_reason_counts": _invalid_reason_counts(scan),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()
    db = create_supabase_admin_client()

    selected_ids = [str(value).strip() for value in args.show_id if str(value).strip()]
    show_ids = selected_ids if selected_ids else _list_impacted_show_ids()
    if args.limit and args.limit > 0:
        show_ids = show_ids[: args.limit]
    if not show_ids:
        print("No impacted shows found.")
        return 0

    mode = "apply" if args.apply else "dry-run"
    print(f"mode: {mode}")
    print(f"shows: {len(show_ids)}")

    totals = {
        "cleanup_scanned": 0,
        "cleanup_invalid": 0,
        "cleanup_promoted": 0,
        "cleanup_deleted": 0,
        "cleanup_fetch_errors": 0,
        "discovered_upserted": 0,
        "discovery_skipped_non_http": 0,
        "discovery_skipped_person_source_non_approved": 0,
        "discovery_skipped_duplicate": 0,
        "failed_shows": 0,
    }
    reason_totals: dict[str, int] = {}
    show_summaries: list[dict[str, Any]] = []

    for show_id in show_ids:
        try:
            stats = _run_show(db=db, show_id=show_id, actor=args.actor, apply=args.apply)
        except Exception as exc:  # noqa: BLE001
            totals["failed_shows"] += 1
            print(f"show={show_id} failed error={exc}")
            continue

        for key in (
            "cleanup_scanned",
            "cleanup_invalid",
            "cleanup_promoted",
            "cleanup_deleted",
            "cleanup_fetch_errors",
            "discovered_upserted",
            "discovery_skipped_non_http",
            "discovery_skipped_person_source_non_approved",
            "discovery_skipped_duplicate",
        ):
            totals[key] += int(stats.get(key) or 0)
        for reason, count in (stats.get("invalid_reason_counts") or {}).items():
            reason_totals[str(reason)] = reason_totals.get(str(reason), 0) + int(count or 0)
        show_summaries.append({"show_id": show_id, **stats})

        print(
            (
                "show={show_id} cleanup_scanned={cleanup_scanned} cleanup_invalid={cleanup_invalid} "
                "cleanup_promoted={cleanup_promoted} cleanup_deleted={cleanup_deleted} "
                "cleanup_fetch_errors={cleanup_fetch_errors} "
                "discovered_upserted={discovered_upserted} "
                "discovery_skipped_non_http={discovery_skipped_non_http} "
                "discovery_skipped_person_source_non_approved={discovery_skipped_person_source_non_approved} "
                "discovery_skipped_duplicate={discovery_skipped_duplicate}"
            ).format(
                show_id=show_id,
                **stats,
            )
        )

    print("reason_totals:", reason_totals)
    print("totals:", totals)
    pending_person_source_links = _count_pending_person_source_links(show_ids)
    print(f"pending_person_source_links={pending_person_source_links}")

    diagnostics_rows: list[dict[str, Any]] = []
    diagnostics_summary: dict[str, Any] | None = None
    if args.diagnose_missing_person_sources:
        diagnostics_rows = _diagnose_missing_person_sources(show_ids=show_ids, names=args.diagnose_name)
        by_reason: dict[str, int] = {}
        for row in diagnostics_rows:
            for key in ("imdb_missing_reason", "tmdb_missing_reason"):
                reason = str(row.get(key) or "").strip()
                if reason:
                    by_reason[reason] = by_reason.get(reason, 0) + 1
        diagnostics_summary = {
            "rows": len(diagnostics_rows),
            "filtered_names": [str(name).strip() for name in args.diagnose_name if str(name).strip()],
            "reason_counts": dict(sorted(by_reason.items())),
        }
        print("diagnostics:", diagnostics_summary)
        for row in diagnostics_rows[:25]:
            print(
                (
                    "diagnostic show_id={show_id} person={person_name} "
                    "imdb_status={existing_imdb_status} imdb_reason={imdb_missing_reason} "
                    "tmdb_status={existing_tmdb_status} tmdb_reason={tmdb_missing_reason}"
                ).format(**row)
            )
        if len(diagnostics_rows) > 25:
            print(f"diagnostic_rows_truncated={len(diagnostics_rows) - 25}")

    warnings: list[str] = []
    errors: list[str] = []
    fetch_errors = int(totals.get("cleanup_fetch_errors") or 0)
    warning = _threshold_warning(
        value=fetch_errors,
        threshold=args.warn_fetch_errors,
        label="cleanup_fetch_errors",
    )
    if warning:
        warnings.append(warning)
    failure = _threshold_failure(
        value=fetch_errors,
        threshold=args.fail_fetch_errors,
        label="cleanup_fetch_errors",
    )
    if failure:
        errors.append(failure)

    warning = _threshold_warning(
        value=pending_person_source_links,
        threshold=args.warn_pending_person_sources,
        label="pending_person_source_links",
    )
    if warning:
        warnings.append(warning)
    failure = _threshold_failure(
        value=pending_person_source_links,
        threshold=args.fail_pending_person_sources,
        label="pending_person_source_links",
    )
    if failure:
        errors.append(failure)

    for message in warnings:
        print(message)
    for message in errors:
        print(message)

    summary_payload = {
        "mode": mode,
        "shows_count": len(show_ids),
        "totals": totals,
        "reason_totals": reason_totals,
        "shows": show_summaries,
        "pending_person_source_links": pending_person_source_links,
    }
    if diagnostics_summary is not None:
        summary_payload["diagnostics"] = diagnostics_summary
    if args.json_summary:
        _write_json_payload(args.json_summary, summary_payload)
        print(f"json_summary_written={args.json_summary}")
    if args.diagnostics_json:
        _write_json_payload(
            args.diagnostics_json,
            {
                "mode": mode,
                "shows_count": len(show_ids),
                "rows": diagnostics_rows,
                "summary": diagnostics_summary or {"rows": 0, "reason_counts": {}},
            },
        )
        print(f"diagnostics_json_written={args.diagnostics_json}")

    if totals["failed_shows"] > 0:
        return 1
    if errors:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
