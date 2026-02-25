#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from scripts.sync import sync_networks_streaming_links as sync_links
from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.utils.env import load_env


@dataclass
class RecomputeSummary:
    scanned: int = 0
    updated: int = 0
    resolved: int = 0
    manual_required: int = 0
    failed: int = 0
    unchanged: int = 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="recompute_network_streaming_completion",
        description="Recompute admin.network_streaming_completion status from policy + current row data.",
    )
    parser.add_argument(
        "--entity-type",
        choices=("network", "streaming", "production"),
        default=None,
        help="Optionally scope recompute to a single entity type.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit.")
    parser.add_argument("--dry-run", action="store_true", help="Print recompute results without writing.")
    parser.add_argument("--verbose", action="store_true", help="Print per-row updates.")
    return parser.parse_args(argv)


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _build_reference_urls(row: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    wikipedia_url = sync_links._normalize_text(row.get("wikipedia_url"))
    if wikipedia_url:
        urls.append(wikipedia_url)
    entity_type = sync_links._normalize_text(row.get("entity_type")).lower()
    entity_id = sync_links._normalize_text(row.get("entity_id"))
    if entity_type == "production" and entity_id:
        urls.append(f"https://www.themoviedb.org/company/{entity_id}")
    return urls


def _status_counter_key(status: str) -> str:
    if status == "resolved":
        return "resolved"
    if status == "failed":
        return "failed"
    return "manual_required"


def run_recompute(args: argparse.Namespace) -> RecomputeSummary:
    load_env()
    db = create_supabase_admin_client()
    summary = RecomputeSummary()
    run_id = f"recompute-resolution-policy-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"

    query = db.schema("admin").table("network_streaming_completion").select("*").order("updated_at", desc=True)
    if args.entity_type:
        query = query.eq("entity_type", args.entity_type)
    if args.limit and args.limit > 0:
        query = query.limit(int(args.limit))

    for row in sync_links._iter_rows_paged(query):
        summary.scanned += 1
        entity_type = sync_links._normalize_text(row.get("entity_type")).lower()
        if entity_type not in {"network", "streaming", "production"}:
            continue
        entity_key = sync_links._normalize_text(row.get("entity_key"))
        entity_id = sync_links._normalize_text(row.get("entity_id"))
        display_name = sync_links._normalize_text(row.get("display_name"))
        wikidata_id = sync_links._normalize_text(row.get("wikidata_id"))
        wikipedia_url = sync_links._normalize_text(row.get("wikipedia_url"))
        hosted_logo_url = sync_links._normalize_text(row.get("hosted_logo_url"))
        hosted_logo_black_url = sync_links._normalize_text(row.get("hosted_logo_black_url"))
        hosted_logo_white_url = sync_links._normalize_text(row.get("hosted_logo_white_url"))
        base_logo_format = sync_links._normalize_text(row.get("base_logo_format")) or "unknown"
        prior_reason = sync_links._normalize_text(row.get("resolution_reason"))
        reason_for_eval = prior_reason if prior_reason in sync_links.HARD_RESOLUTION_FAILURE_REASONS else None

        status, reason, resolution_policy, logo_required = sync_links._build_resolution_status(
            entity_type=entity_type,
            display_name=display_name,
            entity_id=entity_id,
            wikidata_id=wikidata_id,
            wikipedia_url=wikipedia_url,
            hosted_logo_url=hosted_logo_url,
            hosted_logo_black_url=hosted_logo_black_url,
            hosted_logo_white_url=hosted_logo_white_url,
            base_logo_format=base_logo_format,
            reference_urls=_build_reference_urls(row),
            reason=reason_for_eval,
        )

        key = _status_counter_key(status)
        setattr(summary, key, getattr(summary, key) + 1)

        current_status = sync_links._normalize_text(row.get("resolution_status"))
        current_reason = sync_links._normalize_text(row.get("resolution_reason")) or None
        current_policy = sync_links._normalize_text(row.get("resolution_policy")) or "strict"
        current_logo_required = bool(row.get("logo_required"))
        changed = (
            status != current_status
            or reason != current_reason
            or resolution_policy != current_policy
            or logo_required != current_logo_required
        )

        if not changed:
            summary.unchanged += 1
            continue

        if args.verbose:
            print(
                f"recompute_update entity_type={entity_type} entity_key={entity_key} "
                f"status={current_status}->{status} reason={current_reason}->{reason} "
                f"policy={current_policy}->{resolution_policy} logo_required={current_logo_required}->{logo_required}"
            )

        summary.updated += 1
        if args.dry_run:
            continue

        payload = {
            "resolution_status": status,
            "resolution_reason": reason,
            "resolution_policy": resolution_policy,
            "logo_required": logo_required,
            "last_run_id": run_id,
            "last_attempt_at": _now_iso(),
            "updated_at": _now_iso(),
        }
        response = (
            db.schema("admin")
            .table("network_streaming_completion")
            .update(payload)
            .eq("entity_type", entity_type)
            .eq("entity_key", entity_key)
            .execute()
        )
        if hasattr(response, "error") and response.error:
            raise RuntimeError(
                f"failed to update completion row entity_type={entity_type} entity_key={entity_key}: {response.error}"
            )

    return summary


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    summary = run_recompute(args)
    print(f"scanned={summary.scanned}")
    print(f"updated={summary.updated}")
    print(f"unchanged={summary.unchanged}")
    print(f"resolved={summary.resolved}")
    print(f"manual_required={summary.manual_required}")
    print(f"failed={summary.failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
