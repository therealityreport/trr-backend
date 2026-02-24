#!/usr/bin/env python3
"""
Backfill validated person source links for shows with IMDb-backed cast people.

Sequence per show:
1. Cleanup invalid person source links.
2. Rediscover and upsert links with source-driven status/confidence.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any
from urllib.parse import urlparse

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


def _upsert_discovered_links(*, db: Any, show_id: str, actor: str) -> int:
    discovered = admin_show_links._discover_show_links(show_id)
    discovered.extend(admin_show_links._discover_season_links(show_id))
    discovered.extend(admin_show_links._discover_people_links(show_id))

    upserted = 0
    for row in discovered:
        url = str(row.get("url") or "").strip()
        parsed = urlparse(url)
        if not url or not parsed.scheme.startswith("http"):
            continue

        entity_type = str(row.get("entity_type") or "show").strip().lower()
        link_kind = str(row.get("link_kind") or "other").strip().lower()
        status = str(row.get("status") or "pending").strip().lower()
        is_person_source = entity_type == "person" and link_kind in admin_show_links._PERSON_SOURCE_LINK_KINDS
        if is_person_source and status != "approved":
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
            message = str(exc).lower()
            if "duplicate key value violates unique constraint" in message and "entity_links_unique_active" in message:
                continue
            raise
        upserted += 1
    return upserted


def _run_show(*, db: Any, show_id: str, actor: str, apply: bool) -> dict[str, int]:
    if apply:
        cleanup = admin_show_links._cleanup_invalid_person_knowledge_links(show_id)
        discovered = _upsert_discovered_links(db=db, show_id=show_id, actor=actor)
        return {
            "cleanup_scanned": int(cleanup.get("scanned") or 0),
            "cleanup_invalid": int(cleanup.get("invalid") or 0),
            "cleanup_promoted": int(cleanup.get("promoted") or 0),
            "cleanup_deleted": int(cleanup.get("deleted") or 0),
            "cleanup_fetch_errors": int(cleanup.get("validation_failures") or 0),
            "discovered_upserted": discovered,
        }

    scan = admin_show_links._scan_invalid_person_knowledge_links(show_id)
    discovered = admin_show_links._discover_show_links(show_id)
    discovered.extend(admin_show_links._discover_season_links(show_id))
    discovered.extend(admin_show_links._discover_people_links(show_id))
    return {
        "cleanup_scanned": int(scan.get("scanned") or 0),
        "cleanup_invalid": len(scan.get("invalid_rows") or []),
        "cleanup_promoted": len(scan.get("pending_promotions") or []),
        "cleanup_deleted": 0,
        "cleanup_fetch_errors": int(scan.get("validation_failures") or 0),
        "discovered_upserted": len(
            [
                row
                for row in discovered
                if (
                    str(row.get("url") or "").strip()
                    and urlparse(str(row.get("url") or "").strip()).scheme.startswith("http")
                    and not (
                        str(row.get("entity_type") or "").strip().lower() == "person"
                        and str(row.get("link_kind") or "").strip().lower()
                        in admin_show_links._PERSON_SOURCE_LINK_KINDS
                        and str(row.get("status") or "pending").strip().lower() != "approved"
                    )
                )
            ]
        ),
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
        "failed_shows": 0,
    }

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
        ):
            totals[key] += int(stats.get(key) or 0)

        print(
            (
                "show={show_id} cleanup_scanned={cleanup_scanned} cleanup_invalid={cleanup_invalid} "
                "cleanup_promoted={cleanup_promoted} cleanup_deleted={cleanup_deleted} "
                "cleanup_fetch_errors={cleanup_fetch_errors} "
                "discovered_upserted={discovered_upserted}"
            ).format(
                show_id=show_id,
                **stats,
            )
        )

    print("totals:", totals)
    return 0 if totals["failed_shows"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
