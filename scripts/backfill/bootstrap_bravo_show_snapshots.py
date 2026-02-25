#!/usr/bin/env python3
"""Bootstrap Bravo show snapshots for Bravo-linked shows missing persisted snapshots."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, cast

try:
    from fastapi import HTTPException

    from api.routers.admin_show_bravo import (
        _assert_show_sync_ready_for_bravo,
        _build_show_cast_index,
        _normalize_bundle_for_show,
        _show_exists,
        _sync_bravo_video_thumbnails,
        _to_iso_now,
        _upsert_show_snapshot,
        parse_bravo_show_bundle,
    )
    from trr_backend.db import pg
    from trr_backend.db.admin import create_supabase_admin_client
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:
    REPO_ROOT = Path(__file__).resolve().parents[2]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    from fastapi import HTTPException

    from api.routers.admin_show_bravo import (
        _assert_show_sync_ready_for_bravo,
        _build_show_cast_index,
        _normalize_bundle_for_show,
        _show_exists,
        _sync_bravo_video_thumbnails,
        _to_iso_now,
        _upsert_show_snapshot,
        parse_bravo_show_bundle,
    )
    from trr_backend.db import pg
    from trr_backend.db.admin import create_supabase_admin_client
    from trr_backend.utils.env import load_env


_BRAVO_SOURCE_ID = "bravo"
_BRAVO_VARIANT = "default"
_RUNNER = "scripts/backfill/bootstrap_bravo_show_snapshots.py"


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bootstrap_bravo_show_snapshots",
        description="Bootstrap Bravo snapshots for Bravo-linked shows missing snapshots.",
    )
    parser.add_argument(
        "--show-id",
        action="append",
        default=[],
        help="Optional show UUID(s). If omitted, all eligible shows are considered.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Optional cap on number of target shows.")
    parser.add_argument("--dry-run", action="store_true", help="Report targets without writing snapshots.")
    parser.add_argument(
        "--sync-thumbnails",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Immediately sync/mirror Bravo video thumbnails for bootstrapped snapshots.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing remaining shows when a show fails.",
    )
    parser.add_argument(
        "--json-summary",
        default="",
        help="Optional JSON summary output path ('-' prints to stdout).",
    )
    parser.add_argument(
        "--actor",
        default="bootstrap_bravo_show_snapshots",
        help="Actor label stored in bootstrap metadata.",
    )
    return parser.parse_args(argv)


def _normalized_show_ids(values: list[str] | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        cleaned = str(value or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def list_bootstrap_targets(*, show_ids: list[str] | None = None, limit: int = 0) -> list[dict[str, Any]]:
    normalized_show_ids = _normalized_show_ids(show_ids)
    filters = [
        "l.entity_type = 'show'",
        "COALESCE(l.season_number, 0) = 0",
        "LOWER(COALESCE(l.status, '')) IN ('approved','pending')",
        "LOWER(COALESCE(l.url, '')) LIKE '%%bravotv.com%%'",
    ]
    where_params: list[Any] = []
    if normalized_show_ids:
        filters.append("l.show_id::text = ANY(%s::text[])")
        where_params.append(normalized_show_ids)

    where_sql = " AND ".join(filters)
    limit_sql = ""
    limit_params: list[Any] = []
    if limit and limit > 0:
        limit_sql = "LIMIT %s"
        limit_params.append(int(limit))

    rows = pg.fetch_all(
        f"""
        WITH ranked_links AS (
          SELECT
            l.show_id::text AS show_id,
            s.name AS show_name,
            l.url,
            l.link_kind,
            l.status,
            l.created_at,
            l.updated_at,
            ROW_NUMBER() OVER (
              PARTITION BY l.show_id
              ORDER BY
                CASE LOWER(COALESCE(l.status, ''))
                  WHEN 'approved' THEN 0
                  WHEN 'pending' THEN 1
                  ELSE 2
                END,
                CASE LOWER(COALESCE(l.link_kind, ''))
                  WHEN 'bravo' THEN 0
                  WHEN 'official' THEN 0
                  WHEN 'official_site' THEN 0
                  WHEN 'website' THEN 1
                  WHEN 'homepage' THEN 1
                  ELSE 5
                END,
                COALESCE(l.updated_at, l.created_at) DESC NULLS LAST,
                l.created_at DESC NULLS LAST
            ) AS rn
          FROM core.entity_links l
          JOIN core.shows s ON s.id = l.show_id
          WHERE {where_sql}
        )
        SELECT
          r.show_id,
          r.show_name,
          r.url,
          r.link_kind,
          r.status,
          r.created_at,
          r.updated_at
        FROM ranked_links r
        LEFT JOIN core.show_source_latest ssl
          ON ssl.show_id = r.show_id::uuid
         AND ssl.source_id = %s
         AND ssl.variant = %s
        WHERE r.rn = 1
          AND ssl.show_id IS NULL
        ORDER BY r.show_name NULLS LAST, r.show_id
        {limit_sql}
        """,
        [*where_params, _BRAVO_SOURCE_ID, _BRAVO_VARIANT, *limit_params],
    )

    out: list[dict[str, Any]] = []
    for row in rows:
        show_id = str(row.get("show_id") or "").strip()
        show_url = str(row.get("url") or "").strip()
        if not show_id or not show_url:
            continue
        out.append(
            {
                "show_id": show_id,
                "show_name": str(row.get("show_name") or "").strip() or None,
                "show_url": show_url,
                "link_kind": str(row.get("link_kind") or "").strip() or None,
                "link_status": str(row.get("status") or "").strip() or None,
            }
        )
    return out


def _bootstrap_single_show(
    *,
    db: Any,
    show_id: str,
    show_url: str,
    show_name: str | None,
    link_kind: str | None,
    link_status: str | None,
    dry_run: bool,
    sync_thumbnails: bool,
    actor: str,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "show_id": show_id,
        "show_name": show_name,
        "show_url": show_url,
        "status": "ok",
        "error": None,
        "skip_reason": None,
        "snapshot": None,
        "counts": {
            "show_videos": 0,
            "show_news": 0,
            "person_videos": 0,
            "person_news": 0,
            "video_thumbnail_attempted": 0,
            "video_thumbnail_synced": 0,
            "video_thumbnail_failed": 0,
            "video_thumbnail_missing_source": 0,
        },
    }

    if not _show_exists(db, show_id):
        result["status"] = "skipped"
        result["skip_reason"] = "show_not_found"
        return result

    try:
        _assert_show_sync_ready_for_bravo(db, show_id)
    except HTTPException as exc:
        if int(exc.status_code) == 409:
            result["status"] = "skipped"
            result["skip_reason"] = "sync_readiness_unmet"
            result["error"] = str(exc.detail)
            return result
        raise

    if dry_run:
        result["status"] = "dry_run"
        result["skip_reason"] = "dry_run"
        return result

    bundle = parse_bravo_show_bundle(
        str(show_url),
        include_people=False,
        include_videos=True,
        include_news=True,
        include_person_related_content=False,
        hydrate_person_related_dates=False,
    )

    people_refs = [
        {
            "person_id": str(row.get("person_id") or "").strip() or None,
            "person_name": str(row.get("person_name") or "").strip() or None,
            "person_url": None,
        }
        for row in _build_show_cast_index(db, show_id)
    ]
    normalized = _normalize_bundle_for_show(bundle, people_refs=people_refs)

    thumbnail_sync_summary: dict[str, Any]
    if sync_thumbnails:
        thumbnail_sync_summary = _sync_bravo_video_thumbnails(
            db=db,
            admin_user=cast(Any, None),
            show_id=show_id,
            normalized=normalized,
            force=False,
            refresh_from_clip_metadata=True,
        )
    else:
        thumbnail_sync_summary = {
            "attempted": 0,
            "synced": 0,
            "failed": 0,
            "missing_source": 0,
            "imported": 0,
            "skipped": 0,
            "refreshed_from_clip": 0,
            "remaining": 0,
            "errors": [],
            "skipped_reason": "thumbnail_sync_disabled",
        }

    normalized["video_thumbnail_sync"] = {
        **thumbnail_sync_summary,
        "forced": False,
        "synced_at": _to_iso_now(),
        "runner": _RUNNER,
    }

    payload = {
        "source": _BRAVO_SOURCE_ID,
        "variant": _BRAVO_VARIANT,
        "show_id": show_id,
        "show_url": str(show_url),
        "cast_only": False,
        "fetched_at": _to_iso_now(),
        "normalized": {
            **normalized,
            "bootstrap": {
                "runner": _RUNNER,
                "actor": actor,
                "bootstrapped_at": _to_iso_now(),
                "show_link_kind": link_kind,
                "show_link_status": link_status,
            },
        },
        "raw": bundle.get("raw") or bundle,
        "person_url_map": {},
    }

    snapshot = _upsert_show_snapshot(db, show_id=show_id, payload=payload)

    result["status"] = "created"
    result["snapshot"] = snapshot
    result["counts"] = {
        "show_videos": len(normalized.get("videos_show") or []),
        "show_news": len(normalized.get("news_show") or []),
        "person_videos": len(normalized.get("videos_person") or []),
        "person_news": len(normalized.get("news_person") or []),
        "video_thumbnail_attempted": int(thumbnail_sync_summary.get("attempted") or 0),
        "video_thumbnail_synced": int(thumbnail_sync_summary.get("synced") or 0),
        "video_thumbnail_failed": int(thumbnail_sync_summary.get("failed") or 0),
        "video_thumbnail_missing_source": int(thumbnail_sync_summary.get("missing_source") or 0),
    }
    return result


def run_bootstrap(
    *,
    db: Any,
    show_ids: list[str] | None = None,
    limit: int = 0,
    dry_run: bool = False,
    sync_thumbnails: bool = True,
    continue_on_error: bool = False,
    actor: str = "bootstrap_bravo_show_snapshots",
) -> dict[str, Any]:
    targets = list_bootstrap_targets(show_ids=show_ids, limit=limit)

    summary: dict[str, Any] = {
        "runner": _RUNNER,
        "generated_at": _to_iso_now(),
        "dry_run": bool(dry_run),
        "sync_thumbnails": bool(sync_thumbnails),
        "totals": {
            "shows_scanned": len(targets),
            "shows_processed": 0,
            "shows_succeeded": 0,
            "shows_failed": 0,
            "shows_skipped": 0,
            "bootstrap_created": 0,
            "bootstrap_dry_run": 0,
            "video_thumbnail_attempted": 0,
            "video_thumbnail_synced": 0,
            "video_thumbnail_failed": 0,
            "video_thumbnail_missing_source": 0,
        },
        "skip_reasons": {},
        "shows": [],
    }

    for target in targets:
        show_id = str(target.get("show_id") or "").strip()
        show_url = str(target.get("show_url") or "").strip()
        if not show_id or not show_url:
            continue

        try:
            result = _bootstrap_single_show(
                db=db,
                show_id=show_id,
                show_url=show_url,
                show_name=(str(target.get("show_name") or "").strip() or None),
                link_kind=(str(target.get("link_kind") or "").strip() or None),
                link_status=(str(target.get("link_status") or "").strip() or None),
                dry_run=dry_run,
                sync_thumbnails=sync_thumbnails,
                actor=actor,
            )
        except Exception as exc:  # noqa: BLE001
            result = {
                "show_id": show_id,
                "show_name": target.get("show_name"),
                "show_url": show_url,
                "status": "failed",
                "error": str(exc),
                "skip_reason": None,
                "snapshot": None,
                "counts": {},
            }
            if not continue_on_error:
                summary["shows"].append(result)
                summary["totals"]["shows_processed"] += 1
                summary["totals"]["shows_failed"] += 1
                break

        summary["shows"].append(result)
        summary["totals"]["shows_processed"] += 1

        status = str(result.get("status") or "").strip().lower()
        if status in {"created", "dry_run"}:
            summary["totals"]["shows_succeeded"] += 1
        elif status == "skipped":
            summary["totals"]["shows_skipped"] += 1
            reason = str(result.get("skip_reason") or "unknown").strip() or "unknown"
            summary["skip_reasons"][reason] = int(summary["skip_reasons"].get(reason, 0)) + 1
        else:
            summary["totals"]["shows_failed"] += 1

        if status == "created":
            summary["totals"]["bootstrap_created"] += 1
        elif status == "dry_run":
            summary["totals"]["bootstrap_dry_run"] += 1

        counts = result.get("counts") if isinstance(result.get("counts"), dict) else {}
        summary["totals"]["video_thumbnail_attempted"] += int(counts.get("video_thumbnail_attempted") or 0)
        summary["totals"]["video_thumbnail_synced"] += int(counts.get("video_thumbnail_synced") or 0)
        summary["totals"]["video_thumbnail_failed"] += int(counts.get("video_thumbnail_failed") or 0)
        summary["totals"]["video_thumbnail_missing_source"] += int(counts.get("video_thumbnail_missing_source") or 0)

    return summary


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()
    db = create_supabase_admin_client()

    summary = run_bootstrap(
        db=db,
        show_ids=_normalized_show_ids(args.show_id),
        limit=max(0, int(args.limit or 0)),
        dry_run=bool(args.dry_run),
        sync_thumbnails=bool(args.sync_thumbnails),
        continue_on_error=bool(args.continue_on_error),
        actor=str(args.actor or "bootstrap_bravo_show_snapshots"),
    )

    totals = summary.get("totals") if isinstance(summary.get("totals"), dict) else {}
    print(
        "summary: "
        f"shows_scanned={int(totals.get('shows_scanned') or 0)} "
        f"shows_processed={int(totals.get('shows_processed') or 0)} "
        f"shows_succeeded={int(totals.get('shows_succeeded') or 0)} "
        f"shows_failed={int(totals.get('shows_failed') or 0)} "
        f"shows_skipped={int(totals.get('shows_skipped') or 0)} "
        f"bootstrap_created={int(totals.get('bootstrap_created') or 0)} "
        f"thumb_synced={int(totals.get('video_thumbnail_synced') or 0)}"
    )

    if args.json_summary:
        payload = json.dumps(summary, indent=2)
        if args.json_summary == "-":
            print(payload)
        else:
            Path(args.json_summary).write_text(payload, encoding="utf-8")
            print(f"wrote summary to {args.json_summary}")

    return 0 if int(totals.get("shows_failed") or 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
