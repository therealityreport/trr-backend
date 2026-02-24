#!/usr/bin/env python3
"""
Backfill mirrored Bravo video thumbnails into show snapshots.

Default behavior:
- discovers shows with persisted Bravo snapshots (`core.show_source_latest`)
- syncs only items that still need thumbnail mirroring

Use `--force` to remirror all Bravo video items for selected shows.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, cast

try:
    from api.routers.admin_show_bravo import (
        _fetch_show_snapshot,
        _sync_bravo_video_thumbnails,
        _to_iso_now,
        _upsert_show_snapshot,
        _video_item_needs_thumbnail_sync,
    )
    from trr_backend.db.admin import create_supabase_admin_client
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:
    # Allow direct execution without requiring PYTHONPATH=. from repo root.
    REPO_ROOT = Path(__file__).resolve().parents[2]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from api.routers.admin_show_bravo import (
        _fetch_show_snapshot,
        _sync_bravo_video_thumbnails,
        _to_iso_now,
        _upsert_show_snapshot,
        _video_item_needs_thumbnail_sync,
    )
    from trr_backend.db.admin import create_supabase_admin_client
    from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_bravo_video_thumbnails",
        description="Backfill hosted Bravo video thumbnails into show snapshots.",
    )
    parser.add_argument(
        "--show-id",
        action="append",
        default=[],
        help="Optional show UUID(s). If omitted, all shows with Bravo snapshots are processed.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Optional cap on number of shows to process.")
    parser.add_argument("--force", action="store_true", help="Remirror all video thumbnails, not just pending items.")
    parser.add_argument("--dry-run", action="store_true", help="Report pending counts without writing updates.")
    parser.add_argument(
        "--json-summary",
        default="",
        help="Optional JSON summary output path ('-' prints to stdout).",
    )
    return parser.parse_args(argv)


def _list_show_ids_with_bravo_snapshot(db: Any) -> list[str]:
    response = (
        db.schema("core")
        .table("show_source_latest")
        .select("show_id,fetched_at")
        .eq("source_id", "bravo")
        .eq("variant", "default")
        .order("fetched_at", desc=True)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing bravo snapshots: {response.error}")
    rows = response.data or []
    if not isinstance(rows, list):
        return []

    seen: set[str] = set()
    show_ids: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        show_id = str(row.get("show_id") or "").strip()
        if not show_id or show_id in seen:
            continue
        seen.add(show_id)
        show_ids.append(show_id)
    return show_ids


def _count_pending_items(normalized: dict[str, Any]) -> int:
    pending = 0
    for list_key in ("videos_show", "videos_person"):
        items = normalized.get(list_key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict) and _video_item_needs_thumbnail_sync(item):
                pending += 1
    return pending


def _process_show(*, db: Any, show_id: str, force: bool, dry_run: bool) -> dict[str, Any]:
    result: dict[str, Any] = {
        "show_id": show_id,
        "status": "ok",
        "pending_before": 0,
        "pending_after": 0,
        "sync": None,
        "error": None,
    }
    try:
        snapshot = _fetch_show_snapshot(db, show_id)
    except Exception as exc:  # noqa: BLE001
        result["status"] = "missing_snapshot"
        result["error"] = str(exc)
        return result

    payload = snapshot.get("payload")
    if not isinstance(payload, dict):
        result["status"] = "invalid_snapshot_payload"
        result["error"] = "Snapshot payload missing or invalid"
        return result

    normalized = payload.get("normalized")
    if not isinstance(normalized, dict):
        result["status"] = "invalid_normalized_payload"
        result["error"] = "Snapshot normalized payload missing or invalid"
        return result

    pending_before = _count_pending_items(normalized)
    result["pending_before"] = pending_before

    if pending_before == 0 and not force:
        result["status"] = "skipped_no_pending"
        result["pending_after"] = 0
        return result

    if dry_run:
        result["status"] = "dry_run"
        result["pending_after"] = pending_before
        return result

    sync_result = _sync_bravo_video_thumbnails(
        db=db,
        admin_user=cast(Any, None),
        show_id=show_id,
        normalized=normalized,
        force=force,
        refresh_from_clip_metadata=True,
    )
    normalized["video_thumbnail_sync"] = {
        **sync_result,
        "forced": bool(force),
        "synced_at": _to_iso_now(),
        "runner": "scripts/backfill/backfill_bravo_video_thumbnails.py",
    }
    payload["normalized"] = normalized
    _upsert_show_snapshot(db, show_id=show_id, payload=payload)

    pending_after = _count_pending_items(normalized)
    result["pending_after"] = pending_after
    result["sync"] = sync_result
    return result


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()
    db = create_supabase_admin_client()

    selected_ids = [str(value).strip() for value in args.show_id if str(value).strip()]
    show_ids = selected_ids if selected_ids else _list_show_ids_with_bravo_snapshot(db)
    if args.limit and args.limit > 0:
        show_ids = show_ids[: args.limit]

    if not show_ids:
        print("No shows with Bravo snapshots found.")
        return 0

    mode = "dry-run" if args.dry_run else "apply"
    print(f"mode: {mode} force={bool(args.force)} show_count={len(show_ids)}")

    summary: list[dict[str, Any]] = []
    totals = {
        "shows": len(show_ids),
        "ok": 0,
        "dry_run": 0,
        "skipped_no_pending": 0,
        "missing_snapshot": 0,
        "invalid_snapshot_payload": 0,
        "invalid_normalized_payload": 0,
        "errors": 0,
        "pending_before": 0,
        "pending_after": 0,
        "attempted": 0,
        "synced": 0,
        "failed": 0,
        "missing_source": 0,
    }

    for show_id in show_ids:
        result = _process_show(db=db, show_id=show_id, force=bool(args.force), dry_run=bool(args.dry_run))
        summary.append(result)

        status = str(result.get("status") or "ok")
        totals[status] = int(totals.get(status, 0)) + 1
        totals["pending_before"] += int(result.get("pending_before") or 0)
        totals["pending_after"] += int(result.get("pending_after") or 0)

        sync = result.get("sync")
        if isinstance(sync, dict):
            totals["attempted"] += int(sync.get("attempted") or 0)
            totals["synced"] += int(sync.get("synced") or 0)
            totals["failed"] += int(sync.get("failed") or 0)
            totals["missing_source"] += int(sync.get("missing_source") or 0)

        if result.get("error"):
            totals["errors"] += 1

        sync_payload = result.get("sync") if isinstance(result.get("sync"), dict) else {}
        synced_count = int((sync_payload or {}).get("synced") or 0)
        print(
            f"show_id={show_id} status={status} "
            f"pending_before={int(result.get('pending_before') or 0)} "
            f"pending_after={int(result.get('pending_after') or 0)} "
            f"synced={synced_count}"
        )

    print(
        "summary: "
        f"shows={totals['shows']} ok={totals['ok']} dry_run={totals['dry_run']} "
        f"skipped_no_pending={totals['skipped_no_pending']} errors={totals['errors']} "
        f"pending_before={totals['pending_before']} pending_after={totals['pending_after']} "
        f"attempted={totals['attempted']} synced={totals['synced']} failed={totals['failed']} "
        f"missing_source={totals['missing_source']}"
    )

    if args.json_summary:
        payload = {"totals": totals, "shows": summary}
        if args.json_summary == "-":
            print(json.dumps(payload, indent=2))
        else:
            Path(args.json_summary).write_text(json.dumps(payload, indent=2), encoding="utf-8")
            print(f"wrote summary to {args.json_summary}")

    return 0 if totals["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
