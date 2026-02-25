#!/usr/bin/env python3
"""Run daily News/Videos maintenance phases for TRR show admin data."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

try:
    import scripts.backfill.backfill_bravo_video_thumbnails as thumbnail_backfill
    import scripts.backfill.bootstrap_bravo_show_snapshots as bravo_bootstrap
    from api.routers.admin_show_news import _run_google_news_sync_impl
    from trr_backend.db import pg
    from trr_backend.db.admin import create_supabase_admin_client
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:
    REPO_ROOT = Path(__file__).resolve().parents[2]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    import scripts.backfill.backfill_bravo_video_thumbnails as thumbnail_backfill
    import scripts.backfill.bootstrap_bravo_show_snapshots as bravo_bootstrap
    from api.routers.admin_show_news import _run_google_news_sync_impl
    from trr_backend.db import pg
    from trr_backend.db.admin import create_supabase_admin_client
    from trr_backend.utils.env import load_env


PhaseName = Literal["bootstrap", "thumbnails", "google"]

_RUNNER = "scripts/backfill/run_news_video_maintenance.py"


def _to_iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run_news_video_maintenance",
        description="Run Bravo bootstrap, thumbnail sync, and Google News maintenance phases.",
    )
    parser.add_argument(
        "--phase",
        choices=["all", "bootstrap", "thumbnails", "google"],
        default="all",
        help="Maintenance phase to run.",
    )
    parser.add_argument(
        "--show-id",
        action="append",
        default=[],
        help="Optional show UUID(s) to scope the run.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Optional per-phase show limit.")
    parser.add_argument("--dry-run", action="store_true", help="Compute targets and counters without writes.")
    parser.add_argument(
        "--sync-thumbnails",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable thumbnail sync during bootstrap phase.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing shows/phases when individual shows fail.",
    )
    parser.add_argument(
        "--json-summary",
        default="",
        help="Optional JSON summary output path ('-' prints to stdout).",
    )
    parser.add_argument(
        "--actor",
        default="news_video_maintenance",
        help="Actor label included in bootstrap metadata.",
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


def _phase_order(phase: str) -> list[PhaseName]:
    if phase == "all":
        return ["bootstrap", "thumbnails", "google"]
    return [cast(PhaseName, phase)]


def _list_google_news_targets(*, show_ids: list[str] | None = None, limit: int = 0) -> list[dict[str, Any]]:
    normalized_show_ids = _normalized_show_ids(show_ids)
    params: list[Any] = []
    show_filter_sql = ""
    if normalized_show_ids:
        show_filter_sql = "AND l.show_id::text = ANY(%s::text[])"
        params.append(normalized_show_ids)

    limit_sql = ""
    if limit and limit > 0:
        limit_sql = "LIMIT %s"
        params.append(int(limit))

    rows = pg.fetch_all(
        f"""
        WITH ranked_links AS (
          SELECT
            l.show_id::text AS show_id,
            l.url,
            l.status,
            ROW_NUMBER() OVER (
              PARTITION BY l.show_id
              ORDER BY
                CASE LOWER(COALESCE(l.status, ''))
                  WHEN 'approved' THEN 0
                  WHEN 'pending' THEN 1
                  ELSE 2
                END,
                COALESCE(l.updated_at, l.created_at) DESC NULLS LAST,
                l.created_at DESC NULLS LAST
            ) AS rn
          FROM core.entity_links l
          WHERE l.entity_type = 'show'
            AND COALESCE(l.season_number, 0) = 0
            AND l.link_kind = 'google_news_url'
            AND LOWER(COALESCE(l.status, '')) IN ('approved', 'pending')
            {show_filter_sql}
        )
        SELECT show_id, url, status
        FROM ranked_links
        WHERE rn = 1
        ORDER BY show_id
        {limit_sql}
        """,
        params,
    )

    out: list[dict[str, Any]] = []
    for row in rows:
        show_id = str(row.get("show_id") or "").strip()
        topic_url = str(row.get("url") or "").strip()
        if not show_id or not topic_url:
            continue
        out.append(
            {
                "show_id": show_id,
                "topic_url": topic_url,
                "status": str(row.get("status") or "").strip() or None,
            }
        )
    return out


def _run_bootstrap_phase(
    *,
    db: Any,
    show_ids: list[str],
    limit: int,
    dry_run: bool,
    sync_thumbnails: bool,
    continue_on_error: bool,
    actor: str,
) -> dict[str, Any]:
    return bravo_bootstrap.run_bootstrap(
        db=db,
        show_ids=show_ids,
        limit=limit,
        dry_run=dry_run,
        sync_thumbnails=sync_thumbnails,
        continue_on_error=continue_on_error,
        actor=actor,
    )


def _run_thumbnail_phase(
    *,
    db: Any,
    show_ids: list[str],
    limit: int,
    dry_run: bool,
    continue_on_error: bool,
) -> dict[str, Any]:
    all_show_ids = thumbnail_backfill._list_show_ids_with_bravo_snapshot(db)
    if show_ids:
        allowed = set(show_ids)
        target_show_ids = [show_id for show_id in all_show_ids if show_id in allowed]
    else:
        target_show_ids = all_show_ids
    if limit and limit > 0:
        target_show_ids = target_show_ids[:limit]

    summary: dict[str, Any] = {
        "runner": _RUNNER,
        "generated_at": _to_iso_now(),
        "dry_run": bool(dry_run),
        "totals": {
            "shows_scanned": len(target_show_ids),
            "shows_processed": 0,
            "shows_succeeded": 0,
            "shows_failed": 0,
            "shows_skipped": 0,
            "thumbnail_attempted": 0,
            "thumbnail_synced": 0,
            "thumbnail_failed": 0,
            "thumbnail_missing_source": 0,
            "pending_before": 0,
            "pending_after": 0,
        },
        "skip_reasons": {},
        "shows": [],
    }

    for show_id in target_show_ids:
        try:
            result = thumbnail_backfill._process_show(
                db=db,
                show_id=show_id,
                force=False,
                dry_run=dry_run,
            )
        except Exception as exc:  # noqa: BLE001
            result = {
                "show_id": show_id,
                "status": "failed",
                "error": str(exc),
                "pending_before": 0,
                "pending_after": 0,
                "sync": None,
            }
            if not continue_on_error:
                summary["shows"].append(result)
                summary["totals"]["shows_processed"] += 1
                summary["totals"]["shows_failed"] += 1
                break

        summary["shows"].append(result)
        summary["totals"]["shows_processed"] += 1
        status = str(result.get("status") or "").strip().lower()

        if status in {"ok", "dry_run"}:
            summary["totals"]["shows_succeeded"] += 1
        elif status.startswith("skipped") or status.startswith("missing"):
            summary["totals"]["shows_skipped"] += 1
            summary["skip_reasons"][status] = int(summary["skip_reasons"].get(status, 0)) + 1
        else:
            summary["totals"]["shows_failed"] += 1

        summary["totals"]["pending_before"] += int(result.get("pending_before") or 0)
        summary["totals"]["pending_after"] += int(result.get("pending_after") or 0)
        sync = result.get("sync") if isinstance(result.get("sync"), dict) else {}
        summary["totals"]["thumbnail_attempted"] += int(sync.get("attempted") or 0)
        summary["totals"]["thumbnail_synced"] += int(sync.get("synced") or 0)
        summary["totals"]["thumbnail_failed"] += int(sync.get("failed") or 0)
        summary["totals"]["thumbnail_missing_source"] += int(sync.get("missing_source") or 0)

    return summary


def _run_google_phase(
    *,
    db: Any,
    show_ids: list[str],
    limit: int,
    dry_run: bool,
    continue_on_error: bool,
) -> dict[str, Any]:
    targets = _list_google_news_targets(show_ids=show_ids, limit=limit)
    summary: dict[str, Any] = {
        "runner": _RUNNER,
        "generated_at": _to_iso_now(),
        "dry_run": bool(dry_run),
        "totals": {
            "shows_scanned": len(targets),
            "shows_processed": 0,
            "shows_succeeded": 0,
            "shows_failed": 0,
            "shows_skipped": 0,
            "google_synced": 0,
            "google_stale_guard_skipped": 0,
            "google_items_count": 0,
        },
        "skip_reasons": {},
        "shows": [],
    }

    for target in targets:
        show_id = str(target.get("show_id") or "").strip()
        if not show_id:
            continue

        if dry_run:
            result = {
                "show_id": show_id,
                "status": "dry_run",
                "synced": False,
                "stale_guard_skipped": False,
                "count": 0,
                "error": None,
            }
        else:
            try:
                sync_result = _run_google_news_sync_impl(
                    show_id_str=show_id,
                    force=False,
                    db=db,
                    admin_user=cast(Any, None),
                )
                stale_guard_skipped = bool(sync_result.get("stale_guard_skipped"))
                result = {
                    "show_id": show_id,
                    "status": "skipped" if stale_guard_skipped else "ok",
                    "synced": bool(sync_result.get("synced")),
                    "stale_guard_skipped": stale_guard_skipped,
                    "count": int(sync_result.get("count") or 0),
                    "error": None,
                    "skip_reason": "stale_guard_skipped" if stale_guard_skipped else None,
                }
            except Exception as exc:  # noqa: BLE001
                result = {
                    "show_id": show_id,
                    "status": "failed",
                    "synced": False,
                    "stale_guard_skipped": False,
                    "count": 0,
                    "error": str(exc),
                }
                if not continue_on_error:
                    summary["shows"].append(result)
                    summary["totals"]["shows_processed"] += 1
                    summary["totals"]["shows_failed"] += 1
                    break

        summary["shows"].append(result)
        summary["totals"]["shows_processed"] += 1

        status = str(result.get("status") or "").strip().lower()
        if status in {"ok", "dry_run"}:
            summary["totals"]["shows_succeeded"] += 1
        elif status == "skipped":
            summary["totals"]["shows_skipped"] += 1
            reason = str(result.get("skip_reason") or "unknown")
            summary["skip_reasons"][reason] = int(summary["skip_reasons"].get(reason, 0)) + 1
        else:
            summary["totals"]["shows_failed"] += 1

        if bool(result.get("synced")):
            summary["totals"]["google_synced"] += 1
        if bool(result.get("stale_guard_skipped")):
            summary["totals"]["google_stale_guard_skipped"] += 1
        summary["totals"]["google_items_count"] += int(result.get("count") or 0)

    return summary


def _empty_phase_summary(name: PhaseName) -> dict[str, Any]:
    return {
        "phase": name,
        "runner": _RUNNER,
        "generated_at": _to_iso_now(),
        "totals": {
            "shows_scanned": 0,
            "shows_processed": 0,
            "shows_succeeded": 0,
            "shows_failed": 0,
            "shows_skipped": 0,
        },
        "skip_reasons": {},
        "shows": [],
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()
    db = create_supabase_admin_client()

    selected_show_ids = _normalized_show_ids(args.show_id)
    phase_order = _phase_order(args.phase)

    output: dict[str, Any] = {
        "runner": _RUNNER,
        "generated_at": _to_iso_now(),
        "phase": args.phase,
        "dry_run": bool(args.dry_run),
        "continue_on_error": bool(args.continue_on_error),
        "show_ids": selected_show_ids,
        "limit": max(0, int(args.limit or 0)),
        "phases": {
            "bootstrap": _empty_phase_summary("bootstrap"),
            "thumbnails": _empty_phase_summary("thumbnails"),
            "google": _empty_phase_summary("google"),
        },
        "totals": {
            "shows_scanned": 0,
            "shows_processed": 0,
            "shows_succeeded": 0,
            "shows_failed": 0,
            "shows_skipped": 0,
            "bootstrap_created": 0,
            "bootstrap_dry_run": 0,
            "thumbnail_attempted": 0,
            "thumbnail_synced": 0,
            "thumbnail_failed": 0,
            "thumbnail_missing_source": 0,
            "google_synced": 0,
            "google_stale_guard_skipped": 0,
            "google_items_count": 0,
        },
        "hard_failures": [],
    }

    exit_code = 0

    for phase in phase_order:
        try:
            if phase == "bootstrap":
                phase_summary = _run_bootstrap_phase(
                    db=db,
                    show_ids=selected_show_ids,
                    limit=max(0, int(args.limit or 0)),
                    dry_run=bool(args.dry_run),
                    sync_thumbnails=bool(args.sync_thumbnails),
                    continue_on_error=bool(args.continue_on_error),
                    actor=str(args.actor or "news_video_maintenance"),
                )
            elif phase == "thumbnails":
                phase_summary = _run_thumbnail_phase(
                    db=db,
                    show_ids=selected_show_ids,
                    limit=max(0, int(args.limit or 0)),
                    dry_run=bool(args.dry_run),
                    continue_on_error=bool(args.continue_on_error),
                )
            else:
                phase_summary = _run_google_phase(
                    db=db,
                    show_ids=selected_show_ids,
                    limit=max(0, int(args.limit or 0)),
                    dry_run=bool(args.dry_run),
                    continue_on_error=bool(args.continue_on_error),
                )
        except Exception as exc:  # noqa: BLE001
            output["hard_failures"].append({"phase": phase, "error": str(exc)})
            exit_code = 1
            if not args.continue_on_error:
                break
            continue

        output["phases"][phase] = {"phase": phase, **phase_summary}

        phase_totals = phase_summary.get("totals") if isinstance(phase_summary.get("totals"), dict) else {}
        output["totals"]["shows_scanned"] += int(phase_totals.get("shows_scanned") or 0)
        output["totals"]["shows_processed"] += int(phase_totals.get("shows_processed") or 0)
        output["totals"]["shows_succeeded"] += int(phase_totals.get("shows_succeeded") or 0)
        output["totals"]["shows_failed"] += int(phase_totals.get("shows_failed") or 0)
        output["totals"]["shows_skipped"] += int(phase_totals.get("shows_skipped") or 0)

        output["totals"]["bootstrap_created"] += int(phase_totals.get("bootstrap_created") or 0)
        output["totals"]["bootstrap_dry_run"] += int(phase_totals.get("bootstrap_dry_run") or 0)
        output["totals"]["thumbnail_attempted"] += int(
            phase_totals.get("thumbnail_attempted") or phase_totals.get("video_thumbnail_attempted") or 0
        )
        output["totals"]["thumbnail_synced"] += int(
            phase_totals.get("thumbnail_synced") or phase_totals.get("video_thumbnail_synced") or 0
        )
        output["totals"]["thumbnail_failed"] += int(
            phase_totals.get("thumbnail_failed") or phase_totals.get("video_thumbnail_failed") or 0
        )
        output["totals"]["thumbnail_missing_source"] += int(
            phase_totals.get("thumbnail_missing_source") or phase_totals.get("video_thumbnail_missing_source") or 0
        )
        output["totals"]["google_synced"] += int(phase_totals.get("google_synced") or 0)
        output["totals"]["google_stale_guard_skipped"] += int(phase_totals.get("google_stale_guard_skipped") or 0)
        output["totals"]["google_items_count"] += int(phase_totals.get("google_items_count") or 0)

    totals = output["totals"]
    print(
        "summary: "
        f"phase={args.phase} dry_run={bool(args.dry_run)} "
        f"shows_scanned={int(totals.get('shows_scanned') or 0)} "
        f"shows_processed={int(totals.get('shows_processed') or 0)} "
        f"shows_succeeded={int(totals.get('shows_succeeded') or 0)} "
        f"shows_failed={int(totals.get('shows_failed') or 0)} "
        f"shows_skipped={int(totals.get('shows_skipped') or 0)} "
        f"bootstrap_created={int(totals.get('bootstrap_created') or 0)} "
        f"thumb_synced={int(totals.get('thumbnail_synced') or 0)} "
        f"google_synced={int(totals.get('google_synced') or 0)}"
    )

    if args.json_summary:
        payload = json.dumps(output, indent=2)
        if args.json_summary == "-":
            print(payload)
        else:
            Path(args.json_summary).write_text(payload, encoding="utf-8")
            print(f"wrote summary to {args.json_summary}")

    if output["hard_failures"]:
        return 1
    if not args.continue_on_error and int(totals.get("shows_failed") or 0) > 0:
        return 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
