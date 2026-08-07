#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    from scripts.socials.instagram.backfill_progress import build_progress, print_compact
    from trr_backend.db import pg
    from trr_backend.socials import social_season_analytics_impl as social_repo
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from scripts.socials.instagram.backfill_progress import build_progress, print_compact
    from trr_backend.db import pg
    from trr_backend.socials import social_season_analytics_impl as social_repo
    from trr_backend.utils.env import load_env


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    return str(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Recover a stalled Instagram backfill frontier, repair canonical metrics, normalize hosted media, "
            "and dispatch due jobs."
        )
    )
    parser.add_argument("--run-id", required=True, help="social.scrape_runs id")
    parser.add_argument("--stale-after-seconds", type=int, default=900)
    parser.add_argument("--recover-limit", type=int, default=5)
    parser.add_argument("--dispatch-limit", type=int, default=8)
    parser.add_argument("--skip-recover", action="store_true")
    parser.add_argument("--skip-repair", action="store_true")
    parser.add_argument("--skip-media-normalize", action="store_true")
    parser.add_argument("--skip-frontier-recover", action="store_true")
    parser.add_argument("--skip-dispatch", action="store_true")
    parser.add_argument("--skip-progress", action="store_true")
    parser.add_argument("--media-normalize-batch-size", type=int, default=500)
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of compact text")
    return parser.parse_args()


def _emit_step(message: str, *, json_mode: bool) -> None:
    if not json_mode:
        print(message, flush=True)


def main() -> int:
    args = parse_args()
    load_env()
    os.environ.setdefault("TRR_DB_POOL_CLOSE_AFTER_RETURN", "true")

    recovered = []
    canonical_repair: dict[str, Any]
    media_normalization: dict[str, Any]
    frontier_recovery: dict[str, Any]
    dispatch: dict[str, Any]
    progress: dict[str, Any] | None = None
    try:
        if not args.skip_recover:
            _emit_step("step=recover_stale_jobs status=running", json_mode=args.json)
            recovered = social_repo.recover_stale_running_jobs(
                run_id=args.run_id,
                stage=social_repo.SHARED_ACCOUNT_POSTS_STAGE,
                stale_after_seconds=max(1, int(args.stale_after_seconds)),
                limit=max(1, int(args.recover_limit)),
            )
            _emit_step(
                f"step=recover_stale_jobs status=done recovered_jobs={len(recovered)}",
                json_mode=args.json,
            )
        canonical_repair = {"run_id": args.run_id, "canonical_metric_rows_repaired": 0, "skipped": True}
        if not args.skip_repair:
            _emit_step("step=canonical_metric_repair status=running", json_mode=args.json)
            canonical_repair = social_repo.repair_instagram_canonical_metrics_for_run(args.run_id)
            _emit_step(
                "step=canonical_metric_repair status=done repaired_rows={rows}".format(
                    rows=int(canonical_repair.get("canonical_metric_rows_repaired") or 0)
                ),
                json_mode=args.json,
            )
        media_normalization = {
            "run_id": args.run_id,
            "hosted_media_status_rows_normalized": 0,
            "skipped": True,
        }
        if not args.skip_media_normalize:
            _emit_step("step=hosted_media_status_normalize status=running", json_mode=args.json)
            media_normalization = social_repo.normalize_instagram_hosted_media_mirror_status_for_run(
                args.run_id,
                batch_size=max(1, int(args.media_normalize_batch_size or 500)),
            )
            _emit_step(
                "step=hosted_media_status_normalize status=done normalized_rows={rows}".format(
                    rows=int(media_normalization.get("hosted_media_status_rows_normalized") or 0)
                ),
                json_mode=args.json,
            )
        frontier_recovery = {"run_id": args.run_id, "released_frontier_leases": 0, "due_jobs": 0, "skipped": True}
        if not args.skip_frontier_recover:
            _emit_step("step=orphaned_frontier_lease_recover status=running", json_mode=args.json)
            frontier_recovery = social_repo.recover_orphaned_instagram_frontier_leases_for_run(args.run_id)
            _emit_step(
                (
                    "step=orphaned_frontier_lease_recover status=done released_frontiers={frontiers} due_jobs={jobs}"
                ).format(
                    frontiers=int(frontier_recovery.get("released_frontier_leases") or 0),
                    jobs=int(frontier_recovery.get("due_jobs") or 0),
                ),
                json_mode=args.json,
            )
        dispatch = {"dispatched_job_ids": [], "dispatch_attempts": 0, "skipped": True}
        if not args.skip_dispatch:
            _emit_step("step=dispatch_due_jobs status=running", json_mode=args.json)
            dispatch = social_repo.dispatch_due_social_jobs(
                run_id=args.run_id,
                limit=max(1, int(args.dispatch_limit)),
            )
            _emit_step(
                f"step=dispatch_due_jobs status=done dispatched_jobs={len(dispatch.get('dispatched_job_ids') or [])}",
                json_mode=args.json,
            )
        if not args.skip_progress:
            _emit_step("step=progress_snapshot status=running", json_mode=args.json)
            progress = build_progress(args.run_id)
            _emit_step("step=progress_snapshot status=done", json_mode=args.json)

        result = {
            "run_id": args.run_id,
            "recovered_jobs": recovered,
            "canonical_repair": canonical_repair,
            "media_normalization": media_normalization,
            "frontier_recovery": frontier_recovery,
            "dispatch": dispatch,
            "progress": progress,
        }
        if args.json:
            print(json.dumps(_json_safe(result), indent=2, sort_keys=True))
            return 0

        print(
            (
                "recovered_jobs={recovered} canonical_metric_rows_repaired={repaired} "
                "hosted_media_status_rows_normalized={normalized} "
                "released_frontiers={released_frontiers} dispatched_jobs={dispatched}"
            ).format(
                recovered=len(recovered),
                repaired=int(canonical_repair.get("canonical_metric_rows_repaired") or 0),
                normalized=int(media_normalization.get("hosted_media_status_rows_normalized") or 0),
                released_frontiers=int(frontier_recovery.get("released_frontier_leases") or 0),
                dispatched=len(dispatch.get("dispatched_job_ids") or []),
            ),
            flush=True,
        )
        if progress is not None:
            print_compact(progress)
        return 0
    finally:
        pg.close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
