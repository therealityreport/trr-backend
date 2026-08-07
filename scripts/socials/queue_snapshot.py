#!/usr/bin/env python3
"""Print a reusable social queue snapshot for operators."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from scripts.socials.instagram.media_mirror_recovery import (
        MEDIA_STAGES,
        _json_safe,
        _normalize_account,
        fetch_status_snapshot,
    )
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from scripts.socials.instagram.media_mirror_recovery import (
        MEDIA_STAGES,
        _json_safe,
        _normalize_account,
        fetch_status_snapshot,
    )
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="social_queue_snapshot",
        description="Print status counts and stale running jobs for one social scrape queue slice.",
    )
    parser.add_argument("--run-id", required=True, help="social.scrape_runs id")
    parser.add_argument("--platform", choices=["instagram"], default="instagram")
    parser.add_argument("--stage", choices=MEDIA_STAGES, default="media_mirror")
    parser.add_argument("--account", help="Optional account handle filter.")
    parser.add_argument("--stale-after-seconds", type=int, default=900)
    parser.add_argument("--json", action="store_true", help="Emit JSON only.")
    return parser.parse_args(argv)


def build_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    account = _normalize_account(args.account)
    stale_after_seconds = max(30, int(args.stale_after_seconds or 900))
    snapshot = fetch_status_snapshot(
        run_id=args.run_id,
        stage=args.stage,
        account=account,
        stale_after_seconds=stale_after_seconds,
    )
    return {
        "ok": True,
        "platform": args.platform,
        "run_id": args.run_id,
        "stage": args.stage,
        "account": account,
        "stale_after_seconds": stale_after_seconds,
        "snapshot": snapshot,
        "generated_at": datetime.now().astimezone().isoformat(),
    }


def _print_compact(payload: dict[str, Any]) -> None:
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    status_counts: Any = snapshot.get("status_counts") if isinstance(snapshot, dict) else []
    counts = {
        str(row.get("status") or "unknown"): int(row.get("jobs") or 0) for row in status_counts if isinstance(row, dict)
    }
    print(
        (
            "run_id={run_id} platform={platform} stage={stage} stale_running={stale} "
            "queued={queued} running={running} retrying={retrying}"
        ).format(
            run_id=payload.get("run_id"),
            platform=payload.get("platform"),
            stage=payload.get("stage"),
            stale=snapshot.get("stale_running_count", 0) if isinstance(snapshot, dict) else 0,
            queued=counts.get("queued", 0),
            running=counts.get("running", 0),
            retrying=counts.get("retrying", 0),
        )
    )


def main(argv: list[str] | None = None) -> int:
    load_env()
    os.environ.setdefault("TRR_DB_POOL_CLOSE_AFTER_RETURN", "true")
    args = _parse_args(argv)
    try:
        payload = build_snapshot(args)
        if args.json:
            print(json.dumps(_json_safe(payload), indent=2, sort_keys=True))
        else:
            _print_compact(payload)
            print(json.dumps(_json_safe(payload), indent=2, sort_keys=True))
        return 0
    finally:
        pg.close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
