#!/usr/bin/env python3
"""Guard media-safe worker startup when stale media queue claims exist."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

try:
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="media_queue_guard",
        description="Block media-safe worker startup when stale media queue claims exist.",
    )
    parser.add_argument("--allow-stale", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def build_guard_payload(*, allow_stale: bool = False) -> dict[str, Any]:
    stale_after_seconds = max(30, int(os.getenv("SOCIAL_MEDIA_QUEUE_STALE_AFTER_SECONDS", "900") or "900"))
    rows = pg.fetch_all(
        """
        select
          coalesce(platform, 'unknown') as platform,
          lower(
            coalesce(
              nullif(config->>'stage', ''),
              nullif(metadata->>'stage', ''),
              nullif(job_type, ''),
              'unknown'
            )
          ) as stage,
          count(*)::bigint as total
        from social.scrape_jobs
        where status = 'running'
          and lower(
            coalesce(
              nullif(config->>'stage', ''),
              nullif(metadata->>'stage', ''),
              nullif(job_type, ''),
              'unknown'
            )
          ) = any(%s::text[])
          and coalesce(heartbeat_at, started_at, claimed_at, created_at)
            < now() - (%s * interval '1 second')
        group by 1, 2
        """,
        [["media_mirror", "comment_media_mirror"], stale_after_seconds],
    )
    by_stage: dict[str, int] = {}
    by_platform: dict[str, int] = {}
    stale_total = 0
    for row in rows:
        total = int(row.get("total") or 0)
        stage = str(row.get("stage") or "unknown").strip().lower() or "unknown"
        platform = str(row.get("platform") or "unknown").strip().lower() or "unknown"
        stale_total += total
        by_stage[stage] = int(by_stage.get(stage) or 0) + total
        by_platform[platform] = int(by_platform.get(platform) or 0) + total
    media_stale_claims = {
        "total": stale_total,
        "by_stage": by_stage,
        "by_platform": by_platform,
        "stale_after_seconds": stale_after_seconds,
    }
    blocked = stale_total > 0 and not allow_stale
    return {
        "ok": not blocked,
        "blocked": blocked,
        "allow_stale": allow_stale,
        "stale_media_claims": media_stale_claims,
    }


def _print_compact(payload: dict[str, Any]) -> None:
    stale_claims_raw = payload.get("stale_media_claims")
    stale_claims: dict[str, Any] = stale_claims_raw if isinstance(stale_claims_raw, dict) else {}
    stale_total = int(stale_claims.get("total") or 0)
    status = "blocked" if payload.get("blocked") else "ok"
    print(f"media_queue_guard={status} stale_media_claims={stale_total}")


def main(argv: list[str] | None = None) -> int:
    load_env()
    os.environ.setdefault("TRR_DB_POOL_CLOSE_AFTER_RETURN", "true")
    args = _parse_args(argv)
    try:
        payload = build_guard_payload(allow_stale=bool(args.allow_stale))
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            _print_compact(payload)
            if payload.get("blocked"):
                print("Set ALLOW_STALE_MEDIA=1 to override after an operator accepts the risk.", file=sys.stderr)
        return 0 if payload.get("ok") else 1
    finally:
        pg.close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
