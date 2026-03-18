#!/usr/bin/env python3
"""Import a normalized SocialBlade JSON snapshot into Supabase through repository code."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from trr_backend.db import pg
from trr_backend.repositories.socialblade_growth import socialblade_growth_table_exists
from trr_backend.socials.socialblade.service import persist_scraped_payload, sanitize_socialblade_handle


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("json_path", type=Path, help="Path to normalized SocialBlade JSON")
    parser.add_argument(
        "--handle",
        required=True,
        help="Instagram handle to resolve in core.people.external_ids->>'instagram_id'",
    )
    return parser.parse_args()


def _load_payload(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Seed file must contain a JSON object")
    return payload


def _resolve_person_id(handle: str) -> str:
    rows = pg.fetch_all(
        """
        SELECT id::text
        FROM core.people
        WHERE external_ids->>'instagram_id' = %s
        """,
        [handle],
    )
    if not rows:
        raise RuntimeError(f"No person found for instagram_id={handle}")
    if len(rows) > 1:
        raise RuntimeError(f"Ambiguous person lookup for instagram_id={handle}")
    return str(rows[0]["id"])


def _normalize_payload(payload: dict[str, Any], handle: str) -> dict[str, Any]:
    username = sanitize_socialblade_handle(payload.get("username") or handle)
    if not username:
        raise RuntimeError("Seed payload does not contain a valid username")
    return {
        "username": username,
        "platform": "instagram",
        "scraped_at": payload.get("scraped_at"),
        "stats_refreshed": bool(payload.get("stats_refreshed", False)),
        "profile_stats": payload.get("profile_stats", {}),
        "rankings": payload.get("rankings", {}),
        "daily_channel_metrics_60day": payload.get("daily_channel_metrics_60day", {}),
        "daily_total_followers_chart": payload.get("daily_total_followers_chart"),
    }


def main() -> int:
    args = _parse_args()
    handle = sanitize_socialblade_handle(args.handle)
    if not handle:
        raise RuntimeError("Handle must not be empty")
    if not args.json_path.is_file():
        raise FileNotFoundError(f"Seed file not found: {args.json_path}")
    if not socialblade_growth_table_exists():
        raise RuntimeError("pipeline.socialblade_growth_data does not exist. Apply migration 0197 first.")

    payload = _normalize_payload(_load_payload(args.json_path), handle)
    person_id = _resolve_person_id(handle)
    stored = persist_scraped_payload(
        person_id=person_id,
        handle=handle,
        payload=payload,
        source="seed_import",
        force=True,
    )
    print(
        json.dumps(
            {
                "ok": True,
                "person_id": person_id,
                "handle": handle,
                "scraped_at": stored.get("scraped_at"),
                "freshness_status": stored.get("freshness_status"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
