#!/usr/bin/env python3
"""Local worker that checks Instagram cookie health and runs the full repair flow when needed."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.modal import repair_instagram_auth
from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.socials.instagram import cookie_refresh as instagram_cookie_refresh
from trr_backend.utils.env import load_env

REPO_ROOT = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-cookie-age-days", type=int, default=7)
    parser.add_argument("--failure-lookback-hours", type=int, default=24)
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument(
        "--source-env",
        type=Path,
        default=REPO_ROOT / ".env",
        help=f"Source env file used by the repair flow (default: {REPO_ROOT / '.env'})",
    )
    parser.add_argument("--modal-environment", default="")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _parse_timestamp(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        normalized = raw.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _cookie_age_days(refreshed_at: datetime | None) -> float | None:
    if refreshed_at is None:
        return None
    return max(0.0, (datetime.now(tz=UTC) - refreshed_at).total_seconds() / 86_400.0)


def run_worker(
    *,
    max_cookie_age_days: int,
    failure_lookback_hours: int,
    check_only: bool = False,
    source_env: Path | None = None,
    modal_environment: str = "",
) -> dict[str, Any]:
    load_env()

    cookie_file = social_repo._instagram_cookie_refresh_target_path()
    cookie_metadata = instagram_cookie_refresh.read_instagram_cookie_file_metadata(cookie_file)
    refreshed_at = _parse_timestamp(cookie_metadata.get("_cookie_refreshed_at"))
    cookie_age_days = _cookie_age_days(refreshed_at)
    repair_signal = social_repo.get_instagram_auth_repair_signal(
        failure_lookback_hours=failure_lookback_hours,
    )

    trigger_reason_codes = list(repair_signal.get("reason_codes") or [])
    if refreshed_at is None:
        trigger_reason_codes.append("cookie_age_unknown")
    elif cookie_age_days is not None and cookie_age_days >= max(1, int(max_cookie_age_days)):
        trigger_reason_codes.append("cookie_age_exceeded")

    needs_repair = bool(repair_signal.get("needs_repair")) or ("cookie_age_exceeded" in trigger_reason_codes) or (
        "cookie_age_unknown" in trigger_reason_codes
    )

    payload: dict[str, Any] = {
        "ok": True,
        "action": "skip",
        "needs_repair": needs_repair,
        "cookie_file": str(cookie_file),
        "cookie_refreshed_at": refreshed_at.isoformat().replace("+00:00", "Z") if refreshed_at else None,
        "cookie_age_days": round(cookie_age_days, 3) if cookie_age_days is not None else None,
        "trigger": {
            "reason_codes": trigger_reason_codes,
            "cookie_validation": repair_signal.get("cookie_validation"),
            "latest_failure": repair_signal.get("latest_failure"),
        },
    }
    if not needs_repair:
        return payload

    if check_only:
        payload["action"] = "check_only"
        return payload

    repair_summary = repair_instagram_auth.run_repair(
        source_env=source_env or (REPO_ROOT / ".env"),
        modal_environment=str(modal_environment or "").strip(),
    )
    payload["action"] = "repair"
    payload["repair"] = repair_summary
    payload["ok"] = bool(repair_summary.get("ok"))
    if not payload["ok"]:
        payload["failure_reason"] = repair_summary.get("failure_reason")
    return payload


def main() -> int:
    args = parse_args()
    summary = run_worker(
        max_cookie_age_days=args.max_cookie_age_days,
        failure_lookback_hours=args.failure_lookback_hours,
        check_only=bool(args.check_only),
        source_env=args.source_env,
        modal_environment=str(args.modal_environment or "").strip(),
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(json.dumps(summary, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
