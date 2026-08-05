#!/usr/bin/env python3
"""Run the Modal BRAVOTV Instagram public-history proof function."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.modal.deploy_backend import REQUIRED_MODAL_PROFILE  # noqa: E402
from trr_backend.modal_dispatch import get_trr_modal_function_handle  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", default="bravotv", help="Instagram account handle to probe.")
    parser.add_argument("--until-date", default="2025-01-01", help="Historical boundary date.")
    parser.add_argument("--target-years", default="2025,2026", help="Comma-delimited post years to report.")
    parser.add_argument("--max-pages", type=int, default=0, help="0 means no page cap.")
    parser.add_argument(
        "--stop-at-boundary",
        action="store_true",
        help="Stop when the historical boundary is reached instead of continuing into older posts.",
    )
    parser.add_argument("--sample-details-per-page", type=int, default=2)
    parser.add_argument("--sample-comments-per-page", type=int, default=1)
    parser.add_argument("--comments-mode", choices=("sampled", "all"), default="sampled")
    parser.add_argument("--details-mode", choices=("sampled", "all"), default="sampled")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--auto-resume", action="store_true", help="Repeat Modal calls when a public run can resume.")
    parser.add_argument("--state-file", type=Path, help="Local file for durable auto-resume state.")
    parser.add_argument("--output", type=Path, help="Local file for the latest proof payload.")
    parser.add_argument("--max-resume-attempts", type=int, default=25)
    parser.add_argument("--max-auto-wait-seconds", type=int, default=0)
    parser.add_argument("--app-name", default="trr-backend-jobs", help="Modal app name.")
    parser.add_argument(
        "--function-name",
        default="probe_instagram_public_history",
        help="Modal function name for the Instagram public-history proof.",
    )
    parser.add_argument("--json", action="store_true", help="Emit the raw proof payload as JSON.")
    return parser.parse_args(argv if argv is not None else sys.argv[1:])


def verify_instagram_public_history(
    *,
    account: str,
    until_date: str,
    target_years: str,
    max_pages: int,
    continue_after_boundary: bool,
    sample_details_per_page: int,
    sample_comments_per_page: int,
    comments_mode: str,
    details_mode: str,
    resume: bool,
    state_payload: dict[str, Any] | None = None,
    app_name: str,
    function_name: str,
) -> dict[str, Any]:
    normalized_account = str(account or "").strip().lstrip("@") or "bravotv"
    fn = get_trr_modal_function_handle(function_name, app_name=app_name)
    payload = fn.remote(
        account_handle=normalized_account,
        until_date=until_date,
        target_years=target_years,
        max_pages=max_pages,
        continue_after_boundary=continue_after_boundary,
        sample_details_per_page=sample_details_per_page,
        sample_comments_per_page=sample_comments_per_page,
        comments_mode=comments_mode,
        details_mode=details_mode,
        resume=resume,
        state_payload=state_payload,
        scrub_public_env=True,
    )
    result = dict(payload or {}) if isinstance(payload, dict) else {"raw_result": payload}
    result.setdefault("account", normalized_account)
    return result


def main(argv: list[str] | None = None) -> int:
    os.environ["MODAL_PROFILE"] = REQUIRED_MODAL_PROFILE
    args = parse_args(argv)
    state_payload = _read_json(args.state_file) if args.resume or args.auto_resume else None
    payload: dict[str, Any] = {}
    attempts = max(1, int(args.max_resume_attempts or 1)) if args.auto_resume else 1
    for attempt in range(1, attempts + 1):
        payload = verify_instagram_public_history(
            account=args.account,
            until_date=args.until_date,
            target_years=args.target_years,
            max_pages=args.max_pages,
            continue_after_boundary=not args.stop_at_boundary,
            sample_details_per_page=args.sample_details_per_page,
            sample_comments_per_page=args.sample_comments_per_page,
            comments_mode=args.comments_mode,
            details_mode=args.details_mode,
            resume=bool(args.resume or state_payload),
            state_payload=state_payload,
            app_name=args.app_name,
            function_name=args.function_name,
        )
        payload["auto_resume_attempt"] = attempt
        state_payload = (
            payload.get("state_payload") if isinstance(payload.get("state_payload"), dict) else state_payload
        )
        _write_json(args.state_file, state_payload)
        _write_json(args.output, payload)
        if _is_terminal(payload):
            break
        wait_seconds = _resume_wait_seconds(payload)
        if not args.auto_resume or attempt >= attempts or wait_seconds is None:
            break
        if wait_seconds > max(0, int(args.max_auto_wait_seconds or 0)):
            payload["auto_resume_stop_reason"] = "retry_wait_exceeds_current_run_cap"
            payload["auto_resume_next_wait_seconds"] = wait_seconds
            _write_json(args.output, payload)
            break
        if wait_seconds > 0:
            time.sleep(wait_seconds)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Instagram public proof for @{payload.get('account')}: {payload.get('stop_reason')}")
        print(f"  pages_recovered: {payload.get('pages_recovered')}")
        print(f"  unique_posts_recovered: {payload.get('unique_posts_recovered')}")
        print(f"  target_posts_recovered: {payload.get('target_posts_recovered')}")
        print(f"  target_year_counts: {payload.get('target_year_counts')}")
        print(f"  oldest_post_at: {payload.get('oldest_post_at')}")
        print(f"  output_file: {payload.get('output_file')}")
    return 0 if payload.get("stop_reason") in {"historical_boundary_reached", "account_exhausted"} else 1


def _is_terminal(payload: dict[str, Any]) -> bool:
    return str(payload.get("stop_reason") or "") in {"historical_boundary_reached", "account_exhausted"}


def _resume_wait_seconds(payload: dict[str, Any]) -> int | None:
    stop_reason = str(payload.get("stop_reason") or "")
    if stop_reason == "public_empty_after_progress_retry_later":
        return 60
    if stop_reason.startswith("public_graphql_") and stop_reason.endswith("_backoff_required"):
        return max(0, int(payload.get("next_retry_after_seconds") or 0))
    return None


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_json(path: Path | None, payload: Any) -> None:
    if path is None or payload is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
