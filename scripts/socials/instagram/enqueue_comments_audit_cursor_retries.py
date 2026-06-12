#!/usr/bin/env python3
"""Enqueue focused Instagram comment retries from durable audit cursors."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trr_backend.db import pg  # noqa: E402
from trr_backend.socials.instagram.comments_scrapling import job_runner as comments_job_runner  # noqa: E402
from trr_backend.socials.pipelines.comments.instagram import start_social_account_comments_scrape  # noqa: E402
from trr_backend.utils.env import load_env  # noqa: E402

CONFIRM_ENQUEUE = "ENQUEUE AUDIT CURSOR RETRIES"
DEFAULT_STOP_REASONS = ("pagination_deadline_exceeded", "pagination_page_cap_reached")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="enqueue_comments_audit_cursor_retries",
        description="Build or enqueue a focused comments retry from instagram_post_comments_audit cursor payloads.",
    )
    parser.add_argument("--account", default="bravotv", help="Instagram account handle.")
    parser.add_argument("--limit", type=int, default=50, help="Maximum latest audit-cursor targets to inspect.")
    parser.add_argument("--shortcode", action="append", help="Restrict to one shortcode. Repeatable.")
    parser.add_argument(
        "--stop-reason",
        action="append",
        choices=DEFAULT_STOP_REASONS,
        help="Eligible audit stop reason. Repeatable; defaults to deadline and page-cap stops.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        help="Target shortcodes per comments job. Capped by the launcher shard limit.",
    )
    parser.add_argument("--comments-worker-count", type=int, help="Optional worker/shard count override.")
    parser.add_argument("--max-comments-per-post", type=int, default=0, help="0 means uncapped.")
    parser.add_argument("--comments-load-strategy", default="cursor_api")
    parser.add_argument("--skip-launch-auth-probe", action="store_true")
    parser.add_argument("--enqueue", action="store_true", help="Create the comments retry run.")
    parser.add_argument(
        "--confirm-enqueue",
        help=f"Required with --enqueue. Exact value: {CONFIRM_ENQUEUE!r}.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON only.")
    return parser.parse_args(argv)


def _normalize_account(value: Any) -> str:
    return str(value or "").strip().lower().lstrip("@")


def _shortcode_filters(values: list[str] | None) -> list[str]:
    shortcodes: list[str] = []
    for value in values or []:
        shortcodes.extend(part.strip() for part in str(value or "").split(",") if part.strip())
    return list(dict.fromkeys(shortcodes))


def _load_latest_audit_rows(
    *,
    account: str,
    limit: int,
    shortcodes: list[str],
    stop_reasons: list[str],
) -> list[dict[str, Any]]:
    params: list[Any] = [account, stop_reasons, max(1, int(limit or 1))]
    shortcode_sql = ""
    if shortcodes:
        shortcode_sql = "and shortcode = any(%s::text[])"
        params.insert(2, shortcodes)
    return pg.fetch_all(
        f"""
        with ranked as (
          select
            post_id::text,
            shortcode,
            source_account,
            cursor_stop_reason,
            cursor_min_id,
            cursor_param,
            cursor_payload,
            created_at,
            row_number() over (partition by shortcode order by created_at desc) as row_number
          from social.instagram_post_comments_audit
          where ltrim(lower(coalesce(source_account, '')), '@') = %s
            and cursor_stop_reason = any(%s::text[])
            and cursor_payload is not null
            and cursor_payload <> '{{}}'::jsonb
            {shortcode_sql}
        )
        select
          post_id,
          shortcode,
          source_account,
          cursor_stop_reason,
          cursor_min_id,
          cursor_param,
          cursor_payload,
          created_at::text
        from ranked
        where row_number = 1
        order by created_at desc
        limit %s
        """,
        params,
    )


def _select_retry_targets(rows: list[dict[str, Any]]) -> tuple[list[str], list[dict[str, Any]]]:
    selected_shortcodes: list[str] = []
    selected_rows: list[dict[str, Any]] = []
    for row in rows:
        shortcode = str(row.get("shortcode") or "").strip()
        if not shortcode:
            continue
        top_level_checkpoint = comments_job_runner._normalize_audit_top_level_checkpoint(row)
        reply_checkpoints = comments_job_runner._normalize_audit_reply_checkpoints(row)
        if not top_level_checkpoint and not reply_checkpoints:
            continue
        selected_shortcodes.append(shortcode)
        selected_rows.append(
            {
                "shortcode": shortcode,
                "post_id": row.get("post_id"),
                "cursor_stop_reason": row.get("cursor_stop_reason"),
                "created_at": row.get("created_at"),
                "has_top_level_cursor": bool(top_level_checkpoint),
                "reply_resume_count": len(reply_checkpoints),
            }
        )
    return list(dict.fromkeys(selected_shortcodes)), selected_rows


def _build_payload(args: argparse.Namespace) -> dict[str, Any]:
    account = _normalize_account(args.account)
    if not account:
        raise SystemExit("--account is required")
    stop_reasons = list(dict.fromkeys(args.stop_reason or DEFAULT_STOP_REASONS))
    shortcodes = _shortcode_filters(args.shortcode)
    rows = _load_latest_audit_rows(
        account=account,
        limit=max(1, int(args.limit or 1)),
        shortcodes=shortcodes,
        stop_reasons=stop_reasons,
    )
    target_source_ids, selected_rows = _select_retry_targets(rows)
    payload: dict[str, Any] = {
        "ok": True,
        "mode": "enqueue" if args.enqueue else "dry_run",
        "account": account,
        "selected_target_source_ids": target_source_ids,
        "selected_target_source_ids_count": len(target_source_ids),
        "inspected_audit_rows_count": len(rows),
        "eligible_stop_reasons": stop_reasons,
        "batch_size": max(1, int(args.batch_size or 1)),
        "selected_rows": selected_rows,
        "enqueue": {
            "requested": bool(args.enqueue),
            "performed": False,
        },
    }
    if not args.enqueue:
        return payload
    if args.confirm_enqueue != CONFIRM_ENQUEUE:
        payload["ok"] = False
        payload["failure_reason"] = "confirm_enqueue_required"
        return payload
    if not target_source_ids:
        payload["ok"] = False
        payload["failure_reason"] = "no_eligible_audit_cursor_targets"
        return payload
    result = start_social_account_comments_scrape(
        "instagram",
        account,
        mode="profile",
        refresh_policy="all_saved_posts",
        target_source_ids=target_source_ids,
        max_comments_per_post=max(0, int(args.max_comments_per_post or 0)),
        comments_load_strategy=str(args.comments_load_strategy or "cursor_api"),
        initiated_by="audit-cursor-retry-cli",
        comments_worker_count=args.comments_worker_count,
        comments_target_batch_size=max(1, int(args.batch_size or 1)),
        skip_launch_auth_probe=bool(args.skip_launch_auth_probe),
    )
    payload["enqueue"] = {
        "requested": True,
        "performed": True,
        "result": result,
    }
    return payload


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    payload = _build_payload(args)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
