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

from trr_backend.socials.pipelines.comments.instagram import (  # noqa: E402
    INSTAGRAM_COMMENTS_AUDIT_CURSOR_RETRY_STOP_REASONS,
    enqueue_instagram_comments_audit_cursor_retries,
)
from trr_backend.utils.env import load_env  # noqa: E402

CONFIRM_ENQUEUE = "ENQUEUE AUDIT CURSOR RETRIES"
DEFAULT_STOP_REASONS = INSTAGRAM_COMMENTS_AUDIT_CURSOR_RETRY_STOP_REASONS


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
    parser.add_argument(
        "--no-attach-active-run",
        action="store_true",
        help="Fail on an already-active run instead of splitting matching queued targets into batch jobs.",
    )
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


def _build_payload(args: argparse.Namespace) -> dict[str, Any]:
    account = _normalize_account(args.account)
    if not account:
        raise SystemExit("--account is required")
    stop_reasons = list(dict.fromkeys(args.stop_reason or DEFAULT_STOP_REASONS))
    shortcodes = _shortcode_filters(args.shortcode)
    if args.enqueue and args.confirm_enqueue != CONFIRM_ENQUEUE:
        return {
            "ok": False,
            "mode": "enqueue" if args.enqueue else "dry_run",
            "account": account,
            "failure_reason": "confirm_enqueue_required",
            "enqueue": {"requested": bool(args.enqueue), "performed": False},
        }
    return enqueue_instagram_comments_audit_cursor_retries(
        account_handle=account,
        limit=max(1, int(args.limit or 1)),
        shortcodes=shortcodes,
        stop_reasons=stop_reasons,
        batch_size=max(1, int(args.batch_size or 1)),
        comments_worker_count=args.comments_worker_count,
        max_comments_per_post=max(0, int(args.max_comments_per_post or 0)),
        comments_load_strategy=str(args.comments_load_strategy or "cursor_api"),
        skip_launch_auth_probe=bool(args.skip_launch_auth_probe),
        dry_run=not bool(args.enqueue),
        attach_to_active_run=not bool(args.no_attach_active_run),
        initiated_by="audit-cursor-retry-cli",
    )


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
