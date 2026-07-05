#!/usr/bin/env python3
"""Operator wrapper for focused Bravo Instagram comments recovery."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
from pathlib import Path
from typing import Any

try:
    from scripts.socials.instagram import enqueue_comments_audit_cursor_retries as enqueue_cli
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from scripts.socials.instagram import enqueue_comments_audit_cursor_retries as enqueue_cli
    from trr_backend.utils.env import load_env


CONFIRM_SAFE_12 = "SAFE 12 WORKERS"
CONFIRM_BRAVO_ENQUEUE = enqueue_cli.CONFIRM_ENQUEUE
RETRY_TAIL_STOP_REASONS = (
    "network_budget_exhausted",
    "network_policy_blocked",
    "network_stop",
    "network_stopped",
    "proxy_budget_exhausted",
    "proxy_network_stop",
    "static_cdn_budget_exhausted",
)


def _split_csv_values(values: list[str] | None) -> list[str]:
    items: list[str] = []
    for value in values or []:
        items.extend(part.strip() for part in str(value or "").split(",") if part.strip())
    return list(dict.fromkeys(items))


def _read_shortcodes_file(path: str | None) -> list[str]:
    if not path:
        return []
    text = Path(path).read_text(encoding="utf-8")
    values: list[str] = []
    for line in text.splitlines():
        clean = line.split("#", 1)[0].strip()
        if clean:
            values.extend(part.strip() for part in clean.split(",") if part.strip())
    return list(dict.fromkeys(values))


def _normalize_account(value: Any) -> str:
    return str(value or "").strip().lower().lstrip("@")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bravo_straggler_recovery",
        description="Dry-run or enqueue focused Bravo Instagram comment recovery jobs.",
    )
    parser.add_argument("--account", default="bravotv", help="Instagram account handle; defaults to BravoTV.")
    parser.add_argument("--limit", type=int, default=50, help="Maximum latest audit-cursor rows to inspect.")
    parser.add_argument(
        "--approved-shortcode", action="append", help="Approved shortcode. Repeatable or comma-separated."
    )
    parser.add_argument(
        "--approved-shortcodes-file", help="File containing approved shortcodes, one or comma-separated."
    )
    parser.add_argument("--show-filter", action="append", help="Optional show/caption/hashtag filter. Repeatable.")
    parser.add_argument("--retry-tail", action="store_true", help="Use retry/network stop reasons only.")
    parser.add_argument("--batch-size", type=int, default=1, help="Target shortcodes per comments job.")
    parser.add_argument("--comments-worker-count", type=int, help="Explicit comments worker/shard count.")
    parser.add_argument("--safe-preset", choices=("8", "12"), default=os.getenv("SAFE_PRESET"))
    parser.add_argument("--confirm-safe-12", help=f"Required for --safe-preset 12. Exact value: {CONFIRM_SAFE_12!r}.")
    parser.add_argument("--max-comments-per-post", type=int, default=0, help="0 means uncapped.")
    parser.add_argument("--comments-load-strategy", default="public_relay")
    parser.add_argument("--date-start", help="Optional post posted_at lower bound, inclusive.")
    parser.add_argument("--date-end", help="Optional post posted_at upper bound, exclusive.")
    parser.add_argument("--skip-launch-auth-probe", action="store_true")
    parser.add_argument("--force-rerun-existing", action="store_true")
    parser.add_argument("--enqueue", action="store_true", help="Create the comments retry run.")
    parser.add_argument(
        "--confirm-enqueue",
        help=f"Required with --enqueue. Exact value: {CONFIRM_BRAVO_ENQUEUE!r}.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON only.")
    return parser.parse_args(argv)


def _effective_worker_count(args: argparse.Namespace) -> int | None:
    if args.safe_preset == "12":
        if args.confirm_safe_12 != CONFIRM_SAFE_12:
            raise ValueError("confirm_safe_12_required")
        return 12
    if args.safe_preset == "8":
        return 8
    return args.comments_worker_count


def _delegate_argv(args: argparse.Namespace) -> list[str]:
    account = _normalize_account(args.account)
    if not account:
        raise ValueError("account_required")

    approved_shortcodes = list(
        dict.fromkeys(
            [
                *_split_csv_values(args.approved_shortcode),
                *_read_shortcodes_file(args.approved_shortcodes_file),
            ]
        )
    )
    if args.enqueue and not approved_shortcodes:
        raise ValueError("approved_shortcodes_required_for_enqueue")

    worker_count = _effective_worker_count(args)
    batch_size = max(1, int(args.batch_size or 1))
    delegate = [
        "--account",
        account,
        "--limit",
        str(max(1, int(args.limit or 1))),
        "--batch-size",
        str(batch_size),
        "--max-comments-per-post",
        str(max(0, int(args.max_comments_per_post or 0))),
        "--comments-load-strategy",
        str(args.comments_load_strategy or "public_relay"),
        "--json",
    ]
    if args.date_start:
        delegate.extend(["--date-start", str(args.date_start).strip()])
    if args.date_end:
        delegate.extend(["--date-end", str(args.date_end).strip()])
    if worker_count is not None:
        delegate.extend(["--comments-worker-count", str(max(1, int(worker_count)))])
    for shortcode in approved_shortcodes:
        delegate.extend(["--shortcode", shortcode])
    for show_filter in _split_csv_values(args.show_filter):
        delegate.extend(["--show-filter", show_filter])
    if args.retry_tail:
        for reason in RETRY_TAIL_STOP_REASONS:
            delegate.extend(["--stop-reason", reason])
    if args.skip_launch_auth_probe:
        delegate.append("--skip-launch-auth-probe")
    if args.force_rerun_existing:
        delegate.append("--force-rerun-existing")
    if args.enqueue:
        delegate.extend(["--enqueue", "--confirm-enqueue", str(args.confirm_enqueue or "")])
    return delegate


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    try:
        delegate_argv = _delegate_argv(args)
    except ValueError as exc:
        return {
            "ok": False,
            "mode": "enqueue" if args.enqueue else "dry_run",
            "account": _normalize_account(args.account),
            "failure_reason": str(exc),
            "delegate_argv": [],
            "enqueue": {"requested": bool(args.enqueue), "performed": False},
        }

    delegate_args = enqueue_cli._parse_args(delegate_argv)  # noqa: SLF001 - intentional CLI composition.
    payload = enqueue_cli._build_payload(delegate_args)  # noqa: SLF001 - intentional CLI composition.
    payload.setdefault("ok", bool(payload.get("ok", True)))
    payload["bravo_recovery"] = {
        "delegate_argv": delegate_argv,
        "delegate_command": "scripts/socials/instagram/enqueue_comments_audit_cursor_retries.py "
        + shlex.join(delegate_argv),
        "safe_preset": args.safe_preset,
        "batch_size": max(1, int(args.batch_size or 1)),
        "comments_worker_count": _effective_worker_count(args),
        "retry_tail": bool(args.retry_tail),
    }
    return payload


def main(argv: list[str] | None = None) -> int:
    load_env()
    env_args = shlex.split(os.getenv("BRAVO_RECOVERY_ARGS", ""))
    args = _parse_args([*env_args, *(argv if argv is not None else sys.argv[1:])])
    payload = build_payload(args)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
