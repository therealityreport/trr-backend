#!/usr/bin/env python3
"""Benchmark Instagram comments shard runtime without launching a scrape.

Fixture mode is the default. Live mode is read-only and requires explicit
confirmation plus an active-job preflight so this command cannot accidentally
compete with an operator run.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@dataclass(frozen=True, slots=True)
class FixturePost:
    shortcode: str
    top_level_comments: int
    replies: int
    media_comments: int
    hidden_reveal_attempted: bool = False
    hidden_comments_merged: int = 0
    hidden_reveal_skipped: bool = False
    browser_fallback_ms: int = 0
    transport_retries: int = 0
    proxy_session: str = "fixture-session-a"
    retryable_gaps: int = 0
    terminal_unavailable: int = 0
    stop_reason: str = "complete_fetchable"


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="benchmark_comments_shards",
        description="Benchmark Instagram comments shard metrics in fixture mode, or read-only live mode with guards.",
    )
    parser.add_argument(
        "--fixture-profile",
        choices=("tiny", "10", "50"),
        default="tiny",
        help="Fixture-backed profile to run. Fixture mode is the default.",
    )
    parser.add_argument("--output", help="Optional JSON output file.")
    parser.add_argument("--live", action="store_true", help="Use read-only live DB rows instead of fixtures.")
    parser.add_argument("--account", help="Instagram account handle for live mode.")
    parser.add_argument(
        "--confirm-live",
        action="store_true",
        help="Required with --live to acknowledge the command will query live DB state.",
    )
    parser.add_argument(
        "--active-job-preflight",
        action="store_true",
        help="Required with --live; blocks when active comments_scrapling jobs exist for the account.",
    )
    parser.add_argument(
        "--limit-posts",
        type=int,
        default=50,
        help="Maximum recent live job rows to inspect in live mode.",
    )
    return parser.parse_args(argv)


def _normalize_account(value: str | None) -> str:
    return str(value or "").strip().lstrip("@").lower()


def _require_live_guards(args: argparse.Namespace) -> list[str]:
    missing: list[str] = []
    if not _normalize_account(args.account):
        missing.append("--account")
    if not args.confirm_live:
        missing.append("--confirm-live")
    if not args.active_job_preflight:
        missing.append("--active-job-preflight")
    return missing


def _build_fixture_profile(profile: str) -> list[FixturePost]:
    if profile == "tiny":
        return [
            FixturePost(
                shortcode="fixture_reply_tail",
                top_level_comments=18,
                replies=11,
                media_comments=0,
                transport_retries=1,
                proxy_session="fixture-session-a",
                stop_reason="complete_fetchable_with_replies",
            ),
            FixturePost(
                shortcode="fixture_hidden_reveal",
                top_level_comments=9,
                replies=2,
                media_comments=0,
                hidden_reveal_attempted=True,
                hidden_comments_merged=3,
                browser_fallback_ms=420,
                proxy_session="fixture-session-b",
                stop_reason="complete_fetchable_hidden_merged",
            ),
            FixturePost(
                shortcode="fixture_media_comment",
                top_level_comments=7,
                replies=4,
                media_comments=2,
                proxy_session="fixture-session-c",
                stop_reason="complete_fetchable_media_comments",
            ),
        ]

    post_count = 10 if profile == "10" else 50
    posts: list[FixturePost] = []
    for index in range(1, post_count + 1):
        has_hidden_gap = index % 11 == 0
        has_media = index % 7 == 0
        retryable = 1 if index % 13 == 0 else 0
        posts.append(
            FixturePost(
                shortcode=f"fixture_{profile}_{index:03d}",
                top_level_comments=10 + (index % 5) * 3,
                replies=(index % 4) * 4,
                media_comments=1 if has_media else 0,
                hidden_reveal_attempted=has_hidden_gap,
                hidden_comments_merged=2 if has_hidden_gap and not retryable else 0,
                hidden_reveal_skipped=bool(retryable),
                browser_fallback_ms=360 if has_hidden_gap else 0,
                transport_retries=1 if index % 9 == 0 else 0,
                proxy_session=f"fixture-session-{(index % 4) + 1}",
                retryable_gaps=retryable,
                terminal_unavailable=1 if index % 29 == 0 else 0,
                stop_reason="partial_retryable" if retryable else "complete_fetchable",
            )
        )
    return posts


def _estimated_post_runtime_ms(post: FixturePost) -> float:
    comment_work_ms = (post.top_level_comments * 1.8) + (post.replies * 2.5)
    media_work_ms = post.media_comments * 6.0
    retry_work_ms = post.transport_retries * 45.0
    retryable_gap_ms = post.retryable_gaps * 30.0
    return round(
        55.0 + comment_work_ms + media_work_ms + retry_work_ms + retryable_gap_ms + post.browser_fallback_ms,
        2,
    )


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, math.ceil((percentile / 100.0) * len(ordered)))
    return round(ordered[min(rank - 1, len(ordered) - 1)], 2)


def _summarize_fixture_profile(profile: str) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    wall_start = time.perf_counter()
    posts = _build_fixture_profile(profile)
    post_runtime_ms = [_estimated_post_runtime_ms(post) for post in posts]
    wall_seconds = time.perf_counter() - wall_start

    stop_reasons: dict[str, int] = {}
    proxy_sessions: dict[str, int] = {}
    for post in posts:
        stop_reasons[post.stop_reason] = stop_reasons.get(post.stop_reason, 0) + 1
        proxy_sessions[post.proxy_session] = proxy_sessions.get(post.proxy_session, 0) + 1

    top_level_comments = sum(post.top_level_comments for post in posts)
    replies = sum(post.replies for post in posts)
    hidden_merged = sum(post.hidden_comments_merged for post in posts)
    media_comments = sum(post.media_comments for post in posts)

    return {
        "status": "ok",
        "mode": "fixture",
        "fixture_profile": profile,
        "account": None,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "safety": {
            "fixture_mode_default": True,
            "live_mode_requires_confirm_live": True,
            "live_mode_requires_active_job_preflight": True,
            "launched_scrape": False,
        },
        "totals": {
            "posts_processed": len(posts),
            "top_level_comments": top_level_comments,
            "replies": replies,
            "flattened_saved": top_level_comments + replies + hidden_merged,
            "media_comments": media_comments,
            "hidden_reveal_attempts": sum(1 for post in posts if post.hidden_reveal_attempted),
            "hidden_comments_merged": hidden_merged,
            "hidden_reveal_skipped": sum(1 for post in posts if post.hidden_reveal_skipped),
            "transport_retries": sum(post.transport_retries for post in posts),
            "retryable_gaps": sum(post.retryable_gaps for post in posts),
            "terminal_unavailable": sum(post.terminal_unavailable for post in posts),
        },
        "timing": {
            "wall_seconds": round(wall_seconds, 4),
            "fixture_estimated_seconds": round(sum(post_runtime_ms) / 1000.0, 3),
            "browser_fallback_seconds": round(sum(post.browser_fallback_ms for post in posts) / 1000.0, 3),
            "per_post_median_ms": _percentile(post_runtime_ms, 50.0),
            "per_post_p95_ms": _percentile(post_runtime_ms, 95.0),
        },
        "transport": {
            "proxy_session_usage": proxy_sessions,
            "session_count": len(proxy_sessions),
            "redirect_refresh_count": 0,
        },
        "stop_reasons": stop_reasons,
        "sample_posts": [
            {
                "shortcode": post.shortcode,
                "top_level_comments": post.top_level_comments,
                "replies": post.replies,
                "media_comments": post.media_comments,
                "hidden_reveal_attempted": post.hidden_reveal_attempted,
                "stop_reason": post.stop_reason,
                "estimated_runtime_ms": runtime_ms,
            }
            for post, runtime_ms in list(zip(posts, post_runtime_ms, strict=True))[:5]
        ],
    }


def _load_db_helpers():
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env

    return pg, load_env


def _active_live_comments_jobs(account: str) -> dict[str, Any]:
    pg, load_env = _load_db_helpers()
    load_env()
    rows = pg.fetch_all(
        """
        select
          count(*) as active_comment_jobs,
          array_remove(array_agg(distinct run_id::text), null) as active_run_ids
        from social.scrape_jobs
        where platform = 'instagram'
          and coalesce(config->>'stage', metadata->>'stage', job_type) = 'comments_scrapling'
          and status in ('queued', 'pending', 'retrying', 'running')
          and ltrim(lower(coalesce(
            config->>'account',
            metadata->>'account',
            config->>'account_handle',
            metadata->>'account_handle',
            config->>'owner_username',
            metadata->>'owner_username',
            ''
          )), '@') = %s
        """,
        [account],
    )
    row = rows[0] if rows else {}
    return {
        "active_comment_jobs": int(row.get("active_comment_jobs") or 0),
        "active_run_ids": [str(value) for value in row.get("active_run_ids") or []],
    }


def _int_from_nested(mapping: dict[str, Any], *keys: str) -> int:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, bool):
            continue
        try:
            parsed = int(cast(Any, value))
        except (TypeError, ValueError):
            continue
        return max(0, parsed)
    return 0


def _sum_post_latency_samples(metadata: dict[str, Any], key: str) -> int:
    post_latency = metadata.get("post_latency")
    if not isinstance(post_latency, dict):
        return 0
    total = 0
    for sample in post_latency.get("samples") or []:
        if isinstance(sample, dict):
            total += _int_from_nested(sample, key)
    return total


def _transport_retry_count(fetcher_runtime: dict[str, Any], retrieval_meta: dict[str, Any]) -> int:
    explicit = _int_from_nested(retrieval_meta, "transport_retries", "retry_count")
    if explicit:
        return explicit
    retry_reason_counts = fetcher_runtime.get("retry_reason_counts")
    if not isinstance(retry_reason_counts, dict):
        return 0
    return sum(
        max(0, int(value))
        for value in retry_reason_counts.values()
        if not isinstance(value, bool) and str(value).strip().isdigit()
    )


def _summarize_live_rows(account: str, *, limit_posts: int) -> dict[str, Any]:
    pg, load_env = _load_db_helpers()
    load_env()
    rows = pg.fetch_all(
        """
        select
          id::text as job_id,
          status,
          items_found,
          last_error_code,
          metadata
        from social.scrape_jobs
        where platform = 'instagram'
          and coalesce(config->>'stage', metadata->>'stage', job_type) = 'comments_scrapling'
          and ltrim(lower(coalesce(
            config->>'account',
            metadata->>'account',
            config->>'account_handle',
            metadata->>'account_handle',
            config->>'owner_username',
            metadata->>'owner_username',
            ''
          )), '@') = %s
        order by created_at desc
        limit %s
        """,
        [account, max(1, int(limit_posts))],
    )

    stop_reasons: dict[str, int] = {}
    totals = {
        "posts_processed": 0,
        "top_level_comments": 0,
        "replies": 0,
        "flattened_saved": 0,
        "media_comments": 0,
        "hidden_reveal_attempts": 0,
        "hidden_comments_merged": 0,
        "hidden_reveal_skipped": 0,
        "transport_retries": 0,
        "retryable_gaps": 0,
        "terminal_unavailable": 0,
    }
    for row in rows:
        metadata = dict(row.get("metadata") or {})
        activity = dict(metadata.get("activity") or {})
        counters = dict(metadata.get("persist_counters") or {})
        stage_counters = dict(metadata.get("stage_counters") or {})
        fetcher_runtime = dict(metadata.get("fetcher_runtime") or {})
        hidden_comments = dict(fetcher_runtime.get("hidden_comments") or {})
        retrieval_meta = dict(metadata.get("retrieval_meta") or {})
        sample_top_level_comments = _sum_post_latency_samples(metadata, "top_level_comment_count")
        sample_flattened_comments = _sum_post_latency_samples(metadata, "observed_comment_count")
        if not sample_flattened_comments:
            sample_flattened_comments = _sum_post_latency_samples(metadata, "comments_fetched")
        flattened_comments = (
            sample_flattened_comments
            or _int_from_nested(stage_counters, "comments")
            or max(0, int(row.get("items_found") or 0) - _int_from_nested(stage_counters, "posts"))
        )
        top_level_comments = (
            sample_top_level_comments
            or _int_from_nested(counters, "top_level_comments", "top_level_comment_count")
            or _int_from_nested(counters, "comments_upserted")
        )
        reply_comments = _int_from_nested(counters, "replies", "reply_comments", "saved_reply_comments")
        if not reply_comments and flattened_comments and top_level_comments:
            reply_comments = max(0, flattened_comments - top_level_comments)
        stop_reason = str(
            metadata.get("latest_retryable_stop_reason")
            or metadata.get("stop_reason")
            or row.get("last_error_code")
            or row.get("status")
            or "unknown"
        )
        stop_reasons[stop_reason] = stop_reasons.get(stop_reason, 0) + 1
        totals["posts_processed"] += _int_from_nested(activity, "posts_checked", "completed_posts")
        totals["top_level_comments"] += top_level_comments
        totals["replies"] += reply_comments
        totals["flattened_saved"] += flattened_comments
        totals["media_comments"] += _int_from_nested(counters, "media_comments", "media_comment_rows")
        totals["hidden_reveal_attempts"] += _int_from_nested(
            activity,
            "hidden_reveal_attempts",
        ) or _int_from_nested(hidden_comments, "render_attempts")
        totals["hidden_comments_merged"] += _int_from_nested(
            counters,
            "hidden_comments_merged",
        ) or _int_from_nested(hidden_comments, "merged_comments")
        totals["hidden_reveal_skipped"] += _int_from_nested(activity, "hidden_reveal_skipped_due_budget")
        totals["transport_retries"] += _transport_retry_count(fetcher_runtime, retrieval_meta)
        totals["retryable_gaps"] += _int_from_nested(
            metadata,
            "retryable_gaps",
            "checkpoint_backed_repair_target_count",
        )
        totals["terminal_unavailable"] += _int_from_nested(metadata, "terminal_unavailable")
    totals["flattened_saved"] = max(totals["flattened_saved"], totals["top_level_comments"] + totals["replies"])

    return {
        "status": "ok",
        "mode": "live_read_only",
        "fixture_profile": None,
        "account": account,
        "started_at": datetime.now(UTC).isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "safety": {
            "fixture_mode_default": True,
            "live_mode_requires_confirm_live": True,
            "live_mode_requires_active_job_preflight": True,
            "launched_scrape": False,
        },
        "totals": totals,
        "timing": {
            "wall_seconds": 0.0,
            "fixture_estimated_seconds": None,
            "browser_fallback_seconds": None,
            "per_post_median_ms": None,
            "per_post_p95_ms": None,
        },
        "transport": {"proxy_session_usage": {}, "session_count": None, "redirect_refresh_count": None},
        "stop_reasons": stop_reasons,
        "sample_jobs": [
            {
                "job_id": row.get("job_id"),
                "status": row.get("status"),
                "items_found": row.get("items_found"),
                "last_error_code": row.get("last_error_code"),
            }
            for row in rows[:5]
        ],
    }


def run_benchmark(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    if not args.live:
        return 0, _summarize_fixture_profile(str(args.fixture_profile))

    missing = _require_live_guards(args)
    if missing:
        return 2, {
            "status": "refused",
            "mode": "live_read_only",
            "error": "live_mode_requires_explicit_flags",
            "missing_flags": missing,
            "safety": {"launched_scrape": False},
        }

    account = _normalize_account(args.account)
    preflight = _active_live_comments_jobs(account)
    if preflight["active_comment_jobs"] > 0:
        return 2, {
            "status": "blocked_active_jobs",
            "mode": "live_read_only",
            "account": account,
            "active_job_preflight": preflight,
            "safety": {"launched_scrape": False},
        }

    payload = _summarize_live_rows(account, limit_posts=args.limit_posts)
    payload["active_job_preflight"] = preflight
    return 0, payload


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    rc, payload = run_benchmark(args)
    output = json.dumps(payload, indent=2, sort_keys=True, default=str)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output + "\n", encoding="utf-8")
    print(output)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
