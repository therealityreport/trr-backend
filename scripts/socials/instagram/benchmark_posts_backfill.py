#!/usr/bin/env python3
"""Bounded Instagram Backfill Posts benchmark payload helper.

The command is intentionally side-effect-free unless a future implementation
adds an explicit execution backend. It standardizes the JSON fields operators
need when comparing listing/doc-id/resume/detail changes across runs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


BENCHMARK_MODES = {"listing-only", "doc-id-pinning", "resume", "detail-enrichment", "bidirectional-probe"}


@dataclass(frozen=True)
class BenchmarkRequest:
    account: str
    mode: str
    max_pages: int
    source_scope: str = "bravo"
    run_id: str | None = None
    job_id: str | None = None


def _env_truthy(name: str) -> bool:
    return str(os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def instagram_posts_feature_flags_from_env() -> dict[str, bool]:
    return {
        "bidirectional_walk_enabled": _env_truthy("SOCIAL_INSTAGRAM_POSTS_BIDIRECTIONAL_WALK_ENABLED"),
        "per_ip_pacing_enabled": _env_truthy("SOCIAL_INSTAGRAM_POSTS_PER_IP_PACING_ENABLED"),
        "page_proxy_rotation_enabled": _env_truthy("SOCIAL_INSTAGRAM_POSTS_PAGE_PROXY_ROTATION_ENABLED"),
        "shared_warmup_enabled": _env_truthy("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED"),
    }


def build_benchmark_payload(request: BenchmarkRequest, *, now: datetime | None = None) -> dict[str, Any]:
    normalized_account = str(request.account or "").strip().lower().lstrip("@")
    normalized_mode = str(request.mode or "").strip().lower()
    if not normalized_account:
        raise ValueError("account is required")
    if normalized_mode not in BENCHMARK_MODES:
        raise ValueError(f"mode must be one of: {', '.join(sorted(BENCHMARK_MODES))}")
    safe_max_pages = max(1, min(int(request.max_pages or 1), 100))
    return {
        "benchmark": "instagram_posts_backfill",
        "account": normalized_account,
        "source_scope": str(request.source_scope or "bravo").strip().lower() or "bravo",
        "mode": normalized_mode,
        "max_pages": safe_max_pages,
        "run_id": str(request.run_id or "").strip() or None,
        "job_id": str(request.job_id or "").strip() or None,
        "started_at": (now or datetime.now(UTC)).isoformat(),
        "operator_output_contract": {
            "account_scoped": True,
            "bounded": True,
            "dry_run": True,
            "live_scrape_executed": False,
        },
        "run_metrics": {
            "pages_fetched": 0,
            "posts_fetched": 0,
            "posts_upserted": 0,
            "stop_reason": "benchmark_payload_only",
            "cooldown_state": "not_checked",
            "decodo_mode": str(os.getenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER") or "").strip() or "direct_or_env_default",
        },
        "metrics": {
            "pages_per_second": None,
            "posts_per_second": None,
            "doc_id_attempts_per_page": None,
            "warmup_duration_ms": None,
            "resume_cursor_used": None,
            "detail_fetch_attempts_per_post": None,
        },
        "phase_durations_ms": {
            "auth_probe": None,
            "warmup": None,
            "listing": None,
            "detail_refresh": None,
            "comments": None,
            "media": None,
        },
        "request_counts": {
            "listing_pages": 0,
            "doc_id_attempts": 0,
            "detail_fetch_attempts": 0,
        },
        "feature_flags": instagram_posts_feature_flags_from_env(),
        "proxy_pacing": {},
        "warmup_pool": {},
        "bidirectional_probe": {},
        "field_coverage": {},
        "notes": [
            "Populate metrics from a bounded smoke run or run metadata diff.",
            "Do not claim speedup without comparing this payload across equivalent accounts.",
        ],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", required=True, help="Instagram handle to benchmark")
    parser.add_argument("--mode", required=True, choices=sorted(BENCHMARK_MODES))
    parser.add_argument("--max-pages", type=int, default=3)
    parser.add_argument("--source-scope", default="bravo")
    parser.add_argument("--run-id")
    parser.add_argument("--job-id")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = build_benchmark_payload(
        BenchmarkRequest(
            account=args.account,
            mode=args.mode,
            max_pages=args.max_pages,
            source_scope=args.source_scope,
            run_id=args.run_id,
            job_id=args.job_id,
        )
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
