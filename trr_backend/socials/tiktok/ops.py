"""Operator helpers for TikTok social scripts.

These helpers are intentionally separate from ``direct_scrape`` and the
``posts_scrapling`` lane internals. CLI scripts own argument parsing and output;
this module owns reusable operator behavior.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def parse_date(date_str: str) -> datetime:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.strptime(date_str, "%Y-%m-%d")


def proxy_label(proxy_url: str | None) -> str | None:
    """Return a safe proxy label without credentials."""
    raw_proxy = str(proxy_url or "").strip()
    if not raw_proxy:
        return None
    candidates = [raw_proxy]
    if "://" not in raw_proxy:
        candidates.append(f"//{raw_proxy}")
    for candidate in candidates:
        parsed = urlparse(candidate)
        if parsed.hostname:
            return parsed.hostname
    if "@" in raw_proxy:
        return "redacted-proxy"
    return raw_proxy


def emit_diagnostics_summary(
    *,
    target_label: str,
    scrape_mode: str,
    diagnostics: dict[str, object],
) -> None:
    """Print the safe operator-facing diagnostics summary used by the CLI."""
    endpoint_responses = diagnostics.get("endpoint_responses") if isinstance(diagnostics, dict) else {}
    endpoint_responses = endpoint_responses if isinstance(endpoint_responses, dict) else {}
    failure_summary = {
        endpoint: payload.get("failure_reason")
        for endpoint, payload in endpoint_responses.items()
        if isinstance(payload, dict) and payload.get("failure_reason")
    }

    print("\nDiagnostics:")
    print(f"  Target: {target_label}")
    print(f"  Scrape mode: {scrape_mode}")
    print(f"  HTTP client: {diagnostics.get('http_client') or 'requests'}")
    if diagnostics.get("curl_cffi_impersonate"):
        print(f"  Impersonate: {diagnostics.get('curl_cffi_impersonate')}")
    print(f"  Proxy: {proxy_label(str(diagnostics.get('proxy_label'))) if diagnostics.get('proxy_label') else 'none'}")
    print(f"  Auth mode: {diagnostics.get('auth_mode') or 'without_cookies'}")
    if diagnostics.get("risk_state"):
        print(f"  Risk state: {diagnostics.get('risk_state')}")
    if diagnostics.get("operator_summary"):
        print(f"  Operator summary: {diagnostics.get('operator_summary')}")
    if diagnostics.get("operator_action"):
        print(f"  Operator action: {diagnostics.get('operator_action')}")
    if diagnostics.get("triage_bucket"):
        print(f"  Triage bucket: {diagnostics.get('triage_bucket')}")
    if diagnostics.get("stop_reason"):
        print(f"  Stop reason: {diagnostics.get('stop_reason')}")
    if failure_summary:
        print(f"  Endpoint failures: {json.dumps(failure_summary, sort_keys=True)}")


def write_diagnostics_json(path: str | None, diagnostics: dict[str, object]) -> None:
    """Write safe diagnostics JSON for operator review."""
    if not path:
        return
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(diagnostics, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def run_posts_scrapling_smoke(*, account: str, max_pages: int) -> dict[str, Any]:
    """Create and run a TikTok posts Scrapling smoke job."""
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.tiktok.posts_scrapling.job_runner import (
        run_tiktok_posts_scrapling_job,
    )

    normalized_account = account.strip().lower().lstrip("@")
    run_config = {
        "platform": "tiktok",
        "stage": repo.TIKTOK_POSTS_SCRAPLING_STAGE,
        "account": normalized_account,
    }
    run_id = repo._create_run(
        None,
        source_scope="bravo",
        initiated_by="manual_smoke",
        config=run_config,
        status="queued",
    )
    job_id = repo._create_job(
        None,
        run_id=run_id,
        platform="tiktok",
        source_scope="bravo",
        job_type="posts",
        stage=repo.TIKTOK_POSTS_SCRAPLING_STAGE,
        config={
            **run_config,
            "max_pages": max_pages,
        },
        initiated_by="manual_smoke",
        status="queued",
    )
    job = pg.fetch_one("select * from social.scrape_jobs where id = %s::uuid", [job_id])
    if not job:
        return {
            "run_id": run_id,
            "job_id": job_id,
            "status": "missing_job",
            "items_found": None,
            "error_message": f"job {job_id} not found in social.scrape_jobs",
        }

    result = run_tiktok_posts_scrapling_job(job)
    return {
        "run_id": run_id,
        "job_id": job_id,
        "status": result.get("status", "unknown"),
        "items_found": result.get("items_found"),
        "error_message": result.get("error_message"),
        "result": result,
    }
