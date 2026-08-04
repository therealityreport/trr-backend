from __future__ import annotations

import logging
from datetime import UTC, datetime
from time import perf_counter
from types import SimpleNamespace
from typing import Any

from trr_backend.socials.read_models.account_profile.common import (
    _require_provider_ready,
    get_social_account_profile_summary,
)


def _deferred_get_social_account_catalog_run_progress(*args: Any, **kwargs: Any) -> Any:
    _require_provider_ready()
    from trr_backend.socials.pipelines.account_catalog.progress import (
        get_social_account_catalog_run_progress,
    )

    return get_social_account_catalog_run_progress(*args, **kwargs)


logger = logging.getLogger(__name__)

analytics_repo = SimpleNamespace(
    get_social_account_catalog_run_progress=_deferred_get_social_account_catalog_run_progress,
    get_social_account_profile_summary=get_social_account_profile_summary,
)

ACTIVE_CATALOG_RUN_STATUSES = {
    "queued",
    "pending",
    "running",
    "retrying",
    "cancelling",
    "attached",
    "in_progress",
    "processing",
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _active_run_id_from_summary(summary: dict[str, Any]) -> str | None:
    for run in summary.get("catalog_recent_runs") or []:
        status = str(run.get("status") or run.get("run_status") or "").strip().lower()
        run_id = run.get("run_id") or run.get("id")
        if run_id and status in ACTIVE_CATALOG_RUN_STATUSES:
            return str(run_id)
    return None


def build_social_account_profile_dashboard(
    *,
    platform: str,
    account_handle: str,
    detail: str,
    run_id: str | None,
    recent_log_limit: int,
) -> dict[str, Any]:
    started = perf_counter()
    generated_at = _utc_now()
    normalized_detail = "full" if detail == "full" else "lite"
    summary = analytics_repo.get_social_account_profile_summary(
        platform=platform,
        account_handle=account_handle,
        detail=normalized_detail,
    )
    progress_run_id = run_id or _active_run_id_from_summary(summary)
    progress = None
    if progress_run_id:
        progress = analytics_repo.get_social_account_catalog_run_progress(
            platform,
            account_handle,
            progress_run_id,
            recent_log_limit=recent_log_limit,
            fast=True,
        )

    payload = {
        "data": {
            "summary": summary,
            "catalog_run_progress": progress,
        },
        "freshness": {
            "status": "fresh",
            "source": "live",
            "generated_at": generated_at.isoformat(),
            "age_seconds": 0,
        },
        "operational_alerts": summary.get("operational_alerts") or [],
    }
    logger.info(
        "social_profile_dashboard_loaded",
        extra={
            "route": "social_profile_dashboard",
            "platform": platform,
            "handle": account_handle,
            "duration_ms": round((perf_counter() - started) * 1000),
            "freshness_status": "fresh",
            "has_progress": progress is not None,
        },
    )
    return payload
