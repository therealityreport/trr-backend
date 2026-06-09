"""Cross-account backfill-health read model for the social control plane.

This is the aggregation seam behind ``GET /admin/socials/ingest/backfill-health``.
It stitches together the *existing* per-account / control-plane read functions into
a single cross-account operator surface so the admin dashboard does not have to
fan out one request per account:

  * Per active/recent catalog-backfill **run + account** progress
    (:func:`...account_catalog.progress.get_social_account_catalog_run_progress`,
    called ``fast=True``) — run status, posts fetched, 401/403 + checkpoint
    counts/rates, and per-run bandwidth (``proxy_pacing.bytes_total``).
  * Active **auth cooldowns**
    (:func:`...instagram.auth_cooldown.get_active_cooldown`) — which accounts are
    auth-blocked / checkpointed right now, with ``consecutive_auth_failures`` and
    ``blocker_kind``.
  * **Worker / auth health**
    (:func:`...control_plane.worker_health.get_worker_auth_capabilities` and
    :func:`get_worker_health`).
  * **Queue depth** (:func:`...control_plane.queue_status.get_queue_status`,
    summary path).
  * **Bandwidth** rolled up across runs (sum of ``proxy_pacing.bytes_total``),
    exposed as GB plus a derived USD cost when ``DECODO_USD_PER_GB`` is set.

Design notes:
  * NO new per-run SQL: run/account progress, queue, worker, and cooldown reads all
    reuse the canonical read functions. The only direct query here is a small
    *enumeration* of recent catalog-backfill runs (which (platform, account, run)
    tuples to aggregate) — there is no existing cross-account catalog-run lister,
    and ``list_shared_runs`` is scoped to a different ingest mode
    (``shared_account_async``) so it cannot be reused for catalog backfills.
  * Fully fail-open: a failure aggregating any single run is captured on that run's
    entry (``error``) and never aborts the whole payload; a failure of any optional
    section (queue/worker/cooldown) is degraded to a safe default with an ``error``
    string so the dashboard always renders.
"""

from __future__ import annotations

import logging
import os
from time import perf_counter
from typing import Any

import trr_backend.socials.social_season_analytics_impl as _core
from trr_backend.socials.control_plane.queue_status import get_queue_status
from trr_backend.socials.control_plane.worker_health import (
    get_worker_auth_capabilities,
    get_worker_health,
    is_queue_enabled,
)
from trr_backend.socials.instagram.auth_cooldown import get_active_cooldown
from trr_backend.socials.pipelines.account_catalog.progress import (
    get_social_account_catalog_run_progress,
)

logger = logging.getLogger(__name__)

# Decodo bills on decimal GB (1 GB = 1e9 bytes) — mirror decodo_usage._BYTES_PER_GB
# so the derived cost on this dashboard matches the daily budget alert.
_BYTES_PER_GB = 1_000_000_000

# Run statuses we treat as "active" (vs. terminal) for the active/recent split. Kept
# in sync with the active job/run statuses the catalog progress model recognizes.
_ACTIVE_RUN_STATUSES: frozenset[str] = frozenset(
    {
        "queued",
        "pending",
        "running",
        "retrying",
        "cancelling",
        "attached",
        "in_progress",
        "processing",
        "finalizing",
    }
)

_DEFAULT_RUN_LIMIT = 40
_MAX_RUN_LIMIT = 200


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _bytes_to_gb(num_bytes: int) -> float:
    return float(num_bytes) / _BYTES_PER_GB if num_bytes > 0 else 0.0


def _safe_rate(numerator: int, denominator: int) -> float:
    """numerator/denominator clamped to [0, 1], 0 when the denominator is 0."""
    if denominator <= 0:
        return 0.0
    return max(0.0, min(1.0, numerator / denominator))


def _list_recent_catalog_run_targets(*, limit: int) -> list[dict[str, Any]]:
    """Enumerate recent catalog-backfill runs and their (platform, account) targets.

    Returns one entry per (run, platform, account) tuple. A single run can fan out to
    multiple platforms/accounts (``config.platforms`` / ``config.accounts_override``);
    we expand it so each entry maps 1:1 to a per-account progress read.

    Fail-open: returns ``[]`` on any DB error.
    """
    safe_limit = max(1, min(int(limit), _MAX_RUN_LIMIT))
    try:
        rows = _core.pg.fetch_all(
            """
            select
              id::text as run_id,
              status,
              source_scope,
              config,
              created_at,
              started_at,
              completed_at
            from social.scrape_runs
            where coalesce(config->>'pipeline_ingest_mode', '') = %s
            order by created_at desc
            limit %s
            """,
            [_core.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE, safe_limit],
            pool_name=getattr(_core, "SOCIAL_CATALOG_PROGRESS_POOL_NAME", "default"),
        )
    except Exception as exc:  # noqa: BLE001 - never raise from a read surface
        logger.warning("backfill_health: catalog run enumeration failed: %s", exc)
        return []

    targets: list[dict[str, Any]] = []
    supported_platforms = set(_core.CATALOG_SUPPORTED_PLATFORMS)
    for row in rows:
        run_id = str(row.get("run_id") or "").strip()
        if not run_id:
            continue
        run_config = _core._metadata_dict(row.get("config"))
        platforms = [
            _core._normalize_social_account_profile_platform(value)
            for value in _core._as_text_list(run_config.get("platforms") or [])
        ]
        platforms = [platform for platform in platforms if platform in supported_platforms]
        accounts = [
            _core._normalize_social_account_profile_handle(value)
            for value in _core._as_text_list(run_config.get("accounts_override") or [])
        ]
        accounts = [account for account in accounts if account]
        if not platforms or not accounts:
            # Catalog runs always pin at least one platform + one account override; if
            # either is absent the run is not addressable per-account, so skip it.
            continue
        run_status = str(row.get("status") or "").strip().lower()
        created_at = _core._iso(_core._coerce_dt(row.get("created_at")))
        started_at = _core._iso(_core._coerce_dt(row.get("started_at")))
        completed_at = _core._iso(_core._coerce_dt(row.get("completed_at")))
        source_scope = str(row.get("source_scope") or "").strip() or None
        for platform in platforms:
            for account in accounts:
                targets.append(
                    {
                        "run_id": run_id,
                        "platform": platform,
                        "account_handle": account,
                        "run_status": run_status,
                        "source_scope": source_scope,
                        "created_at": created_at,
                        "started_at": started_at,
                        "completed_at": completed_at,
                    }
                )
    return targets


def _count_auth_failures(progress: dict[str, Any]) -> dict[str, int]:
    """Derive 401/403 (unauthorized/forbidden) and checkpoint job counts from a run.

    The per-run progress payload carries per-job error rows under ``recent_logs`` and
    summarized counts; we re-classify the canonical Instagram auth error codes the
    progress model already surfaces (``instagram_graphql_cursor_forbidden`` →403,
    ``instagram_graphql_cursor_unauthorized`` →401, ``*checkpoint_required`` →
    checkpoint) so the dashboard can show counts/rates without new SQL.
    """
    forbidden = 0
    unauthorized = 0
    checkpoint = 0
    considered = 0
    log_rows = progress.get("recent_logs")
    if not isinstance(log_rows, list):
        log_rows = []
    for row in log_rows:
        if not isinstance(row, dict):
            continue
        considered += 1
        code = str(row.get("last_error_code") or row.get("error_code") or "").strip().lower()
        message = str(row.get("error_message") or "").strip().lower()
        if "checkpoint" in code or "checkpoint" in message or code in {"challenge_required", "login_required"}:
            checkpoint += 1
        if "forbidden" in code or "403" in code or "403" in message:
            forbidden += 1
        if "unauthorized" in code or "401" in code or "401" in message:
            unauthorized += 1
    # The progress payload also exposes a first-auth-failure marker; fold it in so a
    # run with a known auth block is never reported as 0 even if logs were trimmed.
    first_auth_failure_code = str(progress.get("first_auth_failure_code") or "").strip().lower()
    if first_auth_failure_code:
        if "checkpoint" in first_auth_failure_code and checkpoint == 0:
            checkpoint = 1
        if "forbidden" in first_auth_failure_code and forbidden == 0:
            forbidden = 1
        if "unauthorized" in first_auth_failure_code and unauthorized == 0:
            unauthorized = 1
    return {
        "considered_jobs": considered,
        "unauthorized_401": unauthorized,
        "forbidden_403": forbidden,
        "checkpoint": checkpoint,
        "auth_failures_total": unauthorized + forbidden + checkpoint,
    }


def _posts_fetched_from_progress(progress: dict[str, Any]) -> int:
    post_progress = _core._metadata_dict(progress.get("post_progress"))
    persist_counters = _core._metadata_dict(progress.get("persist_counters"))
    return max(
        _core._normalize_non_negative_int(post_progress.get("completed_posts")),
        _core._normalize_non_negative_int(persist_counters.get("posts_upserted")),
    )


def _run_bandwidth_bytes(progress: dict[str, Any]) -> int:
    proxy_pacing = _core._metadata_dict(progress.get("proxy_pacing"))
    return _core._normalize_non_negative_int(proxy_pacing.get("bytes_total"))


def _build_run_entry(target: dict[str, Any], *, recent_log_limit: int) -> dict[str, Any]:
    """Aggregate one (run, platform, account) tuple into a dashboard run entry."""
    platform = target["platform"]
    account_handle = target["account_handle"]
    run_id = target["run_id"]
    entry: dict[str, Any] = {
        "run_id": run_id,
        "platform": platform,
        "account_handle": account_handle,
        "source_scope": target.get("source_scope"),
        "created_at": target.get("created_at"),
        "started_at": target.get("started_at"),
        "completed_at": target.get("completed_at"),
        "is_active": str(target.get("run_status") or "").strip().lower() in _ACTIVE_RUN_STATUSES,
    }
    try:
        progress = get_social_account_catalog_run_progress(
            platform,
            account_handle,
            run_id,
            recent_log_limit=recent_log_limit,
            fast=True,
        )
    except ValueError as exc:
        # run_not_found / schema-missing: keep the enumerated row but mark it.
        entry["run_status"] = str(target.get("run_status") or "").strip().lower() or None
        entry["error"] = str(exc)
        entry["posts_fetched"] = 0
        entry["auth_failures"] = _count_auth_failures({})
        entry["bandwidth_bytes"] = 0
        entry["bandwidth_gb"] = 0.0
        return entry
    except Exception as exc:  # noqa: BLE001 - one bad run must not break the page
        logger.warning(
            "backfill_health: progress read failed platform=%s account=%s run=%s error=%s",
            platform,
            account_handle,
            run_id,
            exc,
        )
        entry["run_status"] = str(target.get("run_status") or "").strip().lower() or None
        entry["error"] = f"{type(exc).__name__}: {exc}"
        entry["posts_fetched"] = 0
        entry["auth_failures"] = _count_auth_failures({})
        entry["bandwidth_bytes"] = 0
        entry["bandwidth_gb"] = 0.0
        return entry

    run_status = (
        str(progress.get("run_status") or progress.get("run_state") or target.get("run_status") or "")
        .strip()
        .lower()
        or None
    )
    posts_fetched = _posts_fetched_from_progress(progress)
    auth_failures = _count_auth_failures(progress)
    bandwidth_bytes = _run_bandwidth_bytes(progress)
    considered = auth_failures["considered_jobs"]

    entry.update(
        {
            "run_status": run_status,
            "run_state": str(progress.get("run_state") or "").strip().lower() or None,
            "operational_state": str(progress.get("operational_state") or "").strip().lower() or None,
            "is_active": run_status in _ACTIVE_RUN_STATUSES,
            "posts_fetched": posts_fetched,
            "expected_total_posts": _core._normalize_non_negative_int(progress.get("expected_total_posts")) or None,
            "auth_failures": auth_failures,
            "auth_failure_rate": _safe_rate(auth_failures["auth_failures_total"], considered),
            "checkpoint_rate": _safe_rate(auth_failures["checkpoint"], considered),
            "bandwidth_bytes": bandwidth_bytes,
            "bandwidth_gb": round(_bytes_to_gb(bandwidth_bytes), 6),
            "last_error_code": str(progress.get("last_error_code") or "").strip().lower() or None,
            "repairable_reason": str(progress.get("repairable_reason") or "").strip().lower() or None,
        }
    )
    return entry


def get_backfill_health(
    *,
    run_limit: int = _DEFAULT_RUN_LIMIT,
    recent_log_limit: int = 20,
    include_terminal_runs: bool = True,
) -> dict[str, Any]:
    """Build the cross-account backfill-health payload.

    See module docstring for the section breakdown. Never raises.
    """
    started = perf_counter()
    generated_at = _core._iso(_core._now_utc())

    targets = _list_recent_catalog_run_targets(limit=run_limit)
    run_entries: list[dict[str, Any]] = []
    for target in targets:
        entry = _build_run_entry(target, recent_log_limit=recent_log_limit)
        if not include_terminal_runs and not entry.get("is_active"):
            continue
        run_entries.append(entry)

    # Cross-account auth cooldowns: one lookup per distinct (platform, account) seen
    # across the enumerated runs. get_active_cooldown returns None unless the cooldown
    # deadline is in the future, so this naturally lists only currently-blocked
    # accounts.
    cooldowns: list[dict[str, Any]] = []
    seen_accounts: set[tuple[str, str]] = set()
    for target in targets:
        key = (target["platform"], target["account_handle"])
        if key in seen_accounts:
            continue
        seen_accounts.add(key)
        try:
            cooldown = get_active_cooldown(target["platform"], target["account_handle"])
        except Exception as exc:  # noqa: BLE001
            logger.warning("backfill_health: cooldown read failed for %s: %s", key, exc)
            continue
        if cooldown is not None:
            cooldowns.append(cooldown.to_metadata())

    # Worker / auth health (degrade to safe defaults on failure).
    worker_auth: dict[str, Any]
    try:
        worker_auth = get_worker_auth_capabilities()
    except Exception as exc:  # noqa: BLE001
        logger.warning("backfill_health: worker auth capabilities read failed: %s", exc)
        worker_auth = {"error": f"{type(exc).__name__}: {exc}"}

    worker_health: dict[str, Any]
    try:
        worker_health = get_worker_health()
    except Exception as exc:  # noqa: BLE001
        logger.warning("backfill_health: worker health read failed: %s", exc)
        worker_health = {"healthy": False, "error": f"{type(exc).__name__}: {exc}"}

    # Queue depth (bounded summary path — same as the health-dot/queue surfaces).
    queue_section: dict[str, Any]
    try:
        queue_status = get_queue_status(summary_only=True, include_runs_summary=True)
        queue_block = _core._metadata_dict(queue_status.get("queue"))
        by_status = _core._metadata_dict(queue_block.get("by_status"))
        queue_depth = sum(
            _core._normalize_non_negative_int(by_status.get(status))
            for status in ("queued", "pending", "running", "retrying")
        )
        queue_section = {
            "queue_enabled": bool(queue_status.get("queue_enabled")),
            "queue_depth": queue_depth,
            "by_status": by_status,
            "runs_by_status": _core._metadata_dict(queue_block.get("runs_by_status")),
            "runs_total": _core._normalize_non_negative_int(queue_block.get("runs_total")),
        }
        if queue_block.get("error"):
            queue_section["error"] = str(queue_block.get("error"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("backfill_health: queue status read failed: %s", exc)
        queue_section = {"queue_enabled": is_queue_enabled(), "queue_depth": 0, "error": f"{type(exc).__name__}: {exc}"}

    # Bandwidth roll-up across runs + derived cost.
    total_bytes = sum(int(entry.get("bandwidth_bytes") or 0) for entry in run_entries)
    total_gb = _bytes_to_gb(total_bytes)
    usd_per_gb = _env_float("DECODO_USD_PER_GB", 0.0)
    derived_usd = round(total_gb * usd_per_gb, 4) if usd_per_gb > 0 else None
    bytes_by_account: dict[str, int] = {}
    for entry in run_entries:
        key = f"{entry['platform']}:{entry['account_handle']}"
        bytes_by_account[key] = bytes_by_account.get(key, 0) + int(entry.get("bandwidth_bytes") or 0)

    active_runs = [entry for entry in run_entries if entry.get("is_active")]
    totals = {
        "runs_total": len(run_entries),
        "active_runs": len(active_runs),
        "accounts_tracked": len(seen_accounts),
        "accounts_in_cooldown": len(cooldowns),
        "checkpoint_blocked_accounts": sum(1 for c in cooldowns if c.get("blocker_kind") == "checkpoint"),
        "posts_fetched_total": sum(int(entry.get("posts_fetched") or 0) for entry in run_entries),
        "auth_failures_total": sum(
            int(_core._metadata_dict(entry.get("auth_failures")).get("auth_failures_total") or 0)
            for entry in run_entries
        ),
    }

    payload = {
        "generated_at": generated_at,
        "queue_enabled": bool(queue_section.get("queue_enabled")),
        "totals": totals,
        "runs": run_entries,
        "cooldowns": cooldowns,
        "worker_auth": worker_auth,
        "worker_health": worker_health,
        "queue": queue_section,
        "bandwidth": {
            "bytes_total": total_bytes,
            "gb_total": round(total_gb, 6),
            "usd_per_gb": usd_per_gb,
            "derived_usd": derived_usd,
            "cost_available": usd_per_gb > 0,
            "bytes_by_account": bytes_by_account,
        },
    }
    logger.info(
        "social_backfill_health_loaded",
        extra={
            "route": "social_backfill_health",
            "duration_ms": round((perf_counter() - started) * 1000),
            "runs_total": totals["runs_total"],
            "active_runs": totals["active_runs"],
            "accounts_in_cooldown": totals["accounts_in_cooldown"],
        },
    )
    return payload


__all__ = ["get_backfill_health"]
