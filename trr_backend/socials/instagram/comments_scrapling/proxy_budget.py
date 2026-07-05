"""Per-run bandwidth budget + ledger for budgeted public-comments proxy fan-out.

Phase 3 of the throughput plan. The default public comments lane runs proxy-free;
when ``SOCIAL_INSTAGRAM_COMMENTS_PUBLIC_PROXY_ENABLED=1`` it may use a sticky
Decodo egress. Decodo bills by GB with no native cap, so this module provides a
RUN-SCOPED spend guardrail: it derives estimated USD from proxied response bytes
and a configured ``$/GB`` and trips a per-run kill-switch once the run budget is
reached (the run then falls back to direct egress and emits the already-wired
``proxy_budget_exhausted`` stop reason). A ledger row is written for auditability.

There is intentionally NO daily/cross-run cap: enforcement is bounded to a single
run so it needs no cross-job accumulation. The pure budget math (resolve /
estimate / evaluate) is DB-free and unit-testable; the ledger write reuses the
same ``pg`` helper + ``social_control`` pool as the rate pacer and fails soft
(never raises into the scrape path).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from trr_backend.socials._scrapling_http_utils import resolve_positive_float_env

logger = logging.getLogger("socials.instagram.comments_scrapling.proxy_budget")

# 1 GiB. estimated_usd = proxy_bytes * usd_per_gb / 2**30.
_BYTES_PER_GB = 1_073_741_824

DOLLARS_PER_GB_ENV = "SOCIAL_INSTAGRAM_COMMENTS_PROXY_DOLLARS_PER_GB"
RUN_BUDGET_USD_ENV = "SOCIAL_INSTAGRAM_COMMENTS_PROXY_RUN_BUDGET_USD"

_DEFAULT_RUN_BUDGET_USD = 3.0

LEDGER_TABLE = "social.instagram_comments_public_proxy_budget_ledger"
_POOL_NAME = "social_control"

# Stop reason emitted when a run trips its budget; already recognized as a
# retryable stop reason in job_runner / control_plane.
BUDGET_EXHAUSTED_REASON = "proxy_budget_exhausted"


@dataclass(frozen=True, slots=True)
class ProxyBudgetConfig:
    usd_per_gb: float
    run_budget_usd: float

    @property
    def priced(self) -> bool:
        """Enforcement only applies when a price is configured; with no ``$/GB``
        we cannot derive cost, so the kill-switch stays disabled (never blocks)."""
        return self.usd_per_gb > 0


def resolve_proxy_budget_config() -> ProxyBudgetConfig:
    return ProxyBudgetConfig(
        usd_per_gb=resolve_positive_float_env(DOLLARS_PER_GB_ENV, 0.0, minimum=0.0, maximum=1000.0),
        run_budget_usd=resolve_positive_float_env(
            RUN_BUDGET_USD_ENV, _DEFAULT_RUN_BUDGET_USD, minimum=0.0, maximum=100_000.0
        ),
    )


def estimate_usd(proxy_bytes: int, usd_per_gb: float) -> float:
    if proxy_bytes <= 0 or usd_per_gb <= 0:
        return 0.0
    return (float(proxy_bytes) / _BYTES_PER_GB) * float(usd_per_gb)


@dataclass(frozen=True, slots=True)
class BudgetDecision:
    exhausted: bool
    reason: str  # "ok" | "run_budget_exhausted" | "unpriced"
    run_estimated_usd: float


def evaluate_budget(*, run_proxy_bytes: int, config: ProxyBudgetConfig) -> BudgetDecision:
    """Decide whether this run's proxied bytes have reached the run budget."""
    if not config.priced:
        return BudgetDecision(False, "unpriced", 0.0)
    run_usd = estimate_usd(run_proxy_bytes, config.usd_per_gb)
    if config.run_budget_usd > 0 and run_usd >= config.run_budget_usd:
        return BudgetDecision(True, "run_budget_exhausted", run_usd)
    return BudgetDecision(False, "ok", run_usd)


def record_ledger_row(
    *,
    account_handle: str,
    run_id: str | None = None,
    job_id: str | None = None,
    proxy_provider: str | None = None,
    proxy_fingerprint: str | None = None,
    proxy_session_mode: str | None = None,
    http_client: str | None = None,
    rate_scope: str | None = None,
    request_count: int = 0,
    proxy_bytes_total: int = 0,
    proxy_cdn_bytes_leak: int = 0,
    proxy_bytes_by_host: dict[str, int] | None = None,
    proxy_cdn_bytes_leak_by_host: dict[str, int] | None = None,
    config: ProxyBudgetConfig | None = None,
    budget_exhausted: bool = False,
    metadata: dict | None = None,
    pool_name: str = _POOL_NAME,
) -> bool:
    """Append an audit ledger row for a proxied public-comments run (fails soft)."""
    config = config or resolve_proxy_budget_config()
    estimated = estimate_usd(proxy_bytes_total, config.usd_per_gb)
    try:
        from trr_backend.db import pg
    except Exception as exc:  # noqa: BLE001
        logger.warning("[proxy-budget] pg import failed for ledger write: %s", exc)
        return False
    try:
        with pg.db_connection(label="instagram-comments-proxy-budget-write", pool_name=pool_name) as conn:
            with pg.db_cursor(conn=conn) as cur:
                cur.execute(
                    f"""
                    insert into {LEDGER_TABLE} (
                      run_id, job_id, account_handle, proxy_provider, proxy_fingerprint,
                      proxy_session_mode, http_client, rate_scope, request_count,
                      proxy_bytes_total, proxy_cdn_bytes_leak, proxy_bytes_by_host,
                      proxy_cdn_bytes_leak_by_host, usd_per_gb, estimated_usd, budget_usd,
                      budget_exhausted, metadata
                    ) values (
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb,
                      %s, %s, %s, %s, %s::jsonb
                    )
                    """,
                    (
                        run_id,
                        job_id,
                        str(account_handle or ""),
                        proxy_provider,
                        proxy_fingerprint,
                        proxy_session_mode,
                        http_client,
                        rate_scope,
                        int(request_count or 0),
                        int(proxy_bytes_total or 0),
                        int(proxy_cdn_bytes_leak or 0),
                        json.dumps(proxy_bytes_by_host or {}),
                        json.dumps(proxy_cdn_bytes_leak_by_host or {}),
                        config.usd_per_gb or None,
                        estimated or None,
                        config.run_budget_usd or None,
                        bool(budget_exhausted),
                        json.dumps(metadata or {}),
                    ),
                )
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("[proxy-budget] ledger write failed: %s", exc)
        return False
