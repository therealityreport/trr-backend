"""Pure run-budget math for the public-comments proxy kill-switch.

DB-free: covers config resolution, USD estimation, and the run/unpriced
exhaustion decision. Enforcement is run-scoped (no daily/cross-run cap). Ledger
I/O (record_ledger_row) is exercised against a live DB elsewhere; here we keep
the guardrail logic fast and deterministic.
"""

from __future__ import annotations

import os
from unittest.mock import patch

from trr_backend.socials.instagram.comments_scrapling import proxy_budget as pb


def _clear_env() -> dict[str, str]:
    keep = dict(os.environ)
    for name in (pb.DOLLARS_PER_GB_ENV, pb.RUN_BUDGET_USD_ENV):
        keep.pop(name, None)
    return keep


def test_config_defaults_are_unpriced_with_run_budget_3():
    with patch.dict(os.environ, _clear_env(), clear=True):
        cfg = pb.resolve_proxy_budget_config()
    assert cfg.usd_per_gb == 0.0
    assert cfg.run_budget_usd == 3.0
    # No price configured => enforcement disabled.
    assert cfg.priced is False


def test_config_reads_overrides():
    base = _clear_env()
    env = {**base, pb.DOLLARS_PER_GB_ENV: "2.5", pb.RUN_BUDGET_USD_ENV: "5"}
    with patch.dict(os.environ, env, clear=True):
        cfg = pb.resolve_proxy_budget_config()
    assert cfg.usd_per_gb == 2.5
    assert cfg.run_budget_usd == 5.0
    assert cfg.priced is True


def test_estimate_usd_math():
    # 1 GiB at $3/GB == $3; zero/negative inputs are free.
    assert pb.estimate_usd(pb._BYTES_PER_GB, 3.0) == 3.0
    assert pb.estimate_usd(pb._BYTES_PER_GB // 2, 3.0) == 1.5
    assert pb.estimate_usd(0, 3.0) == 0.0
    assert pb.estimate_usd(pb._BYTES_PER_GB, 0.0) == 0.0


def test_unpriced_config_never_exhausts():
    cfg = pb.ProxyBudgetConfig(usd_per_gb=0.0, run_budget_usd=3.0)
    decision = pb.evaluate_budget(run_proxy_bytes=100 * pb._BYTES_PER_GB, config=cfg)
    assert decision.exhausted is False
    assert decision.reason == "unpriced"


def test_run_budget_exhaustion():
    cfg = pb.ProxyBudgetConfig(usd_per_gb=1.0, run_budget_usd=3.0)
    # 3 GiB at $1/GB == $3 == run budget => exhausted.
    decision = pb.evaluate_budget(run_proxy_bytes=3 * pb._BYTES_PER_GB, config=cfg)
    assert decision.exhausted is True
    assert decision.reason == "run_budget_exhausted"
    assert decision.run_estimated_usd == 3.0


def test_under_budget_is_ok():
    cfg = pb.ProxyBudgetConfig(usd_per_gb=1.0, run_budget_usd=3.0)
    decision = pb.evaluate_budget(run_proxy_bytes=1 * pb._BYTES_PER_GB, config=cfg)
    assert decision.exhausted is False
    assert decision.reason == "ok"
    assert decision.run_estimated_usd == 1.0


def test_zero_run_budget_disables_run_cap():
    # run_budget_usd == 0 means "no run cap" even when priced.
    cfg = pb.ProxyBudgetConfig(usd_per_gb=1.0, run_budget_usd=0.0)
    decision = pb.evaluate_budget(run_proxy_bytes=100 * pb._BYTES_PER_GB, config=cfg)
    assert decision.exhausted is False
    assert decision.reason == "ok"
