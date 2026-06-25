"""B2: umbrella timeout around catalog launch finalization.

A wedged launch probe must not pin the run in ``finalizing`` forever. The timeout helper
raises a recoverable ``CatalogLaunchTimeout`` (callers leave ``launch_state="finalizing"``
so the stale-finalizing recovery sweep re-drives it) instead of blocking indefinitely.
"""

from __future__ import annotations

import threading

import pytest

from trr_backend.socials.pipelines.account_catalog import launch as catalog_launch


def test_run_catalog_launch_with_timeout_passes_through_fast_calls() -> None:
    assert (
        catalog_launch._run_catalog_launch_with_timeout(lambda: "ok", timeout_seconds=5)  # noqa: SLF001
        == "ok"
    )


def test_run_catalog_launch_with_timeout_runs_inline_when_disabled() -> None:
    # timeout_seconds<=0 disables the umbrella entirely (runs inline, no executor).
    assert (
        catalog_launch._run_catalog_launch_with_timeout(lambda: "inline", timeout_seconds=0)  # noqa: SLF001
        == "inline"
    )


def test_run_catalog_launch_with_timeout_raises_recoverable_timeout() -> None:
    release = threading.Event()

    def _block() -> str:
        # Wait on an event we only set in teardown so the abandoned worker thread exits
        # promptly instead of lingering until interpreter shutdown.
        release.wait(timeout=5)
        return "never"

    try:
        with pytest.raises(catalog_launch.CatalogLaunchTimeout) as excinfo:
            catalog_launch._run_catalog_launch_with_timeout(_block, timeout_seconds=0.1)  # noqa: SLF001
        assert excinfo.value.timeout_seconds == 0.1
    finally:
        release.set()


def test_catalog_finalize_launch_timeout_seconds_default_and_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TRR_CATALOG_FINALIZE_LAUNCH_TIMEOUT_S", raising=False)
    assert catalog_launch._catalog_finalize_launch_timeout_seconds() == 100.0  # noqa: SLF001

    monkeypatch.setenv("TRR_CATALOG_FINALIZE_LAUNCH_TIMEOUT_S", "30")
    assert catalog_launch._catalog_finalize_launch_timeout_seconds() == 30.0  # noqa: SLF001

    # 0 (or negative) disables the timeout.
    monkeypatch.setenv("TRR_CATALOG_FINALIZE_LAUNCH_TIMEOUT_S", "0")
    assert catalog_launch._catalog_finalize_launch_timeout_seconds() == 0.0  # noqa: SLF001

    # Invalid values fall back to the default.
    monkeypatch.setenv("TRR_CATALOG_FINALIZE_LAUNCH_TIMEOUT_S", "not-a-number")
    assert catalog_launch._catalog_finalize_launch_timeout_seconds() == 100.0  # noqa: SLF001
