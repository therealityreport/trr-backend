"""Tests for social account profile dashboard composition."""

from __future__ import annotations

from typing import Any

import pytest

from trr_backend.socials import profile_dashboard


def _summary(catalog_recent_runs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "platform": "instagram",
        "account_handle": "thetraitorsus",
        "summary_detail": "lite",
        "catalog_recent_runs": catalog_recent_runs or [],
        "operational_alerts": [{"code": "needs_review"}],
    }


def test_active_catalog_run_fetches_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    monkeypatch.setattr(
        profile_dashboard.analytics_repo,
        "get_social_account_profile_summary",
        lambda **kwargs: _summary([{"run_id": "run-active", "status": "running"}]),
    )

    def fake_progress(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append((args, kwargs))
        return {"run_id": args[2], "status": "running"}

    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_catalog_run_progress", fake_progress)

    payload = profile_dashboard.build_social_account_profile_dashboard(
        platform="instagram",
        account_handle="thetraitorsus",
        detail="lite",
        run_id=None,
        recent_log_limit=12,
    )

    assert payload["data"]["catalog_run_progress"] == {"run_id": "run-active", "status": "running"}
    assert payload["operational_alerts"] == [{"code": "needs_review"}]
    assert calls == [(("instagram", "thetraitorsus", "run-active"), {"recent_log_limit": 12})]


def test_terminal_catalog_run_does_not_fetch_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        profile_dashboard.analytics_repo,
        "get_social_account_profile_summary",
        lambda **kwargs: _summary([{"run_id": "run-done", "status": "completed"}]),
    )

    def fail_progress(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("progress should not be fetched for terminal runs")

    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_catalog_run_progress", fail_progress)

    payload = profile_dashboard.build_social_account_profile_dashboard(
        platform="instagram",
        account_handle="thetraitorsus",
        detail="lite",
        run_id=None,
        recent_log_limit=12,
    )

    assert payload["data"]["catalog_run_progress"] is None


def test_explicit_run_id_overrides_inferred_active_run(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    monkeypatch.setattr(
        profile_dashboard.analytics_repo,
        "get_social_account_profile_summary",
        lambda **kwargs: _summary([{"run_id": "run-active", "status": "running"}]),
    )

    def fake_progress(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append((args, kwargs))
        return {"run_id": args[2]}

    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_catalog_run_progress", fake_progress)

    payload = profile_dashboard.build_social_account_profile_dashboard(
        platform="instagram",
        account_handle="thetraitorsus",
        detail="lite",
        run_id="run-explicit",
        recent_log_limit=25,
    )

    assert payload["data"]["catalog_run_progress"] == {"run_id": "run-explicit"}
    assert calls == [(("instagram", "thetraitorsus", "run-explicit"), {"recent_log_limit": 25})]


def test_progress_call_receives_account_identity_run_id_and_log_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    summary_kwargs: dict[str, Any] = {}
    progress_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def fake_summary(**kwargs: Any) -> dict[str, Any]:
        summary_kwargs.update(kwargs)
        return _summary([{"id": "run-from-id", "run_status": "in_progress"}])

    def fake_progress(*args: Any, **kwargs: Any) -> dict[str, Any]:
        progress_calls.append((args, kwargs))
        return {"logs": []}

    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_profile_summary", fake_summary)
    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_catalog_run_progress", fake_progress)

    profile_dashboard.build_social_account_profile_dashboard(
        platform="tiktok",
        account_handle="bravotv",
        detail="full",
        run_id=None,
        recent_log_limit=100,
    )

    assert summary_kwargs == {
        "platform": "tiktok",
        "account_handle": "bravotv",
        "detail": "full",
        "include_post_embeddings": False,
    }
    assert progress_calls == [(("tiktok", "bravotv", "run-from-id"), {"recent_log_limit": 100})]


def test_summary_exception_is_not_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_summary(**kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("summary failed")

    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_profile_summary", fail_summary)

    with pytest.raises(RuntimeError, match="summary failed"):
        profile_dashboard.build_social_account_profile_dashboard(
            platform="instagram",
            account_handle="thetraitorsus",
            detail="lite",
            run_id=None,
            recent_log_limit=12,
        )


def test_progress_exception_is_not_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        profile_dashboard.analytics_repo,
        "get_social_account_profile_summary",
        lambda **kwargs: _summary([{"run_id": "run-active", "status": "running"}]),
    )

    def fail_progress(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("progress failed")

    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_catalog_run_progress", fail_progress)

    with pytest.raises(RuntimeError, match="progress failed"):
        profile_dashboard.build_social_account_profile_dashboard(
            platform="instagram",
            account_handle="thetraitorsus",
            detail="lite",
            run_id=None,
            recent_log_limit=12,
        )
