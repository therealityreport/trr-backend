"""Focused run-lifecycle repository tests for extracted control-plane seams."""

from __future__ import annotations

import pytest

import trr_backend.repositories.social_season_analytics as social_repo
import trr_backend.socials.control_plane.run_lifecycle as run_lifecycle


def test_legacy_set_run_status_delegates_to_control_plane_run_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(run_lifecycle, "_set_run_status", lambda run_id, status: calls.append((run_id, status)))

    social_repo._set_run_status("run-1", "running")

    assert calls == [("run-1", "running")]


def test_legacy_create_run_delegates_to_control_plane_run_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = "run-1"
    captured: dict[str, object] = {}

    def _fake_create_run(context, **kwargs):
        captured["context"] = context
        captured["kwargs"] = kwargs
        return expected

    monkeypatch.setattr(run_lifecycle, "_create_run", _fake_create_run)

    payload = social_repo._create_run(
        None,
        source_scope="bravo",
        initiated_by="admin@test",
        config={"sync_session_id": "sync-1"},
        status="queued",
    )

    assert payload == expected
    assert captured["context"] is None
    assert captured["kwargs"] == {
        "source_scope": "bravo",
        "initiated_by": "admin@test",
        "config": {"sync_session_id": "sync-1"},
        "status": "queued",
    }


def test_legacy_update_run_summary_delegates_to_control_plane_run_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"total_jobs": 6}

    monkeypatch.setattr(
        run_lifecycle,
        "_update_run_summary",
        lambda run_id, force_recompute=False: {"run_id": run_id, "force_recompute": force_recompute, **expected},
    )

    summary = social_repo._update_run_summary("run-1", force_recompute=True)

    assert summary == {"run_id": "run-1", "force_recompute": True, **expected}


def test_legacy_finalize_run_status_delegates_to_control_plane_run_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"status": "completed"}

    monkeypatch.setattr(
        run_lifecycle,
        "_finalize_run_status",
        lambda run_id, force_recompute=False: {
            "run_id": run_id,
            "force_recompute": force_recompute,
            **expected,
        },
    )

    summary = social_repo._finalize_run_status("run-1", force_recompute=True)

    assert summary == {"run_id": "run-1", "force_recompute": True, **expected}


def test_legacy_reconcile_run_summaries_delegates_to_control_plane_run_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"reconciled_runs": 1, "run_ids": ["run-1"]}

    monkeypatch.setattr(
        run_lifecycle,
        "reconcile_run_summaries",
        lambda **kwargs: {"kwargs": kwargs, **expected},
    )

    payload = social_repo.reconcile_run_summaries(run_ids=["run-1"], limit=25)

    assert payload == {"kwargs": {"run_ids": ["run-1"], "limit": 25}, **expected}


def test_create_run_writes_sync_session_metadata_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def _fake_fetch_one(sql: str, params: list[object]):
        calls.append((" ".join(sql.lower().split()), list(params)))
        if "insert into social.scrape_runs" in sql.lower():
            return {"id": "run-1"}
        if "update social.scrape_runs" in sql.lower():
            return {"id": "run-1"}
        return {}

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)
    monkeypatch.setattr(social_repo, "_column_exists", lambda *_args, **_kwargs: True)

    payload = social_repo._create_run(
        None,
        source_scope="bravo",
        initiated_by="admin@test",
        config={
            "sync_session_id": "11111111-1111-1111-1111-111111111111",
            "pass_kind": "comments_only",
            "pass_attempt": 2,
            "pass_sequence": 3,
        },
        status="queued",
    )

    assert payload == "run-1"
    assert len(calls) == 2
    assert "insert into social.scrape_runs" in calls[0][0]
    assert "update social.scrape_runs" in calls[1][0]
    assert calls[1][1] == [
        "11111111-1111-1111-1111-111111111111",
        "comments_only",
        2,
        3,
        "run-1",
    ]


def test_build_run_summary_payload_normalizes_stage_counts() -> None:
    payload = social_repo._build_run_summary_payload(
        total_jobs="4",
        completed_jobs=1,
        failed_jobs=None,
        active_jobs="2",
        items_found_total="7",
        stage_counts={"posts": {"total": "2", "completed": 1, "failed": 0, "active": "1"}},
    )

    assert payload == {
        "total_jobs": 4,
        "completed_jobs": 1,
        "failed_jobs": 0,
        "active_jobs": 2,
        "items_found_total": 7,
        "stage_counts": {"posts": {"total": 2, "completed": 1, "failed": 0, "active": 1}},
    }


def test_set_run_status_invalidates_week_detail_cache_on_terminal_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invalidation_calls: list[str] = []
    monkeypatch.setattr(social_repo.pg, "fetch_one", lambda *_args, **_kwargs: {"id": "run-1"})
    social_repo.register_week_detail_cache_invalidator(lambda: invalidation_calls.append("called"))
    try:
        social_repo._set_run_status("run-1", "running")
        assert invalidation_calls == []
        social_repo._set_run_status("run-1", "completed")
        assert invalidation_calls == ["called"]
    finally:
        social_repo.register_week_detail_cache_invalidator(None)


def test_update_run_summary_prefers_incremental_counter_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(social_repo, "_run_counter_columns_ready", lambda: True)

    def _fake_fetch_one(sql: str, params: list[object]):  # noqa: ARG001
        normalized = " ".join(sql.lower().split())
        calls.append(normalized)
        if "from social.scrape_runs where id = %s" in normalized and "select total_jobs" in normalized:
            return {
                "total_jobs": 6,
                "completed_jobs": 3,
                "failed_jobs": 1,
                "active_jobs": 2,
                "items_found_total": 77,
                "stage_counts": {"posts": {"total": 3, "completed": 2, "failed": 0, "active": 1}},
            }
        if "update social.scrape_runs set summary = %s::jsonb" in normalized:
            return {"id": "run-1"}
        if "from social.scrape_jobs" in normalized:
            raise AssertionError("full scrape_jobs aggregation should not run in incremental mode")
        return {}

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    summary = social_repo._update_run_summary("run-1")

    assert summary["total_jobs"] == 6
    assert summary["completed_jobs"] == 3
    assert summary["failed_jobs"] == 1
    assert summary["active_jobs"] == 2
    assert summary["items_found_total"] == 77
    assert summary["stage_counts"]["posts"]["active"] == 1
    assert any("select total_jobs" in call for call in calls)
