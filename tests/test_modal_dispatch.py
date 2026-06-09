"""Tests for Modal dispatch helpers."""

from __future__ import annotations

import sys
import types

import pytest

from trr_backend import modal_dispatch
from trr_backend.db import pg


class _FakeStatus:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeInputInfo:
    def __init__(
        self,
        *,
        function_call_id: str,
        status: str,
        task_id: str = "",
        children: list[_FakeInputInfo] | None = None,
    ) -> None:
        self.function_call_id = function_call_id
        self.status = _FakeStatus(status)
        self.task_id = task_id
        self.children = children or []


class _FakeFunctionCall:
    def __init__(self, graph: list[_FakeInputInfo]) -> None:
        self._graph = graph

    def get_call_graph(self) -> list[_FakeInputInfo]:
        return self._graph


def test_inspect_modal_function_call_normalizes_pending_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_modal = types.SimpleNamespace(
        FunctionCall=types.SimpleNamespace(
            from_id=lambda _call_id: _FakeFunctionCall(
                [
                    _FakeInputInfo(
                        function_call_id="fc-parent",
                        status="SUCCESS",
                        children=[_FakeInputInfo(function_call_id="fc-pending", status="PENDING")],
                    )
                ]
            )
        )
    )

    monkeypatch.setattr(modal_dispatch, "modal_dispatch_ready", lambda *, function_name: (True, None))
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    payload = modal_dispatch.inspect_modal_function_call("fc-pending")

    assert payload["status"] == "pending"
    assert payload["task_id"] is None
    assert payload["reason"] == "modal_capacity_pending"
    assert payload["nonterminal"] is True
    assert payload["terminal"] is False


def test_inspect_modal_function_call_normalizes_running_and_terminal_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graphs = {
        "fc-running": [
            _FakeInputInfo(
                function_call_id="fc-running",
                status="PENDING",
                task_id="ta-123",
            )
        ],
        "fc-success": [_FakeInputInfo(function_call_id="fc-success", status="SUCCESS")],
        "fc-failed": [_FakeInputInfo(function_call_id="fc-failed", status="FAILURE")],
    }
    fake_modal = types.SimpleNamespace(
        FunctionCall=types.SimpleNamespace(
            from_id=lambda call_id: _FakeFunctionCall(graphs[call_id]),
        )
    )

    monkeypatch.setattr(modal_dispatch, "modal_dispatch_ready", lambda *, function_name: (True, None))
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    running = modal_dispatch.inspect_modal_function_call("fc-running")
    success = modal_dispatch.inspect_modal_function_call("fc-success")
    failed = modal_dispatch.inspect_modal_function_call("fc-failed")

    assert running["status"] == "running"
    assert running["task_id"] == "ta-123"
    assert success["status"] == "completed"
    assert success["terminal"] is True
    assert failed["status"] == "failed"
    assert failed["reason"] == "modal_failure"


def test_inspect_modal_function_call_returns_unknown_on_inspection_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_modal = types.SimpleNamespace(
        FunctionCall=types.SimpleNamespace(
            from_id=lambda _call_id: (_ for _ in ()).throw(RuntimeError("boom")),
        )
    )

    monkeypatch.setattr(modal_dispatch, "modal_dispatch_ready", lambda *, function_name: (True, None))
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    payload = modal_dispatch.inspect_modal_function_call("fc-broken")

    assert payload["status"] == "unknown"
    assert payload["reason"] == "modal_call_inspection_failed"
    assert payload["error"] == "boom"


def test_modal_social_job_function_name_for_stage_routes_three_backfill_lanes() -> None:
    assert modal_dispatch.modal_social_job_function_name_for_stage("shared_account_posts") == "run_social_posts_job"
    assert modal_dispatch.modal_social_job_function_name_for_stage("threads_posts_scrapling") == "run_social_posts_job"
    assert modal_dispatch.modal_social_job_function_name_for_stage("media_mirror") == "run_social_media_job"
    assert modal_dispatch.modal_social_job_function_name_for_stage("comments_scrapling") == "run_social_comments_job"


def test_modal_social_job_function_names_dedupes_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(modal_dispatch, "modal_social_job_function_name", lambda: "run_social_job")
    monkeypatch.setattr(modal_dispatch, "modal_social_posts_job_function_name", lambda: "run_social_job")
    monkeypatch.setattr(modal_dispatch, "modal_social_media_job_function_name", lambda: "run_social_media_job")
    monkeypatch.setattr(modal_dispatch, "modal_social_comments_job_function_name", lambda: "run_social_comments_job")

    assert modal_dispatch.modal_social_job_function_names() == [
        "run_social_job",
        "run_social_media_job",
        "run_social_comments_job",
    ]


def test_modal_dispatch_config_exposes_comments_lane_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(modal_dispatch, "modal_app_name", lambda: "trr-backend-jobs")
    monkeypatch.setattr(modal_dispatch, "modal_environment_name", lambda: "main")
    monkeypatch.setattr(modal_dispatch, "modal_social_job_function_name", lambda: "run_social_job")
    monkeypatch.setattr(modal_dispatch, "modal_social_posts_job_function_name", lambda: "run_social_posts_job")
    monkeypatch.setattr(modal_dispatch, "modal_social_media_job_function_name", lambda: "run_social_media_job")
    monkeypatch.setattr(modal_dispatch, "modal_social_comments_job_function_name", lambda: "run_social_comments_job")
    monkeypatch.setattr(modal_dispatch, "modal_cast_screentime_function_name", lambda: "run_cast_screentime_analysis")

    config = modal_dispatch.modal_dispatch_config()

    assert config["app_name"] == "trr-backend-jobs"
    assert config["modal_environment"] == "main"
    assert config["social_required_function_names"] == [
        "run_social_job",
        "run_social_posts_job",
        "run_social_media_job",
        "run_social_comments_job",
    ]
    assert config["cast_screentime_function"] == "run_cast_screentime_analysis"
    assert config["social_job_function_names"] == [
        "run_social_job",
        "run_social_posts_job",
        "run_social_media_job",
        "run_social_comments_job",
    ]


def test_dispatch_social_job_uses_stage_specific_function(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_spawn_named_modal_function(**kwargs):
        captured.update(kwargs)
        return {"dispatched": True, "call_id": "fc-123"}

    monkeypatch.setattr(modal_dispatch, "_spawn_named_modal_function", _fake_spawn_named_modal_function)

    modal_dispatch.dispatch_social_job(job_id="job-1", stage="comments_scrapling")

    assert captured["function_name"] == "run_social_comments_job"
    assert captured["kwargs"] == {"job_id": "job-1"}


def test_dispatch_cast_screentime_run_uses_cast_function(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_spawn_named_modal_function(**kwargs):
        captured.update(kwargs)
        return {"dispatched": True, "call_id": "fc-123"}

    monkeypatch.setattr(modal_dispatch, "_spawn_named_modal_function", _fake_spawn_named_modal_function)

    result = modal_dispatch.dispatch_cast_screentime_run(run_id="run-123")

    assert result == {"dispatched": True, "call_id": "fc-123"}
    assert captured["function_name"] == "run_cast_screentime_analysis"
    assert captured["log_label"] == "cast screentime"
    assert captured["dispatcher_name"] == "cast-screentime"
    assert captured["kwargs"] == {"run_id": "run-123"}


def test_dispatch_socialblade_scrape_passes_platform_and_following_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_spawn_named_modal_function(**kwargs):
        captured.update(kwargs)
        return {"dispatched": True, "call_id": "fc-123"}

    monkeypatch.setattr(modal_dispatch, "_spawn_named_modal_function", _fake_spawn_named_modal_function)

    modal_dispatch.dispatch_socialblade_scrape(
        person_id="person-1",
        handle="networkofficial",
        source="season_run",
        force=True,
        platform="instagram",
        scrape_following=True,
        source_scope="creator",
    )

    assert captured["function_name"] == "run_socialblade_scrape"
    assert captured["kwargs"] == {
        "person_id": "person-1",
        "handle": "networkofficial",
        "source": "season_run",
        "force": True,
        "platform": "instagram",
        "scrape_following": True,
        "source_scope": "creator",
    }


def test_spawn_named_modal_function_includes_drift_visible_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeCall:
        object_id = "fc-123"

    class _FakeFunction:
        def spawn(self, **_kwargs: object) -> _FakeCall:
            return _FakeCall()

    fake_modal = types.SimpleNamespace(
        Function=types.SimpleNamespace(
            from_name=lambda _app_name, _function_name: _FakeFunction(),
        )
    )

    monkeypatch.setattr(modal_dispatch, "modal_dispatch_ready", lambda *, function_name: (True, None))
    monkeypatch.setattr(modal_dispatch, "modal_app_name", lambda: "trr-backend-jobs")
    monkeypatch.setattr(modal_dispatch, "modal_environment_name", lambda: "main")
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    payload = modal_dispatch._spawn_named_modal_function(
        function_name="run_social_comments_job",
        log_label="social ingest",
        kwargs={"job_id": "job-1"},
        dispatcher_name="social",
    )

    assert payload["dispatched"] is True
    assert payload["app_name"] == "trr-backend-jobs"
    assert payload["function_name"] == "run_social_comments_job"
    assert payload["modal_environment"] == "main"
    assert payload["execution_metadata"]["execution_backend_canonical"] == "modal"
    assert payload["dispatch_config"]["social_comments_job_function"] == "run_social_comments_job"


def test_spawn_named_modal_function_includes_metadata_when_preflight_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(modal_dispatch, "modal_dispatch_ready", lambda *, function_name: (False, "modal_disabled"))
    monkeypatch.setattr(modal_dispatch, "modal_app_name", lambda: "trr-backend-jobs")
    monkeypatch.setattr(modal_dispatch, "modal_environment_name", lambda: "main")

    payload = modal_dispatch._spawn_named_modal_function(
        function_name="run_social_comments_job",
        log_label="social ingest",
        kwargs={"job_id": "job-1"},
        dispatcher_name="social",
    )

    assert payload["dispatched"] is False
    assert payload["reason"] == "modal_disabled"
    assert payload["app_name"] == "trr-backend-jobs"
    assert payload["function_name"] == "run_social_comments_job"
    assert payload["modal_environment"] == "main"
    assert payload["dispatch_config"]["app_name"] == "trr-backend-jobs"


def test_inspect_modal_function_call_only_requires_modal_app_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_modal = types.SimpleNamespace(
        FunctionCall=types.SimpleNamespace(
            from_id=lambda _call_id: _FakeFunctionCall([_FakeInputInfo(function_call_id="fc-ok", status="SUCCESS")]),
        )
    )

    monkeypatch.setattr(modal_dispatch, "modal_app_name", lambda: "trr-backend-jobs")
    monkeypatch.setattr(
        modal_dispatch,
        "modal_social_job_function_name",
        lambda: "",
    )
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    payload = modal_dispatch.inspect_modal_function_call("fc-ok")

    assert payload["status"] == "completed"
    assert payload["reason"] is None


def test_resolve_modal_function_classifies_missing_app(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_modal = types.SimpleNamespace(
        Function=types.SimpleNamespace(
            from_name=lambda _app_name, _function_name: (_ for _ in ()).throw(
                RuntimeError("App 'trr-backend-jobs' not found in environment 'main'")
            )
        )
    )

    monkeypatch.setattr(modal_dispatch, "modal_dispatch_ready", lambda *, function_name: (True, None))
    monkeypatch.setattr(modal_dispatch, "modal_app_name", lambda: "trr-backend-jobs")
    monkeypatch.setattr(modal_dispatch, "modal_environment_name", lambda: "main")
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    payload = modal_dispatch.resolve_modal_function("run_social_job")

    assert payload["resolved"] is False
    assert payload["reason"] == "modal_app_not_found"
    assert payload["function_name"] == "run_social_job"
    assert payload["modal_environment"] == "main"


def test_resolve_modal_function_classifies_missing_function(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_modal = types.SimpleNamespace(
        Function=types.SimpleNamespace(
            from_name=lambda _app_name, _function_name: (_ for _ in ()).throw(
                RuntimeError("Function 'run_social_job' not found in app 'trr-backend-jobs'")
            )
        )
    )

    monkeypatch.setattr(modal_dispatch, "modal_dispatch_ready", lambda *, function_name: (True, None))
    monkeypatch.setattr(modal_dispatch, "modal_app_name", lambda: "trr-backend-jobs")
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    payload = modal_dispatch.resolve_modal_function("run_social_job")

    assert payload["resolved"] is False
    assert payload["reason"] == "modal_function_not_found"
    assert payload["app_name"] == "trr-backend-jobs"


def test_resolve_modal_function_skips_hydrate_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    hydrated = {"called": False}

    class _FakeHandle:
        def hydrate(self) -> None:
            hydrated["called"] = True

    fake_modal = types.SimpleNamespace(
        Function=types.SimpleNamespace(
            from_name=lambda _app_name, _function_name: _FakeHandle(),
        )
    )

    monkeypatch.setattr(modal_dispatch, "modal_dispatch_ready", lambda *, function_name: (True, None))
    monkeypatch.setattr(modal_dispatch, "modal_app_name", lambda: "trr-backend-jobs")
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    payload = modal_dispatch.resolve_modal_function("run_social_job")

    assert payload["resolved"] is True
    assert payload["reason"] is None
    assert payload["hydrated"] is False
    assert hydrated["called"] is False


def test_resolve_modal_function_hydrates_when_opted_in(monkeypatch: pytest.MonkeyPatch) -> None:
    hydrated = {"called": False}

    class _FakeHandle:
        def hydrate(self) -> None:
            hydrated["called"] = True

    fake_modal = types.SimpleNamespace(
        Function=types.SimpleNamespace(
            from_name=lambda _app_name, _function_name: _FakeHandle(),
        )
    )

    monkeypatch.setenv("TRR_MODAL_RESOLVE_HYDRATE", "1")
    monkeypatch.setattr(modal_dispatch, "modal_dispatch_ready", lambda *, function_name: (True, None))
    monkeypatch.setattr(modal_dispatch, "modal_app_name", lambda: "trr-backend-jobs")
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    payload = modal_dispatch.resolve_modal_function("run_social_job")

    assert payload["resolved"] is True
    assert payload["reason"] is None
    assert payload["hydrated"] is True
    assert hydrated["called"] is True


def test_supports_admin_operation_includes_bravotv_image_runs() -> None:
    assert modal_dispatch.supports_admin_operation("admin_bravotv_image_run") is True


def test_record_dispatcher_heartbeat_preserves_existing_auth_capabilities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        pg,
        "fetch_one",
        lambda _sql, _params: {
            "metadata": {
                "auth_capabilities": {"instagram_authenticated": True},
                "heartbeat_source": "modal_cron",
            }
        },
    )
    monkeypatch.setattr(
        modal_dispatch,
        "_dispatcher_worker_id",
        lambda dispatcher_name: f"modal:{dispatcher_name}-dispatcher",
    )

    def _fake_update_worker_heartbeat(
        worker_id: str,
        *,
        stage: str,
        status: str,
        metadata: dict[str, object],
        supported_platforms: list[str] | None = None,
    ) -> None:
        captured["worker_id"] = worker_id
        captured["stage"] = stage
        captured["status"] = status
        captured["metadata"] = metadata
        captured["supported_platforms"] = supported_platforms

    monkeypatch.setattr(
        "trr_backend.socials.control_plane._resolve_runtime_version_stamp",
        lambda: {"label": "modal"},
    )
    monkeypatch.setattr(
        "trr_backend.socials.control_plane.update_worker_heartbeat",
        _fake_update_worker_heartbeat,
    )

    modal_dispatch._record_dispatcher_heartbeat(
        dispatcher_name="social",
        status="idle",
        metadata_updates={"last_dispatch_success_at": "2026-04-21T08:54:12Z"},
        supported_platforms=["instagram"],
    )

    assert captured["worker_id"] == "modal:social-dispatcher"
    assert captured["stage"] == "any"
    assert captured["status"] == "idle"
    assert captured["supported_platforms"] == ["instagram"]
    assert captured["metadata"] == {
        "auth_capabilities": {"instagram_authenticated": True},
        "dispatcher_name": "social",
        "execution_backend_canonical": "modal",
        "execution_mode_canonical": "remote",
        "heartbeat_source": "modal_cron",
        "last_dispatch_success_at": "2026-04-21T08:54:12Z",
        "runtime_version": {"label": "modal"},
    }
