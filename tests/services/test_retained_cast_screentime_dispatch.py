from __future__ import annotations

from trr_backend.services import retained_cast_screentime_dispatch


def test_start_run_uses_backend_runtime(monkeypatch) -> None:
    monkeypatch.setattr(
        retained_cast_screentime_dispatch.modal_dispatch,
        "dispatch_cast_screentime_run",
        lambda *, run_id: {"dispatched": False, "reason_code": "modal_disabled"},
    )
    monkeypatch.setattr(
        retained_cast_screentime_dispatch.modal_dispatch,
        "modal_dispatch_enabled",
        lambda: False,
    )
    monkeypatch.setattr(
        retained_cast_screentime_dispatch.retained_cast_screentime_runtime,
        "enqueue_run",
        lambda run_id: {"run_id": run_id, "state": "queued", "job_id": f"backend:{run_id}"},
    )

    result = retained_cast_screentime_dispatch.start_run("run-123")

    assert result == {"run_id": "run-123", "state": "queued", "job_id": "backend:run-123"}


def test_start_run_uses_modal_dispatch_when_available(monkeypatch) -> None:
    enqueue_calls: list[str] = []
    modal_result = {
        "dispatched": True,
        "call_id": "fc-123",
        "function_name": "run_cast_screentime_analysis",
    }

    monkeypatch.setattr(
        retained_cast_screentime_dispatch.modal_dispatch,
        "dispatch_cast_screentime_run",
        lambda *, run_id: modal_result,
    )
    monkeypatch.setattr(
        retained_cast_screentime_dispatch.retained_cast_screentime_runtime,
        "enqueue_run",
        lambda run_id: enqueue_calls.append(run_id),
    )

    result = retained_cast_screentime_dispatch.start_run("run-123")

    assert result == {
        "run_id": "run-123",
        "state": "queued",
        "job_id": "modal:fc-123",
        "mode": "modal",
        "dispatch": modal_result,
    }
    assert enqueue_calls == []


def test_start_run_rejects_local_fallback_when_modal_is_enabled(monkeypatch) -> None:
    enqueue_calls: list[str] = []
    monkeypatch.setattr(
        retained_cast_screentime_dispatch.modal_dispatch,
        "dispatch_cast_screentime_run",
        lambda *, run_id: {"dispatched": False, "reason_code": "modal_function_not_found"},
    )
    monkeypatch.setattr(
        retained_cast_screentime_dispatch.modal_dispatch,
        "modal_dispatch_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        retained_cast_screentime_dispatch.retained_cast_screentime_runtime,
        "enqueue_run",
        lambda run_id: enqueue_calls.append(run_id),
    )

    try:
        retained_cast_screentime_dispatch.start_run("run-123")
    except retained_cast_screentime_dispatch.RetainedCastScreentimeDispatchError as exc:
        assert str(exc) == "modal_function_not_found"
    else:  # pragma: no cover - defensive
        raise AssertionError("expected RetainedCastScreentimeDispatchError")

    assert enqueue_calls == []


def test_start_run_wraps_backend_runtime_errors(monkeypatch) -> None:
    def _raise(_run_id: str) -> dict[str, object]:
        raise RuntimeError("backend runtime unavailable")

    monkeypatch.setattr(
        retained_cast_screentime_dispatch.modal_dispatch,
        "dispatch_cast_screentime_run",
        lambda *, run_id: {"dispatched": False, "reason_code": "modal_disabled"},
    )
    monkeypatch.setattr(
        retained_cast_screentime_dispatch.modal_dispatch,
        "modal_dispatch_enabled",
        lambda: False,
    )
    monkeypatch.setattr(
        retained_cast_screentime_dispatch.retained_cast_screentime_runtime,
        "enqueue_run",
        _raise,
    )

    try:
        retained_cast_screentime_dispatch.start_run("run-123")
    except retained_cast_screentime_dispatch.RetainedCastScreentimeDispatchError as exc:
        assert str(exc) == "backend runtime unavailable"
    else:  # pragma: no cover - defensive
        raise AssertionError("expected RetainedCastScreentimeDispatchError")


def test_generate_segment_clip_uses_backend_runtime(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_generate_segment_clip(run_id: str, **kwargs):
        captured["run_id"] = run_id
        captured["kwargs"] = kwargs
        return {"run_id": run_id, "evidence": {"segment_key": kwargs["segment_key"], "mode": kwargs["mode"]}}

    monkeypatch.setattr(
        retained_cast_screentime_dispatch.retained_cast_screentime_runtime,
        "generate_segment_clip",
        _fake_generate_segment_clip,
    )

    result = retained_cast_screentime_dispatch.generate_segment_clip(
        "run-123",
        segment_key="segment-a",
        mode="timestamp",
        duration_seconds=12,
        ttl_days=3,
    )

    assert captured["run_id"] == "run-123"
    assert captured["kwargs"] == {
        "segment_key": "segment-a",
        "mode": "timestamp",
        "duration_seconds": 12,
        "ttl_days": 3,
    }
    assert result == {"run_id": "run-123", "evidence": {"segment_key": "segment-a", "mode": "timestamp"}}


def test_generate_segment_clip_wraps_backend_runtime_errors(monkeypatch) -> None:
    def _raise(run_id: str, **kwargs):
        del run_id, kwargs
        raise ValueError("clip generation failed")

    monkeypatch.setattr(
        retained_cast_screentime_dispatch.retained_cast_screentime_runtime,
        "generate_segment_clip",
        _raise,
    )

    try:
        retained_cast_screentime_dispatch.generate_segment_clip(
            "run-123",
            segment_key="segment-a",
            mode="exact",
            duration_seconds=12,
            ttl_days=3,
        )
    except retained_cast_screentime_dispatch.RetainedCastScreentimeDispatchError as exc:
        assert str(exc) == "clip generation failed"
    else:  # pragma: no cover - defensive
        raise AssertionError("expected RetainedCastScreentimeDispatchError")
