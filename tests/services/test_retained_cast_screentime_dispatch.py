from __future__ import annotations

from trr_backend.services import retained_cast_screentime_dispatch


def test_start_run_delegates_to_legacy_dispatch_client(monkeypatch) -> None:
    monkeypatch.setattr(
        retained_cast_screentime_dispatch.screenalytics_cast_screentime,
        "start_run",
        lambda run_id: {"run_id": run_id, "accepted": True},
    )

    result = retained_cast_screentime_dispatch.start_run("run-123")

    assert result == {"run_id": "run-123", "accepted": True}


def test_generate_segment_clip_delegates_to_legacy_dispatch_client(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_generate_segment_clip(run_id: str, **kwargs):
        captured["run_id"] = run_id
        captured["kwargs"] = kwargs
        return {"run_id": run_id, "evidence": {"segment_key": kwargs["segment_key"]}}

    monkeypatch.setattr(
        retained_cast_screentime_dispatch.screenalytics_cast_screentime,
        "generate_segment_clip",
        _fake_generate_segment_clip,
    )

    result = retained_cast_screentime_dispatch.generate_segment_clip(
        "run-123",
        segment_key="segment-a",
        mode="exact",
        duration_seconds=12,
        ttl_days=3,
    )

    assert captured["run_id"] == "run-123"
    assert captured["kwargs"] == {
        "segment_key": "segment-a",
        "mode": "exact",
        "duration_seconds": 12,
        "ttl_days": 3,
    }
    assert result == {"run_id": "run-123", "evidence": {"segment_key": "segment-a"}}
