from __future__ import annotations

import json

import pytest

import trr_backend.socials.social_season_analytics_impl as social_repo


def test_touch_job_dispatch_metadata_updates_only_dispatch_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing_metadata = {
        "worker_progress": {"items_seen": 12},
        "activity": {"phase": "shared_account_posts"},
        "dispatch": {
            "dispatch_backend": "modal",
            "dispatch_attempt_count": 2,
            "remote_task_id": "task-existing",
            "remote_invocation_id": "call-existing",
        },
    }
    calls: list[tuple[str, list[object]]] = []

    def _fake_fetch_one(sql: str, params: list[object] | None = None, **_kwargs: object) -> dict[str, object]:
        normalized = " ".join(str(sql).split()).lower()
        calls.append((normalized, list(params or [])))
        if normalized.startswith("select metadata from social.scrape_jobs"):
            return {"metadata": existing_metadata}
        if normalized.startswith("update social.scrape_jobs"):
            return {"id": "job-1"}
        raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    social_repo._touch_job_dispatch_metadata(  # noqa: SLF001
        "job-1",
        dispatch_backend="modal",
        remote_invocation_id=None,
        last_dispatch_error=None,
        last_dispatch_error_code=None,
        last_dispatch_error_at=None,
    )

    update_sql, update_params = calls[-1]
    assert "jsonb_set(coalesce(metadata, '{}'::jsonb), '{dispatch}'" in update_sql
    assert "metadata = %s::jsonb" not in update_sql
    assert update_params[1] == "job-1"

    dispatch_payload = json.loads(str(update_params[0]))
    assert "worker_progress" not in dispatch_payload
    assert "activity" not in dispatch_payload
    assert dispatch_payload["dispatch_backend"] == "modal"
    assert dispatch_payload["dispatch_attempt_count"] == 3
    assert dispatch_payload["remote_task_id"] == "task-existing"
    assert dispatch_payload["remote_invocation_id"] is None

    simulated_row_after_update = {**existing_metadata, "dispatch": dispatch_payload}
    assert simulated_row_after_update["worker_progress"] == {"items_seen": 12}
    assert simulated_row_after_update["activity"] == {"phase": "shared_account_posts"}
