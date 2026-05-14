from __future__ import annotations

import pytest

from trr_backend.socials.control_plane import dispatch_runtime


def test_dispatch_runtime_skips_same_active_shared_account(monkeypatch: pytest.MonkeyPatch) -> None:
    legacy = dispatch_runtime.legacy
    monkeypatch.setenv("SOCIAL_SHARED_ACCOUNT_POSTS_PLATFORM_CAP_INSTAGRAM", "2")

    candidates = [
        {
            "id": "job-daily-queued",
            "run_id": "run-catalog",
            "platform": "instagram",
            "job_type": legacy.SHARED_ACCOUNT_POSTS_JOB_TYPE,
            "status": "queued",
            "config": {
                "stage": legacy.SHARED_ACCOUNT_POSTS_STAGE,
                "account": "bravodailydish",
                "pipeline_ingest_mode": legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            },
            "metadata": {},
        },
        {
            "id": "job-tv-queued",
            "run_id": "run-catalog",
            "platform": "instagram",
            "job_type": legacy.SHARED_ACCOUNT_POSTS_JOB_TYPE,
            "status": "queued",
            "config": {
                "stage": legacy.SHARED_ACCOUNT_POSTS_STAGE,
                "account": "bravotv",
                "pipeline_ingest_mode": legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            },
            "metadata": {},
        },
    ]
    dispatched_job_ids: list[str] = []

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(
        legacy,
        "_modal_social_dispatch_resolution",
        lambda: {
            "resolved": True,
            "reason": None,
            "app_name": "trr-backend-jobs",
            "function_name": "run_social_job",
        },
    )
    monkeypatch.setattr(
        legacy,
        "_list_candidate_jobs_for_modal_dispatch",
        lambda *, run_id=None, limit=200: list(candidates),
    )
    monkeypatch.setattr(
        legacy,
        "_current_modal_dispatch_running_counts",
        lambda: (
            {legacy.SHARED_ACCOUNT_POSTS_STAGE: 1},
            {(legacy.SHARED_ACCOUNT_POSTS_STAGE, "instagram"): 1},
            {"run-existing": 1},
            {("run-existing", legacy.SHARED_ACCOUNT_POSTS_STAGE, "instagram"): 1},
            {(legacy.SHARED_ACCOUNT_POSTS_STAGE, "instagram"): {"bravodailydish"}},
        ),
    )
    monkeypatch.setattr(legacy, "_touch_job_dispatch_metadata", lambda job_id, **kwargs: None)
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **kwargs: {})
    monkeypatch.setattr(
        legacy,
        "dispatch_social_job",
        lambda *, job_id, stage=None: (
            dispatched_job_ids.append(job_id) or {"dispatched": True, "reason": None, "call_id": f"call-{job_id}"}
        ),
    )

    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-catalog", limit=2)

    assert result["dispatched_job_ids"] == ["job-tv-queued"]
    assert dispatched_job_ids == ["job-tv-queued"]


def test_dispatch_runtime_marks_claimed_modal_dispatch_running(monkeypatch: pytest.MonkeyPatch) -> None:
    legacy = dispatch_runtime.legacy
    claimed_job = {
        "id": "job-claimed",
        "run_id": "run-1",
        "metadata": {"dispatch": {"dispatch_backend": "modal", "remote_invocation_id": "fc-pending"}},
    }
    marked: list[dict[str, object]] = []

    monkeypatch.setattr(legacy, "_claim_job_by_id", lambda *, job_id, worker_id: claimed_job)
    monkeypatch.setattr(legacy, "_set_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        legacy,
        "_mark_claimed_modal_dispatch_running",
        lambda job: marked.append(dict(job)),
    )
    monkeypatch.setattr(
        legacy,
        "process_claimed_job",
        lambda job, *, worker_id=None: {"id": job["id"], "worker_id": worker_id},
    )

    def dispatch_due_social_jobs_stub(*, run_id=None, limit=None):
        return {"dispatched_job_ids": []}

    monkeypatch.setattr(legacy, "dispatch_due_social_jobs", dispatch_due_social_jobs_stub)

    result = dispatch_runtime.claim_and_process_social_job(job_id="job-claimed", worker_id="modal:test")

    assert result["claimed"] is True
    assert marked == [claimed_job]
