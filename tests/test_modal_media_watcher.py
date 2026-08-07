from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend import modal_jobs

WATCH_ID = "11111111-1111-1111-1111-111111111111"


def test_media_watch_cron_is_installed_only_for_the_singleton_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", "1")
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", raising=False)
    assert "schedule" in modal_jobs._modal_cron_schedule_kwargs("* * * * *")

    monkeypatch.delenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", raising=False)
    assert modal_jobs._modal_cron_schedule_kwargs("* * * * *") == {}


def test_poller_claims_only_the_bounded_number_and_propagates_current_fences(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import trr_backend.repositories.media_watchers as repository

    monkeypatch.setenv("TRR_MEDIA_WATCH_DISPATCH_LIMIT", "2")
    monkeypatch.setattr(modal_jobs, "_validate_modal_maintenance_owner_config", lambda: "modal_singleton_cron")
    monkeypatch.setattr(modal_jobs, "_worker_id", lambda _family: "poller-1")
    monkeypatch.setattr(modal_jobs, "_close_db_pools_after_worker", lambda *_args, **_kwargs: None)
    claims = iter(
        [
            {"id": WATCH_ID, "lease_fence": 4},
            {"id": "22222222-2222-2222-2222-222222222222", "lease_fence": 5},
            {"id": "33333333-3333-3333-3333-333333333333", "lease_fence": 6},
        ]
    )
    claim_calls: list[dict[str, object]] = []
    spawn_calls: list[dict[str, object]] = []

    def claim_due_watch(**kwargs):
        claim_calls.append(kwargs)
        return next(claims)

    monkeypatch.setattr(repository, "claim_due_watch", claim_due_watch)
    monkeypatch.setattr(
        modal_jobs,
        "run_show_season_media_watch_worker",
        SimpleNamespace(spawn=lambda **kwargs: spawn_calls.append(kwargs) or SimpleNamespace(object_id="fc-1")),
    )

    payload = modal_jobs.poll_due_show_season_media_watches.local()

    assert payload["claimed"] == 2
    assert payload["dispatched"] == 2
    assert len(claim_calls) == 2
    assert [call["lease_fence"] for call in spawn_calls] == [4, 5]
    assert all(call["lease_owner"] == "poller-1" for call in spawn_calls)


def test_poller_accepts_api_runtime_as_the_single_scheduler_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    import trr_backend.repositories.media_watchers as repository

    monkeypatch.setattr(modal_jobs, "_validate_modal_maintenance_owner_config", lambda: "api_runtime_scheduler")
    monkeypatch.setattr(repository, "claim_due_watch", lambda **_kwargs: None)
    monkeypatch.setattr(modal_jobs, "_close_db_pools_after_worker", lambda *_args, **_kwargs: None)
    payload = modal_jobs.poll_due_show_season_media_watches.local()
    assert payload["status"] == "completed"
    assert payload["claimed"] == 0


def test_api_runtime_scheduler_dispatches_the_modal_media_poller(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import main
    from trr_backend import modal_dispatch

    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        modal_dispatch,
        "spawn_modal_maintenance_function",
        lambda **kwargs: calls.append(kwargs) or {"dispatched": True},
    )

    assert main._run_modal_media_watch_poller_once() == {"dispatched": True}
    assert calls == [
        {
            "function_name": "poll_due_show_season_media_watches",
            "log_label": "show-season media watch poller",
            "dispatcher_name": "admin",
        }
    ]


def test_worker_heartbeats_current_fence_and_passes_it_to_service(monkeypatch: pytest.MonkeyPatch) -> None:
    import trr_backend.media.watchers.service as service
    import trr_backend.repositories.media_watchers as repository

    heartbeat_calls: list[dict[str, object]] = []
    service_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        repository,
        "heartbeat_lease",
        lambda **kwargs: heartbeat_calls.append(kwargs) or True,
    )
    monkeypatch.setattr(
        service,
        "run_show_season_media_watch",
        lambda watch, **kwargs: service_calls.append({"watch": watch, **kwargs})
        or SimpleNamespace(
            status="completed",
            run_id="run-1",
            summary={"added": 1},
            continuation={},
            error=None,
        ),
    )
    monkeypatch.setattr(modal_jobs, "_close_db_pools_after_worker", lambda *_args, **_kwargs: None)

    payload = modal_jobs.run_show_season_media_watch_worker.local(
        {"id": WATCH_ID, "poll_interval_seconds": 60, "consecutive_failures": 2},
        "worker-a",
        9,
        True,
    )

    assert payload["status"] == "completed"
    assert heartbeat_calls == [
        {
            "watch_id": WATCH_ID,
            "lease_owner": "worker-a",
            "lease_fence": 9,
            "lease_seconds": modal_jobs._show_season_media_watch_lease_seconds(),
        }
    ]
    assert service_calls[0]["lease_owner"] == "worker-a"
    assert service_calls[0]["lease_fence"] == 9
    assert service_calls[0]["backfill"] is True
    assert service_calls[0]["watch"]["poll_interval_seconds"] > 60


def test_worker_stops_on_expired_or_replaced_lease_without_calling_service(monkeypatch: pytest.MonkeyPatch) -> None:
    import trr_backend.media.watchers.service as service
    import trr_backend.repositories.media_watchers as repository

    monkeypatch.setattr(repository, "heartbeat_lease", lambda **_kwargs: False)
    monkeypatch.setattr(
        service,
        "run_show_season_media_watch",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("service must not run after a lost lease")),
    )
    monkeypatch.setattr(modal_jobs, "_close_db_pools_after_worker", lambda *_args, **_kwargs: None)

    payload = modal_jobs.run_show_season_media_watch_worker.local({"id": WATCH_ID}, "worker-a", 9)

    assert payload == {"watch_id": WATCH_ID, "lease_fence": 9, "status": "fenced"}
