from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

import trr_backend.socials.control_plane.worker_health as worker_health
import trr_backend.socials.social_season_analytics_impl as core


def _base_worker_payload(stale_after_seconds: int | None) -> dict[str, Any]:
    return {
        "healthy": False,
        "healthy_workers": 0,
        "fresh_workers": 0,
        "stale_workers": 0,
        "stale_hidden_count": 0,
        "active_workers": 0,
        "total_workers": 0,
        "stale_after_seconds": stale_after_seconds or 0,
        "by_stage": {},
        "by_platform": {},
        "workers": [],
        "reason": "no_workers",
    }


@pytest.mark.parametrize(
    ("get_health", "query_owner"),
    [
        pytest.param(worker_health.get_worker_health, worker_health, id="extracted-worker-health"),
        pytest.param(core.get_worker_health, core, id="legacy-monolith-worker-health"),
    ],
)
def test_worker_health_cache_does_not_hide_modal_executor_mode(
    monkeypatch: pytest.MonkeyPatch,
    get_health: Callable[..., dict[str, Any]],
    query_owner: object,
) -> None:
    monkeypatch.setenv("SOCIAL_QUEUE_ENABLED", "1")
    monkeypatch.setenv("SOCIAL_WORKER_HEALTH_CACHE_TTL_SECONDS", "60")
    monkeypatch.setattr(core, "_worker_health_cache", None)
    monkeypatch.setattr(core, "_build_worker_health_alerts", lambda _payload: [])
    monkeypatch.setattr(core, "_touch_modal_social_dispatcher_heartbeat", lambda **_kwargs: None)
    monkeypatch.setattr(core, "_modal_social_dispatch_ready", lambda: (True, None))
    monkeypatch.setattr(
        query_owner,
        "_query_worker_health",
        lambda *, stale_after_seconds=None: _base_worker_payload(stale_after_seconds),
    )

    monkeypatch.setattr(core, "is_modal_remote_executor_enabled", lambda: False)
    local_payload = get_health(stale_after_seconds=300)
    assert local_payload["reason"] == "no_workers"

    monkeypatch.setattr(core, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(
        core,
        "_build_modal_executor_health_payload",
        lambda *, reason=None: {"healthy": True, "executor_backend": "modal", "reason": reason},
    )

    modal_payload = get_health(stale_after_seconds=300)

    assert modal_payload == {"healthy": True, "executor_backend": "modal", "reason": None}


@pytest.mark.parametrize(
    ("get_health", "query_owner"),
    [
        pytest.param(worker_health.get_worker_health, worker_health, id="extracted-worker-health"),
        pytest.param(core.get_worker_health, core, id="legacy-monolith-worker-health"),
    ],
)
def test_worker_health_cache_does_not_reuse_modal_payload_after_executor_mode_turns_off(
    monkeypatch: pytest.MonkeyPatch,
    get_health: Callable[..., dict[str, Any]],
    query_owner: object,
) -> None:
    monkeypatch.setenv("SOCIAL_QUEUE_ENABLED", "1")
    monkeypatch.setenv("SOCIAL_WORKER_HEALTH_CACHE_TTL_SECONDS", "60")
    monkeypatch.setattr(core, "is_modal_remote_executor_enabled", lambda: False)
    monkeypatch.setattr(core, "_build_worker_health_alerts", lambda _payload: [])
    monkeypatch.setattr(
        core,
        "_worker_health_cache",
        (core.time_module.monotonic(), 300, {"healthy": True, "executor_backend": "modal"}),
    )
    monkeypatch.setattr(
        query_owner,
        "_query_worker_health",
        lambda *, stale_after_seconds=None: _base_worker_payload(stale_after_seconds),
    )

    payload = get_health(stale_after_seconds=300)

    assert payload["reason"] == "no_workers"
    assert payload.get("executor_backend") != "modal"
