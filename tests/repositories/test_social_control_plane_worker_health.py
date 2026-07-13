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


def test_worker_auth_capabilities_uses_source_only_non_instagram_cookie_loaders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_loads: list[str] = []

    monkeypatch.setattr(core, "_load_instagram_cookies_from_sources", lambda: {"sessionid": "instagram"})
    monkeypatch.setattr(core, "_inspect_instagram_cookie_health", lambda _cookies: {"valid": True})
    monkeypatch.setattr(core, "_load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(core, "_load_twikit_credentials", lambda _cookies: {})

    source_cookies = {
        "tiktok": {"sessionid": "tiktok"},
        "facebook": {"c_user": "user", "xs": "token"},
        "threads": {"sessionid": "threads", "csrftoken": "csrf"},
    }
    for platform in source_cookies:
        monkeypatch.setattr(
            core,
            f"_load_{platform}_cookies_from_sources",
            lambda platform=platform: source_loads.append(platform) or source_cookies[platform],
        )
        monkeypatch.setattr(
            core,
            f"_load_{platform}_cookies",
            lambda platform=platform: pytest.fail(f"{platform} freshness loader must not run"),
        )

    payload = worker_health.get_worker_auth_capabilities()

    assert source_loads == ["tiktok", "facebook", "threads"]
    assert payload["instagram_authenticated"] is True
    assert payload["tiktok_authenticated"] is True
    assert payload["facebook_authenticated"] is True
    assert payload["threads_authenticated"] is True


def test_worker_auth_capabilities_can_skip_instagram_live_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(core, "_load_instagram_cookies_from_sources", lambda: {"sessionid": "instagram"})
    monkeypatch.setattr(core, "_inspect_instagram_cookie_health", lambda _cookies: pytest.fail("live inspector ran"))
    monkeypatch.setattr(core, "_instagram_cookie_schema_result", lambda _cookies: {"valid": True})
    monkeypatch.setattr(core, "_load_tiktok_cookies_from_sources", lambda: {})
    monkeypatch.setattr(core, "_load_twitter_auth", lambda: ({}, None))
    monkeypatch.setattr(core, "_load_twikit_credentials", lambda _cookies: {})
    monkeypatch.setattr(core, "_load_facebook_cookies_from_sources", lambda: {})
    monkeypatch.setattr(core, "_load_threads_cookies_from_sources", lambda: {})

    payload = worker_health.get_worker_auth_capabilities(validate_instagram=False)

    assert payload["instagram_authenticated"] is True


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
    monkeypatch.setattr(
        core,
        "_touch_modal_social_dispatcher_heartbeat",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("worker-health reads must not write heartbeats")),
    )
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
