from __future__ import annotations

import ast
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import trr_backend.socials.control_plane.queue_status as queue_status
import trr_backend.socials.control_plane.worker_health as worker_health
import trr_backend.socials.social_season_analytics_impl as core

_REQUIRED_PROVIDER_NAMES = (
    "SOCIAL_WORKER_HEALTH_CACHE_TTL_SECONDS_DEFAULT",
    "SOCIAL_WORKER_HEARTBEAT_STALE_SECONDS_DEFAULT",
    "SocialWorkerUnavailableError",
    "TRUSTED_LOCAL_WORKER_LANE",
    "_build_modal_executor_health_payload",
    "_build_worker_health_alerts",
    "_inspect_instagram_cookie_health",
    "_instagram_cookie_schema_result",
    "_iso",
    "_load_facebook_cookies_from_sources",
    "_load_instagram_cookies_from_sources",
    "_load_threads_cookies_from_sources",
    "_load_tiktok_cookies_from_sources",
    "_load_twikit_credentials",
    "_load_twitter_auth",
    "_metadata_dict",
    "_modal_social_dispatch_ready",
    "_normalize_platform_name",
    "_normalize_required_worker_lane",
    "_normalize_worker_stage",
    "_normalize_worker_status",
    "_now_utc",
    "_query_worker_health",
    "_queue_status_cache",
    "_queue_status_cache_lock",
    "_resolve_positive_int_env",
    "_touch_modal_social_dispatcher_heartbeat",
    "_worker_health_cache",
    "_worker_health_cache_lock",
    "_worker_heartbeat_schema_ready",
    "get_worker_detail",
    "is_modal_remote_executor_enabled",
    "pg",
    "probe_remote_auth_health",
    "time_module",
)
_LEGACY_MODULES = {
    "trr_backend.repositories.social_season_analytics",
    "trr_backend.socials.social_season_analytics_impl",
}


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


def test_worker_health_source_reuses_queue_status_proxy_without_legacy_import() -> None:
    source_path = Path(worker_health.__file__).resolve()
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    imported_names: set[tuple[str, str]] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert not ({alias.name for alias in node.names} & _LEGACY_MODULES)
        elif isinstance(node, ast.ImportFrom):
            assert node.module not in _LEGACY_MODULES
            if node.module == "trr_backend.socials.control_plane.queue_status":
                imported_names.update((node.module, alias.name) for alias in node.names)
        elif isinstance(node, ast.Call) and node.args:
            function_name = (
                node.func.id
                if isinstance(node.func, ast.Name)
                else node.func.attr
                if isinstance(node.func, ast.Attribute)
                else ""
            )
            if function_name in {"import_module", "__import__"}:
                first_argument = node.args[0]
                assert not (isinstance(first_argument, ast.Constant) and first_argument.value in _LEGACY_MODULES)

    assert imported_names == {
        ("trr_backend.socials.control_plane.queue_status", "_legacy_repo"),
        ("trr_backend.socials.control_plane.queue_status", "get_queue_status"),
    }


def test_worker_health_uses_exact_live_queue_status_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert worker_health._core is queue_status._legacy_repo()
    assert queue_status._LEGACY_NAMESPACE is core.__dict__
    assert set(_REQUIRED_PROVIDER_NAMES) <= core.__dict__.keys()
    for name in _REQUIRED_PROVIDER_NAMES:
        assert getattr(worker_health._core, name) is core.__dict__[name]

    replacement = object()
    monkeypatch.setattr(core, "_build_worker_health_alerts", replacement)
    assert worker_health._core._build_worker_health_alerts is replacement


def test_worker_health_shared_provider_writes_through_both_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_cache = object()
    queue_cache = object()
    monkeypatch.setattr(core, "_worker_health_cache", worker_cache)
    monkeypatch.setattr(core, "_queue_status_cache", queue_cache)

    assert worker_health._core._worker_health_cache is worker_cache
    assert worker_health._core._queue_status_cache is queue_cache

    worker_health._clear_worker_health_caches()

    assert core._worker_health_cache is None
    assert core._queue_status_cache is None


def test_worker_health_shared_provider_fails_deterministically_when_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(queue_status, "_LEGACY_NAMESPACE", None)

    with pytest.raises(
        RuntimeError,
        match="Queue-status provider is not configured for read: pg",
    ):
        _ = worker_health._core.pg
    with pytest.raises(
        RuntimeError,
        match="Queue-status provider is not configured for write: _worker_health_cache",
    ):
        worker_health._core._worker_health_cache = None


def test_worker_health_ordinary_import_defers_provider_until_late_publication() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    script = "\n".join(
        (
            "import sys",
            "legacy_name = 'trr_backend.socials.social_season_analytics_impl'",
            "assert legacy_name not in sys.modules",
            "import trr_backend.socials.control_plane.worker_health as leaf",
            "import trr_backend.socials.control_plane.queue_status as queue_status",
            "import trr_backend.socials.control_plane as control_plane",
            "assert legacy_name not in sys.modules",
            "assert queue_status._LEGACY_NAMESPACE is None",
            "try:",
            "    leaf._core.pg",
            "except RuntimeError as exc:",
            "    assert 'Queue-status provider is not configured' in str(exc)",
            "else:",
            "    raise AssertionError('unpublished provider must fail closed')",
            "import trr_backend.socials.social_season_analytics_impl as legacy",
            "legacy = sys.modules[legacy_name]",
            "assert leaf._core is queue_status._legacy_repo()",
            "assert queue_status._LEGACY_NAMESPACE is legacy.__dict__",
            "assert control_plane.get_worker_health is leaf.get_worker_health",
            f"required_names = {tuple(_REQUIRED_PROVIDER_NAMES)!r}",
            "for name in required_names:",
            "    assert getattr(leaf._core, name) is legacy.__dict__[name]",
        )
    )

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=backend_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


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
