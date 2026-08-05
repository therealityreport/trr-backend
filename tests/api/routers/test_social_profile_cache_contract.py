"""Direct, offline contracts for the account-profile route cache helpers."""

from __future__ import annotations

from concurrent.futures import Future

import pytest

from api.routers.socials import _profile_cache as profile_cache
from trr_backend.db.pg import DatabaseServiceUnavailableError


@pytest.fixture(autouse=True)
def _reset_account_profile_caches() -> None:
    profile_cache._clear_account_profile_caches()
    yield
    profile_cache._clear_account_profile_caches()


def test_account_profile_cache_key_normalizes_identity_and_preserves_extras() -> None:
    assert profile_cache._account_profile_cache_key(
        surface="posts",
        platform=" Instagram ",
        account_handle=" @BravoTV ",
        page=2,
        page_size=25,
        search=" CAST ",
        window=" 30D ",
        comments_only=1,
        comment_filter=" Missing ",
        sort_by=" Posted_At ",
        sort_dir=" DESC ",
        post_source_id="  ig-1  ",
        extra=("run-7", 10),
    ) == (
        "posts",
        "instagram",
        "bravotv",
        2,
        25,
        "cast",
        "30d",
        True,
        "missing",
        "posted_at",
        "desc",
        "ig-1",
        "run-7",
        10,
    )


def test_clear_account_profile_caches_clears_all_payloads_and_singleflight() -> None:
    payload_caches = (
        profile_cache._ACCOUNT_PROFILE_SUMMARY_CACHE,
        profile_cache._ACCOUNT_PROFILE_DASHBOARD_CACHE,
        profile_cache._ACCOUNT_PROFILE_PROGRESS_CACHE,
        profile_cache._ACCOUNT_PROFILE_POSTS_CACHE,
        profile_cache._ACCOUNT_PROFILE_HASHTAGS_CACHE,
        profile_cache._ACCOUNT_PROFILE_HASHTAG_TIMELINE_CACHE,
        profile_cache._ACCOUNT_PROFILE_COLLABORATORS_CACHE,
        profile_cache._ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE,
    )
    for index, cache in enumerate(payload_caches):
        cache[("key", index)] = (float("inf"), {"index": index})
    profile_cache._ACCOUNT_PROFILE_SINGLEFLIGHT[("pending",)] = Future()

    profile_cache._clear_account_profile_caches()

    assert all(cache == {} for cache in payload_caches)
    assert profile_cache._ACCOUNT_PROFILE_SINGLEFLIGHT == {}


def test_account_profile_singleflight_caches_deep_copies_and_retries_failures() -> None:
    cache: dict[object, tuple[float, dict[str, object]]] = {}
    cache_lock = profile_cache.Lock()
    calls: list[str] = []

    def loader() -> dict[str, object]:
        calls.append("load")
        return {"items": [1]}

    first = profile_cache._resolve_account_profile_singleflight(
        ("summary", "instagram", "bravotv"),
        loader,
        cache=cache,
        cache_lock=cache_lock,
        ttl_seconds=30,
        max_entries=4,
    )
    first["items"].append(2)
    second = profile_cache._resolve_account_profile_singleflight(
        ("summary", "instagram", "bravotv"),
        loader,
        cache=cache,
        cache_lock=cache_lock,
        ttl_seconds=30,
        max_entries=4,
    )

    assert calls == ["load"]
    assert second == {"items": [1]}

    failure_calls = 0

    def failing_loader() -> dict[str, object]:
        nonlocal failure_calls
        failure_calls += 1
        raise RuntimeError("offline failure")

    for _ in range(2):
        with pytest.raises(RuntimeError, match="offline failure"):
            profile_cache._resolve_account_profile_singleflight(
                ("failure",),
                failing_loader,
            )
    assert failure_calls == 2
    assert profile_cache._ACCOUNT_PROFILE_SINGLEFLIGHT == {}


def test_catalog_freshness_uses_cached_lane_and_force_bypasses_it() -> None:
    calls: list[dict[str, object]] = []

    def loader(**kwargs: object) -> dict[str, object]:
        calls.append(dict(kwargs))
        return {"sequence": len(calls)}

    cached_first = profile_cache._resolve_account_profile_catalog_freshness(
        platform="instagram",
        account_handle="bravotv",
        force=False,
        statement_timeout_ms=900,
        loader=loader,
    )
    cached_second = profile_cache._resolve_account_profile_catalog_freshness(
        platform="instagram",
        account_handle="bravotv",
        force=False,
        statement_timeout_ms=900,
        loader=loader,
    )
    forced = profile_cache._resolve_account_profile_catalog_freshness(
        platform="instagram",
        account_handle="bravotv",
        force=True,
        statement_timeout_ms=900,
        loader=loader,
    )

    assert cached_first == cached_second == {"sequence": 1}
    assert forced == {"sequence": 2}
    assert calls == [
        {
            "platform": "instagram",
            "account_handle": "bravotv",
            "use_cached_live_total_only": True,
            "statement_timeout_ms": 900,
        },
        {
            "platform": "instagram",
            "account_handle": "bravotv",
            "statement_timeout_ms": 900,
        },
    ]


def test_catalog_freshness_returns_stale_payload_on_database_refresh_failure() -> None:
    cache_key = profile_cache._account_profile_cache_key(
        surface="catalog-freshness",
        platform="instagram",
        account_handle="bravotv",
        extra=(700,),
    )
    profile_cache._ACCOUNT_PROFILE_CATALOG_FRESHNESS_CACHE[cache_key] = (
        0.0,
        {"platform": "instagram", "account_handle": "bravotv", "eligible": True},
    )

    def loader(**_kwargs: object) -> dict[str, object]:
        raise DatabaseServiceUnavailableError("offline database", reason="pool_capacity")

    result = profile_cache._resolve_account_profile_catalog_freshness(
        platform="instagram",
        account_handle="bravotv",
        force=False,
        statement_timeout_ms=700,
        loader=loader,
    )

    assert result == {
        "platform": "instagram",
        "account_handle": "bravotv",
        "eligible": True,
        "stale": True,
        "degraded": True,
        "freshness_error": {
            "code": "CATALOG_FRESHNESS_REFRESH_FAILED",
            "message": "offline database",
            "retryable": True,
        },
    }


def test_catalog_run_progress_forwards_exact_arguments_and_caches_result() -> None:
    calls: list[dict[str, object]] = []

    def loader(**kwargs: object) -> dict[str, object]:
        calls.append(dict(kwargs))
        return {"status": "running", "logs": []}

    first = profile_cache._resolve_account_profile_catalog_run_progress(
        platform="instagram",
        account_handle="bravotv",
        run_id="run-7",
        recent_log_limit=12,
        fast=True,
        loader=loader,
    )
    second = profile_cache._resolve_account_profile_catalog_run_progress(
        platform="instagram",
        account_handle="bravotv",
        run_id="run-7",
        recent_log_limit=12,
        fast=True,
        loader=loader,
    )

    assert first == second == {"status": "running", "logs": []}
    assert calls == [
        {
            "platform": "instagram",
            "account_handle": "bravotv",
            "run_id": "run-7",
            "recent_log_limit": 12,
            "fast": True,
        }
    ]
