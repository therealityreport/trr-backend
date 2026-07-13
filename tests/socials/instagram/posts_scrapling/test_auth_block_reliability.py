"""Tests for the posts-lane auth-block reliability layer (A2 + A3 + A4 seams).

Covers:
  * proxy session-key rotation generation suffixing (A2),
  * fetcher.rotate_session re-resolving to the next pool identity (A3),
  * the job-runner auth-block handler doing a bounded rotate-on-block retry and
    recording a cooldown when the budget is exhausted (A2 + A4),
  * checkpoint blocks skipping rotation and the per-page cooldown soft-stop (A4).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _pin_authenticated_scrape_mode(monkeypatch):
    """The default scrape mode flipped to ``public_first``, which skips the
    authenticated-fetch / rotate-on-block path these tests exercise. Pin the
    authenticated mode so the auth-block handler and cooldown seams run.
    """
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "authenticated")


@pytest.fixture
def _mock_scrapling(monkeypatch):
    """Prevent real Scrapling import (mirrors test_fetcher.py)."""
    mock_module = MagicMock()
    mock_module.StealthyFetcher = MagicMock()
    mock_module.ProxyRotator = MagicMock()
    monkeypatch.setitem(__import__("sys").modules, "scrapling.fetchers", mock_module)
    return mock_module


# ---------------------------------------------------------------------------
# Identity-level auth cooldown helpers
# ---------------------------------------------------------------------------


def test_record_identity_auth_block_uses_reserved_sentinel_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.instagram import auth_cooldown

    calls: list[tuple[str, str, str | None]] = []
    expected = object()

    def _record(platform: str, account_handle: str, error_code: str | None):
        calls.append((platform, account_handle, error_code))
        return expected

    monkeypatch.setattr(auth_cooldown, "record_auth_block", _record)

    result = auth_cooldown.record_identity_auth_block(
        platform=" Instagram ",
        error_code="http_401",
    )

    assert result is expected
    assert calls == [("instagram", "__identity__:instagram", "http_401")]


def test_identity_auth_cooldown_helpers_use_existing_get_and_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.instagram import auth_cooldown

    calls: list[tuple[Any, ...]] = []
    expected = object()

    def _get(platform: str, account_handle: str):
        calls.append(("get", platform, account_handle))
        return expected

    def _clear(platform: str, account_handle: str, *, force: bool = False) -> bool:
        calls.append(("clear", platform, account_handle, force))
        return force

    monkeypatch.setattr(auth_cooldown, "get_active_cooldown", _get)
    monkeypatch.setattr(auth_cooldown, "clear_cooldown", _clear)

    assert auth_cooldown.get_active_identity_cooldown(platform="Instagram") is expected
    assert auth_cooldown.clear_identity_cooldown(platform="Instagram") is False
    assert auth_cooldown.clear_identity_cooldown(platform="Instagram", force=True) is True
    assert calls == [
        ("get", "instagram", "__identity__:instagram"),
        ("clear", "instagram", "__identity__:instagram", False),
        ("clear", "instagram", "__identity__:instagram", True),
    ]


# ---------------------------------------------------------------------------
# A2: proxy session-key rotation generation
# ---------------------------------------------------------------------------


def test_proxy_session_key_generation_zero_is_unsuffixed() -> None:
    from trr_backend.socials.instagram.posts_scrapling.job_runner import _posts_proxy_session_key

    base = _posts_proxy_session_key(
        account_handle="thetraitorsus",
        stage="posts_scrapling",
        config={},
        job_metadata={},
        browser_account_id="thetraitorsus",
        rotation_generation=0,
    )
    # Generation 0 must equal the legacy (pre-rotation) key so persisted
    # proxy_session_key values and steady-state IP affinity are unchanged.
    assert ":gen" not in base
    assert base == "thetraitorsus"


def test_proxy_session_key_generation_suffix_changes_key() -> None:
    from trr_backend.socials.instagram.posts_scrapling.job_runner import _posts_proxy_session_key

    kwargs = {
        "account_handle": "thetraitorsus",
        "stage": "posts_scrapling",
        "config": {"shard_count": 2, "shard_index": 1},
        "job_metadata": {},
        "browser_account_id": "thetraitorsus",
    }
    gen0 = _posts_proxy_session_key(**kwargs, rotation_generation=0)
    gen1 = _posts_proxy_session_key(**kwargs, rotation_generation=1)
    gen2 = _posts_proxy_session_key(**kwargs, rotation_generation=2)

    assert gen1 == f"{gen0}:gen1"
    assert gen2 == f"{gen0}:gen2"
    # Distinct keys => distinct Decodo sticky session ids => distinct IPs.
    assert len({gen0, gen1, gen2}) == 3


# ---------------------------------------------------------------------------
# A3: fetcher.rotate_session
# ---------------------------------------------------------------------------


def _build_fetcher(_mock_scrapling, *, identity_provider=None):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    return InstagramPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "sess-1", "domain": ".instagram.com", "path": "/"}],
        raw_cookies={"sessionid": "sess-1", "csrftoken": "csrf-1", "ds_user_id": "111"},
        browser_account_id="acct1",
        identity_provider=identity_provider,
    )


def test_rotate_session_advances_identity_when_sessionid_changes(_mock_scrapling) -> None:
    from trr_backend.socials.instagram.posts_scrapling.session import PostsRotatedIdentity

    next_identity = PostsRotatedIdentity(
        raw_cookies={"sessionid": "sess-2", "csrftoken": "csrf-2", "ds_user_id": "222"},
        cookies=[{"name": "sessionid", "value": "sess-2", "domain": ".instagram.com", "path": "/"}],
        browser_account_id="acct2",
        session_id="ig-2-abc",
    )
    fetcher = _build_fetcher(_mock_scrapling, identity_provider=lambda: next_identity)

    rotated = asyncio.run(fetcher.rotate_session(reason="rotate_on_block:test"))

    assert rotated is True
    # Identity-bearing cookies replaced wholesale; counter advanced; warmup reset.
    assert fetcher._raw_cookies["sessionid"] == "sess-2"
    assert fetcher._raw_cookies["ds_user_id"] == "222"
    assert fetcher._browser_account_id == "acct2"
    assert fetcher._identity_rotation_count == 1
    assert fetcher.runtime_metadata["identity_rotation_count"] == 1


def test_rotate_session_is_noop_for_single_identity(_mock_scrapling) -> None:
    from trr_backend.socials.instagram.posts_scrapling.session import PostsRotatedIdentity

    # One identity => provider yields the same sessionid each call.
    same_identity = PostsRotatedIdentity(
        raw_cookies={"sessionid": "sess-1", "csrftoken": "csrf-1", "ds_user_id": "111"},
        cookies=[],
        browser_account_id="acct1",
        session_id="ig-1-abc",
    )
    fetcher = _build_fetcher(_mock_scrapling, identity_provider=lambda: same_identity)

    rotated = asyncio.run(fetcher.rotate_session(reason="rotate_on_block:test"))

    assert rotated is False
    assert fetcher._identity_rotation_count == 0
    assert fetcher._raw_cookies["sessionid"] == "sess-1"


def test_rotate_session_without_provider_only_swaps_proxy(_mock_scrapling) -> None:
    from trr_backend.socials.instagram.posts_scrapling.proxy import PostsProxyConfig

    fetcher = _build_fetcher(_mock_scrapling, identity_provider=None)
    new_proxy = PostsProxyConfig(
        browser_proxy="http://example:7000",
        api_proxy_url="http://example:7000",
        proxy_rotator=None,
        fingerprint="example:7000:explicit",
    )

    rotated = asyncio.run(fetcher.rotate_session(proxy_config=new_proxy, reason="rotate_on_block"))

    assert rotated is False
    assert fetcher._selected_proxy_fingerprint == "example:7000:explicit"
    assert fetcher._api_proxy_url == "http://example:7000"


# ---------------------------------------------------------------------------
# A2 + A4: job-runner auth-block handler
# ---------------------------------------------------------------------------


def _patch_repo(monkeypatch: pytest.MonkeyPatch, captured_finish: dict[str, Any]) -> None:
    from trr_backend.repositories import social_season_analytics as repo

    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_a, **_k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_k: True)
    monkeypatch.setattr(repo, "_iso", lambda _v: "2026-06-07T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: datetime(2026, 6, 7, tzinfo=UTC))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_a, **_k: {})
    monkeypatch.setattr(repo, "persist_instagram_profile_pagination_state", lambda **_k: {"end_cursor": None})
    monkeypatch.setattr(repo, "latest_instagram_profile_pagination_state", lambda **_k: {})
    monkeypatch.setattr(repo, "instagram_posts_acceleration_flags", lambda: {})

    def _fake_finish_job(job_id, *, status, items_found, error_message=None, metadata=None, **kwargs):  # noqa: ANN001
        captured_finish["status"] = status
        captured_finish["error_message"] = error_message
        captured_finish["metadata"] = metadata
        captured_finish.update(kwargs)

    monkeypatch.setattr(repo, "_finish_job", _fake_finish_job)


def _patch_session_and_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.resolve_posts_scrapling_session",
        lambda **_k: SimpleNamespace(
            cookies={},
            browser_account_id="thetraitorsus",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.select_posts_proxy",
        lambda **_k: None,
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.build_posts_identity_provider",
        lambda _session: None,
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.pg.fetch_one",
        lambda *_a, **_k: {},
    )


def test_auth_block_rotates_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """A2: a hard 401 triggers a sticky-session rotation; the retried cursor
    succeeds, so the job completes without recording a cooldown."""
    from trr_backend.socials.instagram import auth_cooldown
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr
    from trr_backend.socials.instagram.posts_scrapling.persistence import PersistedInstagramPosts

    rotations: list[str] = []
    recorded: list[Any] = []

    class _FakeFetcher:
        runtime_metadata = {"request_count": 1}

        def __init__(self, **_k) -> None:
            self._calls = 0

        async def warmup(self, _a: str) -> None:
            return None

        async def fetch_posts_page(self, _a: str, *, cursor=None, direction="forward"):  # noqa: ANN001
            del cursor, direction
            self._calls += 1
            if self._calls == 1:
                return SimpleNamespace(
                    auth_failed=True,
                    fetch_failed=True,
                    retryable=False,
                    fetch_reason="http_401",
                    posts=[],
                    has_next_page=False,
                    end_cursor=None,
                )
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=False,
                retryable=False,
                fetch_reason=None,
                posts=[{"shortcode": "ok1"}],
                has_next_page=False,
                end_cursor=None,
            )

        async def set_api_proxy_config(self, _cfg, *, reason: str) -> None:
            rotations.append(reason)

        async def rotate_session(self, *, proxy_config=None, reason: str = "rotate_session") -> bool:  # noqa: ANN001
            rotations.append(reason)
            return False

        async def aclose(self) -> None:
            return None

    captured: dict[str, Any] = {}
    _patch_repo(monkeypatch, captured)
    _patch_session_and_proxy(monkeypatch)
    # Authenticated mode defaults to a rotate-on-block budget of 0; allow exactly
    # one bounded rotate-on-block retry so the retried cursor can succeed.
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_ROTATE_ON_BLOCK_MAX_RETRIES", "1")
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", _FakeFetcher)
    monkeypatch.setattr(jr, "persist_instagram_posts", lambda **_k: PersistedInstagramPosts(1, 0, {}))
    monkeypatch.setattr(jr, "_raise_if_auth_cooldown_active", lambda **_k: None)
    monkeypatch.setattr(auth_cooldown, "record_auth_block", lambda *a, **k: recorded.append(a) or None)
    monkeypatch.setattr(auth_cooldown, "clear_cooldown", lambda *a, **k: True)

    job = {"id": "job-1", "run_id": "run-1", "config": {"account": "thetraitorsus", "stage": "posts_scrapling"}}
    jr.run_instagram_posts_scrapling_job(job, worker_id="w1")

    assert captured["status"] == "completed"
    assert rotations == ["rotate_on_block"]  # one A2 sticky-session swap
    assert recorded == []  # success after rotate => no cooldown recorded


def test_auth_block_records_cooldown_when_budget_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    """A2 + A4: persistent 401s exhaust the rotate-on-block budget, then a
    cooldown is recorded and the job fails non-retryably."""
    from trr_backend.socials.instagram import auth_cooldown
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr

    fetch_attempts: list[int] = []
    proxy_swaps: list[str] = []
    session_rotations: list[str] = []
    recorded: list[tuple[Any, ...]] = []

    class _FakeFetcher:
        runtime_metadata = {"request_count": 1}

        def __init__(self, **_k) -> None:
            pass

        async def warmup(self, _a: str) -> None:
            return None

        async def fetch_posts_page(self, _a: str, *, cursor=None, direction="forward"):  # noqa: ANN001
            del cursor, direction
            fetch_attempts.append(1)
            return SimpleNamespace(
                auth_failed=True,
                fetch_failed=True,
                retryable=False,
                fetch_reason="http_403",
                posts=[],
                has_next_page=False,
                end_cursor=None,
            )

        async def set_api_proxy_config(self, _cfg, *, reason: str) -> None:
            proxy_swaps.append(reason)

        async def rotate_session(self, *, proxy_config=None, reason: str = "rotate_session") -> bool:  # noqa: ANN001
            session_rotations.append(reason)
            return False

        async def aclose(self) -> None:
            return None

    captured: dict[str, Any] = {}
    _patch_repo(monkeypatch, captured)
    _patch_session_and_proxy(monkeypatch)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_ROTATE_ON_BLOCK_MAX_RETRIES", "2")
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", _FakeFetcher)
    monkeypatch.setattr(jr, "_raise_if_auth_cooldown_active", lambda **_k: None)
    monkeypatch.setattr(
        auth_cooldown,
        "record_auth_block",
        lambda *a, **k: recorded.append(a) or SimpleNamespace(to_metadata=lambda: {"blocker_kind": "auth"}),
    )

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {"account": "thetraitorsus", "stage": "posts_scrapling"},
        "attempt_count": 1,
        "max_attempts": 1,
    }
    jr.run_instagram_posts_scrapling_job(job, worker_id="w1")

    # 3 fetch attempts: the initial + 2 bounded rotate-on-block retries.
    assert len(fetch_attempts) == 3
    # Generation 1 = pure A2 sticky-session swap. Generation 2 exceeds the
    # per-identity session budget (default 1) so it attempts an A3 identity
    # advance, which (single identity) returns False and falls back to an A2 swap.
    assert session_rotations == ["rotate_on_block:http_403"]
    assert proxy_swaps == ["rotate_on_block", "rotate_on_block"]
    # Budget exhausted => cooldown recorded once, job fails non-retryably.
    assert len(recorded) == 1
    assert recorded[0][0] == "instagram"
    assert captured["status"] == "failed"
    assert captured["metadata"]["error_code"] == "instagram_posts_auth_failed"


def test_checkpoint_block_skips_rotation_and_records_cooldown(monkeypatch: pytest.MonkeyPatch) -> None:
    """A4.6: a checkpoint must NOT auto-rotate-retry; it records a cooldown and
    surfaces a checkpoint error code immediately."""
    from trr_backend.socials.instagram import auth_cooldown
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr

    rotations: list[str] = []
    recorded: list[tuple[Any, ...]] = []

    class _FakeFetcher:
        runtime_metadata = {"request_count": 1}

        def __init__(self, **_k) -> None:
            pass

        async def warmup(self, _a: str) -> None:
            return None

        async def fetch_posts_page(self, _a: str, *, cursor=None, direction="forward"):  # noqa: ANN001
            del cursor, direction
            return SimpleNamespace(
                auth_failed=True,
                fetch_failed=True,
                retryable=False,
                fetch_reason="redirect_to_checkpoint",
                posts=[],
                has_next_page=False,
                end_cursor=None,
            )

        async def set_api_proxy_config(self, _cfg, *, reason: str) -> None:
            rotations.append(reason)

        async def rotate_session(self, *, proxy_config=None, reason: str = "rotate_session") -> bool:  # noqa: ANN001
            rotations.append(reason)
            return False

        async def aclose(self) -> None:
            return None

    captured: dict[str, Any] = {}
    _patch_repo(monkeypatch, captured)
    _patch_session_and_proxy(monkeypatch)
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", _FakeFetcher)
    monkeypatch.setattr(jr, "_raise_if_auth_cooldown_active", lambda **_k: None)
    monkeypatch.setattr(
        auth_cooldown,
        "record_auth_block",
        lambda *a, **k: recorded.append(a) or SimpleNamespace(to_metadata=lambda: {"blocker_kind": "checkpoint"}),
    )

    job = {"id": "job-1", "run_id": "run-1", "config": {"account": "thetraitorsus", "stage": "posts_scrapling"}}
    jr.run_instagram_posts_scrapling_job(job, worker_id="w1")

    assert rotations == []  # no rotation on checkpoint
    assert len(recorded) == 1
    assert captured["status"] == "failed"
    assert captured["metadata"]["error_code"] == "instagram_posts_checkpoint_required"


def test_active_cooldown_soft_stops_and_requeues(monkeypatch: pytest.MonkeyPatch) -> None:
    """A4 READ: an active cooldown at job start soft-stops the job and requeues
    with next_available_at = cooldown_until (status retrying, not failed)."""
    from trr_backend.socials.instagram import auth_cooldown
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr

    cooldown_until = datetime(2026, 6, 7, 13, 0, 0, tzinfo=UTC)

    class _FakeFetcher:
        runtime_metadata = {"request_count": 0}

        def __init__(self, **_k) -> None:
            pass

        async def warmup(self, _a: str) -> None:  # pragma: no cover - never reached
            raise AssertionError("warmup must not run while a cooldown is active")

        async def aclose(self) -> None:
            return None

    captured: dict[str, Any] = {}
    _patch_repo(monkeypatch, captured)
    _patch_session_and_proxy(monkeypatch)
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", _FakeFetcher)
    monkeypatch.setattr(
        auth_cooldown,
        "get_active_cooldown",
        lambda *_a, **_k: SimpleNamespace(
            cooldown_until=cooldown_until,
            consecutive_auth_failures=2,
            last_error_code="http_403",
            blocker_kind="auth",
            to_metadata=lambda: {"blocker_kind": "auth", "cooldown_until": cooldown_until.isoformat()},
        ),
    )

    job = {"id": "job-1", "run_id": "run-1", "config": {"account": "thetraitorsus", "stage": "posts_scrapling"}}
    jr.run_instagram_posts_scrapling_job(job, worker_id="w1")

    assert captured["status"] == "retrying"
    assert captured["metadata"]["auth_cooldown_active"] is True
    assert captured["next_available_at"] == cooldown_until


def test_dispatch_guard_defers_spawn_while_cooldown_active(monkeypatch: pytest.MonkeyPatch) -> None:
    """A4.5: _enqueue_shared_posts_job returns a deferred-by-cooldown result (no
    spawn) when a cooldown is active for the account."""
    from trr_backend.socials import social_season_analytics_impl as impl

    cooldown = {"blocker_kind": "auth", "cooldown_until": "2026-06-07T13:00:00+00:00"}
    monkeypatch.setattr(
        impl,
        "_active_posts_auth_cooldown",
        lambda _platform, _account: cooldown,
    )
    created: list[Any] = []
    monkeypatch.setattr(impl, "_create_job", lambda *a, **k: created.append((a, k)) or "job-x")

    result = impl._enqueue_shared_posts_job(
        run_id="run-1",
        platform="instagram",
        source_scope="network",
        account_handle="thetraitorsus",
        shared_account_source_id=None,
        pipeline_ingest_mode="catalog_backfill",
        runner_count=1,
    )

    # _enqueue_shared_posts_job now returns a _SharedPostsEnqueueResult dataclass
    # instead of a plain job-id string. A deferred spawn is the cooldown branch.
    assert isinstance(result, impl._SharedPostsEnqueueResult)
    assert result.status == "deferred_by_cooldown"
    assert result.queued is False
    assert result.job_id is None
    assert result.cooldown == cooldown
    assert created == []  # spawn was deferred
