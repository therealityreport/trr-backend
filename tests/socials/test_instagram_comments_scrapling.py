"""Proxy selection and session tests for the Instagram comments Scrapling lane."""

from __future__ import annotations

import hashlib
import inspect
from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from trr_backend.socials.instagram.comments_scrapling.fetcher import (
    InstagramCommentsFetchResult,
    InstagramCommentsScraplingFetcher,
)
from trr_backend.socials.instagram.comments_scrapling.job_runner import (
    run_instagram_comments_scrapling_job,
)
from trr_backend.socials.instagram.comments_scrapling.proxy import select_comments_proxy
from trr_backend.socials.instagram.comments_scrapling.session import resolve_comments_scrapling_session
from trr_backend.socials.instagram.scraper import InstagramComment


def test_select_comments_proxy_prefers_explicit_proxy_urls(monkeypatch) -> None:
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS",
        "http://proxy-one:8000, http://proxy-two:9000\nhttp://proxy-three:7000",
    )
    monkeypatch.setenv("DECODO_USERNAME", "ignored-user")
    monkeypatch.setenv("DECODO_PASSWORD", "ignored-pass")

    config = select_comments_proxy()
    assert config is not None
    # Explicit URLs: browser_proxy is a str (first URL), api_proxy_url is the same.
    assert config.browser_proxy == "http://proxy-one:8000"
    assert config.api_proxy_url == "http://proxy-one:8000"
    assert config.proxy_rotator is not None


def test_comments_gap_defaults_require_exact_coverage(monkeypatch) -> None:
    from trr_backend.socials.instagram.comments_scrapling.fetcher import _hidden_unavailable_gap_is_tolerable
    from trr_backend.socials.instagram.comments_scrapling.job_runner import _reported_count_gap_is_tolerable

    for name in (
        "SOCIAL_INSTAGRAM_COMMENTS_RECONCILABLE_GAP_MAX",
        "SOCIAL_INSTAGRAM_COMMENTS_RECONCILABLE_GAP_RATIO",
        "SOCIAL_INSTAGRAM_COMMENTS_HIDDEN_UNAVAILABLE_GAP_MAX",
        "SOCIAL_INSTAGRAM_COMMENTS_HIDDEN_UNAVAILABLE_GAP_RATIO",
    ):
        monkeypatch.delenv(name, raising=False)

    assert _hidden_unavailable_gap_is_tolerable(unresolved_gap=0, target_count=100)
    assert not _hidden_unavailable_gap_is_tolerable(unresolved_gap=1, target_count=100)
    assert _reported_count_gap_is_tolerable(unresolved_gap=0, target_count=100)
    assert not _reported_count_gap_is_tolerable(unresolved_gap=1, target_count=100)


def test_select_comments_proxy_decodo_browser_proxy_is_dict_with_raw_password(monkeypatch) -> None:
    """Decodo browser proxy must be a dict with un-encoded password so
    Scrapling's ProxyRotator passes it directly to Patchright without
    going through construct_proxy_dict() (which breaks on %3D).
    """
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "spq99jlvis")
    monkeypatch.setenv("DECODO_PASSWORD", "z1Snjx5L3xT2ektx=B")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy()
    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert config.browser_proxy["password"] == "z1Snjx5L3xT2ektx=B"  # literal =, no %3D
    assert config.browser_proxy["server"] == "http://gate.decodo.com:7000"
    assert config.browser_proxy["username"].startswith("spq99jlvis")


def test_select_comments_proxy_api_url_has_encoded_credentials(monkeypatch) -> None:
    """httpx needs URL-encoded credentials. The = in the password
    must be %3D in the api_proxy_url."""
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "spq99jlvis")
    monkeypatch.setenv("DECODO_PASSWORD", "z1Snjx5L3xT2ektx=B")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy()
    assert config is not None
    assert config.api_proxy_url is not None
    assert "%3D" in config.api_proxy_url  # = is encoded
    assert "z1Snjx5L3xT2ektx=B" not in config.api_proxy_url  # raw = is NOT in URL


def test_select_comments_proxy_same_upstream_for_both(monkeypatch) -> None:
    """Both browser_proxy and api_proxy_url must point to the same upstream
    so the IP stays consistent across browser warmup and httpx API calls."""
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "user")
    monkeypatch.setenv("DECODO_PASSWORD", "pass")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy()
    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert config.api_proxy_url is not None
    assert "gate.decodo.com:7000" in config.browser_proxy["server"]
    assert "gate.decodo.com:7000" in config.api_proxy_url


def test_select_comments_proxy_returns_none_when_no_proxy_configured(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "none")
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)

    assert select_comments_proxy() is None


def test_select_comments_proxy_adds_decodo_sticky_session_and_duration(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY", "false")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS", "600")
    monkeypatch.setenv("DECODO_USERNAME", "user-username")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy(session_key="bravotv")

    expected_token = hashlib.sha256(b"bravotv").hexdigest()[:16]
    expected_suffix = f"-session-{expected_token}-sessionduration-10"
    expected_egress_token = hashlib.sha256(b"bravotv").hexdigest()[:12]

    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert config.api_proxy_url is not None
    assert config.browser_proxy["username"] == f"user-username{expected_suffix}"
    assert expected_suffix in config.api_proxy_url
    assert config.fingerprint == f"gate.decodo.com:7000:decodo:{expected_egress_token}"


def test_select_comments_proxy_defaults_to_base_decodo_credentials(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SHARD_SESSIONS", "1")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS", "600")
    monkeypatch.setenv("DECODO_USERNAME", "user-username")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy(session_key="thetraitorsus:comments:3")
    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert config.browser_proxy["username"] == "user-username"
    assert config.api_proxy_url is not None
    assert "session-" not in config.api_proxy_url
    assert config.session_mode == "rotating"


def test_select_comments_proxy_ignores_sticky_env_for_explicit_proxy_urls(monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", "http://user:pass@proxy-one:8000")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS", "1800")
    monkeypatch.setenv("DECODO_USERNAME", "ignored-user")
    monkeypatch.setenv("DECODO_PASSWORD", "ignored-pass")

    config = select_comments_proxy(session_key="bravotv")

    assert config is not None
    assert config.browser_proxy == "http://user:pass@proxy-one:8000"
    assert config.api_proxy_url == "http://user:pass@proxy-one:8000"
    assert config.fingerprint == "proxy-one:8000:explicit"


def test_select_comments_proxy_clamps_invalid_or_large_ttl_to_supported_minutes(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY", "false")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS", "999999")
    monkeypatch.setenv("DECODO_USERNAME", "user-username")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy(session_key="bravotv")

    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert config.api_proxy_url is not None
    assert "-sessionduration-1440" in config.browser_proxy["username"]
    assert "-sessionduration-1440" in config.api_proxy_url


def test_select_comments_proxy_fingerprint_stays_log_safe_under_sticky_mode(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY", "false")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS", "600")
    monkeypatch.setenv("DECODO_USERNAME", "user-username")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy(session_key="bravotv")

    assert config is not None
    assert config.fingerprint.startswith("gate.decodo.com:7000:decodo:")
    assert "session-" not in config.fingerprint
    assert "user-username" not in config.fingerprint
    assert "secret" not in config.fingerprint


def test_select_comments_proxy_preserves_preconfigured_sticky_username(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY", "false")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS", "600")
    monkeypatch.setenv("DECODO_USERNAME", "user-username-session-fixed123-sessionduration-30")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy(session_key="bravotv")

    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert config.browser_proxy["username"] == "user-username-session-fixed123-sessionduration-30"
    assert config.session_mode == "sticky_preconfigured"


def test_resolve_comments_scrapling_session_reuses_instagram_auth_session(monkeypatch) -> None:
    fake_session = SimpleNamespace(
        cookies={"sessionid": "session-cookie", "csrftoken": "csrf-cookie"},
        browser_account_id="bravotv",
        session_account_id=None,
    )

    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.session.resolve_instagram_comments_auth_session",
        lambda **_kwargs: fake_session,
    )

    session = resolve_comments_scrapling_session(
        browser_account_id="comment-lane",
        caller_context="unit_test",
    )

    assert session.auth_session is fake_session
    assert session.browser_account_id == "bravotv"
    assert session.cookies == [
        {
            "name": "sessionid",
            "value": "session-cookie",
            "domain": ".instagram.com",
            "path": "/",
        },
        {
            "name": "csrftoken",
            "value": "csrf-cookie",
            "domain": ".instagram.com",
            "path": "/",
        },
    ]


def test_comments_fetcher_runtime_metadata_never_exposes_cookie_values(monkeypatch) -> None:
    mock_fetcher_cls = MagicMock()
    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.build_stealthy_fetcher",
        mock_fetcher_cls,
    )

    fetcher = InstagramCommentsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing-cookie"},
        browser_account_id="test",
    )
    fetcher._warmup_cookie_delta = {"sessionid": "new-sensitive-value", "csrftoken": "secret-token"}

    meta = fetcher.runtime_metadata
    serialized = repr(meta)
    assert "new-sensitive-value" not in serialized
    assert "secret-token" not in serialized
    assert meta.get("warmup_cookie_names") == ["csrftoken", "sessionid"]
    assert meta.get("warmup_cookie_count") == 2
    assert "warmup_cookie_delta" not in meta


def test_comments_fetcher_runtime_metadata_never_exposes_sticky_username(monkeypatch) -> None:
    mock_fetcher_cls = MagicMock()
    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.build_stealthy_fetcher",
        mock_fetcher_cls,
    )

    fetcher = InstagramCommentsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing-cookie"},
        browser_account_id="test",
    )
    fetcher._warmup_cookie_delta = {"sessionid": "new-sensitive-value"}
    fetcher._selected_proxy_fingerprint = "gate.decodo.com:7000:decodo"

    meta = fetcher.runtime_metadata
    serialized = repr(meta)

    assert "session-" not in serialized
    assert "user-username" not in serialized
    assert "secret" not in serialized
    assert meta["selected_proxy_fingerprint"] == "gate.decodo.com:7000:decodo"


def test_job_runner_uses_resolved_browser_account_id_as_proxy_session_key(monkeypatch) -> None:
    captured: dict[str, str | None] = {}

    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.resolve_comments_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies=[],
            browser_account_id="shared-auth",
            auth_session=SimpleNamespace(cookies={}, metadata={}),
        ),
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.select_comments_proxy",
        lambda *, session_key=None: captured.setdefault("session_key", session_key) or None,
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.InstagramCommentsScraplingFetcher",
        lambda **_kwargs: SimpleNamespace(
            warmup=AsyncMock(),
            aclose=AsyncMock(),
            runtime_metadata={},
            fetch_comments_for_shortcode=AsyncMock(
                return_value=InstagramCommentsFetchResult(
                    comments=[],
                    fetch_failed=False,
                    auth_failed=False,
                    fetch_reason=None,
                    request_count=0,
                    retryable=False,
                )
            ),
        ),
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.persist_instagram_comments_for_post",
        lambda **_kwargs: SimpleNamespace(
            comments_upserted=0,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        ),
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.db_connection",
        lambda **_kwargs: nullcontext(SimpleNamespace(commit=lambda: None)),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._touch_job_heartbeat",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._emit_job_progress",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._finish_job",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._finalize_run_status",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        lambda *_args, **_kwargs: {},
    )

    run_instagram_comments_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "status": "queued",
            "config": {
                "account": "bravotv",
                "stage": "comments_scrapling",
                "mode": "profile",
                "instagram_scrape_mode": "authenticated",
                "source_scope": "bravo",
                "target_source_ids": ["ABC12345"],
            },
        }
    )

    assert captured["session_key"] == "shared-auth"


def test_instagram_public_first_comments_job_skips_auth_resolver_and_proxy_selector(monkeypatch) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    captured: dict[str, Any] = {}
    monkeypatch.setenv("DECODO_PROXY_URL", "http://user:pass@gate.decodo.com:7000")
    monkeypatch.setenv("DECODO_USERNAME", "user")
    monkeypatch.setenv("DECODO_PASSWORD", "pass")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", "http://user:pass@proxy-one.test:8000")

    class FakeFetcher:
        runtime_metadata = {"request_count": 2}

        def __init__(self, **kwargs):
            captured["fetcher_kwargs"] = kwargs

        async def warmup(self):
            raise AssertionError("public comments mode must not warm up an authenticated browser session")

        async def aclose(self):
            return None

        async def fetch_comments_for_shortcode(self, shortcode: str, **kwargs):
            captured["fetch_shortcode"] = shortcode
            captured["fetch_kwargs"] = kwargs
            return InstagramCommentsFetchResult(
                comments=[
                    InstagramComment(
                        comment_id="comment-1",
                        text="first",
                        username="viewer",
                        user_id="user-1",
                        created_at=0,
                        date_time="",
                        likes=0,
                        is_reply=False,
                        parent_comment_id=None,
                        reply_count=0,
                        phase="ranked",
                    )
                ],
                fetch_failed=False,
                auth_failed=False,
                fetch_reason="public_complete",
                reported_comment_count=1,
                request_count=2,
                retryable=False,
                diagnostic_metadata={
                    "phase_counts": {"ranked": 1},
                    "public_comments": {
                        "classification": "public_complete",
                        "advertised_count": 1,
                        "recovered_count": 1,
                        "coverage_ratio": 1.0,
                        "terminal_reason": "pagination_complete",
                    },
                },
            )

    def fail_auth_resolver(**_kwargs):
        raise AssertionError("public comments mode must not resolve Instagram auth")

    def fail_proxy_selector(**_kwargs):
        raise AssertionError("public comments mode must not select Decodo/proxy")

    finish_payloads: list[dict[str, Any]] = []
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", fail_auth_resolver)
    monkeypatch.setattr(jr, "select_comments_proxy", fail_proxy_selector)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", FakeFetcher)
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {"ABC12345": 1})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_instagram_comments_audit_cursor_resume_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(
        jr,
        "_load_persisted_replies_by_parent",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("public comments mode must not use the authenticated reply-only shortcut")
        ),
    )
    monkeypatch.setattr(
        jr,
        "_load_persisted_top_level_comments_for_reply_retry",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("public comments mode must not use persisted top-level reply retry")
        ),
    )
    monkeypatch.setattr(
        jr,
        "_reply_only_fast_path_reason",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("public comments mode must not enter reply-only fast path")
        ),
    )
    monkeypatch.setattr(jr, "_insert_instagram_post_comments_audit", lambda **_kwargs: None)
    monkeypatch.setattr(
        jr,
        "persist_instagram_comments_for_post",
        lambda **_kwargs: SimpleNamespace(
            post_id="00000000-0000-0000-0000-000000000001",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_inserted=1,
            comments_refreshed=0,
            comments_changed=1,
            stored_parent_comments=1,
            stored_child_replies=0,
            expected_child_replies=0,
            stored_reply_gap_total=0,
            stored_reply_gap_parent_count=0,
            stored_reply_gap_samples=[],
        ),
    )
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(SimpleNamespace(commit=lambda: None)))
    monkeypatch.setattr(
        jr.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {
            "id": "job-1",
            "run_id": "run-1",
            "status": "running",
            "worker_id": "test-worker",
            "claimed_at": object(),
            "metadata": {},
        },
    )
    monkeypatch.setattr(jr.pg, "fetch_all", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        jr,
        "lifecycle",
        SimpleNamespace(
            now_utc=lambda: datetime(2026, 6, 15, tzinfo=UTC),
            format_time=lambda value: value.isoformat() if value else None,
            new_job_progress_state=lambda: {},
            touch_job_heartbeat=lambda *_args, **_kwargs: True,
            emit_job_progress=lambda **_kwargs: None,
            finish_job=lambda *args, **kwargs: finish_payloads.append({"args": args, "kwargs": kwargs}),
            finalize_run_status=lambda *_args, **_kwargs: {},
            metadata_dict=lambda value: dict(value or {}),
            retry_backoff_seconds=lambda _attempt: 0,
        ),
    )

    jr.run_instagram_comments_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "status": "queued",
            "config": {
                "account": "bravotv",
                "stage": "comments_scrapling",
                "mode": "profile",
                "instagram_scrape_mode": "public_first",
                "source_scope": "bravo",
                "target_source_ids": ["ABC12345"],
            },
        },
        worker_id="test-worker",
    )

    assert captured["fetcher_kwargs"]["raw_cookies"] == {}
    assert captured["fetcher_kwargs"]["proxy_config"] is None
    assert captured["fetch_kwargs"]["load_strategy"] == "public_relay"
    assert captured["fetch_kwargs"].get("reply_only") is None
    assert finish_payloads
    metadata = finish_payloads[-1]["kwargs"]["metadata"]
    assert metadata["comments_load_strategy"] == "public_relay"
    assert metadata["comments_strategy"]["auth_state"] == "public"
    assert metadata["comments_strategy"]["proxy_state"] == "none"


def test_comments_scrapling_browser_calls_use_the_shared_locale_constant() -> None:
    from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsScraplingFetcher

    source = inspect.getsource(InstagramCommentsScraplingFetcher)

    assert source.count("locale=SCRAPLING_BROWSER_LOCALE") == 6
