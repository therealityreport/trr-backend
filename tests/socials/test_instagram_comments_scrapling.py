"""Proxy selection and session tests for the Instagram comments Scrapling lane."""

from __future__ import annotations

import hashlib
from contextlib import nullcontext
from types import SimpleNamespace
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
    assert config.browser_proxy["username"] == "spq99jlvis"


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
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS", "600")
    monkeypatch.setenv("DECODO_USERNAME", "user-username")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy(session_key="bravotv")

    expected_token = hashlib.sha256(b"bravotv").hexdigest()[:16]
    expected_suffix = f"-session-{expected_token}-sessionduration-10"

    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert config.browser_proxy["username"] == f"user-username{expected_suffix}"
    assert expected_suffix in config.api_proxy_url
    assert config.fingerprint == "gate.decodo.com:7000:decodo"


def test_select_comments_proxy_shard_sessions_do_not_force_sticky_affinity(monkeypatch) -> None:
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
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS", "999999")
    monkeypatch.setenv("DECODO_USERNAME", "user-username")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy(session_key="bravotv")

    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert "-sessionduration-1440" in config.browser_proxy["username"]
    assert "-sessionduration-1440" in config.api_proxy_url


def test_select_comments_proxy_fingerprint_stays_log_safe_under_sticky_mode(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS", "600")
    monkeypatch.setenv("DECODO_USERNAME", "user-username")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy(session_key="bravotv")

    assert config is not None
    assert config.fingerprint == "gate.decodo.com:7000:decodo"
    assert "session-" not in config.fingerprint
    assert "user-username" not in config.fingerprint
    assert "secret" not in config.fingerprint


def test_select_comments_proxy_preserves_preconfigured_sticky_username(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
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
    )

    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.session.resolve_instagram_auth_session",
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
    mock_module = MagicMock()
    mock_module.StealthyFetcher = mock_fetcher_cls
    mock_module.ProxyRotator = MagicMock()
    monkeypatch.setitem(__import__("sys").modules, "scrapling.fetchers", mock_module)

    from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsScraplingFetcher

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
    mock_module = MagicMock()
    mock_module.StealthyFetcher = mock_fetcher_cls
    mock_module.ProxyRotator = MagicMock()
    monkeypatch.setitem(__import__("sys").modules, "scrapling.fetchers", mock_module)

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
                "source_scope": "bravo",
                "target_source_ids": ["ABC12345"],
            },
        }
    )

    assert captured["session_key"] == "shared-auth"
