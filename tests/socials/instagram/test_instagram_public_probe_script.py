from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.instagram.public_probe import (
    PublicCommentsStatus,
    PublicDetailStatus,
    PublicModeEnvironmentError,
    PublicPostSummary,
    PublicProbeConfig,
    PublicProbePage,
    run_public_probe,
    validate_public_environment,
)

BLOCKED_ENV = (
    "SOCIAL_INSTAGRAM_COOKIES_JSON",
    "SOCIAL_INSTAGRAM_COOKIES_FILE",
    "INSTAGRAM_COOKIES_JSON",
    "INSTAGRAM_COOKIES_FILE",
    "DECODO_USERNAME",
    "DECODO_PASSWORD",
    "DECODO_GATEWAY",
    "DECODO_PROXY_URL",
    "SOCIAL_INSTAGRAM_PROXY_URLS",
    "SOCIAL_INSTAGRAM_POSTS_PROXY_URLS",
    "SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS",
    "SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER",
    "SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER",
    "SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID",
    "SOCIAL_INSTAGRAM_CHROME_PROFILE",
)


@pytest.fixture(autouse=True)
def clear_blocked_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in BLOCKED_ENV:
        monkeypatch.delenv(name, raising=False)


def test_env_guard_blocks_cookies_decodo_proxy_and_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIES_JSON", "{}")
    monkeypatch.setenv("DECODO_USERNAME", "user")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS", "http://proxy.example")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID", "trr")

    violations = validate_public_environment(strict_public=True, fail_if_cookies=True, fail_if_decodo=True)

    assert "SOCIAL_INSTAGRAM_COOKIES_JSON" in violations
    assert "DECODO_USERNAME" in violations
    assert "SOCIAL_INSTAGRAM_POSTS_PROXY_URLS" in violations
    assert "SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID" in violations
    assert any(item.startswith("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER=decodo") for item in violations)


def test_default_comments_fetcher_treats_non_dict_public_comments_as_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {}

    class FakeCommentsFetcher:
        def __init__(self, **_kwargs: Any) -> None:
            observed["public_only"] = _kwargs["public_only"]

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> SimpleNamespace:
            observed["fetch"] = _kwargs
            return SimpleNamespace(
                diagnostic_metadata={"public_comments": None, "relay_comments": None},
                comments=[],
                fetch_reason="relay unavailable",
            )

        async def aclose(self):
            observed["closed"] = True

    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.InstagramCommentsScraplingFetcher",
        FakeCommentsFetcher,
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.public_probe.InstagramScraper",
        lambda **_kwargs: object(),
    )

    from trr_backend.socials.instagram.public_probe import _default_comments_fetcher

    result = _default_comments_fetcher(PublicPostSummary(shortcode="ABC123", comment_count=3))

    assert result.classification == "public_blocked"
    assert result.recovered_count == 0
    assert result.stop_reason == "relay unavailable"

    assert observed["public_only"] is True and observed["closed"] is True
    assert observed["fetch"]["max_comments"] == 5 and observed["fetch"]["fetch_replies"] is False


def test_probe_raises_before_scraping_when_public_env_is_dirty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DECODO_PROXY_URL", "http://decodo.example")
    called = False

    def fetch_page(_account: str, _cursor: str | None) -> PublicProbePage:
        nonlocal called
        called = True
        raise AssertionError("must not scrape with Decodo env set")

    with pytest.raises(PublicModeEnvironmentError):
        run_public_probe(_config(tmp_path), page_fetcher=fetch_page)

    assert called is False


def test_probe_paginates_persists_state_and_reaches_boundary(tmp_path: Path) -> None:
    pages = [
        PublicProbePage(
            posts=[
                _node("NEW01", "2026-06-01T00:00:00+00:00", comments=5),
                _node("NEW02", "2026-05-01T00:00:00+00:00", comments=0),
            ],
            page_info={"has_next_page": True, "end_cursor": "cursor-2"},
            metadata={"doc_id_used": "doc-1", "retrieval_transport": "requests_enriched"},
            connection_found=True,
        ),
        PublicProbePage(
            posts=[_node("OLD01", "2024-12-31T00:00:00+00:00", comments=2)],
            page_info={"has_next_page": True, "end_cursor": "cursor-3"},
            metadata={"doc_id_used": "doc-1", "retrieval_transport": "requests_enriched"},
            connection_found=True,
        ),
    ]
    calls: list[tuple[str, str | None]] = []

    def fetch_page(account: str, cursor: str | None) -> PublicProbePage:
        calls.append((account, cursor))
        return pages[len(calls) - 1]

    result = run_public_probe(
        _config(tmp_path, max_pages=10),
        page_fetcher=fetch_page,
        detail_fetcher=_detail_success,
        comments_fetcher=_comments_complete,
        clock=lambda: 1_700_000_000,
    )

    assert calls == [("bravotv", None), ("bravotv", "cursor-2")]
    assert result.stop_reason == "historical_boundary_reached"
    assert result.historical_boundary_reached is True
    assert result.pages_recovered == 2
    assert result.unique_posts_recovered == 3
    assert result.post_details == {"attempted": 3, "succeeded": 3, "failed": 0}
    assert result.comments == {"attempted": 2, "complete": 2, "partial": 0, "blocked": 0}
    state_payload = _read_json(tmp_path / "state.json")
    assert state_payload["stop_reason"] == "historical_boundary_reached"
    assert state_payload["cursor"] == "cursor-3"
    output_payload = _read_json(tmp_path / "output.json")
    assert output_payload["auth_state"] == "public"
    assert output_payload["proxy_state"] == "none"
    assert output_payload["decodo_state"] == "none"


def test_probe_zero_max_pages_means_uncapped_until_real_stop(tmp_path: Path) -> None:
    pages = [
        PublicProbePage(
            posts=[_node("NEW01", "2026-06-01T00:00:00+00:00", comments=1)],
            page_info={"has_next_page": True, "end_cursor": "cursor-2"},
            metadata={},
            connection_found=True,
        ),
        PublicProbePage(
            posts=[_node("OLD01", "2024-12-31T00:00:00+00:00", comments=1)],
            page_info={"has_next_page": True, "end_cursor": "cursor-3"},
            metadata={},
            connection_found=True,
        ),
    ]
    calls: list[str | None] = []

    def fetch_page(_account: str, cursor: str | None) -> PublicProbePage:
        calls.append(cursor)
        return pages[len(calls) - 1]

    result = run_public_probe(
        _config(tmp_path, max_pages=0),
        page_fetcher=fetch_page,
        detail_fetcher=_detail_success,
        comments_fetcher=_comments_complete,
    )

    assert calls == [None, "cursor-2"]
    assert result.stop_reason == "historical_boundary_reached"
    assert result.pages_recovered == 2
    assert result.environment["page_cap"] is None


def test_probe_can_continue_after_historical_boundary_into_older_posts(tmp_path: Path) -> None:
    pages = [
        PublicProbePage(
            posts=[_node("NEW01", "2026-06-01T00:00:00+00:00", comments=1)],
            page_info={"has_next_page": True, "end_cursor": "cursor-2"},
            metadata={},
            connection_found=True,
        ),
        PublicProbePage(
            posts=[_node("OLD01", "2024-12-31T00:00:00+00:00", comments=1)],
            page_info={"has_next_page": True, "end_cursor": "cursor-3"},
            metadata={},
            connection_found=True,
        ),
        PublicProbePage(
            posts=[_node("OLDER01", "2023-08-01T00:00:00+00:00", comments=1)],
            page_info={"has_next_page": False, "end_cursor": None},
            metadata={},
            connection_found=True,
        ),
    ]
    calls: list[str | None] = []

    def fetch_page(_account: str, cursor: str | None) -> PublicProbePage:
        calls.append(cursor)
        return pages[len(calls) - 1]

    config = _config(tmp_path, max_pages=10)
    config.continue_after_boundary = True

    result = run_public_probe(
        config,
        page_fetcher=fetch_page,
        detail_fetcher=_detail_success,
        comments_fetcher=_comments_complete,
    )

    assert calls == [None, "cursor-2", "cursor-3"]
    assert result.stop_reason == "account_exhausted"
    assert result.historical_boundary_reached is True
    assert result.continue_after_boundary is True
    assert result.target_year_window_complete is True
    assert result.pages_recovered == 3
    assert result.unique_posts_recovered == 3


def test_probe_reports_2025_and_2026_target_posts(tmp_path: Path) -> None:
    def fetch_page(_account: str, _cursor: str | None) -> PublicProbePage:
        return PublicProbePage(
            posts=[
                _node("POST2026", "2026-06-01T00:00:00+00:00", comments=1),
                _node("POST2025", "2025-04-01T00:00:00+00:00", comments=1),
                _node("POST2024", "2024-12-31T00:00:00+00:00", comments=1),
            ],
            page_info={"has_next_page": False, "end_cursor": None},
            metadata={},
            connection_found=True,
        )

    result = run_public_probe(
        _config(tmp_path),
        page_fetcher=fetch_page,
        detail_fetcher=_detail_success,
        comments_fetcher=_comments_complete,
    )

    assert result.target_years == [2025, 2026]
    assert result.target_posts_recovered == 2
    assert result.target_year_counts == {"2025": 1, "2026": 1}
    assert result.target_year_window_complete is True
    assert [post["shortcode"] for post in result.target_posts] == ["POST2026", "POST2025"]


@pytest.mark.parametrize(
    ("status", "reason", "retry_after"),
    [
        (401, "public_graphql_401_backoff_required", 3600),
        (403, "public_graphql_403_backoff_required", 7200),
        (429, "public_graphql_429_backoff_required", 7200),
    ],
)
def test_probe_maps_public_graphql_blocks_to_exact_backoff_stop_reasons(
    tmp_path: Path,
    status: int,
    reason: str,
    retry_after: int,
) -> None:
    def fetch_page(_account: str, _cursor: str | None) -> PublicProbePage:
        return PublicProbePage(
            posts=[],
            page_info={},
            metadata={
                "retrieval_meta": {
                    "error_status_code": status,
                    "error_code": f"status_{status}",
                }
            },
            connection_found=False,
        )

    result = run_public_probe(
        _config(tmp_path),
        page_fetcher=fetch_page,
        detail_fetcher=_detail_success,
        comments_fetcher=_comments_complete,
    )

    assert result.stop_reason == reason
    assert result.next_retry_after_seconds == retry_after
    assert result.requires_approval is False
    assert _read_json(tmp_path / "state.json")["stop_reason"] == reason


def test_probe_stops_on_repeated_cursor_after_progress(tmp_path: Path) -> None:
    def fetch_page(_account: str, _cursor: str | None) -> PublicProbePage:
        return PublicProbePage(
            posts=[_node("NEW01", "2026-06-01T00:00:00+00:00", comments=1)],
            page_info={"has_next_page": True, "end_cursor": "same-cursor"},
            metadata={},
            connection_found=True,
        )

    config = _config(tmp_path)
    state_path = tmp_path / "state.json"
    state_path.write_text(
        '{"account":"bravotv","cursor":"same-cursor","seen_cursors":["same-cursor"],'
        '"pages_recovered":0,"unique_shortcodes":[],"posts":[]}',
        encoding="utf-8",
    )
    config.resume = True

    result = run_public_probe(
        config,
        page_fetcher=fetch_page,
        detail_fetcher=_detail_success,
        comments_fetcher=_comments_complete,
    )

    assert result.stop_reason == "public_repeated_cursor_manual_review"


def test_probe_marks_detail_partial_after_successful_pagination(tmp_path: Path) -> None:
    def fetch_page(_account: str, _cursor: str | None) -> PublicProbePage:
        return PublicProbePage(
            posts=[_node("NEW01", "2026-06-01T00:00:00+00:00", comments=0)],
            page_info={"has_next_page": False, "end_cursor": None},
            metadata={},
            connection_found=True,
        )

    result = run_public_probe(
        _config(tmp_path),
        page_fetcher=fetch_page,
        detail_fetcher=lambda shortcode: PublicDetailStatus(
            shortcode=shortcode,
            attempted=True,
            succeeded=False,
            stop_reason="metadata_not_found",
        ),
        comments_fetcher=_comments_complete,
    )

    assert result.stop_reason == "details_partial"
    assert result.post_details == {"attempted": 1, "succeeded": 0, "failed": 1}


def test_probe_marks_comments_partial_after_successful_pagination(tmp_path: Path) -> None:
    def fetch_page(_account: str, _cursor: str | None) -> PublicProbePage:
        return PublicProbePage(
            posts=[_node("NEW01", "2026-06-01T00:00:00+00:00", comments=10)],
            page_info={"has_next_page": False, "end_cursor": None},
            metadata={},
            connection_found=True,
        )

    result = run_public_probe(
        _config(tmp_path),
        page_fetcher=fetch_page,
        detail_fetcher=_detail_success,
        comments_fetcher=lambda post: PublicCommentsStatus(
            shortcode=post.shortcode,
            attempted=True,
            classification="public_partial",
            advertised_count=10,
            recovered_count=4,
            terminal_reason="pagination_stalled",
        ),
    )

    assert result.stop_reason == "comments_partial"
    assert result.comments == {"attempted": 1, "complete": 0, "partial": 1, "blocked": 0}


def _config(tmp_path: Path, *, max_pages: int = 3) -> PublicProbeConfig:
    return PublicProbeConfig(
        account="@BravoTV",
        until_date=date(2025, 1, 1),
        max_pages=max_pages,
        sample_details_per_page=2,
        sample_comments_per_page=1,
        comments_mode="sampled",
        details_mode="sampled",
        state_file=tmp_path / "state.json",
        output=tmp_path / "output.json",
        strict_public=True,
        fail_if_cookies=True,
        fail_if_decodo=True,
        delay_seconds=0,
    )


def _node(shortcode: str, iso_timestamp: str, *, comments: int) -> dict[str, Any]:
    epoch = int(datetime.fromisoformat(iso_timestamp).timestamp())
    return {
        "code": shortcode,
        "pk": f"media-{shortcode}",
        "taken_at": epoch,
        "comment_count": comments,
        "like_count": 12,
        "media_type": 1,
    }


def _detail_success(shortcode: str) -> PublicDetailStatus:
    return PublicDetailStatus(
        shortcode=shortcode,
        attempted=True,
        succeeded=True,
        media_type="image",
        media_url_count=1,
        thumbnail_present=True,
        taken_at=datetime.now(UTC).isoformat(),
        caption_present=True,
        source="test",
    )


def _comments_complete(post: PublicPostSummary) -> PublicCommentsStatus:
    return PublicCommentsStatus(
        shortcode=post.shortcode,
        attempted=True,
        classification="public_complete",
        advertised_count=post.comment_count,
        recovered_count=post.comment_count or 0,
        coverage_ratio=1.0,
        terminal_reason="pagination_complete",
        fallback_source="public_relay_comments",
    )


def _read_json(path: Path) -> dict[str, Any]:
    import json

    return json.loads(path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("page_cap", [1, 2])
def test_probe_honors_page_and_sample_caps_including_resume(tmp_path: Path, page_cap: int) -> None:
    page_calls = []
    detail_calls = []
    comment_calls = []

    def page_fetcher(_account, cursor):
        page_calls.append(cursor)
        page_num = len(page_calls)
        return PublicProbePage(
            posts=[_node(f"P{page_num}N{n}", "2026-10-01T00:00:00+00:00", comments=10) for n in range(4)],
            page_info={"has_next_page": True, "end_cursor": f"cursor-{page_num}"},
            metadata={},
            connection_found=True,
        )

    def detail_fetcher(shortcode):
        detail_calls.append(shortcode)
        return _detail_success(shortcode)

    def comments_fetcher(post):
        comment_calls.append(post.shortcode)
        return _comments_complete(post)

    config = replace(_config(tmp_path, max_pages=page_cap), sample_details_per_page=1)
    for resume in [False, True]:
        result = run_public_probe(
            replace(config, resume=resume),
            page_fetcher=page_fetcher,
            detail_fetcher=detail_fetcher,
            comments_fetcher=comments_fetcher,
        )
        multiplier = 2 if resume else 1
        assert len(page_calls) == page_cap * multiplier
        assert len(detail_calls) == page_cap * multiplier
        assert len(comment_calls) == page_cap * multiplier
        assert result.environment["page_cap"] == page_cap
        assert result.stop_reason == "page_cap_reached"
        assert result.next_resume_cursor == f"cursor-{page_cap * multiplier}"


@pytest.mark.parametrize(
    "status,body,reason",
    [
        (200, '<html><a href="/accounts/login/">Log in</a></html>', "redirect_login"),
        (200, "<html>challenge_required</html>", "challenge_required"),
        (200, "", "empty_response"),
        (200, "<html>public shell without embedded media</html>", "metadata_not_found"),
        (401, "unauthorized", "RuntimeError: public_detail_http_401"),
        (403, "forbidden", "RuntimeError: public_detail_http_403"),
        (302, "", "RuntimeError: public_detail_http_302"),
    ],
)
def test_detail_probe_classifies_responses_without_route_retries(monkeypatch, status, body, reason):
    import requests

    from trr_backend.socials.instagram import public_probe as probe

    calls = []
    closed = []

    def factory(**kwargs):
        assert kwargs == {"cookies": {}, "attach_auth_session": False, "strict_public_credentials": True}
        response = requests.Response()
        response.status_code = status
        response._content = body.encode()
        return SimpleNamespace(
            session=SimpleNamespace(close=lambda: closed.append(True)),
            _get=lambda url, **kw: calls.append(url) or response,
            _request_cookies=lambda: {},
        )

    monkeypatch.setattr(probe, "InstagramScraper", factory)
    result = probe._default_detail_fetcher("PUBLIC01")
    assert not result.succeeded and result.stop_reason == reason
    assert result.http_status == status and result.response_bytes == len(body.encode())
    assert len(calls) == 1 and closed == [True]
    assert probe._detail_status_from_state(__import__("dataclasses").asdict(result)) == result


def test_detail_probe_parses_public_shortcode_payload_missing_from_legacy_parser(monkeypatch):
    import json

    import requests

    from trr_backend.socials.instagram import public_probe as probe

    payload = {
        "data": {
            "xdt_shortcode_media": {
                "code": "PUBLIC01",
                "media_type": 2,
                "taken_at": 1700000000,
                "caption": {"text": "Public caption #Bravo @friend"},
                "video_versions": [{"url": "https://cdn.example/video.mp4", "width": 720, "height": 1280}],
                "image_versions2": {
                    "candidates": [{"url": "https://cdn.example/cover.jpg", "width": 720, "height": 1280}]
                },
            }
        }
    }
    body = '<script type="application/json">' + json.dumps(payload) + "</script>"
    response = requests.Response()
    response.status_code = 200
    response._content = body.encode()
    calls = []
    monkeypatch.setattr(
        probe,
        "InstagramScraper",
        lambda **kwargs: SimpleNamespace(
            session=SimpleNamespace(close=lambda: None),
            _get=lambda url, **kw: calls.append(url) or response,
            _request_cookies=lambda: {},
        ),
    )
    result = probe._default_detail_fetcher("PUBLIC01")
    assert result.succeeded and result.caption_present and result.thumbnail_present
    assert result.media_url_count == 1 and result.taken_at == "2023-11-14T22:13:20+00:00"
    assert result.http_status == 200 and len(calls) == 1


def test_detail_probe_reports_an_empty_parsed_shell_as_failure(monkeypatch):
    """Logged-out permalinks can parse to a shortcode-only shell; that is not a detail."""
    import json

    import requests

    from trr_backend.socials.instagram import public_probe as probe

    payload = {"data": {"xdt_shortcode_media": {"code": "PUBLIC01"}}}
    response = requests.Response()
    response.status_code = 200
    response._content = ('<script type="application/json">' + json.dumps(payload) + "</script>").encode()
    monkeypatch.setattr(
        probe,
        "InstagramScraper",
        lambda **kwargs: SimpleNamespace(
            session=SimpleNamespace(close=lambda: None),
            _get=lambda url, **kw: response,
            _request_cookies=lambda: {},
        ),
    )
    result = probe._default_detail_fetcher("PUBLIC01")
    assert not result.succeeded and result.stop_reason == "metadata_empty"
    assert not result.caption_present and result.media_url_count == 0
    assert probe._detail_status_from_state(__import__("dataclasses").asdict(result)) == result
