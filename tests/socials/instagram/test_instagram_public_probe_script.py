from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
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
        taken_at=datetime.now(timezone.utc).isoformat(),
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
