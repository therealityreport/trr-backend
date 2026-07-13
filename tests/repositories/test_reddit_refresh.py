from __future__ import annotations

import threading
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta

import pytest

from trr_backend.repositories import reddit_refresh


def _listing_row(
    *,
    post_id: str,
    created_at: datetime,
    flair: str = "Salt Lake City",
    title: str = "SLC test post",
    selftext: str = "body",
) -> dict:
    return {
        "reddit_post_id": post_id,
        "title": title,
        "selftext": selftext,
        "url": f"https://reddit.com/{post_id}",
        "permalink": f"https://www.reddit.com/r/test/comments/{post_id}",
        "author": "user1",
        "score": 10,
        "num_comments": 3,
        "posted_at": created_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "link_flair_text": flair,
        "source_sorts": ["new"],
        "raw_payload": {"id": post_id},
    }


def test_sanitize_reddit_media_url_decodes_html_and_strips_anchor_tail() -> None:
    raw_url = "https://preview.redd.it/example.jpeg?width=321&amp;format=pjpg&amp;auto=webp&amp;s=abc123</a"

    assert reddit_refresh._sanitize_reddit_media_url(raw_url) == (  # noqa: SLF001
        "https://preview.redd.it/example.jpeg?width=321&format=pjpg&auto=webp&s=abc123"
    )


def test_extract_reddit_media_urls_uses_clean_href_values_without_tag_tail() -> None:
    body_html = (
        '<a href="https://preview.redd.it/example.jpeg?width=321&amp;format=pjpg'
        '&amp;auto=webp&amp;s=abc123">https://preview.redd.it/example.jpeg?width=321'
        "&amp;format=pjpg&amp;auto=webp&amp;s=abc123</a>"
    )

    assert reddit_refresh._extract_reddit_media_urls(body_html) == [  # noqa: SLF001
        ("https://preview.redd.it/example.jpeg?width=321&format=pjpg&auto=webp&s=abc123", "image")
    ]


def test_reddit_http_client_reuses_cached_oauth_token(monkeypatch) -> None:
    monkeypatch.setenv("REDDIT_CLIENT_ID", "client-id")
    monkeypatch.setenv("REDDIT_CLIENT_SECRET", "client-secret")

    class TokenResponse:
        status_code = 200
        content = b"{}"

        def json(self) -> dict[str, object]:
            return {"access_token": "cached-access-token", "expires_in": 3600}

    class TokenSession:
        def __init__(self) -> None:
            self.post_calls = 0

        def post(self, *_args, **_kwargs) -> TokenResponse:  # noqa: ANN002, ANN003
            self.post_calls += 1
            return TokenResponse()

    client = reddit_refresh.RedditHttpClient()
    session = TokenSession()
    client.session = session

    assert client._get_oauth_token() == "cached-access-token"  # noqa: SLF001
    assert client._get_oauth_token() == "cached-access-token"  # noqa: SLF001
    assert session.post_calls == 1


def test_reddit_http_client_coalesces_concurrent_cold_oauth_refreshes(monkeypatch) -> None:
    monkeypatch.setenv("REDDIT_CLIENT_ID", "client-id")
    monkeypatch.setenv("REDDIT_CLIENT_SECRET", "client-secret")

    class TokenResponse:
        status_code = 200
        content = b"{}"

        def json(self) -> dict[str, object]:
            return {"access_token": "shared-access-token", "expires_in": 3600}

    class TokenSession:
        def __init__(self) -> None:
            self.post_calls = 0
            self.post_started = threading.Event()
            self.release_post = threading.Event()

        def post(self, *_args, **_kwargs) -> TokenResponse:  # noqa: ANN002, ANN003
            self.post_calls += 1
            self.post_started.set()
            assert self.release_post.wait(timeout=5)
            return TokenResponse()

    client = reddit_refresh.RedditHttpClient()
    session = TokenSession()
    client.session = session
    caller_count = 12
    start = threading.Barrier(caller_count + 1)
    tokens: list[str | None] = []

    def fetch_token() -> None:
        start.wait(timeout=5)
        tokens.append(client._get_oauth_token())  # noqa: SLF001

    threads = [threading.Thread(target=fetch_token) for _ in range(caller_count)]
    for thread in threads:
        thread.start()
    start.wait(timeout=5)
    assert session.post_started.wait(timeout=5)
    session.release_post.set()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()

    assert tokens == ["shared-access-token"] * caller_count
    assert session.post_calls == 1


def test_reddit_http_client_cooldown_lock_concurrency_smoke() -> None:
    client = reddit_refresh.RedditHttpClient()
    client._adaptive_cooldown = 0.05  # noqa: SLF001
    errors: list[Exception] = []

    def hammer_cooldown() -> None:
        try:
            for _ in range(50):
                with client._state_lock:  # noqa: SLF001
                    client._adaptive_cooldown = min(  # noqa: SLF001
                        client._adaptive_cooldown_max,  # noqa: SLF001
                        client._adaptive_cooldown * 2,  # noqa: SLF001
                    )
                with client._state_lock:  # noqa: SLF001
                    client._adaptive_cooldown = max(  # noqa: SLF001
                        client._adaptive_cooldown_min,  # noqa: SLF001
                        client._adaptive_cooldown * 0.9,  # noqa: SLF001
                    )
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=hammer_cooldown) for _ in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    with client._state_lock:  # noqa: SLF001
        assert client._adaptive_cooldown_min <= client._adaptive_cooldown <= client._adaptive_cooldown_max  # noqa: SLF001


def test_build_reddit_refresh_save_proof_counts_saved_rows(monkeypatch) -> None:
    def _fake_fetch_one(query, params=None, **_kwargs):  # noqa: ANN001
        del params
        normalized = " ".join(str(query).split()).lower()
        if "from social.reddit_refresh_runs" in normalized:
            return {
                "id": "63a7be5d-0000-4000-8000-000000000001",
                "community_id": "community-1",
                "season_id": "season-1",
                "period_key": "bravo-smoke",
                "subreddit": "bravorealhousewives",
                "status": "completed",
                "total_rows": 7,
                "matched_rows": 3,
                "tracked_flair_rows": 2,
                "diagnostics": {"result": {"totals": {"fetched_rows": 8, "matched_rows": 4}}},
            }
        if "count(distinct m.post_id)" in normalized:
            return {"total": 4}
        if "from social.reddit_period_post_matches" in normalized:
            return {"total": 5}
        raise AssertionError(normalized)

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", _fake_fetch_one)

    proof = reddit_refresh.build_reddit_refresh_save_proof("63a7be5d-0000-4000-8000-000000000001")

    assert proof["fetched_count"] == 8
    assert proof["upserted_count"] == 4
    assert proof["materialized_count"] == 5
    assert proof["verified"] is True


def test_fetch_new_window_exhaustive_terminal_page_before_period_start_is_complete_when_exhausted(monkeypatch) -> None:
    newer_post_time = datetime(2025, 9, 10, 12, 0, tzinfo=UTC)
    period_start = datetime(2025, 8, 14, 0, 0, tzinfo=UTC)

    payload = {
        "data": {
            "children": [
                {
                    "data": {
                        "id": "abc123",
                        "title": "SLC trailer",
                        "selftext": "",
                        "url": "https://reddit.com/r/test/comments/abc123",
                        "permalink": "/r/test/comments/abc123",
                        "author": "poster",
                        "score": 5,
                        "num_comments": 1,
                        "created_utc": newer_post_time.timestamp(),
                        "link_flair_text": "Salt Lake City",
                    }
                }
            ],
            "after": None,
        }
    }

    monkeypatch.setattr(reddit_refresh._HTTP_CLIENT, "get_json", lambda path, params: payload)  # noqa: ARG005, SLF001

    rows, pages, complete = reddit_refresh._fetch_new_window_exhaustive(  # noqa: SLF001
        subreddit="bravorealhousewives",
        period_start=period_start,
        period_end=datetime(2025, 9, 16, 23, 0, tzinfo=UTC),
        max_pages=10,
    )

    assert pages == 1
    assert len(rows) == 1
    assert complete is True


def test_fetch_new_window_exhaustive_remains_incomplete_when_page_cap_is_hit(monkeypatch) -> None:
    newer_post_time = datetime(2025, 9, 10, 12, 0, tzinfo=UTC)
    period_start = datetime(2025, 8, 14, 0, 0, tzinfo=UTC)

    payload = {
        "data": {
            "children": [
                {
                    "data": {
                        "id": "abc124",
                        "title": "SLC recap",
                        "selftext": "",
                        "url": "https://reddit.com/r/test/comments/abc124",
                        "permalink": "/r/test/comments/abc124",
                        "author": "poster",
                        "score": 5,
                        "num_comments": 1,
                        "created_utc": newer_post_time.timestamp(),
                        "link_flair_text": "Salt Lake City",
                    }
                }
            ],
            "after": "t3_more",
        }
    }

    monkeypatch.setattr(reddit_refresh._HTTP_CLIENT, "get_json", lambda path, params: payload)  # noqa: ARG005, SLF001

    rows, pages, complete = reddit_refresh._fetch_new_window_exhaustive(  # noqa: SLF001
        subreddit="bravorealhousewives",
        period_start=period_start,
        period_end=datetime(2025, 9, 16, 23, 0, tzinfo=UTC),
        max_pages=1,
    )

    assert pages == 1
    assert len(rows) == 1
    assert complete is False


def test_fetch_new_window_exhaustive_keeps_scanning_past_too_new_pages(monkeypatch) -> None:
    period_start = datetime(2025, 1, 10, 0, 0, tzinfo=UTC)
    period_end = datetime(2025, 1, 20, 23, 59, tzinfo=UTC)

    def _payload(post_id: str, created_at: datetime, after: str | None) -> dict:
        return {
            "data": {
                "children": [
                    {
                        "data": {
                            "id": post_id,
                            "title": "SLC recap",
                            "selftext": "",
                            "url": f"https://reddit.com/r/test/comments/{post_id}",
                            "permalink": f"/r/test/comments/{post_id}",
                            "author": "poster",
                            "score": 5,
                            "num_comments": 1,
                            "created_utc": created_at.timestamp(),
                            "link_flair_text": "Salt Lake City",
                        }
                    }
                ],
                "after": after,
            }
        }

    pages = [
        _payload("too-new-1", datetime(2025, 1, 30, 12, 0, tzinfo=UTC), "page-2"),
        _payload("too-new-2", datetime(2025, 1, 29, 12, 0, tzinfo=UTC), "page-3"),
        _payload("too-new-3", datetime(2025, 1, 28, 12, 0, tzinfo=UTC), "page-4"),
        _payload("in-window", datetime(2025, 1, 15, 12, 0, tzinfo=UTC), None),
    ]
    calls: list[dict] = []

    def fake_get_json(path, params):  # noqa: ANN001, ARG001
        calls.append(dict(params))
        return pages[len(calls) - 1]

    monkeypatch.setattr(reddit_refresh._HTTP_CLIENT, "get_json", fake_get_json)  # noqa: SLF001

    rows, pages_fetched, complete = reddit_refresh._fetch_new_window_exhaustive(  # noqa: SLF001
        subreddit="bravorealhousewives",
        period_start=period_start,
        period_end=period_end,
        max_pages=4,
    )

    assert pages_fetched == 4
    assert complete is True
    assert calls == [
        {"limit": 100, "raw_json": 1},
        {"limit": 100, "raw_json": 1, "after": "page-2"},
        {"limit": 100, "raw_json": 1, "after": "page-3"},
        {"limit": 100, "raw_json": 1, "after": "page-4"},
    ]
    assert [row["reddit_post_id"] for row in rows] == ["too-new-1", "too-new-2", "too-new-3", "in-window"]


def test_fetch_sample_sorts_raises_when_all_sorts_fail(monkeypatch) -> None:
    def fake_get_json(path, params):  # noqa: ANN001, ARG001
        raise reddit_refresh.RedditRefreshError("Reddit request failed (403)", status=403)

    monkeypatch.setattr(reddit_refresh._HTTP_CLIENT, "get_json", fake_get_json)  # noqa: SLF001

    with pytest.raises(reddit_refresh.RedditRefreshError, match=r"Reddit request failed \(403\)") as exc_info:
        reddit_refresh._fetch_sample_sorts(  # noqa: SLF001
            subreddit="bravorealhousewives",
            sort_modes=["new", "hot", "top"],
            limit_per_mode=25,
        )

    assert exc_info.value.status == 403


def test_discover_window_search_backfill_recovers_historical_post(monkeypatch) -> None:
    recovered = _listing_row(post_id="hist001", created_at=datetime(2025, 8, 20, 12, 0, tzinfo=UTC))

    monkeypatch.setattr(
        reddit_refresh,
        "_fetch_new_window_exhaustive",
        lambda subreddit, period_start, period_end, max_pages: ([], 1, False),  # noqa: ARG005
    )
    monkeypatch.setattr(
        reddit_refresh,
        "_fetch_search_backfill",
        lambda **kwargs: (  # noqa: ARG005
            [recovered],
            {
                "enabled": True,
                "queries_run": 1,
                "pages_fetched": 1,
                "rows_fetched": 1,
                "rows_in_window": 1,
                "complete": True,
                "query_diagnostics": [],
            },
        ),
    )

    payload = {
        "subreddit": "bravorealhousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": [],
        "is_show_focused": True,
        "analysis_flairs": ["Salt Lake City"],
        "analysis_all_flairs": ["Salt Lake City"],
        "force_include_flairs": ["Salt Lake City"],
        "sort_modes": ["new"],
        "period_start": "2025-08-14T00:00:00Z",
        "period_end": "2025-09-16T23:00:00Z",
        "exhaustive_window": True,
        "search_backfill": True,
        "max_pages": 500,
    }

    result = reddit_refresh._discover_window(payload)  # noqa: SLF001

    assert result["totals"]["fetched_rows"] == 1
    assert result["totals"]["matched_rows"] == 1
    assert result["threads"][0]["reddit_post_id"] == "hist001"


def test_discover_window_backfill_diagnostics_conservative_completion(monkeypatch) -> None:
    monkeypatch.setattr(
        reddit_refresh,
        "_fetch_new_window_exhaustive",
        lambda subreddit, period_start, period_end, max_pages: ([], 3, False),  # noqa: ARG005
    )
    monkeypatch.setattr(
        reddit_refresh,
        "_fetch_search_backfill",
        lambda **kwargs: (  # noqa: ARG005
            [],
            {
                "enabled": True,
                "queries_run": 2,
                "pages_fetched": 5,
                "rows_fetched": 0,
                "rows_in_window": 0,
                "complete": False,
                "query_diagnostics": [
                    {
                        "flair": "Salt Lake City",
                        "query": 'flair:"Salt Lake City"',
                        "pages_fetched": 3,
                        "rows_fetched": 0,
                        "rows_in_window": 0,
                        "reached_period_start": False,
                        "exhausted_results": False,
                        "complete": False,
                    }
                ],
            },
        ),
    )

    payload = {
        "subreddit": "bravorealhousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": [],
        "is_show_focused": True,
        "analysis_flairs": ["Salt Lake City"],
        "analysis_all_flairs": ["Salt Lake City"],
        "force_include_flairs": ["Salt Lake City"],
        "period_start": "2025-08-14T00:00:00Z",
        "period_end": "2025-09-16T23:00:00Z",
        "exhaustive_window": True,
        "search_backfill": True,
        "max_pages": 500,
    }

    result = reddit_refresh._discover_window(payload)  # noqa: SLF001

    assert result["window_exhaustive_complete"] is False
    assert result["search_backfill"]["complete"] is False
    assert result["search_backfill"]["queries_run"] == 2


def test_fetch_search_backfill_does_not_stop_early_when_period_start_reached(monkeypatch) -> None:
    period_start = datetime(2025, 8, 14, 0, 0, tzinfo=UTC)
    period_end = datetime(2025, 9, 16, 23, 0, tzinfo=UTC)

    def _child(post_id: str, created_at: datetime, flair: str = "Salt Lake City") -> dict:
        return {
            "data": {
                "id": post_id,
                "title": "SLC post",
                "selftext": "",
                "url": f"https://reddit.com/r/test/comments/{post_id}",
                "permalink": f"/r/test/comments/{post_id}",
                "author": "poster",
                "score": 1,
                "num_comments": 1,
                "created_utc": created_at.timestamp(),
                "link_flair_text": flair,
            }
        }

    responses = iter(
        [
            {
                "data": {
                    "children": [_child("old001", datetime(2025, 8, 13, 23, 0, tzinfo=UTC))],
                    "after": "t3_token",
                }
            },
            {
                "data": {
                    "children": [_child("new002", datetime(2025, 8, 20, 12, 0, tzinfo=UTC))],
                    "after": None,
                }
            },
        ]
    )

    monkeypatch.setattr(  # noqa: SLF001
        reddit_refresh._HTTP_CLIENT,
        "get_json",
        lambda path, params: next(responses),  # noqa: ARG005
    )

    rows, diagnostics = reddit_refresh._fetch_search_backfill(  # noqa: SLF001
        subreddit="bravorealhousewives",
        tracked_flairs=["Salt Lake City"],
        show_aliases=[],
        show_terms=[],
        period_start=period_start,
        period_end=period_end,
        max_pages_per_query=2,
        max_total_queries=1,
    )

    row_ids = {row["reddit_post_id"] for row in rows}
    assert "new002" in row_ids
    assert diagnostics["pages_fetched"] == 2
    assert diagnostics["rows_in_window"] == 1


def test_fetch_search_backfill_marks_incomplete_when_page_cap_hit(monkeypatch) -> None:
    period_start = datetime(2025, 8, 14, 0, 0, tzinfo=UTC)
    period_end = datetime(2025, 9, 16, 23, 0, tzinfo=UTC)

    responses = iter(
        [
            {
                "data": {
                    "children": [
                        {
                            "data": {
                                "id": "new001",
                                "title": "SLC post",
                                "selftext": "",
                                "url": "https://reddit.com/r/test/comments/new001",
                                "permalink": "/r/test/comments/new001",
                                "author": "poster",
                                "score": 1,
                                "num_comments": 1,
                                "created_utc": datetime(2025, 8, 20, 12, 0, tzinfo=UTC).timestamp(),
                                "link_flair_text": "Salt Lake City",
                            }
                        }
                    ],
                    "after": "t3_more",
                }
            }
        ]
    )

    monkeypatch.setattr(
        reddit_refresh._HTTP_CLIENT,
        "get_json",
        lambda path, params: next(responses),  # noqa: ARG005
    )

    _rows, diagnostics = reddit_refresh._fetch_search_backfill(  # noqa: SLF001
        subreddit="bravorealhousewives",
        tracked_flairs=["Salt Lake City"],
        show_aliases=[],
        show_terms=[],
        period_start=period_start,
        period_end=period_end,
        max_pages_per_query=1,
        max_total_queries=1,
    )

    assert diagnostics["complete"] is False
    assert diagnostics["query_diagnostics"][0]["exhausted_results"] is False


def test_fetch_search_backfill_optional_queries_do_not_block_required_completeness(monkeypatch) -> None:
    period_start = datetime(2025, 8, 14, 0, 0, tzinfo=UTC)
    period_end = datetime(2025, 9, 16, 23, 0, tzinfo=UTC)

    def _child(post_id: str, created_at: datetime, flair: str = "Salt Lake City") -> dict:
        return {
            "data": {
                "id": post_id,
                "title": "SLC post",
                "selftext": "",
                "url": f"https://reddit.com/r/test/comments/{post_id}",
                "permalink": f"/r/test/comments/{post_id}",
                "author": "poster",
                "score": 1,
                "num_comments": 1,
                "created_utc": created_at.timestamp(),
                "link_flair_text": flair,
            }
        }

    def fake_get_json(path: str, params: dict) -> dict:  # noqa: ANN001
        query = str(params.get("q") or "")
        if query.startswith('flair:"'):
            return {
                "data": {
                    "children": [_child("exact001", datetime(2025, 8, 20, 12, 0, tzinfo=UTC))],
                    "after": None,
                }
            }
        if query == '"Salt Lake City"':
            return {
                "data": {
                    "children": [_child("phrase001", datetime(2025, 8, 20, 13, 0, tzinfo=UTC))],
                    "after": "t3_more",
                }
            }
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(reddit_refresh._HTTP_CLIENT, "get_json", fake_get_json)  # noqa: SLF001

    _rows, diagnostics = reddit_refresh._fetch_search_backfill(  # noqa: SLF001
        subreddit="bravorealhousewives",
        tracked_flairs=["Salt Lake City"],
        show_aliases=[],
        show_terms=[],
        period_start=period_start,
        period_end=period_end,
        max_pages_per_query=1,
        max_total_queries=2,
    )

    assert diagnostics["queries_run"] == 2
    assert diagnostics["required_queries_run"] == 1
    assert diagnostics["required_queries_complete"] == 1
    assert diagnostics["optional_queries_incomplete"] == 1
    assert diagnostics["complete"] is True
    query_diag_by_kind = {
        item.get("query_kind"): item for item in diagnostics["query_diagnostics"] if isinstance(item, dict)
    }
    assert query_diag_by_kind["flair_exact"]["required"] is True
    assert query_diag_by_kind["flair_exact"]["complete"] is True
    assert query_diag_by_kind["flair_phrase"]["required"] is False
    assert query_diag_by_kind["flair_phrase"]["complete"] is False


def test_fetch_search_backfill_handles_required_query_error_as_incomplete(monkeypatch) -> None:
    period_start = datetime(2025, 8, 14, 0, 0, tzinfo=UTC)
    period_end = datetime(2025, 9, 16, 23, 0, tzinfo=UTC)

    def fake_get_json(path: str, params: dict) -> dict:  # noqa: ANN001
        raise reddit_refresh.RedditRefreshError("Reddit rate limit hit, try again shortly.", status=429)

    monkeypatch.setattr(reddit_refresh._HTTP_CLIENT, "get_json", fake_get_json)  # noqa: SLF001

    rows, diagnostics = reddit_refresh._fetch_search_backfill(  # noqa: SLF001
        subreddit="bravorealhousewives",
        tracked_flairs=["Salt Lake City"],
        show_aliases=[],
        show_terms=[],
        period_start=period_start,
        period_end=period_end,
        max_pages_per_query=1,
        max_total_queries=1,
    )

    assert rows == []
    assert diagnostics["queries_run"] == 1
    assert diagnostics["required_queries_run"] == 1
    assert diagnostics["required_queries_complete"] == 0
    assert diagnostics["complete"] is False
    first_diag = diagnostics["query_diagnostics"][0]
    assert first_diag["required"] is True
    assert first_diag["complete"] is False
    assert "rate limit" in str(first_diag.get("error") or "").lower()


def test_apply_match_metadata_scan_flair_requires_rhoslc_term() -> None:
    row = _listing_row(
        post_id="scan001",
        created_at=datetime(2025, 9, 1, 12, 0, tzinfo=UTC),
        flair="Shitpost",
        title="RHOSLC trailer is out",
    )

    output, _hints, tracked_rows = reddit_refresh._apply_match_metadata(  # noqa: SLF001
        rows=[row],
        subreddit="bravorealhousewives",
        terms=["rhoslc", "salt lake city"],
        cast_terms=[],
        analysis_flairs=["Shitpost"],
        analysis_all_flairs=[],
        force_include_flairs=[],
        show_focused=False,
    )

    assert len(output) == 1
    assert tracked_rows == 1
    assert output[0]["passes_flair_filter"] is True
    assert output[0]["flair_mode"] == "scan_term"


def test_apply_match_metadata_scan_flair_excludes_without_rhoslc_term() -> None:
    row = _listing_row(
        post_id="scan002",
        created_at=datetime(2025, 9, 1, 12, 0, tzinfo=UTC),
        flair="Shitpost",
        title="Completely unrelated post",
    )

    output, _hints, tracked_rows = reddit_refresh._apply_match_metadata(  # noqa: SLF001
        rows=[row],
        subreddit="bravorealhousewives",
        terms=["rhoslc", "salt lake city"],
        cast_terms=[],
        analysis_flairs=["Shitpost"],
        analysis_all_flairs=[],
        force_include_flairs=[],
        show_focused=False,
    )

    assert output == []
    assert tracked_rows == 0


def test_apply_match_metadata_all_flair_includes_without_term() -> None:
    row = _listing_row(
        post_id="all001",
        created_at=datetime(2025, 9, 1, 12, 0, tzinfo=UTC),
        flair="Shitpost",
        title="Completely unrelated post",
    )

    output, _hints, tracked_rows = reddit_refresh._apply_match_metadata(  # noqa: SLF001
        rows=[row],
        subreddit="bravorealhousewives",
        terms=["rhoslc", "salt lake city"],
        cast_terms=[],
        analysis_flairs=[],
        analysis_all_flairs=["Shitpost"],
        force_include_flairs=[],
        show_focused=False,
    )

    assert len(output) == 1
    assert tracked_rows == 1
    assert output[0]["passes_flair_filter"] is True
    assert output[0]["flair_mode"] == "all"


def test_discover_window_seed_urls_ingest_rows(monkeypatch) -> None:
    seeded = _listing_row(
        post_id="seed001",
        created_at=datetime(2025, 8, 20, 12, 0, tzinfo=UTC),
        flair="Shitpost",
        title="RHOSLC cast photo",
    )

    monkeypatch.setattr(
        reddit_refresh,
        "_fetch_new_window_exhaustive",
        lambda subreddit, period_start, period_end, max_pages: ([], 1, False),  # noqa: ARG005
    )
    monkeypatch.setattr(
        reddit_refresh,
        "_fetch_search_backfill",
        lambda **kwargs: (  # noqa: ARG005
            [],
            {
                "enabled": True,
                "queries_run": 0,
                "pages_fetched": 0,
                "rows_fetched": 0,
                "rows_in_window": 0,
                "complete": False,
                "query_diagnostics": [],
            },
        ),
    )
    monkeypatch.setattr(
        reddit_refresh,
        "_fetch_seed_rows",
        lambda seed_post_urls: (  # noqa: ARG005
            [seeded],
            {
                "seed_urls_requested": 1,
                "seed_urls_parsed": 1,
                "seed_urls_ingested": 1,
                "seed_urls_failed": 0,
                "seed_ingested_post_ids": ["seed001"],
                "seed_failed_post_ids": [],
                "seed_failed_urls": [],
            },
        ),
    )

    payload = {
        "subreddit": "bravorealhousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": [],
        "is_show_focused": False,
        "analysis_flairs": ["Shitpost"],
        "analysis_all_flairs": [],
        "force_include_flairs": [],
        "period_start": "2025-08-14T00:00:00Z",
        "period_end": "2025-09-16T23:00:00Z",
        "exhaustive_window": True,
        "search_backfill": True,
        "seed_post_urls": ["https://www.reddit.com/r/BravoRealHousewives/comments/seed001/example/"],
        "max_pages": 500,
    }

    result = reddit_refresh._discover_window(payload)  # noqa: SLF001

    assert result["totals"]["fetched_rows"] == 1
    assert result["totals"]["matched_rows"] == 1
    assert result["threads"][0]["reddit_post_id"] == "seed001"
    assert result["seed_urls"]["seed_urls_ingested"] == 1
    assert result["threads"][0]["flair_mode"] == "scan_term"


def test_cached_period_payload_uses_canonical_rows_over_stale_run_blob(monkeypatch) -> None:
    run_row = {
        "id": "00000000-0000-4000-8000-000000000001",
        "subreddit": "bravorealhousewives",
        "status": "completed",
        "diagnostics": {
            "result": {
                "threads": [{"reddit_post_id": "stale"}],
                "totals": {"fetched_rows": 99, "matched_rows": 99, "tracked_flair_rows": 99},
            }
        },
        "total_rows": 99,
        "matched_rows": 99,
        "tracked_flair_rows": 99,
        "created_at": datetime(2026, 2, 28, 12, 0, tzinfo=UTC),
        "completed_at": datetime(2026, 2, 28, 12, 1, tzinfo=UTC),
    }
    canonical_rows = [
        {
            "reddit_post_id": "canon001",
            "title": "RHOSLC thread",
            "selftext": "",
            "url": "https://reddit.com/canon001",
            "permalink": "https://reddit.com/canon001",
            "author": "user",
            "score": 12,
            "num_comments": 5,
            "posted_at": datetime(2025, 8, 20, 12, 0, tzinfo=UTC),
            "link_flair_text": "Shitpost",
            "source_sorts": ["new"],
            "matched_terms": ["rhoslc"],
            "matched_cast_terms": [],
            "cross_show_terms": [],
            "is_show_match": True,
            "passes_flair_filter": True,
            "match_score": 52,
            "flair_mode": "scan_term",
        }
    ]

    calls = {"count": 0}
    monkeypatch.setattr(reddit_refresh, "_column_exists", lambda schema, table, column, **kwargs: True)  # noqa: ARG005

    def fake_fetch_one(*args, **kwargs):  # noqa: ANN002, ANN003
        calls["count"] += 1
        return run_row if calls["count"] <= 2 else None

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(reddit_refresh.pg, "fetch_all", lambda *args, **kwargs: canonical_rows)  # noqa: ARG005

    payload = reddit_refresh.get_cached_period_payload(
        community_id="community-1",
        season_id="season-1",
        period_key="pre-season",
    )

    assert payload is not None
    assert payload["totals"]["fetched_rows"] == 1
    assert payload["threads"][0]["reddit_post_id"] == "canon001"
    assert payload["threads"][0]["flair_mode"] == "scan_term"


def test_replace_period_matches_skips_flair_mode_when_column_missing(monkeypatch) -> None:
    executed: dict[str, str] = {}

    class DummyCursor:
        def __enter__(self):  # noqa: ANN204, D401
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: ANN001, ANN201
            return None

        def execute(self, query, params):  # noqa: ANN001, ANN201
            return None

        def fetchall(self):  # noqa: ANN201
            return []

    monkeypatch.setattr(reddit_refresh, "_column_exists", lambda schema, table, column, **kwargs: False)  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh.pg, "db_cursor", lambda conn=None: DummyCursor())  # noqa: ARG005

    def capture_execute(query, tuples, conn=None):  # noqa: ANN001, ANN201, ARG001
        executed["query"] = query

    monkeypatch.setattr(reddit_refresh.pg, "execute_values_no_return", capture_execute)

    reddit_refresh._replace_period_matches(  # noqa: SLF001
        community_id="community-1",
        season_id="season-1",
        period_key="pre-season",
        period_start=datetime(2025, 8, 14, 0, 0, tzinfo=UTC),
        period_end=datetime(2025, 9, 16, 23, 0, tzinfo=UTC),
        run_id="run-1",
        rows=[
            {
                "reddit_post_id": "abc123",
                "is_show_match": True,
                "passes_flair_filter": True,
                "matched_terms": ["rhoslc"],
                "matched_cast_terms": [],
                "cross_show_terms": [],
                "match_score": 5,
                "source_sorts": ["new"],
                "link_flair_text": "Salt Lake City",
                "canonical_flair_key": "salt lake city",
                "flair_mode": "all",
            }
        ],
        conn=object(),
    )

    assert "flair_mode" not in executed["query"]


def test_cached_period_payload_returns_null_flair_mode_when_column_missing(monkeypatch) -> None:
    run_row = {
        "id": "00000000-0000-4000-8000-000000000001",
        "subreddit": "bravorealhousewives",
        "status": "completed",
        "diagnostics": {},
        "total_rows": 1,
        "matched_rows": 1,
        "tracked_flair_rows": 1,
        "created_at": datetime(2026, 2, 28, 12, 0, tzinfo=UTC),
        "completed_at": datetime(2026, 2, 28, 12, 1, tzinfo=UTC),
    }
    canonical_rows = [
        {
            "reddit_post_id": "canon001",
            "title": "RHOSLC thread",
            "selftext": "",
            "url": "https://reddit.com/canon001",
            "permalink": "https://reddit.com/canon001",
            "author": "user",
            "score": 12,
            "num_comments": 5,
            "posted_at": datetime(2025, 8, 20, 12, 0, tzinfo=UTC),
            "link_flair_text": "Shitpost",
            "source_sorts": ["new"],
            "matched_terms": ["rhoslc"],
            "matched_cast_terms": [],
            "cross_show_terms": [],
            "is_show_match": True,
            "passes_flair_filter": True,
            "match_score": 52,
            "flair_mode": None,
        }
    ]

    monkeypatch.setattr(reddit_refresh, "_column_exists", lambda schema, table, column, **kwargs: False)  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", lambda *args, **kwargs: run_row)  # noqa: ANN002, ANN003, ARG005
    monkeypatch.setattr(reddit_refresh.pg, "fetch_all", lambda *args, **kwargs: canonical_rows)  # noqa: ANN002, ANN003, ARG005

    payload = reddit_refresh.get_cached_period_payload(
        community_id="community-1",
        season_id="season-1",
        period_key="pre-season",
    )

    assert payload is not None
    assert payload["threads"][0]["flair_mode"] is None


def test_get_cached_period_payload_resolves_stable_container_key_to_legacy_period_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_row = {
        "id": "00000000-0000-4000-8000-000000000001",
        "subreddit": "bravorealhousewives",
        "status": "completed",
        "diagnostics": {},
        "total_rows": 1,
        "matched_rows": 1,
        "tracked_flair_rows": 1,
        "created_at": datetime(2026, 2, 28, 12, 0, tzinfo=UTC),
        "completed_at": datetime(2026, 2, 28, 12, 1, tzinfo=UTC),
    }
    canonical_rows = [
        {
            "reddit_post_id": "canon-preseason-1",
            "title": "RHOSLC preseason post",
            "selftext": "",
            "url": "https://reddit.com/canon-preseason-1",
            "permalink": "https://reddit.com/canon-preseason-1",
            "author": "user",
            "score": 15,
            "num_comments": 8,
            "posted_at": datetime(2025, 8, 20, 12, 0, tzinfo=UTC),
            "link_flair_text": "Salt Lake City",
            "source_sorts": ["new"],
            "matched_terms": ["salt lake city"],
            "matched_cast_terms": [],
            "cross_show_terms": [],
            "is_show_match": True,
            "passes_flair_filter": True,
            "match_score": 67,
            "flair_mode": "all",
        }
    ]

    legacy_period_key = "legacy-preseason-key"
    stable_period_key = "community:community-1:season:season-1:container:period-preseason"

    def fake_fetch_one(query, params):  # noqa: ANN001, ANN201
        sql = str(query)
        if "select id," in sql and "from social.reddit_refresh_runs" in sql:
            requested_key = str(params[2]) if len(params) >= 3 else ""
            if requested_key == legacy_period_key:
                return run_row
            return None
        if "select period_key" in sql and "request_payload->>'container_key'" in sql:
            return {"period_key": legacy_period_key}
        return None

    monkeypatch.setattr(reddit_refresh, "_column_exists", lambda schema, table, column, **kwargs: True)  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(reddit_refresh.pg, "fetch_all", lambda *args, **kwargs: canonical_rows)  # noqa: ANN002, ANN003, ARG005

    payload = reddit_refresh.get_cached_period_payload(
        community_id="community-1",
        season_id="season-1",
        period_key=stable_period_key,
    )

    assert payload is not None
    assert payload["totals"]["fetched_rows"] == 1
    assert payload["threads"][0]["reddit_post_id"] == "canon-preseason-1"
    assert payload["threads"][0]["flair_mode"] == "all"


def test_execute_refresh_run_updates_live_progress(monkeypatch) -> None:
    run_id = "63a7be5d-0000-4000-8000-000000000000"
    run_row = {
        "id": run_id,
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "subreddit": "bravorealhousewives",
        "request_payload": {"fetch_comments": True},
    }
    result_payload = {
        "subreddit": "bravorealhousewives",
        "collection_mode": "exhaustive_window",
        "listing_pages_fetched": 3,
        "max_pages_applied": 500,
        "window_exhaustive_complete": True,
        "search_backfill": {"complete": True, "pages_fetched": 2},
        "seed_urls": {},
        "window_start": "2025-08-14T00:00:00Z",
        "window_end": "2025-09-16T23:00:00Z",
        "terms": ["rhoslc"],
        "hints": {"suggested_include_terms": [], "suggested_exclude_terms": []},
        "threads": [
            {
                "reddit_post_id": "post-1",
                "title": "RHOSLC post",
                "text": "body",
                "url": "https://reddit.com/post-1",
                "num_comments": 5,
                "score": 10,
            }
        ],
        "totals": {"fetched_rows": 1, "matched_rows": 1, "tracked_flair_rows": 1},
    }
    running_updates: list[dict] = []

    def fake_discover(payload, *, progress_callback=None):  # noqa: ANN001
        if progress_callback:
            progress_callback({"listing_pages_fetched": 1, "rows_discovered_raw": 8})
            progress_callback({"search_pages_fetched": 1, "rows_discovered_raw": 12, "rows_matched": 2})
        return result_payload

    def fake_update_run(run_id_arg, *, status, diagnostics=None, **kwargs):  # noqa: ANN001
        if status == "running" and diagnostics:
            running_updates.append(diagnostics)

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", lambda *args, **kwargs: run_row)  # noqa: ANN002, ANN003, ARG005
    monkeypatch.setattr(reddit_refresh, "_discover_window", fake_discover)
    monkeypatch.setattr(reddit_refresh, "_update_run", fake_update_run)
    monkeypatch.setattr(reddit_refresh, "_touch_refresh_run_heartbeat", lambda **kwargs: None)
    monkeypatch.setattr(reddit_refresh.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(reddit_refresh, "_upsert_posts", lambda rows, *, conn: None)  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh, "_replace_period_matches", lambda **kwargs: None)
    monkeypatch.setattr(reddit_refresh, "_fetch_post_comments_tree", lambda post_id: [{"reddit_comment_id": "c1"}])  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh, "_upsert_comments", lambda rows, *, conn: len(rows))  # noqa: ARG005
    monkeypatch.setattr(
        reddit_refresh,
        "get_refresh_run",
        lambda run_id_arg: {"run_id": run_id_arg, "status": "completed"},
    )  # noqa: ARG005

    result = reddit_refresh.execute_refresh_run(run_id)

    assert result["status"] == "completed"
    progress_updates = [
        item.get("progress")
        for item in running_updates
        if isinstance(item, dict) and isinstance(item.get("progress"), dict)
    ]
    assert progress_updates
    assert any(update.get("stage") == "discovering_posts" for update in progress_updates)
    assert any(update.get("stage") == "fetching_comments" for update in progress_updates)
    assert any(int(update.get("comments_targets_done") or 0) >= 1 for update in progress_updates)
    assert any(int(update.get("comments_rows_upserted") or 0) >= 1 for update in progress_updates)


def test_execute_refresh_run_marks_partial_when_discovery_raises_reddit_403(monkeypatch) -> None:
    run_id = "63a7be5d-0000-4000-8000-000000000099"
    run_row = {
        "id": run_id,
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "season",
        "subreddit": "bravorealhousewives",
        "request_payload": {"mode": "sync_posts"},
    }
    updates: list[dict] = []

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", lambda *args, **kwargs: run_row)  # noqa: ANN002, ANN003, ARG005
    monkeypatch.setattr(
        reddit_refresh,
        "_discover_window",
        lambda *args, **kwargs: (_ for _ in ()).throw(  # noqa: ARG005
            reddit_refresh.RedditRefreshError("Reddit request failed (403)", status=403)
        ),
    )
    monkeypatch.setattr(
        reddit_refresh,
        "_update_run",
        lambda run_id_arg, **kwargs: updates.append({"run_id": run_id_arg, **kwargs}),  # noqa: ANN001
    )
    monkeypatch.setattr(reddit_refresh, "_touch_refresh_run_heartbeat", lambda **kwargs: None)
    monkeypatch.setattr(
        reddit_refresh,
        "get_refresh_run",
        lambda run_id_arg: {"run_id": run_id_arg, "status": "partial"},
    )

    result = reddit_refresh.execute_refresh_run(run_id)

    assert result == {"run_id": run_id, "status": "partial"}
    partial_update = next((item for item in updates if item.get("status") == "partial"), None)
    assert partial_update is not None
    assert partial_update["error_message"] == "Reddit request failed (403)"
    diagnostics = partial_update.get("diagnostics") or {}
    assert diagnostics.get("error_type") == "RedditRefreshError"
    assert diagnostics.get("failure_reason_code") == "reddit_http_403"
    assert diagnostics.get("terminal_summary", {}).get("status") == "partial"


def test_get_refresh_run_includes_queue_counters(monkeypatch) -> None:
    created_at = datetime(2026, 2, 28, 12, 0, tzinfo=UTC)
    updated_at = datetime(2026, 2, 28, 12, 1, tzinfo=UTC)
    run_id = "63a7be5d-0000-4000-8000-000000000000"

    run_row = {
        "id": run_id,
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "subreddit": "bravorealhousewives",
        "status": "queued",
        "request_payload": {},
        "diagnostics": {},
        "error_message": None,
        "total_rows": 11,
        "matched_rows": 7,
        "tracked_flair_rows": 5,
        "started_at": None,
        "completed_at": None,
        "created_at": created_at,
        "updated_at": updated_at,
    }
    queue_row = {
        "running_total": 2,
        "queued_total": 5,
        "queued_ahead": 3,
    }

    rows = iter([run_row, queue_row])

    def fake_fetch_one(*args, **kwargs):  # noqa: ANN002, ANN003
        return next(rows)

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", fake_fetch_one)

    result = reddit_refresh.get_refresh_run(run_id)

    assert result["run_id"] == run_id
    assert result["totals"]["tracked_flair_rows"] == 5
    assert result["queue"] == {
        "running_total": 2,
        "queued_total": 5,
        "other_running": 2,
        "other_queued": 4,
        "queued_ahead": 3,
    }


def test_create_or_reuse_refresh_run_recovers_stale_queued_runs(monkeypatch) -> None:
    stale_recovered_calls: list[tuple[str, list]] = []
    inserted_payloads: list[dict[str, str]] = []

    def fake_execute_returning(query, params=None):  # noqa: ANN001
        if "stale_queue_recovered" in query:
            stale_recovered_calls.append((query, list(params or [])))
            return [{"id": "old-queued-run"}]
        return []

    rows = iter(
        [
            {
                "id": "new-run",
                "community_id": "community-1",
                "season_id": "season-1",
                "period_key": "pre-season",
                "status": "queued",
                "request_payload": {},
            },  # insert returning
        ]
    )

    monkeypatch.setattr(reddit_refresh.pg, "execute_returning", fake_execute_returning)
    monkeypatch.setattr(
        reddit_refresh.pg,
        "fetch_all",
        lambda *args, **kwargs: [],  # noqa: ANN002, ANN003
    )

    def fake_fetch_one(query, params=None):  # noqa: ANN001
        request_payload = (params or [None, None, None, None, "{}"])[4]
        inserted_payloads.append({"query": query, "request_payload": request_payload})
        return next(rows)

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", fake_fetch_one)

    row = reddit_refresh.create_or_reuse_refresh_run(
        payload={
            "community_id": "community-1",
            "season_id": "season-1",
            "period_key": "pre-season",
            "subreddit": "bravorealhousewives",
        }
    )

    assert row["id"] == "new-run"
    assert row["reused"] is False
    assert len(stale_recovered_calls) == 1
    assert len(inserted_payloads) == 1
    assert "run_config_hash" in inserted_payloads[0]["request_payload"]


def test_create_or_reuse_refresh_run_reuses_matching_active_config(monkeypatch) -> None:
    payload = {
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "subreddit": "bravorealhousewives",
        "coverage_mode": "adaptive_deep",
        "max_pages": 1000,
        "search_backfill": True,
    }
    expected_hash = reddit_refresh._build_run_config_hash(payload)  # noqa: SLF001
    active_row = {
        "id": "active-run",
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "status": "running",
        "request_payload": {
            **payload,
            "run_config_hash": expected_hash,
        },
    }

    monkeypatch.setattr(reddit_refresh.pg, "execute_returning", lambda *args, **kwargs: [])  # noqa: ANN002, ANN003
    monkeypatch.setattr(reddit_refresh.pg, "fetch_all", lambda *args, **kwargs: [active_row])  # noqa: ANN002, ANN003
    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", lambda *args, **kwargs: None)  # noqa: ANN002, ANN003

    row = reddit_refresh.create_or_reuse_refresh_run(payload=payload)

    assert row["id"] == "active-run"
    assert row["reused"] is True


def test_build_run_config_hash_distinguishes_behavior_inputs() -> None:
    base_payload = {
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "subreddit": "bravorealhousewives",
        "show_name": "The Real Housewives of Salt Lake City",
        "show_aliases": ["RHOSLC"],
        "cast_names": ["Lisa Barlow"],
        "coverage_mode": "adaptive_deep",
        "preserve_existing_assignments": True,
        "force_rescrape": False,
    }
    base_hash = reddit_refresh._build_run_config_hash(base_payload)  # noqa: SLF001

    behavior_changes = [
        {"subreddit": "Bravo"},
        {"show_aliases": ["RHOSLC", "Salt Lake City"]},
        {"cast_names": ["Meredith Marks"]},
        {"preserve_existing_assignments": False},
        {"force_rescrape": True},
    ]

    for change in behavior_changes:
        assert reddit_refresh._build_run_config_hash({**base_payload, **change}) != base_hash  # noqa: SLF001


def test_create_or_reuse_refresh_run_recovers_orphaned_matching_queued_run(monkeypatch) -> None:
    payload = {
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "subreddit": "bravorealhousewives",
        "coverage_mode": "standard",
        "max_pages": 500,
    }
    old_created_at = datetime(2026, 3, 22, 10, 0, tzinfo=UTC)
    active_row = {
        "id": "queued-run",
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "status": "queued",
        "created_at": old_created_at,
        "updated_at": old_created_at,
        "attempt_count": 0,
        "claimed_by_worker_id": None,
        "claim_token": None,
        "heartbeat_at": None,
        "request_payload": {
            **payload,
            "run_config_hash": reddit_refresh._build_run_config_hash(payload),  # noqa: SLF001
        },
    }
    inserted_row = {
        "id": "fresh-run",
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "status": "queued",
        "request_payload": {},
    }
    recovered_runs: list[dict] = []

    monkeypatch.setattr(reddit_refresh.pg, "execute_returning", lambda *args, **kwargs: [])  # noqa: ANN002, ANN003
    monkeypatch.setattr(reddit_refresh.pg, "fetch_all", lambda *args, **kwargs: [active_row])  # noqa: ANN002, ANN003
    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", lambda *args, **kwargs: inserted_row)  # noqa: ANN002, ANN003
    monkeypatch.setattr(
        reddit_refresh,
        "_update_run",
        lambda run_id, **kwargs: recovered_runs.append({"run_id": run_id, **kwargs}),
    )

    row = reddit_refresh.create_or_reuse_refresh_run(payload=payload)

    assert row["id"] == "fresh-run"
    assert row["reused"] is False
    assert recovered_runs[0]["run_id"] == "queued-run"
    assert recovered_runs[0]["status"] == "failed"
    assert recovered_runs[0]["diagnostics"]["failure_reason_code"] == "stale_queue"
    assert recovered_runs[0]["diagnostics"]["stale_queue_recovered"] is True


def test_is_orphaned_queued_run_ignores_attempt_count_when_unclaimed() -> None:
    old_updated_at = datetime.now(tz=UTC) - timedelta(
        seconds=reddit_refresh.REDDIT_REFRESH_ORPHANED_QUEUED_REUSE_GRACE_SECONDS_DEFAULT + 1
    )
    row = {
        "status": "queued",
        "attempt_count": 3,
        "claimed_by_worker_id": None,
        "claim_token": None,
        "heartbeat_at": None,
        "updated_at": old_updated_at.isoformat(),
        "created_at": old_updated_at.isoformat(),
    }

    assert (
        reddit_refresh._is_orphaned_queued_run(
            row,
            grace_seconds=reddit_refresh.REDDIT_REFRESH_ORPHANED_QUEUED_REUSE_GRACE_SECONDS_DEFAULT,
        )
        is True
    )


def test_create_or_reuse_refresh_run_does_not_reuse_mismatched_active_config(monkeypatch) -> None:
    payload = {
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "subreddit": "bravorealhousewives",
        "coverage_mode": "standard",
        "max_pages": 500,
    }
    active_row = {
        "id": "active-run",
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "status": "queued",
        "request_payload": {
            **payload,
            "coverage_mode": "max_coverage",
            "max_pages": 1000,
        },
    }
    inserted_row = {
        "id": "new-run",
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "status": "queued",
        "request_payload": {},
    }

    monkeypatch.setattr(reddit_refresh.pg, "execute_returning", lambda *args, **kwargs: [])  # noqa: ANN002, ANN003
    monkeypatch.setattr(reddit_refresh.pg, "fetch_all", lambda *args, **kwargs: [active_row])  # noqa: ANN002, ANN003
    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", lambda *args, **kwargs: inserted_row)  # noqa: ANN002, ANN003

    row = reddit_refresh.create_or_reuse_refresh_run(payload=payload)

    assert row["id"] == "new-run"
    assert row["reused"] is False


def test_claim_refresh_run_for_execution_claims_queued_run(monkeypatch) -> None:
    monkeypatch.setattr(
        reddit_refresh.pg,
        "fetch_one",
        lambda *args, **kwargs: {  # noqa: ANN002, ANN003
            "id": "run-1",
            "community_id": "community-1",
            "season_id": "season-1",
            "period_key": "pre-season",
            "subreddit": "bravorealhousewives",
            "request_payload": {},
            "status": "running",
            "updated_at": datetime.now(tz=UTC),
        },
    )

    claimed = reddit_refresh._claim_refresh_run_for_execution("run-1")  # noqa: SLF001
    assert claimed["id"] == "run-1"
    assert claimed["period_key"] == "pre-season"


def test_claim_refresh_run_for_execution_marks_stale_running_failed(monkeypatch) -> None:
    now = datetime.now(tz=UTC)
    rows = iter(
        [
            None,  # claim update returns nothing
            {
                "id": "run-1",
                "community_id": "community-1",
                "season_id": "season-1",
                "period_key": "pre-season",
                "subreddit": "bravorealhousewives",
                "request_payload": {},
                "status": "running",
                "updated_at": now.replace(year=2024),
            },
        ]
    )
    updates: list[dict] = []

    monkeypatch.setattr(
        reddit_refresh.pg,
        "fetch_one",
        lambda *args, **kwargs: next(rows),  # noqa: ANN002, ANN003
    )
    monkeypatch.setattr(
        reddit_refresh,
        "_update_run",
        lambda run_id, **kwargs: updates.append({"run_id": run_id, **kwargs}),  # noqa: ANN001
    )

    with pytest.raises(RuntimeError, match="stale running"):
        reddit_refresh._claim_refresh_run_for_execution("run-1")  # noqa: SLF001

    assert updates
    assert updates[0]["status"] == "failed"


def test_apply_match_metadata_filters_stopword_hint_tokens() -> None:
    row = _listing_row(
        post_id="hint001",
        created_at=datetime(2025, 9, 1, 12, 0, tzinfo=UTC),
        flair="Salt Lake City",
        title="The Season 6 RHOSLC trailer",
    )

    output, hints, _tracked_rows = reddit_refresh._apply_match_metadata(  # noqa: SLF001
        rows=[row],
        subreddit="bravorealhousewives",
        terms=["rhoslc"],
        cast_terms=[],
        analysis_flairs=[],
        analysis_all_flairs=["Salt Lake City"],
        force_include_flairs=[],
        show_focused=False,
    )

    assert len(output) == 1
    assert "rhoslc" in hints["suggested_include_terms"]
    assert "the" not in hints["suggested_include_terms"]
    assert "season" not in hints["suggested_include_terms"]
    assert "trailer" not in hints["suggested_include_terms"]
    assert "6" not in hints["suggested_include_terms"]


def test_execute_refresh_run_adaptive_deep_completes_after_second_pass(monkeypatch) -> None:
    run_id = "63a7be5d-0000-4000-8000-000000000010"
    run_row = {
        "id": run_id,
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "subreddit": "bravorealhousewives",
        "request_payload": {"fetch_comments": False, "coverage_mode": "adaptive_deep"},
        "status": "running",
        "updated_at": datetime.now(tz=UTC),
    }
    first_result = {
        "subreddit": "bravorealhousewives",
        "collection_mode": "exhaustive_window",
        "listing_pages_fetched": 12,
        "max_pages_applied": 500,
        "max_backfill_queries_applied": 12,
        "max_backfill_pages_per_query_applied": 20,
        "window_exhaustive_complete": False,
        "search_backfill": {
            "complete": False,
            "pages_fetched": 3,
            "queries_run": 6,
            "rows_fetched": 20,
            "rows_in_window": 2,
            "query_diagnostics": [],
        },
        "seed_urls": {},
        "window_start": "2025-08-14T00:00:00Z",
        "window_end": "2025-09-16T23:00:00Z",
        "terms": ["rhoslc"],
        "hints": {"suggested_include_terms": ["rhoslc"], "suggested_exclude_terms": []},
        "threads": [
            {
                "reddit_post_id": "p1",
                "title": "Post 1",
                "text": "body",
                "posted_at": "2025-09-01T00:00:00Z",
                "num_comments": 1,
                "score": 1,
                "passes_flair_filter": True,
            }
        ],
        "totals": {"fetched_rows": 12, "matched_rows": 1, "tracked_flair_rows": 1},
    }
    second_result = {
        "subreddit": "bravorealhousewives",
        "collection_mode": "exhaustive_window",
        "listing_pages_fetched": 40,
        "max_pages_applied": 1000,
        "max_backfill_queries_applied": 30,
        "max_backfill_pages_per_query_applied": 50,
        "window_exhaustive_complete": True,
        "search_backfill": {
            "complete": True,
            "pages_fetched": 10,
            "queries_run": 20,
            "rows_fetched": 120,
            "rows_in_window": 20,
            "query_diagnostics": [],
        },
        "seed_urls": {},
        "window_start": "2025-08-14T00:00:00Z",
        "window_end": "2025-09-16T23:00:00Z",
        "terms": ["rhoslc", "salt lake city"],
        "hints": {"suggested_include_terms": ["rhoslc"], "suggested_exclude_terms": []},
        "threads": [
            {
                "reddit_post_id": "p1",
                "title": "Post 1",
                "text": "body",
                "posted_at": "2025-09-01T00:00:00Z",
                "num_comments": 2,
                "score": 2,
                "passes_flair_filter": True,
            },
            {
                "reddit_post_id": "p2",
                "title": "Post 2",
                "text": "body",
                "posted_at": "2025-09-02T00:00:00Z",
                "num_comments": 3,
                "score": 3,
                "passes_flair_filter": True,
            },
        ],
        "totals": {"fetched_rows": 120, "matched_rows": 2, "tracked_flair_rows": 2},
    }

    discover_payloads: list[dict] = []
    updates: list[dict] = []

    def fake_discover(payload, *, progress_callback=None):  # noqa: ANN001
        discover_payloads.append(dict(payload))
        if progress_callback:
            progress_callback(
                {
                    "listing_pages_fetched": len(discover_payloads),
                    "rows_discovered_raw": 10 * len(discover_payloads),
                }
            )
        return first_result if len(discover_payloads) == 1 else second_result

    def fake_update_run(run_id_arg, **kwargs):  # noqa: ANN001
        updates.append({"run_id": run_id_arg, **kwargs})

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", lambda *args, **kwargs: run_row)  # noqa: ANN002, ANN003, ARG005
    monkeypatch.setattr(reddit_refresh, "_discover_window", fake_discover)
    monkeypatch.setattr(reddit_refresh, "_update_run", fake_update_run)
    monkeypatch.setattr(reddit_refresh, "_touch_refresh_run_heartbeat", lambda **kwargs: None)
    monkeypatch.setattr(reddit_refresh.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(reddit_refresh, "_upsert_posts", lambda rows, *, conn: None)  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh, "_replace_period_matches", lambda **kwargs: None)
    monkeypatch.setattr(
        reddit_refresh,
        "get_refresh_run",
        lambda run_id_arg: {"run_id": run_id_arg, "status": "completed"},
    )  # noqa: ARG005

    result = reddit_refresh.execute_refresh_run(run_id)

    assert result["status"] == "completed"
    assert len(discover_payloads) == 2
    assert int(discover_payloads[1].get("max_pages") or 0) >= 1000
    assert int(discover_payloads[1].get("max_backfill_queries") or 0) >= 30
    assert int(discover_payloads[1].get("max_backfill_pages_per_query") or 0) >= 50
    completed_update = next(
        (item for item in updates if item.get("status") in {"completed", "partial"}),
        None,
    )
    assert completed_update is not None
    assert completed_update["status"] == "completed"
    diagnostics = completed_update.get("diagnostics") or {}
    assert diagnostics.get("coverage_mode") == "adaptive_deep"
    assert diagnostics.get("passes_run") == 2
    assert diagnostics.get("final_completeness") == {
        "listing_complete": True,
        "backfill_complete": True,
    }
    passes = diagnostics.get("passes") or []
    assert len(passes) == 2
    assert passes[0]["window_exhaustive_complete"] is False
    assert passes[1]["window_exhaustive_complete"] is True


def test_execute_refresh_run_adaptive_deep_remains_partial_when_second_pass_incomplete(monkeypatch) -> None:
    run_id = "63a7be5d-0000-4000-8000-000000000011"
    run_row = {
        "id": run_id,
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "pre-season",
        "subreddit": "bravorealhousewives",
        "request_payload": {"fetch_comments": False, "coverage_mode": "adaptive_deep"},
        "status": "running",
        "updated_at": datetime.now(tz=UTC),
    }
    incomplete_result = {
        "subreddit": "bravorealhousewives",
        "collection_mode": "exhaustive_window",
        "listing_pages_fetched": 50,
        "max_pages_applied": 1000,
        "max_backfill_queries_applied": 30,
        "max_backfill_pages_per_query_applied": 50,
        "window_exhaustive_complete": False,
        "search_backfill": {
            "complete": False,
            "pages_fetched": 15,
            "queries_run": 30,
            "rows_fetched": 180,
            "rows_in_window": 15,
            "query_diagnostics": [],
        },
        "seed_urls": {},
        "window_start": "2025-08-14T00:00:00Z",
        "window_end": "2025-09-16T23:00:00Z",
        "terms": ["rhoslc"],
        "hints": {"suggested_include_terms": ["rhoslc"], "suggested_exclude_terms": []},
        "threads": [
            {
                "reddit_post_id": "p1",
                "title": "Post 1",
                "text": "body",
                "posted_at": "2025-09-01T00:00:00Z",
                "num_comments": 1,
                "score": 1,
                "passes_flair_filter": True,
            }
        ],
        "totals": {"fetched_rows": 50, "matched_rows": 1, "tracked_flair_rows": 1},
    }

    discover_payloads: list[dict] = []
    updates: list[dict] = []

    def fake_discover(payload, *, progress_callback=None):  # noqa: ANN001
        discover_payloads.append(dict(payload))
        if progress_callback:
            progress_callback(
                {
                    "listing_pages_fetched": len(discover_payloads),
                    "rows_discovered_raw": 10 * len(discover_payloads),
                }
            )
        return incomplete_result

    def fake_update_run(run_id_arg, **kwargs):  # noqa: ANN001
        updates.append({"run_id": run_id_arg, **kwargs})

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", lambda *args, **kwargs: run_row)  # noqa: ANN002, ANN003, ARG005
    monkeypatch.setattr(reddit_refresh, "_discover_window", fake_discover)
    monkeypatch.setattr(reddit_refresh, "_update_run", fake_update_run)
    monkeypatch.setattr(reddit_refresh, "_touch_refresh_run_heartbeat", lambda **kwargs: None)
    monkeypatch.setattr(reddit_refresh.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(reddit_refresh, "_upsert_posts", lambda rows, *, conn: None)  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh, "_replace_period_matches", lambda **kwargs: None)
    monkeypatch.setattr(
        reddit_refresh,
        "get_refresh_run",
        lambda run_id_arg: {"run_id": run_id_arg, "status": "partial"},
    )  # noqa: ARG005

    result = reddit_refresh.execute_refresh_run(run_id)

    assert result["status"] == "partial"
    assert len(discover_payloads) == 2
    completed_update = next(
        (item for item in updates if item.get("status") in {"completed", "partial"}),
        None,
    )
    assert completed_update is not None
    assert completed_update["status"] == "partial"
    diagnostics = completed_update.get("diagnostics") or {}
    assert diagnostics.get("coverage_mode") == "adaptive_deep"
    assert diagnostics.get("passes_run") == 2
    assert diagnostics.get("final_completeness") == {
        "listing_complete": False,
        "backfill_complete": False,
    }


def test_execute_refresh_run_max_coverage_listing_incomplete_backfill_complete_marks_completed(
    monkeypatch,
) -> None:
    run_id = "63a7be5d-0000-4000-8000-000000000012"
    run_row = {
        "id": run_id,
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "season",
        "subreddit": "bravorealhousewives",
        "request_payload": {"fetch_comments": False, "coverage_mode": "max_coverage"},
        "status": "running",
        "updated_at": datetime.now(tz=UTC),
    }
    result_payload = {
        "subreddit": "bravorealhousewives",
        "collection_mode": "exhaustive_window",
        "listing_pages_fetched": 10,
        "max_pages_applied": 10,
        "max_backfill_queries_applied": 30,
        "max_backfill_pages_per_query_applied": 50,
        "window_exhaustive_complete": False,
        "search_backfill": {
            "complete": True,
            "pages_fetched": 3,
            "queries_run": 1,
            "rows_fetched": 3,
            "rows_in_window": 3,
            "query_diagnostics": [
                {
                    "flair": "London",
                    "query": 'flair:"London"',
                    "pages_fetched": 3,
                    "rows_fetched": 3,
                    "rows_in_window": 3,
                    "reached_period_start": False,
                    "exhausted_results": True,
                    "complete": True,
                }
            ],
        },
        "seed_urls": {},
        "window_start": "2010-01-01T00:00:00Z",
        "window_end": "2026-03-03T00:00:00Z",
        "terms": ["rhoslc", "salt lake city"],
        "hints": {"suggested_include_terms": ["rhoslc"], "suggested_exclude_terms": []},
        "threads": [
            {
                "reddit_post_id": "p1",
                "title": "Post 1",
                "text": "body",
                "posted_at": "2026-03-01T00:00:00Z",
                "num_comments": 2,
                "score": 2,
                "passes_flair_filter": True,
            }
        ],
        "totals": {"fetched_rows": 10, "matched_rows": 1, "tracked_flair_rows": 1},
    }

    updates: list[dict] = []

    def fake_update_run(run_id_arg, **kwargs):  # noqa: ANN001
        updates.append({"run_id": run_id_arg, **kwargs})

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", lambda *args, **kwargs: run_row)  # noqa: ANN002, ANN003, ARG005
    monkeypatch.setattr(reddit_refresh, "_discover_window", lambda *args, **kwargs: result_payload)  # noqa: ANN002, ANN003
    monkeypatch.setattr(reddit_refresh, "_update_run", fake_update_run)
    monkeypatch.setattr(reddit_refresh, "_touch_refresh_run_heartbeat", lambda **kwargs: None)
    monkeypatch.setattr(reddit_refresh.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(reddit_refresh, "_upsert_posts", lambda rows, *, conn: None)  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh, "_replace_period_matches", lambda **kwargs: None)
    monkeypatch.setattr(
        reddit_refresh,
        "get_refresh_run",
        lambda run_id_arg: {"run_id": run_id_arg, "status": "completed"},
    )  # noqa: ARG005

    result = reddit_refresh.execute_refresh_run(run_id)

    assert result["status"] == "completed"
    completed_update = next(
        (item for item in updates if item.get("status") in {"completed", "partial"}),
        None,
    )
    assert completed_update is not None
    assert completed_update["status"] == "completed"
    diagnostics = completed_update.get("diagnostics") or {}
    assert diagnostics.get("coverage_mode") == "max_coverage"
    assert diagnostics.get("final_completeness") == {
        "listing_complete": False,
        "backfill_complete": True,
    }
    assert diagnostics.get("status_resolution") == "listing_incomplete_backfill_complete_max_coverage"


def test_get_reddit_community_analytics_summary_season_scope_filters_season(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_one(query, params):  # noqa: ANN001
        captured["query"] = query
        captured["params"] = list(params)
        return {
            "post_count": 5,
            "tracked_flair_post_count": 4,
            "show_match_post_count": 3,
            "comment_count": 80,
            "score_sum": 900,
            "season_count": 1,
            "updated_at": datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
        }

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(
        reddit_refresh,
        "_fetch_reddit_analytics_extras",
        lambda **kwargs: {  # noqa: ARG005
            "freshness": {"latest_data_timestamp": None, "latest_run_timestamp": None, "latest_run_status": None},
            "coverage": {"tracked_post_count": 4, "stale_container_count": 0, "recovered_container_count": 0},
            "container_statuses": [],
        },
    )

    payload = reddit_refresh.get_reddit_community_analytics_summary(
        community_id="community-1",
        scope="season",
        season_id="season-1",
    )

    assert payload["scope"] == "season"
    assert payload["totals"]["post_count"] == 5
    assert payload["totals"]["tracked_flair_post_count"] == 4
    assert payload["diagnostics"]["row_count"] == 5
    assert payload["coverage"]["tracked_post_count"] == 4
    assert "m.season_id = %s" in str(captured["query"])
    assert captured["params"] == ["community-1", "season-1"]


def test_execute_refresh_run_marks_coverage_incomplete_partial_reason(monkeypatch) -> None:
    run_id = "63a7be5d-0000-4000-8000-000000000123"
    run_row = {
        "id": run_id,
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "episode-3",
        "subreddit": "bravorealhousewives",
        "request_payload": {"mode": "sync_posts", "coverage_mode": "standard"},
        "status": "running",
        "claim_token": "claim-token-1",
        "updated_at": datetime.now(tz=UTC),
    }
    updates: list[dict[str, object]] = []

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", lambda *args, **kwargs: run_row)  # noqa: ANN002, ANN003, ARG005
    monkeypatch.setattr(reddit_refresh, "_touch_refresh_run_heartbeat", lambda **kwargs: None)
    monkeypatch.setattr(reddit_refresh, "_upsert_posts", lambda rows, *, conn: None)  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh, "_replace_period_matches", lambda **kwargs: None)
    monkeypatch.setattr(reddit_refresh.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(
        reddit_refresh,
        "_discover_window",
        lambda *args, **kwargs: {  # noqa: ARG005
            "subreddit": "bravorealhousewives",
            "collection_mode": "exhaustive_window",
            "listing_pages_fetched": 3,
            "max_pages_applied": 500,
            "window_exhaustive_complete": False,
            "search_backfill": {"complete": False, "pages_fetched": 1},
            "seed_urls": {},
            "window_start": "2025-09-30T00:00:00Z",
            "window_end": "2025-10-07T00:00:00Z",
            "terms": ["rhoslc"],
            "hints": {"suggested_include_terms": [], "suggested_exclude_terms": []},
            "threads": [{"reddit_post_id": "post-1", "title": "example"}],
            "totals": {"fetched_rows": 1, "matched_rows": 1, "tracked_flair_rows": 1},
        },
    )
    monkeypatch.setattr(
        reddit_refresh,
        "_update_run",
        lambda run_id_arg, **kwargs: updates.append({"run_id": run_id_arg, **kwargs}),
    )
    monkeypatch.setattr(
        reddit_refresh,
        "get_refresh_run",
        lambda run_id_arg: {"run_id": run_id_arg, "status": "partial"},
    )

    result = reddit_refresh.execute_refresh_run(run_id)

    assert result == {"run_id": run_id, "status": "partial"}
    partial_update = next(item for item in updates if item.get("status") == "partial")
    diagnostics = partial_update.get("diagnostics") or {}
    assert diagnostics.get("failure_reason_code") == "coverage_incomplete"
    assert diagnostics.get("status_resolution") == "coverage_incomplete"
    assert "Analytics remain usable" in str(diagnostics.get("operator_hint") or "")


def test_list_reddit_refresh_backfill_targets_selects_stale_latest_runs(monkeypatch) -> None:
    monkeypatch.setattr(
        reddit_refresh,
        "_canonical_reddit_container_keys_for_season",
        lambda season_id: ["period-preseason", "episode-3", "episode-4", "episode-5", "episode-6"],  # noqa: ARG005
    )
    monkeypatch.setattr(
        reddit_refresh,
        "_fetch_latest_refresh_runs_for_season",
        lambda **kwargs: [  # noqa: ARG005
            {
                "id": "run-failed",
                "canonical_container_key": "episode-3",
                "period_key": "episode-3",
                "status": "failed",
                "tracked_flair_rows": 0,
                "matched_rows": 0,
                "request_payload": {"period_key": "episode-3", "mode": "sync_full"},
                "diagnostics": {},
                "error_message": "boom",
            },
            {
                "id": "run-403",
                "canonical_container_key": "episode-4",
                "period_key": "episode-4",
                "status": "partial",
                "tracked_flair_rows": 0,
                "matched_rows": 0,
                "request_payload": {"period_key": "episode-4", "mode": "sync_full"},
                "diagnostics": {"failure_reason_code": "reddit_http_403"},
                "error_message": "Reddit request failed (403)",
            },
            {
                "id": "run-coverage",
                "canonical_container_key": "episode-5",
                "period_key": "episode-5",
                "status": "partial",
                "tracked_flair_rows": 12,
                "matched_rows": 15,
                "request_payload": {"period_key": "episode-5", "mode": "sync_full"},
                "diagnostics": {"failure_reason_code": "coverage_incomplete"},
                "error_message": None,
            },
            {
                "id": "run-good",
                "canonical_container_key": "episode-6",
                "period_key": "episode-6",
                "status": "completed",
                "tracked_flair_rows": 18,
                "matched_rows": 18,
                "request_payload": {"period_key": "episode-6", "mode": "sync_full"},
                "diagnostics": {},
                "error_message": None,
            },
            {
                "id": "run-debug",
                "canonical_container_key": "debug-max-coverage-20260303a",
                "period_key": "debug-max-coverage-20260303a",
                "status": "completed",
                "tracked_flair_rows": 999,
                "matched_rows": 999,
                "request_payload": {"period_key": "debug-max-coverage-20260303a", "mode": "sync_full"},
                "diagnostics": {},
                "error_message": None,
            },
        ],
    )

    payload = reddit_refresh.list_reddit_refresh_backfill_targets(
        community_id="community-1",
        season_id="season-1",
    )

    target_keys = [item["container_key"] for item in payload["targets"]]
    assert target_keys == ["episode-3", "episode-4", "episode-5"]
    assert payload["summary"]["stale_container_count"] == 3
    assert payload["summary"]["requested_container_count"] == 5
    assert payload["skipped"][0]["container_key"] == "period-preseason"
    assert payload["skipped"][0]["reason"] == "no_previous_run"
    assert payload["skipped"][1]["container_key"] == "episode-6"
    assert payload["skipped"][1]["reason"] == "fresh_successful_run"


def test_fetch_reddit_analytics_extras_season_scope_filters_to_canonical_container_keys(
    monkeypatch,
) -> None:
    fetch_one_calls: list[tuple[str, list[object]]] = []
    fetch_all_calls: list[tuple[str, list[object]]] = []

    monkeypatch.setattr(reddit_refresh, "_column_exists", lambda *args, **kwargs: False)  # noqa: ANN002, ANN003
    monkeypatch.setattr(
        reddit_refresh,
        "_canonical_reddit_container_keys_for_season",
        lambda season_id: ["period-preseason", "episode-1", "period-postseason"],  # noqa: ARG005
    )
    monkeypatch.setattr(
        reddit_refresh,
        "_fetch_latest_refresh_runs_for_season",
        lambda **kwargs: [  # noqa: ARG005
            {
                "id": "run-pre",
                "canonical_container_key": "period-preseason",
                "period_key": "period-preseason",
                "status": "completed",
                "diagnostics": {},
                "error_message": None,
                "updated_at": datetime(2026, 3, 22, 17, 0, tzinfo=UTC),
                "completed_at": datetime(2026, 3, 22, 17, 0, tzinfo=UTC),
            },
            {
                "id": "run-ep1",
                "canonical_container_key": "episode-1",
                "period_key": "episode-1",
                "status": "partial",
                "diagnostics": {"failure_reason_code": "coverage_incomplete"},
                "error_message": None,
                "updated_at": datetime(2026, 3, 22, 18, 0, tzinfo=UTC),
                "completed_at": datetime(2026, 3, 22, 18, 0, tzinfo=UTC),
            },
        ],
    )

    def fake_fetch_one(query, params):  # noqa: ANN001
        fetch_one_calls.append((query, list(params)))
        if "unmapped_post_count" in query:
            return {
                "unmapped_post_count": 28,
                "unmapped_tracked_post_count": 12,
            }
        return {
            "tracked_post_count": 28,
            "detail_scraped_post_count": 20,
            "comment_saved_post_count": 19,
            "media_post_count": 15,
            "mirrored_media_post_count": 10,
            "reported_comment_count": 500,
            "latest_data_timestamp": datetime(2026, 3, 22, 18, 30, tzinfo=UTC),
        }

    def fake_fetch_all(query, params):  # noqa: ANN001
        fetch_all_calls.append((query, list(params)))
        return [
            {
                "canonical_container_key": "period-preseason",
                "matched_post_count": 8,
                "tracked_post_count": 8,
                "show_match_post_count": 5,
                "detail_scraped_post_count": 8,
                "comment_saved_post_count": 8,
                "media_post_count": 6,
                "mirrored_media_post_count": 4,
            },
            {
                "canonical_container_key": "episode-1",
                "matched_post_count": 20,
                "tracked_post_count": 20,
                "show_match_post_count": 12,
                "detail_scraped_post_count": 12,
                "comment_saved_post_count": 11,
                "media_post_count": 9,
                "mirrored_media_post_count": 6,
            },
            {
                "canonical_container_key": "unmapped",
                "matched_post_count": 999,
                "tracked_post_count": 999,
                "show_match_post_count": 999,
                "detail_scraped_post_count": 999,
                "comment_saved_post_count": 999,
                "media_post_count": 999,
                "mirrored_media_post_count": 999,
            },
        ]

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(reddit_refresh.pg, "fetch_all", fake_fetch_all)

    payload = reddit_refresh._fetch_reddit_analytics_extras(
        community_id="community-1",
        scope="season",
        season_id="season-1",
    )

    assert payload["coverage"]["tracked_post_count"] == 28
    assert payload["coverage"]["container_count"] == 3
    assert payload["coverage"]["stale_container_count"] == 2
    assert payload["coverage"]["recovered_container_count"] == 1
    assert [item["container_key"] for item in payload["container_statuses"]] == [
        "period-preseason",
        "episode-1",
        "period-postseason",
    ]
    assert payload["container_statuses"][2]["stale_reason_code"] == "no_previous_run"
    assert payload["freshness"]["latest_run_status"] == "partial"
    assert any("canonical_container_key" in query for query, _params in fetch_one_calls)
    assert any("canonical_container_key" in query for query, _params in fetch_all_calls)
    coverage_query = next(params for query, params in fetch_one_calls if "latest_data_timestamp" in query)
    assert coverage_query[-1] == ["period-preseason", "episode-1", "period-postseason"]
    assert payload["coverage"]["scope"] == "canonical_windows"
    assert payload["coverage"]["unmapped_post_count"] == 28


def test_run_reddit_refresh_worker_loop_once_returns_one_when_no_work(monkeypatch) -> None:
    monkeypatch.setattr(reddit_refresh, "claim_next_refresh_run", lambda **kwargs: None)

    result = reddit_refresh.run_reddit_refresh_worker_loop(worker_id="worker-1", once=True, poll_seconds=0.2)

    assert result == 1


def test_run_reddit_refresh_worker_loop_continues_after_run_failure(monkeypatch) -> None:
    run_id = "63a7be5d-0000-4000-8000-000000000101"
    claims = [
        {"id": run_id, "attempt_count": 1},
        None,
    ]
    claim_calls = 0
    execute_calls: list[dict] = []

    class StopLoopError(Exception):
        pass

    def fake_claim_next_refresh_run(**kwargs):  # noqa: ANN001
        nonlocal claim_calls
        claim_calls += 1
        return claims.pop(0)

    def fake_execute_refresh_run(run_id_arg, **kwargs):  # noqa: ANN001
        execute_calls.append({"run_id": run_id_arg, **kwargs})
        raise RuntimeError("simulated refresh failure")

    monkeypatch.setattr(reddit_refresh, "claim_next_refresh_run", fake_claim_next_refresh_run)
    monkeypatch.setattr(reddit_refresh, "execute_refresh_run", fake_execute_refresh_run)
    monkeypatch.setattr(reddit_refresh.time, "sleep", lambda _seconds: (_ for _ in ()).throw(StopLoopError))

    with pytest.raises(StopLoopError):
        reddit_refresh.run_reddit_refresh_worker_loop(worker_id="worker-1", once=False, poll_seconds=0.2)

    assert claim_calls == 2
    assert execute_calls == [
        {
            "run_id": run_id,
            "preclaimed_run": {"id": run_id, "attempt_count": 1},
            "worker_id": "worker-1",
            "raise_on_failure": False,
        }
    ]


def test_execute_refresh_run_suppresses_non_403_failure_when_requested(monkeypatch) -> None:
    run_id = "63a7be5d-0000-4000-8000-000000000102"
    run_row = {
        "id": run_id,
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "season",
        "subreddit": "bravorealhousewives",
        "request_payload": {"mode": "sync_posts"},
        "claim_token": "claim-token-1",
    }
    updates: list[dict] = []

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", lambda *args, **kwargs: run_row)  # noqa: ANN002, ANN003, ARG005
    monkeypatch.setattr(
        reddit_refresh,
        "_discover_window",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("simulated refresh failure")),  # noqa: ARG005
    )
    monkeypatch.setattr(
        reddit_refresh,
        "_update_run",
        lambda run_id_arg, **kwargs: updates.append({"run_id": run_id_arg, **kwargs}),  # noqa: ANN001
    )
    monkeypatch.setattr(reddit_refresh, "_touch_refresh_run_heartbeat", lambda **kwargs: None)
    monkeypatch.setattr(
        reddit_refresh,
        "get_refresh_run",
        lambda run_id_arg: {"run_id": run_id_arg, "status": "failed"},
    )

    result = reddit_refresh.execute_refresh_run(run_id, raise_on_failure=False)

    assert result == {"run_id": run_id, "status": "failed"}
    failed_update = next((item for item in updates if item.get("status") == "failed"), None)
    assert failed_update is not None
    assert failed_update["release_claim"] is True
    assert failed_update["claim_token"] == "claim-token-1"
    assert failed_update["error_message"] == "simulated refresh failure"


def test_execute_refresh_run_sync_details_emits_terminal_summary(monkeypatch) -> None:
    run_id = "63a7be5d-0000-4000-8000-000000000099"
    run_row = {
        "id": run_id,
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "community:community-1:season:season-1:container:period-preseason",
        "subreddit": "bravorealhousewives",
        "request_payload": {"mode": "sync_details", "force_rescrape": False},
        "status": "running",
        "claim_token": "claim-token-1",
        "updated_at": datetime.now(tz=UTC),
    }
    updates: list[dict] = []

    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", lambda *args, **kwargs: run_row)  # noqa: ANN002, ANN003, ARG005
    monkeypatch.setattr(reddit_refresh, "_touch_refresh_run_heartbeat", lambda **kwargs: None)
    monkeypatch.setattr(reddit_refresh, "_column_exists", lambda *args, **kwargs: False)  # noqa: ANN002, ANN003
    monkeypatch.setattr(reddit_refresh, "_fetch_post_comments_tree", lambda post_id: [])  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh, "_upsert_comments", lambda rows, *, conn: 0)  # noqa: ARG005
    monkeypatch.setattr(
        reddit_refresh, "get_refresh_run", lambda run_id_arg: {"run_id": run_id_arg, "status": "completed"}
    )  # noqa: ARG005
    monkeypatch.setattr(
        reddit_refresh,
        "_update_run",
        lambda run_id_arg, **kwargs: updates.append({"run_id": run_id_arg, **kwargs}),
    )

    class _FakeCursor:
        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN204
            return False

        def execute(self, sql, params):  # noqa: ANN001
            self.sql = sql
            self.params = params

        def fetchall(self):  # noqa: ANN204
            return [
                {
                    "reddit_post_id": "abc123",
                    "url": "https://reddit.com/r/BravoRealHousewives/comments/abc123/thread/",
                    "raw_payload": {},
                }
            ]

    class _FakeConnection:
        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN204
            return False

    monkeypatch.setattr(reddit_refresh.pg, "db_connection", lambda: _FakeConnection())
    monkeypatch.setattr(reddit_refresh.pg, "db_cursor", lambda conn=None: _FakeCursor())  # noqa: ARG005

    result = reddit_refresh.execute_refresh_run(run_id, worker_id="worker-1")

    assert result["status"] == "completed"
    completed_update = next(item for item in updates if item.get("status") in {"completed", "partial"})
    diagnostics = completed_update["diagnostics"]
    assert diagnostics["terminal_summary"]["mode"] == "sync_details"
    assert diagnostics["terminal_summary"]["status"] == "completed"
    assert diagnostics["terminal_summary"]["detail_posts_total"] == 1
    assert diagnostics["terminal_summary"]["detail_posts_done"] == 1
    assert diagnostics["progress"]["detail_posts_total"] == 1
    assert diagnostics["progress"]["detail_posts_done"] == 1


def test_execute_refresh_run_sync_full_runs_detail_phase_without_duplicate_inline_comments(monkeypatch) -> None:
    run_id = "63a7be5d-0000-4000-8000-000000000100"
    run_row = {
        "id": run_id,
        "community_id": "community-1",
        "season_id": "season-1",
        "period_key": "community:community-1:season:season-1:container:episode-18",
        "subreddit": "bravorealhousewives",
        "request_payload": {
            "mode": "sync_full",
            "fetch_comments": True,
            "comment_delta_only": True,
            "force_rescrape": False,
        },
        "status": "running",
        "claim_token": "claim-token-1",
        "updated_at": datetime.now(tz=UTC),
    }
    updates: list[dict] = []
    comment_fetches: list[str] = []

    monkeypatch.setattr(reddit_refresh, "_touch_refresh_run_heartbeat", lambda **kwargs: None)
    monkeypatch.setattr(reddit_refresh, "_column_exists", lambda *args, **kwargs: False)  # noqa: ANN002, ANN003
    monkeypatch.setattr(
        reddit_refresh,
        "_discover_window",
        lambda payload, progress_callback=None: {  # noqa: ARG005
            "subreddit": "bravorealhousewives",
            "window_start": "2026-01-20T05:00:00.000Z",
            "window_end": "2026-01-27T05:00:00.000Z",
            "window_exhaustive_complete": True,
            "listing_pages_fetched": 1,
            "terms": ["rhoslc"],
            "hints": {"suggested_include_terms": [], "suggested_exclude_terms": []},
            "threads": [
                {
                    "reddit_post_id": "abc123",
                    "text": "Body",
                    "num_comments": 4,
                    "link_flair_text": "Salt Lake City",
                }
            ],
            "totals": {"fetched_rows": 1, "matched_rows": 1, "tracked_flair_rows": 1},
        },
    )
    monkeypatch.setattr(reddit_refresh, "_is_result_incomplete", lambda result: (False, False))  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh, "_upsert_posts", lambda rows, *, conn: None)  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh, "_replace_period_matches", lambda **kwargs: None)
    monkeypatch.setattr(
        reddit_refresh,
        "_fetch_post_comments_tree",
        lambda post_id: comment_fetches.append(post_id) or [],
    )
    monkeypatch.setattr(reddit_refresh, "_upsert_comments", lambda rows, *, conn: 0)  # noqa: ARG005
    monkeypatch.setattr(
        reddit_refresh, "get_refresh_run", lambda run_id_arg: {"run_id": run_id_arg, "status": "completed"}
    )  # noqa: ARG005
    monkeypatch.setattr(
        reddit_refresh,
        "_update_run",
        lambda run_id_arg, **kwargs: updates.append({"run_id": run_id_arg, **kwargs}),
    )

    class _FakeCursor:
        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN204
            return False

        def execute(self, sql, params):  # noqa: ANN001
            self.sql = sql
            self.params = params

        def fetchall(self):  # noqa: ANN204
            return [
                {
                    "reddit_post_id": "abc123",
                    "url": "https://reddit.com/r/BravoRealHousewives/comments/abc123/thread/",
                    "raw_payload": {},
                }
            ]

    class _FakeConnection:
        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN204
            return False

    monkeypatch.setattr(reddit_refresh.pg, "db_connection", lambda: _FakeConnection())
    monkeypatch.setattr(reddit_refresh.pg, "db_cursor", lambda conn=None: _FakeCursor())  # noqa: ARG005

    result = reddit_refresh.execute_refresh_run(run_id, preclaimed_run=run_row, worker_id="worker-1")

    assert result["status"] == "completed"
    assert comment_fetches == ["abc123"]
    completed_update = next(item for item in updates if item.get("status") in {"completed", "partial"})
    diagnostics = completed_update["diagnostics"]
    assert diagnostics["mode"] == "sync_full"
    assert diagnostics["comments"]["enabled"] is False
    assert diagnostics["detail_posts_total"] == 1
    assert diagnostics["detail_posts_done"] == 1
    assert diagnostics["terminal_summary"]["mode"] == "sync_full"


def test_list_reddit_community_posts_applies_flair_and_container_filters(monkeypatch) -> None:
    captured_queries: list[tuple[str, list]] = []

    def fake_fetch_one(query, params):  # noqa: ANN001
        captured_queries.append((query, list(params)))
        return {"total_count": 1}

    def fake_fetch_all(query, params):  # noqa: ANN001
        captured_queries.append((query, list(params)))
        return [
            {
                "reddit_post_id": "abc123",
                "title": "RHOSLC post",
                "selftext": "body",
                "url": "https://reddit.com/abc123",
                "permalink": "https://reddit.com/abc123",
                "author": "poster",
                "score": 22,
                "num_comments": 14,
                "posted_at": datetime(2025, 9, 1, 0, 0, tzinfo=UTC),
                "link_flair_text": "Salt Lake City",
                "source_sorts": ["new"],
                "matched_terms": ["rhoslc"],
                "matched_cast_terms": [],
                "cross_show_terms": [],
                "is_show_match": True,
                "passes_flair_filter": True,
                "match_score": 65,
                "flair_mode": "all",
            }
        ]

    monkeypatch.setattr(reddit_refresh, "_column_exists", lambda schema, table, column, **kwargs: True)  # noqa: ARG005
    monkeypatch.setattr(reddit_refresh.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(reddit_refresh.pg, "fetch_all", fake_fetch_all)

    payload = reddit_refresh.list_reddit_community_posts(
        community_id="community-1",
        scope="season",
        season_id="season-1",
        container_key="period-preseason",
        flair_key=" Salt Lake City ",
        page=1,
        per_page=10,
    )

    assert payload["scope"] == "season"
    assert payload["flair_key"] == "salt lake city"
    assert payload["pagination"]["total_count"] == 1
    assert payload["posts"][0]["reddit_post_id"] == "abc123"
    assert any("period-preseason" in params for _query, params in captured_queries)
    assert any("canonical_flair_key" in query for query, _ in captured_queries)


def test_get_reddit_community_flair_detail_requires_flair_key() -> None:
    with pytest.raises(ValueError, match="flair_key is required"):
        reddit_refresh.get_reddit_community_flair_detail(
            community_id="community-1",
            flair_key="",
            scope="all",
            season_id=None,
        )
