from __future__ import annotations

from datetime import UTC, datetime

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


def test_fetch_new_window_exhaustive_terminal_page_before_period_start_is_incomplete(monkeypatch) -> None:
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
    assert complete is False


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
        "analysis_flares": ["Salt Lake City"],
        "analysis_all_flares": ["Salt Lake City"],
        "force_include_flares": ["Salt Lake City"],
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
        "analysis_flares": ["Salt Lake City"],
        "analysis_all_flares": ["Salt Lake City"],
        "force_include_flares": ["Salt Lake City"],
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
        analysis_flares=["Shitpost"],
        analysis_all_flares=[],
        force_include_flares=[],
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
        analysis_flares=["Shitpost"],
        analysis_all_flares=[],
        force_include_flares=[],
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
        analysis_flares=[],
        analysis_all_flares=["Shitpost"],
        force_include_flares=[],
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
        "analysis_flares": ["Shitpost"],
        "analysis_all_flares": [],
        "force_include_flares": [],
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

    def fake_fetch_one(*args, **kwargs):  # noqa: ANN002, ANN003
        calls["count"] += 1
        return run_row if calls["count"] == 1 else None

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
