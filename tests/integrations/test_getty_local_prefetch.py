from __future__ import annotations

from contextlib import contextmanager

from trr_backend.integrations import getty_local_prefetch


def test_fetch_person_getty_prefetch_payload_discovery_mode_skips_grouped_events(monkeypatch) -> None:
    search_calls: list[dict[str, object]] = []
    grouped_event_calls: list[dict[str, object]] = []

    @contextmanager
    def _fake_bridge():
        yield getty_local_prefetch.LocalGettyBridge(
            session=object(),  # type: ignore[arg-type]
            auth_details={"auth_mode": "chrome_profile_browser_session"},
            search_page_fetcher=lambda url: ("<html></html>", url, 200),
        )

    def _fake_search_editorial_assets(phrase: str, **kwargs):
        search_calls.append({"phrase": phrase, **kwargs})
        editorial_id = phrase.lower().replace(" ", "-")
        return [
            {
                "editorial_id": editorial_id,
                "detail_url": f"https://www.gettyimages.com/detail/news-photo/{editorial_id}/1",
                "original_image_url": f"https://media.gettyimages.com/id/{editorial_id}/photo/example.jpg",
            }
        ]

    def _fake_search_grouped_events(*args, **kwargs):
        grouped_event_calls.append({"args": args, "kwargs": kwargs})
        return []

    monkeypatch.setattr(getty_local_prefetch, "local_getty_bridge", _fake_bridge)
    monkeypatch.setattr(
        getty_local_prefetch.getty_integration,
        "search_editorial_assets",
        _fake_search_editorial_assets,
    )
    monkeypatch.setattr(
        getty_local_prefetch.getty_integration,
        "search_grouped_events",
        _fake_search_grouped_events,
    )

    payload = getty_local_prefetch.fetch_person_getty_prefetch_payload(
        "Brandi Glanville",
        show_name="The Real Housewives of Beverly Hills",
        mode="discovery",
    )

    assert [call["phrase"] for call in search_calls] == [
        "Brandi Glanville Bravo",
        "Brandi Glanville",
    ]
    assert all(call["include_details"] is False for call in search_calls)
    assert all(call["query_params"] == {"sort": "newest"} for call in search_calls)
    assert grouped_event_calls == []
    assert payload["prefetch_mode"] == "discovery"
    assert payload["discovery_ready"] is True
    assert payload["enrichment_status"] == "pending"
    assert payload["candidate_manifest_total"] == 2
    assert payload["detail_enrichment_total"] == 2
    assert payload["merged_events_total"] == 0
    assert payload["deferred_editorial_ids"] == [
        "brandi-glanville",
        "brandi-glanville-bravo",
    ]
    assert payload["discovery_manifest"] == payload["merged"]


def test_fetch_person_getty_prefetch_payload_emits_query_progress(monkeypatch) -> None:
    @contextmanager
    def _fake_bridge():
        yield getty_local_prefetch.LocalGettyBridge(
            session=object(),  # type: ignore[arg-type]
            auth_details={"auth_mode": "chrome_profile_browser_session", "auth_warning": None},
            search_page_fetcher=lambda url: ("<html></html>", url, 200),
        )

    def _fake_search_editorial_assets(phrase: str, **kwargs):
        candidate_progress_cb = kwargs.get("candidate_progress_cb")
        if callable(candidate_progress_cb):
            candidate_progress_cb(
                {
                    "type": "page",
                    "phase": "search",
                    "phrase": phrase,
                    "query_url": f"https://www.gettyimages.com/search?q={phrase}",
                    "requested_page": 1,
                    "current_page": 1,
                    "page_candidate_count": 60,
                    "new_unique_count": 60,
                    "fetched_candidates_total": 60,
                    "termination_reason": None,
                    "site_image_total": 4823,
                    "site_event_total": 340,
                    "site_video_total": 62,
                }
            )
        editorial_id = phrase.lower().replace(" ", "-")
        return [
            {
                "editorial_id": editorial_id,
                "detail_url": f"https://www.gettyimages.com/detail/news-photo/{editorial_id}/1",
                "original_image_url": f"https://media.gettyimages.com/id/{editorial_id}/photo/example.jpg",
            }
        ]

    monkeypatch.setattr(getty_local_prefetch, "local_getty_bridge", _fake_bridge)
    monkeypatch.setattr(
        getty_local_prefetch.getty_integration,
        "search_editorial_assets",
        _fake_search_editorial_assets,
    )
    monkeypatch.setattr(
        getty_local_prefetch.getty_integration,
        "search_grouped_events",
        lambda *args, **kwargs: [],
    )

    progress_events: list[dict[str, object]] = []
    payload = getty_local_prefetch.fetch_person_getty_prefetch_payload(
        "Brandi Glanville",
        mode="discovery",
        progress_cb=lambda event: progress_events.append(dict(event)),
    )

    assert payload["merged_total"] == 2
    assert progress_events[0]["phase"] == "bridge_ready"
    assert any(event.get("type") == "query_started" for event in progress_events)
    assert any(event.get("type") == "page" for event in progress_events)
    assert any(event.get("type") == "query_completed" for event in progress_events)
    assert progress_events[-1]["status"] == "completed"


def test_fetch_person_getty_prefetch_payload_requires_authenticated_profile(monkeypatch) -> None:
    @contextmanager
    def _fake_bridge():
        yield getty_local_prefetch.LocalGettyBridge(
            session=object(),  # type: ignore[arg-type]
            auth_details={
                "auth_mode": "chrome_profile_browser_session",
                "auth_warning": "Codex Getty Chrome profile is not authenticated; Getty scraping may be truncated.",
            },
            search_page_fetcher=lambda url: ("<html></html>", url, 200),
        )

    monkeypatch.delenv("TRR_GETTY_EMAIL", raising=False)
    monkeypatch.delenv("TRR_GETTY_PASSWORD", raising=False)
    monkeypatch.setattr(getty_local_prefetch, "load_env", lambda: None)
    monkeypatch.setattr(getty_local_prefetch, "local_getty_bridge", _fake_bridge)
    monkeypatch.setattr(
        getty_local_prefetch,
        "_build_isolated_bridge_from_bridge",
        lambda bridge: None,
    )

    try:
        getty_local_prefetch.fetch_person_getty_prefetch_payload(
            "Brandi Glanville",
            mode="discovery",
        )
    except getty_local_prefetch.GettyPrefetchSessionError as exc:
        assert exc.code == "getty_profile_not_authenticated"
        assert "not authenticated" in str(exc).lower()
    else:
        raise AssertionError("expected GettyPrefetchSessionError")


def test_fetch_person_getty_prefetch_payload_reports_login_bootstrap_failure(monkeypatch) -> None:
    @contextmanager
    def _fake_bridge():
        yield getty_local_prefetch.LocalGettyBridge(
            session=object(),  # type: ignore[arg-type]
            auth_details={
                "auth_mode": "chrome_profile_browser_session",
                "auth_warning": "Codex Getty Chrome profile is not authenticated; Getty scraping may be truncated.",
            },
            search_page_fetcher=lambda url: ("<html></html>", url, 200),
        )

    monkeypatch.setenv("TRR_GETTY_EMAIL", "hidden@example.com")
    monkeypatch.setenv("TRR_GETTY_PASSWORD", "hidden-password")
    monkeypatch.setattr(getty_local_prefetch, "load_env", lambda: None)
    monkeypatch.setattr(getty_local_prefetch, "local_getty_bridge", _fake_bridge)
    monkeypatch.setattr(
        getty_local_prefetch,
        "_build_isolated_bridge_from_bridge",
        lambda bridge: None,
    )

    try:
        getty_local_prefetch.fetch_person_getty_prefetch_payload(
            "Brandi Glanville",
            mode="discovery",
        )
    except getty_local_prefetch.GettyPrefetchSessionError as exc:
        assert exc.code == "getty_login_bootstrap_failed"
        assert "login bootstrap failed" in str(exc).lower()
    else:
        raise AssertionError("expected GettyPrefetchSessionError")


def test_fetch_person_getty_prefetch_payload_fails_fast_on_session_truncation(monkeypatch) -> None:
    @contextmanager
    def _fake_bridge():
        yield getty_local_prefetch.LocalGettyBridge(
            session=object(),  # type: ignore[arg-type]
            auth_details={"auth_mode": "chrome_profile_browser_session", "auth_warning": None},
            search_page_fetcher=lambda url: ("<html></html>", url, 200),
        )

    def _fake_search_editorial_assets(phrase: str, **kwargs):
        summary = kwargs.get("query_summary_out")
        if isinstance(summary, dict):
            summary.update(
                {
                    "query_url": f"https://www.gettyimages.com/search?q={phrase}",
                    "site_image_total": 4823,
                    "fetched_candidates_total": 180,
                    "termination_reason": "pagination_rewrite",
                    "expected_page": 4,
                    "current_page": 1,
                    "pagination_rewrite_detected": True,
                    "response_url": "https://www.gettyimages.com/search?page=1",
                    "first_editorial_ids": ["1", "2", "3"],
                }
            )
        return []

    monkeypatch.setattr(getty_local_prefetch, "local_getty_bridge", _fake_bridge)
    monkeypatch.setattr(getty_local_prefetch, "load_env", lambda: None)
    monkeypatch.setattr(
        getty_local_prefetch,
        "_build_isolated_bridge_from_bridge",
        lambda bridge: None,
    )
    monkeypatch.setattr(
        getty_local_prefetch.getty_integration,
        "search_editorial_assets",
        _fake_search_editorial_assets,
    )
    monkeypatch.setattr(
        getty_local_prefetch.getty_integration,
        "search_grouped_events",
        lambda *args, **kwargs: [],
    )

    try:
        getty_local_prefetch.fetch_person_getty_prefetch_payload(
            "Brandi Glanville",
            mode="discovery",
        )
    except getty_local_prefetch.GettyPrefetchSessionError as exc:
        assert exc.code == "getty_session_truncated"
        assert "truncated after page 3" in str(exc).lower()
    else:
        raise AssertionError("expected GettyPrefetchSessionError")
