from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

from trr_backend.integrations import getty_local_prefetch


def test_resolve_getty_browser_mode_defaults_to_isolated(monkeypatch) -> None:
    monkeypatch.delenv("TRR_GETTY_BROWSER_MODE", raising=False)
    assert getty_local_prefetch._resolve_getty_browser_mode() == "isolated"

    monkeypatch.setenv("TRR_GETTY_BROWSER_MODE", " cookies ")
    assert getty_local_prefetch._resolve_getty_browser_mode() == "cookies"

    monkeypatch.setenv("TRR_GETTY_BROWSER_MODE", "unexpected")
    assert getty_local_prefetch._resolve_getty_browser_mode() == "isolated"


def test_local_getty_bridge_prefers_isolated_browser_mode(monkeypatch) -> None:
    profile_dir = Path("/tmp/codex-agent")
    attempts: list[tuple[str, Path]] = []
    isolated_bridge = getty_local_prefetch.LocalGettyBridge(
        session=object(),  # type: ignore[arg-type]
        auth_details={"auth_mode": "chrome_profile_browser_login_bootstrap_isolated"},
        profile_dir="/tmp/trr-getty-profile-123",
    )

    monkeypatch.delenv("TRR_GETTY_BROWSER_MODE", raising=False)
    monkeypatch.setattr(getty_local_prefetch, "_iter_profile_dirs", lambda: [profile_dir])
    monkeypatch.setattr(
        getty_local_prefetch,
        "_build_isolated_browser_bridge",
        lambda candidate: attempts.append(("isolated", candidate)) or isolated_bridge,
    )
    monkeypatch.setattr(
        getty_local_prefetch,
        "_build_browser_bridge",
        lambda candidate: attempts.append(("live", candidate)) or None,
    )
    monkeypatch.setattr(
        getty_local_prefetch,
        "_build_cookie_bridge",
        lambda: pytest.fail("cookie fallback should not be used when isolated bridge succeeds"),
    )

    with getty_local_prefetch.local_getty_bridge() as bridge:
        assert bridge is isolated_bridge

    assert attempts == [("isolated", profile_dir)]


def test_local_getty_bridge_live_mode_is_opt_in(monkeypatch) -> None:
    profile_dir = Path("/tmp/codex-agent")
    attempts: list[tuple[str, Path]] = []
    live_bridge = getty_local_prefetch.LocalGettyBridge(
        session=object(),  # type: ignore[arg-type]
        auth_details={"auth_mode": "chrome_profile_browser_session"},
        profile_dir=str(profile_dir),
    )

    monkeypatch.setenv("TRR_GETTY_BROWSER_MODE", "live")
    monkeypatch.setattr(getty_local_prefetch, "_iter_profile_dirs", lambda: [profile_dir])
    monkeypatch.setattr(
        getty_local_prefetch,
        "_build_browser_bridge",
        lambda candidate: attempts.append(("live", candidate)) or live_bridge,
    )
    monkeypatch.setattr(
        getty_local_prefetch,
        "_build_isolated_browser_bridge",
        lambda candidate: attempts.append(("isolated", candidate)) or None,
    )
    monkeypatch.setattr(
        getty_local_prefetch,
        "_build_cookie_bridge",
        lambda: pytest.fail("cookie fallback should not be used when live bridge succeeds"),
    )

    with getty_local_prefetch.local_getty_bridge() as bridge:
        assert bridge is live_bridge

    assert attempts == [("live", profile_dir)]


def test_getty_job_slot_rejects_concurrent_browser_jobs(monkeypatch) -> None:
    @contextmanager
    def _locked(_name: str):
        raise RuntimeError("browser_runtime_locked:getty-prefetch-playwright")
        yield

    monkeypatch.setattr(getty_local_prefetch, "_resolve_getty_max_concurrent_jobs", lambda: 1)
    monkeypatch.setattr(getty_local_prefetch, "exclusive_runtime_lock", _locked)

    with pytest.raises(getty_local_prefetch.GettyPrefetchSessionError) as excinfo:
        with getty_local_prefetch._getty_job_slot():
            pass

    assert excinfo.value.code == "getty_browser_job_locked"


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


def test_fetch_show_getty_prefetch_payload_discovery_mode_uses_show_queries(monkeypatch) -> None:
    search_calls: list[dict[str, object]] = []

    @contextmanager
    def _fake_bridge():
        yield getty_local_prefetch.LocalGettyBridge(
            session=object(),  # type: ignore[arg-type]
            auth_details={"auth_mode": "chrome_profile_browser_session", "auth_warning": None},
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

    monkeypatch.setattr(getty_local_prefetch, "local_getty_bridge", _fake_bridge)
    monkeypatch.setattr(
        getty_local_prefetch.getty_integration,
        "search_editorial_assets",
        _fake_search_editorial_assets,
    )

    payload = getty_local_prefetch.fetch_show_getty_prefetch_payload(
        "The Real Housewives of Beverly Hills",
        season=3,
        episode=2,
        mode="discovery",
    )

    assert [call["phrase"] for call in search_calls] == [
        "The Real Housewives of Beverly Hills Season 3 Episode 2",
        "The Real Housewives of Beverly Hills Bravo",
    ]
    assert all(call["include_details"] is False for call in search_calls)
    assert payload["prefetch_mode"] == "discovery"
    assert payload["merged_total"] == 2
    assert payload["merged_events_total"] == 0
    assert payload["detail_enrichment_total"] == 2
    assert payload["deferred_editorial_ids"] == [
        "the-real-housewives-of-beverly-hills-bravo",
        "the-real-housewives-of-beverly-hills-season-3-episode-2",
    ]


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


def test_fetch_person_getty_prefetch_payload_retries_with_isolated_bridge(monkeypatch) -> None:
    def base_fetcher(url: str) -> tuple[str, str, int]:
        return "<html></html>", url, 200

    def isolated_fetcher(url: str) -> tuple[str, str, int]:
        return "<html></html>", url, 200

    query_attempts: list[tuple[str, str]] = []

    base_bridge = getty_local_prefetch.LocalGettyBridge(
        session=object(),  # type: ignore[arg-type]
        auth_details={"auth_mode": "chrome_profile_browser_session", "auth_warning": None},
        search_page_fetcher=base_fetcher,
        profile_dir="/tmp/codex-agent",
    )
    isolated_bridge = getty_local_prefetch.LocalGettyBridge(
        session=object(),  # type: ignore[arg-type]
        auth_details={"auth_mode": "chrome_profile_browser_login_bootstrap_isolated", "auth_warning": None},
        search_page_fetcher=isolated_fetcher,
        profile_dir="/tmp/trr-getty-profile-123",
    )

    @contextmanager
    def _fake_bridge():
        yield base_bridge

    def _fake_search_editorial_assets(phrase: str, **kwargs):
        summary = kwargs.get("query_summary_out")
        search_page_fetcher = kwargs.get("search_page_fetcher")
        attempt_source = "isolated" if search_page_fetcher is isolated_fetcher else "live"
        query_attempts.append((phrase, attempt_source))
        if isinstance(summary, dict):
            summary.update(
                {
                    "query_url": f"https://www.gettyimages.com/search?q={phrase}",
                    "site_image_total": 4823,
                    "response_url": f"https://www.gettyimages.com/search?q={phrase}",
                }
            )
            if search_page_fetcher is base_fetcher:
                summary.update(
                    {
                        "fetched_candidates_total": 180,
                        "termination_reason": "pagination_rewrite",
                        "expected_page": 4,
                        "current_page": 1,
                        "pagination_rewrite_detected": True,
                        "first_editorial_ids": ["1", "2", "3"],
                    }
                )
                return []
            editorial_id = phrase.lower().replace(" ", "-")
            summary.update(
                {
                    "fetched_candidates_total": 1,
                    "termination_reason": "natural_exhaustion",
                    "expected_page": 1,
                    "current_page": 1,
                    "first_editorial_ids": [editorial_id],
                }
            )
            return [
                {
                    "editorial_id": editorial_id,
                    "detail_url": f"https://www.gettyimages.com/detail/news-photo/{editorial_id}/1",
                    "original_image_url": f"https://media.gettyimages.com/id/{editorial_id}/photo/example.jpg",
                }
            ]
        return []

    monkeypatch.setattr(getty_local_prefetch, "load_env", lambda: None)
    monkeypatch.setattr(getty_local_prefetch, "local_getty_bridge", _fake_bridge)
    monkeypatch.setattr(
        getty_local_prefetch,
        "_build_isolated_bridge_from_bridge",
        lambda bridge: isolated_bridge,
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

    payload = getty_local_prefetch.fetch_person_getty_prefetch_payload(
        "Brandi Glanville",
        mode="discovery",
    )

    assert query_attempts == [
        ("Brandi Glanville Bravo", "live"),
        ("Brandi Glanville Bravo", "isolated"),
        ("Brandi Glanville", "isolated"),
    ]
    assert payload["auth_mode"] == "chrome_profile_browser_login_bootstrap_isolated"
    assert payload["session_validated"] is True
    assert payload["session_truncated"] is False
    assert payload["merged_total"] == 2


def test_build_query_specs_uses_shared_query_plan() -> None:
    """_build_query_specs delegates to the shared query-plan builder."""
    specs = getty_local_prefetch._build_query_specs("Lisa Barlow")
    assert len(specs) == 2
    assert specs[0]["phrase"] == "Lisa Barlow Bravo"
    assert specs[0]["scope"] == "bravo"
    assert specs[0]["label"] == "Bravo Search"
    assert specs[0]["query_params"]["sort"] == "newest"
    assert specs[1]["phrase"] == "Lisa Barlow"
    assert specs[1]["scope"] == "broad"
    assert specs[1]["label"] == "Broad Search"


def test_build_query_specs_with_credit_rows_adds_network_queries() -> None:
    """When credit_show_rows are provided, extra network queries appear."""
    credit_rows = [
        {"networks": ["Bravo", "E!"], "streaming_providers": ["Peacock"]},
    ]
    specs = getty_local_prefetch._build_query_specs(
        "Kyle Richards",
        credit_show_rows=credit_rows,
    )
    phrases = [s["phrase"] for s in specs]
    assert "Kyle Richards Bravo" in phrases
    assert "Kyle Richards" in phrases
    assert "Kyle Richards Peacock" in phrases
    # All should have sort=newest default
    assert all(s["query_params"].get("sort") == "newest" for s in specs)


def test_prefetch_full_mode_grouped_events_match_live_params(monkeypatch) -> None:
    """Full-mode prefetch grouped-event calls should use the same params as the live backend."""
    grouped_calls: list[dict[str, object]] = []

    @contextmanager
    def _fake_bridge():
        yield getty_local_prefetch.LocalGettyBridge(
            session=object(),  # type: ignore[arg-type]
            auth_details={"auth_mode": "chrome_profile_browser_session"},
            search_page_fetcher=lambda url: ("<html></html>", url, 200),
        )

    def _fake_search_editorial_assets(phrase: str, **kwargs):
        return [{"editorial_id": phrase.lower().replace(" ", "-"), "detail_url": "https://example.com/1"}]

    def _fake_search_grouped_events(*args, **kwargs):
        grouped_calls.append({"args": args, "kwargs": kwargs})
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

    getty_local_prefetch.fetch_person_getty_prefetch_payload(
        "Brandi Glanville",
        mode="full",
    )

    assert len(grouped_calls) == 2

    # Bravo grouped: full_scan_person_assets=True
    bravo_call = grouped_calls[0]["kwargs"]
    assert bravo_call["source_query_scope"] == "bravo"
    assert bravo_call["full_scan_person_assets"] is True

    # Broad grouped: person_match_required=True, minimum_grouped_image_count=2
    broad_call = grouped_calls[1]["kwargs"]
    assert broad_call["source_query_scope"] == "broad"
    assert broad_call["person_match_required"] is True
    assert broad_call["minimum_grouped_image_count"] == 2
    assert broad_call["query_params"]["sort"] == "best"
    assert "numberofpeople" in broad_call["query_params"]


def test_fetch_person_getty_prefetch_payload_auto_mode_falls_back_to_local_browser(monkeypatch) -> None:
    @contextmanager
    def _fake_bridge():
        yield getty_local_prefetch.LocalGettyBridge(
            session=object(),  # type: ignore[arg-type]
            auth_details={"auth_mode": "chrome_profile_browser_session", "auth_warning": None},
            search_page_fetcher=lambda url: ("<html></html>", url, 200),
        )

    def _fake_search_editorial_assets(phrase: str, **kwargs):
        return [
            {
                "editorial_id": phrase.lower().replace(" ", "-"),
                "detail_url": f"https://www.gettyimages.com/detail/news-photo/{phrase}/1",
            }
        ]

    monkeypatch.setattr(getty_local_prefetch, "_getty_remote_transport_enabled", lambda: True)
    monkeypatch.setattr(
        getty_local_prefetch,
        "probe_getty_remote_access",
        lambda **_kwargs: {
            "platform": "getty",
            "ready": False,
            "reason": "challenge_page",
            "proxy_fingerprint": "gate.decodo.com:7000:decodo",
        },
    )
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

    payload = getty_local_prefetch.fetch_person_getty_prefetch_payload(
        "Brandi Glanville",
        mode="discovery",
        transport_mode="auto",
    )

    assert payload["getty_transport_mode"] == "local_browser"
    assert payload["getty_runtime_probe_status"] == "blocked"
    assert payload["getty_runtime_probe_reason"] == "challenge_page"
    assert payload["getty_fallback_invoked"] is True
    assert payload["getty_primary_failure_reason"] == "challenge_page"


def test_fetch_person_getty_prefetch_payload_decodo_remote_carries_transport_metadata(monkeypatch) -> None:
    remote_bridge = getty_local_prefetch.LocalGettyBridge(
        session=object(),  # type: ignore[arg-type]
        auth_details={
            "auth_mode": "decodo_remote",
            "auth_warning": None,
            "getty_proxy_fingerprint": "gate.decodo.com:7000:decodo",
        },
        search_page_fetcher=None,
    )

    def _fake_search_editorial_assets(phrase: str, **kwargs):
        summary = kwargs.get("query_summary_out")
        if isinstance(summary, dict):
            summary.update(
                {
                    "query_url": f"https://www.gettyimages.com/search?q={phrase}",
                    "fetched_candidates_total": 1,
                    "termination_reason": "natural_exhaustion",
                    "current_page": 1,
                    "expected_page": 1,
                }
            )
        return [{"editorial_id": phrase.lower().replace(" ", "-"), "detail_url": "https://example.com/1"}]

    monkeypatch.setattr(getty_local_prefetch, "_build_remote_getty_bridge", lambda: remote_bridge)
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

    payload = getty_local_prefetch.fetch_person_getty_prefetch_payload(
        "Brandi Glanville",
        mode="discovery",
        transport_mode="decodo_remote",
    )

    assert payload["getty_transport_mode"] == "decodo_remote"
    assert payload["getty_proxy_fingerprint"] == "gate.decodo.com:7000:decodo"
    assert payload["getty_runtime_probe_status"] == "not_run"
    assert payload["getty_fallback_invoked"] is False


def test_probe_getty_remote_access_includes_request_exception_diagnostics(monkeypatch) -> None:
    class _Session:
        def close(self) -> None:
            return None

    def _fake_search_editorial_assets(phrase: str, **kwargs):
        summary = kwargs.get("query_summary_out")
        if isinstance(summary, dict):
            summary.update(
                {
                    "query_url": f"https://www.gettyimages.com/search?q={phrase}",
                    "candidate_count": 0,
                    "termination_reason": "request_exception",
                    "request_exception_class": "ProxyError",
                    "request_exception_message": "proxy tunnel failed",
                    "request_http_status": 407,
                    "request_redirect_url": None,
                    "fetched_candidates_total": 0,
                }
            )
        return []

    monkeypatch.setattr(getty_local_prefetch, "load_env", lambda: None)
    monkeypatch.setattr(
        getty_local_prefetch.getty_transport,
        "build_remote_getty_session",
        lambda: (_Session(), {"getty_proxy_fingerprint": "gate.decodo.com:7000:decodo"}),
    )
    monkeypatch.setattr(
        getty_local_prefetch.getty_integration,
        "search_editorial_assets",
        _fake_search_editorial_assets,
    )

    payload = getty_local_prefetch.probe_getty_remote_access(probe_phrase="Bravo")

    assert payload["ready"] is False
    assert payload["queries"][0]["request_exception_class"] == "ProxyError"
    assert payload["queries"][0]["request_exception_message"] == "proxy tunnel failed"
    assert payload["queries"][0]["request_http_status"] == 407
    assert payload["queries"][0]["fetched_candidates_total"] == 0
