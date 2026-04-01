from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from trr_backend.repositories import social_season_analytics as social_repo


def test_execute_run_with_inline_worker_registration_registers_and_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(
        social_repo,
        "get_worker_auth_capabilities",
        lambda: {"instagram_authenticated": True, "tiktok_authenticated": True},
    )

    def _fake_heartbeat(worker_id: str, **kwargs):  # noqa: ANN001
        events.append(("start", {"worker_id": worker_id, **kwargs}))
        return {"worker_id": worker_id}

    def _fake_stop(worker_id: str, **kwargs):  # noqa: ANN001
        events.append(("stop", {"worker_id": worker_id, **kwargs}))
        return {"worker_id": worker_id}

    monkeypatch.setattr(social_repo, "update_worker_heartbeat", _fake_heartbeat)
    monkeypatch.setattr(social_repo, "mark_worker_stopped", _fake_stop)
    monkeypatch.setattr(
        social_repo,
        "execute_run",
        lambda run_id, **kwargs: {"run_id": run_id, "status": "completed", "worker_id": kwargs.get("worker_id")},
    )

    payload = social_repo.execute_run_with_inline_worker_registration("run-1", worker_id="inline-worker")

    assert payload["run_id"] == "run-1"
    assert events[0][0] == "start"
    assert events[0][1]["metadata"]["inline_worker"] is True
    assert events[0][1]["metadata"]["auth_capabilities"] == {
        "instagram_authenticated": True,
        "tiktok_authenticated": True,
    }
    assert events[-1][0] == "stop"


def test_start_social_account_catalog_backfill_conflicts_when_start_lock_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(social_repo, "_assert_social_account_profile_exists", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        social_repo,
        "get_active_social_account_catalog_run",
        lambda *_args, **_kwargs: {"run_id": "run-active", "status": "queued"},
    )
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda **_kwargs: nullcontext("conn"))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda **_kwargs: nullcontext("cur"))
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one_with_cursor",
        lambda _cur, query, _params: {"locked": False} if "pg_try_advisory_lock" in query else {"unlocked": True},
    )

    with pytest.raises(social_repo.SocialIngestConflictError) as excinfo:
        social_repo.start_social_account_catalog_backfill("instagram", "bravotv")

    assert excinfo.value.detail["run_id"] == "run-active"
    assert excinfo.value.detail["platform"] == "instagram"


def test_resume_tail_passes_frontier_seed_into_new_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(social_repo, "_normalize_social_account_profile_platform", lambda platform: platform)
    monkeypatch.setattr(social_repo, "_normalize_social_account_profile_handle", lambda handle: handle)
    monkeypatch.setattr(
        social_repo,
        "_latest_account_frontier",
        lambda *_args, **_kwargs: {
            "id": "frontier-1",
            "run_id": "run-old",
            "next_cursor": "cursor-123",
            "total_posts": 50,
            "posts_checked": 30,
            "posts_saved": 30,
            "pages_scanned": 4,
            "last_transport": "public",
            "exhausted": False,
        },
    )
    captured: dict[str, object] = {}

    def _fake_start(platform: str, account_handle: str, **kwargs):  # noqa: ANN001
        captured["platform"] = platform
        captured["account_handle"] = account_handle
        captured.update(kwargs)
        return {"run_id": "run-new", "status": "queued"}

    monkeypatch.setattr(social_repo, "start_social_account_catalog_backfill", _fake_start)

    payload = social_repo.resume_tail_social_account_catalog("instagram", "bravotv")

    assert payload["resumed_from_cursor"] is True
    assert captured["resume_frontier_cursor"] == "cursor-123"
    assert captured["resume_frontier_snapshot"]["run_id"] == "run-old"


def test_refresh_tiktok_post_detail_uses_stored_canonical_url(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeScraper:
        def __init__(self, *, cookies):  # noqa: ANN003
            self.cookies = cookies
            self.urls: list[str] = []

        def _ytdlp_get_video_metadata(self, url: str) -> dict[str, object]:  # noqa: SLF001
            self.urls.append(url)
            return {"id": "123"}

        def _parse_ytdlp_metadata(self, metadata, _config):  # noqa: ANN001, SLF001
            assert metadata == {"id": "123"}
            return SimpleNamespace(video_id="", username="creator", to_dict=lambda: {})

    scraper = _FakeScraper(cookies={})

    monkeypatch.setattr(social_repo, "_load_tiktok_cookies", lambda: {})
    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScraper", lambda cookies: scraper)
    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScrapeConfig", lambda username: {"username": username})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda **_kwargs: nullcontext("conn"))
    monkeypatch.setattr(social_repo, "_upsert_tiktok_post", lambda *_args, **_kwargs: {"id": "post-1"})

    payload = social_repo._refresh_tiktok_post_detail_sync(
        SimpleNamespace(),
        source_id="123",
        account="renamed-handle",
        row_json={"raw_data": {"url": "https://www.tiktok.com/@_/video/123"}},
        detail_job_id=None,
    )

    assert payload["status"] == "success"
    assert scraper.urls == ["https://www.tiktok.com/@_/video/123"]


def test_refresh_post_comments_tiktok_returns_media_mirror_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeScraper:
        comments_auth_failed = False
        last_comment_fetch_reason = ""
        _last_api_fail_reason = ""

        def __init__(self, *, cookies):  # noqa: ANN003
            self.cookies = cookies

        def fetch_comments(self, *_args, **_kwargs):
            return [SimpleNamespace(replies=[])]

    monkeypatch.setattr(social_repo, "get_season_context", lambda _season_id: SimpleNamespace(season_id="season-1"))
    monkeypatch.setattr(social_repo, "_load_tiktok_cookies", lambda: {})
    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScraper", _FakeScraper)
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {"id": "post-1", "account": "creator"},
    )
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda **_kwargs: nullcontext("conn"))
    monkeypatch.setattr(social_repo, "_trim_nested_comment_replies", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(social_repo, "_is_comment_fetch_complete", lambda **_kwargs: True)
    monkeypatch.setattr(social_repo, "_mark_missing_comments_for_anchor", lambda **_kwargs: 0)
    monkeypatch.setattr(social_repo, "_reconcile_post_comment_count", lambda **_kwargs: None)
    monkeypatch.setattr(social_repo, "_count_stored_comments", lambda *_args, **_kwargs: {"post-1": 1})

    def _fake_upsert(*_args, persist_stats=None, **_kwargs):  # noqa: ANN001
        if persist_stats is not None:
            persist_stats["comment_media_mirror_jobs_enqueued"] = 2
            persist_stats["comment_media_mirror_job_enqueue_errors"] = 1
        return 1

    monkeypatch.setattr(social_repo, "_upsert_tiktok_comment_tree", _fake_upsert)

    payload = social_repo.refresh_post_comments("season-1", platform="tiktok", source_id="123", fetch_replies=False)

    assert payload["comment_media_mirror_jobs_enqueued"] == 2
    assert payload["comment_media_mirror_job_enqueue_errors"] == 1


def test_get_tiktok_content_health_uses_description_and_canonical_url(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(social_repo, "_tiktok_filter_sql", lambda **_kwargs: ("p.season_id = %s", ["season-1"]))

    def _fake_fetch_all(query: str, params: list[object]) -> list[dict[str, object]]:
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    payload = social_repo.get_tiktok_content_health("season-1")

    assert payload == {"season_id": "season-1", "thresholds": {}, "posts": []}
    assert "coalesce(p.description, '') as caption" in str(captured["query"])
    assert "raw_data ->> 'url'" in str(captured["query"])
    assert "https://www.tiktok.com/@_/video/" in str(captured["query"])


def test_scrape_shared_tiktok_partition_marks_unreached_boundary_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeScraper:
        def __init__(self, *, cookies):  # noqa: ANN003
            self.cookies = cookies

        def fetch_posts(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(social_repo, "_load_tiktok_cookies", lambda: {})
    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScraper", _FakeScraper)
    fake_scraper = _FakeScraper(cookies={})
    monkeypatch.setattr(
        social_repo,
        "_bootstrap_shared_tiktok_account_context",
        lambda **_kwargs: {"scraper": fake_scraper, "sec_uid": "sec-1", "profile_snapshot": {}, "total_posts": 25},
    )

    _rows, meta = social_repo._scrape_shared_tiktok_posts_partitioned(
        run_id="run-1",
        account_handle="creator",
        config={
            "cursor_start": 0,
            "cursor_end": 100,
            "profile_snapshot": {},
            "discovery_total_posts": 25,
        },
    )

    assert meta["error_code"] == "tiktok_partition_cursor_end_not_reached"
    assert meta["retryable"] is True
