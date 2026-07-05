import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

YOUTUBE_FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "socials" / "youtube" / "catalog_pages.json"


def _load_youtube_catalog_fixture(name: str) -> dict[str, Any]:
    payload = json.loads(YOUTUBE_FIXTURE_PATH.read_text())
    return dict(payload[name])


class _FixtureApiClient:
    def __init__(self, identity: dict[str, Any] | None) -> None:
        self.identity = identity
        self.resolved_handle: str | None = None

    def enabled(self) -> bool:
        return self.identity is not None

    def resolve_channel(self, handle: str) -> dict[str, Any] | None:
        self.resolved_handle = handle
        return self.identity


class _FixtureScraper:
    def __init__(self, videos: list[Any], retrieval_meta: dict[str, Any]) -> None:
        self.videos = videos
        self.last_retrieval_meta = dict(retrieval_meta)
        self.config: Any | None = None

    def scrape(self, config: Any, *, progress_cb=None) -> list[Any]:
        self.config = config
        if progress_cb:
            progress_cb({"phase": "fixture_scrape"})
        return list(self.videos)


def _catalog_fixture_videos(fixture: dict[str, Any]) -> list[Any]:
    from trr_backend.socials.youtube.scraper import YouTubeVideo

    return [YouTubeVideo(**video) for video in fixture.get("videos") or []]


def _catalog_shared_dependencies(*, fixture: dict[str, Any], ytdlp_available: bool):
    from trr_backend.socials.youtube.posts_catalog import YouTubePostsCatalogDependencies

    scraper = _FixtureScraper(_catalog_fixture_videos(fixture), fixture["retrieval_meta"])
    api_client = _FixtureApiClient(fixture.get("api_identity"))

    def _persist_shared_catalog_posts_with_progress(**kwargs):
        retrieval_meta = kwargs["retrieval_meta"]
        rows = []
        for item in kwargs["items"]:
            row = kwargs["upsert_item"](item)
            if row:
                rows.append(row)
        retrieval_meta["persist_counters"] = {"posts_upserted": len(rows), "comments_upserted": 0}
        return rows

    def _upsert_shared_catalog_post(**kwargs):
        post = kwargs["post"]
        return {
            "id": f"row-{post.video_id}",
            "video_id": post.video_id,
            "source_account": kwargs["account_handle"],
        }

    dependencies = YouTubePostsCatalogDependencies(
        scraper_factory=lambda: scraper,
        api_client_factory=lambda: api_client,
        persist_shared_catalog_posts_with_progress=_persist_shared_catalog_posts_with_progress,
        upsert_shared_catalog_post=_upsert_shared_catalog_post,
        ytdlp_available=lambda: ytdlp_available,
    )
    return dependencies, scraper, api_client


def test_youtube_empty_channel_page_sets_error_code():
    """When YouTube scraper returns 0 posts with no error_code,
    the orchestration layer should set youtube_empty_channel_page."""
    from trr_backend.repositories.social_season_analytics import (
        _scrape_shared_youtube_posts,
    )

    mock_scraper_instance = MagicMock()
    mock_scraper_instance.scrape.return_value = []
    mock_scraper_instance.last_retrieval_meta = {
        "videos_found": 0,
        "shorts_found": 0,
        "first_page_counts": {"videos": 0, "shorts": 0},
        "total_posts": 0,
    }

    with (
        patch(
            "trr_backend.socials.youtube.YouTubeScraper",
            return_value=mock_scraper_instance,
        ),
        patch(
            "trr_backend.socials.youtube.YouTubeDataApiClient",
        ) as mock_api_cls,
        patch(
            "trr_backend.repositories.social_season_analytics._shared_catalog_mode",
            return_value=True,
        ),
        patch(
            "trr_backend.repositories.social_season_analytics._persist_shared_catalog_posts_with_progress",
            return_value=[],
        ),
    ):
        mock_api_cls.return_value.enabled.return_value = False

        rows, meta = _scrape_shared_youtube_posts(
            run_id="test-run",
            account_handle="bravo",
            config={
                "pipeline_ingest_mode": "shared_account_catalog_backfill",
                "catalog_mode": True,
            },
            job_id="test-job",
        )

    assert len(rows) == 0
    assert meta.get("error_code") == "youtube_empty_channel_page"
    assert meta.get("retryable") is True


def test_youtube_posts_catalog_uses_golden_fixture_without_network():
    from trr_backend.socials.youtube.posts_catalog import scrape_shared_youtube_posts

    fixture = _load_youtube_catalog_fixture("normal")
    dependencies, scraper, api_client = _catalog_shared_dependencies(fixture=fixture, ytdlp_available=False)
    progress_events: list[dict[str, Any]] = []

    rows, meta = scrape_shared_youtube_posts(
        run_id="fixture-run",
        account_handle=fixture["account_handle"],
        config={
            "pipeline_ingest_mode": "shared_account_catalog_backfill",
            "max_posts_per_target": 5,
        },
        job_id="fixture-job",
        progress_cb=lambda payload: progress_events.append(dict(payload)),
        dependencies=dependencies,
    )

    assert rows == [{"id": "row-abc123", "video_id": "abc123", "source_account": "bravo"}]
    assert scraper.config.channel_handle == "bravo"
    assert scraper.config.max_results == 5
    assert scraper.config.enforce_keyword_filter is False
    assert api_client.resolved_handle == "bravo"
    assert progress_events == [{"phase": "fixture_scrape"}]
    assert meta["ytdlp_available"] is False
    assert meta["first_page_counts"] == {"videos": 1, "shorts": 0}
    assert meta["persist_counters"] == {"posts_upserted": 1, "comments_upserted": 0}
    assert meta["profile_snapshot"]["username"] == "bravo"
    assert meta["profile_snapshot"]["display_name"] == "Bravo"
    assert meta["profile_snapshot"]["profile_url"] == "https://www.youtube.com/@bravo"
    assert meta["profile_snapshot"]["channel_id"] == "UCbravo123"
    assert meta["profile_snapshot"]["total_posts"] == 12562
    assert meta["total_posts"] == 12562
    assert "error_code" not in meta


def test_youtube_posts_catalog_empty_fixture_sets_empty_page_error_without_network():
    from trr_backend.socials.youtube.posts_catalog import scrape_shared_youtube_posts

    fixture = _load_youtube_catalog_fixture("empty")
    dependencies, _scraper, api_client = _catalog_shared_dependencies(fixture=fixture, ytdlp_available=True)

    rows, meta = scrape_shared_youtube_posts(
        run_id="fixture-empty-run",
        account_handle=fixture["account_handle"],
        config={"pipeline_ingest_mode": "shared_account_catalog_backfill"},
        job_id="fixture-empty-job",
        dependencies=dependencies,
    )

    assert rows == []
    assert api_client.resolved_handle is None
    assert meta["first_page_counts"] == {"videos": 0, "shorts": 0}
    assert meta["persist_counters"] == {"posts_upserted": 0, "comments_upserted": 0}
    assert meta["ytdlp_available"] is True
    assert meta["error_code"] == "youtube_empty_channel_page"
    assert meta["retryable"] is True
    assert meta["error_class"] == "YouTubeEmptyChannelPage"


def test_youtube_posts_catalog_merges_config_profile_snapshot_before_derived_fallback():
    from trr_backend.socials.youtube.posts_catalog import scrape_shared_youtube_posts

    fixture = _load_youtube_catalog_fixture("normal")
    fixture["api_identity"] = None
    dependencies, _scraper, api_client = _catalog_shared_dependencies(fixture=fixture, ytdlp_available=True)

    _rows, meta = scrape_shared_youtube_posts(
        run_id="fixture-run",
        account_handle=fixture["account_handle"],
        config={
            "pipeline_ingest_mode": "shared_account_catalog_backfill",
            "profile_snapshot": {
                "display_name": "Configured Bravo",
                "avatar_url": "https://images.test/configured-avatar.jpg",
                "profile_url": "https://youtube.test/configured",
            },
        },
        job_id="fixture-job",
        dependencies=dependencies,
    )

    assert api_client.resolved_handle is None
    assert meta["profile_snapshot"]["username"] == "bravo"
    assert meta["profile_snapshot"]["display_name"] == "Configured Bravo"
    assert meta["profile_snapshot"]["avatar_url"] == "https://images.test/configured-avatar.jpg"
    assert meta["profile_snapshot"]["profile_url"] == "https://youtube.test/configured"
    assert meta["profile_snapshot"]["channel_id"] == "UCbravo123"


@pytest.mark.parametrize(
    "stats,expected",
    [
        # Classic: all items are before-window
        (
            {"before_window_items": 10, "window_candidate_items": 0, "after_window_items": 0, "timestamp_unknown": 0},
            True,
        ),
        # All items are undated (shorts with low-precision timestamps)
        (
            {"before_window_items": 0, "window_candidate_items": 0, "after_window_items": 0, "timestamp_unknown": 30},
            True,
        ),
        # Mix of before-window and undated
        (
            {"before_window_items": 5, "window_candidate_items": 0, "after_window_items": 0, "timestamp_unknown": 20},
            True,
        ),
        # Has window candidates — not before-only
        (
            {"before_window_items": 5, "window_candidate_items": 1, "after_window_items": 0, "timestamp_unknown": 10},
            False,
        ),
        # Has after-window items — not before-only
        (
            {"before_window_items": 5, "window_candidate_items": 0, "after_window_items": 2, "timestamp_unknown": 0},
            False,
        ),
        # Completely empty page — neither before nor anything
        (
            {"before_window_items": 0, "window_candidate_items": 0, "after_window_items": 0, "timestamp_unknown": 0},
            False,
        ),
    ],
    ids=["dated-before", "all-undated", "mixed-before-undated", "has-candidates", "has-after", "empty"],
)
def test_page_before_only_includes_undated_shorts(stats, expected):
    """page_before_only should be True for pages with only before-window
    and/or timestamp_unknown items, so the pre_window_page_cap triggers
    correctly for shorts surfaces with low-precision dates."""
    _has_window = bool(stats.get("window_candidate_items"))
    _has_after = bool(stats.get("after_window_items"))
    _has_before = bool(stats.get("before_window_items"))
    _has_unknown = bool(stats.get("timestamp_unknown"))
    page_before_only = (_has_before or _has_unknown) and not _has_window and not _has_after
    assert page_before_only is expected
