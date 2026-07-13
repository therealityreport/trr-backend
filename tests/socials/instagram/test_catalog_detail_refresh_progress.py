from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.instagram import catalog_ingest as catalog
from trr_backend.socials.pipelines.account_catalog import progress


def _patch_catalog_and_core(monkeypatch: pytest.MonkeyPatch, name: str, value: Any) -> None:
    monkeypatch.setattr(catalog, name, value)
    if hasattr(catalog._core, name):
        monkeypatch.setattr(catalog._core, name, value)


@contextmanager
def _fake_account_execution(*_args: Any, **_kwargs: Any):
    yield "bravotv"


@contextmanager
def _fake_db_connection(**_kwargs: Any):
    yield object()


class _FakeDetailScraper:
    def fetch_post_info(self, shortcode: str, *, delay: float) -> dict[str, Any]:
        assert shortcode == "SHORT1"
        assert delay >= 0
        return {"items": [{"code": shortcode}]}

    def _parse_post_node(self, node: dict[str, Any], _config: Any) -> SimpleNamespace:
        post = SimpleNamespace(
            shortcode=node["code"],
            pk="media-1",
            username="bravotv",
            caption="detail caption",
            post_type="reel",
            media_urls=["https://example.test/detail.mp4"],
            thumbnail_url="https://example.test/detail.jpg",
            likes=12,
            comments=7,
            video_views_observed=44,
            taken_at=1_700_000_000,
            hashtags=[],
            mentions=[],
            collaborators=[],
            profile_tags=[],
        )
        post.to_dict = lambda: {"code": node["code"]}
        return post


def test_detail_refresh_stamps_metadata_on_successful_fetched_post(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_now = datetime(2026, 7, 8, 12, 0, tzinfo=UTC)
    captured_posts: list[Any] = []
    progress_updates: list[dict[str, Any]] = []

    def _fake_upsert_instagram_post(*_args: Any, post: Any, **_kwargs: Any) -> dict[str, Any]:
        captured_posts.append(post)
        return {
            "id": "post-1",
            "shortcode": post.shortcode,
            "metadata_source": getattr(post, "metadata_source", None),
            "metadata_scraped_at": getattr(post, "metadata_scraped_at", None),
        }

    _patch_catalog_and_core(monkeypatch, "_now_utc", lambda: fixed_now)
    _patch_catalog_and_core(monkeypatch, "_shared_instagram_account_execution", _fake_account_execution)
    _patch_catalog_and_core(monkeypatch, "_shared_instagram_frontier_auth_validation", lambda _config: (False, None))
    _patch_catalog_and_core(monkeypatch, "_build_shared_instagram_scraper", lambda **_kwargs: _FakeDetailScraper())
    _patch_catalog_and_core(
        monkeypatch,
        "_shared_account_expected_total_posts_from_config",
        lambda *_a, **_k: 17_518,
    )
    _patch_catalog_and_core(monkeypatch, "_refresh_instagram_post_metrics_only", lambda **_kwargs: None)
    _patch_catalog_and_core(monkeypatch, "_upsert_instagram_post", _fake_upsert_instagram_post)
    _patch_catalog_and_core(
        monkeypatch,
        "_load_existing_social_account_posts",
        lambda *_args, **_kwargs: [
            {
                "id": "post-1",
                "shortcode": "SHORT1",
                "likes": 1,
                "comments_count": 1,
                "views": 1,
                "metadata_scraped_at": None,
            }
        ],
    )
    monkeypatch.setattr(catalog.pg, "db_connection", _fake_db_connection)

    rows, meta = catalog._scrape_shared_instagram_post_details_refresh(
        run_id="run-1",
        account_handle="bravotv",
        config={
            "selected_tasks": ["post_details"],
            "date_start": "2026-06-01T00:00:00+00:00",
            "date_end": "2026-08-01T00:00:00+00:00",
            "details_refresh_force_detail_fetch": True,
            "details_refresh_skip_media_followups": True,
            "details_refresh_write_batch_size": 1,
        },
        job_id="job-1",
        progress_cb=progress_updates.append,
    )

    assert meta["details_refreshed_posts"] == 1
    assert rows[0]["metadata_scraped_at"] == fixed_now
    assert rows[0]["metadata_source"] == "api_permalink"
    assert captured_posts[0].metadata_scraped_at == fixed_now
    assert captured_posts[0].metadata_source == "api_permalink"
    assert captured_posts[0].metadata_error is None
    assert meta["expected_total_posts"] == 17_518
    assert meta["total_posts"] == 1
    assert meta["completion_target_posts"] == 1
    assert meta["completion_target_source"] == "bounded_catalog"
    assert {update["total_posts"] for update in progress_updates} == {1}
    assert [update["phase"] for update in progress_updates] == [
        "details_refresh_fetch",
        "details_refresh_update",
    ]


def test_detail_refresh_progress_uses_terminal_job_rows_over_stale_nonterminal_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(progress, "instagram_posts_acceleration_flags", lambda: {"flags": {}})
    run_config = {
        "selected_tasks": ["post_details"],
        "effective_selected_tasks": ["post_details"],
        "pagination_state": {},
        "stage_graph": {
            "detail_refresh": {
                "status": "queued",
                "selected": True,
                "blocker_reasons": [],
            },
        },
        "target_readiness": {"detail_gap_count": 687},
    }
    job_rows = [
        {
            "stage": "shared_account_posts",
            "status": "completed",
            "metadata": {"fetcher_runtime": {"auth_state": "anonymous"}},
        }
    ]

    stage_payload = progress._catalog_progress_stage_graph_payload(
        run_config=run_config,
        stages_payload={},
        job_rows=job_rows,
    )
    runtime_payload = progress._catalog_posts_runtime_additive_payload(
        platform="instagram",
        account_handle="bravotv",
        run_id="run-1",
        run_config=run_config,
        job_rows=job_rows,
    )

    assert stage_payload["stage_graph"]["detail_refresh"]["status"] == "completed"
    assert runtime_payload["details_progress"]["status"] == "completed"


@pytest.mark.parametrize("bounded_total", [360, 0])
def test_bounded_detail_refresh_shard_progress_and_completion_use_enumerated_rows(
    monkeypatch: pytest.MonkeyPatch,
    bounded_total: int,
) -> None:
    progress_updates: list[dict[str, Any]] = []
    rows = [
        {
            "id": f"post-{index}",
            "shortcode": f"SHORT{index}",
            "likes": 1,
            "comments_count": 1,
            "views": 1,
        }
        for index in range(bounded_total)
    ]

    _patch_catalog_and_core(monkeypatch, "_shared_instagram_account_execution", _fake_account_execution)
    _patch_catalog_and_core(monkeypatch, "_shared_instagram_frontier_auth_validation", lambda _config: (False, None))
    _patch_catalog_and_core(monkeypatch, "_build_shared_instagram_scraper", lambda **_kwargs: SimpleNamespace())
    _patch_catalog_and_core(monkeypatch, "_load_existing_social_account_posts", lambda *_args, **_kwargs: rows)
    _patch_catalog_and_core(
        monkeypatch,
        "_shared_account_expected_total_posts_from_config",
        lambda *_args, **_kwargs: 17_518,
    )

    refreshed_rows, meta = catalog._scrape_shared_instagram_post_details_refresh(
        run_id="run-bounded",
        account_handle="bravotv",
        config={
            "selected_tasks": ["post_details"],
            "date_start": "2026-06-01T00:00:00+00:00",
            "date_end": "2026-08-01T00:00:00+00:00",
            "details_refresh_dry_run": True,
            "details_refresh_skip_media_followups": True,
            "details_refresh_shard_index": 1,
            "details_refresh_shard_count": 6,
        },
        job_id="job-bounded",
        progress_cb=progress_updates.append,
    )

    enumerated_total = bounded_total // 6
    assert len(refreshed_rows) == enumerated_total
    assert meta["details_refresh_rows_seen"] == enumerated_total
    assert meta["total_posts"] == enumerated_total
    assert meta["completion_target_posts"] == enumerated_total
    assert meta["completion_target_source"] == "bounded_catalog"
    if progress_updates:
        assert progress_updates[-1]["posts_checked"] == enumerated_total
        assert progress_updates[-1]["total_posts"] == enumerated_total
    else:
        assert enumerated_total == 0

