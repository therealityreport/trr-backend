from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.instagram import catalog_ingest as catalog


def _patch_catalog_and_core(monkeypatch: pytest.MonkeyPatch, name: str, value: Any) -> None:
    monkeypatch.setattr(catalog, name, value)
    if hasattr(catalog._core, name):
        monkeypatch.setattr(catalog._core, name, value)


def _post(**overrides: Any) -> SimpleNamespace:
    raw_data = dict(overrides.pop("raw_data", {}))
    values = {
        "shortcode": "ABC123",
        "taken_at": 1_700_000_000,
        "post_url": None,
        "permalink_url": None,
        "caption": "caption",
        "post_type": "image",
        "media_urls": [],
        "thumbnail_url": None,
        "hashtags": [],
        "mentions": [],
        "collaborators": [],
        "profile_tags": [],
        "likes": 10,
        "comments": 4,
        "video_views_observed": 20,
        "music_info": None,
        "audio_url": None,
        "sponsored": False,
        "child_posts_data": None,
        "username": "bravotv",
        "video_play_count": None,
        "video_duration": None,
    }
    values.update(overrides)
    post = SimpleNamespace(**values)
    post.to_dict = lambda: dict(raw_data)
    return post


def test_shared_catalog_payload_omits_unknown_metrics_and_zero_timestamp() -> None:
    post = _post(
        taken_at=0,
        likes=0,
        comments=0,
        video_views_observed=None,
        raw_data={
            "like_and_view_counts_disabled": True,
            "comment_count_hidden": True,
        },
    )

    payload = catalog._shared_catalog_instagram_post_payload(
        run_id="run-1",
        account_handle="bravotv",
        post=post,
    )

    assert payload is not None
    assert payload["posted_at"] is None
    assert "likes" not in payload
    assert "comments_count" not in payload
    assert "views" not in payload


def test_single_shared_catalog_upsert_overwrites_known_lower_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_pg_upsert(table: str, payload: dict[str, Any], *, conflict_col: str, conn: Any = None):
        captured.update({"table": table, "payload": payload, "conflict_col": conflict_col, "conn": conn})
        return {"id": "row-1", "source_id": payload["source_id"], **payload}

    _patch_catalog_and_core(monkeypatch, "_pg_upsert", _fake_pg_upsert)
    _patch_catalog_and_core(
        monkeypatch,
        "_sync_instagram_catalog_post_collaborators",
        lambda *_args, **_kwargs: None,
    )

    row = catalog._upsert_shared_catalog_instagram_post(
        run_id="run-1",
        account_handle="bravotv",
        post=_post(
            likes=3,
            comments=1,
            video_views_observed=8,
            raw_data={"like_count": 3, "comment_count": 1, "video_view_count": 8},
        ),
    )

    assert row is not None
    assert captured["table"] == catalog.PLATFORM_CATALOG_POST_TABLES["instagram"]
    assert captured["conflict_col"] == "source_id"
    assert captured["payload"]["likes"] == 3
    assert captured["payload"]["comments_count"] == 1
    assert captured["payload"]["views"] == 8


def test_batch_shared_catalog_upsert_groups_unknown_metric_payloads(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def _fake_pg_upsert_many(table: str, payloads: list[dict[str, Any]], *, conflict_col: str, conn: Any = None):
        calls.append({"table": table, "payloads": payloads, "conflict_col": conflict_col, "conn": conn})
        return [
            {"id": f"row-{payload['source_id']}", "source_id": payload["source_id"], **payload} for payload in payloads
        ]

    _patch_catalog_and_core(monkeypatch, "_pg_upsert_many", _fake_pg_upsert_many)
    _patch_catalog_and_core(
        monkeypatch,
        "_sync_instagram_catalog_post_collaborators",
        lambda *_args, **_kwargs: None,
    )

    rows = catalog._batch_upsert_shared_catalog_instagram_posts(
        run_id="run-1",
        account_handle="bravotv",
        posts=[
            _post(shortcode="KNOWN", likes=2, comments=0, video_views_observed=5),
            _post(
                shortcode="UNKNOWN",
                likes=0,
                comments=0,
                video_views_observed=None,
                raw_data={"like_and_view_counts_disabled": True, "comment_count_hidden": True},
            ),
        ],
    )

    assert {row["source_id"] for row in rows} == {"KNOWN", "UNKNOWN"}
    known_payload = next(payload for call in calls for payload in call["payloads"] if payload["source_id"] == "KNOWN")
    unknown_payload = next(
        payload for call in calls for payload in call["payloads"] if payload["source_id"] == "UNKNOWN"
    )
    assert known_payload["likes"] == 2
    assert known_payload["comments_count"] == 0
    assert known_payload["views"] == 5
    assert "likes" not in unknown_payload
    assert "comments_count" not in unknown_payload
    assert "views" not in unknown_payload
    assert len(calls) == 2


class _FakeGraphQLScraper:
    def _iter_posts_from_graphql(self, data: dict[str, Any]):
        page_info = data["page_info"]
        for node in data["nodes"]:
            yield node, page_info

    def _parse_post_node(self, node: dict[str, Any], _config: Any) -> SimpleNamespace:
        return _post(
            shortcode=node["code"],
            taken_at=node.get("taken_at_timestamp"),
            likes=node.get("like_count", 0),
            comments=node.get("comment_count", 0),
            video_views_observed=node.get("video_view_count"),
        )

    def _extract_timestamp(self, node: dict[str, Any]) -> int:
        return int(node.get("taken_at_timestamp") or 0)

    def _extract_profile_total_posts(self, _data: dict[str, Any], *, source: str) -> int | None:
        return None


@contextmanager
def _fake_account_execution(*_args: Any, **_kwargs: Any):
    yield "bravotv"


def test_partitioned_scrape_accepts_opaque_cursor_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = _FakeGraphQLScraper()

    monkeypatch.setattr(catalog.time_module, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(catalog, "_shared_instagram_account_execution", _fake_account_execution)
    monkeypatch.setattr(catalog, "_build_shared_instagram_scraper", lambda **_kwargs: scraper)
    monkeypatch.setattr(catalog, "_shared_instagram_frontier_auth_validation", lambda _config: (False, None))
    monkeypatch.setattr(
        catalog,
        "_fetch_shared_instagram_graphql_page",
        lambda **_kwargs: (
            {
                "nodes": [{"code": "OPAQUE1", "taken_at_timestamp": 1_700_000_000}],
                "page_info": {"has_next_page": True, "end_cursor": "opaque-end"},
            },
            {},
            "public",
        ),
    )
    _patch_catalog_and_core(
        monkeypatch,
        "_persist_shared_catalog_posts_batch",
        lambda **kwargs: ([{"source_id": post.shortcode} for post in kwargs["posts"]], [], {}),
    )

    rows, meta = catalog._scrape_shared_instagram_posts_partitioned(
        run_id="run-1",
        account_handle="bravotv",
        config={"cursor_start": "opaque-start", "cursor_end": "opaque-end"},
        job_id="job-1",
    )

    assert rows == [{"source_id": "OPAQUE1"}]
    assert meta["reached_partition_boundary"] is True
    assert meta["partition_stop_reason"] == "cursor_boundary_reached"
    assert meta["partial_scrape"] is False


def test_partitioned_scrape_mid_cursor_empty_marks_retryable_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = _FakeGraphQLScraper()
    calls = 0

    def _fake_fetch(**kwargs: Any):
        nonlocal calls
        calls += 1
        if calls == 1:
            return (
                {
                    "nodes": [{"code": "PARTIAL1", "taken_at_timestamp": 1_700_000_000}],
                    "page_info": {"has_next_page": True, "end_cursor": "cursor-2"},
                },
                {},
                "public",
            )
        assert kwargs.get("cursor") == "cursor-2"
        return None, {}, "public"

    monkeypatch.setattr(catalog.time_module, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(catalog, "_shared_instagram_account_execution", _fake_account_execution)
    monkeypatch.setattr(catalog, "_build_shared_instagram_scraper", lambda **_kwargs: scraper)
    monkeypatch.setattr(catalog, "_shared_instagram_frontier_auth_validation", lambda _config: (False, None))
    monkeypatch.setattr(catalog, "_fetch_shared_instagram_graphql_page", _fake_fetch)
    _patch_catalog_and_core(
        monkeypatch,
        "_persist_shared_catalog_posts_batch",
        lambda **kwargs: ([{"source_id": post.shortcode} for post in kwargs["posts"]], [], {}),
    )

    rows, meta = catalog._scrape_shared_instagram_posts_partitioned(
        run_id="run-1",
        account_handle="bravotv",
        config={},
        job_id="job-1",
    )

    assert rows == [{"source_id": "PARTIAL1"}]
    assert meta["partial_scrape"] is True
    assert meta["reached_partition_boundary"] is False
    assert meta["partition_stop_reason"] == "instagram_graphql_cursor_empty_page"
    assert meta["error_code"] == "instagram_graphql_cursor_empty_page"
    assert meta["retryable"] is True
    assert meta["graphql_cursor"] == "cursor-2"


def test_discovery_mid_scan_error_marks_incomplete_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = _FakeGraphQLScraper()
    calls = 0

    def _fake_fetch(**kwargs: Any):
        nonlocal calls
        calls += 1
        if calls == 1:
            return (
                {
                    "nodes": [{"code": "DISC1", "taken_at_timestamp": 1_700_000_000}],
                    "page_info": {"has_next_page": True, "end_cursor": "opaque-next"},
                },
                {},
                "public",
            )
        return (
            None,
            {
                "error_code": "instagram_graphql_cursor_request_failed",
                "error_class": "HTTPError",
                "retryable": True,
                "graphql_cursor": kwargs.get("cursor"),
            },
            "public",
        )

    monkeypatch.setattr(catalog.time_module, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(catalog, "_shared_instagram_account_execution", _fake_account_execution)
    monkeypatch.setattr(catalog, "_build_shared_instagram_scraper", lambda **_kwargs: scraper)
    monkeypatch.setattr(catalog, "_fetch_shared_instagram_graphql_page", _fake_fetch)
    monkeypatch.setattr(catalog, "_catalog_full_history_posts_per_shard", lambda _platform: 10)

    partitions, meta = catalog._discover_instagram_cursor_partitions(
        account_handle="bravotv",
        runner_count=1,
        auth_allowed=False,
    )

    assert len(partitions) == 1
    assert partitions[0].cursor_start is None
    assert partitions[0].cursor_end is None
    assert partitions[0].metadata["incomplete_coverage"] is True
    assert meta["partial_discovery"] is True
    assert meta["incomplete_coverage"] is True
    assert meta["complete_coverage"] is False
    assert meta["error_code"] == "instagram_graphql_cursor_request_failed"


def test_discovery_mid_scan_empty_page_marks_incomplete_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = _FakeGraphQLScraper()
    calls = 0

    def _fake_fetch(**_kwargs: Any):
        nonlocal calls
        calls += 1
        if calls == 1:
            return (
                {
                    "nodes": [{"code": "DISC_EMPTY1", "taken_at_timestamp": 1_700_000_000}],
                    "page_info": {"has_next_page": True, "end_cursor": "opaque-next"},
                },
                {},
                "public",
            )
        return None, {}, "public"

    monkeypatch.setattr(catalog.time_module, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(catalog, "_shared_instagram_account_execution", _fake_account_execution)
    monkeypatch.setattr(catalog, "_build_shared_instagram_scraper", lambda **_kwargs: scraper)
    monkeypatch.setattr(catalog, "_fetch_shared_instagram_graphql_page", _fake_fetch)
    monkeypatch.setattr(catalog, "_catalog_full_history_posts_per_shard", lambda _platform: 10)

    partitions, meta = catalog._discover_instagram_cursor_partitions(
        account_handle="bravotv",
        runner_count=1,
        auth_allowed=False,
    )

    assert len(partitions) == 1
    assert partitions[0].metadata["incomplete_coverage"] is True
    assert meta["partial_discovery"] is True
    assert meta["complete_coverage"] is False
    assert meta["error_code"] == "instagram_graphql_discovery_cursor_empty_page"
    assert meta["retryable"] is True
