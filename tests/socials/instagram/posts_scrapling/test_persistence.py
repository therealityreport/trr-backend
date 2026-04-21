from __future__ import annotations

import json
from contextlib import contextmanager

import pytest


def test_adapt_graph_node_to_post_dto():
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "shortcode": "CxTestShort",
        "taken_at_timestamp": 1700000000,
        "__typename": "GraphVideo",
        "display_url": "https://example.com/img.jpg",
        "video_view_count": 12345,
        "edge_media_preview_like": {"count": 100},
        "edge_media_to_comment": {"count": 50},
        "edge_media_to_caption": {"edges": [{"node": {"text": "Hello #world @friend"}}]},
        "owner": {"username": "testuser", "id": "999"},
        "id": "3200000000000",
    }
    dto = _graph_node_to_post_dto(node, account_handle="testuser")
    assert dto.shortcode == "CxTestShort"
    assert dto.likes == 100
    assert dto.comments == 50
    assert dto.video_views == 12345
    assert dto.caption == "Hello #world @friend"
    assert dto.post_type == "video"
    assert dto.username == "testuser"
    assert dto.pk == "3200000000000"
    assert dto.taken_at == 1700000000
    assert hasattr(dto, "to_dict")


def test_adapt_graph_node_carousel():
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "shortcode": "CxCarousel",
        "taken_at_timestamp": 1700000000,
        "__typename": "GraphSidecar",
        "display_url": "https://example.com/img.jpg",
        "edge_sidecar_to_children": {
            "edges": [
                {"node": {"display_url": "https://example.com/1.jpg"}},
                {"node": {"display_url": "https://example.com/2.jpg", "video_url": "https://example.com/2.mp4"}},
            ]
        },
        "edge_media_preview_like": {"count": 0},
        "edge_media_to_comment": {"count": 0},
        "edge_media_to_caption": {"edges": []},
        "owner": {"username": "testuser"},
        "id": "pk123",
    }
    dto = _graph_node_to_post_dto(node, account_handle="testuser")
    assert dto.post_type == "carousel"
    # display_url + child display_urls + child video_url (deduped)
    assert len(dto.media_urls) >= 3


def test_adapt_graph_node_image():
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "shortcode": "CxImage",
        "taken_at_timestamp": 1700000000,
        "__typename": "GraphImage",
        "display_url": "https://example.com/photo.jpg",
        "edge_media_preview_like": {"count": 42},
        "edge_media_to_comment": {"count": 5},
        "edge_media_to_caption": {"edges": [{"node": {"text": "Nice photo"}}]},
        "owner": {"username": "photog"},
        "id": "pk456",
    }
    dto = _graph_node_to_post_dto(node, account_handle="photog")
    assert dto.post_type == "image"
    assert dto.video_views == 0


def test_adapt_xdt_media_dict_video():
    """The profile timeline connection returns XDTMediaDict — the shape IG
    uses as of April 2026. Verify the adapter reads the new field names."""
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "__typename": "XDTMediaDict",
        "code": "DXKD0wtAHRz",
        "pk": "3875927249152668787",
        "id": "3875927249152668787_2554414",
        "media_type": 2,  # video
        "like_count": 36530,
        "comment_count": 2295,
        "taken_at": 1776272482,
        "caption": {"text": "Hello world #fyp"},
        "user": {"pk": "2554414", "username": "bravotv"},
        "image_versions2": {
            "candidates": [
                {"url": "https://cdn.example.com/thumb1.jpg", "width": 1080, "height": 1920},
                {"url": "https://cdn.example.com/thumb2.jpg", "width": 640, "height": 1138},
            ]
        },
        "video_versions": [
            {"url": "https://cdn.example.com/video1.mp4", "width": 720, "height": 1280},
        ],
    }
    dto = _graph_node_to_post_dto(node, account_handle="bravotv")
    assert dto.shortcode == "DXKD0wtAHRz"  # from `code`, not `shortcode`
    assert dto.post_type == "video"  # media_type=2 → video
    assert dto.likes == 36530  # from `like_count`
    assert dto.comments == 2295  # from `comment_count`
    assert dto.taken_at == 1776272482  # from `taken_at` (no _timestamp suffix)
    assert dto.caption == "Hello world #fyp"  # from caption.text dict
    assert dto.username == "bravotv"  # from user.username
    assert dto.pk == "3875927249152668787"  # prefers pk over composite id
    assert "https://cdn.example.com/thumb1.jpg" in dto.media_urls
    assert "https://cdn.example.com/video1.mp4" in dto.media_urls
    assert dto.thumbnail_url == "https://cdn.example.com/thumb1.jpg"


def test_adapt_xdt_media_dict_carousel():
    """XDTMediaDict carousel shape uses carousel_media (list of XDTMediaDict children)."""
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "__typename": "XDTMediaDict",
        "code": "XCAROU1",
        "pk": "1111",
        "media_type": 8,  # carousel
        "like_count": 100,
        "comment_count": 10,
        "taken_at": 1700000000,
        "caption": {"text": "Album"},
        "user": {"username": "u"},
        "carousel_media": [
            {"image_versions2": {"candidates": [{"url": "https://cdn.example.com/c1.jpg"}]}},
            {
                "image_versions2": {"candidates": [{"url": "https://cdn.example.com/c2.jpg"}]},
                "video_versions": [{"url": "https://cdn.example.com/c2.mp4"}],
            },
        ],
    }
    dto = _graph_node_to_post_dto(node, account_handle="u")
    assert dto.post_type == "carousel"  # media_type=8 → carousel
    assert len(dto.media_urls) >= 3  # 2 image urls + 1 video url


def test_persist_instagram_posts_tracks_skip_reasons_and_accumulates_job_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling.persistence import persist_instagram_posts

    @contextmanager
    def _fake_conn(*, label: str | None = None):
        del label
        yield object()

    captured_updates: list[dict[str, object]] = []

    monkeypatch.setattr(pg, "db_connection", _fake_conn)
    monkeypatch.setattr(repo, "get_season_context", lambda _season_id: None)

    def _fake_upsert(_context, *, job_id, account, post, conn):  # noqa: ANN001
        del job_id, account, conn
        shortcode = str(getattr(post, "shortcode", "") or "")
        if shortcode == "keep-me":
            return {"id": "post-1"}
        if shortcode == "drop-me":
            return None
        raise RuntimeError("db write failed")

    def _fake_fetch_one(sql, params):  # noqa: ANN001
        normalized = " ".join(str(sql).split()).lower()
        if normalized.startswith("select metadata from social.scrape_jobs"):
            return {
                "metadata": {
                    "posts_scrapling_persist_diagnostics": {
                        "posts_upserted": 2,
                        "posts_skipped": 1,
                        "posts_skipped_by_reason": {"missing_shortcode": 1},
                    }
                }
            }
        if normalized.startswith("update social.scrape_jobs"):
            captured_updates.append(json.loads(str(params[0])))
            return {"id": "job-1"}
        raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(repo, "_upsert_instagram_post", _fake_upsert)
    monkeypatch.setattr(pg, "fetch_one", _fake_fetch_one)

    result = persist_instagram_posts(
        account_handle="traitors",
        post_nodes=[
            "not-a-dict",
            {"__typename": "GraphImage"},
            {
                "shortcode": "keep-me",
                "taken_at_timestamp": 1700000000,
                "__typename": "GraphImage",
                "display_url": "https://example.com/keep.jpg",
                "owner": {"username": "traitors"},
                "id": "keep-1",
            },
            {
                "shortcode": "drop-me",
                "taken_at_timestamp": 1700000000,
                "__typename": "GraphImage",
                "display_url": "https://example.com/drop.jpg",
                "owner": {"username": "traitors"},
                "id": "drop-1",
            },
            {
                "shortcode": "explode-me",
                "taken_at_timestamp": 1700000000,
                "__typename": "GraphImage",
                "display_url": "https://example.com/explode.jpg",
                "owner": {"username": "traitors"},
                "id": "explode-1",
            },
        ],
        run_id="run-1",
        job_id="job-1",
        season_id=None,
    )

    assert result.posts_upserted == 1
    assert result.posts_skipped == 4
    assert result.posts_skipped_by_reason == {
        "invalid_node_type": 1,
        "missing_shortcode": 1,
        "canonical_upsert_returned_none": 1,
        "canonical_upsert_exception": 1,
    }
    assert captured_updates[-1]["posts_scrapling_persist_diagnostics"] == {
        "posts_upserted": 3,
        "posts_skipped": 5,
        "posts_skipped_by_reason": {
            "canonical_upsert_exception": 1,
            "canonical_upsert_returned_none": 1,
            "invalid_node_type": 1,
            "missing_shortcode": 2,
        },
    }
