from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any

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
    assert dto.thumbnail_url == "https://example.com/img.jpg"
    assert dto.media_urls == ["https://example.com/1.jpg", "https://example.com/2.mp4"]


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
    assert dto.media_urls == ["https://cdn.example.com/video1.mp4"]
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
    assert dto.media_urls == ["https://cdn.example.com/c1.jpg", "https://cdn.example.com/c2.mp4"]


def test_adapt_xdt_media_dict_preserves_rich_listing_fields():
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "__typename": "XDTMediaDict",
        "code": "RICH123",
        "pk": "rich-pk",
        "id": "rich-pk_owner",
        "inputUrl": "https://www.instagram.com/p/RICH123/",
        "media_type": 8,
        "product_type": "clips",
        "like_count": 12,
        "comment_count": 3,
        "taken_at": 1776272482,
        "caption": {"id": "caption-rich", "text": "Rich listing", "is_edited": True, "has_translation": False},
        "user": {
            "pk": "owner-1",
            "username": "bravotv",
            "full_name": "Bravo",
            "profile_pic_url": "https://cdn.example.com/bravo-small.jpg",
            "hd_profile_pic_url_info": {"url": "https://cdn.example.com/bravo-hd.jpg"},
            "is_verified": True,
        },
        "image_versions2": {
            "candidates": [
                {
                    "url": "https://cdn.example.com/cover.jpg",
                    "width": 1080,
                    "height": 1350,
                }
            ]
        },
        "accessibility_caption": "Cast portrait alt text",
        "comments_disabled": True,
        "like_and_view_counts_disabled": True,
        "commenting_disabled_for_viewer": True,
        "is_paid_partnership": True,
        "isAdvertisement": True,
        "can_viewer_reshare": False,
        "has_audio": True,
        "media_repost_count": 5,
        "music_info": {"artist_name": "Composer", "song_name": "Theme"},
        "audio_url": "https://cdn.example.com/audio.m4a",
        "video_duration": 9.5,
        "play_count": 77,
        "usertags": {
            "in": [
                {
                    "position": [0.5, 0.25],
                    "user": {
                        "pk": "tag-1",
                        "username": "host",
                        "full_name": "Host Person",
                        "profile_pic_url": "https://cdn.example.com/host.jpg",
                    },
                }
            ]
        },
        "coauthor_producers": [{"pk": "co-1", "username": "peacock", "full_name": "Peacock"}],
        "carousel_media": [
            {
                "pk": "child-1",
                "media_type": 1,
                "original_width": 1080,
                "original_height": 1350,
                "accessibility_caption": "Child image alt",
                "image_versions2": {"candidates": [{"url": "https://cdn.example.com/child-1.jpg"}]},
            }
        ],
    }

    dto = _graph_node_to_post_dto(node, account_handle="bravotv")

    assert dto.input_url == "https://www.instagram.com/p/RICH123/"
    assert dto.source_post_id == "rich-pk"
    assert dto.alt_text == "Cast portrait alt text"
    assert dto.width == 1080
    assert dto.height == 1350
    assert dto.product_type == "clips"
    assert dto.sponsored is True
    assert dto.is_paid_partnership is True
    assert dto.is_advertisement is True
    assert dto.comments_disabled is True
    assert dto.is_comments_disabled is True
    assert dto.like_and_view_counts_disabled is True
    assert dto.commenting_disabled_for_viewer is True
    assert dto.can_viewer_reshare is False
    assert dto.has_audio is True
    assert dto.media_repost_count == 5
    assert dto.owner_detail.username == "bravotv"
    assert dto.owner_detail.user_id == "owner-1"
    assert dto.owner_detail.full_name == "Bravo"
    assert dto.owner_detail.profile_pic_url_hd == "https://cdn.example.com/bravo-hd.jpg"
    assert dto.owner_detail.is_verified is True
    assert dto.owner_user_id == "owner-1"
    assert dto.owner_profile_pic_url_hd == "https://cdn.example.com/bravo-hd.jpg"
    assert dto.tagged_users_detail[0].username == "host"
    assert dto.collaborators_detail[0].username == "peacock"
    assert dto.music_info == {"artist_name": "Composer", "song_name": "Theme"}
    assert dto.audio_url == "https://cdn.example.com/audio.m4a"
    assert dto.video_duration == 9.5
    assert dto.video_play_count == 77
    assert dto.child_posts_data[0]["alt_text"] == "Child image alt"


def test_adapt_xdt_media_dict_media_repost_count_alternate_alias_persists():
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "__typename": "XDTMediaDict",
        "code": "ALTREPOST",
        "pk": "alt-repost-pk",
        "media_type": 1,
        "like_count": 10,
        "comment_count": 2,
        "taken_at": 1776272482,
        "caption": {"text": "Alternate alias"},
        "user": {"username": "traitors"},
        "image_versions2": {"candidates": [{"url": "https://cdn.example.com/alt-repost.jpg"}]},
        "repostCount": "42",
    }

    dto = _graph_node_to_post_dto(node, account_handle="traitors")

    assert dto.media_repost_count == 42


def test_adapt_xdt_media_dict_media_repost_count_snake_case_persists():
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "__typename": "XDTMediaDict",
        "code": "SNAKEREPOST",
        "pk": "snake-repost-pk",
        "media_type": 1,
        "like_count": 10,
        "comment_count": 2,
        "taken_at": 1776272482,
        "caption": {"text": "Snake alias"},
        "user": {"username": "traitors"},
        "image_versions2": {"candidates": [{"url": "https://cdn.example.com/snake-repost.jpg"}]},
        "media_repost_count": 15,
    }

    dto = _graph_node_to_post_dto(node, account_handle="traitors")

    assert dto.media_repost_count == 15


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
        "inline_comments_upserted": 0,
        "inline_comments_skipped": 0,
        "posts_skipped_by_reason": {
            "canonical_upsert_exception": 1,
            "canonical_upsert_returned_none": 1,
            "invalid_node_type": 1,
            "missing_shortcode": 2,
        },
    }


def test_persist_instagram_posts_persists_inline_comment_samples(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling.persistence import persist_instagram_posts

    @contextmanager
    def _fake_conn(*, label: str | None = None):
        del label
        yield object()

    captured_comments: list[Any] = []
    captured_updates: list[dict[str, object]] = []
    fake_context = object()

    monkeypatch.setattr(pg, "db_connection", _fake_conn)
    monkeypatch.setattr(repo, "get_season_context", lambda _season_id: fake_context)
    monkeypatch.setattr(repo, "_upsert_instagram_post", lambda *_args, **_kwargs: {"id": "post-1"})

    def _fake_batch_comments(_context, **kwargs):  # noqa: ANN001
        assert _context is fake_context
        assert kwargs["post_id"] == "post-1"
        assert kwargs["run_id"] == "run-1"
        assert kwargs["job_id"] == "job-1"
        assert kwargs["enable_media_followups"] is False
        captured_comments.extend(kwargs["comments"])
        return len(kwargs["comments"])

    def _fake_fetch_one(sql, params):  # noqa: ANN001
        normalized = " ".join(str(sql).split()).lower()
        if normalized.startswith("select metadata from social.scrape_jobs"):
            return {"metadata": {}}
        if normalized.startswith("update social.scrape_jobs"):
            captured_updates.append(json.loads(str(params[0])))
            return {"id": "job-1"}
        raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(repo, "_batch_upsert_instagram_comments", _fake_batch_comments)
    monkeypatch.setattr(pg, "fetch_one", _fake_fetch_one)

    result = persist_instagram_posts(
        account_handle="traitors",
        post_nodes=[
            {
                "__typename": "XDTMediaDict",
                "code": "INLINE123",
                "pk": "inline-pk",
                "media_type": 1,
                "taken_at": 1776272482,
                "image_versions2": {"candidates": [{"url": "https://cdn.example.com/inline.jpg"}]},
                "user": {"username": "traitors"},
                "latestComments": [
                    {
                        "id": "latest-1",
                        "text": "sample only",
                        "ownerUsername": "viewer",
                        "ownerFullName": "Viewer Name",
                    }
                ],
                "firstComment": {"id": "first-1", "text": "first sample", "ownerUsername": "firstviewer"},
            }
        ],
        run_id="run-1",
        job_id="job-1",
        season_id="season-1",
    )

    assert result.posts_upserted == 1
    assert [comment.comment_id for comment in captured_comments] == ["latest-1", "first-1"]
    assert [comment.source_snapshot_type for comment in captured_comments] == [
        "listing_inline_sample",
        "listing_inline_sample",
    ]
    assert captured_comments[0].owner_full_name == "Viewer Name"
    assert captured_updates[-1]["posts_scrapling_persist_diagnostics"]["inline_comments_upserted"] == 2
    assert captured_updates[-1]["posts_scrapling_persist_diagnostics"]["inline_comments_skipped"] == 0
