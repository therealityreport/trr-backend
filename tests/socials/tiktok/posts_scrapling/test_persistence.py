from __future__ import annotations


def test_adapt_tiktok_item_to_post_dto():
    from trr_backend.socials.tiktok.posts_scrapling.persistence import _tiktok_item_to_post_dto

    item = {
        "id": "7300000000000000000",
        "desc": "Test post #fyp",
        "createTime": 1700000000,
        "author": {"uniqueId": "testuser", "nickname": "Test User", "avatarThumb": "https://p.tiktok.com/av.jpg"},
        "stats": {"diggCount": 100, "commentCount": 50, "shareCount": 25, "playCount": 10000, "collectCount": 5},
        "music": {"title": "Original Sound", "authorName": "testuser"},
        "video": {"duration": 30, "cover": "https://p.tiktok.com/cover.jpg"},
    }
    dto = _tiktok_item_to_post_dto(item, account_handle="testuser")
    assert dto.video_id == "7300000000000000000"
    assert dto.likes == 100
    assert dto.comments == 50
    assert dto.views == 10000
    assert dto.saves == 5
    assert dto.username == "testuser"
    assert dto.duration == 30
    assert dto.music_title == "Original Sound"
    assert dto.create_time == 1700000000
    assert hasattr(dto, "to_dict")


def test_adapt_tiktok_item_missing_fields_graceful():
    from trr_backend.socials.tiktok.posts_scrapling.persistence import _tiktok_item_to_post_dto

    item = {"id": "7400000000000000000", "createTime": 1700000000}
    dto = _tiktok_item_to_post_dto(item, account_handle="fallback_user")
    assert dto.video_id == "7400000000000000000"
    assert dto.username == "fallback_user"  # fell back to account_handle
    assert dto.likes == 0
    assert dto.description == ""


def test_adapt_tiktok_item_extracts_hashtags_from_challenges():
    from trr_backend.socials.tiktok.posts_scrapling.persistence import _tiktok_item_to_post_dto

    item = {
        "id": "7500000000000000000",
        "createTime": 1700000000,
        "desc": "Hello world",
        "challenges": [
            {"title": "fyp"},
            {"title": "foryou"},
            {"title": ""},  # should be filtered
        ],
    }
    dto = _tiktok_item_to_post_dto(item, account_handle="u")
    assert "fyp" in dto.hashtags
    assert "foryou" in dto.hashtags
    assert "" not in dto.hashtags
