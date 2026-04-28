from __future__ import annotations

from types import SimpleNamespace

from trr_backend.repositories import social_season_analytics as social_repo


def test_apply_instagram_comment_queryable_columns_uses_full_comment_payload(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        social_repo,
        "_column_exists",
        lambda schema, table, column, **_kwargs: (
            schema == "social"
            and table == "instagram_comments"
            and column
            in {
                "author_full_name",
                "author_profile_pic_url_hd",
                "parent_comment_external_id",
                "reply_depth",
                "source_snapshot_type",
            }
        ),
    )
    payload: dict[str, object] = {}
    comment = SimpleNamespace(
        owner_full_name="Comment Author",
        owner_profile_pic_url_hd="https://cdn.example.com/author-hd.jpg",
        to_dict=lambda: {
            "owner": {
                "full_name": "Raw Name",
                "profile_pic_url_hd": "https://cdn.example.com/raw-hd.jpg",
            }
        },
    )

    social_repo._apply_instagram_comment_queryable_columns(
        payload,
        comment,
        parent_external_id="parent-1",
        reply_depth=2,
    )

    assert payload == {
        "author_full_name": "Comment Author",
        "author_profile_pic_url_hd": "https://cdn.example.com/author-hd.jpg",
        "parent_comment_external_id": "parent-1",
        "reply_depth": 2,
        "source_snapshot_type": "full_comments_scrape",
    }


def test_serialize_comment_tree_includes_queryable_instagram_author_fields() -> None:
    payload = social_repo._serialize_comment_tree(
        {
            "id": "row-1",
            "comment_id": "comment-1",
            "author": "viewer",
            "user_id": "user-1",
            "author_full_name": "Viewer Name",
            "author_profile_pic_url": "https://cdn.example.com/viewer.jpg",
            "hosted_author_profile_pic_url": "https://cdn.trr.example/viewer.jpg",
            "author_profile_pic_url_hd": "https://cdn.example.com/viewer-hd.jpg",
            "author_is_verified": True,
            "text": "Saved full comment",
            "likes": 12,
            "is_reply": False,
            "reply_count": 1,
            "parent_comment_external_id": "parent-source-1",
            "reply_depth": 0,
            "source_snapshot_type": "full_comments_scrape",
            "replies": [],
        }
    )

    assert payload["author_full_name"] == "Viewer Name"
    assert payload["hosted_author_profile_pic_url"] == "https://cdn.trr.example/viewer.jpg"
    assert payload["author_is_verified"] is True
    assert payload["parent_comment_external_id"] == "parent-source-1"
    assert payload["source_snapshot_type"] == "full_comments_scrape"
    assert payload["user"] == {
        "id": "user-1",
        "username": "viewer",
        "display_name": "Viewer Name",
        "url": None,
        "bio": None,
        "avatar_url": "https://cdn.trr.example/viewer.jpg",
        "source_avatar_url": "https://cdn.example.com/viewer.jpg",
        "profile_pic_url_hd": "https://cdn.example.com/viewer-hd.jpg",
        "is_verified": True,
        "region": None,
        "language": None,
    }


def test_instagram_profile_response_excludes_raw_payloads() -> None:
    payload = social_repo._instagram_profile_response(
        {
            "id": "profile-row-1",
            "profile_id": "528817151",
            "username": "nasa",
            "normalized_username": "nasa",
            "url": "https://www.instagram.com/nasa",
            "full_name": "NASA",
            "biography": "Exploring the universe.",
            "followers_count": 96_000_000,
            "follows_count": 80,
            "posts_count": 4519,
            "highlight_reel_count": 7,
            "igtv_video_count": 171,
            "is_business_account": True,
            "is_private": False,
            "is_verified": True,
            "raw_data": {"secret": "raw"},
            "about_raw": {"accounts_with_shared_followers": []},
        },
        [
            {
                "title": "NASA",
                "url": "https://www.nasa.gov/",
                "shim_url": "https://l.instagram.com/?u=https%3A%2F%2Fwww.nasa.gov%2F",
                "normalized_domain": "nasa.gov",
                "link_type": "external",
            }
        ],
    )

    assert payload["id"] == "528817151"
    assert payload["counts"]["followers"] == 96_000_000
    assert payload["external_links"][0]["normalized_domain"] == "nasa.gov"
    assert "raw_data" not in payload
    assert "about_raw" not in payload


def test_instagram_following_rows_from_payload_marks_rows_as_following() -> None:
    rows, next_cursor, has_more = social_repo._instagram_following_rows_from_payload(
        {
            "users": [
                {
                    "pk": "4014759590",
                    "username": "realcolinfurze",
                    "full_name": "colin furze",
                    "is_private": False,
                    "is_verified": True,
                }
            ],
            "next_max_id": "cursor-2",
        },
        owner_username="mrbeast",
        source_cursor="cursor-1",
        source_page_ordinal=3,
        starting_rank=25,
    )

    assert next_cursor == "cursor-2"
    assert has_more is True
    assert rows == [
        {
            "pk": "4014759590",
            "username": "realcolinfurze",
            "full_name": "colin furze",
            "is_private": False,
            "is_verified": True,
            "username_scrape": "mrbeast",
            "type": "Following",
            "source_rank": 25,
            "source_cursor": "cursor-1",
            "source_page_ordinal": 3,
        }
    ]


def test_instagram_profile_stages_are_shared_job_config_stages() -> None:
    assert social_repo.INSTAGRAM_PROFILE_SNAPSHOT_STAGE == "instagram_profile_snapshot"
    assert social_repo.INSTAGRAM_PROFILE_FOLLOWING_STAGE == "instagram_profile_following"
