from __future__ import annotations

from types import SimpleNamespace

from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.socials.instagram import profile_stages


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
        source_snapshot_type="rendered_hidden_comments",
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
        "source_snapshot_type": "rendered_hidden_comments",
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


def test_fetch_instagram_following_rows_uses_profile_page_content_query(monkeypatch) -> None:
    calls: list[str] = []

    class _Client:
        def get_json(self, url: str, **kwargs):
            calls.append(url)
            assert url == "https://www.instagram.com/api/v1/friendships/123456/following/"
            assert kwargs["headers"]["x-asbd-id"] == "359341"
            assert kwargs["headers"]["x-instagram-ajax"] == "1039382002"
            assert kwargs["headers"]["x-web-session-id"] == "session:parts"
            assert kwargs["headers"]["x-fb-lsd"] == "lsd-token"
            assert kwargs["params"] == {"count": 2}
            return {
                "users": [
                    {
                        "pk": "789012",
                        "username": "followedaccount",
                        "full_name": "Followed Account",
                    }
                ],
                "has_more": False,
            }

    class _Scraper:
        _request_client = _Client()

        def fetch_profile_page_content_graphql(self, username: str, **_kwargs):
            assert username == "sourceaccount"
            return {"data": {"user": {"id": "123456", "username": "sourceaccount"}}}

        def fetch_profile_info(self, *_args, **_kwargs):  # pragma: no cover - should not be called
            raise AssertionError("web_profile_info fallback should not be used")

        def _get_headers(self, referer: str):
            assert referer == "https://www.instagram.com/sourceaccount/"
            return {"x-ig-app-id": "936619743392459"}

        def _get_profile_page_context_cache_entry(self, username: str):
            assert username == "sourceaccount"
            return {
                "spin_r": "1039382002",
                "web_session_id": "session:parts",
                "lsd": "lsd-token",
            }

        def _request_cookies(self):
            return {"sessionid": "session", "csrftoken": "csrf"}

        def _get(self, *_args, **_kwargs):  # pragma: no cover - not used by fake client
            raise AssertionError("unexpected direct get")

    monkeypatch.setattr(profile_stages, "_instagram_profile_scraper", lambda *_args, **_kwargs: _Scraper())

    rows, meta = profile_stages._fetch_instagram_following_rows(  # noqa: SLF001
        account_handle="sourceaccount",
        config={"page_size": 2, "max_pages": 1, "max_relationships": 2},
    )

    assert calls == ["https://www.instagram.com/api/v1/friendships/123456/following/"]
    assert meta["profile_id"] == "123456"
    assert meta["pages_fetched"] == 1
    assert rows[0]["username"] == "followedaccount"


def test_instagram_profile_stages_are_shared_job_config_stages() -> None:
    assert social_repo.INSTAGRAM_PROFILE_SNAPSHOT_STAGE == "instagram_profile_snapshot"
    assert social_repo.INSTAGRAM_PROFILE_FOLLOWING_STAGE == "instagram_profile_following"


def test_instagram_following_snapshot_completeness_requires_no_cursor_or_more_flag() -> None:
    assert (
        profile_stages._instagram_following_snapshot_is_complete(  # noqa: SLF001
            {
                "has_more": False,
                "next_cursor": None,
                "pages_fetched": 2,
                "max_pages": 3,
                "rows_fetched": 2,
                "max_relationships": 10,
                "profile_following_count": 2,
            }
        )
        is True
    )
    incomplete_cases = [
        {"has_more": True, "next_cursor": None},
        {"has_more": False, "next_cursor": "cursor-2"},
        {"has_more": False, "next_cursor": None, "pages_fetched": 3, "max_pages": 3},
        {"has_more": False, "next_cursor": None, "rows_fetched": 10, "max_relationships": 10},
        {
            "has_more": False,
            "next_cursor": None,
            "rows_fetched": 2,
            "profile_following_count": 3,
        },
    ]
    for metadata in incomplete_cases:
        assert profile_stages._instagram_following_snapshot_is_complete(metadata) is False  # noqa: SLF001
    assert profile_stages._instagram_following_snapshot_is_complete({}) is False  # noqa: SLF001


def _install_instagram_relationship_persistence_fakes(monkeypatch, *, active_rows):
    owner_row = {
        "id": "owner-row-id",
        "profile_id": "owner-instagram-id",
        "username": "sourceaccount",
        "normalized_username": "sourceaccount",
    }
    upserted: list[str] = []
    marked_missing: list[str] = []
    snapshots: list[dict[str, object]] = []
    snapshot_items: list[dict[str, object]] = []

    monkeypatch.setattr(profile_stages, "_instagram_profile_tables_ready", lambda **_kwargs: True)
    monkeypatch.setattr(profile_stages, "_instagram_profile_snapshot_tables_ready", lambda **_kwargs: True)
    monkeypatch.setattr(profile_stages, "_instagram_profile_row_for_username", lambda *_args, **_kwargs: owner_row)
    monkeypatch.setattr(profile_stages, "_instagram_profile_fetch_one", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        profile_stages,
        "_active_instagram_profile_relationship_rows",
        lambda *_args, **_kwargs: [dict(row) for row in active_rows],
    )

    def fake_execute_one(_sql, _params, *, label: str = "instagram_profile_execute_one", **_kwargs):
        if label == "instagram_profile_relationship_insert":
            row_id = f"inserted-row-{len(upserted) + 1}"
            upserted.append(row_id)
            return {"id": row_id}
        raise AssertionError(f"unexpected execute label: {label}")

    def fake_mark_missing(*, relationship_row_id: str, **_kwargs):
        marked_missing.append(relationship_row_id)
        return next(row for row in active_rows if row["id"] == relationship_row_id)

    def fake_create_snapshot(**kwargs):
        snapshots.append(dict(kwargs))
        return {"id": f"snapshot-{len(snapshots)}"}

    def fake_insert_snapshot_item(**kwargs):
        snapshot_items.append(dict(kwargs))

    monkeypatch.setattr(profile_stages, "_instagram_profile_execute_one", fake_execute_one)
    monkeypatch.setattr(profile_stages, "_mark_instagram_profile_relationship_missing", fake_mark_missing)
    monkeypatch.setattr(profile_stages, "_create_instagram_profile_following_snapshot", fake_create_snapshot)
    monkeypatch.setattr(
        profile_stages,
        "_insert_instagram_profile_relationship_snapshot_item",
        fake_insert_snapshot_item,
    )
    return {
        "upserted": upserted,
        "marked_missing": marked_missing,
        "snapshots": snapshots,
        "snapshot_items": snapshot_items,
    }


def test_complete_instagram_following_snapshot_marks_unobserved_rows_missing(monkeypatch) -> None:
    captures = _install_instagram_relationship_persistence_fakes(
        monkeypatch,
        active_rows=[
            {
                "id": "same-user-id",
                "owner_profile_id": "owner-row-id",
                "related_user_id": "user-1",
                "related_username": "oldhandle",
                "related_normalized_username": "oldhandle",
            },
            {
                "id": "same-username",
                "owner_profile_id": "owner-row-id",
                "related_user_id": None,
                "related_username": "usernameonly",
                "related_normalized_username": "usernameonly",
            },
            {
                "id": "missing-row",
                "owner_profile_id": "owner-row-id",
                "related_user_id": "user-3",
                "related_username": "unobserved",
                "related_normalized_username": "unobserved",
            },
        ],
    )

    result = profile_stages.persist_instagram_profile_relationships(
        [
            {"pk": "user-1", "username": "renamedhandle", "type": "Following"},
            {"username": "UsernameOnly", "type": "Following"},
        ],
        owner_username="sourceaccount",
        source_scope="network",
        snapshot_metadata={
            "has_more": False,
            "next_cursor": None,
            "pages_fetched": 2,
            "max_pages": 3,
            "rows_fetched": 2,
            "max_relationships": 10,
            "profile_following_count": 2,
        },
    )

    assert result["source_is_complete"] is True
    assert result["rows_upserted"] == 2
    assert result["rows_missing"] == 1
    assert captures["upserted"] == ["inserted-row-1", "inserted-row-2"]
    assert captures["marked_missing"] == ["missing-row"]
    assert captures["snapshots"][0]["source_is_complete"] is True
    assert [item["is_present"] for item in captures["snapshot_items"]] == [True, True, False]


def test_capped_instagram_following_snapshot_does_not_mark_missing(monkeypatch) -> None:
    captures = _install_instagram_relationship_persistence_fakes(
        monkeypatch,
        active_rows=[
            {
                "id": "same-user-id",
                "owner_profile_id": "owner-row-id",
                "related_user_id": "user-1",
                "related_username": "oldhandle",
                "related_normalized_username": "oldhandle",
            },
            {
                "id": "unobserved-active-row",
                "owner_profile_id": "owner-row-id",
                "related_user_id": "user-2",
                "related_username": "stillactive",
                "related_normalized_username": "stillactive",
            },
        ],
    )

    result = profile_stages.persist_instagram_profile_relationships(
        [{"pk": "user-1", "username": "renamedhandle", "type": "Following"}],
        owner_username="sourceaccount",
        source_scope="network",
        snapshot_metadata={
            "has_more": False,
            "next_cursor": None,
            "pages_fetched": 1,
            "max_pages": 1,
            "rows_fetched": 1,
            "max_relationships": 1,
            "profile_following_count": 2,
        },
    )

    assert result["source_is_complete"] is False
    assert result["rows_upserted"] == 1
    assert result["rows_missing"] == 0
    assert captures["upserted"] == ["inserted-row-1"]
    assert captures["marked_missing"] == []
    assert captures["snapshots"][0]["source_is_complete"] is False
    assert [item["is_present"] for item in captures["snapshot_items"]] == [True]
