from __future__ import annotations

import pytest

from trr_backend.services import admin_media

SHOW_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
ASSET_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
ENTITY_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
LINK_ID = "dddddddd-dddd-dddd-dddd-dddddddddddd"


def test_season_assets_builds_the_existing_bounded_pagination_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_get(show_id: str, season_number: int, **kwargs):
        captured.update({"show_id": show_id, "season_number": season_number, **kwargs})
        return ([{"id": "1"}, {"id": "2"}, {"id": "3"}], 5)

    monkeypatch.setattr(admin_media.show_reads_repo, "get_show_season_assets", fake_get)

    payload, query_count = admin_media.get_show_season_assets(
        show_id=SHOW_ID,
        season_number=6,
        limit=2,
        offset=4,
        sources=["tmdb"],
        full=False,
    )

    assert query_count == 5
    assert captured == {
        "show_id": SHOW_ID,
        "season_number": 6,
        "limit": 3,
        "offset": 4,
        "sources": ["tmdb"],
        "full": False,
    }
    assert payload == {
        "assets": [{"id": "1"}, {"id": "2"}],
        "pagination": {
            "limit": 2,
            "offset": 4,
            "count": 2,
            "has_more": True,
            "next_cursor": "b2Zmc2V0OjY=",
            "cursor": "b2Zmc2V0OjQ=",
            "full": False,
            "truncated": False,
        },
    }


def test_create_media_link_requires_an_existing_asset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin_media.admin_media_repo, "media_asset_exists", lambda _asset_id: (False, 1))

    with pytest.raises(admin_media.MediaAssetNotFoundError):
        admin_media.create_media_link(
            media_asset_id=ASSET_ID,
            entity_type="season",
            entity_id=ENTITY_ID,
            kind="gallery",
            context={},
        )


def test_context_patch_response_preserves_zero_and_clamps_thumbnail_crop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        admin_media.admin_media_repo,
        "update_media_link_context",
        lambda _link_id, _patch: (
            {
                "id": LINK_ID,
                "context": {
                    "people_count": 0,
                    "people_count_source": "manual",
                    "thumbnail_crop": {"x": -10, "y": 120, "zoom": 9, "mode": "manual"},
                },
            },
            2,
        ),
    )

    result, query_count = admin_media.update_media_link_context(
        LINK_ID,
        {"people_count": 0},
    )

    assert query_count == 2
    assert result == {
        "link_id": LINK_ID,
        "people_count": 0,
        "people_count_source": "manual",
        "thumbnail_crop": {"x": 0.0, "y": 100.0, "zoom": 4.0, "mode": "manual"},
    }
