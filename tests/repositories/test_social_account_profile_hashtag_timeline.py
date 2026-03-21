from __future__ import annotations

from typing import Any

import pytest

import trr_backend.repositories.social_season_analytics as social_repo


def test_build_social_account_profile_hashtag_timeline_payload_ranks_years_and_segments() -> None:
    payload = social_repo._build_social_account_profile_hashtag_timeline_payload(
        platform="instagram",
        account_handle="bravotv",
        year_rows=[
            {"year": 2022, "hashtag": "beta", "usage_count": 10},
            {"year": 2022, "hashtag": "alpha", "usage_count": 10},
            {"year": 2022, "hashtag": "gamma", "usage_count": 7},
            {"year": 2023, "hashtag": "gamma", "usage_count": 11},
            {"year": 2023, "hashtag": "delta", "usage_count": 9},
            {"year": 2024, "hashtag": "beta", "usage_count": 12},
            {"year": 2024, "hashtag": "gamma", "usage_count": 8},
        ],
    )

    assert [item["year"] for item in payload["years"]] == [2022, 2023, 2024]

    series_by_hashtag = {item["hashtag"]: item for item in payload["series"]}
    alpha_points = series_by_hashtag["alpha"]["points"]
    beta_points = series_by_hashtag["beta"]["points"]

    assert alpha_points == [
        {
            "year": 2022,
            "label": "2022",
            "order": 1,
            "rank": 1,
            "usage_count": 10,
            "in_top_ten": True,
            "segment_id": 1,
        }
    ]
    assert beta_points == [
        {
            "year": 2022,
            "label": "2022",
            "order": 1,
            "rank": 2,
            "usage_count": 10,
            "in_top_ten": True,
            "segment_id": 1,
        },
        {
            "year": 2023,
            "label": "2023",
            "order": 2,
            "rank": 11,
            "usage_count": 0,
            "in_top_ten": False,
            "segment_id": None,
        },
        {
            "year": 2024,
            "label": "2024",
            "order": 3,
            "rank": 1,
            "usage_count": 12,
            "in_top_ten": True,
            "segment_id": 2,
        },
    ]


def test_get_social_account_profile_hashtag_timeline_prefers_catalog_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fallback_calls: list[tuple[Any, ...]] = []

    monkeypatch.setattr(social_repo, "_assert_social_account_profile_exists", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        social_repo,
        "_shared_catalog_hashtag_timeline_year_rows",
        lambda *_args, **_kwargs: [
            {"year": 2022, "hashtag": "bravo", "usage_count": 8},
            {"year": 2023, "hashtag": "bravo", "usage_count": 9},
        ],
    )
    monkeypatch.setattr(
        social_repo,
        "_social_account_profile_hashtag_timeline_year_rows",
        lambda *args, **kwargs: fallback_calls.append((args, kwargs)) or [],
    )

    payload = social_repo.get_social_account_profile_hashtag_timeline("instagram", "bravotv")

    assert payload["series"][0]["hashtag"] == "bravo"
    assert fallback_calls == []
