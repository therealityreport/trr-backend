from __future__ import annotations

from trr_backend.utils.episode_appearances import aggregate_episode_appearances


def test_aggregate_episode_appearances_filters_and_orders() -> None:
    rows = [
        {
            "idx": 5,
            "imdb_show_id": "tt11363282",
            "person_id": "person-1",
            "cast_member_name": "Alpha",
            "seasons": ["1"],
            "total_episodes": 3,
            "imdb_episode_title_ids": ["tt010"],
        },
        {
            "idx": 2,
            "imdb_show_id": "tt00000000",
            "person_id": "person-2",
            "cast_member_name": "Beta",
            "seasons": ["1"],
        },
        {
            "idx": 1,
            "imdb_show_id": "tt11363282",
            "person_id": "person-1",
            "cast_member_name": "Alpha",
            "seasons": ["2"],
            "total_episodes": 5,
            "imdb_episode_title_ids": ["tt011"],
        },
        {
            "idx": 3,
            "imdb_show_id": "tt11363282",
            "cast_member_name": "Gamma",
            "seasons": ["1"],
        },
    ]

    aggregated = aggregate_episode_appearances(rows, imdb_show_id="tt11363282")

    assert [member.cast_member_name for member in aggregated] == ["Alpha", "Gamma"]

    alpha = aggregated[0]
    assert alpha.person_id == "person-1"
    assert alpha.total_episodes == 5
    assert alpha.seasons == ["1", "2"]
    assert alpha.imdb_episode_title_ids == ["tt010", "tt011"]
