from __future__ import annotations

from trr_backend.integrations.imdb.episodic_client import (
    _parse_available_seasons_from_payload,
    _parse_episode_credits_page_from_payload,
)


SAMPLE_PAYLOAD: dict = {
    "data": {
        "title": {
            "creditsV2": {
                "edges": [
                    {
                        "node": {
                            "nav": {
                                "displayableSeasons": {
                                    "edges": [
                                        {"node": {"season": "5"}},
                                        {"node": {"season": "6"}},
                                        {"node": {"season": "specials"}},  # ignored (non-numeric)
                                        {"node": {"season": ""}},  # ignored (empty)
                                        {"node": {}},  # ignored (missing)
                                    ]
                                }
                            },
                            "episodeCredits": {
                                "pageInfo": {"endCursor": "cursor1", "hasNextPage": False},
                                "edges": [
                                    {
                                        "node": {
                                            "title": {
                                                "id": "tt31031349",
                                                "releaseYear": {"year": 2024},
                                                "titleText": {"text": "Costume or Couture?"},
                                                "series": {
                                                    "displayableEpisodeNumber": {
                                                        "displayableSeason": {"text": "5"},
                                                        "episodeNumber": {"text": "1"},
                                                    }
                                                },
                                            },
                                            "creditedRoles": {
                                                "edges": [
                                                    {
                                                        "node": {
                                                            "text": "Self",
                                                            "category": {"text": "Self"},
                                                            "attributes": [{"text": "as Britani"}],
                                                            "characters": {
                                                                "edges": [{"node": {"name": "Self"}}]
                                                            },
                                                        }
                                                    }
                                                ]
                                            },
                                        }
                                    },
                                    {
                                        "node": {
                                            "title": {
                                                "id": "tt99999999",
                                                "releaseYear": {"year": 2023},
                                                "titleText": {"text": "Flashback Special"},
                                                "series": {
                                                    "displayableEpisodeNumber": {
                                                        "displayableSeason": {"text": "5"},
                                                        "episodeNumber": {"text": "2"},
                                                    }
                                                },
                                            },
                                            "creditedRoles": {
                                                "edges": [
                                                    {
                                                        "node": {
                                                            "text": "Self",
                                                            "category": {"text": "Self"},
                                                            "attributes": [
                                                                {"text": "archive footage"},
                                                                {"text": "as Britani"},
                                                                {"text": "archive footage"},
                                                            ],
                                                            "characters": {
                                                                "edges": [
                                                                    {"node": {"name": "Self"}},
                                                                    {"node": {"name": "Self"}},
                                                                ]
                                                            },
                                                        }
                                                    }
                                                ]
                                            },
                                        }
                                    },
                                    {
                                        "node": {
                                            "title": {
                                                "id": "tt00000000",
                                                "titleText": {"text": "Untitled"},
                                                "series": {
                                                    "displayableEpisodeNumber": {
                                                        "displayableSeason": {"text": "X"},
                                                        "episodeNumber": {"text": "abc"},
                                                    }
                                                },
                                            },
                                            "creditedRoles": {"edges": []},
                                        }
                                    },
                                ],
                            },
                        }
                    }
                ]
            }
        }
    }
}


def test_parse_available_seasons_from_payload() -> None:
    seasons = _parse_available_seasons_from_payload(SAMPLE_PAYLOAD)
    assert seasons == [5, 6]


def test_parse_episode_credits_page_from_payload_normalizes_credits() -> None:
    page = _parse_episode_credits_page_from_payload(SAMPLE_PAYLOAD)
    assert page.end_cursor == "cursor1"
    assert page.has_next_page is False

    credits_by_title_id = {c.episode.title_id: c for c in page.credits}
    assert set(credits_by_title_id) == {"tt31031349", "tt99999999", "tt00000000"}

    c1 = credits_by_title_id["tt31031349"]
    assert c1.episode.season_number == 5
    assert c1.episode.episode_number == 1
    assert c1.episode.episode_code == "S5.E1"
    assert c1.episode.title == "Costume or Couture?"
    assert c1.episode.year == 2024
    assert c1.job == "Self"
    assert c1.credit_category == "Self"
    assert c1.attributes == ["as Britani"]
    assert c1.characters == ["Self"]
    assert c1.is_archive_footage is False

    c2 = credits_by_title_id["tt99999999"]
    assert c2.episode.episode_code == "S5.E2"
    assert c2.attributes == ["archive footage", "as Britani"]
    assert c2.is_archive_footage is True

    c3 = credits_by_title_id["tt00000000"]
    assert c3.episode.season_number is None
    assert c3.episode.episode_number is None
    assert c3.episode.episode_code is None
    assert c3.episode.year is None

