from __future__ import annotations

from unittest.mock import MagicMock

from api.routers.admin_show_sync import _enrich_imdb_mediaindex_rows_with_episode_context


def test_enrich_imdb_mediaindex_rows_with_episode_context_sets_show_season_episode_fields() -> None:
    db = MagicMock()
    response = MagicMock()
    response.error = None
    response.data = [
        {
            "id": "episode-1",
            "imdb_episode_id": "tt7654321",
            "title": "Reunion Part 1",
            "episode_number": 16,
            "season_number": 5,
            "air_date": "2025-01-22",
            "show_id": "show-123",
            "show_name": "The Real Housewives of Salt Lake City",
        }
    ]
    episode_query = db.schema.return_value.table.return_value.select.return_value.eq.return_value.in_.return_value
    episode_query.execute.return_value = response

    rows = [
        {
            "show_id": "show-123",
            "source": "imdb",
            "kind": "media",
            "image_type": "Still Frame",
            "metadata": {
                "tags": {
                    "people": [{"imdb_id": "nm100", "name": "Lisa Barlow"}],
                    "titles": [
                        {"imdb_id": "tt8819906", "title": "The Real Housewives of Salt Lake City"},
                        {"imdb_id": "tt7654321", "title": "Reunion Part 1"},
                    ],
                    "image_type": "Still Frame",
                }
            },
        }
    ]

    _enrich_imdb_mediaindex_rows_with_episode_context(
        db,
        show_id="show-123",
        show_name="The Real Housewives of Salt Lake City",
        show_imdb_id="tt8819906",
        rows=rows,
    )

    row = rows[0]
    metadata = row["metadata"]
    assert row["kind"] == "episode_still"
    assert row["image_type"] == "Still Frame"
    assert metadata["show_id"] == "show-123"
    assert metadata["show_name"] == "The Real Housewives of Salt Lake City"
    assert metadata["show_imdb_id"] == "tt8819906"
    assert metadata["episode_imdb_id"] == "tt7654321"
    assert metadata["season_number"] == 5
    assert metadata["episode_number"] == 16
    assert metadata["people_names"] == ["Lisa Barlow"]
    assert metadata["title_names"] == ["The Real Housewives of Salt Lake City", "Reunion Part 1"]
