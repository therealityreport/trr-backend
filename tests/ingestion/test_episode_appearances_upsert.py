from __future__ import annotations

from unittest.mock import MagicMock

from trr_backend.repositories.episode_appearances import upsert_episode_appearances


def test_upsert_episode_appearances_uses_on_conflict() -> None:
    db = MagicMock()
    table = db.schema.return_value.table.return_value

    response = MagicMock()
    response.error = None
    response.data = []
    table.upsert.return_value.execute.return_value = response

    rows = [
        {
            "show_id": "show-1",
            "person_id": "person-1",
            "episode_imdb_id": "tt0000001",
            "credit_category": "Self",
        }
    ]

    upsert_episode_appearances(db, rows)

    db.schema.assert_called_once_with("core")
    db.schema.return_value.table.assert_called_once_with("episode_appearances")
    _, kwargs = table.upsert.call_args
    assert kwargs["on_conflict"] == "show_id,person_id,episode_imdb_id,credit_category"
