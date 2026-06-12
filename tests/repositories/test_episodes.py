from trr_backend.repositories.episodes import _build_episode_upsert_payload


def test_episode_upsert_payload_defaults_missing_external_ids() -> None:
    payload = _build_episode_upsert_payload(
        {
            "show_id": "show-1",
            "season_number": 10,
            "episode_number": 1,
            "external_ids": None,
            "tmdb_episode_id": 6749886,
            "imdb_episode_id": None,
        }
    )

    assert payload == {
        "show_id": "show-1",
        "season_number": 10,
        "episode_number": 1,
        "external_ids": {},
        "tmdb_episode_id": 6749886,
    }


def test_episode_upsert_payload_preserves_external_ids() -> None:
    payload = _build_episode_upsert_payload(
        {
            "show_id": "show-1",
            "season_number": 1,
            "episode_number": 1,
            "external_ids": {"tmdb": 123, "imdb": "tt123"},
        }
    )

    assert payload["external_ids"] == {"tmdb": 123, "imdb": "tt123"}
