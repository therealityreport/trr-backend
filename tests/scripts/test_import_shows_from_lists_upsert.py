from __future__ import annotations

from unittest.mock import MagicMock

from trr_backend.ingestion.show_importer import upsert_candidates_into_supabase
from trr_backend.ingestion.shows_from_lists import CandidateShow


def test_upsert_candidates_inserts_when_missing(monkeypatch):
    from trr_backend.ingestion import show_importer as mod

    fake_db = object()
    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", lambda *args, **kwargs: None)

    insert_mock = MagicMock(return_value={"id": "00000000-0000-0000-0000-000000000001", "title": "New Show"})
    update_mock = MagicMock()
    monkeypatch.setattr(mod, "insert_show", insert_mock)
    monkeypatch.setattr(mod, "update_show", update_mock)

    candidates = [
        CandidateShow(imdb_id="tt1111111", tmdb_id=123, title="New Show", source_tags={"imdb-list:ls1"})
    ]

    result = upsert_candidates_into_supabase(
        candidates,
        dry_run=False,
        annotate_imdb_episodic=False,
        supabase_client=fake_db,
    )

    assert result.created == 1
    assert result.updated == 0
    assert result.skipped == 0
    insert_mock.assert_called_once()
    update_mock.assert_not_called()


def test_upsert_candidates_updates_external_ids_without_clobber(monkeypatch):
    from trr_backend.ingestion import show_importer as mod

    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    existing = {
        "id": "00000000-0000-0000-0000-000000000002",
        "title": "Existing Show",
        "premiere_date": None,
        "external_ids": {
            "imdb": "tt1111111",
            "import_sources": ["old-source"],
            "tmdb_meta": {"origin_country": ["US"]},
        },
    }

    fake_db = object()
    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *args, **kwargs: existing)
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "insert_show", MagicMock())

    update_mock = MagicMock(return_value={**existing, "external_ids": {}})
    monkeypatch.setattr(mod, "update_show", update_mock)

    candidates = [
        CandidateShow(
            imdb_id="tt1111111",
            tmdb_id=123,
            title="Existing Show",
            first_air_date="2020-01-01",
            origin_country=["US"],
            source_tags={"imdb-list:ls1", "tmdb-list:8301263"},
        )
    ]

    result = upsert_candidates_into_supabase(
        candidates,
        dry_run=False,
        annotate_imdb_episodic=True,
        supabase_client=fake_db,
    )

    assert result.created == 0
    assert result.updated == 1
    assert result.skipped == 0

    args, kwargs = update_mock.call_args
    assert args[1] == existing["id"]
    patch = args[2]
    assert set(patch.keys()) == {"external_ids", "premiere_date"}

    external_ids = patch["external_ids"]
    assert external_ids["imdb"] == "tt1111111"
    assert external_ids["tmdb"] == 123
    assert set(external_ids["import_sources"]) == {"old-source", "imdb-list:ls1", "tmdb-list:8301263"}
    assert external_ids["tmdb_meta"]["origin_country"] == ["US"]
    assert external_ids["tmdb_meta"]["first_air_date"] == "2020-01-01"
    assert external_ids["imdb_episodic"]["supported"] is True
    assert patch["premiere_date"] == "2020-01-01"
