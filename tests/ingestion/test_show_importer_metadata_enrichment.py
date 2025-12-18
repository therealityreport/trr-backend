from __future__ import annotations

from unittest.mock import MagicMock
from uuid import UUID

from trr_backend.ingestion.show_metadata_enricher import EnrichSummary, ShowEnrichmentPatch
from trr_backend.ingestion.show_importer import upsert_candidates_into_supabase
from trr_backend.ingestion.shows_from_lists import CandidateShow


def test_show_importer_applies_show_meta_patches(monkeypatch):
    from trr_backend.ingestion import show_importer as mod

    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    show_id = UUID("00000000-0000-0000-0000-0000000000aa")
    existing = {
        "id": str(show_id),
        "title": "Existing Show",
        "description": None,
        "premiere_date": None,
        "external_ids": {"imdb": "tt1234567"},
    }

    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *args, **kwargs: existing)
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "insert_show", MagicMock())

    update_mock = MagicMock(return_value={**existing, "external_ids": {**existing["external_ids"], "show_meta": {}}})
    monkeypatch.setattr(mod, "update_show", update_mock)

    summary = EnrichSummary(
        attempted=1,
        updated=1,
        skipped=0,
        skipped_complete=0,
        failed=0,
        patches=[
            ShowEnrichmentPatch(
                show_id=show_id,
                external_ids_update={
                    "show_meta": {
                        "show": "Existing Show",
                        "imdb_series_id": "tt1234567",
                        "tmdb_series_id": None,
                        "network": None,
                        "streaming": None,
                        "show_total_seasons": None,
                        "show_total_episodes": None,
                        "most_recent_episode": None,
                        "most_recent_episode_obj": {
                            "season": None,
                            "episode": None,
                            "title": None,
                            "air_date": None,
                            "imdb_episode_id": None,
                        },
                        "source": {},
                        "fetched_at": "2025-12-18T00:00:00Z",
                        "region": "US",
                    }
                },
            )
        ],
        failures=[],
    )

    monkeypatch.setattr(mod, "enrich_shows_after_upsert", lambda *args, **kwargs: summary)

    fake_db = object()
    candidates = [CandidateShow(imdb_id="tt1234567", tmdb_id=None, title="Existing Show")]
    result = upsert_candidates_into_supabase(
        candidates,
        dry_run=False,
        annotate_imdb_episodic=False,
        enrich_show_metadata=True,
        supabase_client=fake_db,
    )

    assert result.created == 0
    assert result.updated == 0
    assert result.skipped == 1

    update_mock.assert_called_once()
    args, _kwargs = update_mock.call_args
    assert args[1] == show_id
    patch = args[2]
    assert set(patch.keys()) == {"external_ids"}
    assert "show_meta" in patch["external_ids"]
