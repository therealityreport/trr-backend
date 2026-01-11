from __future__ import annotations

from unittest.mock import MagicMock
from uuid import UUID

from trr_backend.ingestion.show_importer import upsert_candidates_into_supabase
from trr_backend.ingestion.show_metadata_enricher import EnrichSummary, ShowEnrichmentPatch
from trr_backend.ingestion.shows_from_lists import CandidateShow


def test_show_importer_applies_show_update_patches(monkeypatch):
    from trr_backend.ingestion import show_importer as mod

    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    show_id = UUID("00000000-0000-0000-0000-0000000000aa")
    existing = {
        "id": str(show_id),
        "name": "Existing Show",
        "description": None,
        "premiere_date": None,
        "imdb_id": "tt1234567",
        "tmdb_id": None,
    }

    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *args, **kwargs: existing)
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "insert_show", MagicMock())

    update_mock = MagicMock(return_value={**existing, "description": "Updated description"})
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
                show_update={
                    "description": "Updated description",
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
    # Show is updated via enrichment patches (show_update applied)
    assert result.updated == 1
    assert result.skipped == 0

    # update_show is called for the enrichment-based resolution flags update
    update_mock.assert_called()
    # Verify at least one call was for this show_id
    call_show_ids = [str(call[0][1]) for call in update_mock.call_args_list]
    assert str(show_id) in call_show_ids
