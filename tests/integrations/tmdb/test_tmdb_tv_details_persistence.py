from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from trr_backend.models.shows import ShowRecord


def test_stage1_tmdb_list_ingestion_persists_tv_details_into_tmdb_meta(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.ingestion import show_importer as mod
    from trr_backend.ingestion.shows_from_lists import TmdbListItem

    repo_root = Path(__file__).resolve().parents[3]
    details = json.loads(
        (repo_root / "tests" / "fixtures" / "tmdb" / "tv_details_full_sample.json").read_text(encoding="utf-8")
    )

    monkeypatch.setattr(
        mod,
        "fetch_tmdb_list_items",
        lambda *args, **kwargs: [
            TmdbListItem(
                tmdb_id=12345,
                imdb_id="tt1353056",
                name="RuPaul's Drag Race",
                first_air_date="2009-02-02",
                origin_country=["US"],
            )
        ],
    )
    monkeypatch.setattr(mod, "fetch_tv_details", lambda *args, **kwargs: details)

    candidates = mod.collect_candidates_from_lists(
        imdb_list_urls=[],
        tmdb_lists=["8301263"],
        resolve_tmdb_external_ids=False,
        tmdb_fetch_details=True,
    )
    assert len(candidates) == 1

    result = mod.upsert_candidates_into_supabase(
        candidates,
        dry_run=True,
        annotate_imdb_episodic=False,
    )

    assert result.created == 1
    row = result.upserted_show_rows[0]
    external_ids = row["external_ids"]
    assert external_ids["tmdb"] == 12345

    tmdb_meta = external_ids["tmdb_meta"]
    assert tmdb_meta["id"] == 12345
    assert tmdb_meta["name"] == "RuPaul's Drag Race"
    assert tmdb_meta["overview"] == "A series featuring drag performers competing for the crown."
    assert tmdb_meta["vote_average"] == 8.5
    assert tmdb_meta["vote_count"] == 123456
    assert tmdb_meta["number_of_seasons"] == 16
    assert tmdb_meta["number_of_episodes"] == 250
    assert tmdb_meta["networks"][0]["name"] == "VH1"
    assert tmdb_meta["seasons"][0]["season_number"] == 16


def test_stage2_uses_tmdb_meta_and_does_not_refetch_tv_details(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    details = json.loads(
        (repo_root / "tests" / "fixtures" / "tmdb" / "tv_details_full_sample.json").read_text(encoding="utf-8")
    )
    providers = json.loads(
        (repo_root / "tests" / "fixtures" / "tmdb" / "tv_watch_providers_sample.json").read_text(encoding="utf-8")
    )

    from trr_backend.ingestion import show_metadata_enricher as mod

    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-18T00:00:00Z")
    monkeypatch.setattr(mod, "resolve_api_key", lambda: "fake")
    monkeypatch.setattr(mod, "find_by_imdb_id", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))

    fetch_details_mock = MagicMock(return_value=details)
    monkeypatch.setattr(mod, "fetch_tv_details", fetch_details_mock)
    monkeypatch.setattr(mod, "fetch_tv_watch_providers", lambda *args, **kwargs: providers)
    monkeypatch.setattr(
        mod.HttpImdbTitleMetadataClient,
        "fetch_title_page",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("IMDb title fallback should not be invoked.")),
    )
    monkeypatch.setattr(
        mod.HttpImdbTitleMetadataClient,
        "fetch_episodes_page",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("IMDb episodes fallback should not be invoked.")),
    )

    show = ShowRecord(
        id=UUID("00000000-0000-0000-0000-000000000010"),
        title="RuPaul's Drag Race",
        external_ids={"imdb": "tt1353056", "tmdb": 12345, "tmdb_meta": details},
    )

    summary = mod.enrich_shows_after_upsert([show], region="US", concurrency=1, force_refresh=False)
    assert summary.failed == 0
    assert summary.updated == 1

    assert fetch_details_mock.call_count == 0

    patch = summary.patches[0].external_ids_update
    show_meta = patch["show_meta"]
    assert show_meta["network"] == "VH1"
    assert show_meta["show_total_seasons"] == 16
    assert show_meta["show_total_episodes"] == 250
    assert show_meta["most_recent_episode"] == "S16.E16 - Grand Finale (2024-04-19)"
    assert show_meta["source"]["tmdb"] == "details|watch_providers"

