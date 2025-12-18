from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID
from datetime import datetime, timezone

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
    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-18T00:00:00Z")

    candidates = mod.collect_candidates_from_lists(
        imdb_list_urls=[],
        tmdb_lists=["8301263"],
        resolve_tmdb_external_ids=False,
    )
    assert len(candidates) == 1

    result = mod.upsert_candidates_into_supabase(
        candidates,
        dry_run=True,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=True,
    )

    assert result.created == 1
    row = result.upserted_show_rows[0]
    external_ids = row["external_ids"]
    assert external_ids["tmdb"] == 12345

    tmdb_meta = external_ids["tmdb_meta"]
    assert tmdb_meta["_v"] == 1
    assert tmdb_meta["id"] == 12345
    assert tmdb_meta["name"] == "RuPaul's Drag Race"
    assert tmdb_meta["overview"] == "A series featuring drag performers competing for the crown."
    assert tmdb_meta["vote_average"] == 8.5
    assert tmdb_meta["vote_count"] == 123456
    assert tmdb_meta["number_of_seasons"] == 16
    assert tmdb_meta["number_of_episodes"] == 250
    assert tmdb_meta["networks"][0]["name"] == "VH1"
    assert tmdb_meta["seasons"][0]["season_number"] == 16
    assert tmdb_meta["alternative_titles"][0]["iso_3166_1"] == "US"
    assert tmdb_meta["external_ids"]["imdb_id"] == "tt1353056"
    assert tmdb_meta["external_ids"]["tvdb_id"] == 8501
    assert tmdb_meta["language"] == "en-US"
    assert tmdb_meta["fetched_at"] == "2025-12-18T00:00:00Z"

    # Canonical ids are filled from tmdb_meta.external_ids.
    assert external_ids["tvdb"] == 8501
    assert external_ids["wikidata"] == "Q123456"
    assert external_ids["facebook"] == "rupaulsdragrace"
    assert external_ids["instagram"] == "rupaulsdragrace"
    assert external_ids["twitter"] == "rupaulsdragrace"
    assert external_ids["tvrage"] == 1234


def test_stage1_tmdb_no_details_avoids_tv_details_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.ingestion import show_importer as mod
    from trr_backend.ingestion.shows_from_lists import CandidateShow

    fetch_details_mock = MagicMock(side_effect=AssertionError("Stage 1 should not fetch /tv/{id} when disabled."))
    monkeypatch.setattr(mod, "fetch_tv_details", fetch_details_mock)

    candidates = [
        CandidateShow(
            imdb_id="tt1353056",
            tmdb_id=12345,
            title="RuPaul's Drag Race",
            first_air_date="2009-02-02",
            origin_country=["US"],
        )
    ]

    result = mod.upsert_candidates_into_supabase(
        candidates,
        dry_run=True,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=False,
    )

    assert fetch_details_mock.call_count == 0
    assert result.created == 1
    external_ids = result.upserted_show_rows[0]["external_ids"]
    assert external_ids["tmdb"] == 12345
    assert external_ids["tmdb_meta"]["first_air_date"] == "2009-02-02"
    assert "vote_average" not in external_ids["tmdb_meta"]


@pytest.mark.parametrize("status_code", [404, 422])
def test_stage1_tmdb_details_4xx_is_non_fatal(monkeypatch: pytest.MonkeyPatch, status_code: int) -> None:
    from trr_backend.ingestion import show_importer as mod
    from trr_backend.ingestion.shows_from_lists import CandidateShow
    from trr_backend.integrations.tmdb.client import TmdbClientError

    monkeypatch.setattr(
        mod,
        "fetch_tv_details",
        lambda *args, **kwargs: (_ for _ in ()).throw(TmdbClientError("not found", status_code=status_code)),
    )

    candidates = [CandidateShow(imdb_id=None, tmdb_id=999999, title="Missing TMDb Show")]
    result = mod.upsert_candidates_into_supabase(
        candidates,
        dry_run=True,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=True,
    )

    assert result.created == 1
    external_ids = result.upserted_show_rows[0]["external_ids"]
    assert external_ids["tmdb"] == 999999
    # No curated details payload was stored.
    assert "vote_average" not in external_ids.get("tmdb_meta", {})


def test_stage1_tmdb_external_ids_fill_missing_but_preserve_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.ingestion import show_importer as mod
    from trr_backend.ingestion.shows_from_lists import CandidateShow

    repo_root = Path(__file__).resolve().parents[3]
    details = json.loads(
        (repo_root / "tests" / "fixtures" / "tmdb" / "tv_details_full_sample.json").read_text(encoding="utf-8")
    )

    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        mod,
        "find_show_by_tmdb_id",
        lambda *args, **kwargs: {
            "id": "00000000-0000-0000-0000-000000000040",
            "title": "Existing Show",
            "premiere_date": None,
            "external_ids": {
                "tmdb": 12345,
                "imdb": "tt9999999",
                "tvdb": 9999,
                "tmdb_meta": {"_v": 1, "id": 12345, "language": "en-US", "fetched_at": "2000-01-01T00:00:00Z"},
            },
        },
    )
    monkeypatch.setattr(mod, "fetch_tv_details", lambda *args, **kwargs: details)
    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-18T00:00:00Z")

    result = mod.upsert_candidates_into_supabase(
        [CandidateShow(imdb_id=None, tmdb_id=12345, title="Existing Show")],
        dry_run=True,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=True,
        supabase_client=object(),
    )

    assert result.updated == 1
    row = result.upserted_show_rows[0]
    external_ids = row["external_ids"]

    # Existing canonical ids are preserved.
    assert external_ids["imdb"] == "tt9999999"
    assert external_ids["tvdb"] == 9999

    # Missing canonical ids are filled from tmdb_meta.external_ids.
    assert external_ids["wikidata"] == "Q123456"
    assert external_ids["facebook"] == "rupaulsdragrace"
    assert external_ids["instagram"] == "rupaulsdragrace"
    assert external_ids["twitter"] == "rupaulsdragrace"
    assert external_ids["tvrage"] == 1234


def test_stage1_tmdb_details_skips_when_fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.ingestion import show_importer as mod
    from trr_backend.ingestion.shows_from_lists import CandidateShow

    fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    existing = {
        "id": "00000000-0000-0000-0000-000000000030",
        "title": "Existing Show",
        "tmdb_id": 12345,
        "premiere_date": None,
        "external_ids": {
            "tmdb": 12345,
            "imdb": "tt1353056",
            "tmdb_meta": {
                "_v": 1,
                "id": 12345,
                "language": "en-US",
                "fetched_at": fetched_at,
                "alternative_titles": [],
                "external_ids": {"imdb_id": "tt1353056"},
            },
        },
    }

    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", lambda *args, **kwargs: existing)
    monkeypatch.setattr(mod, "insert_show", MagicMock())
    monkeypatch.setattr(mod, "update_show", MagicMock())

    fetch_details_mock = MagicMock(side_effect=AssertionError("Should not refetch fresh tmdb_meta."))
    monkeypatch.setattr(mod, "fetch_tv_details", fetch_details_mock)

    candidates = [CandidateShow(imdb_id=None, tmdb_id=12345, title="Existing Show")]
    result = mod.upsert_candidates_into_supabase(
        candidates,
        dry_run=False,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=True,
        tmdb_details_max_age_days=90,
        supabase_client=object(),
    )

    assert fetch_details_mock.call_count == 0
    assert result.skipped == 1


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


def test_stage2_multiple_shows_does_not_refetch_tv_details_when_tmdb_meta_present(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    providers = json.loads(
        (repo_root / "tests" / "fixtures" / "tmdb" / "tv_watch_providers_sample.json").read_text(encoding="utf-8")
    )

    from trr_backend.ingestion import show_metadata_enricher as mod

    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-18T00:00:00Z")
    monkeypatch.setattr(mod, "resolve_api_key", lambda: "fake")

    fetch_details_mock = MagicMock(side_effect=AssertionError("Stage 2 should reuse tmdb_meta and not refetch details."))
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

    meta_a = {
        "id": 111,
        "networks": [{"name": "VH1"}],
        "number_of_seasons": 16,
        "number_of_episodes": 250,
        "last_episode_to_air": {"season_number": 16, "episode_number": 16, "name": "Finale", "air_date": "2024-04-19"},
    }
    meta_b = {
        "id": 222,
        "networks": [{"name": "Bravo"}],
        "number_of_seasons": 10,
        "number_of_episodes": 100,
        "last_episode_to_air": {"season_number": 10, "episode_number": 12, "name": "The End", "air_date": "2024-01-01"},
    }

    shows = [
        ShowRecord(id=UUID("00000000-0000-0000-0000-000000000020"), title="Show A", external_ids={"tmdb": 111, "tmdb_meta": meta_a, "imdb": "tt0000001"}),
        ShowRecord(id=UUID("00000000-0000-0000-0000-000000000021"), title="Show B", external_ids={"tmdb": 222, "tmdb_meta": meta_b, "imdb": "tt0000002"}),
    ]

    summary = mod.enrich_shows_after_upsert(shows, region="US", concurrency=1, force_refresh=False)
    assert summary.failed == 0
    assert summary.updated == 2
    assert fetch_details_mock.call_count == 0
