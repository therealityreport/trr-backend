from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from trr_backend.ingestion.show_metadata_enricher import enrich_shows_after_upsert, is_show_meta_complete
from trr_backend.integrations.imdb.title_metadata_client import (
    parse_imdb_episodes_page,
    parse_imdb_title_page,
    pick_most_recent_episode,
)
from trr_backend.models.shows import ShowRecord


def test_parse_imdb_title_page_from_jsonld() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "title_page_sample.html").read_text(encoding="utf-8")

    meta = parse_imdb_title_page(html)
    assert meta.title == "Sample Show"
    assert meta.network == "VH1"
    assert meta.total_seasons == 2
    assert meta.total_episodes == 20


def test_parse_imdb_episodes_page_seasons_and_most_recent_episode() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    overview_html = (repo_root / "tests" / "fixtures" / "imdb" / "episodes_page_overview_sample.html").read_text(
        encoding="utf-8"
    )
    season_html = (repo_root / "tests" / "fixtures" / "imdb" / "episodes_page_season3_sample.html").read_text(
        encoding="utf-8"
    )

    overview = parse_imdb_episodes_page(overview_html)
    assert overview.available_seasons == [1, 2, 3]
    assert overview.episodes == []

    season3 = parse_imdb_episodes_page(season_html, season=3)
    assert {e.imdb_episode_id for e in season3.episodes} == {"tt9000001", "tt9000002"}
    picked = pick_most_recent_episode(season3.episodes)
    assert picked is not None
    assert picked.season == 3
    assert picked.episode == 2
    assert picked.title == "Episode Two"
    assert picked.air_date == "2024-04-19"
    assert picked.imdb_episode_id == "tt9000002"


def test_enrich_shows_after_upsert_tmdb_primary(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    find_payload = json.loads((repo_root / "tests" / "fixtures" / "tmdb" / "find_by_imdb_id_sample.json").read_text())
    details_payload = json.loads((repo_root / "tests" / "fixtures" / "tmdb" / "tv_details_sample.json").read_text())
    providers_payload = json.loads(
        (repo_root / "tests" / "fixtures" / "tmdb" / "tv_watch_providers_sample.json").read_text()
    )

    from trr_backend.ingestion import show_metadata_enricher as mod

    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-18T00:00:00Z")
    monkeypatch.setattr(mod, "resolve_api_key", lambda: "fake")
    monkeypatch.setattr(mod, "find_by_imdb_id", lambda *args, **kwargs: find_payload)
    fetch_details_mock = MagicMock(return_value=details_payload)
    monkeypatch.setattr(mod, "fetch_tv_details", fetch_details_mock)
    monkeypatch.setattr(mod, "fetch_tv_watch_providers", lambda *args, **kwargs: providers_payload)
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
        id=UUID("00000000-0000-0000-0000-000000000001"),
        name="RuPaul's Drag Race",
        external_ids={"imdb": "tt1353056"},
    )

    summary = enrich_shows_after_upsert([show], region="US", concurrency=1, force_refresh=True)
    assert summary.failed == 0
    assert summary.updated == 1
    assert len(summary.patches) == 1
    assert fetch_details_mock.call_count == 1

    patch = summary.patches[0].external_ids_update
    assert patch["tmdb"] == 12345

    show_meta = patch["show_meta"]
    assert show_meta["show"] == "RuPaul's Drag Race"
    assert show_meta["imdb_series_id"] == "tt1353056"
    assert show_meta["tmdb_series_id"] == 12345
    assert show_meta["network"] == "VH1"
    assert show_meta["streaming"] == "Hulu, Paramount+"
    assert show_meta["show_total_seasons"] == 16
    assert show_meta["show_total_episodes"] == 250
    assert show_meta["most_recent_episode"] == "S16.E16 - Grand Finale (2024-04-19)"
    assert show_meta["most_recent_episode_obj"] == {
        "season": 16,
        "episode": 16,
        "title": "Grand Finale",
        "air_date": "2024-04-19",
        "imdb_episode_id": None,
    }
    assert show_meta["source"]["tmdb"] == "find|details|watch_providers"
    assert show_meta["fetched_at"] == "2025-12-18T00:00:00Z"
    assert show_meta["region"] == "US"


def test_enrich_shows_after_upsert_imdb_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    title_html = (repo_root / "tests" / "fixtures" / "imdb" / "title_page_sample.html").read_text(encoding="utf-8")
    overview_html = (repo_root / "tests" / "fixtures" / "imdb" / "episodes_page_overview_sample.html").read_text(
        encoding="utf-8"
    )
    season_html = (repo_root / "tests" / "fixtures" / "imdb" / "episodes_page_season3_sample.html").read_text(
        encoding="utf-8"
    )

    from trr_backend.ingestion import show_metadata_enricher as mod

    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-18T00:00:00Z")
    monkeypatch.setattr(mod, "resolve_api_key", lambda: None)

    def fake_fetch_title_page(self, imdb_id: str) -> str:  # noqa: ANN001
        return title_html

    def fake_fetch_episodes_page(self, imdb_id: str, *, season: int | None = None) -> str:  # noqa: ANN001
        return season_html if season == 3 else overview_html

    monkeypatch.setattr(mod.HttpImdbTitleMetadataClient, "fetch_title_page", fake_fetch_title_page)
    monkeypatch.setattr(mod.HttpImdbTitleMetadataClient, "fetch_episodes_page", fake_fetch_episodes_page)

    show = ShowRecord(
        id=UUID("00000000-0000-0000-0000-000000000002"),
        name="Sample Show",
        external_ids={"imdb": "tt1353056"},
    )

    summary = enrich_shows_after_upsert([show], region="US", concurrency=1, force_refresh=True)
    assert summary.failed == 0
    assert summary.updated == 1

    show_meta = summary.patches[0].external_ids_update["show_meta"]
    assert show_meta["show"] == "Sample Show"
    assert show_meta["imdb_series_id"] == "tt1353056"
    assert show_meta["tmdb_series_id"] is None
    assert show_meta["network"] == "VH1"
    assert show_meta["show_total_seasons"] == 3
    assert show_meta["show_total_episodes"] == 20
    assert show_meta["most_recent_episode"] == "S3.E2 - Episode Two (2024-04-19) [imdbEpisodeId=tt9000002]"
    assert show_meta["source"]["imdb"] == "title|episodes"


def test_is_show_meta_complete() -> None:
    assert not is_show_meta_complete({})
    assert not is_show_meta_complete({"show": "X"})

    assert is_show_meta_complete(
        {
            "show": "Sample Show",
            "network": "VH1",
            "streaming": None,
            "show_total_seasons": 3,
            "show_total_episodes": 20,
            "imdb_series_id": "tt1353056",
            "tmdb_series_id": None,
            "most_recent_episode": None,
        }
    )


def test_enrich_shows_after_upsert_skips_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.ingestion import show_metadata_enricher as mod

    monkeypatch.setattr(mod, "resolve_api_key", lambda: "fake")
    monkeypatch.setattr(mod, "find_by_imdb_id", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(mod, "fetch_tv_details", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(mod, "fetch_tv_watch_providers", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))

    show = ShowRecord(
        id=UUID("00000000-0000-0000-0000-000000000003"),
        name="Sample Show",
        external_ids={
            "show_meta": {
                "show": "Sample Show",
                "network": "VH1",
                "streaming": "",
                "show_total_seasons": 3,
                "show_total_episodes": 20,
                "imdb_series_id": "tt1353056",
                "tmdb_series_id": None,
                "most_recent_episode": None,
                "region": "US",
                "fetched_at": "2025-12-18T00:00:00Z",
            }
        },
    )

    summary = enrich_shows_after_upsert([show], region="US", concurrency=1, force_refresh=False)
    assert summary.skipped_complete == 1
    assert summary.attempted == 0
    assert summary.updated == 0
    assert summary.patches == []


def test_enrich_shows_after_upsert_does_not_overwrite_ids_with_null(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    title_html = (repo_root / "tests" / "fixtures" / "imdb" / "title_page_sample.html").read_text(encoding="utf-8")
    overview_html = (repo_root / "tests" / "fixtures" / "imdb" / "episodes_page_overview_sample.html").read_text(
        encoding="utf-8"
    )
    season_html = (repo_root / "tests" / "fixtures" / "imdb" / "episodes_page_season3_sample.html").read_text(
        encoding="utf-8"
    )

    from trr_backend.ingestion import show_metadata_enricher as mod

    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-18T00:00:00Z")
    monkeypatch.setattr(mod, "resolve_api_key", lambda: None)

    def fake_fetch_title_page(self, imdb_id: str) -> str:  # noqa: ANN001
        assert imdb_id == "tt1353056"
        return title_html

    monkeypatch.setattr(mod.HttpImdbTitleMetadataClient, "fetch_title_page", fake_fetch_title_page)

    def fake_fetch_episodes_page(self, imdb_id: str, *, season: int | None = None) -> str:  # noqa: ANN001
        assert imdb_id == "tt1353056"
        return season_html if season == 3 else overview_html

    monkeypatch.setattr(mod.HttpImdbTitleMetadataClient, "fetch_episodes_page", fake_fetch_episodes_page)

    show = ShowRecord(
        id=UUID("00000000-0000-0000-0000-000000000004"),
        name="Sample Show",
        external_ids={
            "show_meta": {
                # Present ids should not be overwritten by missing external_ids.imdb/tmdb.
                "imdb_series_id": "tt1353056",
                "tmdb_series_id": 12345,
                # Force work by leaving required fields incomplete.
                "network": None,
                "streaming": None,
                "show_total_seasons": 3,
                "show_total_episodes": None,
                "most_recent_episode": None,
                "region": "US",
                "fetched_at": "2025-12-18T00:00:00Z",
            }
        },
    )

    summary = enrich_shows_after_upsert([show], region="US", concurrency=1, force_refresh=False)
    assert summary.failed == 0
    assert summary.updated == 1

    show_meta = summary.patches[0].external_ids_update["show_meta"]
    assert show_meta["imdb_series_id"] == "tt1353056"
    assert show_meta["tmdb_series_id"] == 12345
