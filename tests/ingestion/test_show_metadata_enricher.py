from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from trr_backend.ingestion.show_metadata_enricher import enrich_shows_after_upsert
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
    title_html = (repo_root / "tests" / "fixtures" / "imdb" / "title_page_sample.html").read_text(encoding="utf-8")
    overview_html = (repo_root / "tests" / "fixtures" / "imdb" / "episodes_page_overview_sample.html").read_text(
        encoding="utf-8"
    )
    season_html = (repo_root / "tests" / "fixtures" / "imdb" / "episodes_page_season3_sample.html").read_text(
        encoding="utf-8"
    )

    from trr_backend.ingestion import show_metadata_enricher as mod

    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-18T00:00:00Z")
    monkeypatch.setattr(mod, "resolve_api_key", lambda: "fake")
    monkeypatch.setattr(mod, "find_by_imdb_id", lambda *args, **kwargs: find_payload)
    fetch_details_mock = MagicMock(return_value=details_payload)
    monkeypatch.setattr(mod, "fetch_tv_details", fetch_details_mock)
    monkeypatch.setattr(mod, "fetch_tv_watch_providers", lambda *args, **kwargs: providers_payload)
    monkeypatch.setattr(mod, "fetch_imdb_title_html", lambda *args, **kwargs: title_html)
    monkeypatch.setattr(mod, "fetch_imdb_mediaindex_html", lambda *args, **kwargs: None)

    def fake_fetch_episodes_page(self, imdb_id: str, *, season: int | None = None) -> str:  # noqa: ANN001
        return season_html if season is not None else overview_html

    monkeypatch.setattr(mod.HttpImdbTitleMetadataClient, "fetch_episodes_page", fake_fetch_episodes_page)

    show = ShowRecord(
        id=UUID("00000000-0000-0000-0000-000000000001"),
        name="RuPaul's Drag Race",
        imdb_id="tt1353056",
    )

    summary = enrich_shows_after_upsert([show], region="US", concurrency=1, force_refresh=True)
    assert summary.failed == 0
    assert summary.updated == 1
    assert len(summary.patches) == 1
    assert fetch_details_mock.call_count == 1

    patch = summary.patches[0]
    # Check show_update contains expected fields
    assert patch.show_update.get("tmdb_id") == 12345
    assert patch.show_update.get("tmdb_name") == "RuPaul's Drag Race"
    assert patch.show_update.get("tmdb_meta", {}).get("number_of_seasons") == 16
    assert patch.show_update.get("tmdb_meta", {}).get("number_of_episodes") == 250
    assert patch.tmdb_network_ids == [1]


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
    monkeypatch.setattr(mod, "fetch_imdb_title_html", lambda *args, **kwargs: title_html)
    monkeypatch.setattr(mod, "fetch_imdb_mediaindex_html", lambda *args, **kwargs: [])

    def fake_fetch_title_page(self, imdb_id: str) -> str:  # noqa: ANN001
        return title_html

    def fake_fetch_episodes_page(self, imdb_id: str, *, season: int | None = None) -> str:  # noqa: ANN001
        return season_html if season == 3 else overview_html

    monkeypatch.setattr(mod.HttpImdbTitleMetadataClient, "fetch_title_page", fake_fetch_title_page)
    monkeypatch.setattr(mod.HttpImdbTitleMetadataClient, "fetch_episodes_page", fake_fetch_episodes_page)

    show = ShowRecord(
        id=UUID("00000000-0000-0000-0000-000000000002"),
        name="Sample Show",
        imdb_id="tt1353056",
    )

    summary = enrich_shows_after_upsert([show], region="US", concurrency=1, force_refresh=True)
    assert summary.failed == 0
    assert summary.updated == 1

    patch = summary.patches[0]
    # Check imdb meta fields
    assert patch.show_update.get("imdb_title") == "Sample Show"
    assert patch.show_update.get("imdb_meta", {}).get("title") == "Sample Show"
    # total_seasons/episodes are in show_update (from HTML parsing), not imdb_meta (from JSON-LD)
    assert patch.show_update.get("show_total_seasons") == 3
    assert patch.show_update.get("show_total_episodes") == 20


def test_enrich_shows_after_upsert_imdb_meta(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    title_html = (repo_root / "tests" / "fixtures" / "imdb" / "title_page_tt8819906_sample.html").read_text(
        encoding="utf-8"
    )
    overview_html = (repo_root / "tests" / "fixtures" / "imdb" / "episodes_page_overview_sample.html").read_text(
        encoding="utf-8"
    )
    season_html = (repo_root / "tests" / "fixtures" / "imdb" / "episodes_page_season3_sample.html").read_text(
        encoding="utf-8"
    )

    from trr_backend.ingestion import show_metadata_enricher as mod

    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-29T00:00:00Z")
    monkeypatch.setattr(mod, "resolve_api_key", lambda: None)
    monkeypatch.setattr(mod, "fetch_imdb_title_html", lambda *args, **kwargs: title_html)
    monkeypatch.setattr(mod, "fetch_imdb_mediaindex_html", lambda *args, **kwargs: None)

    def fake_fetch_episodes_page(self, imdb_id: str, *, season: int | None = None) -> str:  # noqa: ANN001
        return season_html if season is not None else overview_html

    monkeypatch.setattr(mod.HttpImdbTitleMetadataClient, "fetch_episodes_page", fake_fetch_episodes_page)

    show = ShowRecord(
        id=UUID("00000000-0000-0000-0000-000000000005"),
        name="Love Island USA",
        imdb_id="tt8819906",
    )

    summary = enrich_shows_after_upsert([show], region="US", concurrency=1, force_refresh=True)
    assert summary.failed == 0
    assert summary.updated == 1

    patch = summary.patches[0]
    imdb_meta = patch.show_update.get("imdb_meta") or {}
    assert imdb_meta.get("description") == (
        "U.S. version of the British show 'Love Island' where a group of singles come to stay in a villa for a few "
        "weeks and have to couple up with one another."
    )
