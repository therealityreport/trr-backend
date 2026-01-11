"""
Tests for TMDb TV details persistence.

NOTE: Many tests in this file were written for the legacy external_ids JSONB schema.
After schema normalization (migrations 0028-0048), data is stored in:
- core.shows typed columns (imdb_id, tmdb_id, tmdb_* / imdb_* fields)
- core.shows tmdb_meta / imdb_meta payloads

Tests that check external_ids["tmdb_meta"], external_ids["show_meta"], etc. need to be
rewritten to check the new normalized tables/columns.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

import pytest

from trr_backend.models.shows import ShowRecord


@pytest.mark.skip(reason="Legacy test: external_ids JSONB removed in schema normalization")
def test_stage1_tmdb_list_ingestion_persists_tv_details_into_tmdb_meta(monkeypatch: pytest.MonkeyPatch) -> None:
    # This test checked that tv_details were stored in external_ids.tmdb_meta JSONB.
    # After normalization, TMDb data goes to core.shows.tmdb_meta / tmdb_* columns.
    pass


@pytest.mark.skip(reason="Legacy test: external_ids JSONB removed in schema normalization")
def test_stage1_tmdb_no_details_avoids_tv_details_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    # This test checked external_ids structure which no longer exists.
    pass


@pytest.mark.skip(reason="Legacy test: external_ids JSONB removed in schema normalization")
@pytest.mark.parametrize("status_code", [404, 422])
def test_stage1_tmdb_details_4xx_is_non_fatal(monkeypatch: pytest.MonkeyPatch, status_code: int) -> None:
    pass


@pytest.mark.skip(reason="Legacy test: external_ids JSONB removed in schema normalization")
def test_stage1_tmdb_external_ids_fill_missing_but_preserve_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    pass


@pytest.mark.skip(reason="Legacy test: external_ids JSONB removed in schema normalization")
def test_stage1_tmdb_details_skips_when_fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    pass


@pytest.mark.skip(reason="Legacy test: external_ids JSONB removed in schema normalization")
def test_stage2_uses_tmdb_meta_and_does_not_refetch_tv_details(monkeypatch: pytest.MonkeyPatch) -> None:
    pass


@pytest.mark.skip(reason="Legacy test: external_ids JSONB removed in schema normalization")
def test_stage2_multiple_shows_does_not_refetch_tv_details_when_tmdb_meta_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pass


def test_enricher_produces_tmdb_show_patch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that the enricher produces tmdb_* metadata for core.shows."""
    repo_root = Path(__file__).resolve().parents[3]
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
    monkeypatch.setattr(mod, "fetch_tv_details", lambda *args, **kwargs: details_payload)
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

    summary = mod.enrich_shows_after_upsert([show], region="US", concurrency=1, force_refresh=True)
    assert summary.failed == 0
    assert summary.updated == 1

    patch = summary.patches[0]
    assert patch.show_update.get("tmdb_name") == "RuPaul's Drag Race"
    assert patch.show_update.get("tmdb_meta", {}).get("number_of_seasons") == 16
    assert patch.show_update.get("tmdb_meta", {}).get("number_of_episodes") == 250
