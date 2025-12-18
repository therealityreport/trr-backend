from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def test_stage1_tmdb_fetch_images_upserts_show_images_and_sets_primary_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.ingestion import show_importer as mod
    from trr_backend.ingestion.shows_from_lists import CandidateShow

    repo_root = Path(__file__).resolve().parents[3]
    images_payload = json.loads(
        (repo_root / "tests" / "fixtures" / "tmdb" / "tv_images_sample.json").read_text(encoding="utf-8")
    )

    monkeypatch.setattr(mod, "assert_core_shows_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "assert_core_show_images_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "find_show_by_imdb_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "find_show_by_tmdb_id", lambda *args, **kwargs: None)

    inserted_row = {
        "id": "00000000-0000-0000-0000-000000000010",
        "title": "RuPaul's Drag Race",
        "description": None,
        "premiere_date": None,
        "external_ids": {"tmdb": 12345},
    }
    monkeypatch.setattr(mod, "insert_show", lambda *args, **kwargs: dict(inserted_row))

    update_show_mock = MagicMock(side_effect=lambda db, show_id, patch: {**dict(inserted_row), **dict(patch)})
    monkeypatch.setattr(mod, "update_show", update_show_mock)

    fetch_images_mock = MagicMock(return_value=images_payload)
    monkeypatch.setattr(mod, "fetch_tv_images", fetch_images_mock)
    monkeypatch.setattr(mod, "_now_utc_iso", lambda: "2025-12-18T00:00:00Z")

    upsert_images_mock = MagicMock(return_value=[])
    monkeypatch.setattr(mod, "upsert_show_images", upsert_images_mock)

    result = mod.upsert_candidates_into_supabase(
        [CandidateShow(imdb_id=None, tmdb_id=12345, title="RuPaul's Drag Race")],
        dry_run=False,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=False,
        tmdb_fetch_images=True,
        supabase_client=object(),
    )
    assert result.created == 1

    assert fetch_images_mock.call_count == 1
    _, call_kwargs = fetch_images_mock.call_args
    assert call_kwargs["include_image_language"] == "en,null"
    assert upsert_images_mock.call_count == 1

    rows = upsert_images_mock.call_args[0][1]
    assert isinstance(rows, list)
    assert len(rows) == 8  # 4 posters (deduped), 2 backdrops, 2 logos

    poster_dup = [r for r in rows if r.get("kind") == "poster" and r.get("file_path") == "/poster_dup.jpg"]
    assert len(poster_dup) == 1
    assert poster_dup[0].get("iso_639_1") == "en"

    assert all(r.get("show_id") == inserted_row["id"] for r in rows)
    assert all(r.get("source") == "tmdb" for r in rows)
    assert all(r.get("fetched_at") == "2025-12-18T00:00:00Z" for r in rows)

    assert update_show_mock.call_count == 1
    patch = update_show_mock.call_args[0][2]
    assert patch["primary_tmdb_poster_path"] == "/poster_en_votes_high.jpg"
    assert patch["primary_tmdb_backdrop_path"] == "/backdrop_en.jpg"
    assert patch["primary_tmdb_logo_path"] == "/logo_en.jpg"


def test_stage1_tmdb_no_images_skips_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.ingestion import show_importer as mod
    from trr_backend.ingestion.shows_from_lists import CandidateShow

    fetch_images_mock = MagicMock(side_effect=AssertionError("Stage 1 should not fetch /tv/{id}/images when disabled."))
    monkeypatch.setattr(mod, "fetch_tv_images", fetch_images_mock)

    result = mod.upsert_candidates_into_supabase(
        [CandidateShow(imdb_id=None, tmdb_id=12345, title="Show")],
        dry_run=True,
        annotate_imdb_episodic=False,
        tmdb_fetch_details=False,
        tmdb_fetch_images=False,
    )
    assert result.created == 1
    assert fetch_images_mock.call_count == 0
