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
        "name": "RuPaul's Drag Race",
        "description": None,
        "premiere_date": None,
        "tmdb_series_id": 12345,
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
    assert all(r.get("tmdb_id") == 12345 for r in rows)
    assert all(r.get("source") == "tmdb" for r in rows)
    assert all(r.get("source_image_id") == r.get("file_path") for r in rows)
    assert all(r.get("url_path") == r.get("file_path") for r in rows)
    assert all(r.get("url") == f"https://image.tmdb.org/t/p/original{r.get('file_path')}" for r in rows)
    assert all(r.get("image_type") == r.get("kind") for r in rows)
    assert all(r.get("fetched_at") == "2025-12-18T00:00:00Z" for r in rows)
    assert all(r.get("updated_at") == "2025-12-18T00:00:00Z" for r in rows)
    assert all("url_original" not in r for r in rows)
    assert all("vote_average" not in r for r in rows)
    assert all("vote_count" not in r for r in rows)
    assert all(isinstance(r.get("width"), int) for r in rows)
    assert all(isinstance(r.get("height"), int) for r in rows)
    assert all(isinstance(r.get("aspect_ratio"), float) for r in rows)
    assert all("metadata" in r for r in rows)

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


def test_show_images_read_path_uses_tmdb_id_not_show_id() -> None:
    from trr_backend.db.show_images import list_tmdb_show_images

    class _Resp:
        def __init__(self, data, error=None):
            self.data = data
            self.error = error

    class _Query:
        def __init__(self, parent, table_name: str):
            self.parent = parent
            self.table_name = table_name
            self.filters: list[tuple[str, object]] = []
            self._single = False

        def select(self, *args, **kwargs):
            return self

        def eq(self, key, value):
            self.filters.append((str(key), value))
            return self

        def single(self):
            self._single = True
            return self

        def execute(self):
            if self.table_name == "shows":
                # Show B has tmdb_series_id=222
                return _Resp({"tmdb_series_id": 222})
            if self.table_name == "v_show_images":
                tmdb_id = next((v for (k, v) in self.filters if k == "tmdb_id"), None)
                assert tmdb_id == 222
                source = next((v for (k, v) in self.filters if k == "source"), None)
                assert source == "tmdb"
                # Simulated view data: includes a "bad" row that points to Show B by show_id but has tmdb_id=111.
                all_rows = [
                    {
                        "id": "00000000-0000-0000-0000-000000000101",
                        "show_id": "00000000-0000-0000-0000-0000000000b2",
                        "tmdb_id": 111,
                        "show_name": "Show B",
                        "source": "tmdb",
                        "kind": "backdrop",
                        "file_path": "/wrong.jpg",
                        "url_original": "https://image.tmdb.org/t/p/original/wrong.jpg",
                    },
                    {
                        "id": "00000000-0000-0000-0000-000000000202",
                        "show_id": "00000000-0000-0000-0000-0000000000b2",
                        "tmdb_id": 222,
                        "show_name": "Show B",
                        "source": "tmdb",
                        "kind": "backdrop",
                        "file_path": "/right.jpg",
                        "url_original": "https://image.tmdb.org/t/p/original/right.jpg",
                    },
                ]
                return _Resp([r for r in all_rows if r["tmdb_id"] == tmdb_id and r["source"] == source])
            raise AssertionError(f"Unexpected table: {self.table_name}")

    class _Schema:
        def __init__(self, parent):
            self.parent = parent

        def table(self, name: str):
            return _Query(self.parent, name)

    class _Db:
        def schema(self, name: str):
            assert name == "core"
            return _Schema(self)

    rows = list_tmdb_show_images(_Db(), show_id="00000000-0000-0000-0000-0000000000b2")
    assert [r["file_path"] for r in rows] == ["/right.jpg"]
    assert rows[0]["show_name"] == "Show B"
    assert rows[0]["url_original"] == "https://image.tmdb.org/t/p/original/right.jpg"
    assert "vote_average" not in rows[0]
    assert "vote_count" not in rows[0]
