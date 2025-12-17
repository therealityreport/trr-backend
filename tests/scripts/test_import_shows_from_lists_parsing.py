from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from trr_backend.ingestion.shows_from_lists import fetch_imdb_list_items, fetch_tmdb_list_items, parse_imdb_list_url


@dataclass
class _FakeResponse:
    status_code: int
    content: bytes

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, url_to_response: dict[str, _FakeResponse]) -> None:
        self._url_to_response = url_to_response
        self.calls: list[str] = []

    def get(self, url: str, *args, **kwargs) -> _FakeResponse:  # noqa: ANN002, ANN003
        self.calls.append(url)
        return self._url_to_response.get(url, _FakeResponse(status_code=404, content=b""))


def test_parse_imdb_list_url_extracts_ls_id() -> None:
    assert parse_imdb_list_url("https://www.imdb.com/list/ls123456789/") == "ls123456789"


def test_fetch_imdb_list_items_parses_jsonld_and_paginates() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    page1 = (repo_root / "tests" / "fixtures" / "imdb" / "list_sample.html").read_bytes()
    page2 = (repo_root / "tests" / "fixtures" / "imdb" / "list_sample_page2.html").read_bytes()

    session = _FakeSession(
        {
            "https://www.imdb.com/list/ls123456789/": _FakeResponse(status_code=200, content=page1),
            "https://www.imdb.com/list/ls123456789/?page=2": _FakeResponse(status_code=200, content=page2),
        }
    )

    items = fetch_imdb_list_items("ls123456789", session=session, max_pages=3)
    assert {i.imdb_id for i in items} == {"tt1111111", "tt2222222", "tt3333333"}
    assert any(i.title == "Sample Series One" for i in items)


def test_fetch_tmdb_list_items_parses_tv_items_and_external_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    # Patch the TMDb integration calls inside the ingestion module.
    from trr_backend.ingestion import shows_from_lists as mod

    monkeypatch.setattr(
        mod,
        "fetch_list_items",
        lambda *args, **kwargs: [
            {"media_type": "tv", "id": 100, "name": "TMDb Show", "first_air_date": "2019-01-01", "origin_country": ["US"]},
            {"media_type": "movie", "id": 200, "title": "Ignore Movie"},
        ],
    )
    monkeypatch.setattr(mod, "fetch_tv_external_ids", lambda *args, **kwargs: {"imdb_id": "tt9999999"})

    items = fetch_tmdb_list_items("8301263", api_key="fake", session=object(), resolve_external_ids=True)
    assert len(items) == 1
    assert items[0].tmdb_id == 100
    assert items[0].imdb_id == "tt9999999"
    assert items[0].name == "TMDb Show"
    assert items[0].first_air_date == "2019-01-01"
    assert items[0].origin_country == ["US"]

