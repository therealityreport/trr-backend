from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from trr_backend.ingestion.shows_from_lists import (
    fetch_imdb_list_items,
    fetch_tmdb_list_items,
    parse_imdb_list_id,
    parse_imdb_list_page,
    parse_imdb_list_url,
)


@dataclass
class _FakeResponse:
    status_code: int
    text: str


class _FakeSession:
    def __init__(self, url_to_response: dict[str, _FakeResponse]) -> None:
        self._url_to_response = url_to_response
        self.calls: list[str] = []

    def get(self, url: str, *args, **kwargs) -> _FakeResponse:  # noqa: ANN002, ANN003
        self.calls.append(url)
        return self._url_to_response.get(url, _FakeResponse(status_code=404, text=""))


def test_parse_imdb_list_url_extracts_ls_id() -> None:
    assert parse_imdb_list_url("https://www.imdb.com/list/ls123456789/") == "ls123456789"


def test_parse_imdb_list_id_accepts_share_link() -> None:
    assert parse_imdb_list_id("ls4106677119") == "ls4106677119"
    assert parse_imdb_list_id("https://www.imdb.com/list/ls4106677119/?ref_=ext_shr_lnk") == "ls4106677119"


def test_parse_imdb_list_page_parses_jsonld_itemlist() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "list_jsonld_sample.html").read_text(encoding="utf-8")

    items = parse_imdb_list_page(html)
    assert [(i.imdb_id, i.title) for i in items] == [
        ("tt1111111", "JSON-LD Series One"),
        ("tt2222222", "JSON-LD Series Two"),
        ("tt3333333", "JSON-LD Series Three"),
    ]
    assert all(i.year is None for i in items)
    assert all(i.extra.get("source") == "jsonld" for i in items)


def test_parse_imdb_list_page_html_fallback_extracts_optional_fields() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "list_html_fallback_sample.html").read_text(encoding="utf-8")

    items = parse_imdb_list_page(html)
    assert {i.imdb_id for i in items} == {"tt9999999", "tt8888888"}

    first = next(i for i in items if i.imdb_id == "tt9999999")
    assert first.title == "Fallback Series One"
    assert first.year == 2024
    assert first.extra.get("source") == "html"
    assert first.extra.get("rank") == 1
    assert isinstance(first.extra.get("description"), str) and "long description" in first.extra.get("description")

    second = next(i for i in items if i.imdb_id == "tt8888888")
    assert second.title == "Fallback Series Two"
    assert second.year is None
    assert second.extra.get("source") == "html"
    assert second.extra.get("rank") == 2


def test_fetch_imdb_list_items_parses_jsonld_and_paginates() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    page1 = (repo_root / "tests" / "fixtures" / "imdb" / "list_sample.html").read_text(encoding="utf-8")
    page2 = (repo_root / "tests" / "fixtures" / "imdb" / "list_sample_page2.html").read_text(encoding="utf-8")

    session = _FakeSession(
        {
            "https://www.imdb.com/list/ls123456789/": _FakeResponse(status_code=200, text=page1),
            "https://www.imdb.com/list/ls123456789/?page=2": _FakeResponse(status_code=200, text=page2),
        }
    )

    items = fetch_imdb_list_items("ls123456789", session=session, max_pages=3)
    assert {i.imdb_id for i in items} == {"tt1111111", "tt2222222", "tt3333333"}
    assert any(i.title == "Sample Series One" for i in items)


def test_fetch_imdb_list_items_stops_when_no_new_ids() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    page1 = (repo_root / "tests" / "fixtures" / "imdb" / "list_sample.html").read_text(encoding="utf-8")

    session = _FakeSession(
        {
            "https://www.imdb.com/list/ls123456789/": _FakeResponse(status_code=200, text=page1),
            # Page 2 repeats the same IDs; importer should stop after this page.
            "https://www.imdb.com/list/ls123456789/?page=2": _FakeResponse(status_code=200, text=page1),
        }
    )

    items = fetch_imdb_list_items("ls123456789", session=session, max_pages=5)
    assert {i.imdb_id for i in items} == {"tt1111111", "tt2222222"}
    assert session.calls == [
        "https://www.imdb.com/list/ls123456789/",
        "https://www.imdb.com/list/ls123456789/?page=2",
    ]


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
