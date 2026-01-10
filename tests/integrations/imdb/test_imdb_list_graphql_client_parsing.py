from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_parse_title_list_main_page_fixture_extracts_expected_fields() -> None:
    from trr_backend.ingestion.shows_from_lists import _parse_imdb_title_list_main_page_payload

    repo_root = Path(__file__).resolve().parents[3]
    payload = json.loads(
        (repo_root / "tests" / "fixtures" / "imdb" / "title_list_main_page_sample.json").read_text(encoding="utf-8")
    )

    total, items = _parse_imdb_title_list_main_page_payload(payload, list_id="ls4106677119")
    assert total == 166
    assert len(items) == 2

    rupaul = next(i for i in items if i.imdb_id == "tt1353056")
    assert rupaul.title == "RuPaul's Drag Race"
    assert rupaul.imdb_rating == 8.5
    assert rupaul.imdb_vote_count == 123456
    assert rupaul.description == "Plucky contestants compete for a drag title."
    assert rupaul.release_year == 2009
    assert rupaul.end_year is None
    assert rupaul.episodes_total == 220
    assert rupaul.title_type == "tvSeries"
    assert rupaul.primary_image_url.endswith("sample1.jpg")
    assert rupaul.primary_image_caption == "Poster"
    assert rupaul.certificate == "TV-14"
    assert rupaul.runtime_seconds == 3600
    assert rupaul.genres == ("Reality-TV",)
    assert rupaul.list_rank == 1
    assert rupaul.list_item_note is None

    rhoslc = next(i for i in items if i.imdb_id == "tt11363282")
    assert rhoslc.title == "The Real Housewives of Salt Lake City"
    assert rhoslc.imdb_rating == 6.6
    assert rhoslc.imdb_vote_count == 1125
    assert rhoslc.description == "Follows the affluent wives in Salt Lake City."
    assert rhoslc.release_year == 2020
    assert rhoslc.episodes_total == 70
    assert rhoslc.primary_image_url.endswith("sample2.jpg")
    assert rhoslc.primary_image_caption == "Key art"
    assert rhoslc.genres == ("Reality-TV", "Drama")
    assert rhoslc.list_rank == 2
    assert rhoslc.list_item_note == "List note for item 2"


def test_fetch_imdb_list_items_graphql_paginates_via_jump_to_position(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.ingestion import shows_from_lists as mod

    def make_edge(pos: int) -> dict:
        imdb_id = f"tt{pos:07d}"
        return {
            "node": {"absolutePosition": pos, "description": None},
            "listItem": {
                "id": imdb_id,
                "titleText": {"text": f"Show {pos}"},
                "ratingsSummary": {"aggregateRating": 7.1, "voteCount": 100},
                "plot": {"plotText": {"plainText": f"Plot {pos}"}},
                "releaseYear": {"year": 2000, "endYear": None},
                "episodes": {"episodes": {"total": 10}},
                "titleType": {"id": "tvSeries"},
                "primaryImage": {
                    "url": "https://m.media-amazon.com/images/M/sample.jpg",
                    "caption": {"plainText": "cap"},
                },
                "certificate": {"rating": "TV-14"},
                "runtime": {"seconds": 3600},
                "titleGenres": {"genres": [{"genre": {"text": "Reality-TV"}}]},
            },
        }

    class _FakeGraphqlClient:
        def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            self.calls: list[int] = []

        def fetch_title_list_main_page(self, list_id: str, *, first: int, jump_to_position: int, **kwargs) -> dict:  # noqa: ANN001
            self.calls.append(jump_to_position)
            if jump_to_position == 1:
                edges = [make_edge(p) for p in range(1, 251)]
            elif jump_to_position == 251:
                edges = [make_edge(251)]
            else:
                edges = []
            return {"data": {"list": {"titleListItemSearch": {"total": 251, "edges": edges}}}}

    fake = _FakeGraphqlClient()

    def _factory(*args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        return fake

    monkeypatch.setattr(mod, "HttpImdbListGraphqlClient", _factory)

    items = mod.fetch_imdb_list_items("ls9999999", max_pages=5, use_graphql=True)
    assert len(items) == 251
    assert fake.calls == [1, 251]
    assert {items[0].imdb_id, items[-1].imdb_id} == {"tt0000001", "tt0000251"}


def test_fetch_imdb_list_items_falls_back_to_html_when_graphql_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.ingestion import shows_from_lists as mod

    class _FakeGraphqlClient:
        def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            pass

        def fetch_title_list_main_page(self, *args, **kwargs) -> dict:  # noqa: ANN002, ANN003
            raise RuntimeError("boom")

    monkeypatch.setattr(mod, "HttpImdbListGraphqlClient", _FakeGraphqlClient)

    repo_root = Path(__file__).resolve().parents[3]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "list_jsonld_sample.html").read_text(encoding="utf-8")

    class _FakeResponse:
        def __init__(self, status_code: int, text: str) -> None:
            self.status_code = status_code
            self.text = text

    class _FakeSession:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def get(self, url: str, *args, **kwargs) -> _FakeResponse:  # noqa: ANN002, ANN003
            self.calls.append(url)
            if url == "https://www.imdb.com/list/ls123456789/":
                return _FakeResponse(200, html)
            return _FakeResponse(404, "")

    session = _FakeSession()
    items = mod.fetch_imdb_list_items("ls123456789", session=session, max_pages=1, use_graphql=True)
    assert {i.imdb_id for i in items} == {"tt1111111", "tt2222222", "tt3333333"}
    assert all(i.extra.get("source") == "jsonld" for i in items)
