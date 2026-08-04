from __future__ import annotations

from typing import Any

import requests

from trr_backend.integrations.imdb import name_filmography
from trr_backend.integrations.imdb.name_filmography import parse_name_filmography_html


def test_parse_name_filmography_keeps_credit_title_links_deduped_in_source_order() -> None:
    html = """
    <a href="/title/tt1000001/?ref_=nm_flmg_job_1_cdt_t_1">Show &amp; One</a>
    <a href="/title/tt1000001/?ref_=nm_flmg_job_1_cdt_t_2">Duplicate</a>
    <a href="/title/tt1000002/?ref_=nm_flmg_job_2_cdt_t_3"><span>Show Two</span></a>
    <a href="/title/tt9999999/?ref_=nm_known_for_1">Not filmography</a>
    """

    assert parse_name_filmography_html(html) == [
        {
            "imdb_title_id": "tt1000001",
            "show_name": "Show & One",
            "external_url": "https://www.imdb.com/title/tt1000001/",
        },
        {
            "imdb_title_id": "tt1000002",
            "show_name": "Show Two",
            "external_url": "https://www.imdb.com/title/tt1000002/",
        },
    ]


def test_fetch_name_filmography_is_bounded_and_soft_failing(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class FakeResponse:
        status_code = 200

        def iter_content(self, chunk_size: int):
            assert chunk_size == 64 * 1024
            yield b'<a href="/title/tt1000001/?ref_=nm_flmg_job_1_cdt_t_1">Show One</a>'

        def close(self) -> None:
            return None

    def fake_get(url: str, **kwargs: Any) -> FakeResponse:
        calls.append((url, kwargs))
        return FakeResponse()

    monkeypatch.setattr(name_filmography.requests, "get", fake_get)

    assert name_filmography.fetch_name_filmography("nm1000001")[0]["imdb_title_id"] == "tt1000001"
    assert calls[0][0] == "https://m.imdb.com/name/nm1000001/fullcredits"
    assert calls[0][1]["timeout"] == (1.0, 2.0)
    assert calls[0][1]["stream"] is True

    monkeypatch.setattr(
        name_filmography.requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(requests.Timeout("slow IMDb")),
    )
    assert name_filmography.fetch_name_filmography("nm1000001") == []
