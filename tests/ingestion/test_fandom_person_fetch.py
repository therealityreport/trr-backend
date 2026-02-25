from __future__ import annotations

import requests as real_requests

from trr_backend.ingestion.fandom_person_scraper import fetch_fandom_person_html


class _FakeRequestsResponse:
    def __init__(self, *, status_code: int, text: str, url: str):
        self.status_code = status_code
        self.text = text
        self.url = url

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise real_requests.HTTPError(f"{self.status_code} error")


class _FakeSession:
    def __init__(self, response: _FakeRequestsResponse):
        self._response = response

    def get(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return self._response


class _ApiUrlOpenResponse:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN201
        return False

    def read(self) -> bytes:
        payload = '{"parse":{"text":{"*":"<html><h1>Lisa Barlow</h1></html>"},"revid":123}}'
        return payload.encode("utf-8")


def test_fetch_fandom_person_html_falls_back_to_api_on_403(monkeypatch) -> None:
    response = _FakeRequestsResponse(
        status_code=403,
        text="Forbidden",
        url="https://real-housewives.fandom.com/wiki/Lisa_Barlow",
    )
    session = _FakeSession(response)

    def _fake_urlopen(request, timeout=30):  # noqa: ANN001, ANN202
        full_url = getattr(request, "full_url", "")
        assert "api.php?action=parse" in full_url
        return _ApiUrlOpenResponse()

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)

    html, final_url = fetch_fandom_person_html(
        "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
        session=session,
    )
    assert "Lisa Barlow" in html
    assert "fandom_revid:123" in html
    assert final_url == "https://real-housewives.fandom.com/wiki/Lisa_Barlow"
