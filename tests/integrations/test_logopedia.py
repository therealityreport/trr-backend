from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from trr_backend.integrations.logopedia import LogopediaRequestError, fetch_logopedia_logo_candidates


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict:
        return self._payload


def test_fetch_logopedia_logo_candidates_retries_retryable_status_and_applies_timeout_tuple() -> None:
    session = MagicMock()
    session.get.side_effect = [
        _FakeResponse(200, {"query": {"search": [{"title": "Bravo"}]}}),
        _FakeResponse(502, {}),
        _FakeResponse(
            200,
            {
                "query": {
                    "pages": {
                        "1": {
                            "imageinfo": [
                                {
                                    "url": "https://static.wikia.nocookie.net/logopedia/images/a/ab/Bravo_logo.png",
                                    "size": 1024,
                                }
                            ]
                        }
                    }
                }
            },
        ),
    ]

    with patch("trr_backend.integrations.logopedia._candidate_titles", return_value=["Bravo"]):
        out = fetch_logopedia_logo_candidates("Bravo", session=session, timeout_seconds=12.0)

    assert out == ["https://static.wikia.nocookie.net/logopedia/images/a/ab/Bravo_logo.png"]
    assert session.get.call_count == 3
    assert session.get.call_args.kwargs["timeout"] == (5.0, 12.0)


def test_fetch_logopedia_logo_candidates_does_not_retry_non_retryable_http_error() -> None:
    session = MagicMock()
    session.get.side_effect = [
        _FakeResponse(200, {"query": {"search": [{"title": "Bravo"}]}}),
        _FakeResponse(404, {}),
    ]

    with patch("trr_backend.integrations.logopedia._candidate_titles", return_value=["Bravo"]):
        with pytest.raises(LogopediaRequestError, match="logopedia_http_404"):
            fetch_logopedia_logo_candidates("Bravo", session=session)

    assert session.get.call_count == 2


def test_fetch_logopedia_logo_candidates_retries_timeout_then_raises_timeout_reason() -> None:
    session = MagicMock()
    session.get.side_effect = requests.Timeout("read timeout")

    with pytest.raises(LogopediaRequestError, match="logopedia_timeout"):
        fetch_logopedia_logo_candidates("Bravo", session=session)

    assert session.get.call_count == 2
