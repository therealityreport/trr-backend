from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

from trr_backend.integrations.brandfetch import (
    BrandfetchNotFoundError,
    BrandfetchRequestError,
    fetch_brandfetch_logo_candidates,
)


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict:
        return self._payload


def test_fetch_brandfetch_logo_candidates_retries_retryable_status_and_applies_timeout_tuple() -> None:
    session = MagicMock()
    session.get.side_effect = [
        _FakeResponse(429, {}),
        _FakeResponse(
            200,
            {
                "logos": [
                    {
                        "formats": [
                            {"format": "svg", "src": "https://cdn.example.com/logo.svg", "width": 500},
                            {"format": "png", "src": "https://cdn.example.com/logo.png", "width": 400},
                        ]
                    }
                ]
            },
        ),
    ]

    out = fetch_brandfetch_logo_candidates(
        "bravotv.com",
        api_key="test-key",
        timeout_seconds=12.0,
        session=session,
    )

    assert out == ["https://cdn.example.com/logo.png", "https://cdn.example.com/logo.svg"]
    assert session.get.call_count == 2
    assert session.get.call_args.kwargs["timeout"] == (5.0, 12.0)


def test_fetch_brandfetch_logo_candidates_raises_not_found_without_retry() -> None:
    session = MagicMock()
    session.get.return_value = _FakeResponse(404, {})

    with pytest.raises(BrandfetchNotFoundError, match="brandfetch_not_found"):
        fetch_brandfetch_logo_candidates("missing.example.com", api_key="test-key", session=session)

    assert session.get.call_count == 1


def test_fetch_brandfetch_logo_candidates_retries_timeout_then_raises_timeout_reason() -> None:
    session = MagicMock()
    session.get.side_effect = requests.Timeout("read timeout")

    with pytest.raises(BrandfetchRequestError, match="brandfetch_timeout"):
        fetch_brandfetch_logo_candidates("bravotv.com", api_key="test-key", session=session)

    assert session.get.call_count == 2
