from __future__ import annotations

import socket
from types import SimpleNamespace

import pytest

from trr_backend.socials.media_url_safety import (
    MediaUrlSafetyPolicy,
    UnsafeMediaUrlError,
    safe_requests_get,
    validate_media_url,
)


def test_validate_media_url_rejects_private_literal_ip() -> None:
    with pytest.raises(UnsafeMediaUrlError, match="media_url_blocked_ip"):
        validate_media_url(
            "http://127.0.0.1/private.jpg",
            policy=MediaUrlSafetyPolicy(("example.com",)),
        )


def test_validate_media_url_rejects_allowed_host_resolving_to_private_ip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.5", 443))],
    )

    with pytest.raises(UnsafeMediaUrlError, match="media_url_resolves_to_blocked_ip"):
        validate_media_url(
            "https://cdninstagram.com/image.jpg",
            policy=MediaUrlSafetyPolicy(("cdninstagram.com",)),
        )


def test_safe_requests_get_revalidates_redirect_targets() -> None:
    class _FakeResponse:
        status_code = 302
        headers = {"location": "http://169.254.169.254/latest/meta-data"}
        url = "https://cdninstagram.com/image.jpg"

        def close(self) -> None:
            return None

    class _FakeClient:
        def get(self, *_args, **_kwargs) -> SimpleNamespace:
            return _FakeResponse()

    with pytest.raises(UnsafeMediaUrlError, match="media_url_blocked_ip"):
        safe_requests_get(
            _FakeClient(),
            "https://cdninstagram.com/image.jpg",
            policy=MediaUrlSafetyPolicy(("cdninstagram.com",), resolve_dns=False),
        )
