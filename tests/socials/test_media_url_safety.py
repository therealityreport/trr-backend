from __future__ import annotations

import ipaddress
import socket
from typing import Any

import pytest
import requests

from trr_backend.socials import media_url_safety
from trr_backend.socials.media_url_safety import (
    MediaUrlSafetyPolicy,
    UnsafeMediaUrlError,
    _is_blocked_ip,
    allowed_hosts_for_platform,
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


def test_validate_media_url_rejects_localhost(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 6379))],
    )

    with pytest.raises(UnsafeMediaUrlError):
        validate_media_url(
            "http://localhost:6379/x",
            policy=MediaUrlSafetyPolicy(("cdninstagram.com",)),
        )


def test_validate_media_url_rejects_localhost_suffix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 6379))],
    )

    with pytest.raises(UnsafeMediaUrlError):
        validate_media_url(
            "http://something.localhost/x",
            policy=MediaUrlSafetyPolicy(("cdninstagram.com",)),
        )


def test_validate_media_url_allows_reserved_test_host_with_test_host_policy() -> None:
    assert (
        validate_media_url(
            "http://example.com/x",
            policy=MediaUrlSafetyPolicy(("cdninstagram.com",), allow_test_hosts=True),
        )
        == "http://example.com/x"
    )


def test_validate_media_url_empty_allowlist_fails_closed() -> None:
    with pytest.raises(UnsafeMediaUrlError, match="media_url_host_not_allowed"):
        validate_media_url(
            "http://evil.example-not-reserved.com/x",
            policy=MediaUrlSafetyPolicy(()),
        )


def test_allowed_hosts_for_platform_includes_reddit() -> None:
    allowed_hosts = allowed_hosts_for_platform("reddit")

    assert allowed_hosts
    assert "redd.it" in allowed_hosts


def test_is_blocked_ip_rejects_ipv4_mapped_ipv6() -> None:
    assert _is_blocked_ip(ipaddress.ip_address("::ffff:169.254.169.254")) is True


@pytest.mark.parametrize("max_bytes", [1024, 75 * 1024 * 1024])
@pytest.mark.parametrize("headers_only", [False, True])
def test_safe_requests_get_revalidates_redirect_targets(
    monkeypatch: pytest.MonkeyPatch, max_bytes: int, headers_only: bool
) -> None:
    class _FakeResponse:
        status_code = 302
        headers = {"location": "http://169.254.169.254/latest/meta-data"}
        url = "https://cdninstagram.com/image.jpg"

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))],
    )
    contacted: list[tuple[str, int, bool]] = []

    def send(_prepared: Any, *, address: str, **_kwargs: Any) -> Any:
        contacted.append((address, _kwargs["max_bytes"], _kwargs["headers_only"]))
        return _FakeResponse()

    monkeypatch.setattr(media_url_safety, "_send_pinned_request", send)

    with pytest.raises(UnsafeMediaUrlError, match="media_url_blocked_ip"):
        safe_requests_get(
            requests,
            "https://cdninstagram.com/image.jpg",
            policy=MediaUrlSafetyPolicy(("cdninstagram.com",), resolve_dns=False),
            max_bytes=max_bytes,
            headers_only=headers_only,
        )
    assert contacted == [("8.8.8.8", max_bytes, headers_only)]
