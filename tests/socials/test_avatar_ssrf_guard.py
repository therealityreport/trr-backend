from __future__ import annotations

import socket

import pytest

from trr_backend.socials.media_url_safety import UnsafeMediaUrlError
from trr_backend.socials.social_season_analytics_impl import _download_avatar_to_tempfile


def test_download_avatar_rejects_cloud_metadata_literal_ip() -> None:
    with pytest.raises(UnsafeMediaUrlError, match="media_url_blocked_ip"):
        _download_avatar_to_tempfile(
            "http://169.254.169.254/latest/meta-data",
            platform="instagram",
            headers={},
        )


def test_download_avatar_rejects_loopback_literal_ip() -> None:
    with pytest.raises(UnsafeMediaUrlError, match="media_url_blocked_ip"):
        _download_avatar_to_tempfile(
            "http://127.0.0.1/a.jpg",
            platform="instagram",
            headers={},
        )


def test_download_avatar_rejects_disallowed_public_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 80))],
    )

    with pytest.raises(UnsafeMediaUrlError, match="media_url_host_not_allowed"):
        _download_avatar_to_tempfile(
            "http://evil-cdn.org/a.jpg",
            platform="instagram",
            headers={},
        )
