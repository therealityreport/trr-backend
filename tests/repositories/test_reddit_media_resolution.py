from __future__ import annotations

from typing import Any

import pytest

from trr_backend.repositories import reddit_execution as execution


class Response:
    def __init__(self, status: int = 200, *, headers: dict | None = None, body: bytes = b""):
        self.status_code = status
        self.headers = headers or {}
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def raise_for_status(self):
        assert self.status_code < 400

    def iter_content(self, _size):
        yield self.body


def http(monkeypatch: pytest.MonkeyPatch, *responses: Response) -> list[tuple[str, dict]]:
    remaining = list(responses)
    calls: list[tuple[str, dict]] = []

    def get(url: str, **options: Any):
        calls.append((url, options))
        assert remaining, "Unexpected share-page request"
        return remaining.pop(0)

    monkeypatch.setattr(execution.requests, "get", get)
    return calls


@pytest.mark.parametrize("url", ["https://i.redd.it/post.gif", "https://media1.tenor.com/m/asset.gif"])
def test_direct_media_does_not_fetch_share_metadata(monkeypatch, url):
    calls = http(monkeypatch)
    assert execution.media_download_url(url) == url
    assert calls == []


def test_tenor_share_redirect_resolves_its_gif_metadata(monkeypatch):
    calls = http(
        monkeypatch,
        Response(302, headers={"Location": "/view/real-gif-123"}),
        Response(body=b'<meta content="https://media1.tenor.com/m/real.gif" property="og:image">'),
    )
    assert execution.media_download_url("https://tenor.com/share.gif") == "https://media1.tenor.com/m/real.gif"
    assert [c[0] for c in calls] == ["https://tenor.com/share.gif", "https://tenor.com/view/real-gif-123"]
    assert all(c[1] == {"timeout": 20, "allow_redirects": False, "stream": True} for c in calls)


def test_tenor_cdn_redirect_is_passed_to_the_media_validator(monkeypatch):
    calls = http(monkeypatch, Response(302, headers={"Location": "https://media.tenor.com/real.gif"}))
    assert execution.media_download_url("https://tenor.com/share.gif") == "https://media.tenor.com/real.gif"
    assert len(calls) == 1


@pytest.mark.parametrize("destination", ["http://127.0.0.1/private", "https://media.tenor.com.evil.example/a.gif"])
def test_tenor_does_not_follow_an_untrusted_redirect(monkeypatch, destination):
    calls = http(monkeypatch, Response(302, headers={"Location": destination}))
    with pytest.raises(RuntimeError, match="outside its media provider"):
        execution.media_download_url("https://tenor.com/share.gif")
    assert len(calls) == 1


@pytest.mark.parametrize(
    "body",
    [b"<html>no media</html>", b'<meta property="og:image" content="https://other.example/not-tenor.gif">'],
)
def test_missing_or_untrusted_metadata_stays_unresolved(monkeypatch, body):
    http(monkeypatch, Response(body=body))
    with pytest.raises(RuntimeError, match="no verified media URL"):
        execution.media_download_url("https://tenor.com/share.gif")


def test_share_page_size_and_redirect_limits_are_bounded(monkeypatch):
    http(monkeypatch, Response(body=b"x" * (2 * 1024 * 1024 + 1)))
    with pytest.raises(RuntimeError, match="size limit"):
        execution.media_download_url("https://tenor.com/share.gif")
    calls = http(monkeypatch, *(Response(302, headers={"Location": "/loop"}) for _ in range(4)))
    with pytest.raises(RuntimeError, match="redirect limit"):
        execution.media_download_url("https://tenor.com/share.gif")
    assert len(calls) == 4


def test_direct_tenor_media_response_is_preserved(monkeypatch):
    http(monkeypatch, Response(headers={"Content-Type": "image/gif"}))
    assert execution.media_download_url("https://tenor.com/actual.gif") == "https://tenor.com/actual.gif"


def test_non_https_tenor_share_is_rejected_before_request(monkeypatch):
    calls = http(monkeypatch)
    with pytest.raises(RuntimeError, match="must use HTTPS"):
        execution.media_download_url("http://tenor.com/share.gif")
    assert calls == []
