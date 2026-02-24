from __future__ import annotations

import json
from urllib.parse import quote

import pytest

from trr_backend.socials.instagram.permalink_metadata import (
    fetch_permalink_media_item,
    parse_permalink_metadata,
)


def test_fetch_permalink_media_item_parses_data_sjs_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    media_item = {"id": "media-1", "taken_at": 1739481600, "product_type": "clips"}
    payload = {
        "__bbox": {
            "result": {
                "data": {
                    "xdt_api__v1__media__shortcode__web_info": {
                        "items": [media_item],
                    }
                }
            }
        }
    }
    html = f'<html><body><script type="application/json" data-sjs>{json.dumps(payload)}</script></body></html>'

    class _FakeResponse:
        text = html

        def raise_for_status(self) -> None:
            return None

    class _FakeSession:
        def get(self, *_args, **_kwargs):
            return _FakeResponse()

    found = fetch_permalink_media_item("DUHvBbEDhfw", session=_FakeSession())  # type: ignore[arg-type]
    assert found is not None
    assert found["id"] == "media-1"
    assert found["product_type"] == "clips"


def test_fetch_permalink_media_item_supports_wrapped_data_sjs_payload() -> None:
    media_item = {"id": "media-wrapped", "taken_at": 1739481600}
    payload = {
        "__bbox": {
            "result": {
                "data": {
                    "xdt_api__v1__media__shortcode__web_info": {
                        "items": [media_item],
                    }
                }
            }
        }
    }
    wrapped = f"window.__additionalDataLoaded('/p/DUHvBbEDhfw/', {json.dumps(payload)});"
    html = f'<html><body><script type="application/json" data-sjs>{wrapped}</script></body></html>'

    class _FakeResponse:
        text = html

        def raise_for_status(self) -> None:
            return None

    class _FakeSession:
        def get(self, *_args, **_kwargs):
            return _FakeResponse()

    found = fetch_permalink_media_item("DUHvBbEDhfw", session=_FakeSession())  # type: ignore[arg-type]
    assert found is not None
    assert found["id"] == "media-wrapped"


def test_fetch_permalink_media_item_uses_route_order_with_fallback() -> None:
    media_item = {"id": "media-route", "taken_at": 1739481600}
    payload = {
        "__bbox": {
            "result": {
                "data": {
                    "xdt_api__v1__media__shortcode__web_info": {
                        "items": [media_item],
                    }
                }
            }
        }
    }
    html = f'<html><body><script type="application/json" data-sjs>{json.dumps(payload)}</script></body></html>'
    requested_urls: list[str] = []

    class _FakeResponse:
        def __init__(self, *, status_code: int, text: str = ""):
            self.status_code = status_code
            self.text = text

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                import requests

                raise requests.HTTPError(f"{self.status_code} error")

    class _FakeSession:
        def get(self, url: str, *_args, **_kwargs):
            requested_urls.append(url)
            if "/reel/" in url:
                return _FakeResponse(status_code=404)
            if "/p/" in url:
                return _FakeResponse(status_code=200, text=html)
            return _FakeResponse(status_code=404)

    found = fetch_permalink_media_item(
        "https://www.instagram.com/reel/DUHvBbEDhfw/",
        session=_FakeSession(),  # type: ignore[arg-type]
    )
    assert found is not None
    assert found["id"] == "media-route"
    assert requested_urls[0].endswith("/reel/DUHvBbEDhfw/")
    assert requested_urls[1].endswith("/p/DUHvBbEDhfw/")


def test_parse_permalink_metadata_extracts_fields_from_media_item() -> None:
    efg = quote(json.dumps({"video_info": {"duration_s": 17.4}}))
    metadata = parse_permalink_metadata(
        {
            "taken_at": 1739481600,
            "product_type": "clips",
            "media_type": 2,
            "caption": {"text": "Tonight on #RHOSLC with @bravotv and @bravoandy"},
            "usertags": {"in": [{"user": {"username": "housewife_1"}}]},
            "coauthor_producers": [{"username": "collab_a"}],
            "invited_coauthor_producers": [{"user": {"username": "collab_b"}}],
            "video_versions": [{"url": f"https://cdn.test/video.mp4?efg={efg}"}],
            "image_versions2": {"candidates": [{"url": "https://cdn.test/thumb.jpg"}]},
        }
    )

    assert metadata.post_format == "reel"
    assert metadata.profile_tags == ["housewife_1"]
    assert metadata.collaborators == ["collab_a", "collab_b"]
    assert metadata.hashtags == ["RHOSLC"]
    assert metadata.mentions == ["@bravotv", "@bravoandy"]
    assert metadata.duration_seconds == 17
    assert metadata.thumbnail_url == "https://cdn.test/thumb.jpg"
    assert metadata.media_urls


def test_parse_permalink_metadata_duration_falls_back_to_dash_manifest() -> None:
    metadata = parse_permalink_metadata(
        {
            "taken_at": 1739481600,
            "media_type": 2,
            "caption": {"text": "No encoded duration here"},
            "video_versions": [{"url": "https://cdn.test/video.mp4"}],
            "video_dash_manifest": '<MPD mediaPresentationDuration="PT1M5.5S"></MPD>',
        }
    )
    assert metadata.duration_seconds == 66
