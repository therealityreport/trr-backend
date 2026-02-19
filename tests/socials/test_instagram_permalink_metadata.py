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
