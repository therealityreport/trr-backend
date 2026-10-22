"""Offline regression for native GIF evidence through ordinary media hosting."""

import io
from datetime import UTC, datetime

import pytest
from PIL import Image

from tests.ingestion.test_media import Response, Storage, noop, storage
from trr_backend.ingestion.canonical import _comment_media_urls
from trr_backend.ingestion.evidence import Evidence, sanitize_payload


@pytest.mark.parametrize("host", ["fbsbx.com", "evil.fbsbx.com", "cdn.fbsbx.com.evil.example", "example.com"])
def test_gif_delivery_exception_does_not_expand_to_other_hosts(host):
    assert sanitize_payload({"url": f"https://{host}/asset.gif?oh=synthetic&oe=ABC&sessionid=secret"}) == {
        "url": f"https://{host}/asset.gif"
    }


def test_signed_native_gif_survives_evidence_selection_and_idempotent_hosting():
    url = "https://cdn.fbsbx.com/asset.gif?oh=asset%2Bsignature&oe=ABC&_nc_cat=1"
    payload = {
        "pk": "123",
        "text": "",
        "giphy_media_info": {
            "id": "456",
            "first_party_cdn_proxied_images": {
                "fixed_height": {"url": url + "&sessionid=secret&access_token=secret#fragment"}
            },
        },
    }
    evidence = Evidence.capture(
        source_identity="instagram:comments:fixture:graphql_comments",
        captured_at=datetime.now(UTC),
        parser_version="fixture",
        payload=payload,
    )
    native = evidence.read(pinned=True)
    assert "secret" not in evidence.payload_json
    assert native["text"] == ""
    assert _comment_media_urls(native) == [url]
    output = io.BytesIO()
    frames = [Image.new("RGB", (8, 8), color) for color in ("red", "blue")]
    frames[0].save(output, format="GIF", save_all=True, append_images=frames[1:], duration=100, loop=0)
    data = output.getvalue()
    requested = []

    def opener(client, actual, **kwargs):
        assert actual == url
        requested.append(actual)
        return Response(data=data, headers={"Content-Type": "image/gif"})

    client = Storage()
    media = storage(client, opener=opener)
    first, _ = media.host(_comment_media_urls(native)[0], root="comments", name="fixture", check=noop, request=noop)
    writes = len(client.writes)
    second, _ = media.host(_comment_media_urls(native)[0], root="comments", name="fixture", check=noop, request=noop)
    assert first == second
    assert len(client.writes) == writes == 1
    assert requested == [url, url]
    saved = next(iter(client.objects.values()))[0]
    with Image.open(io.BytesIO(saved)) as image:
        assert image.n_frames == 2
