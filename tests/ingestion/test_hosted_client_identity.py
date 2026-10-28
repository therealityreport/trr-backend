"""Public storage client identity and access-denial diagnostics, without network I/O."""

from threading import Event
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from scripts.ingestion.january_audit import hosted
from scripts.ingestion.january_audit.offline_hosted import BASE, Client, Response, png


def object_plan():
    return {
        "storage_key": "social/a.png",
        "url": BASE + "/social/a.png",
        "roles": [{"shortcode": "ABC", "role": "cover_original"}],
        "expected_bytes": None,
        "historical_sha256": None,
        "required_decode_mode": "image_full_decode",
        "format": "png",
        "width": 2,
        "height": 2,
        "content_type": "image/png",
    }


@pytest.mark.parametrize(
    "status,body,expected,provider",
    [
        (200, None, None, None),
        (403, b"error code: 1010\n", "cdn_client_signature_denied", "1010"),
        (403, b"forbidden", "cdn_access_denied", None),
    ],
)
def test_public_storage_uses_descriptive_identity_and_retains_safe_denial(status, body, expected, provider):
    item = object_plan()
    response = Response(png() if body is None else body, status=status)
    response.headers["CF-Ray"] = "a4ff61afbbb2d327-MIA"
    stop = Event()

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            assert self.trust_env is False  # pyright: ignore[reportAttributeAccessIssue]
            assert url == item["url"]
            assert kwargs["headers"] == {
                "Accept-Encoding": "identity",
                "User-Agent": hosted.PUBLIC_STORAGE_USER_AGENT,
            }
            assert kwargs["stream"] and not kwargs["allow_redirects"]
            return response

    with patch("requests.Session", Session):
        result = hosted.inspect_object(item, Client(png()), SimpleNamespace(bucket="trr-media-prod"), stop=stop)

    assert not stop.is_set()  # A 403 is not rate-limit or source-unavailable proof.
    if expected is None:
        assert result["status"] == "verified"
        assert result["storage_cdn_full_byte_equality"] is True
    else:
        assert result["status"] == "failed" and result["error_code"] == expected
        assert result["cdn_http_status"] == 403
        assert result["cdn_provider_code"] == provider
        assert result["cdn_request_id"] == "a4ff61afbbb2d327-MIA"
        assert result["retry_after_seconds"] is None
        assert "body" not in result


def test_denial_diagnostics_bound_the_body_and_omit_untrusted_request_ids():
    item = object_plan()
    response = Response(b"x" * 512 + b"error code: 1010\n", status=403)
    response.headers["CF-Ray"] = "untrusted\nvalue"
    reads = []
    read = response.raw.read

    def bounded(size):
        reads.append(size)
        return read(size)

    response.raw.read = bounded

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, *args, **kwargs):
            return response

    with patch("requests.Session", Session):
        result = hosted.inspect_object(item, Client(png()), SimpleNamespace(bucket="trr-media-prod"))
    assert reads == [512]
    assert result["error_code"] == "cdn_access_denied"
    assert result["cdn_provider_code"] is None and result["cdn_request_id"] is None
