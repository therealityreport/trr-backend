from copy import deepcopy

import pytest

from trr_backend.ingestion.canonical import _comment_media_urls
from trr_backend.ingestion.evidence import sanitize_payload
from trr_backend.socials.instagram.comments_scrapling.fetcher import _graphql_comment_media_urls
from trr_backend.socials.instagram.scraper import InstagramScraper

REAL = "https://scontent.cdninstagram.com/native.gif"
PLACEHOLDER = "https://static.cdninstagram.com/placeholder.gif"


def extractors():
    return [
        _comment_media_urls,
        _graphql_comment_media_urls,
        object.__new__(InstagramScraper)._extract_comment_media_urls,
    ]


@pytest.mark.parametrize("key", ["giphy_media_info", "giphyMediaInfo"])
@pytest.mark.parametrize("nested", [False, True])
def test_native_preferred_gif_excludes_fallback_and_retains_evidence(key, nested):
    info = {
        "id": "123",
        "first_party_cdn_proxied_images": {"fixed_height": {"url": REAL}},
        "images": {"fixed_height": {"url": PLACEHOLDER}},
    }
    source = {key: info, "text": ""}
    if nested:
        source = {"attachment": source}
    original = deepcopy(source)
    retained = sanitize_payload(source)
    for extract in extractors():
        assert extract(source) == [REAL]
        assert extract(retained) == [REAL]
    assert source == original and retained == original


@pytest.mark.parametrize("proxied,expected", [(None, [PLACEHOLDER]), ({}, [])])
def test_native_nullish_gif_preference(proxied, expected):
    source = {
        "giphy_media_info": {
            "first_party_cdn_proxied_images": proxied,
            "images": {"fixed_height": {"url": PLACEHOLDER}},
        }
    }
    for extract in extractors():
        assert extract(source) == expected


def test_native_missing_preferred_gif_uses_fallback():
    source = {"giphy_media_info": {"images": {"fixed_height": {"url": PLACEHOLDER}}}}
    for extract in extractors():
        assert extract(source) == [PLACEHOLDER]


@pytest.mark.parametrize(
    "proxied,expected",
    [
        ({"fixed_height": {"url": REAL}, "original": {"url": PLACEHOLDER}}, [REAL]),
        ({"url": REAL}, []),
    ],
)
def test_proxied_gif_uses_only_native_renderer_fixed_height(proxied, expected):
    source = {
        "giphy_media_info": {
            "first_party_cdn_proxied_images": proxied,
            "images": {"fixed_height": {"url": PLACEHOLDER}},
        }
    }
    for extract in extractors():
        assert extract(source) == expected
