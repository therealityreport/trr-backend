from copy import deepcopy

import pytest

from trr_backend.ingestion.canonical import _comment_media_urls
from trr_backend.ingestion.evidence import sanitize_payload
from trr_backend.socials.instagram.comments_scrapling.fetcher import _graphql_comment_media_urls
from trr_backend.socials.instagram.scraper import InstagramScraper

REAL = "https://scontent.cdninstagram.com/native.gif"
PLACEHOLDER = "https://static.cdninstagram.com/placeholder.gif"
UNAVAILABLE = "https://static.cdninstagram.com/rsrc.php/v4/yr/r/xg_5YoVlvjp.gif"


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


@pytest.mark.parametrize("key", ["giphy_media_info", "giphyMediaInfo"])
@pytest.mark.parametrize("explicit", ["media_urls", "mediaUrls", "media_url", "mediaUrl"])
def test_native_gif_supersedes_same_comment_stale_explicit_placeholder_without_mutation(key, explicit):
    source = {
        "pk": "18029668664695097",
        "text": "",
        explicit: [UNAVAILABLE] if explicit in {"media_urls", "mediaUrls"} else UNAVAILABLE,
        key: {
            "id": "1072011872270984",
            "first_party_cdn_proxied_images": {"fixed_height": {"url": REAL}},
            "images": {"fixed_height": {"url": UNAVAILABLE}},
        },
    }
    original = deepcopy(source)
    retained = sanitize_payload(source)
    retained_original = deepcopy(retained)
    for extract in extractors():
        assert extract(source) == [REAL]
        assert extract(retained) == [REAL]
    assert source == original and retained == retained_original


def test_native_gif_keeps_unrelated_explicit_media_including_other_rsrc_assets():
    unrelated = "https://static.cdninstagram.com/rsrc.php/v4/other/real.gif"
    source = {
        "media_urls": [UNAVAILABLE, unrelated, PLACEHOLDER],
        "giphy_media_info": {"first_party_cdn_proxied_images": {"fixed_height": {"url": REAL}}},
    }
    original = deepcopy(source)
    for extract in extractors():
        assert extract(source) == [unrelated, PLACEHOLDER, REAL]
    assert source == original


@pytest.mark.parametrize(
    "info",
    [
        None,
        {"first_party_cdn_proxied_images": {}, "images": {"fixed_height": {"url": REAL}}},
        {"first_party_cdn_proxied_images": {"fixed_height": {"url": UNAVAILABLE}}},
        {"first_party_cdn_proxied_images": {"fixed_height": {"url": ""}}},
        {"first_party_cdn_proxied_images": {"fixed_height": {"url": "https://"}}},
        {"first_party_cdn_proxied_images": {"fixed_height": {"url": " " + REAL}}},
        {"first_party_cdn_proxied_images": {"fixed_height": {"url": "javascript:placeholder"}}},
        {"images": {"fixed_height": {"url": UNAVAILABLE}}},
    ],
)
def test_placeholder_is_retained_without_selected_real_native_gif(info):
    source = {"media_urls": [UNAVAILABLE], "giphy_media_info": info}
    original = deepcopy(source)
    for extract in extractors():
        assert UNAVAILABLE in extract(source)
    assert source == original


def test_child_gif_does_not_prune_parent_explicit_placeholder():
    source = {
        "media_urls": [UNAVAILABLE],
        "attachment": {"giphy_media_info": {"first_party_cdn_proxied_images": {"fixed_height": {"url": REAL}}}},
    }
    original = deepcopy(source)
    for extract in extractors():
        assert extract(source) == [UNAVAILABLE, REAL]
    assert source == original


def test_parent_gif_does_not_prune_child_explicit_placeholder():
    source = {
        "giphy_media_info": {"first_party_cdn_proxied_images": {"fixed_height": {"url": REAL}}},
        "attachment": {"media_url": UNAVAILABLE},
    }
    original = deepcopy(source)
    for extract in extractors():
        assert set(extract(source)) == {UNAVAILABLE, REAL}
    assert source == original


def test_native_null_preferred_branch_can_supersede_stale_explicit_placeholder_with_fallback():
    source = {
        "media_urls": [UNAVAILABLE],
        "giphy_media_info": {
            "first_party_cdn_proxied_images": None,
            "images": {"fixed_height": {"url": REAL}},
        },
    }
    original = deepcopy(source)
    for extract in extractors():
        assert extract(source) == [REAL]
    assert source == original
