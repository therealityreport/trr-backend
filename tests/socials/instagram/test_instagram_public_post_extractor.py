from __future__ import annotations

import json
from typing import Any


def _html_with_app_json(payload: dict[str, Any]) -> str:
    return f"""
    <html>
      <head><title>Instagram</title></head>
      <body>
        <script type="application/json" data-sjs>{json.dumps(payload)}</script>
      </body>
    </html>
    """


def test_extracts_single_video_public_post_from_application_json() -> None:
    from trr_backend.socials.instagram.public_post_extractor import parse_public_post_from_html

    html = _html_with_app_json(
        {
            "__bbox": {
                "result": {
                    "data": {
                        "xdt_shortcode_media": {
                            "__typename": "XDTMediaDict",
                            "code": "PUBLIC01",
                            "pk": "media-1",
                            "media_type": 2,
                            "product_type": "clips",
                            "taken_at": 1_700_000_000,
                            "like_count": 1234,
                            "comment_count": 56,
                            "play_count": 7890,
                            "caption": {"text": "A public reel #Traitors @peacock"},
                            "user": {
                                "pk": "owner-1",
                                "username": "bravotv",
                                "full_name": "Bravo",
                                "is_verified": True,
                                "profile_pic_url": "https://cdn.example.com/bravo.jpg",
                            },
                            "usertags": {
                                "in": [
                                    {
                                        "position": [0.25, 0.75],
                                        "user": {"pk": "tag-1", "username": "host", "full_name": "Host"},
                                    }
                                ]
                            },
                            "coauthor_producers": [
                                {"pk": "co-1", "username": "peacock", "full_name": "Peacock"}
                            ],
                            "image_versions2": {
                                "candidates": [
                                    {"url": "https://cdn.example.com/cover-low.jpg", "width": 640, "height": 800},
                                    {"url": "https://cdn.example.com/cover-hd.jpg", "width": 1080, "height": 1350},
                                ]
                            },
                            "video_versions": [
                                {"url": "https://cdn.example.com/video-sd.mp4", "width": 480, "height": 854},
                                {"url": "https://cdn.example.com/video-hd.mp4", "width": 720, "height": 1280},
                            ],
                        }
                    }
                }
            }
        }
    )

    post = parse_public_post_from_html(html, shortcode="PUBLIC01")

    assert post is not None
    assert post.owner == {
        "username": "bravotv",
        "user_id": "owner-1",
        "full_name": "Bravo",
        "is_verified": True,
        "profile_pic_url": "https://cdn.example.com/bravo.jpg",
        "profile_pic_url_hd": None,
    }
    assert post.caption == "A public reel #Traitors @peacock"
    assert post.taken_at == 1_700_000_000
    assert post.like_count == 1234
    assert post.comment_count == 56
    assert post.view_count == 7890
    assert post.media_type == "video"
    assert post.product_type == "clips"
    assert post.profile_tags == ["host"]
    assert post.tagged_users_detail[0]["tag_x"] == 0.25
    assert post.coauthors == ["peacock"]
    assert post.hashtags == ["Traitors"]
    assert post.mentions == ["@peacock"]
    assert [candidate.url for candidate in post.image_candidates] == [
        "https://cdn.example.com/cover-low.jpg",
        "https://cdn.example.com/cover-hd.jpg",
    ]
    assert [candidate.url for candidate in post.video_candidates] == [
        "https://cdn.example.com/video-sd.mp4",
        "https://cdn.example.com/video-hd.mp4",
    ]
    assert post.media_urls == ["https://cdn.example.com/video-hd.mp4"]
    assert post.thumbnail_url == "https://cdn.example.com/cover-hd.jpg"


def test_extracts_carousel_children_and_selected_media_urls() -> None:
    from trr_backend.socials.instagram.public_post_extractor import parse_public_post_from_html

    html = _html_with_app_json(
        {
            "require": [
                [
                    "RelayPrefetchedStreamCache",
                    {
                        "data": {
                            "shortcode_media": {
                                "__typename": "GraphSidecar",
                                "shortcode": "CAROU01",
                                "id": "media-2",
                                "taken_at_timestamp": 1_701_111_111,
                                "owner": {"id": "owner-2", "username": "traitorsus"},
                                "edge_media_preview_like": {"count": 88},
                                "edge_media_to_comment": {"count": 9},
                                "edge_media_to_caption": {
                                    "edges": [{"node": {"text": "Carousel night #Finale"}}]
                                },
                                "edge_media_to_tagged_user": {
                                    "edges": [
                                        {
                                            "node": {
                                                "x": 0.4,
                                                "y": 0.6,
                                                "user": {"id": "tag-2", "username": "contestant"},
                                            }
                                        }
                                    ]
                                },
                                "coauthorProducers": [{"id": "co-2", "username": "nbc"}],
                                "edge_sidecar_to_children": {
                                    "edges": [
                                        {
                                            "node": {
                                                "__typename": "GraphImage",
                                                "display_resources": [
                                                    {
                                                        "src": "https://cdn.example.com/slide1-small.jpg",
                                                        "config_width": 320,
                                                        "config_height": 400,
                                                    },
                                                    {
                                                        "src": "https://cdn.example.com/slide1-large.jpg",
                                                        "config_width": 1080,
                                                        "config_height": 1350,
                                                    },
                                                ],
                                            }
                                        },
                                        {
                                            "node": {
                                                "__typename": "GraphVideo",
                                                "display_url": "https://cdn.example.com/slide2-cover.jpg",
                                                "video_url": "https://cdn.example.com/slide2-direct.mp4",
                                                "video_versions": [
                                                    {
                                                        "url": "https://cdn.example.com/slide2-hd.mp4",
                                                        "width": 720,
                                                        "height": 1280,
                                                    }
                                                ],
                                            }
                                        },
                                    ]
                                },
                            }
                        }
                    },
                ]
            ]
        }
    )

    post = parse_public_post_from_html(html, shortcode="CAROU01")

    assert post is not None
    assert post.media_type == "carousel"
    assert post.like_count == 88
    assert post.comment_count == 9
    assert post.profile_tags == ["contestant"]
    assert post.coauthors == ["nbc"]
    assert post.media_urls == [
        "https://cdn.example.com/slide1-large.jpg",
        "https://cdn.example.com/slide2-hd.mp4",
    ]
    assert post.thumbnail_url == "https://cdn.example.com/slide1-large.jpg"
    assert [candidate.slide_index for candidate in post.image_candidates] == [0, 0, 1]
    assert [candidate.url for candidate in post.video_candidates] == [
        "https://cdn.example.com/slide2-hd.mp4",
        "https://cdn.example.com/slide2-direct.mp4",
    ]
    assert post.children[0]["media_type"] == "image"
    assert post.children[1]["media_type"] == "video"


def test_parse_returns_none_when_shortcode_is_absent() -> None:
    from trr_backend.socials.instagram.public_post_extractor import parse_public_post_from_html

    html = _html_with_app_json({"data": {"xdt_shortcode_media": {"code": "OTHER01", "media_type": 1}}})

    assert parse_public_post_from_html(html, shortcode="PUBLIC01") is None


def test_resolve_instagram_media_prefers_public_app_json(monkeypatch) -> None:
    import trr_backend.socials.instagram.permalink_metadata as permalink_metadata

    html = _html_with_app_json(
        {
            "data": {
                "xdt_shortcode_media": {
                    "code": "PUBLIC01",
                    "media_type": 1,
                    "taken_at": 1_700_000_000,
                    "like_count": 7,
                    "comment_count": 3,
                    "caption": {"text": "Still public"},
                    "user": {"username": "bravotv"},
                    "image_versions2": {
                        "candidates": [{"url": "https://cdn.example.com/public.jpg", "width": 1080, "height": 1350}]
                    },
                }
            }
        }
    )

    monkeypatch.setattr(
        permalink_metadata,
        "fetch_public_post_html",
        lambda *_args, **_kwargs: (html, 200),
    )

    resolution = permalink_metadata.resolve_instagram_media("https://www.instagram.com/p/PUBLIC01/")

    assert resolution.source == "public_app_json"
    assert resolution.media_urls == ["https://cdn.example.com/public.jpg"]
    assert resolution.thumbnail_url == "https://cdn.example.com/public.jpg"
    assert resolution.metadata is not None
    assert resolution.metadata.raw_media["like_count"] == 7
    assert resolution.attempts == [
        {
            "source": "public_app_json",
            "success": True,
            "reason_code": None,
            "http_status": 200,
            "selected_url_count": 1,
        }
    ]
