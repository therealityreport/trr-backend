from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import quote

import pytest

from trr_backend.socials.instagram.permalink_metadata import (
    _graphql_extract_collaborators,
    _graphql_extract_collaborators_detail,
    _metadata_from_graphql_node,
    _shortcode_to_media_id,
    fetch_instagram_facebook_crosspost_metadata,
    fetch_permalink_media_item,
    parse_permalink_metadata,
    resolve_instagram_media,
)

_FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "instagram" / "scrapling"


def _fixture_json(name: str) -> dict[str, object]:
    return json.loads((_FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _data_sjs_html(payload: dict[str, object]) -> str:
    return f'<html><body><script type="application/json" data-sjs>{json.dumps(payload)}</script></body></html>'


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
        status_code = 200
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


def test_fetch_permalink_media_item_supports_fetch_xdt_media_dict_fixture() -> None:
    payload = _fixture_json("post_fetch_xdt_media_dict.json")
    html = _data_sjs_html(payload)

    class _FakeResponse:
        status_code = 200
        text = html

        def raise_for_status(self) -> None:
            return None

    class _FakeSession:
        def get(self, *_args, **_kwargs):
            return _FakeResponse()

        def post(self, *_args, **_kwargs):
            raise AssertionError("inline fetch__XDTMediaDict should avoid a second GraphQL POST")

    found = fetch_permalink_media_item("DXKD0wtAHRz", session=_FakeSession())  # type: ignore[arg-type]
    assert found is not None
    assert found["pk"] == "3875927249152668787"
    assert found["id"] == "3875927249152668787_2554414"
    assert found["code"] == "DXKD0wtAHRz"
    assert found["product_type"] == "clips"
    assert found["user"]["pk"] == "2554414"
    assert found["comment_count"] == 2295
    assert found["comments_disabled"] is False
    assert found["commenting_disabled_for_viewer"] is False

    parsed = parse_permalink_metadata(found)
    assert parsed.post_format == "reel"
    assert parsed.media_type == "2"
    assert parsed.raw_media["pk"] == "3875927249152668787"
    assert parsed.raw_media["crosspost_metadata"] == {
        "post_id": "fb-post-1",
        "permalink_url": "https://www.facebook.com/bravo/posts/fb-post-1",
    }
    assert parsed.hashtags == ["RHOSLC"]
    assert parsed.mentions == ["@bravotv"]

    crosspost = fetch_instagram_facebook_crosspost_metadata(
        "DXKD0wtAHRz",
        session=_FakeSession(),  # type: ignore[arg-type]
    )
    assert crosspost is not None
    assert crosspost.comments_count == 742
    assert crosspost.raw_media["id"] == "3875927249152668787_2554414"
    assert crosspost.facebook_post_id == "fb-post-1"
    assert crosspost.facebook_post_id != "3875927249152668787"
    assert crosspost.facebook_post_id != "3875927249152668787_2554414"


def test_fetch_instagram_facebook_crosspost_metadata_ignores_fetch_xdt_null_fb_count() -> None:
    payload = _fixture_json("post_fetch_xdt_media_dict_null_fb.json")
    html = _data_sjs_html(payload)

    class _FakeResponse:
        status_code = 200
        text = html

        def __init__(self, response_payload: dict[str, object] | None = None):
            self._payload = response_payload or payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class _FakeSession:
        def get(self, *_args, **_kwargs):
            return _FakeResponse()

        def post(self, *_args, **_kwargs):
            return _FakeResponse(payload)

    assert fetch_instagram_facebook_crosspost_metadata("DXKD0wtAHR0", session=_FakeSession()) is None  # type: ignore[arg-type]


def test_post_detail_excludes_ig_direct_badge_count_query() -> None:
    payload = _fixture_json("ig_direct_badge_count_off_msys.json")
    html = _data_sjs_html(payload)

    class _FakeResponse:
        status_code = 200
        text = html

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return payload

    class _FakeSession:
        def get(self, *_args, **_kwargs):
            return _FakeResponse()

        def post(self, *_args, **_kwargs):
            return _FakeResponse()

    assert fetch_permalink_media_item("DXKD0wtAHRz", session=_FakeSession()) is None  # type: ignore[arg-type]
    assert fetch_instagram_facebook_crosspost_metadata("DXKD0wtAHRz", session=_FakeSession()) is None  # type: ignore[arg-type]


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


def test_fetch_permalink_media_item_rejects_malformed_shortcode_or_url() -> None:
    class _FakeSession:
        def get(self, *_args, **_kwargs):
            raise AssertionError("network should not be called for malformed shortcode input")

    found = fetch_permalink_media_item(
        "https://www.instagram.com/reel/not-a-valid-shortcode!!!/",
        session=_FakeSession(),  # type: ignore[arg-type]
    )
    assert found is None


def test_fetch_instagram_facebook_crosspost_metadata_reads_post_root_graphql() -> None:
    requested_posts: list[dict[str, object]] = []

    class _FakeResponse:
        status_code = 200
        text = "<html></html>"

        def __init__(self, payload: dict[str, object] | None = None):
            self._payload = payload or {}

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class _FakeSession:
        def get(self, *_args, **_kwargs):
            return _FakeResponse()

        def post(self, *_args, **kwargs):
            requested_posts.append(dict(kwargs.get("data") or {}))
            return _FakeResponse(
                {
                    "data": {
                        "xdt_api__v1__media__shortcode__web_info": {
                            "items": [
                                {
                                    "fb_comment_count": 742,
                                    "fb_like_count": "1,234",
                                    "is_shared_to_fb": True,
                                    "crosspost_metadata": {"post_id": "fb-post-1"},
                                    "social_context": {"url": "https://www.facebook.com/Bravo/posts/fb-post-1"},
                                }
                            ]
                        }
                    }
                }
            )

    metadata = fetch_instagram_facebook_crosspost_metadata(
        "DVfQnTcjsCA",
        session=_FakeSession(),  # type: ignore[arg-type]
        cookies={"sessionid": "session", "csrftoken": "csrf"},
    )

    assert metadata is not None
    assert metadata.comments_count == 742
    assert metadata.likes_count == 1234
    assert metadata.is_shared_to_fb is True
    assert metadata.facebook_post_id == "fb-post-1"
    assert metadata.facebook_post_url == "https://www.facebook.com/Bravo/posts/fb-post-1"
    assert metadata.auth_state == "authenticated"
    assert requested_posts[0]["fb_api_req_friendly_name"] == "PolarisPostRootQuery"
    assert json.loads(str(requested_posts[0]["variables"])) == {"shortcode": "DVfQnTcjsCA"}


def test_fetch_instagram_facebook_crosspost_metadata_ignores_anonymous_null_fields() -> None:
    class _FakeResponse:
        status_code = 200
        text = "<html></html>"

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "data": {
                    "xdt_api__v1__media__shortcode__web_info": {
                        "items": [
                            {
                                "fb_comment_count": None,
                                "fb_like_count": None,
                                "is_shared_to_fb": None,
                                "crosspost_metadata": None,
                                "social_context": None,
                            }
                        ]
                    }
                }
            }

    class _FakeSession:
        def get(self, *_args, **_kwargs):
            return _FakeResponse()

        def post(self, *_args, **_kwargs):
            return _FakeResponse()

    assert fetch_instagram_facebook_crosspost_metadata("DVfQnTcjsCA", session=_FakeSession()) is None  # type: ignore[arg-type]


def test_fetch_instagram_facebook_crosspost_metadata_reads_inline_payload_doc_id() -> None:
    media_item = {"fb_comment_count": 15, "is_shared_to_fb": True}
    payload = {
        "__bbox": {
            "result": {
                "data": {
                    "xdt_api__v1__media__shortcode__web_info": {
                        "items": [media_item],
                    }
                }
            }
        },
        "expectedPreloaders": [
            {
                "friendlyName": "PolarisPostRootQuery",
                "queryID": "doc-inline",
                "variables": {"shortcode": "DVfQnTcjsCA"},
            }
        ],
    }
    html = f'<html><body><script type="application/json" data-sjs>{json.dumps(payload)}</script></body></html>'

    class _FakeResponse:
        status_code = 200
        text = html

        def raise_for_status(self) -> None:
            return None

    class _FakeSession:
        def get(self, *_args, **_kwargs):
            return _FakeResponse()

        def post(self, *_args, **_kwargs):
            raise AssertionError("inline integer should avoid a second GraphQL POST")

    metadata = fetch_instagram_facebook_crosspost_metadata("DVfQnTcjsCA", session=_FakeSession())  # type: ignore[arg-type]

    assert metadata is not None
    assert metadata.comments_count == 15
    assert metadata.doc_id_used == "doc-inline"
    assert metadata.auth_state == "inline"


def test_shortcode_to_media_id_converts_known_good_shortcode() -> None:
    assert _shortcode_to_media_id("ABC") == "66"


def test_shortcode_to_media_id_rejects_invalid_character() -> None:
    with pytest.raises(ValueError, match="!'"):
        _shortcode_to_media_id("AB!")


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


def test_parse_permalink_metadata_prefers_highest_width_media_candidates() -> None:
    metadata = parse_permalink_metadata(
        {
            "taken_at": 1739481600,
            "media_type": 2,
            "video_versions": [
                {"url": "https://cdn.test/video-low.mp4", "width": 640, "height": 360},
                {"url": "https://cdn.test/video-high.mp4", "width": 1280, "height": 720},
            ],
            "image_versions2": {
                "candidates": [
                    {"url": "https://cdn.test/thumb-low.jpg", "width": 360},
                    {"url": "https://cdn.test/thumb-high.jpg", "width": 1080},
                ]
            },
        }
    )

    assert metadata.media_urls == ["https://cdn.test/video-high.mp4"]
    assert metadata.thumbnail_url == "https://cdn.test/thumb-high.jpg"


def test_resolve_instagram_media_uses_graphql_shortcode_fallback_when_api_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Disable the new highest-priority public_app_json path so the intended
    # graphql_shortcode fallback is exercised (and no live network is hit).
    monkeypatch.setattr(
        "trr_backend.socials.instagram.permalink_metadata.fetch_public_post_html",
        lambda *_args, **_kwargs: (None, None),
    )

    class _FakeResponse:
        def __init__(self, payload: dict[str, object]):
            self._payload = payload
            self.status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class _FakeSession:
        def post(self, *_args, **_kwargs):
            return _FakeResponse(
                {
                    "data": {
                        "xdt_shortcode_media": {
                            "__typename": "GraphVideo",
                            "is_video": True,
                            "video_url": "https://cdn.test/graphql-video.mp4",
                            "display_url": "https://cdn.test/graphql-thumb.jpg",
                            "taken_at_timestamp": 1739481600,
                        }
                    }
                }
            )

    resolution = resolve_instagram_media(
        "DUHvBbEDhfw",
        session=_FakeSession(),  # type: ignore[arg-type]
        fetch_post_info=lambda _shortcode: (_ for _ in ()).throw(RuntimeError("api unavailable")),
    )

    assert resolution.source == "graphql_shortcode"
    assert resolution.media_urls == ["https://cdn.test/graphql-video.mp4"]
    assert resolution.thumbnail_url == "https://cdn.test/graphql-thumb.jpg"
    # public_app_json is the new highest-priority path; with it disabled it
    # records a failed attempt first, then api_media_info, then graphql.
    assert resolution.attempts[0]["source"] == "public_app_json"
    assert resolution.attempts[0]["success"] is False
    assert resolution.attempts[1]["source"] == "api_media_info"
    assert resolution.attempts[1]["success"] is False
    assert resolution.attempts[2]["source"] == "graphql_shortcode"
    assert resolution.attempts[2]["success"] is True


def test_resolve_instagram_media_falls_back_to_og_when_other_sources_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Disable the new highest-priority public_app_json path so the intended
    # og_fallback path is exercised (and no live network is hit).
    monkeypatch.setattr(
        "trr_backend.socials.instagram.permalink_metadata.fetch_public_post_html",
        lambda *_args, **_kwargs: (None, None),
    )

    html = """
    <html>
      <head>
        <meta property="og:image" content="https://cdn.test/og-image.jpg" />
      </head>
    </html>
    """

    class _FakeGetResponse:
        status_code = 200
        text = html

        def raise_for_status(self) -> None:
            return None

    class _FakeSession:
        def post(self, *_args, **_kwargs):
            raise RuntimeError("graphql failed")

        def get(self, *_args, **_kwargs):
            return _FakeGetResponse()

    resolution = resolve_instagram_media(
        "DUHvBbEDhfw",
        session=_FakeSession(),  # type: ignore[arg-type]
        fetch_post_info=lambda _shortcode: (_ for _ in ()).throw(RuntimeError("api failed")),
    )

    assert resolution.source == "og_fallback"
    assert resolution.media_urls == ["https://cdn.test/og-image.jpg"]
    assert resolution.attempts[-1]["source"] == "og_fallback"
    assert resolution.attempts[-1]["success"] is True


# ---------------------------------------------------------------------------
# Tags vs. collaborators extraction tests
# ---------------------------------------------------------------------------


def test_graphql_node_extracts_profile_tags_and_collaborators() -> None:
    """_metadata_from_graphql_node should extract tags and collaborators from
    GraphQL response format (Bug 1 fix)."""
    node = {
        "__typename": "GraphImage",
        "display_url": "https://cdn.test/image.jpg",
        "taken_at_timestamp": 1739481600,
        "edge_media_to_caption": {"edges": [{"node": {"text": "Test post"}}]},
        "edge_media_to_tagged_user": {
            "edges": [
                {
                    "node": {
                        "user": {
                            "username": "tagged_user_1",
                            "id": "111",
                            "full_name": "Tagged One",
                            "is_verified": True,
                            "profile_pic_url": "https://pic/1.jpg",
                        },
                        "x": 0.31,
                        "y": 0.72,
                    }
                },
                {"node": {"user": {"username": "tagged_user_2", "id": "222"}}},
            ]
        },
        "coauthor_producers": [
            {"username": "collab_a", "id": "333", "full_name": "Collab A", "is_verified": False},
        ],
        "invited_coauthor_producers": [
            {"username": "collab_b", "id": "444"},
        ],
    }
    metadata = _metadata_from_graphql_node(node)
    assert metadata is not None

    # String-level tags and collaborators
    assert metadata.profile_tags == ["tagged_user_1", "tagged_user_2"]
    assert metadata.collaborators == ["collab_a", "collab_b"]

    # Rich detail objects
    assert metadata.tagged_users_detail is not None
    assert len(metadata.tagged_users_detail) == 2
    assert metadata.tagged_users_detail[0]["username"] == "tagged_user_1"
    assert metadata.tagged_users_detail[0]["full_name"] == "Tagged One"
    assert metadata.tagged_users_detail[0]["is_verified"] is True
    assert metadata.tagged_users_detail[0]["tag_x"] == 0.31
    assert metadata.tagged_users_detail[0]["tag_y"] == 0.72
    assert metadata.tagged_users_detail[0]["tag_position_source"] == "graphql_node.xy"

    assert metadata.collaborators_detail is not None
    assert len(metadata.collaborators_detail) == 2
    assert metadata.collaborators_detail[0]["username"] == "collab_a"


def test_graphql_node_with_zero_tags_returns_empty_lists() -> None:
    """When a GraphQL node has no tags or collaborators, the metadata should
    have empty lists (not None)."""
    node = {
        "__typename": "GraphImage",
        "display_url": "https://cdn.test/image.jpg",
        "taken_at_timestamp": 1739481600,
    }
    metadata = _metadata_from_graphql_node(node)
    assert metadata is not None
    assert metadata.profile_tags == []
    assert metadata.collaborators == []
    assert metadata.tagged_users_detail == []
    assert metadata.collaborators_detail == []


def test_graphql_extract_collaborators_handles_camelcase_key() -> None:
    """GraphQL extraction should handle coauthorProducers (camelCase) for
    parity with scraper.py."""
    node = {
        "coauthorProducers": [
            {"username": "camel_collab", "id": "555"},
        ],
    }
    assert _graphql_extract_collaborators(node) == ["camel_collab"]
    details = _graphql_extract_collaborators_detail(node)
    assert len(details) == 1
    assert details[0]["username"] == "camel_collab"


def test_parse_permalink_metadata_extracts_detail_objects() -> None:
    """parse_permalink_metadata should populate tagged_users_detail and
    collaborators_detail from REST API format (Bug 2 fix)."""
    metadata = parse_permalink_metadata(
        {
            "taken_at": 1739481600,
            "media_type": 1,
            "usertags": {
                "in": [
                    {
                        "user": {
                            "username": "tag1",
                            "pk": "100",
                            "full_name": "Tag One",
                            "is_verified": True,
                            "profile_pic_url": "https://pic/tag1.jpg",
                        },
                        "position": [0.2, 0.9],
                    },
                    {"user": {"username": "tag2", "pk": "200"}, "position": {"x": 0.8, "y": 0.1}},
                ]
            },
            "coauthor_producers": [
                {"username": "coauth1", "pk": "300", "full_name": "Coauth One"},
            ],
            "image_versions2": {"candidates": [{"url": "https://cdn.test/img.jpg"}]},
        }
    )
    assert metadata.tagged_users_detail is not None
    assert len(metadata.tagged_users_detail) == 2
    assert metadata.tagged_users_detail[0]["username"] == "tag1"
    assert metadata.tagged_users_detail[0]["profile_pic_url"] == "https://pic/tag1.jpg"
    assert metadata.tagged_users_detail[0]["tag_x"] == 0.2
    assert metadata.tagged_users_detail[0]["tag_y"] == 0.9
    assert metadata.tagged_users_detail[0]["tag_position_source"] == "rest_usertags.position_array"
    assert metadata.tagged_users_detail[1]["tag_x"] == 0.8
    assert metadata.tagged_users_detail[1]["tag_y"] == 0.1
    assert metadata.tagged_users_detail[1]["tag_position_source"] == "rest_usertags.position_object"

    assert metadata.collaborators_detail is not None
    assert len(metadata.collaborators_detail) == 1
    assert metadata.collaborators_detail[0]["username"] == "coauth1"


def test_parse_permalink_metadata_extracts_child_posts_with_slide_tags() -> None:
    metadata = parse_permalink_metadata(
        {
            "taken_at": 1739481600,
            "media_type": 8,
            "carousel_media": [
                {
                    "image_versions2": {"candidates": [{"url": "https://cdn.test/slide-1.jpg"}]},
                    "original_width": 1080,
                    "original_height": 1350,
                    "usertags": {
                        "in": [
                            {
                                "user": {"username": "slide_tag", "pk": "100"},
                                "position": [0.2, 0.8],
                            }
                        ]
                    },
                },
                {
                    "image_versions2": {"candidates": [{"url": "https://cdn.test/slide-2.jpg"}]},
                    "original_width": 1080,
                    "original_height": 1350,
                },
            ],
        }
    )

    assert metadata.child_posts_data is not None
    assert len(metadata.child_posts_data) == 2
    assert metadata.child_posts_data[0]["slide_index"] == 0
    assert metadata.child_posts_data[0]["display_url"] == "https://cdn.test/slide-1.jpg"
    assert metadata.child_posts_data[0]["tagged_users_detail"][0]["username"] == "slide_tag"
    assert metadata.child_posts_data[0]["tagged_users_detail"][0]["tag_x"] == 0.2
    assert metadata.child_posts_data[0]["tagged_users_detail"][0]["tag_y"] == 0.8
    assert metadata.child_posts_data[1]["slide_index"] == 1
    assert metadata.child_posts_data[1]["tagged_users_detail"] == []


def test_parse_permalink_metadata_ignores_invalid_tag_positions() -> None:
    metadata = parse_permalink_metadata(
        {
            "taken_at": 1739481600,
            "media_type": 1,
            "usertags": {
                "in": [
                    {"user": {"username": "bad_coords_1"}, "position": ["left", "top"]},
                    {"user": {"username": "bad_coords_2"}, "position": [None, 0.5]},
                    {
                        "user": {"username": "clamped_coords"},
                        "position": {"x": 1.5, "y": -0.5},
                    },
                ]
            },
            "image_versions2": {"candidates": [{"url": "https://cdn.test/img.jpg"}]},
        }
    )
    assert metadata.tagged_users_detail is not None
    assert len(metadata.tagged_users_detail) == 3
    assert "tag_x" not in metadata.tagged_users_detail[0]
    assert "tag_y" not in metadata.tagged_users_detail[0]
    assert "tag_x" not in metadata.tagged_users_detail[1]
    assert "tag_y" not in metadata.tagged_users_detail[1]
    assert metadata.tagged_users_detail[2]["tag_x"] == 1.0
    assert metadata.tagged_users_detail[2]["tag_y"] == 0.0


def test_empty_tags_from_metadata_are_not_none() -> None:
    """When metadata extraction finds 0 tags, it should return [] (not None),
    so `is not None` checks will use the authoritative empty list (Bug 3 fix)."""
    metadata = parse_permalink_metadata(
        {
            "taken_at": 1739481600,
            "media_type": 1,
            # No usertags, no coauthor_producers
            "image_versions2": {"candidates": [{"url": "https://cdn.test/img.jpg"}]},
        }
    )
    assert metadata.profile_tags == []
    assert metadata.profile_tags is not None  # Critical: not None so enrichment uses it
    assert metadata.collaborators == []
    assert metadata.collaborators is not None


def test_graphql_fallback_preserves_tags_and_collaborators(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When API path fails and GraphQL fallback succeeds, tags and collaborators
    should still be extracted (Bug 1 regression)."""

    # Disable the new highest-priority public_app_json path so the intended
    # graphql_shortcode fallback is exercised (and no live network is hit).
    monkeypatch.setattr(
        "trr_backend.socials.instagram.permalink_metadata.fetch_public_post_html",
        lambda *_args, **_kwargs: (None, None),
    )

    class _FakeResponse:
        def __init__(self, payload: dict[str, object]):
            self._payload = payload
            self.status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class _FakeSession:
        def post(self, *_args, **_kwargs):
            return _FakeResponse(
                {
                    "data": {
                        "xdt_shortcode_media": {
                            "__typename": "GraphImage",
                            "display_url": "https://cdn.test/img.jpg",
                            "taken_at_timestamp": 1739481600,
                            "edge_media_to_tagged_user": {
                                "edges": [
                                    {"node": {"user": {"username": "tagged1"}}},
                                    {"node": {"user": {"username": "tagged2"}}},
                                ]
                            },
                            "coauthor_producers": [{"username": "collab1"}],
                        }
                    }
                }
            )

    resolution = resolve_instagram_media(
        "DUHvBbEDhfw",
        session=_FakeSession(),  # type: ignore[arg-type]
        fetch_post_info=lambda _: (_ for _ in ()).throw(RuntimeError("api unavailable")),
    )
    assert resolution.source == "graphql_shortcode"
    assert resolution.metadata is not None
    assert resolution.metadata.profile_tags == ["tagged1", "tagged2"]
    assert resolution.metadata.collaborators == ["collab1"]
    assert resolution.metadata.tagged_users_detail is not None
    assert len(resolution.metadata.tagged_users_detail) == 2
    assert resolution.metadata.collaborators_detail is not None
    assert len(resolution.metadata.collaborators_detail) == 1


def test_og_fallback_has_none_detail_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When resolution falls through to OG fallback, detail objects should be
    None (not extracted), so enrichment preserves scraper data."""
    # Disable the new highest-priority public_app_json path so the intended
    # og_fallback path is exercised (and no live network is hit).
    monkeypatch.setattr(
        "trr_backend.socials.instagram.permalink_metadata.fetch_public_post_html",
        lambda *_args, **_kwargs: (None, None),
    )
    html = """
    <html>
      <head>
        <meta property="og:image" content="https://cdn.test/og-image.jpg" />
      </head>
    </html>
    """

    class _FakeGetResponse:
        status_code = 200
        text = html

        def raise_for_status(self) -> None:
            return None

    class _FakeSession:
        def post(self, *_args, **_kwargs):
            raise RuntimeError("graphql failed")

        def get(self, *_args, **_kwargs):
            return _FakeGetResponse()

    resolution = resolve_instagram_media(
        "DUHvBbEDhfw",
        session=_FakeSession(),  # type: ignore[arg-type]
        fetch_post_info=lambda _: (_ for _ in ()).throw(RuntimeError("api failed")),
    )

    assert resolution.source == "og_fallback"
    assert resolution.metadata is not None
    # OG fallback cannot extract tags/collaborators — detail should be None
    assert resolution.metadata.tagged_users_detail is None
    assert resolution.metadata.collaborators_detail is None
    # String lists are empty (not None) — this is correct for OG path
    assert resolution.metadata.profile_tags == []
    assert resolution.metadata.collaborators == []
