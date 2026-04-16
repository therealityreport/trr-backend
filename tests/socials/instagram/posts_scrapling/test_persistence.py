from __future__ import annotations


def test_adapt_graph_node_to_post_dto():
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "shortcode": "CxTestShort",
        "taken_at_timestamp": 1700000000,
        "__typename": "GraphVideo",
        "display_url": "https://example.com/img.jpg",
        "video_view_count": 12345,
        "edge_media_preview_like": {"count": 100},
        "edge_media_to_comment": {"count": 50},
        "edge_media_to_caption": {"edges": [{"node": {"text": "Hello #world @friend"}}]},
        "owner": {"username": "testuser", "id": "999"},
        "id": "3200000000000",
    }
    dto = _graph_node_to_post_dto(node, account_handle="testuser")
    assert dto.shortcode == "CxTestShort"
    assert dto.likes == 100
    assert dto.comments == 50
    assert dto.video_views == 12345
    assert dto.caption == "Hello #world @friend"
    assert dto.post_type == "video"
    assert dto.username == "testuser"
    assert dto.pk == "3200000000000"
    assert dto.taken_at == 1700000000
    assert hasattr(dto, "to_dict")


def test_adapt_graph_node_carousel():
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "shortcode": "CxCarousel",
        "taken_at_timestamp": 1700000000,
        "__typename": "GraphSidecar",
        "display_url": "https://example.com/img.jpg",
        "edge_sidecar_to_children": {
            "edges": [
                {"node": {"display_url": "https://example.com/1.jpg"}},
                {"node": {"display_url": "https://example.com/2.jpg", "video_url": "https://example.com/2.mp4"}},
            ]
        },
        "edge_media_preview_like": {"count": 0},
        "edge_media_to_comment": {"count": 0},
        "edge_media_to_caption": {"edges": []},
        "owner": {"username": "testuser"},
        "id": "pk123",
    }
    dto = _graph_node_to_post_dto(node, account_handle="testuser")
    assert dto.post_type == "carousel"
    # display_url + child display_urls + child video_url (deduped)
    assert len(dto.media_urls) >= 3


def test_adapt_graph_node_image():
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "shortcode": "CxImage",
        "taken_at_timestamp": 1700000000,
        "__typename": "GraphImage",
        "display_url": "https://example.com/photo.jpg",
        "edge_media_preview_like": {"count": 42},
        "edge_media_to_comment": {"count": 5},
        "edge_media_to_caption": {"edges": [{"node": {"text": "Nice photo"}}]},
        "owner": {"username": "photog"},
        "id": "pk456",
    }
    dto = _graph_node_to_post_dto(node, account_handle="photog")
    assert dto.post_type == "image"
    assert dto.video_views == 0


def test_adapt_xdt_media_dict_video():
    """The profile timeline connection returns XDTMediaDict — the shape IG
    uses as of April 2026. Verify the adapter reads the new field names."""
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "__typename": "XDTMediaDict",
        "code": "DXKD0wtAHRz",
        "pk": "3875927249152668787",
        "id": "3875927249152668787_2554414",
        "media_type": 2,  # video
        "like_count": 36530,
        "comment_count": 2295,
        "taken_at": 1776272482,
        "caption": {"text": "Hello world #fyp"},
        "user": {"pk": "2554414", "username": "bravotv"},
        "image_versions2": {
            "candidates": [
                {"url": "https://cdn.example.com/thumb1.jpg", "width": 1080, "height": 1920},
                {"url": "https://cdn.example.com/thumb2.jpg", "width": 640, "height": 1138},
            ]
        },
        "video_versions": [
            {"url": "https://cdn.example.com/video1.mp4", "width": 720, "height": 1280},
        ],
    }
    dto = _graph_node_to_post_dto(node, account_handle="bravotv")
    assert dto.shortcode == "DXKD0wtAHRz"  # from `code`, not `shortcode`
    assert dto.post_type == "video"  # media_type=2 → video
    assert dto.likes == 36530  # from `like_count`
    assert dto.comments == 2295  # from `comment_count`
    assert dto.taken_at == 1776272482  # from `taken_at` (no _timestamp suffix)
    assert dto.caption == "Hello world #fyp"  # from caption.text dict
    assert dto.username == "bravotv"  # from user.username
    assert dto.pk == "3875927249152668787"  # prefers pk over composite id
    assert "https://cdn.example.com/thumb1.jpg" in dto.media_urls
    assert "https://cdn.example.com/video1.mp4" in dto.media_urls
    assert dto.thumbnail_url == "https://cdn.example.com/thumb1.jpg"


def test_adapt_xdt_media_dict_carousel():
    """XDTMediaDict carousel shape uses carousel_media (list of XDTMediaDict children)."""
    from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto

    node = {
        "__typename": "XDTMediaDict",
        "code": "XCAROU1",
        "pk": "1111",
        "media_type": 8,  # carousel
        "like_count": 100,
        "comment_count": 10,
        "taken_at": 1700000000,
        "caption": {"text": "Album"},
        "user": {"username": "u"},
        "carousel_media": [
            {"image_versions2": {"candidates": [{"url": "https://cdn.example.com/c1.jpg"}]}},
            {
                "image_versions2": {"candidates": [{"url": "https://cdn.example.com/c2.jpg"}]},
                "video_versions": [{"url": "https://cdn.example.com/c2.mp4"}],
            },
        ],
    }
    dto = _graph_node_to_post_dto(node, account_handle="u")
    assert dto.post_type == "carousel"  # media_type=8 → carousel
    assert len(dto.media_urls) >= 3  # 2 image urls + 1 video url
