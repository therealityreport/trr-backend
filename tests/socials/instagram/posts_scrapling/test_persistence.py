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
