from __future__ import annotations

from trr_backend.socials.instagram.posts_scrapling.persistence import _graph_node_to_post_dto


def test_graph_node_to_post_dto_uses_shared_normalizer_for_queryable_fields() -> None:
    dto = _graph_node_to_post_dto(
        {
            "code": "DXrNv_lEotv",
            "pk": "3885259576224942959",
            "media_type": 1,
            "caption": {
                "pk": "caption-1",
                "text": "commissions are open #graphicdesign @hroien",
                "is_edited": False,
                "has_translation": True,
            },
            "like_count": 3,
            "comment_count": 1,
            "taken_at": 1777379165,
            "user": {
                "pk": "61503085324",
                "username": "jographicss",
                "full_name": "joseph",
                "profile_pic_url": "https://cdn.example.com/profile.jpg",
                "profile_pic_url_hd": "https://cdn.example.com/profile-hd.jpg",
                "is_verified": False,
            },
            "image_versions2": {
                "candidates": [{"url": "https://cdn.example.com/post.jpg", "height": 1800, "width": 1440}]
            },
            "usertags": {
                "in": [
                    {
                        "position": [0.5, 0.5],
                        "user": {
                            "pk": "43137384920",
                            "username": "hroien",
                            "full_name": "joseph",
                        },
                    }
                ]
            },
            "coauthor_producers": [{"pk": "43137384920", "username": "hroien"}],
            "location": {"id": "loc-1", "name": "Studio"},
            "comments_disabled": False,
            "like_and_view_counts_disabled": True,
            "audio_url": "https://cdn.example.com/audio.m4a",
        },
        account_handle="jographicss",
    )

    assert dto.shortcode == "DXrNv_lEotv"
    assert dto.caption_id == "caption-1"
    assert dto.caption_has_translation is True
    assert dto.owner_user_id == "61503085324"
    assert dto.owner_profile_pic_url_hd == "https://cdn.example.com/profile-hd.jpg"
    assert dto.hashtags == ["graphicdesign"]
    assert dto.mentions == ["@hroien"]
    assert dto.profile_tags == ["hroien"]
    assert dto.collaborators == ["hroien"]
    assert dto.location_id == "loc-1"
    assert dto.location_name == "Studio"
    assert dto.like_and_view_counts_disabled is True
    assert dto.audio_url == "https://cdn.example.com/audio.m4a"
