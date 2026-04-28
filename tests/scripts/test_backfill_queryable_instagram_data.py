from __future__ import annotations

from scripts.socials.instagram.backfill_queryable_instagram_data import _post_dto_from_row


def test_post_dto_from_row_preserves_queryable_xdt_fields() -> None:
    dto = _post_dto_from_row(
        {
            "shortcode": "DXrNv_lEotv",
            "media_id": "3885259576224942959",
            "username": "jographicss",
            "source_account": "jographicss",
            "caption": "commissions are open!",
            "media_type": "image",
            "likes": 3,
            "comments_count": 1,
            "views": 0,
            "raw_data": {
                "pk": "3885259576224942959",
                "code": "DXrNv_lEotv",
                "caption": {
                    "pk": "18072449729658081",
                    "text": "commissions are open!",
                    "has_translation": False,
                },
                "caption_is_edited": False,
                "user": {
                    "id": "61503085324",
                    "username": "jographicss",
                    "hd_profile_pic_url_info": {"url": "https://cdn.test/avatar-hd.jpg"},
                },
                "location": {"id": "1916295438661954", "name": "Cranberry Marsh"},
                "original_width": 1440,
                "original_height": 1800,
                "comments_disabled": False,
                "like_and_view_counts_disabled": True,
                "commenting_disabled_for_viewer": False,
                "is_paid_partnership": False,
                "isAdvertisement": False,
                "can_viewer_reshare": True,
                "has_audio": False,
                "media_repost_count": 0,
            },
        }
    )

    assert dto is not None
    assert dto.source_post_id == "3885259576224942959"
    assert dto.caption_id == "18072449729658081"
    assert dto.caption_is_edited is False
    assert dto.caption_has_translation is False
    assert dto.owner_user_id == "61503085324"
    assert dto.owner_profile_pic_url_hd == "https://cdn.test/avatar-hd.jpg"
    assert dto.location_id == "1916295438661954"
    assert dto.location_name == "Cranberry Marsh"
    assert dto.original_width == 1440
    assert dto.original_height == 1800
    assert dto.comments_disabled is False
    assert dto.like_and_view_counts_disabled is True
    assert dto.commenting_disabled_for_viewer is False
    assert dto.media_repost_count == 0
    assert dto.is_paid_partnership is False
    assert dto.is_advertisement is False
    assert dto.can_viewer_reshare is True
    assert dto.has_audio is False
