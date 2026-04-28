from __future__ import annotations

from trr_backend.socials.instagram.post_normalizer import (
    normalize_instagram_comment,
    normalize_instagram_post,
)
from trr_backend.socials.instagram.profile_normalizer import normalize_instagram_profile
from trr_backend.socials.instagram.profile_relationship_normalizer import (
    normalize_instagram_profile_relationships,
)


def test_normalize_xdt_post_extracts_rich_post_fields() -> None:
    post = normalize_instagram_post(
        {
            "__typename": "XDTMediaDict",
            "code": "DXT123",
            "pk": "987654321",
            "id": "987654321_34166823",
            "media_type": 2,
            "product_type": "clips",
            "taken_at": 1776272482,
            "caption": {
                "id": "caption-1",
                "text": "Launch day #Artemis @nasa",
                "created_at": 1776272483,
                "is_edited": True,
            },
            "like_count": 1000,
            "comment_count": 50,
            "user": {
                "pk": "34166823",
                "username": "nasa",
                "full_name": "NASA",
                "profile_pic_url": "https://cdn.example.com/nasa-avatar.jpg",
                "is_verified": True,
            },
            "image_versions2": {
                "candidates": [
                    {"url": "https://cdn.example.com/thumb-1080.jpg", "width": 1080, "height": 1350},
                    {"url": "https://cdn.example.com/thumb-640.jpg", "width": 640, "height": 800},
                ]
            },
            "video_versions": [
                {"url": "https://cdn.example.com/video.mp4", "width": 720, "height": 1280},
            ],
            "usertags": {
                "in": [
                    {
                        "position": [0.25, 0.75],
                        "user": {
                            "pk": "42",
                            "username": "astro_crew",
                            "full_name": "Astro Crew",
                            "profile_pic_url": "https://cdn.example.com/astro.jpg",
                            "is_verified": False,
                        },
                    }
                ]
            },
            "coauthor_producers": [
                {
                    "pk": "7",
                    "username": "iss",
                    "full_name": "International Space Station",
                    "profile_pic_url": "https://cdn.example.com/iss.jpg",
                    "is_verified": True,
                }
            ],
            "location": {"id": "loc-1", "name": "Kennedy Space Center", "lat": 28.5729, "lng": -80.649},
            "comments_disabled": True,
            "music_info": {"artist_name": "NASA Audio", "song_name": "Countdown"},
            "audio_url": "https://cdn.example.com/audio.m4a",
            "video_duration": 12.5,
            "contextItems": [{"type": "media_note", "text": "Pinned launch context"}],
        },
        account_handle="nasa",
    )

    assert post.source_shape == "xdt_media_dict"
    assert post.shortcode == "DXT123"
    assert post.source_id == "987654321"
    assert post.media_type == "reel"
    assert post.owner is not None
    assert post.owner.username == "nasa"
    assert post.owner.user_id == "34166823"
    assert post.owner.full_name == "NASA"
    assert post.owner.is_verified is True
    assert post.caption.text == "Launch day #Artemis @nasa"
    assert post.caption.caption_id == "caption-1"
    assert post.caption.is_edited is True
    assert post.hashtags == ["Artemis"]
    assert post.mentions == ["@nasa"]
    assert post.tagged_users[0].username == "astro_crew"
    assert post.tagged_users[0].tag_x == 0.25
    assert post.tagged_users[0].tag_y == 0.75
    assert post.collaborators[0].username == "iss"
    assert "https://cdn.example.com/thumb-1080.jpg" in post.media_urls
    assert "https://cdn.example.com/video.mp4" in post.media_urls
    assert post.width == 1080
    assert post.height == 1350
    assert post.location is not None
    assert post.location.name == "Kennedy Space Center"
    assert post.flags["comments_disabled"] is True
    assert post.music_info == {"artist_name": "NASA Audio", "song_name": "Countdown"}
    assert post.audio_url == "https://cdn.example.com/audio.m4a"
    assert post.video_duration == 12.5
    assert post.context_items[0]["type"] == "media_note"


def test_normalize_media_info_shortcode_style_fixture_extracts_children() -> None:
    post = normalize_instagram_post(
        {
            "items": [
                {
                    "code": "MEDIA123",
                    "pk": "123",
                    "id": "123_456",
                    "media_type": 8,
                    "taken_at": 1776272000,
                    "caption": {"text": "Carousel from orbit #Space"},
                    "user": {"pk": "456", "username": "nasa"},
                    "image_versions2": {
                        "candidates": [{"url": "https://cdn.example.com/cover.jpg", "width": 1080, "height": 1080}]
                    },
                    "carousel_media": [
                        {
                            "pk": "child-1",
                            "media_type": 1,
                            "original_width": 1080,
                            "original_height": 1080,
                            "image_versions2": {
                                "candidates": [
                                    {
                                        "url": "https://cdn.example.com/child-1.jpg",
                                        "width": 1080,
                                        "height": 1080,
                                    }
                                ]
                            },
                            "accessibility_caption": "Earth from space",
                        },
                        {
                            "pk": "child-2",
                            "media_type": 2,
                            "original_width": 720,
                            "original_height": 1280,
                            "video_versions": [
                                {"url": "https://cdn.example.com/child-2.mp4", "width": 720, "height": 1280}
                            ],
                        },
                    ],
                    "usertags": {
                        "in": [
                            {
                                "position": {"x": 0.1, "y": 0.2},
                                "user": {"pk": "99", "username": "earthobservatory"},
                            }
                        ]
                    },
                }
            ]
        }
    )

    assert post.source_shape == "media_info_rest"
    assert post.shortcode == "MEDIA123"
    assert post.media_type == "carousel"
    assert post.permalink == "https://www.instagram.com/p/MEDIA123/"
    assert post.hashtags == ["Space"]
    assert post.child_posts[0].source_id == "child-1"
    assert post.child_posts[0].media_type == "image"
    assert post.child_posts[0].alt_text == "Earth from space"
    assert post.child_posts[1].media_type == "video"
    assert "https://cdn.example.com/child-2.mp4" in post.media_urls
    assert post.tagged_users[0].username == "earthobservatory"
    assert post.tagged_users[0].tag_position_source == "rest_usertags.position_object"


def test_normalize_apify_aliases_excludes_latest_comment_samples() -> None:
    post = normalize_instagram_post(
        {
            "id": "apify-id-1",
            "shortCode": "APIFY123",
            "type": "Video",
            "caption": "Behind the scenes #Traitors @bravotv",
            "displayUrl": "https://cdn.example.com/apify-cover.jpg",
            "videoUrl": "https://cdn.example.com/apify-video.mp4",
            "dimensionsWidth": 1920,
            "dimensionsHeight": 1080,
            "ownerUsername": "bravotv",
            "ownerFullName": "Bravo",
            "ownerId": "bravo-1",
            "ownerProfilePicUrl": "https://cdn.example.com/bravo.jpg",
            "taggedUsers": [{"username": "host", "fullName": "Host Name", "id": "host-1", "x": 0.4, "y": 0.6}],
            "coauthorProducers": [{"username": "peacock", "id": "peacock-1", "is_verified": True}],
            "locationName": "Castle",
            "locationId": "castle-1",
            "musicInfo": {"song_name": "Theme"},
            "latestComments": [{"id": "latest-1", "text": "sample only", "ownerUsername": "viewer"}],
            "firstComment": {"id": "first-1", "text": "also sample only"},
        }
    )

    assert post.source_shape == "apify_adapter"
    assert post.shortcode == "APIFY123"
    assert post.source_id == "apify-id-1"
    assert post.owner is not None
    assert post.owner.username == "bravotv"
    assert post.owner.full_name == "Bravo"
    assert post.tagged_users[0].username == "host"
    assert post.tagged_users[0].tag_x == 0.4
    assert post.collaborators[0].username == "peacock"
    assert post.width == 1920
    assert post.height == 1080
    assert post.location is not None
    assert post.location.name == "Castle"
    assert post.comment_samples_excluded == ["latestComments", "firstComment"]
    assert post.comments == []


def test_normalize_nasa_style_profile_payload() -> None:
    profile = normalize_instagram_profile(
        {
            "data": {
                "user": {
                    "id": "34166823",
                    "pk": "34166823",
                    "username": "nasa",
                    "inputUrl": "https://www.instagram.com/nasa",
                    "full_name": "NASA",
                    "biography": "Exploring the universe and our home planet.",
                    "edge_followed_by": {"count": 96000000},
                    "edge_follow": {"count": 77},
                    "edge_owner_to_timeline_media": {"count": 4123},
                    "highlightReelCount": 7,
                    "igtvVideoCount": 171,
                    "is_business_account": True,
                    "joinedRecently": False,
                    "hasChannel": False,
                    "is_private": False,
                    "is_verified": True,
                    "external_url": "https://www.nasa.gov/",
                    "externalUrlShimmed": "https://l.instagram.com/?u=https%3A%2F%2Fwww.nasa.gov%2F",
                    "externalUrls": [
                        {
                            "url": "https://www.nasa.gov/",
                            "title": "NASA",
                            "lynx_url": "https://l.instagram.com/?u=https%3A%2F%2Fwww.nasa.gov%2F",
                            "link_type": "external",
                        },
                        {"url": "https://science.nasa.gov/", "title": "NASA Science"},
                    ],
                    "bio_links": [{"url": "https://plus.nasa.gov/", "title": "NASA Plus"}],
                    "profile_pic_url": "https://cdn.example.com/nasa-small.jpg",
                    "profile_pic_url_hd": "https://cdn.example.com/nasa-hd.jpg",
                    "business_category_name": "Government organization",
                    "category_name": "Science, Technology & Engineering",
                    "about": {
                        "country": "United States",
                        "date_joined": "2012-11-01",
                        "date_verified": "2014-01-15",
                        "former_usernames_count": 0,
                    },
                }
            }
        }
    )

    assert profile.source_shape == "web_profile_info"
    assert profile.profile_id == "34166823"
    assert profile.pk == "34166823"
    assert profile.input_url == "https://www.instagram.com/nasa"
    assert profile.url == "https://www.instagram.com/nasa"
    assert profile.username == "nasa"
    assert profile.full_name == "NASA"
    assert profile.biography.startswith("Exploring the universe")
    assert profile.followers_count == 96000000
    assert profile.follows_count == 77
    assert profile.posts_count == 4123
    assert profile.highlight_reel_count == 7
    assert profile.igtv_video_count == 171
    assert profile.is_business_account is True
    assert profile.joined_recently is False
    assert profile.has_channel is False
    assert profile.is_private is False
    assert profile.is_verified is True
    assert profile.external_url == "https://www.nasa.gov/"
    assert profile.external_url_shimmed == "https://l.instagram.com/?u=https%3A%2F%2Fwww.nasa.gov%2F"
    assert [link.url for link in profile.external_links] == [
        "https://www.nasa.gov/",
        "https://science.nasa.gov/",
        "https://plus.nasa.gov/",
    ]
    assert profile.external_links[0].shim_url == "https://l.instagram.com/?u=https%3A%2F%2Fwww.nasa.gov%2F"
    assert profile.external_links[0].link_type == "external"
    assert profile.profile_pic_url_hd == "https://cdn.example.com/nasa-hd.jpg"
    assert profile.country == "United States"
    assert profile.date_joined == "2012-11-01"
    assert profile.date_verified == "2014-01-15"
    assert profile.former_usernames_count == 0


def test_normalize_following_relationship_rows_only() -> None:
    result = normalize_instagram_profile_relationships(
        [
            {
                "username_scrape": "nasa",
                "type": "Following",
                "id": "528817151",
                "username": "nasagoddard",
                "full_name": "NASA Goddard",
                "profile_pic_url": "https://cdn.example.com/goddard.jpg",
                "is_private": False,
                "is_verified": True,
            }
        ],
        owner_username="nasa",
        intended_relationship_type="following",
        source_cursor="cursor-1",
        source_page_ordinal=2,
    )

    assert result.mismatches == []
    assert len(result.relationships) == 1
    relationship = result.relationships[0]
    assert relationship.relationship_type == "following"
    assert relationship.owner_username == "nasa"
    assert relationship.related_username == "nasagoddard"
    assert relationship.related_user_id == "528817151"
    assert relationship.related_full_name == "NASA Goddard"
    assert relationship.related_profile_pic_url == "https://cdn.example.com/goddard.jpg"
    assert relationship.related_is_private is False
    assert relationship.related_is_verified is True
    assert relationship.source_type == "Following"
    assert relationship.source_cursor == "cursor-1"
    assert relationship.source_page_ordinal == 2


def test_normalize_followers_relationship_rows_are_rejected() -> None:
    result = normalize_instagram_profile_relationships(
        [
            {
                "username_scrape": "nasa",
                "type": "Followers",
                "id": "viewer-1",
                "username": "fan_account",
                "full_name": "Fan Account",
            }
        ],
        owner_username="nasa",
        intended_relationship_type="following",
    )

    assert result.relationships == []
    assert len(result.mismatches) == 1
    assert result.mismatches[0].code == "followers_out_of_scope"
    assert result.mismatches[0].source_relationship_type == "Followers"


def test_normalize_nested_comment_tree() -> None:
    comment = normalize_instagram_comment(
        {
            "id": "comment-1",
            "text": "Top-level comment",
            "ownerUsername": "viewer",
            "ownerId": "viewer-1",
            "ownerProfilePicUrl": "https://cdn.example.com/viewer.jpg",
            "timestamp": 1776273000,
            "likesCount": 12,
            "repliesCount": 1,
            "replies": [
                {
                    "pk": "reply-1",
                    "text": "Nested reply",
                    "user": {
                        "pk": "reply-user-1",
                        "username": "reply_user",
                        "profile_pic_url": "https://cdn.example.com/reply.jpg",
                        "is_verified": True,
                    },
                    "created_at": 1776273100,
                    "comment_like_count": 3,
                    "child_comment_count": 0,
                }
            ],
        }
    )

    assert comment.comment_id == "comment-1"
    assert comment.text == "Top-level comment"
    assert comment.author.username == "viewer"
    assert comment.author.user_id == "viewer-1"
    assert comment.author.profile_pic_url == "https://cdn.example.com/viewer.jpg"
    assert comment.created_at == 1776273000
    assert comment.likes_count == 12
    assert comment.replies_count == 1
    assert len(comment.replies) == 1
    reply = comment.replies[0]
    assert reply.comment_id == "reply-1"
    assert reply.parent_comment_id == "comment-1"
    assert reply.author.username == "reply_user"
    assert reply.author.user_id == "reply-user-1"
    assert reply.author.profile_pic_url == "https://cdn.example.com/reply.jpg"
    assert reply.author.is_verified is True
    assert reply.created_at == 1776273100
    assert reply.likes_count == 3
