"""Tests for person image extraction from IMDb GraphQL credits."""

from __future__ import annotations

from trr_backend.integrations.imdb.fullcredits_cast_parser import extract_person_images_from_graphql


def test_extract_person_images_with_primary_image() -> None:
    """Test extracting primaryImage from GraphQL node."""
    credits = [
        {
            "node": {
                "name": {
                    "id": "nm0724202",
                    "nameText": {"text": "Kyle Richards"},
                    "primaryImage": {
                        "url": "https://m.media-amazon.com/images/M/MV5BMjI1MzA1ODQzMF5BMl5BanBnXkFtZTgwMDM3Mjc4NTE@._V1_.jpg",
                        "width": 340,
                        "height": 238,
                        "caption": {"plainText": "Kyle Richards"},
                    },
                },
                "episodeCredits": {"total": 328},
            }
        }
    ]

    images = extract_person_images_from_graphql(credits)

    assert len(images) == 1
    assert images[0]["imdb_person_id"] == "nm0724202"
    assert images[0]["source"] == "imdb_graphql"
    assert (
        images[0]["url"]
        == "https://m.media-amazon.com/images/M/MV5BMjI1MzA1ODQzMF5BMl5BanBnXkFtZTgwMDM3Mjc4NTE@._V1_.jpg"
    )
    assert images[0]["width"] == 340
    assert images[0]["height"] == 238
    assert images[0]["caption"] == "Kyle Richards"


def test_extract_person_images_without_primary_image() -> None:
    """Test that no image is extracted when primaryImage is missing."""
    credits = [
        {
            "node": {
                "name": {
                    "id": "nm0724202",
                    "nameText": {"text": "Kyle Richards"},
                    # No primaryImage field
                },
                "episodeCredits": {"total": 328},
            }
        }
    ]

    images = extract_person_images_from_graphql(credits)

    assert len(images) == 0


def test_extract_person_images_with_null_primary_image() -> None:
    """Test that no image is extracted when primaryImage is null."""
    credits = [
        {
            "node": {
                "name": {
                    "id": "nm0724202",
                    "nameText": {"text": "Kyle Richards"},
                    "primaryImage": None,
                },
                "episodeCredits": {"total": 328},
            }
        }
    ]

    images = extract_person_images_from_graphql(credits)

    assert len(images) == 0


def test_extract_person_images_missing_url() -> None:
    """Test that image is skipped if URL is missing."""
    credits = [
        {
            "node": {
                "name": {
                    "id": "nm0724202",
                    "nameText": {"text": "Kyle Richards"},
                    "primaryImage": {
                        "width": 340,
                        "height": 238,
                        # Missing url
                    },
                },
                "episodeCredits": {"total": 328},
            }
        }
    ]

    images = extract_person_images_from_graphql(credits)

    assert len(images) == 0


def test_extract_person_images_multiple_credits() -> None:
    """Test extracting images from multiple credits (some with, some without images)."""
    credits = [
        {
            "node": {
                "name": {
                    "id": "nm0001",
                    "nameText": {"text": "Person One"},
                    "primaryImage": {
                        "url": "https://example.com/1.jpg",
                        "width": 100,
                        "height": 150,
                    },
                },
            }
        },
        {
            "node": {
                "name": {
                    "id": "nm0002",
                    "nameText": {"text": "Person Two"},
                    # No primaryImage
                },
            }
        },
        {
            "node": {
                "name": {
                    "id": "nm0003",
                    "nameText": {"text": "Person Three"},
                    "primaryImage": {
                        "url": "https://example.com/3.jpg",
                        "width": 200,
                        "height": 250,
                        "caption": {"plainText": "Person Three at event"},
                    },
                },
            }
        },
    ]

    images = extract_person_images_from_graphql(credits)

    assert len(images) == 2
    assert images[0]["imdb_person_id"] == "nm0001"
    assert images[0]["url"] == "https://example.com/1.jpg"
    assert images[0]["caption"] is None
    assert images[1]["imdb_person_id"] == "nm0003"
    assert images[1]["url"] == "https://example.com/3.jpg"
    assert images[1]["caption"] == "Person Three at event"


def test_extract_person_images_handles_missing_dimensions() -> None:
    """Test that images are extracted even if width/height are missing."""
    credits = [
        {
            "node": {
                "name": {
                    "id": "nm0724202",
                    "nameText": {"text": "Kyle Richards"},
                    "primaryImage": {
                        "url": "https://example.com/image.jpg",
                        # Missing width and height
                    },
                },
            }
        }
    ]

    images = extract_person_images_from_graphql(credits)

    assert len(images) == 1
    assert images[0]["url"] == "https://example.com/image.jpg"
    assert images[0]["width"] is None
    assert images[0]["height"] is None


def test_extract_person_images_handles_missing_caption() -> None:
    """Test that images are extracted even if caption is missing."""
    credits = [
        {
            "node": {
                "name": {
                    "id": "nm0724202",
                    "nameText": {"text": "Kyle Richards"},
                    "primaryImage": {
                        "url": "https://example.com/image.jpg",
                        "width": 340,
                        "height": 238,
                        # No caption field
                    },
                },
            }
        }
    ]

    images = extract_person_images_from_graphql(credits)

    assert len(images) == 1
    assert images[0]["caption"] is None


def test_extract_person_images_skips_credits_without_name_id() -> None:
    """Test that credits without name ID are skipped."""
    credits = [
        {
            "node": {
                "name": {
                    # Missing id field
                    "nameText": {"text": "Unknown Person"},
                    "primaryImage": {
                        "url": "https://example.com/image.jpg",
                    },
                },
            }
        }
    ]

    images = extract_person_images_from_graphql(credits)

    assert len(images) == 0
