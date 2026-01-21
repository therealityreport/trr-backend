from __future__ import annotations

from trr_backend.repositories.media_assets import (
    transform_person_images_to_media,
    transform_show_images_to_media,
)


def test_transform_show_images_preserves_urls_and_hosted() -> None:
    rows = [
        {
            "show_id": "00000000-0000-0000-0000-000000000001",
            "source": "tmdb",
            "source_image_id": "abc123",
            "url": "https://image.tmdb.org/t/p/original/foo.jpg",
            "hosted_url": "https://cdn.example.com/media/foo.jpg",
            "kind": "poster",
            "width": 1000,
            "height": 1500,
            "caption": "Promo",
            "file_path": "/foo.jpg",
            "iso_639_1": "en",
            "metadata": {"vote_average": 7.5},
        }
    ]

    assets, links = transform_show_images_to_media(rows)

    assert len(assets) == 1
    asset = assets[0]
    assert asset["source_url"] == "https://image.tmdb.org/t/p/original/foo.jpg"
    assert asset["hosted_url"] == "https://cdn.example.com/media/foo.jpg"
    assert asset["metadata"]["vote_average"] == 7.5

    assert len(links) == 1
    link = links[0]
    assert link["entity_type"] == "show"
    assert link["kind"] == "poster"
    assert link["context"]["file_path"] == "/foo.jpg"


def test_transform_show_images_dedup_by_source_url() -> None:
    rows = [
        {
            "show_id": "00000000-0000-0000-0000-000000000001",
            "source": "imdb",
            "source_image_id": None,
            "url": "https://m.media-amazon.com/images/M/abc.jpg",
            "kind": "gallery",
        },
        {
            "show_id": "00000000-0000-0000-0000-000000000002",
            "source": "imdb",
            "source_image_id": None,
            "url": "https://m.media-amazon.com/images/M/abc.jpg",
            "kind": "gallery",
        },
    ]

    assets, links = transform_show_images_to_media(rows)

    assert len(assets) == 1
    assert len(links) == 2


def test_transform_person_images_primary_flag() -> None:
    rows = [
        {
            "person_id": "00000000-0000-0000-0000-000000000010",
            "source": "imdb_graphql",
            "url": "https://m.media-amazon.com/images/M/xyz.jpg",
            "width": 640,
            "height": 480,
            "caption": "Sample",
            "is_primary": False,
        }
    ]

    assets, links = transform_person_images_to_media(rows)

    assert len(assets) == 1
    assert len(links) == 1
    assert links[0]["is_primary"] is False
