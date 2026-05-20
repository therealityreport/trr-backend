from __future__ import annotations

from trr_backend.services.person_images import url_selection


def test_pick_autocount_url_prefers_hosted_when_available() -> None:
    row = {
        "source": "fandom",
        "image_url": "https://real-housewives.fandom.com/wiki/Special:FilePath/Bad.png",
        "thumb_url": "https://static.wikia.nocookie.net/real-housewives/images/1/1a/Good.png",
        "hosted_url": "https://cdn.example.com/x.png",
    }
    assert url_selection.pick_autocount_url(row) == row["hosted_url"]


def test_pick_autocount_urls_normalizes_stale_wikia_revision_path() -> None:
    row = {
        "source": "fandom",
        "thumb_url": "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie_k_s3.jpeg/revision/latest",
    }
    urls = url_selection.pick_autocount_urls(row)
    assert urls[0] == "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie_k_s3.jpeg"


def test_pick_autocount_url_prefers_tmdb_image() -> None:
    row = {
        "source": "tmdb",
        "image_url": "https://image.tmdb.org/t/p/original/x.png",
        "hosted_url": "https://cdn.example.com/x.png",
    }
    assert url_selection.pick_autocount_url(row) == row["image_url"]


def test_build_media_link_autocount_urls_normalizes_fandom_source_url() -> None:
    row = {
        "source": "fandom-gallery",
        "hosted_url": "https://cdn.example.com/x.png",
        "source_url": "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie.jpeg/revision/latest",
        "metadata": {"source_page_url": "https://real-housewives.fandom.com/wiki/Angie"},
    }

    urls = url_selection.build_media_link_autocount_urls(row)

    assert urls == [
        "https://cdn.example.com/x.png",
        "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie.jpeg",
        row["source_url"],
    ]


def test_should_reset_getty_hosted_state_resets_getty_hosted_url() -> None:
    assert url_selection.should_reset_getty_hosted_state(
        desired_original_url="https://media.gettyimages.com/id/example/photo/full.jpg",
        current_source_url="https://media.gettyimages.com/id/example/photo/full.jpg",
        hosted_url="https://media.gettyimages.com/id/example/photo/preview.jpg",
        hosted_key="images/people/example.jpg",
        metadata={},
    )


def test_should_reset_getty_hosted_state_keeps_matching_hosted_key() -> None:
    assert not url_selection.should_reset_getty_hosted_state(
        desired_original_url="https://media.gettyimages.com/id/example/photo/full.jpg",
        current_source_url="https://media.gettyimages.com/id/example/photo/full.jpg",
        hosted_url="https://cdn.example.com/images/people/example.jpg",
        hosted_key="images/people/example.jpg",
        metadata={"mirrored_from": "https://media.gettyimages.com/id/example/photo/full.jpg"},
    )
