from __future__ import annotations

from trr_backend.ingestion.imdb_show_mediaindex import fetch_imdb_show_mediaindex_rows
from trr_backend.integrations.imdb.mediaindex_images import ImdbMediaImage


def test_fetch_imdb_show_mediaindex_rows_flattens_tags_and_maps_still_frame(monkeypatch) -> None:
    images = [
        ImdbMediaImage(
            imdb_id="tt1234567",
            imdb_image_id="rm100",
            position=1,
            caption="Sample caption",
            width=1600,
            height=900,
            url="https://m.media-amazon.com/images/M/rm100.jpg",
            viewer_path="/title/tt1234567/mediaviewer/rm100/",
            viewer_url="https://www.imdb.com/title/tt1234567/mediaviewer/rm100/",
            image_type="Still Frame",
            metadata={},
        )
    ]

    monkeypatch.setattr(
        "trr_backend.ingestion.imdb_show_mediaindex.fetch_imdb_mediaindex_images",
        lambda *_args, **_kwargs: images,
    )
    monkeypatch.setattr(
        "trr_backend.ingestion.imdb_show_mediaindex.fetch_imdb_mediaviewer_tags",
        lambda *_args, **_kwargs: {
            "image_type": "Still Frame",
            "people": [
                {"imdb_id": "nm100", "name": "Person One"},
                {"imdb_id": "nm200", "name": "Person Two"},
            ],
            "titles": [
                {"imdb_id": "tt1234567", "title": "The Real Housewives of Salt Lake City"},
                {"imdb_id": "tt7654321", "title": "Reunion Part 1"},
            ],
        },
    )

    rows = fetch_imdb_show_mediaindex_rows(
        "tt1234567",
        show_id="show-123",
        max_pages=1,
        include_tags=True,
    )

    assert len(rows) == 1
    row = rows[0]
    metadata = row["metadata"]
    assert row["kind"] == "episode_still"
    assert row["image_type"] == "Still Frame"
    assert metadata["imdb_image_type"] == "Still Frame"
    assert metadata["people_names"] == ["Person One", "Person Two"]
    assert metadata["people_imdb_ids"] == ["nm100", "nm200"]
    assert metadata["title_names"] == ["The Real Housewives of Salt Lake City", "Reunion Part 1"]
    assert metadata["title_imdb_ids"] == ["tt1234567", "tt7654321"]


def test_fetch_imdb_show_mediaindex_rows_maps_non_still_types_without_tags(monkeypatch) -> None:
    images = [
        ImdbMediaImage(
            imdb_id="tt1234567",
            imdb_image_id="rm200",
            position=1,
            caption=None,
            width=1000,
            height=1500,
            url="https://m.media-amazon.com/images/M/rm200.jpg",
            viewer_path="/title/tt1234567/mediaviewer/rm200/",
            viewer_url="https://www.imdb.com/title/tt1234567/mediaviewer/rm200/",
            image_type="Poster",
            metadata={},
        )
    ]
    monkeypatch.setattr(
        "trr_backend.ingestion.imdb_show_mediaindex.fetch_imdb_mediaindex_images",
        lambda *_args, **_kwargs: images,
    )

    rows = fetch_imdb_show_mediaindex_rows(
        "tt1234567",
        show_id="show-123",
        max_pages=1,
        include_tags=False,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["kind"] == "poster"
    assert row["image_type"] == "Poster"
    assert row["metadata"]["imdb_image_type"] == "Poster"
