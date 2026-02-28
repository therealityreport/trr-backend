from __future__ import annotations

from trr_backend.ingestion.cast_photo_sources import fetch_imdb_cast_photos


def test_fetch_imdb_cast_photos_sets_imdb_metadata_defaults(monkeypatch) -> None:
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaindex_html",
        lambda imdb_person_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaindex_images",
        lambda html, imdb_person_id: [
            {
                "source_image_id": "rm123456",
                "viewer_id": "rm123456",
                "mediaviewer_url_path": "/name/nm0000001/mediaviewer/rm123456/",
                "url": "https://m.media-amazon.com/images/M/MV5BBASE._UX640_.jpg",
                "url_path": "/images/M/MV5BBASE._UX640_.jpg",
                "width": 640,
                "height": 360,
            }
        ],
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaviewer_html",
        lambda imdb_person_id, viewer_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaviewer_details",
        lambda html, viewer_id=None: {
            "url": "https://m.media-amazon.com/images/M/MV5BDETAIL._V1_.jpg",
            "url_path": "/images/M/MV5BDETAIL._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Andy Cohen in Watch What Happens Live (2022)",
            "gallery_index": 2,
            "gallery_total": 45,
            "people_imdb_ids": ["nm0000001"],
            "people_names": ["Andy Cohen"],
            "title_imdb_ids": ["tt0000001"],
            "title_names": ["Watch What Happens Live (2022)"],
        },
    )

    rows = fetch_imdb_cast_photos(
        imdb_person_id="nm0000001",
        person_id="00000000-0000-0000-0000-000000000001",
        limit=10,
    )

    assert len(rows) == 1
    row = rows[0]
    metadata = row.get("metadata") or {}
    assert metadata["source_variant"] == "imdb_person_gallery"
    assert metadata["source_logo"] == "IMDb"
    assert metadata["source_page_url"] == "https://www.imdb.com/name/nm0000001/mediaviewer/rm123456/"
    assert metadata["source_file_url"] == "https://m.media-amazon.com/images/M/MV5BDETAIL._V1_.jpg"
    assert metadata["source_page_title"] == "Watch What Happens Live (2022)"
    assert metadata["asset_name"] == "Watch What Happens Live (2022)"
    assert metadata["name"] == "Watch What Happens Live (2022)"
    assert row["people_names"] == ["Andy Cohen"]
    assert row["title_names"] == ["Watch What Happens Live (2022)"]
