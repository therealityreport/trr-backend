from __future__ import annotations

from pathlib import Path

from trr_backend.integrations.imdb.mediaindex_images import parse_imdb_mediaindex_images


def test_parse_imdb_mediaindex_html() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "mediaindex_tt8819906_sample.html").read_text(encoding="utf-8")

    images, page_info = parse_imdb_mediaindex_images(html, imdb_id="tt8819906")
    assert page_info["has_next_page"] is True
    assert page_info["end_cursor"] == "CURSOR1"
    assert page_info["build_id"] == "TESTBUILD"

    assert len(images) == 2
    first = images[0]
    assert first["source_image_id"] == "rm3107491329"
    assert first["url"] == "https://m.media-amazon.com/images/M/1.jpg"
    assert first["url_path"] == "/images/M/1.jpg"
    assert first["width"] == 1000
    assert first["height"] == 1500
    assert first["caption"] == "Love Island USA"
    assert first["image_type"] == "publicity"
    assert first["viewer_path"] == "/title/tt8819906/mediaviewer/rm3107491329/"
    assert first["viewer_url"] == "https://www.imdb.com/title/tt8819906/mediaviewer/rm3107491329/"


def test_parse_imdb_mediaindex_graphql_payload() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "mediaindex_viewer_graphql_tt8819906_sample.html").read_text(
        encoding="utf-8"
    )

    images, page_info = parse_imdb_mediaindex_images(html, imdb_id="tt8819906")
    assert page_info["has_next_page"] is False
    assert page_info["end_cursor"] is None

    assert len(images) == 1
    row = images[0]
    assert row["source_image_id"] == "rm3107491335"
    assert row["image_type"] == "still_frame"
    assert row["position"] == 3
    assert row["caption"] == "Viewer shot"
    assert row["metadata"]["all_images"]["total"] == 1
