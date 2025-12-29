from __future__ import annotations

from pathlib import Path

from trr_backend.integrations.imdb.title_page_metadata import parse_imdb_title_html


def test_parse_imdb_title_page_metadata_from_fixture() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "title_page_tt8819906_sample.html").read_text(
        encoding="utf-8"
    )

    result = parse_imdb_title_html(html, imdb_id="tt8819906")

    assert result["title"] == "Love Island USA"
    assert (
        result["description"]
        == "U.S. version of the British show 'Love Island' where a group of singles come to stay in a villa for a few weeks and have to couple up with one another."
    )
    assert "Reality TV" in result["tags"]
    assert "Reality TV Dating" in result["tags"]
    assert "Reality-TV" in result["genres"]
    assert "Game-Show" in result["genres"]
    assert result["content_rating"] == "TV-MA"
    assert result["aggregate_rating_value"] == 5.2
    assert result["aggregate_rating_count"] == 3291
