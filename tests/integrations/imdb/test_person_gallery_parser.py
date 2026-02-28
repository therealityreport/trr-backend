from __future__ import annotations

from pathlib import Path

from trr_backend.integrations.imdb.person_gallery import (
    parse_imdb_person_mediaindex_images,
    parse_imdb_person_mediaviewer_details,
)


def _read_fixture(name: str) -> str:
    base = Path(__file__).resolve().parents[2] / "fixtures" / "imdb"
    return (base / name).read_text(encoding="utf-8")


def test_parse_person_mediaindex_images_picks_largest_srcset() -> None:
    html = _read_fixture("person_mediaindex_nm11883948_sample.html")
    images = parse_imdb_person_mediaindex_images(html, "nm11883948")

    assert images, "Expected at least one image"
    assert len(images) == len({row["source_image_id"] for row in images})

    primary = next(row for row in images if row["viewer_id"] == "rm1679992066")
    assert primary["url"].endswith("_UX776_.jpg")
    assert primary["width"] == 776


def test_parse_person_mediaviewer_details_extracts_people_titles() -> None:
    html = _read_fixture("person_mediaviewer_nm11883948_rm1679992066_sample.html")
    details = parse_imdb_person_mediaviewer_details(html, viewer_id="rm1679992066")

    assert details["gallery_index"] == 1
    assert details["gallery_total"] == 46
    assert details["people_imdb_ids"]
    assert "nm11883948" in details["people_imdb_ids"]
    assert details["title_imdb_ids"] == ["tt36951580"]
    assert details["url"].endswith("_V1_.jpg")
    assert details["width"] == 640
    assert "Lisa Barlow" in (details["caption"] or "")


def test_parse_person_mediaviewer_details_uses_caption_links_when_sections_missing() -> None:
    html = """
    <html>
      <body>
        <span>3 of 46</span>
        <div data-testid="media-viewer">
          <img
            data-image-id="rm1679992066-curr"
            src="https://m.media-amazon.com/images/M/MV5BTEST._V1_.jpg"
          />
        </div>
        <div class="ipc-html-content-inner-div">
          <a href="/name/nm0000001/?ref_=mv_desc">Andy Cohen</a>,
          <a href="/name/nm0000002/?ref_=mv_desc">Wes O'Dell</a>,
          and <a href="/name/nm0000003/?ref_=mv_desc">Fraser Olender</a>
          in
          <a href="/title/tt1234567/?ref_=mv_desc">Fraser Olender &amp; Wes O'Dell (2022)</a>
        </div>
      </body>
    </html>
    """
    details = parse_imdb_person_mediaviewer_details(html, viewer_id="rm1679992066")

    assert details["people_imdb_ids"] == ["nm0000001", "nm0000002", "nm0000003"]
    assert details["people_names"] == ["Andy Cohen", "Wes O'Dell", "Fraser Olender"]
    assert details["title_imdb_ids"] == ["tt1234567"]
    assert details["title_names"] == ["Fraser Olender & Wes O'Dell (2022)"]


def test_parse_person_mediaviewer_details_uses_caption_text_fallback_when_links_missing() -> None:
    html = """
    <html>
      <body>
        <span>7 of 46</span>
        <div data-testid="media-viewer">
          <img
            data-image-id="rm2679992066-curr"
            src="https://m.media-amazon.com/images/M/MV5BTEST2._V1_.jpg"
            alt="Andy Cohen, Wes O'Dell, and Fraser Olender in Fraser Olender & Wes O'Dell (2022)"
          />
        </div>
      </body>
    </html>
    """
    details = parse_imdb_person_mediaviewer_details(html, viewer_id="rm2679992066")

    assert details["title_imdb_ids"] is None
    assert details["title_names"] == ["Fraser Olender & Wes O'Dell"]
    assert details["people_names"] == ["Andy Cohen", "Wes O'Dell", "Fraser Olender"]


def test_parse_person_mediaviewer_details_extracts_singular_title_section_label() -> None:
    html = """
    <html>
      <body>
        <span>2 of 12</span>
        <div data-testid="media-viewer">
          <img
            data-image-id="rm567890-curr"
            src="https://m.media-amazon.com/images/M/MV5BTEST3._V1_.jpg"
          />
        </div>
        <h3>People</h3>
        <div><a href="/name/nm0000001/?ref_=mv_desc">Andy Cohen</a></div>
        <h3>Title (1)</h3>
        <div><a href="/title/tt35051926/?ref_=mv_desc">Reunion Part 3</a></div>
      </body>
    </html>
    """

    details = parse_imdb_person_mediaviewer_details(html, viewer_id="rm567890")

    assert details["people_imdb_ids"] == ["nm0000001"]
    assert details["title_imdb_ids"] == ["tt35051926"]
    assert details["title_names"] == ["Reunion Part 3"]
