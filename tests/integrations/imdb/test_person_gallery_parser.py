from __future__ import annotations

from pathlib import Path

from trr_backend.integrations.imdb.person_gallery import (
    extract_imdb_person_mediaindex_total,
    parse_imdb_person_mediaindex_images,
    parse_imdb_person_mediaindex_payload,
    parse_imdb_person_mediaindex_state,
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
    assert details["imdb_title_id"] == "tt36951580"
    assert details["imdb_title_url"] == "https://www.imdb.com/title/tt36951580/"
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
    assert details["imdb_title_id"] == "tt1234567"
    assert details["imdb_title_url"] == "https://www.imdb.com/title/tt1234567/"
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


def test_parse_person_mediaviewer_details_extracts_image_type_from_next_data() -> None:
    html = """
    <html>
      <body>
        <script id="__NEXT_DATA__" type="application/json">
          {
            "props": {
              "pageProps": {
                "contentData": {
                  "data": {
                    "name": {
                      "images": {
                        "edges": [
                          {
                            "node": {
                              "id": "rm987654321",
                              "type": "still_frame",
                              "url": "https://m.media-amazon.com/images/M/MV5BTYPE._V1_.jpg"
                            }
                          }
                        ]
                      }
                    }
                  }
                }
              }
            }
          }
        </script>
        <div data-testid="media-viewer">
          <img
            data-image-id="rm987654321-curr"
            src="https://m.media-amazon.com/images/M/MV5BTYPE._V1_.jpg"
          />
        </div>
      </body>
    </html>
    """

    details = parse_imdb_person_mediaviewer_details(html, viewer_id="rm987654321")
    assert details["image_type"] == "still_frame"


def test_extract_imdb_person_mediaindex_total_from_next_data() -> None:
    html = """
    <html>
      <body>
        <script id="__NEXT_DATA__" type="application/json">
          {"props":{"pageProps":{"contentData":{"data":{"name":{"all_images":{"total":603}}}}}}}
        </script>
      </body>
    </html>
    """
    assert extract_imdb_person_mediaindex_total(html) == 603


def test_extract_imdb_person_mediaindex_total_from_text_fallback() -> None:
    html = "<html><body><span>1-50 of 603</span></body></html>"
    assert extract_imdb_person_mediaindex_total(html) == 603


def test_parse_imdb_person_mediaindex_state_reads_next_data_pagination() -> None:
    html = """
    <html>
      <body>
        <script id="__NEXT_DATA__" type="application/json">
          {
            "props": {
              "pageProps": {
                "contentData": {
                  "data": {
                    "name": {
                      "all_images": {
                        "total": 603,
                        "pageInfo": {"hasNextPage": true, "endCursor": "CURSOR_1"},
                        "edges": [
                          {
                            "position": 1,
                            "node": {
                              "id": "rm1234567890",
                              "url": "https://m.media-amazon.com/images/M/MV5BTEST123._V1_.jpg",
                              "width": 1920,
                              "height": 1080,
                              "imageType": "event",
                              "caption": {"plainText": "Sample caption"}
                            }
                          }
                        ]
                      }
                    }
                  }
                }
              }
            }
          }
        </script>
      </body>
    </html>
    """
    images, page_info = parse_imdb_person_mediaindex_state(html, "nm1234567")
    assert len(images) == 1
    assert images[0]["viewer_id"] == "rm1234567890"
    assert images[0]["source_image_id"] == "MV5BTEST123"
    assert images[0]["image_type"] == "event"
    assert page_info["has_next_page"] is True
    assert page_info["end_cursor"] == "CURSOR_1"
    assert page_info["total"] == 603


def test_parse_imdb_person_mediaindex_payload_reads_graphql_shape() -> None:
    payload = {
        "data": {
            "name": {
                "all_images": {
                    "total": 120,
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "edges": [
                        {
                            "position": 51,
                            "node": {
                                "id": "rm111222333",
                                "url": "https://m.media-amazon.com/images/M/MV5BPAGED._V1_.jpg",
                                "width": 1600,
                                "height": 900,
                                "imageType": "still_frame",
                            },
                        }
                    ],
                }
            }
        }
    }
    images, page_info = parse_imdb_person_mediaindex_payload(payload, "nm7654321")
    assert len(images) == 1
    assert images[0]["viewer_id"] == "rm111222333"
    assert images[0]["image_type"] == "still_frame"
    assert images[0]["mediaviewer_url_path"] == "/name/nm7654321/mediaviewer/rm111222333/"
    assert page_info == {"has_next_page": False, "end_cursor": None, "total": 120}
