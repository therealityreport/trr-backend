from __future__ import annotations

from trr_backend.integrations import bravo_jsonapi


def test_fetch_show_galleries_normalizes_media_and_file_records(monkeypatch):
    detail_payload = {
        "data": {
            "id": "gallery-uuid",
            "relationships": {
                "field_media_items": {
                    "data": [
                        {"type": "media--image", "id": "media-1"},
                        {"type": "media--image", "id": "media-2"},
                    ]
                }
            },
        },
        "included": [
            {
                "type": "media--image",
                "id": "media-1",
                "attributes": {
                    "drupal_internal__mid": 101,
                    "name": "watch-what-happens-live-season-15-gallery-15040-01.jpg",
                    "field_caption": {"value": "<p>Brandi gets lifted by Adam Rippon and Andy Cohen!</p>"},
                    "field_credit": None,
                    "field_image_description": None,
                },
                "relationships": {
                    "field_media_image": {
                        "data": {
                            "type": "file--file",
                            "id": "file-1",
                            "meta": {
                                "alt": "Lift shot",
                                "width": 1825,
                                "height": 1217,
                            },
                        }
                    }
                },
            },
            {
                "type": "media--image",
                "id": "media-2",
                "attributes": {
                    "drupal_internal__mid": 102,
                    "name": "gettyimages-928663262.jpg",
                    "field_caption": {"value": "<p>Brandi looks lovely in a red dress.</p>"},
                    "field_credit": {"value": "<p>Bravo</p>"},
                    "field_image_description": None,
                },
                "relationships": {
                    "field_media_image": {
                        "data": {
                            "type": "file--file",
                            "id": "file-2",
                            "meta": {
                                "alt": "Red dress",
                                "width": 1217,
                                "height": 1825,
                            },
                        }
                    }
                },
            },
            {
                "type": "file--file",
                "id": "file-1",
                "attributes": {
                    "filename": "watch-what-happens-live-season-15-gallery-15040-01.jpg",
                    "filemime": "image/jpeg",
                    "filesize": 12345,
                    "uri": {"url": "/sites/bravo/files/gallery-01.jpg"},
                },
            },
            {
                "type": "file--file",
                "id": "file-2",
                "attributes": {
                    "filename": "gettyimages-928663262.jpg",
                    "filemime": "image/jpeg",
                    "filesize": 23456,
                    "uri": {"url": "/sites/bravo/files/gallery-02.jpg"},
                },
            },
        ],
    }

    def fake_get_json(client, url, params=None):  # noqa: ARG001
        return detail_payload

    def fake_get_html(client, url):  # noqa: ARG001
        return """
        <script data-drupal-selector="drupal-settings-json">
        {"ls_adobe_analytics":{
          "showSite":"Watch What Happens Live with Andy Cohen",
          "season":"Season 15",
          "publishedDate":"2018-03-07",
          "people":"Brandi Glanville, Adam Rippon"
        }}
        </script>
        """

    monkeypatch.setattr(bravo_jsonapi, "_get_json", fake_get_json)
    monkeypatch.setattr(bravo_jsonapi, "_get_html", fake_get_html)

    assets = bravo_jsonapi.fetch_gallery_assets(
        {"uuid": "gallery-uuid", "title": "Brandi Glanville & Adam Rippon", "nid": 15040, "path": "/gallery/path"}
    )

    assert len(assets) == 2
    assert assets[0]["gallery_nid"] == 15040
    assert assets[0]["season_number"] == 15
    assert assets[0]["file_url"] == "https://www.bravotv.com/sites/bravo/files/gallery-01.jpg"
    assert assets[0]["field_caption"]["value"] == "<p>Brandi gets lifted by Adam Rippon and Andy Cohen!</p>"
    assert assets[1]["file_name"] == "gettyimages-928663262.jpg"
    assert assets[1]["field_credit"]["value"] == "<p>Bravo</p>"
