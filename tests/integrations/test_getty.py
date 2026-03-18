from __future__ import annotations

import json

from trr_backend.integrations import getty


def test_search_editorial_assets_reports_progress(monkeypatch) -> None:
    monkeypatch.setattr(
        getty,
        "_search_asset_candidates_for_phrase",
        lambda phrase, **kwargs: [
            {"detail_url": "https://www.gettyimages.com/detail/news-photo/example-one/1"},
            {"detail_url": "https://www.gettyimages.com/detail/news-photo/example-two/2"},
        ],
    )
    monkeypatch.setattr(
        getty,
        "fetch_asset_detail",
        lambda detail_url, **kwargs: {
            "detail_url": detail_url,
            "object_name": detail_url.rsplit("/", 1)[-1],
            "editorial_id": detail_url.rsplit("/", 1)[-1],
        },
    )

    progress_events: list[tuple[int, int, str]] = []
    results = getty.search_editorial_assets(
        "Lisa Barlow Bravo",
        limit=10,
        progress_cb=lambda current, total, message: progress_events.append((current, total, message)),
    )

    assert len(results) == 2
    assert progress_events[0] == (0, 2, "Getty search found 2 candidate asset pages.")
    assert progress_events[1] == (0, 2, "Fetching Getty assets 1-2/2...")
    assert progress_events[2] == (1, 2, "Fetched Getty asset 1/2: 1")
    assert progress_events[3] == (2, 2, "Fetched Getty asset 2/2: 2")


def test_search_editorial_assets_fetches_detail_urls_in_batches(monkeypatch) -> None:
    monkeypatch.setattr(
        getty,
        "_search_asset_candidates_for_phrase",
        lambda phrase, **kwargs: [
            {"detail_url": "https://www.gettyimages.com/detail/news-photo/example-one/1"},
            {"detail_url": "https://www.gettyimages.com/detail/news-photo/example-two/2"},
            {"detail_url": "https://www.gettyimages.com/detail/news-photo/example-three/3"},
        ],
    )

    fetched_urls: list[str] = []

    def _fake_fetch(detail_url: str, **kwargs):
        fetched_urls.append(detail_url)
        return {
            "detail_url": detail_url,
            "object_name": detail_url.rsplit("/", 1)[-1],
            "editorial_id": detail_url.rsplit("/", 1)[-1],
        }

    monkeypatch.setattr(getty, "fetch_asset_detail", _fake_fetch)

    results = getty.search_editorial_assets(
        "Lisa Barlow Bravo",
        limit=3,
        detail_batch_size=2,
        detail_max_workers=2,
    )

    assert [item["detail_url"] for item in results] == fetched_urls
    assert fetched_urls == [
        "https://www.gettyimages.com/detail/news-photo/example-one/1",
        "https://www.gettyimages.com/detail/news-photo/example-two/2",
        "https://www.gettyimages.com/detail/news-photo/example-three/3",
    ]


def test_search_editorial_assets_forwards_custom_query_params(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    def _fake_search(phrase: str, **kwargs):
        captured.append({"phrase": phrase, "query_params": kwargs.get("query_params")})
        return []

    monkeypatch.setattr(getty, "_search_asset_candidates_for_phrase", _fake_search)

    results = getty.search_editorial_assets(
        "Mary Cosby",
        limit=25,
        query_params={"artistexact": "bravo"},
    )

    assert results == []
    assert captured == [{"phrase": "Mary Cosby", "query_params": {"artistexact": "bravo"}}]


def test_extract_search_asset_candidates_reads_grouped_event_metadata() -> None:
    payload = {
        "searchItems": [
            {
                "landingUrl": "/detail/news-photo/example/2254325572",
                "eventName": 'UT: BRAVO\'S "The Real Housewives of Salt Lake City" - Season 6',
                "eventId": "event-1",
                "eventUrlSlug": "rhoslc-season-6",
                "title": "The Real Housewives of Salt Lake City - Season 6",
                "caption": "Reunion -- Pictured: Mary Cosby",
                "collapsedImageCount": 16,
            }
        ]
    }
    html = (
        "<html><body>"
        f'<script type="application/json" data-component="Search">{json.dumps(payload)}</script>'
        "</body></html>"
    )

    candidates = getty._extract_search_asset_candidates(html)

    assert candidates == [
        {
            "detail_url": "https://www.gettyimages.com/detail/news-photo/example/2254325572",
            "event_name": 'UT: BRAVO\'S "The Real Housewives of Salt Lake City" - Season 6',
            "event_id": "event-1",
            "event_url_slug": "rhoslc-season-6",
            "event_date": None,
            "search_title": "The Real Housewives of Salt Lake City - Season 6",
            "search_caption": "Reunion -- Pictured: Mary Cosby",
            "grouped_image_count": 16,
        }
    ]


def test_extract_search_asset_candidates_reads_prerender_search_payload_from_search_id_script() -> None:
    payload = {
        "search": {
            "gallery": {
                "assets": [
                    {
                        "landingUrl": (
                            "/detail/news-photo/reunion-pictured-mary-cosby-whitney-rose-news-photo/2254325741"
                        ),
                        "assetId": "2254325741",
                        "title": "The Real Housewives of Salt Lake City - Season 6",
                        "caption": 'THE REAL HOUSEWIVES OF SALT LAKE CITY -- "Reunion" -- Pictured: Mary Cosby',
                        "collapsedImageCount": 0,
                    }
                ]
            }
        }
    }
    html = f'<html><body><script id="Search_21955" type="application/json">{json.dumps(payload)}</script></body></html>'

    candidates = getty._extract_search_asset_candidates(html)

    assert candidates == [
        {
            "detail_url": "https://www.gettyimages.com/detail/news-photo/reunion-pictured-mary-cosby-whitney-rose-news-photo/2254325741",
            "event_name": None,
            "event_id": None,
            "event_url_slug": None,
            "event_date": None,
            "search_title": "The Real Housewives of Salt Lake City - Season 6",
            "search_caption": 'THE REAL HOUSEWIVES OF SALT LAKE CITY -- "Reunion" -- Pictured: Mary Cosby',
            "grouped_image_count": 0,
            "editorial_id": "2254325741",
        }
    ]


def test_fetch_asset_detail_exposes_preview_image_url(monkeypatch) -> None:
    detail_payload = {
        "asset": {
            "objectName": "NUP_209430_00480.jpg",
            "editorialId": "2254325572",
            "caption": "Sample caption",
            "compUrl": "https://media.gettyimages.com/comp.jpg",
            "galleryHighResCompUrl": "https://media.gettyimages.com/gallery-high.jpg",
            "thumbUrl": "https://media.gettyimages.com/thumb.jpg",
        }
    }
    html = (
        "<html><body>"
        f'<script type="application/json" data-component="AssetDetail">{json.dumps(detail_payload)}</script>'
        "</body></html>"
    )

    class _Response:
        text = html

        @staticmethod
        def raise_for_status() -> None:
            return None

    class _Session:
        @staticmethod
        def get(*args, **kwargs):
            return _Response()

    monkeypatch.setattr(getty, "_session", lambda session=None: _Session())

    detail = getty.fetch_asset_detail("https://www.gettyimages.com/detail/news-photo/example/2254325572")

    assert detail is not None
    assert detail["preview_image_url"] == "https://media.gettyimages.com/gallery-high.jpg"
    assert detail["comp_url"] == "https://media.gettyimages.com/comp.jpg"
    assert detail["thumb_url"] == "https://media.gettyimages.com/thumb.jpg"


def test_fetch_asset_detail_extracts_detail_fields_and_people_count(monkeypatch) -> None:
    detail_payload = {
        "asset": {
            "objectName": "NUP_209175_01424.JPG",
            "editorialId": "2246511561",
            "caption": "Sample caption",
            "galleryHighResCompUrl": "https://media.gettyimages.com/gallery-high.jpg",
            "keywords": [
                {"text": "BravoCon", "type": "Unknown"},
                {"text": "Two People", "type": "Unknown"},
                {"text": "Lisa Barlow", "type": "SpecificPeople"},
            ],
        }
    }
    html = (
        "<html><body>"
        f'<script type="application/json" data-component="AssetDetail">{json.dumps(detail_payload)}</script>'
        "<h2>DETAILS</h2>"
        "<span>Restrictions:</span><span>Editorial use only.</span>"
        "<span>Credit:</span><span>Bravo / Contributor</span>"
        "<span>Editorial #:</span><span>2246511561</span>"
        "<span>Collection:</span><span>NBCUniversal</span>"
        "<span>Date created:</span><span>November 16, 2025</span>"
        "<span>Upload date:</span><span>November 17, 2025</span>"
        "<span>License type:</span><span>Rights-managed</span>"
        "<span>Release info:</span><span>Not released. More information</span>"
        "<span>Source:</span><span>NBCUniversal</span>"
        "<span>Object name:</span><span>NUP_209175_01424.JPG</span>"
        "<span>Max file size:</span><span>2000 x 3000 px (6.67 x 10.00 in) - 300 dpi - 3 MB</span>"
        "</body></html>"
    )

    class _Response:
        text = html

        @staticmethod
        def raise_for_status() -> None:
            return None

    class _Session:
        @staticmethod
        def get(*args, **kwargs):
            return _Response()

    monkeypatch.setattr(getty, "_session", lambda session=None: _Session())

    detail = getty.fetch_asset_detail("https://www.gettyimages.com/detail/news-photo/example/2254325572")

    assert detail is not None
    assert detail["restrictions"] == "Editorial use only."
    assert detail["credit"] == "Bravo / Contributor"
    assert detail["max_file_size"] == "2000 x 3000 px (6.67 x 10.00 in) - 300 dpi - 3 MB"
    assert detail["keyword_texts"] == ["BravoCon", "Two People", "Lisa Barlow"]
    assert detail["people_count"] == 2


def test_search_grouped_events_requires_person_match_when_requested(monkeypatch) -> None:
    monkeypatch.setattr(
        getty,
        "_search_asset_candidates_for_phrase",
        lambda phrase, **kwargs: [
            {
                "detail_url": "https://www.gettyimages.com/editorial-images/entertainment/event/amazon-home/1",
                "event_name": "Amazon Home For The Holidays",
                "event_id": "event-1",
                "event_url_slug": "amazon-home",
                "grouped_image_count": 4,
            },
            {
                "detail_url": "https://www.gettyimages.com/editorial-images/entertainment/event/unrelated/2",
                "event_name": "Unrelated Barlow Event",
                "event_id": "event-2",
                "event_url_slug": "unrelated-barlow",
                "grouped_image_count": 3,
            },
        ],
    )
    monkeypatch.setattr(
        getty,
        "fetch_asset_detail",
        lambda detail_url, **kwargs: {
            "detail_url": detail_url,
            "editorial_id": "2246035169" if "amazon-home" in detail_url else "2",
            "object_name": "AMAZON_HOME.JPG" if "amazon-home" in detail_url else "UNRELATED.JPG",
            "caption": (
                "Lisa Barlow attends Amazon Home For The Holidays."
                if "amazon-home" in detail_url
                else "Tom Barlow attends an unrelated event."
            ),
        },
    )

    results = getty.search_grouped_events(
        "Lisa Barlow",
        limit=10,
        person_name="Lisa Barlow",
        person_match_required=True,
        source_query_scope="broad",
    )

    assert len(results) == 1
    assert results[0]["event_name"] == "Amazon Home For The Holidays"
    assert results[0]["source_query_scope"] == "broad"
    assert results[0]["matched_asset"]["editorial_id"] == "2246035169"


def test_search_grouped_events_respects_minimum_grouped_image_count(monkeypatch) -> None:
    monkeypatch.setattr(
        getty,
        "_search_asset_candidates_for_phrase",
        lambda phrase, **kwargs: [
            {
                "detail_url": "https://www.gettyimages.com/editorial-images/entertainment/event/keep/1",
                "event_name": "Keep Event",
                "event_id": "event-1",
                "event_url_slug": "keep-event",
                "grouped_image_count": 2,
            },
            {
                "detail_url": "https://www.gettyimages.com/editorial-images/entertainment/event/drop/2",
                "event_name": "Drop Event",
                "event_id": "event-2",
                "event_url_slug": "drop-event",
                "grouped_image_count": 1,
            },
        ],
    )
    monkeypatch.setattr(
        getty,
        "fetch_asset_detail",
        lambda detail_url, **kwargs: {
            "detail_url": detail_url,
            "editorial_id": detail_url.rsplit("/", 1)[-1],
            "caption": "Lisa Barlow attends the event.",
        },
    )

    results = getty.search_grouped_events(
        "Lisa Barlow",
        limit=10,
        person_name="Lisa Barlow",
        person_match_required=True,
        minimum_grouped_image_count=2,
        source_query_scope="broad",
    )

    assert [result["event_name"] for result in results] == ["Keep Event"]


def test_scan_event_page_for_person_returns_all_matching_assets(monkeypatch) -> None:
    """scan_event_page_for_person should paginate through event images
    and return only those matching the person name."""
    fake_candidates = [
        {
            "detail_url": "https://www.gettyimages.com/detail/news-photo/img-one/1",
            "event_name": "BravoCon 2023",
        },
        {
            "detail_url": "https://www.gettyimages.com/detail/news-photo/img-two/2",
            "event_name": "BravoCon 2023",
        },
        {
            "detail_url": "https://www.gettyimages.com/detail/news-photo/img-three/3",
            "event_name": "BravoCon 2023",
        },
    ]

    call_count = {"n": 0}

    def fake_search_candidates(phrase, *, limit, session=None, query_params=None):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return list(fake_candidates)
        return []

    monkeypatch.setattr(getty, "_search_asset_candidates_for_phrase", fake_search_candidates)

    def fake_fetch_detail(detail_url, *, session=None):
        asset_id = detail_url.rsplit("/", 1)[-1]
        base = {
            "detail_url": detail_url,
            "object_name": f"OBJ_{asset_id}",
            "editorial_id": asset_id,
        }
        if asset_id in ("1", "3"):
            base["caption"] = "Brandi Glanville at BravoCon"
        else:
            base["caption"] = "Andy Cohen at BravoCon"
        return base

    monkeypatch.setattr(getty, "fetch_asset_detail", fake_fetch_detail)

    results = getty.scan_event_page_for_person(
        event_url="https://www.gettyimages.com/photos/bravocon-2023?eventid=99999",
        person_name="Brandi Glanville",
    )

    assert results is not None
    assert len(results["matched_assets"]) == 2
    assert results["person_image_count"] == 2
    assert results["total_scanned"] == 3
    assert results["matched_assets"][0]["editorial_id"] == "1"
    assert results["matched_assets"][1]["editorial_id"] == "3"


def test_scan_event_page_for_person_respects_limit(monkeypatch) -> None:
    """scan_event_page_for_person should stop after scanning scan_limit assets."""
    fake_candidates = [
        {"detail_url": f"https://www.gettyimages.com/detail/news-photo/img/{i}"}
        for i in range(1, 201)
    ]

    def fake_search_candidates(phrase, *, limit, session=None, query_params=None):
        return list(fake_candidates)

    monkeypatch.setattr(getty, "_search_asset_candidates_for_phrase", fake_search_candidates)

    fetched_count = {"n": 0}

    def fake_fetch_detail(detail_url, *, session=None):
        fetched_count["n"] += 1
        return {
            "detail_url": detail_url,
            "object_name": f"OBJ_{fetched_count['n']}",
            "editorial_id": str(fetched_count["n"]),
            "caption": "Brandi Glanville at event",
        }

    monkeypatch.setattr(getty, "fetch_asset_detail", fake_fetch_detail)

    results = getty.scan_event_page_for_person(
        event_url="https://www.gettyimages.com/photos/event?eventid=1",
        person_name="Brandi Glanville",
        scan_limit=50,
    )

    assert results["total_scanned"] == 50


def test_search_grouped_events_full_scan_returns_multiple_matched_assets(monkeypatch) -> None:
    """When full_scan_person_assets=True, search_grouped_events should
    return all person-matching assets per event, not just one."""
    monkeypatch.setattr(
        getty,
        "_search_asset_candidates_for_phrase",
        lambda phrase, **kwargs: [
            {
                "detail_url": "https://www.gettyimages.com/photos/bravocon-2023?eventid=100",
                "event_name": "BravoCon 2023",
                "grouped_image_count": 50,
            }
        ],
    )

    monkeypatch.setattr(
        getty,
        "fetch_asset_detail",
        lambda detail_url, **kwargs: {
            "detail_url": detail_url,
            "object_name": "OBJ_REP",
            "editorial_id": "rep",
            "caption": "Brandi Glanville at BravoCon",
        },
    )

    monkeypatch.setattr(
        getty,
        "scan_event_page_for_person",
        lambda event_url, *, person_name, **kwargs: {
            "event_url": event_url,
            "total_scanned": 50,
            "person_image_count": 5,
            "matched_assets": [
                {"editorial_id": str(i), "object_name": f"OBJ_{i}", "caption": "Brandi Glanville"}
                for i in range(1, 6)
            ],
            "representative_asset": {"editorial_id": "1", "object_name": "OBJ_1"},
        },
    )

    results = getty.search_grouped_events(
        "Brandi Glanville Bravo",
        limit=10,
        person_name="Brandi Glanville",
        full_scan_person_assets=True,
        source_query_scope="bravo",
    )

    assert len(results) == 1
    event = results[0]
    assert event["person_image_count"] == 5
    assert len(event.get("matched_assets_list", [])) == 5
    assert event["source_query_scope"] == "bravo"


def test_extract_detail_section_fields_stops_at_tag_cloud() -> None:
    """Object name should not include tag cloud text or footer content."""
    from bs4 import BeautifulSoup

    html = """
    <html><body>
    <div>Object name:</div>
    <div>NUP_162086_1491.jpg</div>
    <div>Brandi Glanville Photos</div>
    <div>2010-2019 Photos</div>
    <div>Arguing Photos</div>
    <div>CONTENT</div>
    <div>Royalty-free</div>
    <div>Creative Video</div>
    </body></html>
    """
    soup = BeautifulSoup(html, "html.parser")
    fields = getty._extract_detail_section_fields(soup)
    object_name = fields.get("object_name_display", "")
    assert object_name == "NUP_162086_1491.jpg", f"Got polluted object_name: {object_name!r}"
