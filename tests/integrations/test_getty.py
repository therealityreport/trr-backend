from __future__ import annotations

import json

from trr_backend.integrations import getty


def test_getty_search_page_cap_defaults_to_none() -> None:
    assert getty.MAX_SEARCH_PAGES is None


def test_search_asset_candidates_for_phrase_runs_until_natural_exhaustion_when_uncapped(monkeypatch) -> None:
    page_one_assets = [
        {"landingUrl": f"/detail/news-photo/example-{idx}/{idx}", "assetId": str(idx)}
        for idx in range(1, getty.DEFAULT_SEARCH_PAGE_SIZE + 1)
    ]
    responses = {
        1: (
            "<html><body>"
            '<script type="application/json" data-component="Search">'
            f"{json.dumps({'searchItems': page_one_assets})}"
            "</script>"
            "</body></html>"
        ),
        2: (
            "<html><body>"
            '<script type="application/json" data-component="Search">'
            '{"searchItems":[{"landingUrl":"/detail/news-photo/example-61/61","assetId":"61"}]}'
            "</script>"
            "</body></html>"
        ),
    }

    class _Response:
        def __init__(self, page: int) -> None:
            self.status_code = 200
            self.url = f"https://www.gettyimages.com/search/2/image?page={page}"
            self.text = responses.get(page, "<html><body></body></html>")

        def raise_for_status(self) -> None:
            return None

    class _Session:
        def get(self, url: str, **kwargs):
            if "page=3" in url:
                return _Response(3)
            if "page=2" in url:
                return _Response(2)
            return _Response(1)

    monkeypatch.setattr(getty, "_session", lambda session=None: _Session())

    candidates = getty._search_asset_candidates_for_phrase(
        "Lisa Barlow",
        limit=None,
        max_search_pages=None,
    )

    assert len(candidates) == getty.DEFAULT_SEARCH_PAGE_SIZE + 1
    assert candidates[0]["editorial_id"] == "1"
    assert candidates[-1]["editorial_id"] == "61"


def test_search_asset_candidates_records_query_summary_and_detects_page_rewrite(monkeypatch) -> None:
    def _page_html(page: int) -> str:
        payload = {
            "searchItems": [
                {"landingUrl": f"/detail/news-photo/example-{page}-{idx}/{page}{idx}", "assetId": f"{page}{idx}"}
                for idx in range(1, getty.DEFAULT_SEARCH_PAGE_SIZE + 1)
            ]
        }
        current_page = 1 if page >= 4 else page
        return (
            "<html><body>"
            '<input aria-label="Pagination page number input" value="'
            f"{current_page}"
            '"/>'
            '<script id="Search_535122" type="text/javascript">'
            f'window.remotes["search"]["search"].state="%7B%22queries%22%3A%5B%7B%22state%22%3A%7B%22data%22%3A%7B%22pageSize%22%3A60%2C%22lastPage%22%3A81%2C%22totalNumberOfResults%22%3A4823%7D%7D%7D%5D%7D";'
            "</script>"
            "<div>View 62 videos</div><div>340 Events</div><div>4,823 Images</div>"
            f'<script type="application/json" data-component="Search">{json.dumps(payload)}</script>'
            "</body></html>"
        )

    class _Response:
        def __init__(self, page: int) -> None:
            self.status_code = 200
            self.url = (
                "https://www.gettyimages.com/search/2/image?family=editorial&page=1"
                if page >= 4
                else f"https://www.gettyimages.com/search/2/image?family=editorial&page={page}"
            )
            self.text = _page_html(page if page < 4 else 1)

        def raise_for_status(self) -> None:
            return None

    class _Session:
        def get(self, url: str, **kwargs):
            if "page=4" in url:
                return _Response(4)
            if "page=3" in url:
                return _Response(3)
            if "page=2" in url:
                return _Response(2)
            return _Response(1)

    summary: dict[str, object] = {}
    monkeypatch.setattr(getty, "_session", lambda session=None: _Session())

    candidates = getty._search_asset_candidates_for_phrase(
        "Brandi Glanville",
        limit=None,
        query_params={"sort": "newest"},
        query_summary_out=summary,
    )

    assert len(candidates) == getty.DEFAULT_SEARCH_PAGE_SIZE * 3
    assert summary["query_url"] == getty._build_search_url("Brandi Glanville", query_params={"sort": "newest"})
    assert summary["site_image_total"] == 4823
    assert summary["site_event_total"] == 340
    assert summary["site_video_total"] == 62
    assert summary["pagination_rewrite_detected"] is True
    assert summary["termination_reason"] == "pagination_rewrite"
    assert summary["expected_page"] == 4
    assert summary["current_page"] == 1
    assert summary["response_url"] == "https://www.gettyimages.com/search/2/image?family=editorial&page=1"
    assert summary["first_editorial_ids"][:3] == ["11", "12", "13"]


def test_search_asset_candidates_supports_browser_backed_page_fetcher() -> None:
    def _page_html(page: int) -> str:
        payload = {
            "searchItems": [
                {"landingUrl": f"/detail/news-photo/example-{page}-{idx}/{page}{idx}", "assetId": f"{page}{idx}"}
                for idx in range(1, getty.DEFAULT_SEARCH_PAGE_SIZE + 1)
            ]
        }
        return (
            "<html><body>"
            '<input aria-label="Pagination page number input" value="'
            f"{page}"
            '"/>'
            '<script id="Search_535122" type="text/javascript">'
            f'window.remotes["search"]["search"].state="%7B%22queries%22%3A%5B%7B%22state%22%3A%7B%22data%22%3A%7B%22pageSize%22%3A60%2C%22lastPage%22%3A81%2C%22totalNumberOfResults%22%3A4823%7D%7D%7D%5D%7D";'
            "</script>"
            "<div>View 62 videos</div><div>340 Events</div><div>4,823 Images</div>"
            f'<script type="application/json" data-component="Search">{json.dumps(payload)}</script>'
            "</body></html>"
        )

    visited_urls: list[str] = []

    def _fetch(url: str) -> tuple[str, str | None, int | None]:
        visited_urls.append(url)
        if "page=4" in url:
            return "<html><body></body></html>", url, 200
        if "page=3" in url:
            return _page_html(3), url, 200
        if "page=2" in url:
            return _page_html(2), url, 200
        return _page_html(1), url, 200

    summary: dict[str, object] = {}
    candidates = getty._search_asset_candidates_for_phrase(
        "Brandi Glanville",
        limit=None,
        query_params={"sort": "newest"},
        query_summary_out=summary,
        search_page_fetcher=_fetch,
    )

    assert len(candidates) == getty.DEFAULT_SEARCH_PAGE_SIZE * 3
    assert any("page=3" in url for url in visited_urls)
    assert any("page=4" in url for url in visited_urls)
    assert summary["pagination_rewrite_detected"] is False
    assert summary["termination_reason"] == "natural_exhaustion"


def test_search_asset_candidates_supports_dict_page_fetcher_results() -> None:
    def _page_html(page: int) -> str:
        payload = {
            "searchItems": [
                {"landingUrl": f"/detail/news-photo/example-{page}-{idx}/{page}{idx}", "assetId": f"{page}{idx}"}
                for idx in range(1, getty.DEFAULT_SEARCH_PAGE_SIZE + 1)
            ]
        }
        return (
            "<html><body>"
            '<input aria-label="Pagination page number input" value="'
            f"{page}"
            '"/>'
            f'<script type="application/json" data-component="Search">{json.dumps(payload)}</script>'
            "</body></html>"
        )

    def _fetch(url: str):
        if "page=2" in url:
            return {
                "html": _page_html(2),
                "response_url": url,
                "status_code": 200,
                "current_page": 2,
                "first_editorial_ids": ["21", "22", "23"],
                "page_signature": "21|22|23",
            }
        return {
            "html": _page_html(1),
            "response_url": url,
            "status_code": 200,
            "current_page": 1,
            "first_editorial_ids": ["11", "12", "13"],
            "page_signature": "11|12|13",
        }

    summary: dict[str, object] = {}
    candidates = getty._search_asset_candidates_for_phrase(
        "Brandi Glanville",
        limit=120,
        query_params={"sort": "newest"},
        query_summary_out=summary,
        search_page_fetcher=_fetch,
    )

    assert len(candidates) == 120
    assert summary["termination_reason"] == "limit_reached"


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


def test_search_editorial_assets_discovery_mode_skips_detail_and_grouped_event_enrichment(monkeypatch) -> None:
    raw_candidates = [
        {
            "detail_url": "https://www.gettyimages.com/detail/news-photo/example-one/1",
            "editorial_id": "1",
            "object_name": "example-one",
        },
        {
            "detail_url": "https://www.gettyimages.com/detail/news-photo/example-two/2",
            "editorial_id": "2",
            "object_name": "example-two",
        },
    ]
    monkeypatch.setattr(
        getty,
        "_search_asset_candidates_for_phrase",
        lambda phrase, **kwargs: list(raw_candidates),
    )

    def _unexpected_detail_fetch(*args, **kwargs):
        raise AssertionError("fetch_asset_detail should not run in discovery mode")

    def _unexpected_grouped_merge(*args, **kwargs):
        raise AssertionError("_merge_grouped_event_metadata should not run in discovery mode")

    monkeypatch.setattr(getty, "fetch_asset_detail", _unexpected_detail_fetch)
    monkeypatch.setattr(getty, "_merge_grouped_event_metadata", _unexpected_grouped_merge)

    results = getty.search_editorial_assets(
        "Brandi Glanville",
        include_details=False,
    )

    assert results == raw_candidates


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


def test_search_editorial_assets_marks_challenge_pages_unavailable(monkeypatch) -> None:
    class _Response:
        status_code = 200
        url = "https://www.gettyimages.com/search/2/image"
        text = "<html><body>Verify you are human</body></html>"

        @staticmethod
        def raise_for_status() -> None:
            return None

    class _Session:
        @staticmethod
        def get(*args, **kwargs):
            return _Response()

    diagnostics: dict[str, object] = {}
    monkeypatch.setattr(getty, "_session", lambda session=None: _Session())

    results = getty.search_editorial_assets("Brandi Glanville", diagnostics_out=diagnostics)

    assert results == []
    assert diagnostics["status"] == "unavailable"
    assert diagnostics["failure_stage"] == "search"
    assert diagnostics["unavailable_reason"] == "challenge_page"
    assert diagnostics["page_classification"] == "challenge_page"


def test_search_editorial_assets_marks_detail_failures_degraded(monkeypatch) -> None:
    monkeypatch.setattr(
        getty,
        "_search_asset_candidates_for_phrase",
        lambda phrase, **kwargs: [
            {"detail_url": "https://www.gettyimages.com/detail/news-photo/example-one/1"},
        ],
    )
    monkeypatch.setattr(getty, "fetch_asset_detail", lambda detail_url, **kwargs: None)

    diagnostics: dict[str, object] = {}

    results = getty.search_editorial_assets("Brandi Glanville", diagnostics_out=diagnostics)

    assert results == []
    assert diagnostics["status"] == "degraded"
    assert diagnostics["failure_stage"] == "detail"
    assert diagnostics["unavailable_reason"] == "detail_fetch_failed"


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


def test_fetch_asset_detail_exposes_original_and_preview_image_urls(monkeypatch) -> None:
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
    assert detail["original_image_url"] == "https://media.gettyimages.com/gallery-high.jpg"
    assert detail["preview_image_url"] == "https://media.gettyimages.com/comp.jpg"
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


def test_infer_people_count_supports_one_person_keyword() -> None:
    assert getty._infer_people_count(["One Person"]) == 1


def test_describe_asset_person_match_prefers_people_overlay_over_noisy_tags() -> None:
    details = getty.describe_asset_person_match(
        {
            "search_people_overlay_names": ["Brandi Glanville"],
            "people": [{"text": "Andy Cohen"}],
            "caption": "Watch What Happens Live With Andy Cohen -- Episode 1501",
        },
        "Brandi Glanville",
    )

    assert details["matched"] is True
    assert details["reason"] == "solo_overlay"
    assert details["matched_name"] == "Brandi Glanville"


def test_describe_asset_person_match_allows_single_letter_wwhl_caption_typo() -> None:
    details = getty.describe_asset_person_match(
        {
            "caption": (
                "Watch What Happens Live With Andy Cohen - Season 19 "
                "WATCH WHAT HAPPENS LIVE WITH ANDY COHEN -- Episode 19109 -- "
                "Pictured: Jill Zarin, Brandy Glanville"
            ),
        },
        "Brandi Glanville",
    )

    assert details["matched"] is True
    assert details["reason"] == "caption_typo"
    assert details["matched_name"] == "Brandy Glanville"


def test_describe_asset_person_match_rejects_known_false_positive_event() -> None:
    details = getty.describe_asset_person_match(
        {
            "event_name": "CA: Hilary Roberts Birthday Celebration And Red Songbird Foundation Launch Party",
            "title": "6 Brandi Glanville Photos and High-Res Pictures",
            "people": [{"text": "Brandi Glanville"}],
        },
        "Brandi Glanville",
    )

    assert details["matched"] is False
    assert details["reason"] == "known_exception"
    assert details["deny_reason"] == "hilary_roberts_event_false_positive"


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
    fake_candidates = [{"detail_url": f"https://www.gettyimages.com/detail/news-photo/img/{i}"} for i in range(1, 201)]

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
                {"editorial_id": str(i), "object_name": f"OBJ_{i}", "caption": "Brandi Glanville"} for i in range(1, 6)
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


def test_fetch_asset_detail_prefers_largest_image_url(monkeypatch) -> None:
    """original_image_url should prefer downloadableCompUrl over smaller preview variants."""
    from unittest.mock import MagicMock

    asset_json = {
        "thumbUrl": "https://media.gettyimages.com/id/123/thumb.jpg?s=170x170",
        "compUrl": "https://media.gettyimages.com/id/123/comp.jpg?s=594x594",
        "downloadableCompUrl": "https://media.gettyimages.com/id/123/download.jpg?s=2048x2048",
        "title": "Test Image",
        "id": "123",
    }
    html = f'<html><script data-component="AssetDetail">{json.dumps({"asset": asset_json})}</script></html>'
    mock_response = MagicMock()
    mock_response.text = html
    mock_response.raise_for_status = MagicMock()

    def mock_get(url, **kwargs):
        return mock_response

    monkeypatch.setattr(getty, "_session", lambda s=None: MagicMock(get=mock_get))

    result = getty.fetch_asset_detail("https://www.gettyimages.com/detail/news-photo/test/123")
    assert result is not None
    assert "2048x2048" in result["original_image_url"], f"Expected largest URL, got: {result['original_image_url']}"
    assert "594x594" in result["preview_image_url"], f"Expected preview URL, got: {result['preview_image_url']}"


def test_extract_best_image_urls_from_display_sizes() -> None:
    """_extract_best_image_urls should preserve preview-only URLs without promoting them to original."""
    asset_json = {
        "thumbUrl": "https://media.gettyimages.com/thumb.jpg",
        "displaySizes": [
            {"name": "high_res_comp", "uri": "https://media.gettyimages.com/highres.jpg"},
            {"name": "comp", "uri": "https://media.gettyimages.com/comp.jpg"},
            {"name": "preview", "uri": "https://media.gettyimages.com/preview.jpg"},
        ],
    }
    urls = getty._extract_best_image_urls(asset_json)
    assert urls["highResCompUrl"] == "https://media.gettyimages.com/highres.jpg"
    assert urls["compUrl"] == "https://media.gettyimages.com/comp.jpg"
    assert urls["previewUrl"] == "https://media.gettyimages.com/preview.jpg"
    assert urls["thumbUrl"] == "https://media.gettyimages.com/thumb.jpg"


def test_fetch_asset_detail_prefers_high_res_url_over_tiny_getty_comp(monkeypatch) -> None:
    detail_payload = {
        "asset": {
            "objectName": "NUP_200213_00303.JPG",
            "editorialId": "1246182942",
            "caption": "Watch What Happens Live sample caption",
            "downloadableCompUrl": (
                "https://media.gettyimages.com/id/1246182942/photo/"
                "watch-what-happens-live-with-andy-cohen-season-20.jpg?p=1&s=594x594&w=gi&k=small"
            ),
            "galleryHighResCompUrl": (
                "https://media.gettyimages.com/id/1246182942/photo/"
                "watch-what-happens-live-with-andy-cohen-season-20.jpg?p=1&w=gi&k=large"
            ),
            "compUrl": (
                "https://media.gettyimages.com/id/1246182942/photo/"
                "watch-what-happens-live-with-andy-cohen-season-20.jpg?p=1&s=594x594&w=gi&k=small"
            ),
        }
    }
    html = (
        "<html><body>"
        f'<script type="application/json" data-component="AssetDetail">{json.dumps(detail_payload)}</script>'
        "<span>Max file size:</span><span>3000 x 2000 px (10.00 x 6.67 in) - 300 dpi - 2 MB</span>"
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

    detail = getty.fetch_asset_detail("https://www.gettyimages.com/detail/news-photo/example/1246182942")

    assert detail is not None
    assert detail["original_image_url"] == detail_payload["asset"]["galleryHighResCompUrl"]
    assert detail["preview_image_url"] == detail_payload["asset"]["compUrl"]


def test_fetch_asset_detail_prefers_large_main_image_over_downloadable_preview(monkeypatch) -> None:
    detail_payload = {
        "asset": {
            "objectName": "BRAVOCON_1435767826.JPG",
            "editorialId": "1435767826",
            "caption": "Legends Ball - 2022 BravoCon",
            "downloadableCompUrl": (
                "https://media.gettyimages.com/id/1435767826/photo/"
                "legends-ball-2022-bravocon.jpg?p=1&s=594x594&w=gi&k=preview"
            ),
            "largeMainImageURL": (
                "https://media.gettyimages.com/id/1435767826/photo/"
                "legends-ball-2022-bravocon.jpg?s=2048x2048&w=gi&k=20&c=full"
            ),
            "deliveryUrls": {
                "HighResComp": (
                    "https://media.gettyimages.com/id/1435767826/photo/"
                    "legends-ball-2022-bravocon.jpg?s=2048x2048&w=gi&k=20&c=delivery"
                )
            },
            "galleryComp1024Url": (
                "https://media.gettyimages.com/id/1435767826/photo/"
                "legends-ball-2022-bravocon.jpg?s=1024x1024&w=gi&k=20&c=mid"
            ),
            "compUrl": (
                "https://media.gettyimages.com/id/1435767826/photo/"
                "legends-ball-2022-bravocon.jpg?s=594x594&w=0&k=20&c=small"
            ),
        }
    }
    html = (
        "<html><body>"
        f'<script type="application/json" data-component="AssetDetail">{json.dumps(detail_payload)}</script>'
        "<span>Max file size:</span><span>2000 x 3000 px (6.67 x 10.00 in) - 300 dpi - 4 MB</span>"
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

    detail = getty.fetch_asset_detail("https://www.gettyimages.com/detail/news-photo/example/1435767826")

    assert detail is not None
    assert detail["original_image_url"] == detail_payload["asset"]["deliveryUrls"]["HighResComp"]
    assert detail["preview_image_url"] == detail_payload["asset"]["galleryComp1024Url"]


def test_search_grouped_events_passes_numberofpeople_query_param(monkeypatch) -> None:
    """query_params like numberofpeople should flow through to the search URL."""
    captured_urls: list[str] = []

    def fake_search_candidates(phrase, *, limit, session=None, query_params=None, **kwargs):
        url = getty._build_search_url(phrase, query_params=query_params)
        captured_urls.append(url)
        return []

    monkeypatch.setattr(getty, "_search_asset_candidates_for_phrase", fake_search_candidates)

    getty.search_grouped_events(
        "Brandi Glanville",
        limit=10,
        query_params={"numberofpeople": "one", "sort": "best"},
        source_query_scope="broad",
    )

    assert len(captured_urls) == 1
    assert "numberofpeople=one" in captured_urls[0]
    assert "groupbyevent=true" in captured_urls[0]


# ---------------------------------------------------------------------------
# build_query_plan tests
# ---------------------------------------------------------------------------


def test_build_query_plan_minimal_without_credit_rows() -> None:
    """Without credit data the plan produces bravo + broad only."""
    plan = getty.build_query_plan("Lisa Barlow")
    assert len(plan) == 2
    assert plan[0]["phrase"] == "Lisa Barlow Bravo"
    assert plan[0].get("label") == "Bravo Search"
    assert plan[1]["phrase"] == "Lisa Barlow"
    assert plan[1].get("label") == "Broad Search"


def test_build_query_plan_with_bravo_credit() -> None:
    """With a Bravo credit the plan includes Bravo-primary + broad + credit terms."""
    credit_rows = [
        {"networks": ["Bravo"], "streaming_providers": ["Peacock"]},
    ]
    plan = getty.build_query_plan("Lisa Barlow", credit_show_rows=credit_rows)
    phrases = [entry["phrase"] for entry in plan]
    assert phrases[0] == "Lisa Barlow Bravo"
    assert "Lisa Barlow" in phrases
    assert "Lisa Barlow Peacock" in phrases


def test_build_query_plan_nbc_family_adds_collection_variant() -> None:
    """NBC-family terms produce an extra collections=nbc query."""
    credit_rows = [
        {"networks": ["USA Network"], "streaming_providers": []},
    ]
    plan = getty.build_query_plan("Kyle Richards", credit_show_rows=credit_rows)
    nbc_variants = [entry for entry in plan if entry.get("query_params", {}).get("collections") == "nbc"]
    assert len(nbc_variants) >= 1
    assert any("USA Network" in entry["phrase"] for entry in nbc_variants)


def test_build_query_plan_deduplicates_phrases() -> None:
    """Duplicate phrase+params signatures are suppressed."""
    credit_rows = [
        {"networks": ["Bravo", "bravo"], "streaming_providers": []},
    ]
    plan = getty.build_query_plan("Teresa Giudice", credit_show_rows=credit_rows)
    # Each (phrase, params) signature should be unique
    signatures = [
        (
            entry["phrase"].casefold(),
            tuple(sorted(entry.get("query_params", {}).items())),
        )
        for entry in plan
    ]
    assert len(signatures) == len(set(signatures))
    # "bravo" (lowercase dup) should not generate an additional plain query
    plain_bravo_count = sum(
        1
        for entry in plan
        if entry["phrase"].casefold() == "teresa giudice bravo"
        and not entry.get("query_params")
    )
    assert plain_bravo_count == 1


def test_build_query_plan_empty_person_returns_empty() -> None:
    assert getty.build_query_plan("") == []
    assert getty.build_query_plan("  ") == []


def test_normalize_query_term() -> None:
    assert getty.normalize_query_term("  Lisa   Barlow  ") == "Lisa Barlow"
    assert getty.normalize_query_term(None) == ""


def test_is_nbc_family_term() -> None:
    assert getty.is_nbc_family_term("Bravo") is True
    assert getty.is_nbc_family_term("peacock") is True
    assert getty.is_nbc_family_term("HBO") is False
    assert getty.is_nbc_family_term("") is False


def test_search_editorial_assets_skip_grouped_merge(monkeypatch) -> None:
    """skip_grouped_merge=True prevents the redundant grouped re-search."""
    grouped_search_calls: list[str] = []

    def _fake_search_candidates(phrase, *, limit, session=None, query_params=None, **kwargs):
        return [{"detail_url": "https://example.com/detail/1", "editorial_id": "1"}]

    def _fake_merge_grouped(phrase, candidates, **kwargs):
        grouped_search_calls.append(phrase)
        return candidates

    def _fake_fetch_detail(url, **kwargs):
        return {"editorial_id": "1", "object_name": "Test"}

    monkeypatch.setattr(getty, "_search_asset_candidates_for_phrase", _fake_search_candidates)
    monkeypatch.setattr(getty, "_merge_grouped_event_metadata", _fake_merge_grouped)
    monkeypatch.setattr(getty, "fetch_asset_detail", _fake_fetch_detail)

    # With skip_grouped_merge=True, _merge_grouped_event_metadata should not be called
    getty.search_editorial_assets("Test Person", limit=10, skip_grouped_merge=True)
    assert grouped_search_calls == []

    # Without skip, it should be called
    getty.search_editorial_assets("Test Person", limit=10, skip_grouped_merge=False)
    assert grouped_search_calls == ["Test Person"]
