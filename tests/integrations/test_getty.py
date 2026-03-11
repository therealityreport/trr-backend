from __future__ import annotations

from trr_backend.integrations import getty


def test_search_editorial_assets_reports_progress(monkeypatch) -> None:
    monkeypatch.setattr(
        getty,
        "_search_detail_urls_for_phrase",
        lambda phrase, **kwargs: [
            "https://www.gettyimages.com/detail/news-photo/example-one/1",
            "https://www.gettyimages.com/detail/news-photo/example-two/2",
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
        "_search_detail_urls_for_phrase",
        lambda phrase, **kwargs: [
            "https://www.gettyimages.com/detail/news-photo/example-one/1",
            "https://www.gettyimages.com/detail/news-photo/example-two/2",
            "https://www.gettyimages.com/detail/news-photo/example-three/3",
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
