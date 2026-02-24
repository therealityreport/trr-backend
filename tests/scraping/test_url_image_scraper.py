from __future__ import annotations

from pathlib import Path

from trr_backend.scraping.url_image_scraper import ImageCandidate, extract_images_from_html, scrape_url_for_images


def _read_fixture(name: str) -> str:
    base = Path(__file__).resolve().parents[1] / "fixtures" / "scraping"
    return (base / name).read_text(encoding="utf-8")


def test_extracts_pinterest_media_and_skips_icons() -> None:
    html = _read_fixture("eonline_pinterest_sample.html")
    base_url = "https://www.eonline.com/photos/34966/sample"

    candidates = extract_images_from_html(html, base_url, min_width=200, limit=50)
    urls = [candidate.best_url for candidate in candidates]

    assert "https://akns-images.eonline.com/eol_images/Entire_Site/2022730/rs_634x707-220830160858-rhoslc6.jpg" in urls
    assert "https://akns-images.eonline.com/eol_images/Entire_Site/2022730/rs_634x707-220830160858-rhoslc.jpg" in urls
    assert "https://akns-images.eonline.com/eol_images/Entire_Site/2022730/rs_634x707-220830160857-rhoslc3.jpg" in urls

    assert "https://assets.pinterest.com/images/pidgets/pinit_fg_en_round_red_32.png" not in urls

    alt_texts = [candidate.alt_text for candidate in candidates if candidate.alt_text]
    assert any("Jen Shah" in text for text in alt_texts)
    assert any("Lisa Barlow" in text for text in alt_texts)


def test_scrape_populates_candidate_bytes_from_head(monkeypatch) -> None:
    import trr_backend.scraping.url_image_scraper as scraper

    monkeypatch.setenv("SCRAPE_PREVIEW_HEAD_MAX", "5")
    monkeypatch.setenv("SCRAPE_PREVIEW_HEAD_TIMEOUT_S", "0.1")
    monkeypatch.setenv("SCRAPE_PREVIEW_HEAD_WORKERS", "2")

    monkeypatch.setattr(scraper, "fetch_page_html", lambda url: ("<html></html>", "Example"))
    monkeypatch.setattr(
        scraper,
        "extract_images_from_html",
        lambda html, base_url, min_width=200, limit=50: [
            ImageCandidate(
                id="1",
                original_url="https://example.com/img.jpg",
                best_url="https://example.com/img.jpg",
                width=800,
                height=600,
            )
        ],
    )

    class FakeHeadResponse:
        status_code = 200
        headers = {"Content-Length": "1234"}

    monkeypatch.setattr(scraper.requests, "head", lambda *args, **kwargs: FakeHeadResponse())

    result = scrape_url_for_images("https://example.com/page")
    assert result.error is None
    assert result.total_found == 1
    assert result.images[0].bytes == 1234


def test_direct_image_sets_bytes_from_head(monkeypatch) -> None:
    import trr_backend.scraping.url_image_scraper as scraper

    class FakeHeadResponse:
        status_code = 200
        headers = {"Content-Type": "image/jpeg", "Content-Length": "999"}

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(scraper.requests, "head", lambda *args, **kwargs: FakeHeadResponse())

    result = scrape_url_for_images("https://example.com/direct.jpg")
    assert result.error is None
    assert result.total_found == 1
    assert result.images[0].bytes == 999


def test_extracts_heading_and_bio_context_for_article_images() -> None:
    html = """
    <div class="blog-post__body">
      <h2>Golnesa Gharachedaghi's official bio for The Valley: Persian Style Season 1</h2>
      <div class="embedded-entity image">
        <div class="media__image">
          <img src="https://www.bravotv.com/sites/bravo/files/2025/12/golnesa.jpg" alt="Golnesa portrait" />
        </div>
      </div>
      <p>Golnesa has swapped nights in the club for playdates at the park.</p>
    </div>
    """

    candidates = extract_images_from_html(
        html,
        "https://www.bravotv.com/the-daily-dish/sample",
        min_width=200,
        limit=10,
    )

    assert len(candidates) == 1
    assert candidates[0].context is not None
    assert candidates[0].context.startswith("Golnesa Gharachedaghi")
    assert "playdates at the park" in candidates[0].context


def test_prefers_heading_and_bio_over_short_figcaption() -> None:
    html = """
    <div class="blog-post__body">
      <h2>Reza Farahan and Adam Farahan's official bio for The Valley: Persian Style Season 1</h2>
      <figure>
        <img src="https://www.bravotv.com/sites/bravo/files/2025/12/reza-adam.jpg" alt="Reza and Adam portrait" />
        <figcaption>Reza and Adam in The Valley: Persian Style Season 1.</figcaption>
      </figure>
      <p>After years of ups and downs, Reza and Adam are embracing a calmer chapter of life.</p>
    </div>
    """

    candidates = extract_images_from_html(
        html,
        "https://www.bravotv.com/the-daily-dish/sample",
        min_width=200,
        limit=10,
    )

    assert len(candidates) == 1
    assert candidates[0].context is not None
    assert candidates[0].context.startswith("Reza Farahan and Adam Farahan")
    assert "calmer chapter of life" in candidates[0].context


def test_prefers_heading_and_bio_over_inline_caption_div() -> None:
    html = """
    <div class="blog-post__body">
      <h2>Amir Boroumand and Natasha Kashanian's official bio for The Valley: Persian Style Season 1</h2>
      <div class="embedded-entity image">
        <div class="media__image">
          <img
            src="https://www.bravotv.com/sites/bravo/files/2025/12/amir-natasha.jpg"
            alt="Amir and Natasha portrait"
          />
          <div class="media__caption">Amir and Natasha in The Valley: Persian Style Season 1.</div>
        </div>
      </div>
      <p>Natasha Kashanian and Amir Boroumand bring a grounded energy to their group.</p>
    </div>
    """

    candidates = extract_images_from_html(
        html,
        "https://www.bravotv.com/the-daily-dish/sample",
        min_width=200,
        limit=10,
    )

    assert len(candidates) == 1
    assert candidates[0].context is not None
    assert candidates[0].context.startswith("Amir Boroumand and Natasha Kashanian")
    assert "grounded energy" in candidates[0].context


def test_scrape_msn_uses_detail_api_for_images_and_context(monkeypatch) -> None:
    import trr_backend.scraping.url_image_scraper as scraper

    monkeypatch.setenv("SCRAPE_PREVIEW_HEAD_MAX", "0")
    monkeypatch.setattr(
        scraper,
        "fetch_page_html",
        lambda url: ("<html><head><title>Shell</title></head></html>", "Shell"),
    )
    monkeypatch.setattr(scraper, "extract_images_from_html", lambda html, base_url, min_width=200, limit=50: [])

    payload = {
        "title": "The Valley: Persian style cast previews 'age-appropriate' drama",
        "abstract": "A cast reveal for the upcoming season.",
        "publishedDateTime": "2025-12-29T18:44:37Z",
        "body": (
            "<h2>Reza Farahan</h2>"
            '<img data-reference="image" data-document-id="cms/api/amp/image/AA1Tfmxs">'
            "<p>After years of ups and downs, Reza is ready for this new chapter.</p>"
        ),
        "imageResources": [
            {
                "width": 1200,
                "height": 800,
                "url": "https://img-s-msn-com.akamaized.net/tenant/amp/entityid/AA1Tfmxs.img",
                "title": "Reza Farahan in The Valley: Persian Style",
                "caption": "<a>Photo: Bravo</a>",
                "cmsId": "cms/api/amp/image/AA1Tfmxs",
            }
        ],
    }

    class FakeGetResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return payload

    monkeypatch.setattr(scraper.requests, "get", lambda *args, **kwargs: FakeGetResponse())

    result = scrape_url_for_images(
        "https://www.msn.com/en-us/entertainment/news/the-valley-persian-style-cast-previews-age-appropriate-drama/ar-AA1TfjXH"
    )

    assert result.error is None
    assert result.total_found == 1
    assert result.page_title == payload["title"]
    assert result.page_published_at == payload["publishedDateTime"]
    assert result.images[0].best_url == "https://img-s-msn-com.akamaized.net/tenant/amp/entityid/AA1Tfmxs.img"
    assert result.images[0].context is not None
    assert result.images[0].context.startswith("Reza Farahan")
    assert "new chapter" in result.images[0].context
