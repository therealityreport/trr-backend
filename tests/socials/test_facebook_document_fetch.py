from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from trr_backend.socials.facebook.document_fetch import FacebookDocumentFetcher
from trr_backend.socials.facebook.scraper import FacebookScraper


def test_document_fetch_uses_canonical_cookie_loader_for_authenticated_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "trr_backend.socials.facebook.document_fetch._load_facebook_cookies",
        lambda: {"c_user": "1", "xs": "token"},
    )
    session = MagicMock()
    session.get.return_value = MagicMock(status_code=200, text="<html></html>", raise_for_status=lambda: None)

    fetcher = FacebookDocumentFetcher(session=session)
    html = fetcher.fetch(
        "https://www.facebook.com/Bravo/posts/123",
        headers={"user-agent": "test"},
        referer="https://www.facebook.com/Bravo/posts/123",
    )

    assert html == "<html></html>"
    assert fetcher.runtime_metadata["request_count"] == 1
    assert fetcher.runtime_metadata["transport"] == "authenticated_document_fetch"


def test_scrape_post_updates_fallback_chain_when_authenticated_document_fetch_used(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = FacebookScraper(cookies={"c_user": "1", "xs": "token"})
    monkeypatch.setattr(scraper, "_fetch_html", lambda *_args, **_kwargs: "<html></html>")
    monkeypatch.setattr(
        scraper,
        "_fetch_html_with_document_fetcher",
        lambda *_args, **_kwargs: (
            "<html><head>"
            '<meta property="og:url" content="https://www.facebook.com/Bravo/posts/123" />'
            '<meta property="og:title" content="Bravo Post" />'
            "</head><body></body></html>"
        ),
    )

    post, comments = scraper.scrape_post("https://www.facebook.com/Bravo/posts/123", delay_seconds=0)

    assert post is not None
    assert comments == []
    assert scraper.runtime_metadata["fallback_chain"] == ["public_ssr", "authenticated_document_fetch"]
    assert scraper.runtime_metadata["transport"] == "authenticated_document_fetch"
