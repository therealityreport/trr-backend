from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.ops import instagram_direct_catalog as direct_catalog


class _FakeInstagramScraper:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self.pages = list(pages)
        self.last_retrieval_meta: dict[str, Any] = {}
        self.fetch_calls: list[dict[str, Any]] = []

    def fetch_posts_graphql(self, account_handle: str, **kwargs: Any) -> dict[str, Any] | None:
        self.fetch_calls.append({"account_handle": account_handle, **kwargs})
        page = self.pages.pop(0)
        self.last_retrieval_meta = dict(page.get("meta") or {})
        return page.get("result")

    def _iter_posts_from_graphql(self, data: dict[str, Any]):
        page_info = dict(data.get("page_info") or {})
        for node in data.get("nodes") or []:
            yield node, page_info

    def _parse_post_node(self, node: dict[str, Any], _config: Any) -> SimpleNamespace:
        return SimpleNamespace(
            shortcode=str(node.get("shortcode") or "shortcode"),
            caption=str(node.get("caption") or ""),
        )


@pytest.fixture
def _patched_scraper(monkeypatch: pytest.MonkeyPatch):
    def _install(scraper: _FakeInstagramScraper) -> _FakeInstagramScraper:
        monkeypatch.setattr(direct_catalog, "_make_instagram_scraper", lambda **_kwargs: scraper)
        monkeypatch.setattr(
            direct_catalog,
            "_make_scrape_config",
            lambda **kwargs: SimpleNamespace(username=kwargs["username"]),
        )
        return scraper

    return _install


def test_direct_instagram_catalog_marks_soft_block_empty_page_retryable(
    tmp_path,
    _patched_scraper,
) -> None:
    scraper = _patched_scraper(
        _FakeInstagramScraper(
            [
                {
                    "result": {"nodes": [], "page_info": {"has_next_page": False}},
                    "meta": {
                        "transport": "requests_enriched",
                        "error_code": "instagram_graphql_empty_response",
                        "retryable": True,
                    },
                }
            ]
        )
    )

    result = direct_catalog.run_direct_instagram_catalog_backfill(
        direct_catalog.DirectInstagramCatalogBackfillOptions(
            account="BravoTV",
            dry_run=True,
            delay=0,
            repo_root=tmp_path,
        )
    )

    assert scraper.fetch_calls[0]["account_handle"] == "bravotv"
    assert result["status"] == "blocked"
    assert result["error_code"] == "instagram_direct_catalog_soft_block_empty_page"
    assert result["error_message"] == "instagram_graphql_empty_response"
    assert result["retryable"] is True
    assert result["stop_reason"] == "soft_block_empty_page"
    assert result["total_posts"] == 0
    assert result["total_saved"] == 0


def test_direct_instagram_catalog_stops_on_db_error_before_advancing_cursor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    _patched_scraper,
) -> None:
    scraper = _patched_scraper(
        _FakeInstagramScraper(
            [
                {
                    "result": {
                        "nodes": [{"shortcode": "abc123", "caption": "hello"}],
                        "page_info": {"has_next_page": True, "end_cursor": "next-cursor"},
                    },
                    "meta": {"transport": "requests_enriched"},
                }
            ]
        )
    )

    def _raise_db_error(**_kwargs: Any) -> list[dict[str, Any]]:
        raise RuntimeError("db down")

    monkeypatch.setattr(direct_catalog, "_load_batch_upsert", lambda: _raise_db_error)

    result = direct_catalog.run_direct_instagram_catalog_backfill(
        direct_catalog.DirectInstagramCatalogBackfillOptions(
            account="BravoTV",
            resume_cursor="current-cursor",
            delay=0,
            repo_root=tmp_path,
        )
    )

    assert scraper.fetch_calls[0]["cursor"] == "current-cursor"
    assert result["status"] == "failed"
    assert result["error_code"] == "instagram_direct_catalog_db_upsert_failed"
    assert result["retryable"] is True
    assert result["stop_reason"] == "db_upsert_failed"
    assert result["db_errors"] == 1
    assert result["total_posts"] == 1
    assert result["total_saved"] == 0
    assert result["resume_cursor"] == "current-cursor"
