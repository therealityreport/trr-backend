from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path

import pytest

from trr_backend.scraping.url_image_scraper import ImageCandidate, ScrapeResult

mod = importlib.import_module("scripts.import.download_scraped_images_local")


def _fake_scrape_result() -> ScrapeResult:
    return ScrapeResult(
        url="https://example.com/gallery",
        page_title="Example Gallery",
        page_published_at="2026-02-24T00:00:00Z",
        domain="example.com",
        images=[
            ImageCandidate(
                id="1",
                original_url="https://cdn.example.com/a.jpg",
                best_url="https://cdn.example.com/a.jpg",
                width=1200,
                height=800,
                alt_text="Cast Photo A",
            ),
            ImageCandidate(
                id="2",
                original_url="https://cdn.example.com/b.jpg",
                best_url="https://cdn.example.com/b.jpg",
                width=1100,
                height=900,
                context="Cast Photo B",
            ),
        ],
        total_found=2,
        error=None,
    )


def _fake_download(url: str, *, referer: str | None = None, timeout: float = 30.0) -> tuple[bytes, str, str]:
    del referer, timeout
    payload = f"payload::{url}".encode()
    digest = hashlib.sha256(payload).hexdigest()
    return payload, digest, "image/jpeg"


def test_download_images_from_url_writes_files_and_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(mod, "scrape_url_for_images", lambda *args, **kwargs: _fake_scrape_result())
    monkeypatch.setattr(mod, "download_and_hash_image", _fake_download)

    output_dir = tmp_path / "downloads"
    manifest = mod.download_images_from_url(
        url="https://example.com/gallery",
        output_dir=output_dir,
        limit=10,
    )

    assert manifest["downloaded"] == 2
    assert manifest["skipped_existing"] == 0
    assert manifest["errors"] == []

    jpg_files = sorted(output_dir.glob("*.jpg"))
    assert len(jpg_files) == 2
    assert jpg_files[0].name.startswith("001-cast-photo-a-")
    assert jpg_files[1].name.startswith("002-cast-photo-b-")

    manifest_path = output_dir / "manifest.json"
    assert manifest_path.exists()
    parsed = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert parsed["total_candidates"] == 2
    assert len(parsed["images"]) == 2


def test_download_images_from_url_skips_existing_when_overwrite_false(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(mod, "scrape_url_for_images", lambda *args, **kwargs: _fake_scrape_result())
    monkeypatch.setattr(mod, "download_and_hash_image", _fake_download)

    output_dir = tmp_path / "downloads"
    first_run = mod.download_images_from_url(
        url="https://example.com/gallery",
        output_dir=output_dir,
    )
    second_run = mod.download_images_from_url(
        url="https://example.com/gallery",
        output_dir=output_dir,
        overwrite=False,
    )

    assert first_run["downloaded"] == 2
    assert second_run["downloaded"] == 0
    assert second_run["skipped_existing"] == 2
