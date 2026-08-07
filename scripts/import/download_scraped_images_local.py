#!/usr/bin/env python3
"""
Download scraped image candidates from a URL to local disk.

Usage:
    PYTHONPATH=. python scripts/import/download_scraped_images_local.py \
      --url "https://deadline.com/..." \
      --output-dir "~/Downloads/Bachelorette"
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast
from urllib.parse import urlparse

from trr_backend.scraping.url_image_scraper import (
    download_and_hash_image,
    scrape_url_for_images,
)

_CONTENT_TYPE_EXTENSION_OVERRIDES = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/avif": ".avif",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
    "image/tiff": ".tiff",
}

_SAFE_NAME_RE = re.compile(r"[^a-z0-9]+")


@dataclass(slots=True)
class DownloadRecord:
    index: int
    source_url: str
    best_url: str
    filename: str
    output_path: str
    sha256: str
    content_type: str
    bytes: int
    width: int | None
    height: int | None
    alt_text: str | None
    context: str | None


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="download_scraped_images_local",
        description="Scrape image candidates from a URL and download locally.",
    )
    parser.add_argument(
        "--url",
        required=True,
        help="Source page URL to scrape for image candidates.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path.home() / "Downloads" / "Bachelorette"),
        help="Directory where downloaded images are saved.",
    )
    parser.add_argument(
        "--min-width",
        type=int,
        default=200,
        help="Minimum image width for scrape candidates (default: 200).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum image candidates to scrape (default: 100).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite files when generated filename already exists.",
    )
    parser.add_argument(
        "--manifest-name",
        default="manifest.json",
        help="Manifest filename written in output directory (default: manifest.json).",
    )
    return parser.parse_args(argv)


def _slugify(value: str | None, *, fallback: str = "image", max_len: int = 42) -> str:
    raw = (value or "").strip().lower()
    cleaned = _SAFE_NAME_RE.sub("-", raw).strip("-")
    if not cleaned:
        cleaned = fallback
    cleaned = cleaned[:max_len].strip("-")
    return cleaned or fallback


def _guess_extension(content_type: str, image_url: str) -> str:
    normalized = (content_type or "").split(";", 1)[0].strip().lower()
    if normalized in _CONTENT_TYPE_EXTENSION_OVERRIDES:
        return _CONTENT_TYPE_EXTENSION_OVERRIDES[normalized]

    guessed = mimetypes.guess_extension(normalized) if normalized else None
    if guessed == ".jpe":
        guessed = ".jpg"
    if guessed:
        return guessed

    suffix = Path(urlparse(image_url).path).suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        return ".jpg"
    if suffix in {".png", ".webp", ".avif", ".gif", ".bmp", ".tif", ".tiff"}:
        return ".tiff" if suffix == ".tif" else suffix
    return ".jpg"


def _build_filename(*, index: int, label: str | None, sha256: str, content_type: str, image_url: str) -> str:
    slug = _slugify(label)
    ext = _guess_extension(content_type, image_url)
    return f"{index:03d}-{slug}-{sha256[:12]}{ext}"


def download_images_from_url(
    *,
    url: str,
    output_dir: Path,
    min_width: int = 200,
    limit: int = 100,
    overwrite: bool = False,
    manifest_name: str = "manifest.json",
) -> dict[str, object]:
    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    result = scrape_url_for_images(url, min_width=min_width, limit=limit)
    if result.error:
        raise RuntimeError(f"Failed to scrape URL: {result.error}")

    records: list[DownloadRecord] = []
    errors: list[str] = []
    skipped_existing = 0

    for index, candidate in enumerate(result.images, start=1):
        best_url = str(candidate.best_url or candidate.original_url)
        try:
            image_data, sha256, content_type = download_and_hash_image(
                best_url,
                referer=result.url,
            )
            filename = _build_filename(
                index=index,
                label=candidate.alt_text or candidate.context,
                sha256=sha256,
                content_type=content_type,
                image_url=best_url,
            )
            destination = output_dir / filename

            if destination.exists() and not overwrite:
                skipped_existing += 1
                continue

            destination.write_bytes(image_data)
            records.append(
                DownloadRecord(
                    index=index,
                    source_url=str(candidate.original_url),
                    best_url=best_url,
                    filename=filename,
                    output_path=str(destination),
                    sha256=sha256,
                    content_type=content_type,
                    bytes=len(image_data),
                    width=candidate.width,
                    height=candidate.height,
                    alt_text=candidate.alt_text,
                    context=candidate.context,
                )
            )
        except Exception as exc:
            errors.append(f"Candidate {index} ({best_url}): {exc}")

    manifest = {
        "source_url": result.url,
        "page_title": result.page_title,
        "page_published_at": result.page_published_at,
        "domain": result.domain,
        "total_candidates": len(result.images),
        "downloaded": len(records),
        "skipped_existing": skipped_existing,
        "errors": errors,
        "images": [asdict(record) for record in records],
    }

    manifest_path = output_dir / manifest_name
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    output_dir = Path(args.output_dir)

    try:
        manifest = download_images_from_url(
            url=args.url,
            output_dir=output_dir,
            min_width=args.min_width,
            limit=args.limit,
            overwrite=args.overwrite,
            manifest_name=args.manifest_name,
        )
    except Exception as exc:
        print(f"[download_scraped_images_local] {exc}", file=sys.stderr)
        return 1

    print(f"Source URL: {manifest['source_url']}")
    print(f"Output dir: {output_dir.expanduser().resolve()}")
    print(f"Candidates: {manifest['total_candidates']}")
    print(f"Downloaded: {manifest['downloaded']}")
    print(f"Skipped existing: {manifest['skipped_existing']}")
    manifest_errors = cast("list[str]", manifest["errors"])
    print(f"Errors: {len(manifest_errors)}")

    if manifest_errors:
        print("Download errors:", file=sys.stderr)
        for err in manifest_errors:
            print(f"- {err}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
