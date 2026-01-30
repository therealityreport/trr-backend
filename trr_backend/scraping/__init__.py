"""URL-based image scraping utilities."""

from trr_backend.scraping.url_image_scraper import (
    ImageCandidate,
    extract_images_from_html,
    fetch_page_html,
)

__all__ = [
    "ImageCandidate",
    "extract_images_from_html",
    "fetch_page_html",
]
