"""Facebook scraping module."""

from .crawlee_adapter import run_stage_with_crawlee
from .scraper import FacebookComment, FacebookPost, FacebookScrapeConfig, FacebookScraper

__all__ = ["FacebookScraper", "FacebookScrapeConfig", "FacebookPost", "FacebookComment", "run_stage_with_crawlee"]
