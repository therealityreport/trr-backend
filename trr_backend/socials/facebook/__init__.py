"""Facebook scraping module."""

from .crawlee_adapter import run_stage_with_crawlee
from .direct_scrape import (
    comment_to_payload,
    post_to_payload,
    preview_facebook_page,
    scrape_facebook,
    scrape_facebook_post,
    search_facebook_posts,
)

# Compatibility exports for existing router/test monkeypatch paths. Move new
# route-facing orchestration to direct_scrape; keep scraper classes here until
# all supported external imports migrate deliberately.
from .scraper import (
    FacebookComment,
    FacebookMediaProvenance,
    FacebookPost,
    FacebookScrapeConfig,
    FacebookScraper,
    FacebookSearchConfig,
    FacebookShare,
)

__all__ = [
    "FacebookScraper",
    "FacebookScrapeConfig",
    "FacebookSearchConfig",
    "FacebookPost",
    "FacebookComment",
    "FacebookShare",
    "FacebookMediaProvenance",
    "run_stage_with_crawlee",
    "post_to_payload",
    "comment_to_payload",
    "scrape_facebook",
    "search_facebook_posts",
    "preview_facebook_page",
    "scrape_facebook_post",
]
