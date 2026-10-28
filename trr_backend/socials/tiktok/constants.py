"""Shared TikTok constants.

Stage and worker-lane names, environment variable names, API URLs, the browser
user agent, page sizes, retry budgets, and timeout defaults for the TikTok
platform Module live here so the scraper transports, parsers, HTTP client, posts
Scrapling lane, and the legacy repository all reuse one definition.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Stage and worker-lane names
# ---------------------------------------------------------------------------
TIKTOK_POSTS_SCRAPLING_STAGE = "tiktok_posts_scrapling"
TIKTOK_POSTS_SCRAPLING_WORKER_LANE = "tiktok_posts_scrapling"

# ---------------------------------------------------------------------------
# Environment variable names
# ---------------------------------------------------------------------------
ENV_TIKTOK_HTTP_CLIENT = "SOCIAL_TIKTOK_HTTP_CLIENT"
ENV_TIKTOK_CURL_CFFI_IMPERSONATE = "SOCIAL_TIKTOK_CURL_CFFI_IMPERSONATE"
ENV_TIKTOK_PROXY_URLS = "SOCIAL_TIKTOK_PROXY_URLS"
ENV_CRAWLEE_PROXY_URLS_TIKTOK = "SOCIAL_CRAWLEE_PROXY_URLS_TIKTOK"
ENV_TIKTOK_YTDLP_USE_CANONICAL_COOKIES = "SOCIAL_TIKTOK_YTDLP_USE_CANONICAL_COOKIES"
ENV_TIKTOK_YTDLP_PROXY_URL = "SOCIAL_TIKTOK_YTDLP_PROXY_URL"
ENV_TIKTOK_COOKIES_NETSCAPE_FILE = "TIKTOK_COOKIES_NETSCAPE_FILE"
ENV_TIKTOK_COOKIES_FILE = "TIKTOK_COOKIES_FILE"
ENV_SOCIAL_TIKTOK_COOKIES_FILE = "SOCIAL_TIKTOK_COOKIES_FILE"
ENV_TIKTOK_COMMENT_CONCURRENCY = "SOCIAL_TIKTOK_COMMENT_CONCURRENCY"
ENV_TIKTOK_ENABLE_DIRECT_COMMENT_API_EXPERIMENT = "SOCIAL_TIKTOK_ENABLE_DIRECT_COMMENT_API_EXPERIMENT"

# ---------------------------------------------------------------------------
# API URLs and request identity
# ---------------------------------------------------------------------------
TIKTOK_USER_DETAIL_URL = "https://www.tiktok.com/api/user/detail/"
TIKTOK_POST_LIST_URL = "https://www.tiktok.com/api/post/item_list/"
TIKTOK_COMMENTS_URL = "https://www.tiktok.com/api/comment/list/"
TIKTOK_COMMENT_REPLIES_URL = "https://www.tiktok.com/api/comment/list/reply/"
TIKTOK_OEMBED_URL = "https://www.tiktok.com/oembed"
TIKTOK_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/144.0.0.0 Safari/537.36"
)
TIKTOK_DEFAULT_NETSCAPE_COOKIE_PATH = "data/tiktok_cookies_netscape.txt"

# ---------------------------------------------------------------------------
# HTTP client defaults
# ---------------------------------------------------------------------------
DEFAULT_HTTP_CLIENT = "requests"
DEFAULT_CURL_CFFI_IMPERSONATE = "chrome"
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
TIKTOK_MAX_RETRIES = 3
TIKTOK_RETRY_BACKOFF_FACTOR = 1.5
TIKTOK_REQUEST_TIMEOUT_SECONDS = (10, 45)
TIKTOK_COMMENT_REQUEST_TIMEOUT_SECONDS = TIKTOK_REQUEST_TIMEOUT_SECONDS

# ---------------------------------------------------------------------------
# Page sizes
# ---------------------------------------------------------------------------
# TikTok page-size assumptions. The scraper translates `max_pages` into a
# `max_posts` budget by multiplying by this value. Keep in sync with the
# actual TikTok feed page size; adjust only after verifying against fresh
# responses (see transports/browser_intercept.py:_scrape_browser_intercept).
TIKTOK_POSTS_PER_PAGE: int = 50
TIKTOK_DEFAULT_MAX_POSTS: int = 10_000
# Posts Scrapling lane page sizing (``/api/post/item_list/`` ``count``).
TIKTOK_POST_PAGE_SIZE = 30
TIKTOK_POST_PAGE_SIZE_MIN = 10
TIKTOK_POST_PAGE_SIZE_MAX = 50

# ---------------------------------------------------------------------------
# yt-dlp and retrieval metadata
# ---------------------------------------------------------------------------
YTDLP_COOKIE_TTL_SECONDS = 30 * 24 * 3600
YTDLP_API_FALLBACK_ERROR_CODES = frozenset({"ytdlp_unavailable", "ytdlp_timeout", "ytdlp_nonzero_exit"})
PRESERVED_RETRIEVAL_META_KEYS = ("proxy_source", "curl_cffi_impersonate")
TRUTHY_ENV_VALUES = frozenset({"1", "true", "yes", "on"})
URL_RE = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)

__all__ = [
    "DEFAULT_CURL_CFFI_IMPERSONATE",
    "DEFAULT_HTTP_CLIENT",
    "ENV_CRAWLEE_PROXY_URLS_TIKTOK",
    "ENV_SOCIAL_TIKTOK_COOKIES_FILE",
    "ENV_TIKTOK_COMMENT_CONCURRENCY",
    "ENV_TIKTOK_COOKIES_FILE",
    "ENV_TIKTOK_COOKIES_NETSCAPE_FILE",
    "ENV_TIKTOK_CURL_CFFI_IMPERSONATE",
    "ENV_TIKTOK_ENABLE_DIRECT_COMMENT_API_EXPERIMENT",
    "ENV_TIKTOK_HTTP_CLIENT",
    "ENV_TIKTOK_PROXY_URLS",
    "ENV_TIKTOK_YTDLP_PROXY_URL",
    "ENV_TIKTOK_YTDLP_USE_CANONICAL_COOKIES",
    "PRESERVED_RETRIEVAL_META_KEYS",
    "RETRYABLE_STATUS_CODES",
    "TIKTOK_COMMENTS_URL",
    "TIKTOK_COMMENT_REPLIES_URL",
    "TIKTOK_COMMENT_REQUEST_TIMEOUT_SECONDS",
    "TIKTOK_DEFAULT_MAX_POSTS",
    "TIKTOK_DEFAULT_NETSCAPE_COOKIE_PATH",
    "TIKTOK_MAX_RETRIES",
    "TIKTOK_OEMBED_URL",
    "TIKTOK_POSTS_PER_PAGE",
    "TIKTOK_POSTS_SCRAPLING_STAGE",
    "TIKTOK_POSTS_SCRAPLING_WORKER_LANE",
    "TIKTOK_POST_LIST_URL",
    "TIKTOK_POST_PAGE_SIZE",
    "TIKTOK_POST_PAGE_SIZE_MAX",
    "TIKTOK_POST_PAGE_SIZE_MIN",
    "TIKTOK_REQUEST_TIMEOUT_SECONDS",
    "TIKTOK_RETRY_BACKOFF_FACTOR",
    "TIKTOK_USER_AGENT",
    "TIKTOK_USER_DETAIL_URL",
    "TRUTHY_ENV_VALUES",
    "URL_RE",
    "YTDLP_API_FALLBACK_ERROR_CODES",
    "YTDLP_COOKIE_TTL_SECONDS",
]
