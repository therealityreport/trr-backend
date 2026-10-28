"""Playwright browser-intercept transport for the TikTok scraper (scroll + response capture)."""

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from trr_backend.socials.tiktok.constants import TIKTOK_DEFAULT_MAX_POSTS, TIKTOK_POSTS_PER_PAGE
from trr_backend.socials.tiktok.models import TikTokPost, TikTokScrapeConfig

logger = logging.getLogger(__name__)


class BrowserInterceptTransportMixin:
    """Scrape by scrolling a headless browser and intercepting TikTok's own API responses."""

    if TYPE_CHECKING:
        # State and sibling-mixin methods supplied by ``TikTokScraper`` and the other mixins it composes.
        cookies: dict[str, str]
        last_retrieval_meta: dict[str, Any]

        @staticmethod
        def _coerce_timestamp(value: Any) -> int: ...
        def _get_headers(self, referer: str | None = None) -> dict: ...
        def fetch_user_detail(self, username: str, delay: float = 2.0) -> dict | None: ...
        def build_profile_snapshot(
            self,
            username: str,
            *,
            user_data: dict | None = None,
            html_data: dict | None = None,
        ) -> dict[str, Any]: ...
        def _parse_post_item(self, item: dict, config: TikTokScrapeConfig) -> TikTokPost: ...
        def _emit_progress(
            self,
            progress_cb: Callable[[dict[str, Any]], None] | None,
            *,
            phase: str,
            pages_scanned: int,
            posts_checked: int,
            matched_posts: int,
        ) -> None: ...
        def _set_retrieval_meta(
            self,
            *,
            context_mode: str | None = None,
            auth_mode: str | None = None,
            **values: Any,
        ) -> None: ...

    def _browser_cookie_payload(self) -> list[dict[str, Any]]:
        """Convert flat cookie dict to Playwright's ``add_cookies`` format."""
        cookies: list[dict[str, Any]] = []
        for name, value in (self.cookies or {}).items():
            if not str(name or "").strip() or not str(value or "").strip():
                continue
            cookies.append(
                {
                    "name": str(name),
                    "value": str(value),
                    "domain": ".tiktok.com",
                    "path": "/",
                    "secure": True,
                }
            )
        return cookies

    def _scrape_browser_intercept(
        self,
        config: TikTokScrapeConfig,
        *,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[TikTokPost]:
        """Scrape by scrolling a headless browser and intercepting API responses.

        Navigates to the TikTok profile, auto-scrolls, and captures post data
        from the ``/api/post/item_list/`` responses TikTok sends during its own
        infinite-scroll pagination.  Zero rate-limiting overhead because requests
        originate from a real browser session.
        """
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # noqa: BLE001
            logger.error("Playwright not available for browser_intercept mode: %s", exc)
            self.last_retrieval_meta.update(
                {
                    "retrieval_mode": "browser_intercept",
                    "auth_mode": "with_cookies" if self.cookies else "without_cookies",
                    "error_code": "playwright_unavailable",
                    "error_class": type(exc).__name__,
                    "playwright_error": type(exc).__name__,
                    "intercepted_post_responses": 0,
                    "intercepted_user_detail_responses": 0,
                    "dom_cards_seen": 0,
                    "scroll_iterations": 0,
                }
            )
            return []

        from trr_backend.socials.browser_cookie_refresh import launch_browser

        logger.info(
            "Starting browser_intercept scrape for @%s",
            config.username,
        )

        posts: list[TikTokPost] = []
        seen_ids: set[str] = set()
        reached_date_limit = False
        no_new_data_scrolls = 0
        max_no_new_data_scrolls = 5
        scroll_count = 0
        intercepted_post_responses = 0
        intercepted_user_detail_responses = 0
        dom_cards_seen = 0
        playwright_error: str | None = None
        auth_mode = "with_cookies" if self.cookies else "without_cookies"

        self.last_retrieval_meta.update(
            {
                "retrieval_mode": "browser_intercept",
                "auth_mode": auth_mode,
                "intercepted_post_responses": intercepted_post_responses,
                "intercepted_user_detail_responses": intercepted_user_detail_responses,
                "dom_cards_seen": dom_cards_seen,
                "scroll_iterations": scroll_count,
                "playwright_error": playwright_error,
            }
        )

        user_agent = self._get_headers().get("user-agent", "Mozilla/5.0")
        timeout_ms = 60_000
        max_posts = config.max_pages * TIKTOK_POSTS_PER_PAGE if config.max_pages else TIKTOK_DEFAULT_MAX_POSTS

        browser: Any | None = None
        context: Any | None = None
        try:
            with sync_playwright() as playwright:
                browser = launch_browser(playwright, headless=True)
                assert browser is not None  # launch_browser always returns a browser
                context = browser.new_context(
                    viewport={"width": 1280, "height": 900},
                    user_agent=user_agent,
                )
                assert context is not None  # new_context always returns a context
                browser_cookies = self._browser_cookie_payload()
                if browser_cookies:
                    context.add_cookies(browser_cookies)
                page = context.new_page()

                # Navigate to profile
                page.goto(
                    f"https://www.tiktok.com/@{config.username}",
                    wait_until="domcontentloaded",
                    timeout=timeout_ms,
                )
                page.wait_for_timeout(3_000)

                # Build profile snapshot for avatar backfill
                profile_avatar_url: str | None = None
                try:
                    user_data = self.fetch_user_detail(config.username, 0)
                    if user_data:
                        snapshot = self.build_profile_snapshot(
                            config.username,
                            user_data=user_data,
                        )
                        profile_avatar_url = str(snapshot.get("avatar_url") or "").strip() or None
                except Exception:  # noqa: BLE001
                    pass

                # Register response interceptor for browser traffic relevant to recovery triage.
                def _handle_post_list_response(response: Any) -> None:
                    nonlocal intercepted_post_responses, intercepted_user_detail_responses, reached_date_limit
                    nonlocal no_new_data_scrolls
                    try:
                        response_url = str(response.url or "")
                        normalized_response_url = response_url.lower()
                        if (
                            "/api/user/detail/" in normalized_response_url
                            or "fetch_user_detail" in normalized_response_url
                        ):
                            if response.ok:
                                try:
                                    browser_user_detail_payload = response.json()
                                except Exception:  # noqa: BLE001
                                    browser_user_detail_payload = None
                                if isinstance(browser_user_detail_payload, (dict, list)):
                                    intercepted_user_detail_responses += 1
                                    self.last_retrieval_meta["intercepted_user_detail_responses"] = (
                                        intercepted_user_detail_responses
                                    )
                        if "/api/post/item_list/" not in normalized_response_url:
                            return
                        intercepted_post_responses += 1
                        self.last_retrieval_meta["intercepted_post_responses"] = intercepted_post_responses
                        if not response.ok:
                            return
                        payload = response.json()
                        items = payload.get("itemList") or []
                        if not items:
                            return

                        new_posts_this_batch = 0
                        for item in items:
                            video_id = str(item.get("id") or item.get("aweme_id") or item.get("videoId") or "")
                            if not video_id or video_id in seen_ids:
                                continue
                            seen_ids.add(video_id)

                            # Check date range
                            create_time = self._coerce_timestamp(item.get("createTime") or item.get("createTimeISO"))
                            if create_time <= 0:
                                continue
                            in_range = config.is_in_date_range(create_time)
                            if in_range is None:  # Before date range — stop
                                reached_date_limit = True
                                return
                            if in_range is False:  # After date range — skip
                                continue

                            # Check hashtag filter
                            description = str(item.get("desc") or item.get("text") or "")
                            if config.matches_hashtags(description):
                                post = self._parse_post_item(item, config)
                                if profile_avatar_url and not post.user_avatar_url:
                                    post.user_avatar_url = profile_avatar_url
                                posts.append(post)
                                new_posts_this_batch += 1

                        if new_posts_this_batch > 0:
                            no_new_data_scrolls = 0
                            logger.info(
                                "browser_intercept: +%d posts (total: %d)",
                                new_posts_this_batch,
                                len(posts),
                            )
                            self._emit_progress(
                                progress_cb,
                                phase="browser_intercept_batch",
                                pages_scanned=scroll_count,
                                posts_checked=len(seen_ids),
                                matched_posts=len(posts),
                            )
                    except Exception:  # noqa: BLE001
                        pass  # Ignore parse errors — don't break the scroll loop

                page.on("response", _handle_post_list_response)

                # Auto-scroll loop
                scroll_interval_ms = 800
                while not reached_date_limit and no_new_data_scrolls < max_no_new_data_scrolls:
                    if len(posts) >= max_posts:
                        logger.info(
                            "browser_intercept: reached max posts (%d)",
                            max_posts,
                        )
                        break

                    prev_count = len(posts)
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(scroll_interval_ms)
                    scroll_count += 1

                    # Check if we got new posts from the intercepted responses
                    if len(posts) == prev_count:
                        no_new_data_scrolls += 1
                    else:
                        no_new_data_scrolls = 0
                    dom_cards_seen = max(dom_cards_seen, int(page.locator("a[href*='/video/']").count() or 0))
                    self.last_retrieval_meta["dom_cards_seen"] = dom_cards_seen
                    self.last_retrieval_meta["scroll_iterations"] = scroll_count

                    if scroll_count % 20 == 0:
                        logger.info(
                            "browser_intercept: scrolled %d times, %d posts collected",
                            scroll_count,
                            len(posts),
                        )

        except Exception as exc:
            playwright_error = type(exc).__name__
            logger.error(
                "browser_intercept failed for @%s: %s",
                config.username,
                exc,
            )
            self.last_retrieval_meta.update(
                {
                    "retrieval_mode": "browser_intercept",
                    "error_code": "browser_intercept_error",
                    "error_class": playwright_error,
                    "error_message": str(exc),
                    "intercepted_post_responses": intercepted_post_responses,
                    "intercepted_user_detail_responses": intercepted_user_detail_responses,
                    "dom_cards_seen": dom_cards_seen,
                    "scroll_iterations": scroll_count,
                    "playwright_error": playwright_error,
                }
            )
        finally:
            # Close context first so pages/cookies are flushed; wrap each
            # close independently so one teardown failure doesn't skip the
            # other. The outer `with sync_playwright()` also cleans up, but
            # explicit close releases FDs sooner under high concurrency.
            if context is not None:
                try:
                    context.close()
                except Exception:  # noqa: BLE001 - teardown best-effort
                    logger.debug("tiktok playwright context close failed", exc_info=True)
            if browser is not None:
                try:
                    browser.close()
                except Exception:  # noqa: BLE001 - teardown best-effort
                    logger.debug("tiktok playwright browser close failed", exc_info=True)

        stop_reason = (
            "date_start_reached"
            if reached_date_limit
            else "max_posts_reached"
            if len(posts) >= max_posts
            else "no_new_data"
            if no_new_data_scrolls >= max_no_new_data_scrolls
            else "unknown"
        )
        logger.info(
            "browser_intercept complete for @%s: %d posts in %d scrolls (stop: %s)",
            config.username,
            len(posts),
            scroll_count,
            stop_reason,
        )
        error_code = str(self.last_retrieval_meta.get("error_code") or "").strip() or None
        error_class = str(self.last_retrieval_meta.get("error_class") or "").strip() or None
        error_message = str(self.last_retrieval_meta.get("error_message") or "").strip() or None
        fallback_chain = self.last_retrieval_meta.get("fallback_chain") or None
        self._set_retrieval_meta(
            retrieval_mode="browser_intercept",
            auth_mode=auth_mode,
            posts_checked=len(seen_ids),
            pages_scanned=scroll_count,
            stop_reason=stop_reason,
            intercepted_post_responses=intercepted_post_responses,
            intercepted_user_detail_responses=intercepted_user_detail_responses,
            dom_cards_seen=dom_cards_seen,
            scroll_iterations=scroll_count,
            playwright_error=playwright_error,
            error_code=error_code,
            error_class=error_class,
            error_message=error_message,
            fallback_chain=fallback_chain,
        )
        self.last_retrieval_meta["playwright_error"] = playwright_error
        return posts

    def _classify_browser_intercept_failure(
        self,
        *,
        posts_found: int,
        intercepted_post_responses: int,
        intercepted_user_detail_responses: int,
        dom_cards_seen: int,
        scroll_iterations: int,
        authenticated: bool,
        playwright_error: str | None,
    ) -> str:
        if playwright_error:
            return "playwright_runtime_change"
        if not authenticated:
            return "auth_or_session_state"
        if intercepted_post_responses == 0 and intercepted_user_detail_responses > 0:
            return "interception_target_drift"
        if dom_cards_seen == 0 and scroll_iterations > 0:
            return "scroll_or_pagination_drift"
        if posts_found <= 0:
            return "unclassified_zero_posts"
        return "healthy"


__all__ = [
    "BrowserInterceptTransportMixin",
]
