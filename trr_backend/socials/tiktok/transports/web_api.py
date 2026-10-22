"""Direct TikTok web API transport: user detail, post list, comments, replies, profile HTML, oembed."""

import json
import logging
import os
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests

from trr_backend.socials.tiktok.constants import (
    ENV_TIKTOK_COMMENT_CONCURRENCY,
    ENV_TIKTOK_ENABLE_DIRECT_COMMENT_API_EXPERIMENT,
    TIKTOK_OEMBED_URL,
    TIKTOK_USER_AGENT,
    TRUTHY_ENV_VALUES,
)
from trr_backend.socials.tiktok.models import TikTokComment, TikTokPost, TikTokScrapeConfig

logger = logging.getLogger(__name__)


class WebApiTransportMixin:
    """Talk to TikTok's web API through the scraper's configured HTTP session."""

    @staticmethod
    def _is_auth_related_failure(reason: str | None) -> bool:
        value = str(reason or "").strip().lower()
        if not value:
            return False
        markers = (
            "auth",
            "login",
            "challenge",
            "captcha",
            "blocked",
            "forbidden",
            "unauthorized",
        )
        return any(marker in value for marker in markers)

    def _set_comment_failure_reason(self, reason: str | None) -> None:
        normalized = str(reason or "").strip()
        if not normalized:
            return
        self._last_api_fail_reason = normalized
        self.last_comment_fetch_reason = normalized
        if self._is_auth_related_failure(normalized):
            self.comments_auth_failed = True

    @staticmethod
    def _normalize_optional_non_negative_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_optional_non_negative_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return max(0.0, float(value))
        except (TypeError, ValueError):
            return None

    def _record_reply_cap_event(
        self,
        *,
        reason: str,
        comment_id: str,
        limit: Any = None,
        replies_fetched: int = 0,
        pages_fetched: int = 0,
    ) -> None:
        events = list(self.last_comment_fetch_meta.get("reply_cap_events") or [])
        event = {
            "reason": str(reason or "").strip() or "reply_cap",
            "comment_id": str(comment_id or "").strip() or None,
            "limit": limit,
            "replies_fetched": max(0, int(replies_fetched or 0)),
            "pages_fetched": max(0, int(pages_fetched or 0)),
        }
        events.append({key: value for key, value in event.items() if value is not None})
        self.last_comment_fetch_meta["reply_cap_events"] = events

    @staticmethod
    def _should_skip_api_pagination(reason: str | None) -> bool:
        normalized = str(reason or "").strip().lower()
        return normalized in {"non_json_response", "challenge_or_blocked"}

    def _env_flag_enabled(self, env_name: str) -> bool:
        return str(os.getenv(env_name) or "").strip().lower() in TRUTHY_ENV_VALUES

    def _direct_comment_api_experiment_enabled(self) -> bool:
        if self._direct_comment_api_enabled_override is not None:
            return self._direct_comment_api_enabled_override
        return self._env_flag_enabled(ENV_TIKTOK_ENABLE_DIRECT_COMMENT_API_EXPERIMENT)

    def _get_headers(self, referer: str | None = None) -> dict:
        """Get request headers."""
        return {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "origin": "https://www.tiktok.com",
            "referer": referer or "https://www.tiktok.com/",
            "user-agent": TIKTOK_USER_AGENT,
        }

    def _rate_limit(self, delay: float, *, fast_mode: bool = False):
        """Apply adaptive rate limiting between requests.

        In fast_mode, reduces delay based on consecutive success count.
        """
        if self._request_count > 0:
            if fast_mode:
                consecutive = getattr(self, "_consecutive_success", 0)
                if consecutive >= 20:
                    effective_delay = delay * 0.15
                elif consecutive >= 5:
                    effective_delay = delay * 0.25
                else:
                    effective_delay = delay * 0.5
            else:
                effective_delay = delay
            logger.debug(f"Rate limiting: waiting {effective_delay:.3f}s (base={delay}s, fast={fast_mode})")
            time.sleep(effective_delay)
        self._request_count += 1

    def _record_endpoint_response(
        self,
        *,
        endpoint: str,
        failure_reason: str | None = None,
        response: requests.Response | None = None,
    ) -> None:
        endpoint_name = str(endpoint or "").strip()
        if not endpoint_name:
            return
        endpoint_responses = dict(self.last_retrieval_meta.get("endpoint_responses") or {})
        payload: dict[str, Any] = {"endpoint": endpoint_name}
        normalized_failure_reason = str(failure_reason or "").strip().lower() or None
        if normalized_failure_reason:
            payload["failure_reason"] = normalized_failure_reason
        if response is not None:
            headers = getattr(response, "headers", None) or {}
            content_type = str(headers.get("content-type") or "").strip() or None
            content_length_raw = str(headers.get("content-length") or "").strip()
            try:
                content_length = int(content_length_raw) if content_length_raw else len(response.content or b"")
            except (TypeError, ValueError):
                content_length = len(response.content or b"")
            request_id = (
                str(headers.get("x-tt-logid") or "").strip() or str(headers.get("x-request-id") or "").strip() or None
            )
            payload.update(
                {
                    "http_status": int(getattr(response, "status_code", 0) or 0),
                    "content_type": content_type,
                    "content_length": max(0, int(content_length)),
                    "request_id": request_id,
                }
            )
        endpoint_responses[endpoint_name] = {key: value for key, value in payload.items() if value is not None}
        self.last_retrieval_meta["endpoint_responses"] = endpoint_responses

    def _safe_response_json(self, response: requests.Response, *, endpoint: str) -> dict | None:
        """Parse response JSON and classify challenge/non-json failures."""
        try:
            return response.json()
        except ValueError:
            content_type = (response.headers.get("content-type") or "").lower()
            body = (response.text or "")[:256].lower()
            if "text/html" in content_type or "<html" in body or "captcha" in body or "verify" in body:
                self._last_api_fail_reason = "challenge_or_blocked"
            else:
                self._last_api_fail_reason = "non_json_response"
            self._record_endpoint_response(
                endpoint=endpoint,
                failure_reason=self._last_api_fail_reason,
                response=response,
            )
            logger.warning(
                "TikTok returned non-JSON response (status=%s, content-type=%s, reason=%s)",
                response.status_code,
                content_type or "unknown",
                self._last_api_fail_reason,
            )
            return None

    def _fetch_profile_html(self, username: str, delay: float = 2.0) -> dict | None:
        """Fetch profile page HTML and extract embedded post data."""
        self._rate_limit(delay)

        url = f"https://www.tiktok.com/@{username}"
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": TIKTOK_USER_AGENT,
        }

        try:
            response = self.session.get(
                url,
                headers=headers,
                cookies=self.cookies,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            self._record_endpoint_response(endpoint="profile_html", response=response)
            html = response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch TikTok profile HTML for @{username}: {e}")
            return None

        # Try __UNIVERSAL_DATA_FOR_REHYDRATION__ first (current TikTok format)
        match = self._REHYDRATION_RE.search(html)
        if match:
            try:
                data = json.loads(match.group(1))
                return data
            except json.JSONDecodeError:
                logger.warning("Failed to parse __UNIVERSAL_DATA_FOR_REHYDRATION__ JSON")

        # Fallback to SIGI_STATE (older TikTok format)
        match = self._SIGI_STATE_RE.search(html)
        if match:
            try:
                data = json.loads(match.group(1))
                return data
            except json.JSONDecodeError:
                logger.warning("Failed to parse SIGI_STATE JSON")

        logger.error("Could not extract embedded data from TikTok profile page")
        return None

    def fetch_user_detail(self, username: str, delay: float = 2.0) -> dict | None:
        """Fetch user detail to get secUid needed for post list."""
        self._rate_limit(delay)

        params = {"uniqueId": username}
        headers = self._get_headers(f"https://www.tiktok.com/@{username}")

        try:
            response = self.session.get(
                self.USER_DETAIL_URL,
                params=params,
                headers=headers,
                cookies=self.cookies,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            parsed = self._safe_response_json(response, endpoint="fetch_user_detail")
            if parsed is None:
                return None
            return parsed
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch user detail for @{username}: {e}")
            self._last_api_fail_reason = "request_error"
            response = getattr(e, "response", None)
            self._record_endpoint_response(
                endpoint="fetch_user_detail",
                failure_reason=self._last_api_fail_reason,
                response=response,
            )
            return None

    def fetch_posts(
        self,
        username: str,
        sec_uid: str,
        cursor: int = 0,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
    ) -> dict | None:
        """Fetch posts from user's profile."""
        self._rate_limit(delay, fast_mode=fast_mode)

        params = {
            "secUid": sec_uid,
            "count": 30,
            "cursor": cursor,
        }
        headers = self._get_headers(f"https://www.tiktok.com/@{username}")

        try:
            response = self.session.get(
                self.USER_POST_URL,
                params=params,
                headers=headers,
                cookies=self.cookies,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            parsed = self._safe_response_json(response, endpoint="fetch_posts")
            if parsed is None:
                return None
            self._record_endpoint_response(endpoint="fetch_posts", response=response)
            return parsed
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch posts: {e}")
            self._last_api_fail_reason = "request_error"
            response = getattr(e, "response", None)
            self._record_endpoint_response(
                endpoint="fetch_posts",
                failure_reason=self._last_api_fail_reason,
                response=response,
            )
            return None

    def _scrape_api(
        self,
        config: TikTokScrapeConfig,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[TikTokPost]:
        """Scrape via the direct TikTok web API (original implementation)."""

        html_data: dict | None = None

        # Get user detail first to get secUid
        user_data = self.fetch_user_detail(config.username, config.delay_seconds)
        api_preflight_fail_reason = self._last_api_fail_reason

        sec_uid = None
        use_api = False
        api_pagination_blocked_reason: str | None = None
        api_posts_found = False
        ytdlp_used = False

        if user_data and user_data.get("userInfo", {}).get("user", {}).get("secUid"):
            user_info = user_data.get("userInfo", {})
            user = user_info.get("user", {})
            sec_uid = user.get("secUid")
            use_api = True
            logger.info(f"Found user: {user.get('nickname')} (@{config.username})")
        else:
            logger.warning(f"API returned no data for @{config.username}; falling back to HTML scraping")

        # Try HTML scraping fallback to get initial posts + secUid
        html_posts: list[dict] = []
        if not use_api:
            html_data = self._fetch_profile_html(config.username, config.delay_seconds)
            if html_data:
                html_posts, sec_uid = self._extract_posts_from_html_data(html_data, config.username)
                if html_posts:
                    logger.info(f"Extracted {len(html_posts)} posts from HTML page")
                if sec_uid:
                    logger.info(f"Got secUid from HTML: {sec_uid[:20]}...")
                    if self._should_skip_api_pagination(api_preflight_fail_reason):
                        api_pagination_blocked_reason = api_preflight_fail_reason
                        logger.info(
                            "Skipping TikTok API pagination for @%s after poisoned preflight (%s)",
                            config.username,
                            api_preflight_fail_reason,
                        )
                    else:
                        use_api = True  # Can try API for pagination now

        # Process HTML-extracted posts first
        profile_snapshot = self.build_profile_snapshot(config.username, user_data=user_data, html_data=html_data)
        profile_avatar_url = str(profile_snapshot.get("avatar_url") or "").strip() or None

        posts = []
        existing_ids: set[str] = set()
        posts_checked = 0
        reached_date_limit = False
        pages_scanned = 0

        for item in html_posts:
            posts_checked += 1
            create_time = self._coerce_timestamp(item.get("createTime") or item.get("createTimeISO"))
            if create_time <= 0:
                continue

            in_range = config.is_in_date_range(create_time)
            if in_range is None:
                reached_date_limit = True
                break
            if in_range is False:
                continue

            description = str(item.get("desc") or item.get("text") or "")
            if config.matches_hashtags(description):
                post = self._parse_post_item(item, config)
                if not getattr(post, "author_nickname", None) and profile_snapshot.get("display_name"):
                    post.author_nickname = str(profile_snapshot.get("display_name") or "")
                if not getattr(post, "user_avatar_url", None) and profile_avatar_url:
                    post.user_avatar_url = profile_avatar_url
                if post.video_id and post.video_id not in existing_ids:
                    posts.append(post)
                    existing_ids.add(post.video_id)
                logger.info(f"Found #{len(posts)}: {post.video_id} ({post.date_time}) - {post.views:,} views")
        if html_posts:
            self._emit_progress(
                progress_cb,
                phase="scrape_html_page",
                pages_scanned=1,
                posts_checked=posts_checked,
                matched_posts=len(posts),
            )

        # Continue with API pagination if we have secUid and haven't hit date limit
        if sec_uid and use_api and not reached_date_limit:
            cursor = 0
            page_num = 0

            while not reached_date_limit:
                page_num += 1
                pages_scanned = page_num
                if config.max_pages and page_num > config.max_pages:
                    logger.info(f"Reached max pages limit ({config.max_pages})")
                    break

                logger.info(f"Fetching API page {page_num}...")
                data = self.fetch_posts(
                    config.username,
                    sec_uid,
                    cursor,
                    config.delay_seconds,
                    fast_mode=config.fast_mode,
                )
                if not data:
                    break

                items = data.get("itemList", [])
                if not items:
                    logger.info("No more posts found")
                    break

                for item in items:
                    posts_checked += 1
                    create_time = self._coerce_timestamp(item.get("createTime") or item.get("createTimeISO"))
                    if create_time <= 0:
                        continue

                    in_range = config.is_in_date_range(create_time)
                    if in_range is None:
                        reached_date_limit = True
                        break
                    if in_range is False:
                        continue

                    description = str(item.get("desc") or item.get("text") or "")
                    if config.matches_hashtags(description):
                        post = self._parse_post_item(item, config)
                        if not getattr(post, "author_nickname", None) and profile_snapshot.get("display_name"):
                            post.author_nickname = str(profile_snapshot.get("display_name") or "")
                        if not getattr(post, "user_avatar_url", None) and profile_avatar_url:
                            post.user_avatar_url = profile_avatar_url
                        # Avoid duplicates from HTML extraction / prior pages.
                        if post.video_id and post.video_id not in existing_ids:
                            posts.append(post)
                            existing_ids.add(post.video_id)
                            api_posts_found = True
                            logger.info(
                                f"Found #{len(posts)}: {post.video_id} ({post.date_time}) - {post.views:,} views"
                            )
                self._emit_progress(
                    progress_cb,
                    phase="scrape_api_page",
                    pages_scanned=page_num,
                    posts_checked=posts_checked,
                    matched_posts=len(posts),
                )

                has_more = data.get("hasMore", False)
                cursor = data.get("cursor", 0)
                if not has_more or not cursor:
                    logger.info("No more pages available")
                    break

                logger.info(f"Page {page_num}: checked {len(items)} posts, {len(posts)} matches total")

        logger.info(f"Scrape complete: checked {posts_checked} posts, found {len(posts)} matches")

        # yt-dlp fallback: always try when API/HTML found few or no matching posts,
        # since yt-dlp can paginate much deeper into the profile history.
        if len(posts) < 5 and self._has_ytdlp():
            logger.info(
                f"API/HTML found only {len(posts)} posts in date range; "
                "trying yt-dlp bulk fallback for deeper pagination..."
            )
            ytdlp_posts = self._scrape_via_ytdlp(
                config,
                max_videos_hint=config.ytdlp_max_videos_hint,
                max_posts_hint=config.ytdlp_max_videos_hint,
                progress_cb=progress_cb,
            )
            ytdlp_used = bool(ytdlp_posts)
            ytdlp_meta = dict(self.last_retrieval_meta or {})
            for p in ytdlp_posts:
                if p.video_id and p.video_id not in existing_ids:
                    posts.append(p)
                    existing_ids.add(p.video_id)
            if ytdlp_posts:
                posts_checked = max(
                    posts_checked,
                    self._safe_int_metric(ytdlp_meta.get("posts_checked") or ytdlp_meta.get("videos_scanned") or 0),
                    len(posts),
                )
                logger.info(f"yt-dlp added {len(ytdlp_posts)} posts (total now {len(posts)})")
                self._emit_progress(
                    progress_cb,
                    phase="scrape_ytdlp_fallback",
                    pages_scanned=pages_scanned,
                    posts_checked=max(posts_checked, len(posts)),
                    matched_posts=len(posts),
                )

        # Backfill missing thumbnails via oembed API
        missing_thumb = [p for p in posts if not p.thumbnail_url and p.url]
        if missing_thumb:
            logger.info(f"Backfilling thumbnails via oembed for {len(missing_thumb)} posts...")
            filled = 0
            for p in missing_thumb:
                thumb = self._fetch_oembed_thumbnail(p.url)
                if thumb:
                    p.thumbnail_url = thumb
                    filled += 1
            if filled:
                logger.info(f"Oembed backfill: {filled}/{len(missing_thumb)} thumbnails resolved")

        self._set_retrieval_meta(
            retrieval_mode=(
                "ytdlp_fallback" if ytdlp_used else ("api" if api_posts_found else ("html" if html_posts else "none"))
            ),
            api_fail_reason=self._last_api_fail_reason,
            api_preflight_fail_reason=api_preflight_fail_reason,
            api_pagination_blocked_reason=api_pagination_blocked_reason,
            pages_scanned=pages_scanned,
            posts_checked=posts_checked,
            videos_scanned=posts_checked,
            first_page_count=len(posts[:30]),
            total_posts=max(self._safe_int_metric(profile_snapshot.get("total_posts")), len(posts)),
            profile_snapshot=profile_snapshot,
        )
        return posts

    def fetch_comments(
        self,
        video_id: str,
        username: str | None = None,
        max_comments: int | None = None,
        fetch_replies: bool = True,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
        max_replies_per_comment: int | None = None,
        max_reply_pages: int | None = None,
        reply_fetch_deadline_seconds: float | None = None,
    ) -> list["TikTokComment"]:
        """
        Fetch comments for a TikTok video including replies.

        Args:
            video_id: The video ID (aweme_id)
            username: Username for building the post URL (optional)
            max_comments: Maximum number of top-level comments to fetch
            fetch_replies: Whether to fetch replies to comments
            delay: Delay between API requests
            max_replies_per_comment: Maximum replies to fetch for each top-level comment
            max_reply_pages: Maximum reply API pages to fetch for each top-level comment
            reply_fetch_deadline_seconds: Shared reply-fetch deadline for this video

        Returns:
            List of TikTokComment objects with nested replies
        """
        self._last_api_fail_reason = None
        self.last_comment_fetch_reason = None
        self.comments_auth_failed = False
        self.last_comment_fetch_meta = {}
        if not self._direct_comment_api_experiment_enabled():
            self.last_comment_fetch_reason = "direct_api_parked"
            self.last_comment_fetch_meta = {
                "retrieval_mode": "direct_api_parked",
                "reason": "direct_api_parked",
            }
            logger.warning("tiktok_comments_direct_api_parked video_id=%s", video_id)
            return []
        max_replies_per_comment = self._normalize_optional_non_negative_int(max_replies_per_comment)
        max_reply_pages = self._normalize_optional_non_negative_int(max_reply_pages)
        normalized_reply_deadline_seconds = self._normalize_optional_non_negative_float(reply_fetch_deadline_seconds)
        reply_deadline_at = (
            time.monotonic() + normalized_reply_deadline_seconds
            if normalized_reply_deadline_seconds is not None
            else None
        )
        self.last_comment_fetch_meta = {
            "retrieval_mode": "direct_api",
            "reply_cap_events": [],
            "reply_caps": {
                "max_replies_per_comment": max_replies_per_comment,
                "max_reply_pages": max_reply_pages,
                "reply_fetch_deadline_seconds": normalized_reply_deadline_seconds,
            },
        }
        post_url = f"https://www.tiktok.com/@{username}/video/{video_id}" if username else ""
        logger.info(f"Fetching comments for video {video_id}")

        comments = []
        cursor = 0
        comments_fetched = 0

        def _emit_progress(phase: str, **payload: Any) -> None:
            if progress_callback is None:
                return
            event = {
                "phase": phase,
                "video_id": video_id,
                "comments_fetched": comments_fetched,
                **payload,
            }
            try:
                progress_callback(event)
            except Exception:  # noqa: BLE001
                logger.debug("TikTok comment progress callback failed video_id=%s", video_id, exc_info=True)

        _emit_progress("fetch_tiktok_comments_start")

        while True:
            self._rate_limit(delay, fast_mode=fast_mode)

            params = {
                "aweme_id": video_id,
                "count": 50,
                "cursor": cursor,
                # TikTok's comments endpoints currently require aid=1988.
                "aid": 1988,
            }
            headers = self._get_headers(post_url or "https://www.tiktok.com/")
            _emit_progress("fetch_tiktok_comments_request", cursor=cursor)

            try:
                response = self.session.get(
                    self.COMMENTS_URL,
                    params=params,
                    headers=headers,
                    cookies=self.cookies,
                    timeout=self.COMMENT_REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                data = self._safe_response_json(response, endpoint="fetch_comments")
                if data is not None:
                    self._record_endpoint_response(endpoint="fetch_comments", response=response)
            except requests.exceptions.RequestException as e:
                self._set_comment_failure_reason("request_error")
                self._record_endpoint_response(
                    endpoint="fetch_comments",
                    failure_reason=self._last_api_fail_reason,
                    response=getattr(e, "response", None),
                )
                logger.error(f"Failed to fetch comments: {e}")
                break
            if not data:
                if self._last_api_fail_reason:
                    self._set_comment_failure_reason(self._last_api_fail_reason)
                else:
                    self._set_comment_failure_reason("empty_response")
                break

            status_code = int(data.get("status_code", 0) or 0)
            if status_code != 0:
                self._set_comment_failure_reason(f"comment_status_{status_code}")
                logger.warning(
                    "TikTok comments API returned non-zero status (video_id=%s status_code=%s status_msg=%s)",
                    video_id,
                    status_code,
                    data.get("status_msg", ""),
                )
                break

            # Parse comments
            for comment_data in data.get("comments", []):
                comment = self._parse_comment(comment_data, video_id, post_url)
                comments.append(comment)
                comments_fetched += 1

                # Fetch replies if requested and comment has replies
                if fetch_replies and comment.reply_count > 0:
                    replies = self._fetch_comment_replies(
                        video_id,
                        comment.comment_id,
                        post_url,
                        delay,
                        fast_mode=fast_mode,
                        progress_callback=progress_callback,
                        max_replies=max_replies_per_comment,
                        max_pages=max_reply_pages,
                        deadline_at=reply_deadline_at,
                        deadline_seconds=normalized_reply_deadline_seconds,
                    )
                    comment.replies = replies
                    logger.info(f"  Comment {comment.comment_id}: {len(replies)} replies fetched")

                if max_comments and comments_fetched >= max_comments:
                    break

            logger.info(f"Fetched {len(comments)} comments so far...")
            _emit_progress("fetch_tiktok_comments_page", cursor=cursor, comments_fetched=len(comments))

            # Check for more pages
            if max_comments and comments_fetched >= max_comments:
                break
            if not data.get("has_more", False):
                break
            cursor = data.get("cursor", 0)
            if not cursor:
                break

        logger.info(f"Total: {len(comments)} comments fetched for video {video_id}")
        _emit_progress("fetch_tiktok_comments_done", comments_fetched=len(comments))
        return comments

    def _fetch_comment_replies(
        self,
        video_id: str,
        comment_id: str,
        post_url: str,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
        max_replies: int | None = None,
        max_pages: int | None = None,
        deadline_at: float | None = None,
        deadline_seconds: float | None = None,
    ) -> list["TikTokComment"]:
        """Fetch replies to a specific comment."""
        replies = []
        cursor = 0
        pages_fetched = 0
        cap_recorded = False

        def _record_cap(reason: str, *, limit: Any = None) -> None:
            nonlocal cap_recorded
            if cap_recorded:
                return
            cap_recorded = True
            self._record_reply_cap_event(
                reason=reason,
                comment_id=comment_id,
                limit=limit,
                replies_fetched=len(replies),
                pages_fetched=pages_fetched,
            )

        def _emit_progress(phase: str, **payload: Any) -> None:
            if progress_callback is None:
                return
            event = {
                "phase": phase,
                "video_id": video_id,
                "comment_id": comment_id,
                "replies_fetched": len(replies),
                **payload,
            }
            try:
                progress_callback(event)
            except Exception:  # noqa: BLE001
                logger.debug(
                    "TikTok reply progress callback failed video_id=%s comment_id=%s",
                    video_id,
                    comment_id,
                    exc_info=True,
                )

        while True:
            if max_replies is not None and len(replies) >= max_replies:
                _record_cap("max_replies_per_comment", limit=max_replies)
                break
            if max_pages is not None and pages_fetched >= max_pages:
                _record_cap("max_reply_pages", limit=max_pages)
                break
            if deadline_at is not None and time.monotonic() >= deadline_at:
                _record_cap("reply_deadline_seconds", limit=deadline_seconds)
                break
            self._rate_limit(delay, fast_mode=fast_mode)

            params = {
                "item_id": video_id,
                "comment_id": comment_id,
                "count": 50,
                "cursor": cursor,
                # TikTok's replies endpoint currently requires aid=1988.
                "aid": 1988,
            }
            headers = self._get_headers(post_url or "https://www.tiktok.com/")
            _emit_progress("fetch_tiktok_comment_replies_request", cursor=cursor)

            try:
                response = self.session.get(
                    self.COMMENT_REPLIES_URL,
                    params=params,
                    headers=headers,
                    cookies=self.cookies,
                    timeout=self.COMMENT_REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                data = self._safe_response_json(response, endpoint="fetch_comment_replies")
                if data is not None:
                    self._record_endpoint_response(endpoint="fetch_comment_replies", response=response)
            except requests.exceptions.RequestException as e:
                self._set_comment_failure_reason("request_error")
                self._record_endpoint_response(
                    endpoint="fetch_comment_replies",
                    failure_reason=self._last_api_fail_reason,
                    response=getattr(e, "response", None),
                )
                logger.error(f"Failed to fetch replies for comment {comment_id}: {e}")
                break
            if not data:
                if self._last_api_fail_reason:
                    self._set_comment_failure_reason(self._last_api_fail_reason)
                else:
                    self._set_comment_failure_reason("empty_response")
                break

            status_code = int(data.get("status_code", 0) or 0)
            if status_code != 0:
                self._set_comment_failure_reason(f"reply_status_{status_code}")
                logger.warning(
                    "TikTok replies API returned non-zero status (comment_id=%s status_code=%s status_msg=%s)",
                    comment_id,
                    status_code,
                    data.get("status_msg", ""),
                )
                break
            pages_fetched += 1

            # Parse reply comments
            for reply_data in data.get("comments", []):
                if max_replies is not None and len(replies) >= max_replies:
                    _record_cap("max_replies_per_comment", limit=max_replies)
                    break
                reply = self._parse_comment(reply_data, video_id, post_url, is_reply=True, parent_id=comment_id)
                replies.append(reply)
            _emit_progress("fetch_tiktok_comment_replies_page", cursor=cursor, replies_fetched=len(replies))
            if max_replies is not None and len(replies) >= max_replies:
                _record_cap("max_replies_per_comment", limit=max_replies)
                break

            # Check for more pages
            if not data.get("has_more", False):
                break
            if max_pages is not None and pages_fetched >= max_pages:
                _record_cap("max_reply_pages", limit=max_pages)
                break
            cursor = data.get("cursor", 0)
            if not cursor:
                break

        return replies

    def fetch_comments_concurrent(
        self,
        video_ids: list[tuple[str, str | None]],
        max_comments: int | None = None,
        fetch_replies: bool = True,
        delay: float = 2.0,
        *,
        fast_mode: bool = False,
        max_workers: int | None = None,
        max_replies_per_comment: int | None = None,
        max_reply_pages: int | None = None,
        reply_fetch_deadline_seconds: float | None = None,
    ) -> dict[str, list["TikTokComment"]]:
        """Fetch comments for multiple videos concurrently.

        Args:
            video_ids: List of (video_id, username) tuples
            max_comments: Max comments per video
            fetch_replies: Whether to fetch reply chains
            delay: Base delay between requests
            fast_mode: Use aggressive rate limiting
            max_workers: Concurrency level (default from env or 3)
            max_replies_per_comment: Maximum replies to fetch for each top-level comment
            max_reply_pages: Maximum reply API pages to fetch for each top-level comment
            reply_fetch_deadline_seconds: Shared reply-fetch deadline for each video

        Returns:
            Dict mapping video_id -> list of comments
        """
        if max_workers is None:
            max_workers = int(os.getenv(ENV_TIKTOK_COMMENT_CONCURRENCY, "3"))
        max_workers = max(1, min(max_workers, 8))

        if len(video_ids) <= 1 or max_workers <= 1:
            result: dict[str, list[TikTokComment]] = {}
            for vid, uname in video_ids:
                result[vid] = self.fetch_comments(
                    vid,
                    username=uname,
                    max_comments=max_comments,
                    fetch_replies=fetch_replies,
                    delay=delay,
                    fast_mode=fast_mode,
                    max_replies_per_comment=max_replies_per_comment,
                    max_reply_pages=max_reply_pages,
                    reply_fetch_deadline_seconds=reply_fetch_deadline_seconds,
                )
            return result

        rate_lock = threading.Lock()
        original_rate_limit = self._rate_limit

        def _synchronized_rate_limit(d: float, *, fast_mode: bool = False):
            with rate_lock:
                original_rate_limit(d, fast_mode=fast_mode)

        def _fetch_one(vid: str, uname: str | None) -> tuple[str, list[TikTokComment]]:
            self._rate_limit = _synchronized_rate_limit  # type: ignore[assignment]
            try:
                comments = self.fetch_comments(
                    vid,
                    username=uname,
                    max_comments=max_comments,
                    fetch_replies=fetch_replies,
                    delay=delay,
                    fast_mode=fast_mode,
                    max_replies_per_comment=max_replies_per_comment,
                    max_reply_pages=max_reply_pages,
                    reply_fetch_deadline_seconds=reply_fetch_deadline_seconds,
                )
                return vid, comments
            except Exception as exc:
                logger.error("Concurrent TikTok comment fetch failed for %s: %s", vid, exc)
                return vid, []

        logger.info(
            "Fetching TikTok comments for %d videos concurrently (workers=%d, fast=%s)",
            len(video_ids),
            max_workers,
            fast_mode,
        )
        results: dict[str, list[TikTokComment]] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch_one, vid, uname): vid for vid, uname in video_ids}
            completed = 0
            for future in as_completed(futures):
                vid, comments = future.result()
                results[vid] = comments
                completed += 1
                if completed % 10 == 0:
                    logger.info("TikTok concurrent comments: %d/%d done", completed, len(video_ids))

        self._rate_limit = original_rate_limit  # type: ignore[assignment]
        logger.info(
            "TikTok concurrent comment fetch complete: %d videos, %d total comments",
            len(results),
            sum(len(v) for v in results.values()),
        )
        return results

    def _fetch_oembed_thumbnail(self, video_url: str) -> str | None:
        """Fetch thumbnail URL from TikTok's public oembed endpoint."""
        try:
            resp = self.session.get(
                TIKTOK_OEMBED_URL,
                params={"url": video_url},
                timeout=10,
            )
            if resp.status_code == 200:
                self._record_endpoint_response(endpoint="fetch_oembed_thumbnail", response=resp)
                data = resp.json()
                return data.get("thumbnail_url") or None
        except Exception:
            pass
        return None


__all__ = [
    "WebApiTransportMixin",
]
