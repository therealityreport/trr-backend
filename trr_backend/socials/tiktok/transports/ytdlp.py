"""yt-dlp transport for the TikTok scraper: profile listing, single-video metadata, and bulk scrape."""

import json
import logging
import shutil
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from trr_backend.socials.tiktok.models import TikTokPost, TikTokScrapeConfig, _coerce_utc_datetime
from trr_backend.socials.tiktok.parsing.ytdlp import find_ytdlp_cookie_file
from trr_backend.socials.tiktok.transports.ytdlp_auth import (
    build_ytdlp_auth_args,
)
from trr_backend.socials.tiktok.transports.ytdlp_auth import (
    remove_temporary_cookie_file as _remove_quietly,
)
from trr_backend.socials.tiktok.transports.ytdlp_auth import (
    write_netscape_cookie_file as _write_ytdlp_netscape_cookie_file,  # noqa: F401 - compatibility export
)

logger = logging.getLogger(__name__)


class YtDlpTransportMixin:
    """Scrape TikTok profiles through the ``yt-dlp`` CLI (no auth required)."""

    # yt-dlp based fallback methods (no auth required)

    @staticmethod
    def _has_ytdlp() -> bool:
        """Check if yt-dlp is available on the system."""
        return shutil.which("yt-dlp") is not None

    def _find_ytdlp_cookie_file(self) -> str | None:
        """Find a Netscape-format cookie file for yt-dlp."""
        return find_ytdlp_cookie_file()

    def _ytdlp_auth_args(self) -> tuple[list[str], dict[str, Any], str | None]:
        """Build yt-dlp cookie/proxy args from the same auth the scraper was given.

        Returns ``(args, meta, temp_path)``. ``temp_path`` is a cookie file this
        call created and the caller must remove after the subprocess exits.

        Cookie precedence: an operator-provided Netscape file, then the canonical
        cookie map passed to this scraper (written to an owner-only temp file).
        Set ``SOCIAL_TIKTOK_YTDLP_USE_CANONICAL_COOKIES=0`` to opt out of the
        canonical cookies. The proxy is opt-in via ``SOCIAL_TIKTOK_YTDLP_PROXY_URL``
        so existing deployments keep yt-dlp on the direct route by default.
        """
        return build_ytdlp_auth_args(self.cookies, cookie_file=self._find_ytdlp_cookie_file())

    def _ytdlp_list_videos(
        self,
        username: str,
        date_start: datetime | None = None,
        date_end: datetime | None = None,
        max_videos: int = 500,
    ) -> list[dict]:
        """
        List videos from a TikTok profile using yt-dlp --flat-playlist.

        Returns a list of dicts with id, upload_date, view_count, title.
        Note: TikTok's public profile pagination is limited (typically
        last ~50 videos). Date filtering is done in Python because
        yt-dlp's --dateafter/--datebefore flags don't work reliably
        with TikTok's flat-playlist mode.
        """
        if not self._has_ytdlp():
            logger.warning("yt-dlp not found; skipping yt-dlp fallback")
            return []

        url = f"https://www.tiktok.com/@{username}"
        cmd = [
            "yt-dlp",
            "--flat-playlist",
            "--print",
            "%(id)s\t%(upload_date)s\t%(view_count)s\t%(title)s",
            "--playlist-end",
            str(max_videos),
            url,
        ]

        # Format date boundaries for Python-side filtering
        ds_str = date_start.strftime("%Y%m%d") if date_start else None
        de_str = date_end.strftime("%Y%m%d") if date_end else None

        auth_args, _auth_meta, temp_cookie_path = self._ytdlp_auth_args()
        cmd[-1:-1] = auth_args
        logger.info(f"yt-dlp listing videos for @{username} (max {max_videos})...")
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        except subprocess.TimeoutExpired:
            logger.warning("yt-dlp listing timed out")
            return []
        finally:
            _remove_quietly(temp_cookie_path)
        if proc.returncode != 0:
            logger.warning("yt-dlp listing exited non-zero (code=%s)", proc.returncode)
            return []

        videos: list[dict] = []
        for line in proc.stdout.strip().splitlines():
            parts = line.split("\t", 3)
            if len(parts) < 2:
                continue
            vid_id = parts[0]
            upload_date = parts[1] if len(parts) > 1 else ""
            view_count = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            title = parts[3] if len(parts) > 3 else ""

            # Filter by date in Python
            if upload_date and ds_str and upload_date < ds_str:
                continue
            if upload_date and de_str and upload_date > de_str:
                continue

            videos.append(
                {
                    "id": vid_id,
                    "upload_date": upload_date,
                    "view_count": view_count,
                    "title": title,
                }
            )

        logger.info(f"yt-dlp listed {len(videos)} videos for @{username} (filtered to date range)")
        return videos

    def _ytdlp_get_video_metadata(self, video_url: str) -> dict | None:
        """Get full metadata for a single TikTok video via yt-dlp."""
        if not self._has_ytdlp():
            return None

        auth_args, _auth_meta, temp_cookie_path = self._ytdlp_auth_args()
        cmd = ["yt-dlp", "--dump-json", "--no-download", *auth_args, video_url]
        try:
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            finally:
                _remove_quietly(temp_cookie_path)
            if proc.returncode != 0:
                logger.warning("yt-dlp metadata exited non-zero (code=%s)", proc.returncode)
                return None
            return json.loads(proc.stdout)
        except (subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
            logger.warning("yt-dlp metadata failed (%s)", type(exc).__name__)
            return None

    def _scrape_via_ytdlp(
        self,
        config: TikTokScrapeConfig,
        *,
        max_videos_hint: int | None = None,
        max_posts_hint: int | None = None,
        progress_cb: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[TikTokPost]:
        """
        Fallback scraper using yt-dlp bulk mode.

        Uses --flat-playlist --dump-json to get full metadata in a single
        pass, avoiding slow per-video fetches. Automatically scales the
        playlist depth based on how far back the date range extends.
        When a Netscape-format cookie file is available, it's passed to
        yt-dlp for authenticated access.
        """
        if not self._has_ytdlp():
            self._set_retrieval_meta(
                context_mode="ytdlp",
                auth_mode="none",
                retrieval_mode="ytdlp",
                error_code="ytdlp_unavailable",
                stop_reason="ytdlp_unavailable",
                pages_scanned=0,
                posts_checked=0,
                videos_scanned=0,
                ytdlp_posts_found=0,
                ytdlp_max_videos=0,
                ytdlp_cookie_file_present=False,
                ytdlp_cookie_file_used=False,
                fallback_chain=["yt_dlp"],
            )
            return []

        logger.info("Attempting yt-dlp bulk fallback scraper...")

        # Estimate how many videos to fetch based on date range.
        # @bravotv posts ~16 videos/day across all shows; use 22/day for buffer.
        max_videos: int | None = None
        start_dt = _coerce_utc_datetime(config.date_start)
        end_dt = _coerce_utc_datetime(config.date_end)
        if start_dt:
            days_back = max(0, (datetime.now(tz=UTC) - start_dt).days)
            max_videos = max(500, min(12000, days_back * 22))
        config_max_videos_hint = (
            max(0, int(config.ytdlp_max_videos_hint or 0)) if config.ytdlp_max_videos_hint is not None else 0
        )
        if max_videos_hint is None and config_max_videos_hint > 0:
            max_videos_hint = config_max_videos_hint
        if max_posts_hint is None and max_videos_hint is not None and max_videos_hint > 0:
            max_posts_hint = max_videos_hint
        # With a date range, keep the date-based floor so older posts are not
        # silently missed. Without a date range, an explicit small hint is a
        # real canary/backfill cap and keeps fallback runs bounded.
        if max_videos_hint is not None and max_videos_hint > 0:
            if start_dt is None and max_posts_hint is not None:
                max_videos = min(25_000, max_videos_hint)
            elif max_videos is None or max_videos_hint > max_videos:
                max_videos = min(25_000, max_videos_hint)

        url = f"https://www.tiktok.com/@{config.username}"
        cmd = [
            "yt-dlp",
            "--flat-playlist",
            "--dump-json",
        ]
        if max_videos is not None:
            cmd.extend(["--playlist-end", str(max_videos)])

        # Pass the canonical cookies (or an operator Netscape file) and the
        # optional yt-dlp proxy so the production path uses refreshed auth.
        auth_args, ytdlp_auth_meta, temp_cookie_path = self._ytdlp_auth_args()
        cookie_file = bool(ytdlp_auth_meta.get("ytdlp_cookie_file_present"))
        cookie_file_used = cookie_file
        cmd.extend(auth_args)
        if cookie_file_used:
            logger.info("yt-dlp using cookies (source=%s)", ytdlp_auth_meta.get("ytdlp_cookie_source"))

        cmd.append(url)

        listing_scope = f"up to {max_videos} videos" if max_videos is not None else "entire playlist"
        logger.info("yt-dlp bulk listing @%s (%s)...", config.username, listing_scope)
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=max(120, max_videos // 3) if max_videos is not None else 900,
            )
        except subprocess.TimeoutExpired:
            logger.warning("yt-dlp bulk listing timed out")
            self._set_retrieval_meta(
                context_mode="ytdlp",
                auth_mode="ytdlp_cookies" if cookie_file_used else "none",
                retrieval_mode="ytdlp",
                error_code="ytdlp_timeout",
                stop_reason="timeout",
                pages_scanned=0,
                posts_checked=0,
                videos_scanned=0,
                ytdlp_posts_found=0,
                ytdlp_max_videos=max_videos,
                ytdlp_cookie_file_present=bool(cookie_file),
                ytdlp_cookie_file_used=cookie_file_used,
                ytdlp_cookie_source=ytdlp_auth_meta.get("ytdlp_cookie_source"),
                ytdlp_proxy_enabled=ytdlp_auth_meta.get("ytdlp_proxy_enabled"),
                fallback_chain=["yt_dlp"],
            )
            return []
        finally:
            _remove_quietly(temp_cookie_path)
        if int(getattr(proc, "returncode", 0) or 0) != 0:
            logger.warning("yt-dlp bulk listing exited non-zero (code=%s)", proc.returncode)
            self._set_retrieval_meta(
                context_mode="ytdlp",
                auth_mode="ytdlp_cookies" if cookie_file_used else "none",
                retrieval_mode="ytdlp",
                error_code="ytdlp_nonzero_exit",
                stop_reason="nonzero_exit",
                error_message=f"yt-dlp exited non-zero (code={proc.returncode})",
                pages_scanned=0,
                posts_checked=0,
                videos_scanned=0,
                ytdlp_posts_found=0,
                ytdlp_max_videos=max_videos,
                ytdlp_cookie_file_present=bool(cookie_file),
                ytdlp_cookie_file_used=cookie_file_used,
                ytdlp_cookie_source=ytdlp_auth_meta.get("ytdlp_cookie_source"),
                ytdlp_proxy_enabled=ytdlp_auth_meta.get("ytdlp_proxy_enabled"),
                fallback_chain=["yt_dlp"],
            )
            return []

        posts: list[TikTokPost] = []
        total = 0
        stop_reason = "playlist_exhausted"
        for line in proc.stdout.strip().splitlines():
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            total += 1

            ts = data.get("timestamp", 0) or 0
            item_dt = datetime.fromtimestamp(ts, tz=UTC)
            if start_dt and item_dt < start_dt:
                stop_reason = "date_start_reached"
                break  # Before date range, stop
            if end_dt and item_dt > end_dt:
                continue  # After date range, skip

            description = data.get("title", "") or data.get("description", "") or ""
            if not config.matches_hashtags(description):
                continue

            post = self._parse_ytdlp_metadata(data, config)
            posts.append(post)
            logger.info(f"Found #{len(posts)}: {post.video_id} ({post.date_time}) - {post.views:,} views")

            if progress_cb and (total == 1 or total % 100 == 0):
                try:
                    progress_cb(
                        {
                            "phase": "scrape_ytdlp_fallback",
                            "pages_scanned": 0,
                            "posts_checked": total,
                            "matched_posts": len(posts),
                            "total_posts": max_videos_hint or config.ytdlp_max_videos_hint or None,
                        }
                    )
                except Exception:
                    logger.debug("TikTok yt-dlp progress callback raised", exc_info=True)

            if max_posts_hint and len(posts) >= max_posts_hint:
                stop_reason = "max_posts_reached"
                break

        logger.info(f"yt-dlp bulk: scanned {total} videos, found {len(posts)} matches")
        self._set_retrieval_meta(
            context_mode="ytdlp",
            auth_mode="ytdlp_cookies" if cookie_file_used else "none",
            retrieval_mode="ytdlp",
            pages_scanned=0,
            posts_checked=total,
            videos_scanned=total,
            ytdlp_posts_found=len(posts),
            ytdlp_max_videos=max_videos,
            stop_reason=stop_reason,
            fallback_chain=["yt_dlp"],
            ytdlp_cookie_file_present=bool(cookie_file),
            ytdlp_cookie_file_used=cookie_file_used,
            ytdlp_cookie_source=ytdlp_auth_meta.get("ytdlp_cookie_source"),
            ytdlp_proxy_enabled=ytdlp_auth_meta.get("ytdlp_proxy_enabled"),
        )
        return posts


__all__ = [
    "YtDlpTransportMixin",
]
