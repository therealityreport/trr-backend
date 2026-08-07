"""Official YouTube Data API client used by the hybrid ingest path."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)

YOUTUBE_DATA_API_BASE_URL = "https://www.googleapis.com/youtube/v3"
YOUTUBE_DATA_API_TIMEOUT_SECONDS = 20


def _coerce_utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")


class YouTubeDataApiClient:
    """Small wrapper around the official YouTube Data API."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        session: requests.Session | None = None,
        timeout_seconds: int = YOUTUBE_DATA_API_TIMEOUT_SECONDS,
    ) -> None:
        self.api_key = str(api_key or os.getenv("SOCIAL_AUTH_YOUTUBE_API_KEY") or "").strip()
        self.session = session or requests.Session()
        self.timeout_seconds = max(5, int(timeout_seconds or YOUTUBE_DATA_API_TIMEOUT_SECONDS))

    def enabled(self) -> bool:
        return bool(self.api_key)

    def _request(self, path: str, *, params: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled():
            raise RuntimeError("YouTube Data API key is not configured")
        request_params = {**params, "key": self.api_key}
        response = self.session.get(
            f"{YOUTUBE_DATA_API_BASE_URL}{path}",
            params=request_params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def resolve_channel(self, handle: str) -> dict[str, Any] | None:
        normalized_handle = str(handle or "").strip().lstrip("@")
        if not normalized_handle:
            return None

        try:
            payload = self._request(
                "/channels",
                params={
                    "part": "snippet,contentDetails,statistics",
                    "forHandle": normalized_handle,
                    "maxResults": 1,
                },
            )
        except Exception:
            logger.debug("YouTube API channel resolve failed for handle=%s", normalized_handle, exc_info=True)
            return None

        items = payload.get("items")
        if not isinstance(items, list) or not items:
            return None
        item = items[0] if isinstance(items[0], dict) else {}
        snippet_raw = item.get("snippet")
        snippet = snippet_raw if isinstance(snippet_raw, dict) else {}
        content_details_raw = item.get("contentDetails")
        content_details = content_details_raw if isinstance(content_details_raw, dict) else {}
        related_raw = content_details.get("relatedPlaylists")
        related = related_raw if isinstance(related_raw, dict) else {}
        return {
            "channel_id": str(item.get("id") or "").strip() or None,
            "title": str(snippet.get("title") or "").strip() or None,
            "canonical_handle": normalized_handle,
            "uploads_playlist_id": str(related.get("uploads") or "").strip() or None,
            "payload": item,
        }

    def list_upload_playlist_items(
        self,
        uploads_playlist_id: str,
        *,
        page_token: str | None = None,
        max_results: int = 50,
    ) -> dict[str, Any]:
        return self._request(
            "/playlistItems",
            params={
                "part": "contentDetails,snippet,status",
                "playlistId": uploads_playlist_id,
                "pageToken": str(page_token or "").strip() or None,
                "maxResults": max(1, min(int(max_results or 50), 50)),
            },
        )

    def list_videos(self, video_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
        normalized_ids = [str(item or "").strip() for item in video_ids if str(item or "").strip()]
        if not normalized_ids:
            return {}
        payload = self._request(
            "/videos",
            params={
                "part": "snippet,contentDetails,statistics,status,topicDetails,liveStreamingDetails",
                "id": ",".join(normalized_ids[:50]),
                "maxResults": min(len(normalized_ids), 50),
            },
        )
        results: dict[str, dict[str, Any]] = {}
        for item in payload.get("items") or []:
            if not isinstance(item, dict):
                continue
            video_id = str(item.get("id") or "").strip()
            if not video_id:
                continue
            results[video_id] = item
        return results

    def list_channel_videos(
        self,
        *,
        handle: str,
        page_token: str | None = None,
        published_after: datetime | None = None,
        published_before: datetime | None = None,
        max_pages: int = 4,
    ) -> dict[str, Any]:
        channel = self.resolve_channel(handle)
        uploads_playlist_id = str((channel or {}).get("uploads_playlist_id") or "").strip()
        if not uploads_playlist_id:
            return {
                "channel": channel,
                "items": [],
                "next_page_token": None,
                "api_calls": 0,
            }

        items: list[dict[str, Any]] = []
        next_page_token = str(page_token or "").strip() or None
        api_calls = 1 if channel else 0
        seen_ids: list[str] = []
        after_iso = _coerce_utc_iso(published_after)
        before_iso = _coerce_utc_iso(published_before)

        for _ in range(max(1, int(max_pages or 1))):
            payload = self.list_upload_playlist_items(
                uploads_playlist_id,
                page_token=next_page_token,
                max_results=50,
            )
            api_calls += 1
            page_items_raw = payload.get("items")
            page_items = page_items_raw if isinstance(page_items_raw, list) else []
            for page_item in page_items:
                if not isinstance(page_item, dict):
                    continue
                content_details_raw = page_item.get("contentDetails")
                content_details = content_details_raw if isinstance(content_details_raw, dict) else {}
                snippet_raw = page_item.get("snippet")
                snippet = snippet_raw if isinstance(snippet_raw, dict) else {}
                resource_id_raw = snippet.get("resourceId")
                resource_id = resource_id_raw if isinstance(resource_id_raw, dict) else {}
                video_id = str(content_details.get("videoId") or resource_id.get("videoId") or "").strip()
                published_at = str(content_details.get("videoPublishedAt") or snippet.get("publishedAt") or "").strip()
                if not video_id:
                    continue
                if after_iso and published_at and published_at < after_iso:
                    continue
                if before_iso and published_at and published_at > before_iso:
                    continue
                items.append(page_item)
                seen_ids.append(video_id)
            next_page_token = str(payload.get("nextPageToken") or "").strip() or None
            if not next_page_token:
                break

        detail_rows: dict[str, dict[str, Any]] = {}
        for index in range(0, len(seen_ids), 50):
            batch = seen_ids[index : index + 50]
            if not batch:
                continue
            detail_rows.update(self.list_videos(batch))
            api_calls += 1

        merged_items: list[dict[str, Any]] = []
        for item in items:
            content_details_raw = item.get("contentDetails")
            content_details = content_details_raw if isinstance(content_details_raw, dict) else {}
            snippet_raw = item.get("snippet")
            snippet = snippet_raw if isinstance(snippet_raw, dict) else {}
            video_id = str(content_details.get("videoId") or snippet.get("resourceId", {}).get("videoId") or "").strip()
            if not video_id:
                continue
            merged_items.append(
                {
                    "video_id": video_id,
                    "playlist_item": item,
                    "video": detail_rows.get(video_id) or {},
                }
            )

        return {
            "channel": channel,
            "items": merged_items,
            "next_page_token": next_page_token,
            "api_calls": api_calls,
        }

    def list_comment_threads(
        self,
        video_id: str,
        *,
        page_token: str | None = None,
        max_results: int = 100,
    ) -> dict[str, Any]:
        return self._request(
            "/commentThreads",
            params={
                "part": "snippet,replies",
                "videoId": str(video_id or "").strip(),
                "order": "time",
                "pageToken": str(page_token or "").strip() or None,
                "maxResults": max(1, min(int(max_results or 100), 100)),
                "textFormat": "plainText",
            },
        )

    def list_replies(
        self,
        parent_id: str,
        *,
        page_token: str | None = None,
        max_results: int = 100,
    ) -> dict[str, Any]:
        return self._request(
            "/comments",
            params={
                "part": "snippet",
                "parentId": str(parent_id or "").strip(),
                "pageToken": str(page_token or "").strip() or None,
                "maxResults": max(1, min(int(max_results or 100), 100)),
                "textFormat": "plainText",
            },
        )
