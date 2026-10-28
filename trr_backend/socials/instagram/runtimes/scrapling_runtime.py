"""Opt-in Scrapling Instagram runtime canary.

This runtime stays disabled unless `INSTAGRAM_SCRAPLING_RUNTIME_ENABLED` is
truthy. The dispatcher can then keep Scrapling in the priority list without
routing production traffic to it by default.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
from dataclasses import replace
from typing import Any

from trr_backend.socials._scrapling_http_utils import env_truthy, response_text, status_code
from trr_backend.socials.instagram.constants import (
    GRAPHQL_URL,
    PERMALINK_URL,
    PROFILE_INFO_URL,
    PROFILE_POSTS_FRIENDLY_NAME,
    PROFILE_POSTS_ROOT_FIELD_NAME,
    resolve_profile_posts_doc_ids,
)
from trr_backend.socials.instagram.post_normalizer import normalize_instagram_post
from trr_backend.socials.instagram.public_post_extractor import parse_public_post_from_html
from trr_backend.socials.instagram.runtimes.crawlee_runtime import _node_to_post
from trr_backend.socials.instagram.runtimes.protocol import (
    InstagramRuntime,
    Post,
    PostDetail,
    ProfileInfo,
    RuntimeHealth,
    RuntimeUnsupported,
)
from trr_backend.socials.scrapling_transport import DEFAULT_TRANSPORT, build_fetcher

logger = logging.getLogger(__name__)

_ENABLED_ENV = "INSTAGRAM_SCRAPLING_RUNTIME_ENABLED"


class ScraplingRuntime:
    name = "scrapling"

    def __init__(self, cookies: dict[str, str] | None = None) -> None:
        self._cookies = dict(cookies or {})

    def healthcheck(self) -> RuntimeHealth:
        if not env_truthy(_ENABLED_ENV, False):
            return RuntimeHealth(healthy=False, reason=f"{_ENABLED_ENV.lower()}_not_enabled")
        try:
            # This is the exact constructor seam used by _fetch_json. It
            # deliberately makes no request, browser launch, or network call.
            fetcher = build_fetcher()
        except Exception as exc:  # noqa: BLE001
            return RuntimeHealth(
                healthy=False,
                reason=f"scrapling_fetcher_construction_failed: {exc.__class__.__name__}",
            )
        if not any(callable(getattr(fetcher, method_name, None)) for method_name in ("async_fetch", "fetch", "get")):
            return RuntimeHealth(healthy=False, reason="scrapling_fetcher_fetch_method_unavailable")
        return RuntimeHealth(healthy=True)

    async def fetch_profile(self, username: str) -> ProfileInfo:
        payload = await self._fetch_json(PROFILE_INFO_URL, params={"username": username})
        user = _profile_user(payload)
        if not user:
            raise RuntimeUnsupported(f"ScraplingRuntime returned empty profile payload for {username}")
        return ProfileInfo(
            username=str(user.get("username") or username),
            user_id=str(user.get("id") or user.get("pk") or ""),
            full_name=(user.get("full_name") or None),
            biography=(user.get("biography") or None),
            follower_count=_safe_int(_nested_count(user, "edge_followed_by") or user.get("follower_count")),
            following_count=_safe_int(_nested_count(user, "edge_follow") or user.get("following_count")),
            post_count=_safe_int(_nested_count(user, "edge_owner_to_timeline_media") or user.get("media_count")),
            is_private=bool(user.get("is_private")),
            is_verified=bool(user.get("is_verified")),
        )

    async def fetch_posts(self, username: str, *, limit: int) -> list[Post]:
        if limit <= 0:
            return []
        payload = await self._fetch_json(PROFILE_INFO_URL, params={"username": username})
        posts: list[Post] = []
        seen_posts: set[str] = set()
        seen_cursors: set[str] = set()
        while True:
            edges = _timeline_edges(payload)
            if not edges:
                raise RuntimeUnsupported(f"ScraplingRuntime returned empty posts payload for {username}")
            for edge in edges:
                node = edge.get("node")
                if not isinstance(node, dict):
                    continue
                post = _post_from_node(node)
                if post.shortcode and post.shortcode not in seen_posts:
                    posts.append(post)
                    seen_posts.add(post.shortcode)
                    if len(posts) >= limit:
                        return posts
            connection = _timeline_connection(payload)
            page_info = connection.get("page_info") or {}
            if not page_info.get("has_next_page"):
                break
            cursor = str(page_info.get("end_cursor") or "").strip()
            if not cursor or cursor in seen_cursors:
                raise RuntimeUnsupported("ScraplingRuntime pagination cursor did not advance")
            seen_cursors.add(cursor)
            # Bound even malformed feeds that keep returning duplicate posts.
            if len(seen_cursors) > limit:
                raise RuntimeUnsupported("ScraplingRuntime pagination made insufficient progress")
            count = min(33, limit - len(posts))
            payload = await self._fetch_json(
                GRAPHQL_URL,
                data={
                    "doc_id": resolve_profile_posts_doc_ids()[0],
                    "fb_api_caller_class": "RelayModern",
                    "fb_api_req_friendly_name": PROFILE_POSTS_FRIENDLY_NAME,
                    "variables": json.dumps(
                        {
                            "username": username,
                            "after": cursor,
                            "before": None,
                            "first": count,
                            "last": None,
                            "data": {"count": count},
                        }
                    ),
                },
            )
        if not posts:
            raise RuntimeUnsupported(f"ScraplingRuntime returned no supported posts for {username}")
        return posts

    async def fetch_post_detail(self, shortcode: str) -> PostDetail:
        response = await self._fetch_response(PERMALINK_URL.format(shortcode=shortcode))
        html = response_text(response)
        extracted = parse_public_post_from_html(html, shortcode=shortcode)
        if extracted is not None:
            node = extracted.to_raw_media()
        else:
            try:
                node = _detail_node(_response_json(response))
            except (TypeError, ValueError):
                node = {}
        if not node:
            raise RuntimeUnsupported(f"ScraplingRuntime returned unsupported detail payload for {shortcode}")

        post = _post_from_node(node)
        if not post.shortcode:
            raise RuntimeUnsupported(f"ScraplingRuntime returned detail without shortcode for {shortcode}")

        normalized = normalize_instagram_post(node)
        return PostDetail(
            post=post,
            hashtags=tuple(normalized.hashtags),
            mentions=tuple(normalized.mentions),
            permalink=normalized.permalink or PERMALINK_URL.format(shortcode=post.shortcode),
        )

    async def _fetch_response(
        self, url: str, *, params: dict[str, Any] | None = None, data: dict[str, Any] | None = None
    ) -> Any:
        fetcher = build_fetcher()
        kwargs: dict[str, Any] = {
            "cookies": dict(self._cookies),
            # Browser timeouts use milliseconds; the basic HTTP fetcher uses seconds.
            "timeout": DEFAULT_TRANSPORT.timeout_ms / 1000,
            "follow_redirects": False,
            "retries": 1,
            # An explicit empty proxy disables libcurl environment proxy lookup.
            "proxies": {"all": ""},
            "headers": {"x-ig-app-id": "936619743392459"},
        }
        if data is not None:
            kwargs["data"] = data
        else:
            kwargs["params"] = params or {}
        response = await _call_fetcher(fetcher, url, **kwargs)
        status = status_code(response)
        if status >= 300:
            raise RuntimeUnsupported(f"ScraplingRuntime HTTP {status} for {url}")
        return response

    async def _fetch_json(
        self, url: str, *, params: dict[str, Any] | None = None, data: dict[str, Any] | None = None
    ) -> Any:
        response = await self._fetch_response(url, params=params, data=data)
        try:
            return _response_json(response)
        except (TypeError, ValueError) as exc:
            raise RuntimeUnsupported(f"ScraplingRuntime returned non-JSON payload for {url}") from exc


async def _call_fetcher(fetcher: Any, url: str, **kwargs: Any) -> Any:
    for method_name in ("post",) if "data" in kwargs else ("async_fetch", "fetch", "get"):
        method = getattr(fetcher, method_name, None)
        if not callable(method):
            continue
        if method_name == "async_fetch":
            result = method(url, **kwargs)
            if inspect.isawaitable(result):
                return await result
            return result
        result = await asyncio.to_thread(method, url, **kwargs)
        return await result if inspect.isawaitable(result) else result
    raise RuntimeUnsupported("ScraplingRuntime fetcher has no supported fetch method")


def _post_from_node(node: dict[str, Any]) -> Post:
    post = _node_to_post(node)
    normalized = normalize_instagram_post(node)
    return replace(
        post,
        caption=normalized.caption.text or post.caption,
        media_urls=tuple(normalized.media_urls) or post.media_urls,
        media_type=normalized.media_type or post.media_type,
        like_count=_safe_int(node.get("like_count")) if "like_count" in node else post.like_count,
        comment_count=_safe_int(node.get("comment_count")) if "comment_count" in node else post.comment_count,
    )


def _response_json(response: Any) -> Any:
    json_method = getattr(response, "json", None)
    if callable(json_method):
        return json_method()
    return json.loads(response_text(response))


def _profile_user(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("user"), dict):
        return data["user"]
    user = payload.get("user")
    if isinstance(user, dict):
        return user
    if isinstance(payload.get("username"), str):
        return payload
    return {}


def _timeline_connection(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    timeline = _profile_user(payload).get("edge_owner_to_timeline_media")
    if isinstance(timeline, dict):
        return timeline
    data = payload.get("data")
    connection = data.get(PROFILE_POSTS_ROOT_FIELD_NAME) if isinstance(data, dict) else None
    return connection if isinstance(connection, dict) else {}


def _timeline_edges(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []

    user = _profile_user(payload)
    timeline = user.get("edge_owner_to_timeline_media") if isinstance(user, dict) else None
    edges = timeline.get("edges") if isinstance(timeline, dict) else None
    if isinstance(edges, list):
        return [edge for edge in edges if isinstance(edge, dict)]

    data = payload.get("data")
    connection = data.get(PROFILE_POSTS_ROOT_FIELD_NAME) if isinstance(data, dict) else None
    edges = connection.get("edges") if isinstance(connection, dict) else None
    if isinstance(edges, list):
        return [edge for edge in edges if isinstance(edge, dict)]
    return []


def _detail_node(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if isinstance(data, dict):
        shortcode_media = data.get("xdt_shortcode_media")
        if isinstance(shortcode_media, dict):
            return shortcode_media
    graphql = payload.get("graphql")
    if isinstance(graphql, dict) and isinstance(graphql.get("shortcode_media"), dict):
        return graphql["shortcode_media"]
    items = payload.get("items")
    if isinstance(items, list) and items and isinstance(items[0], dict):
        return items[0]
    node = payload.get("node")
    if isinstance(node, dict):
        return node
    if any(key in payload for key in ("shortcode", "code", "pk", "id")):
        return payload
    return {}


def _nested_count(mapping: dict[str, Any], key: str) -> Any:
    value = mapping.get(key)
    if isinstance(value, dict):
        return value.get("count")
    return None


def _safe_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


# Protocol conformance check (runs at import; cheap).
assert isinstance(ScraplingRuntime(), InstagramRuntime)  # type: ignore[misc]

# Prevent RuntimeUnsupported from being flagged as unused (dispatcher imports it).
_ = RuntimeUnsupported
