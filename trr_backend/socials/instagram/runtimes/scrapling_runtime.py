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
from typing import Any

from trr_backend.socials._scrapling_http_utils import env_truthy, response_text, status_code
from trr_backend.socials.instagram.constants import PERMALINK_URL, PROFILE_INFO_URL, PROFILE_POSTS_ROOT_FIELD_NAME
from trr_backend.socials.instagram.post_normalizer import normalize_instagram_post
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
            import scrapling  # noqa: F401
        except ImportError as exc:
            return RuntimeHealth(
                healthy=False,
                reason=f"scrapling_not_installed: pip install scrapling ({exc})",
            )
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
        edges = _timeline_edges(payload)
        if not edges:
            raise RuntimeUnsupported(f"ScraplingRuntime returned empty posts payload for {username}")
        posts: list[Post] = []
        for edge in edges[:limit]:
            node = edge.get("node") if isinstance(edge, dict) else None
            if not isinstance(node, dict):
                continue
            post = _node_to_post(node)
            if post.shortcode:
                posts.append(post)
        if not posts:
            raise RuntimeUnsupported(f"ScraplingRuntime returned no supported posts for {username}")
        return posts

    async def fetch_post_detail(self, shortcode: str) -> PostDetail:
        payload = await self._fetch_json(PERMALINK_URL.format(shortcode=shortcode))
        node = _detail_node(payload)
        if not node:
            raise RuntimeUnsupported(f"ScraplingRuntime returned unsupported detail payload for {shortcode}")

        post = _node_to_post(node)
        if not post.shortcode:
            raise RuntimeUnsupported(f"ScraplingRuntime returned detail without shortcode for {shortcode}")

        normalized = normalize_instagram_post(node)
        return PostDetail(
            post=post,
            hashtags=tuple(normalized.hashtags),
            mentions=tuple(normalized.mentions),
            permalink=normalized.permalink or PERMALINK_URL.format(shortcode=post.shortcode),
        )

    async def _fetch_json(self, url: str, *, params: dict[str, Any] | None = None) -> Any:
        """Fetch JSON with Scrapling's basic fetcher.

        This helper intentionally avoids browser-backed fetchers. The canary
        runtime should prove lightweight JSON transport before moving policy or
        stateful browser behavior into this runtime.
        """
        fetcher = build_fetcher()
        kwargs: dict[str, Any] = {
            "params": params or {},
            "cookies": dict(self._cookies),
            "timeout": DEFAULT_TRANSPORT.timeout_ms,
        }
        response = await _call_fetcher(fetcher, url, **kwargs)
        status = status_code(response)
        if status >= 400:
            raise RuntimeUnsupported(f"ScraplingRuntime HTTP {status} for {url}")
        try:
            return _response_json(response)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeUnsupported(f"ScraplingRuntime returned non-JSON payload for {url}") from exc


async def _call_fetcher(fetcher: Any, url: str, **kwargs: Any) -> Any:
    for method_name in ("async_fetch", "fetch", "get"):
        method = getattr(fetcher, method_name, None)
        if not callable(method):
            continue
        if method_name == "async_fetch":
            result = method(url, **kwargs)
            if inspect.isawaitable(result):
                return await result
            return result
        return await asyncio.to_thread(method, url, **kwargs)
    raise RuntimeUnsupported("ScraplingRuntime fetcher has no supported fetch method")


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
