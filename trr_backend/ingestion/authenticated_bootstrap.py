"""Read the initial comment connection from one bound WWW document.

This pure extractor does not execute ServerJS, resolve a session, or label an
initial container response as a pagination-query response.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from urllib.parse import urlsplit

from trr_backend.ingestion.evidence import sanitize_payload
from trr_backend.socials.instagram.posts_scrapling.fetcher import _DocumentJsonScripts

PARENT_CONNECTION = "xdt_api__v1__media__media_id__comments__connection"
CONTAINER_PRELOADER_PREFIX = "adp_PolarisPostCommentsContainerQueryRelayPreloader_"
_MAX_DOCUMENT_BYTES = 8 * 1024 * 1024
_MAX_ITEMS = 10_000


class AuthenticatedBootstrapError(ValueError):
    """The bound document cannot prove one complete initial connection."""

    def __init__(
        self,
        *,
        failure_code: str = "source_incomplete",
        retry_after_seconds: float | None = None,
        reason: str = "invalid_structure",
    ):
        self.reason = reason
        self.failure_code = failure_code
        self.retry_after_seconds = retry_after_seconds
        super().__init__("native_comments_bootstrap_invalid")


def _invalid(*, failure_code="source_incomplete", retry_after_seconds=None, reason="invalid_structure"):
    # Never put source text, URLs, tokens or exception details in this channel.
    return AuthenticatedBootstrapError(
        failure_code=failure_code, retry_after_seconds=retry_after_seconds, reason=reason
    )


class _Document(_DocumentJsonScripts):
    def __init__(self):
        super().__init__()
        self.media_urls = []

    def handle_starttag(self, tag, attrs):
        super().handle_starttag(tag, attrs)
        attributes = dict(attrs)
        if tag == "link" and "canonical" in str(attributes.get("rel") or "").lower().split():
            self.media_urls.append(attributes.get("href"))
        if tag == "meta" and str(attributes.get("property") or "").lower() == "og:url":
            self.media_urls.append(attributes.get("content"))


def _bind_url(value, shortcode):
    if not isinstance(value, str) or not value or value != value.strip():
        raise _invalid(reason="document_url_mismatch")
    try:
        parts = urlsplit(value)
        if (
            parts.scheme != "https"
            or parts.netloc not in {"www.instagram.com", "instagram.com"}
            or parts.query
            or parts.fragment
        ):
            raise _invalid(reason="document_url_mismatch")
        match = re.fullmatch(r"/(?:[A-Za-z0-9._]{1,30}/)?(?:p|reel|tv)/([A-Za-z0-9_-]{1,40})/?", parts.path)
        if match is None or match[1] != shortcode:
            raise _invalid(reason="document_url_mismatch")
    except ValueError:
        raise _invalid(reason="document_url_mismatch") from None


def _connection(result):
    from trr_backend.ingestion.comments import comment_page_failure
    from trr_backend.socials.instagram.graphql_response import _primitive_failure

    if not isinstance(result, dict):
        raise _invalid()
    primitive = _primitive_failure(result)
    failure = comment_page_failure(result)
    # The comment classifier can find a stronger nested/rate failure even when
    # the envelope primitive stops at a generic error. Keep its rate precedence.
    if failure and failure[0] in {"rate_limited", "source_auth_failed"}:
        raise _invalid(failure_code=failure[0], retry_after_seconds=failure[1])
    if primitive and primitive.auth_failed:
        raise _invalid(failure_code="source_auth_failed")
    if primitive and primitive.reason == "rate_limited":
        raise _invalid(failure_code="rate_limited")

    if (
        primitive
        or failure
        or result.get("errors")
        or result.get("error") is not None
        or result.get("status") not in (None, "ok")
        or "incremental" in result
        or "hasNext" in result
    ):
        raise _invalid()
    data = result.get("data")
    connection = data.get(PARENT_CONNECTION) if isinstance(data, dict) else None
    if not isinstance(connection, dict):
        raise _invalid()
    edges, info = connection.get("edges"), connection.get("page_info")
    if (
        not isinstance(edges, list)
        or any(
            not isinstance(edge, dict) or not isinstance(edge.get("node"), dict) or not edge["node"] for edge in edges
        )
        or not isinstance(info, dict)
        or type(info.get("has_next_page")) is not bool
    ):
        raise _invalid()
    cursor = info.get("end_cursor")
    if (cursor is not None and (not isinstance(cursor, str) or not cursor.strip())) or (
        info["has_next_page"] and cursor is None
    ):
        raise _invalid()
    return connection


def extract_parent_bootstrap(html: str, *, shortcode: str, document_url: str) -> dict:
    """Return sanitized native content, without cursor/document provenance.

    ``document_url`` must be the caller's successful, nonredirected GET URL.
    This explicit binding is required even when canonical/OG metadata is absent;
    every such metadata URL, when present, must name the same shortcode.
    Only direct ScheduledServerJS -> RelayPrefetchedStreamCache.next entries
    qualify. Nested lookalikes, patches and incomplete stream frames cannot.
    """
    if not isinstance(shortcode, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", shortcode):
        raise _invalid()
    _bind_url(document_url, shortcode)
    if not isinstance(html, str):
        raise _invalid()
    try:
        if len(html) > _MAX_DOCUMENT_BYTES or len(html.encode("utf-8")) > _MAX_DOCUMENT_BYTES:
            raise _invalid()
        document = _Document()
        document.feed(html)
        document.close()
        for url in document.media_urls:
            _bind_url(url, shortcode)
        if len(document.scripts) > _MAX_ITEMS:
            raise _invalid()
        matches = []
        visited = 0
        for script in document.scripts:
            root = json.loads(script)
            if not isinstance(root, dict) or not isinstance(root.get("require"), list):
                continue
            for outer in root["require"]:
                visited += 1
                if visited > _MAX_ITEMS:
                    raise _invalid()
                if not isinstance(outer, list) or outer[:2] != ["ScheduledServerJS", "handle"]:
                    continue
                if (
                    len(outer) != 4
                    or outer[2] is not None
                    or not isinstance(outer[3], list)
                    or len(outer[3]) not in (1, 3)
                    or not isinstance(outer[3][0], dict)
                ):
                    raise _invalid(reason="serverjs_call_shape_invalid")
                box = outer[3][0].get("__bbox")
                if not isinstance(box, dict):
                    raise _invalid()
                inner_rows = box.get("require", [])
                if not isinstance(inner_rows, list):
                    raise _invalid()
                for inner in inner_rows:
                    visited += 1
                    if visited > _MAX_ITEMS:
                        raise _invalid()
                    if not isinstance(inner, list) or inner[:2] != ["RelayPrefetchedStreamCache", "next"]:
                        continue
                    if len(inner) != 4 or inner[2] != [] or not isinstance(inner[3], list) or len(inner[3]) != 2:
                        raise _invalid(reason="relay_call_shape_invalid")
                    preloader, wrapped = inner[3]
                    if not isinstance(preloader, str) or not preloader.startswith(CONTAINER_PRELOADER_PREFIX):
                        continue
                    if not re.fullmatch(re.escape(CONTAINER_PRELOADER_PREFIX) + r"[A-Za-z0-9_-]{1,200}", preloader):
                        raise _invalid()
                    frame = wrapped.get("__bbox") if isinstance(wrapped, Mapping) else None
                    if (
                        not isinstance(frame, dict)
                        or frame.get("complete") is not True
                        or type(frame.get("sequence_number")) is not int
                        or frame["sequence_number"] != 0
                    ):
                        raise _invalid(reason="comments_stream_incomplete")
                    matches.append(_connection(frame.get("result")))
                    if len(matches) > 1:
                        raise _invalid(reason="comments_preloader_ambiguous")
        if len(matches) != 1:
            raise _invalid(reason="comments_preloader_missing")
        payload = sanitize_payload({"data": {PARENT_CONNECTION: matches[0]}})
        _connection(payload)
        return payload
    except AuthenticatedBootstrapError:
        raise
    except (ValueError, RecursionError, UnicodeError):
        raise _invalid(reason="document_decoding_failed") from None
