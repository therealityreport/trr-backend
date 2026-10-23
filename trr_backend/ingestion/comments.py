"""Native comment pages, offline replay, and fenced incremental persistence.

The source callback owns transport and credentials. Its observer runs before any
transport DTO parsing; only sealed, allowlisted evidence enters this module.
A page receipt is progress, never proof that an entire post was observed.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol, TypedDict
from uuid import uuid4

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.canonical import normalize_comment_source, write_comments
from trr_backend.ingestion.domain import Conflict, Failure, LostOwnership, Outcome, Stage, stable_json, utc_timestamp
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.persistence import Store
from trr_backend.socials.instagram.post_normalizer import InstagramComment

COMMENT_PARSER_VERSION = "instagram-comments-v1"
_CONNECTIONS = (
    "edge_media_to_parent_comment",
    "edge_media_to_comment",
    "xdt_api__v1__media__media_id__comments__connection",
    "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection",
)
_UNAVAILABLE = frozenset({"deleted", "comments_disabled", "restricted", "media_unavailable"})


class CommentSourceError(ValueError):
    """Evidence cannot safely prove a comment page."""


class CursorStalledError(CommentSourceError):
    """A retained lane repeated a cursor; an audited root restart is required."""

    failure = Failure("cursor_stalled")


@dataclass(frozen=True)
class CommentPage:
    observed_at: datetime
    comments: tuple[InstagramComment, ...]
    source_payloads: tuple[dict[str, Any], ...]
    parent_comment_id: str | None
    next_cursor: str | None
    exhausted: bool
    expected_count: int | None
    unavailable_reason: str | None = None
    cursor_param: str | None = None
    started_from_root: bool = False
    cursor_provenance: dict[str, Any] | None = None


@dataclass(frozen=True)
class CommentFetchSummary:
    expected_count: int | None = None
    fetch_failed: bool = False
    auth_failed: bool = False
    retryable: bool = False
    top_level_checkpoint: dict[str, Any] | None = None
    reply_checkpoints: tuple[dict[str, Any], ...] = ()
    full_observation: bool = False
    reply_lanes_rejected: tuple[str, ...] = ()
    reply_access_code: str | None = None
    reply_retry_after_seconds: float | None = None


@dataclass(frozen=True)
class CommentResumeNode:
    """Previously committed native content; never a new source observation."""

    source_payload: dict[str, Any]
    observed_at: datetime


class CommentSource(Protocol):
    def __call__(
        self,
        *,
        shortcode: str,
        checkpoint: dict[str, Any],
        resume_inventory: tuple[CommentResumeNode, ...],
        source_observer: Callable[[str, Mapping[str, Any]], None],
        pre_source_request: Callable[[], None],
    ) -> CommentFetchSummary: ...


def native_child_rejection(payload: Mapping[str, Any]) -> bool:
    """Operation-local native denial; HTTP and authentication guards still win."""
    pending = [payload]
    native_code = False
    while pending:
        value = pending.pop()
        if isinstance(value, list):
            pending.extend(value)
            continue
        if not isinstance(value, Mapping) or any(value.get(key) for key in ("id", "pk", "comment_id", "text")):
            continue
        if any(type(value.get(key)) is int and value[key] >= 300 for key in ("http_status", "status_code")):
            return False
        message = " ".join(str(value.get(key) or "").lower() for key in ("message", "error_message", "error_type"))
        if any(
            value.get(key) for key in ("login_required", "auth_failed", "checkpoint_required", "challenge_required")
        ) or any(
            token in message
            for token in ("login", "checkpoint", "challenge", "unauthorized", "forbidden", "authentication")
        ):
            return False
        native_code = native_code or any(str(value.get(key)) == "1675004" for key in ("code", "error_code"))
        pending.extend(
            value[key]
            for key in ("errors", "error", "extensions", "data", "graphql", "comments", "child_comments", *_CONNECTIONS)
            if key in value
        )
    return native_code or payload.get("reply_access_untrusted") is True


def _reply_access_retry(state, summary, now):
    previous = state.get("reply_access") or {}
    if previous.get("retry_source") == "server_retry_after" and previous.get("next_attempt_at"):
        retry_at = datetime.fromisoformat(previous["next_attempt_at"].replace("Z", "+00:00"))
        if retry_at > now:
            return Failure("reply_access_rejected", retry_at=retry_at)
    attempts = max(0, int(previous.get("attempts") or 0)) + 1
    server_delay = summary.reply_retry_after_seconds
    retry_at = None
    if server_delay is not None and math.isfinite(server_delay) and server_delay >= 0:
        try:
            retry_at = now + timedelta(seconds=server_delay)
        except OverflowError:
            retry_at = datetime.max.replace(tzinfo=UTC)
    state["reply_access"] = {
        "code": summary.reply_access_code or "reply_access_rejected",
        "rejected_at": utc_timestamp(now),
        "attempts": attempts,
        "retry_source": "server_retry_after" if retry_at is not None else None,
        "next_attempt_at": utc_timestamp(retry_at) if retry_at is not None else None,
        "lanes": sorted(set(summary.reply_lanes_rejected)),
    }
    return Failure("reply_access_rejected", retry_at=retry_at)


def _count(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def comment_page_failure(value: Any) -> tuple[str, float | None] | None:
    """Inspect request envelopes, never ordinary identified comment content."""
    if isinstance(value, list):
        failures = [failure for item in value if (failure := comment_page_failure(item)) is not None]
        return next((item for item in failures if item[0] != "source_incomplete"), failures[0] if failures else None)
    if not isinstance(value, Mapping):
        return None
    if any(value.get(key) for key in ("id", "pk", "comment_id", "text")):
        return None
    status = value.get("status")
    failed = status is not None and (not isinstance(status, str) or status not in {"ok", "success"})
    failed = failed or any(
        value.get(key)
        for key in (
            "error",
            "errors",
            "error_type",
            "auth_failed",
            "login_required",
            "require_login",
            "checkpoint_required",
            "challenge_required",
            "rate_limited",
            "feedback_required",
        )
    )
    codes = [value.get(key) for key in ("status_code", "http_status")]
    for key in ("status_code", "http_status"):
        code = value.get(key)
        if code is not None and (type(code) is not int or code >= 300):
            failed = True
    message = " ".join(
        str(value.get(key) or "").lower()
        for key in (
            "message",
            "error_message",
            "error",
            "errors",
            "error_type",
            "status",
        )
    )
    auth = any(
        value.get(key)
        for key in ("auth_failed", "login_required", "require_login", "checkpoint_required", "challenge_required")
    ) or any(code in (401, 403) for code in codes)
    auth = auth or any(
        marker in message
        for marker in (
            "login_required",
            "login required",
            "checkpoint",
            "challenge_required",
            "unauthorized",
            "forbidden",
            "authentication",
        )
    )
    rate = (
        value.get("rate_limited")
        or value.get("feedback_required")
        or 429 in codes
        or any(
            marker in message
            for marker in (
                "rate_limit",
                "rate limit",
                "too many requests",
                "please wait",
                "feedback_required",
            )
        )
    )
    if rate:
        retry = value.get("retry_after")
        try:
            retry = float(retry) if type(retry) in (int, float) else None
        except OverflowError:
            retry = None
        return "rate_limited", retry if retry is not None and math.isfinite(retry) and retry >= 0 else None
    if auth:
        return "source_auth_failed", None
    if any(
        marker in message
        for marker in (
            "login_required",
            "login required",
            "checkpoint",
            "challenge_required",
            "rate limit",
            "too many requests",
            "please wait",
            "unauthorized",
            "forbidden",
            "feedback_required",
            "authentication",
        )
    ):
        failed = True
    containers = {
        "items",
        "data",
        "graphql",
        "shortcode_media",
        "xdt_shortcode_media",
        "media",
        "comments",
        "child_comments",
        "replies",
        "page_info",
        *_CONNECTIONS,
    }
    nested = comment_page_failure([value[key] for key in containers.intersection(value)])
    return nested or (("source_incomplete", None) if failed else None)


def _find_connection(payload: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]] | None:
    for key in _CONNECTIONS:
        if isinstance(payload.get(key), dict):
            return key, payload[key]
    for key in ("data", "graphql", "shortcode_media", "xdt_shortcode_media", "media"):
        child = payload.get(key)
        if isinstance(child, dict):
            found = _find_connection(child)
            if found is not None:
                return found
    return None


def _native_reply_count(source: Mapping[str, Any], *, strict: bool) -> int | None:
    values = [
        source[key]
        for key in ("child_comment_count", "childCommentCount", "repliesCount", "reply_count", "replies_count")
        if key in source
    ]
    thread = source.get("edge_threaded_comments")
    if isinstance(thread, dict) and "count" in thread:
        values.append(thread["count"])
    if strict and any(value is not None and _count(value) is None for value in values):
        raise CommentSourceError("Native public reply count must be a nonnegative integer or null")
    return max((value for value in values if _count(value) is not None), default=None)


def _node(source: Any, parent: str | None = None, *, strict_counts: bool = False) -> dict[str, Any]:
    if not isinstance(source, dict):
        raise CommentSourceError("Comment node is not an object")
    if source.get("source_snapshot_type", "full_comments_scrape") != "full_comments_scrape":
        raise CommentSourceError("Comment samples cannot become full-comments observations")
    result = dict(source)
    reply_count = _native_reply_count(source, strict=strict_counts)
    if reply_count is not None:
        # Normalize only this extraction view. Retained evidence and the other
        # native aliases keep their original values, including explicit nulls.
        result["child_comment_count"] = reply_count
    if source.get("created_at") is None and source.get("created_at_utc") is not None:
        result["created_at"] = source["created_at_utc"]
    liked = source.get("edge_liked_by")
    if not any(key in source for key in ("comment_like_count", "like_count", "likes")) and isinstance(liked, dict):
        if _count(liked.get("count")) is not None:
            result["comment_like_count"] = liked["count"]
    identity = source.get("comment_id") or source.get("id") or source.get("pk")
    if not isinstance(identity, (str, int)) or isinstance(identity, bool) or not str(identity):
        raise CommentSourceError("Native comment identity is missing")
    if parent:
        existing = source.get("parent_comment_id") or source.get("parent_comment_external_id")
        if existing and str(existing) != parent:
            raise CommentSourceError("Native reply has conflicting parents")
        result["parent_comment_id"] = parent
    children = source.get("replies", source.get("child_comments", source.get("preview_child_comments", [])))
    thread = source.get("edge_threaded_comments")
    if isinstance(thread, dict):
        edges = thread.get("edges")
        if not isinstance(edges, list) or any(not isinstance(edge, dict) or "node" not in edge for edge in edges):
            raise CommentSourceError("Native reply connection is malformed")
        children = [edge["node"] for edge in edges]
    if not isinstance(children, list):
        raise CommentSourceError("Native reply list is malformed")
    result["replies"] = [_node(child, str(identity), strict_counts=strict_counts) for child in children]
    # The supported native Instagram reply topology is one parent level. An
    # explicit reply endpoint/embedded child node is a leaf unless source
    # content itself supplies another child count or connection.
    if (
        parent
        and not strict_counts
        and not any(
            key in result for key in ("child_comment_count", "reply_count", "replies_count", "edge_threaded_comments")
        )
    ):
        result["child_comment_count"] = 0
    result.pop("child_comments", None)
    result["source_snapshot_type"] = "full_comments_scrape"
    return result


def parse_comment_evidence(
    evidence: Evidence, *, pinned: bool = False, parser_version: str = COMMENT_PARSER_VERSION
) -> CommentPage:
    """Normalize retained REST/GraphQL pages without any source access.

    Keep native attachment/user fields and graph edges in raw_data; provide the
    explicit reply list required by the independent canonical writer alongside.
    """
    if parser_version != COMMENT_PARSER_VERSION:
        raise CommentSourceError("Requested comment parser version is unavailable")
    payload = evidence.read(pinned=pinned)
    if comment_page_failure(payload):
        raise CommentSourceError("Native comment page contains failed request metadata")
    if payload.get("source_snapshot_type") == "native_rest_list_v1":
        payload = _native_list_page(payload)
    provenance = payload.get("cursor_payload")
    if isinstance(provenance, dict) and provenance.get("adapter_family") not in (None, "instagram_public_relay"):
        raise CommentSourceError("Native comment evidence uses an unsupported adapter family")
    if isinstance(provenance, dict) and provenance.get("adapter_family") == "instagram_public_relay":
        required = {
            "adapter_family",
            "adapter_version",
            "source",
            "relay_doc_id",
            "relay_friendly_name",
            "sort_order",
            "page_size",
        }
        if (
            not required.issubset(provenance)
            or type(provenance["adapter_version"]) is not int
            or provenance["adapter_version"] != 1
            or provenance["source"] not in {"root", "resume"}
            or any(
                not isinstance(provenance[key], str) or not provenance[key]
                for key in ("relay_doc_id", "relay_friendly_name", "sort_order")
            )
            or type(provenance["page_size"]) is not int
            or not 1 <= provenance["page_size"] <= 50
            or (
                provenance.get("request_cursor") is not None
                and (not isinstance(provenance["request_cursor"], str) or not provenance["request_cursor"])
            )
            or (provenance["source"] == "resume") != (provenance.get("request_cursor") is not None)
        ):
            raise CommentSourceError("Public Relay page has invalid cursor provenance")
        provenance = dict(provenance)
    else:
        provenance = None
    parent = payload.get("parent_comment_id") or payload.get("parent_comment_pk")
    parent = str(parent) if parent is not None else None
    if payload.get("comments_disabled") is True:
        return CommentPage(evidence.captured_at, (), (), parent, None, False, None, "comments_disabled")
    found = _find_connection(payload)
    if found is not None:
        name, connection = found
        if "parent_comment_id__child_comments" in name and not parent:
            raise CommentSourceError("Native child connection requires its parent identity")
        edges = connection.get("edges")
        if not isinstance(edges, list) or any(not isinstance(edge, dict) or "node" not in edge for edge in edges):
            raise CommentSourceError("Native comment connection is malformed")
        nodes = [edge["node"] for edge in edges]
        page_info = connection.get("page_info") or {}
        more = page_info.get("has_next_page")
        cursor = page_info.get("end_cursor") if more is not False else None
        cursor_param = "after" if cursor else None
        expected = _count(connection.get("count"))
    elif any(isinstance(payload.get(key), list) for key in ("comments", "child_comments", "replies")):
        if not parent and any(key in payload for key in ("child_comments", "replies")):
            raise CommentSourceError("Native child page requires its parent identity")
        nodes = payload.get("comments", payload.get("child_comments", payload.get("replies")))
        if parent:
            more = payload.get("has_more_tail_child_comments")
            if payload.get("has_more_head_child_comments") is True:
                more = True
            cursor_keys = (("next_min_child_cursor", "min_id"), ("next_max_child_cursor", "max_id"))
        else:
            more = payload.get("has_more_comments")
            if payload.get("has_more_headload_comments") is True:
                more = True
            cursor_keys = (
                ("cached_comments_cursor", "cached_comments_cursor"),
                ("tao_cursor", "tao_cursor"),
                ("next_max_id", "max_id"),
                ("next_min_id", "min_id"),
                ("bifilter_token", "bifilter_token"),
            )
            if payload.get("has_more_headload_comments") is True and payload.get("has_more_comments") is not True:
                cursor_keys = (cursor_keys[3], cursor_keys[2], cursor_keys[0], cursor_keys[1], cursor_keys[4])
        cursor, cursor_param = next(
            ((payload[key], param) for key, param in cursor_keys if payload.get(key)), (None, None)
        )
        expected = _count(payload.get("comment_count"))
    else:
        raise CommentSourceError("Unsupported native comment evidence (DOM is not a native page)")
    if more is not None and type(more) is not bool:
        raise CommentSourceError("Native pagination flag is not boolean")
    if cursor is not None and not isinstance(cursor, (str, int)):
        raise CommentSourceError("Native cursor is malformed")
    sources = tuple(_node(node, parent, strict_counts=provenance is not None) for node in nodes)
    comments = tuple(normalize_comment_source(node) for node in sources)
    identities: dict[str, str | None] = {}

    def validate(comment: InstagramComment) -> None:
        if comment.comment_id in identities:
            raise CommentSourceError("Duplicate native comment identity within a page")
        identities[comment.comment_id] = comment.parent_comment_id
        for reply in comment.replies:
            validate(reply)

    for comment in comments:
        validate(comment)
    # GraphQL retains its final edge cursor even on an exhausted connection.
    exhausted = more is False and not cursor
    if exhausted:
        cursor_param = None
    return CommentPage(
        evidence.captured_at,
        comments,
        sources,
        parent,
        str(cursor) if cursor else None,
        exhausted,
        expected,
        cursor_param=cursor_param,
        started_from_root=isinstance(payload.get("cursor_payload"), dict)
        and payload["cursor_payload"].get("source") == "root",
        cursor_provenance=provenance,
    )


def _native_list_page(envelope: dict[str, Any]) -> dict[str, Any]:
    """Unwrap retained native list rows without inventing terminal metadata."""
    if set(envelope) - {"source_snapshot_type", "items", "parent_comment_id", "cursor_payload", "status"}:
        raise CommentSourceError("Native list envelope contains unsupported fields")
    items = envelope.get("items")
    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        raise CommentSourceError("Native list envelope requires object entries")
    keys = {
        "comment_count",
        "has_more_comments",
        "has_more_headload_comments",
        "next_min_id",
        "next_max_id",
        "cached_comments_cursor",
        "tao_cursor",
        "bifilter_token",
        "has_more_tail_child_comments",
        "has_more_head_child_comments",
        "next_min_child_cursor",
        "next_max_child_cursor",
    }
    result = {key: value for key, value in envelope.items() if key in {"parent_comment_id", "cursor_payload"}}
    rows = []
    for item in items:
        identity = any(item.get(key) for key in ("id", "pk", "comment_id", "text", "user", "owner"))
        nested = "child_comments" if result.get("parent_comment_id") else "comments"
        if nested in item:
            if not isinstance(item[nested], list):
                raise CommentSourceError("Native list page rows are malformed")
            rows.extend(item[nested])
        elif identity:
            rows.append(item)
        elif not keys.intersection(item):
            raise CommentSourceError("Native list entry has neither identity nor pagination proof")
        for key in keys.intersection(item):
            if item[key] is not None:
                if key in result and result[key] != item[key]:
                    raise CommentSourceError("Native list pagination metadata conflicts")
                result[key] = item[key]
    result["child_comments" if result.get("parent_comment_id") else "comments"] = rows
    return result


def _progress(checkpoint: Mapping[str, Any]) -> dict[str, Any]:
    state = checkpoint.get("comment_pages") or {}
    if not isinstance(state, dict) or state.get("version", 1) != 1:
        raise CommentSourceError("Unsupported comment checkpoint version")
    # Canonical JSON copy prevents fetch callbacks from mutating durable state.
    return json.loads(stable_json(state or {"version": 1, "observed": {}, "lanes": {}, "expected": None}))


def _advance(state: dict[str, Any], page: CommentPage, *, scan_started_at: datetime) -> None:
    provenance = page.cursor_provenance
    if provenance is not None:
        if state.get("adapter_family") not in (None, "instagram_public_relay") or (
            state["lanes"] and state.get("adapter_family") is None
        ):
            raise CommentSourceError("Comment cursor adapter family changed")
        state["adapter_family"] = "instagram_public_relay"
        state["adapter_version"] = 1
        lane_key = page.parent_comment_id or "__top__"
        previous_lane = state["lanes"].get(lane_key) or {}
        request_cursor = provenance.get("request_cursor")
        if request_cursor:
            previous_cursors = previous_lane.get("cursors") or []
            if (
                previous_lane.get("exhausted")
                or not previous_cursors
                or previous_cursors[-1] != request_cursor
                or any(
                    previous_lane.get(key) != provenance[key]
                    for key in ("relay_doc_id", "relay_friendly_name", "sort_order", "page_size")
                )
            ):
                raise CommentSourceError("Public Relay continuation does not match its durable query lane")
        elif page.parent_comment_id:
            # An embedded child cursor has no proved child-query identity. A
            # child root request establishes that binding without replacing the
            # top scan or treating retained inventory as a fresh observation.
            state["lanes"].pop(lane_key, None)
    elif state.get("adapter_family") == "instagram_public_relay":
        raise CommentSourceError("Public Relay page lacks its cursor provenance")
    if page.started_from_root and not page.parent_comment_id and "__top__" in state["lanes"]:
        # A deliberate root restart begins a fresh observation manifest. Earlier
        # canonical rows and immutable page receipts remain durable; they cannot
        # count as fresh coverage or prevent the new scan's missing lifecycle.
        state.update(observed={}, lanes={}, expected=None)
    if not page.parent_comment_id and "__top__" not in state["lanes"]:
        state["scan_started_at"] = utc_timestamp(scan_started_at)

    def advance_lane(key: str, cursor: str | None, exhausted: bool, param: str | None) -> None:
        lane = state["lanes"].setdefault(key, {"cursors": [], "exhausted": False, "repeated": False})
        seen = lane.setdefault("cursor_keys", [])
        if cursor:
            cursor_key = f"{param}:{cursor}"
            if cursor_key in seen:
                lane["repeated"] = True
            else:
                seen.append(cursor_key)
                lane["cursors"].append(cursor)
        lane["cursor_param"] = param
        lane["exhausted"] = exhausted

    advance_lane(page.parent_comment_id or "__top__", page.next_cursor, page.exhausted, page.cursor_param)
    if provenance is not None:
        lane = state["lanes"][page.parent_comment_id or "__top__"]
        lane.update(
            {key: provenance[key] for key in ("relay_doc_id", "relay_friendly_name", "sort_order", "page_size")}
        )
        if page.parent_comment_id and page.expected_count is not None and page.parent_comment_id in state["observed"]:
            parent = state["observed"][page.parent_comment_id]
            parent["expected_replies"] = max(parent.get("expected_replies") or 0, page.expected_count)
    if page.started_from_root:
        state["lanes"][page.parent_comment_id or "__top__"]["started_from_root"] = True
    if page.expected_count is not None and not page.parent_comment_id:
        state["expected"] = page.expected_count

    def visit(comment: InstagramComment, source: dict[str, Any], *, native_reply: bool = False) -> None:
        thread = source.get("edge_threaded_comments")
        if isinstance(thread, dict):
            info = thread.get("page_info") or {}
            exhausted = info.get("has_next_page") is False
            cursor = info.get("end_cursor") if not exhausted else None
            advance_lane(comment.comment_id, str(cursor) if cursor else None, exhausted, "after" if cursor else None)
        previous = state["observed"].get(comment.comment_id)
        parent = comment.parent_comment_id
        if previous and previous["parent"] != parent:
            raise CommentSourceError("One post-scoped comment changed parent")
        expected = _count(source.get("child_comment_count", source.get("reply_count", source.get("replies_count"))))
        state["observed"][comment.comment_id] = {
            "parent": parent,
            "expected_replies": expected if expected is not None else (previous or {}).get("expected_replies"),
        }
        if provenance is not None and native_reply:
            nested = bool(source["replies"]) or isinstance(thread, dict)
            counts = [
                source[key]
                for key in ("child_comment_count", "childCommentCount", "reply_count", "replies_count", "repliesCount")
                if key in source
            ]
            # Native depth-one reply endpoints/edges prove leaf topology even
            # when their optional child-count field is null. Keep that field
            # unknown; the topology proof is separate from a numeric count.
            state["observed"][comment.comment_id]["native_reply_leaf"] = not nested and all(
                value is None or (type(value) is int and value == 0) for value in counts
            )
            if nested or any(type(value) is int and value > 0 for value in counts):
                state["observed"][comment.comment_id]["unsupported_reply_topology"] = True
        for child, raw in zip(comment.replies, source["replies"], strict=True):
            visit(child, raw, native_reply=True)

    for comment, source in zip(page.comments, page.source_payloads, strict=True):
        visit(comment, source, native_reply=page.parent_comment_id is not None)


def _valid_source_summary(summary: CommentFetchSummary) -> bool:
    return not (
        summary.fetch_failed
        or summary.reply_lanes_rejected
        or summary.auth_failed
        or summary.retryable
        or summary.top_level_checkpoint
        or summary.reply_checkpoints
    )


def _scan_cutoff(state: Mapping[str, Any], *, captured_at: datetime) -> datetime | None:
    value = state.get("scan_started_at")
    if not isinstance(value, str):
        return None
    try:
        started = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if started.utcoffset() is None or started > captured_at:
        return None
    return started.astimezone(UTC)


class CommentSettlement(TypedDict):
    basis: Literal["count_parity", "public_native_exhaustion"]
    expected_total: int
    observed_total: int
    count_gap: int


def _settlement(state: dict[str, Any], summary: CommentFetchSummary) -> CommentSettlement | None:
    if not summary.full_observation or not _valid_source_summary(summary):
        return None
    expected = summary.expected_count if summary.expected_count is not None else state["expected"]
    if _count(expected) is None:
        return None
    top = state["lanes"].get("__top__", {})
    if not top.get("exhausted") or any(lane.get("repeated") for lane in state["lanes"].values()):
        return None
    observed_total = len(state["observed"])
    count_gap = max(expected - observed_total, 0)
    if count_gap and not (
        state.get("adapter_family") == "instagram_public_relay"
        and state.get("adapter_version") == 1
        and top.get("started_from_root") is True
        and top.get("exhausted") is True
    ):
        return None
    child_counts = Counter(item["parent"] for item in state["observed"].values())
    for identity, observed in state["observed"].items():
        # Unknown counts remain unknown. A separate native leaf or complete
        # public reply-lane proof can satisfy topology without inventing a count.
        count = observed["expected_replies"]
        lane = state["lanes"].get(identity)
        if observed.get("unsupported_reply_topology"):
            return None
        if count is None:
            root_exhausted = (
                state.get("adapter_family") == "instagram_public_relay"
                and state.get("adapter_version") == 1
                and lane is not None
                and lane.get("started_from_root") is True
                and lane.get("exhausted") is True
                and all(lane.get(key) for key in ("relay_doc_id", "relay_friendly_name", "sort_order", "page_size"))
            )
            if not observed.get("native_reply_leaf") and not root_exhausted:
                return None
        elif child_counts[identity] < count:
            return None
        if lane is not None and not lane.get("exhausted"):
            return None
    return {
        "basis": "public_native_exhaustion" if count_gap else "count_parity",
        "expected_total": expected,
        "observed_total": observed_total,
        "count_gap": count_gap,
    }


def _complete(state: dict[str, Any], summary: CommentFetchSummary) -> bool:
    return _settlement(state, summary) is not None


class CommentActivity:
    def __init__(
        self,
        store: Store,
        fetch_source: CommentSource | None = None,
        *,
        writer: Callable = write_comments,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self.store, self.fetch_source, self.writer, self.clock = store, fetch_source, writer, clock

    def execute(
        self,
        *,
        run_id: str,
        account: str,
        generation: int,
        target_key: str,
        owner_token: str,
        post_id: str,
        checkpoint: dict[str, Any] | None = None,
        fetch: CommentSource | None = None,
        metadata_writes_enabled: bool = True,
        assignment: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not post_id:
            raise ValueError("A canonical parent post must be loaded before source requests")
        source = fetch or self.fetch_source
        if source is None:
            raise ValueError("An explicit comment source callback is required")
        owned = {
            "run_id": run_id,
            "account": account,
            "generation": generation,
            "stage": Stage.COMMENTS,
            "target_key": target_key,
            "owner_token": owner_token,
        }
        target = self.store.acquire(**owned)
        current_checkpoint = dict(target["checkpoint"])
        if checkpoint is not None and checkpoint != current_checkpoint:
            raise Conflict("Supplied checkpoint differs from durable comment progress")
        with self.store.transaction() as conn, conn.cursor() as cur:
            cur.execute("select shortcode from social.instagram_posts where id=%s", (post_id,))
            post = cur.fetchone()
            if not post or post[0] != target_key:
                raise Conflict("Canonical parent post does not match the comment target shortcode")
        attempt_started_at = datetime.fromisoformat(utc_timestamp(self.clock()))
        state = _progress(current_checkpoint)
        resume_inventory, retained = self._resume_inventory(owned, state)

        def observe(kind: str, payload: Mapping[str, Any]) -> None:
            nonlocal current_checkpoint, state, retained
            if kind not in {"comments_api", "graphql_comments", "graphql_replies", "graphql_preview"}:
                raise CommentSourceError("Unsupported source comment page kind")
            retained_payload = dict(payload)
            rejected_reply = kind == "graphql_replies" and native_child_rejection(payload)
            if comment_page_failure(payload) or rejected_reply:
                # Error messages are intentionally outside the evidence allowlist.
                # Preserve a fixed failed-request fact so offline replay cannot
                # discard that rejection while retaining terminal pagination.
                retained_payload["status"] = "fail"
            evidence = Evidence.capture(
                source_identity=f"instagram:comments:{target_key}:{kind}",
                captured_at=self.clock(),
                parser_version=COMMENT_PARSER_VERSION,
                payload=retained_payload,
            )
            evidence_id = self.store.retain_evidence(**owned, evidence=evidence)
            if rejected_reply:
                return
            # Preview samples remain retained but cannot become full-comments rows.
            if kind == "graphql_preview":
                return
            page = parse_comment_evidence(evidence, pinned=True)
            updated = _progress({"comment_pages": state})
            from trr_backend.ingestion.lane_restart import pending_restarts

            restarts = dict(pending_restarts(current_checkpoint))
            lane_key = page.parent_comment_id or "__top__"
            restarted = restarts.get(lane_key)
            if restarted:
                if not page.started_from_root:
                    raise CommentSourceError("Authorized lane restart must begin at its root")
                if page.parent_comment_id:
                    # A new child root must prove its own inventory, while the
                    # committed rows/evidence from the old scan stay retained.
                    updated["observed"] = {
                        key: value
                        for key, value in updated["observed"].items()
                        if value["parent"] != page.parent_comment_id
                    }
            _advance(updated, page, scan_started_at=attempt_started_at)
            access = updated.get("reply_access")
            if access and page.parent_comment_id and page.exhausted:
                access["lanes"] = [identity for identity in access["lanes"] if identity != page.parent_comment_id]
                if not access["lanes"]:
                    updated.pop("reply_access")
            if not page.parent_comment_id and (page.started_from_root or "__top__" not in state["lanes"]):
                updated["scan_root_evidence_id"] = evidence_id
            next_checkpoint = {**current_checkpoint, "comment_pages": updated}
            if restarted:
                restarts.pop(lane_key)
                if lane_key == "__top__":
                    # The new parent scan supersedes any pending child restart.
                    restarts = {}
                next_checkpoint["public_lane_restarts"] = restarts
                next_checkpoint["public_lane_restart_history"] = [
                    *current_checkpoint.get("public_lane_restart_history", []),
                    {**restarted, "root_evidence_id": evidence_id, "committed_at": self.clock().isoformat()},
                ]
            self._persist_page(
                owned,
                post_id=post_id,
                evidence=evidence,
                evidence_id=evidence_id,
                page=page,
                expected_checkpoint=current_checkpoint,
                checkpoint=next_checkpoint,
                metadata_writes_enabled=metadata_writes_enabled,
                assignment=assignment,
            )
            current_checkpoint, state = next_checkpoint, updated
            retained = (evidence_id, evidence, page)
            if state.get("adapter_family") == "instagram_public_relay" and any(
                lane.get("repeated") and key not in current_checkpoint.get("public_lane_restarts", {})
                for key, lane in state["lanes"].items()
            ):
                raise CursorStalledError("Comment lane has a repeated cursor")

        summary = source(
            shortcode=target_key,
            checkpoint=json.loads(stable_json(current_checkpoint)),
            resume_inventory=resume_inventory,
            source_observer=observe,
            pre_source_request=lambda: self.store.check_owner(**owned, record_request=True),
        )
        if not isinstance(summary, CommentFetchSummary):
            raise TypeError("Comment source must return a typed CommentFetchSummary")
        if (
            summary.reply_lanes_rejected
            and not summary.fetch_failed
            and not summary.auth_failed
            and state["lanes"].get("__top__", {}).get("exhausted")
        ):
            failure = _reply_access_retry(state, summary, self.clock())
            current_checkpoint = {**current_checkpoint, "comment_pages": state}
            self.store.checkpoint_failure(**owned, failure=failure, checkpoint=current_checkpoint, requests=0)
            return {
                "outcome": Outcome.UNRESOLVED.value,
                "checkpoint": current_checkpoint,
                "complete": False,
                "failure": failure,
            }
        if retained is None:
            return {"outcome": Outcome.UNRESOLVED.value, "checkpoint": current_checkpoint, "complete": False}
        evidence_id, evidence, page = retained
        if not _valid_source_summary(summary):
            return {"outcome": Outcome.UNRESOLVED.value, "checkpoint": current_checkpoint, "complete": False}
        if page.unavailable_reason in _UNAVAILABLE and not state["observed"]:
            receipt = self.store.commit_target(
                **owned,
                observed_at=evidence.captured_at,
                evidence_id=evidence_id,
                outcome=Outcome.UNAVAILABLE,
                write_canonical=lambda conn: {"post_id": post_id, "reason": page.unavailable_reason},
            )
            return {"outcome": Outcome.UNAVAILABLE.value, "checkpoint": current_checkpoint, "receipt": receipt}
        scan_cutoff = _scan_cutoff(state, captured_at=evidence.captured_at)
        settlement = _settlement(state, summary)
        if scan_cutoff is None or settlement is None:
            return {"outcome": Outcome.UNRESOLVED.value, "checkpoint": current_checkpoint, "complete": False}

        state = {**state, "settlement": settlement}
        current_checkpoint = {**current_checkpoint, "comment_pages": state}

        def finish(conn):
            # Native public exhaustion can still leave a known count gap. It
            # cannot prove that unseen saved comments disappeared. Only count
            # parity may reconcile missing rows, using the original scan cutoff.
            if settlement["count_gap"] == 0:
                with conn.cursor() as cur:
                    cur.execute(
                        """update social.instagram_comments set is_missing=true,missing_at=%s
                        where post_id=%s and not (comment_id=any(%s)) and scraped_at<=%s
                        and not is_missing""",
                        (evidence.captured_at, post_id, list(state["observed"]), scan_cutoff),
                    )
            return {"post_id": post_id, "observed_comment_ids": sorted(state["observed"]), "settlement": settlement}

        receipt = self.store.commit_target(
            **owned,
            observed_at=evidence.captured_at,
            evidence_id=evidence_id,
            write_canonical=finish,
            checkpoint=current_checkpoint,
        )
        return {
            "outcome": Outcome.COMMITTED.value,
            "checkpoint": current_checkpoint,
            "complete": True,
            "receipt": receipt,
        }

    def _resume_inventory(
        self, owned: dict[str, Any], state: dict[str, Any]
    ) -> tuple[tuple[CommentResumeNode, ...], tuple[str, Evidence, CommentPage] | None]:
        if not state["lanes"]:
            return (), None
        root_id = state.get("scan_root_evidence_id")
        with self.store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            page_args = [owned["run_id"], owned["target_key"]]
            root_filter = ""
            if root_id:
                cur.execute(
                    """select committed_at,page_id from social.instagram_ingestion_comment_pages
                    where run_id=%s and target_key=%s and stage='comments' and evidence_id::text=%s""",
                    (owned["run_id"], owned["target_key"], root_id),
                )
                root = cur.fetchone()
                if root is None:
                    raise CommentSourceError("Durable scan root lacks committed evidence")
                root_filter = "and (p.committed_at,p.page_id)>=(%s,%s)"
                page_args.extend((root["committed_at"], root["page_id"]))
            cur.execute(
                f"""select e.evidence_id,e.source_identity,e.captured_at,e.parser_version,
                e.payload,e.content_hash,e.schema_version from social.instagram_ingestion_comment_pages p
                join social.instagram_ingestion_evidence e using(evidence_id,run_id,stage,target_key)
                where p.run_id=%s and p.target_key=%s and p.stage='comments' {root_filter}
                order by p.committed_at,p.page_id""",
                page_args,
            )
            pages = cur.fetchall()
            final_checkpoint = {}
            if pages:
                cur.execute(
                    """select checkpoint from social.instagram_ingestion_comment_pages
                    where run_id=%s and target_key=%s and stage='comments' and evidence_id=%s""",
                    (owned["run_id"], owned["target_key"], pages[-1]["evidence_id"]),
                )
                final_checkpoint = cur.fetchone()["checkpoint"]
        nodes: dict[str, CommentResumeNode] = {}
        proof = None
        for row in pages:
            retained = Evidence(
                source_identity=row["source_identity"],
                captured_at=row["captured_at"],
                parser_version=row["parser_version"],
                payload_json=row["payload"],
                content_hash=row["content_hash"],
                schema_version=row["schema_version"],
            )
            page = parse_comment_evidence(retained, pinned=True)
            proof = (str(row["evidence_id"]), retained, page)
            if page.started_from_root and not page.parent_comment_id:
                nodes.clear()

            def visit(comment: InstagramComment, raw: dict[str, Any], observed_at: datetime = page.observed_at) -> None:
                identity = comment.comment_id
                if identity in state["observed"]:
                    if state["observed"][identity]["parent"] != comment.parent_comment_id:
                        raise CommentSourceError("Retained resume inventory changed parent identity")
                    payload = dict(raw, parent_comment_id=comment.parent_comment_id)
                    for key in ("replies", "child_comments", "preview_child_comments", "edge_threaded_comments"):
                        payload.pop(key, None)
                    count = state["observed"][identity]["expected_replies"]
                    if count is not None:
                        payload["child_comment_count"] = count
                    nodes[identity] = CommentResumeNode(payload, observed_at)
                for child, source in zip(comment.replies, raw["replies"], strict=True):
                    visit(child, source)

            for comment, raw in zip(page.comments, page.source_payloads, strict=True):
                visit(comment, raw)
        receipt_state = _progress(final_checkpoint) if pages else {}
        if "scan_started_at" not in state:
            # Legacy progress may lack the safe missing-row cutoff. Retain that
            # absence: it cannot settle or mark missing without a fresh root.
            receipt_state.pop("scan_started_at", None)
        # Reply retry administration is written when releasing a target, outside
        # the atomic native-page receipt. Every page-derived field must still
        # match its committed proof exactly.
        receipt_proof = {key: value for key, value in receipt_state.items() if key != "reply_access"}
        target_proof = {key: value for key, value in state.items() if key != "reply_access"}
        if set(nodes) != set(state["observed"]) or not pages or receipt_proof != target_proof:
            raise CommentSourceError("Durable comment inventory lacks matching committed page proof")
        return tuple(nodes[key] for key in sorted(nodes)), proof

    def _persist_page(
        self,
        owned: dict[str, Any],
        *,
        post_id: str,
        evidence: Evidence,
        evidence_id: str,
        page: CommentPage,
        expected_checkpoint: dict[str, Any],
        checkpoint: dict[str, Any],
        metadata_writes_enabled: bool,
        assignment: Mapping[str, Any] | None,
    ) -> None:
        """Canonical page, receipt and checkpoint share Store's lock/fence order."""
        with self.store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = self.store._run(cur, owned["run_id"], owned["account"])
            if run["generation"] != owned["generation"] or run["state"] not in {"queued", "running"}:
                raise LostOwnership("Comment page belongs to a superseded execution")
            cur.execute(
                """select * from social.instagram_ingestion_targets
                where run_id=%s and stage='comments' and target_key=%s and owner_token=%s
                and owner_started_at is not null and owner_expires_at>clock_timestamp()
                and outcome='unresolved' for update""",
                (owned["run_id"], owned["target_key"], owned["owner_token"]),
            )
            target = cur.fetchone()
            if not target:
                raise LostOwnership("Comment page writer lost target ownership")
            if target["checkpoint"] != expected_checkpoint:
                raise Conflict("Comment checkpoint advanced concurrently")
            identity = self.writer(
                conn,
                run_id=owned["run_id"],
                account=owned["account"],
                post_id=post_id,
                observed_at=evidence.captured_at,
                comments=page.comments,
                source_payloads=page.source_payloads,
                metadata_writes_enabled=metadata_writes_enabled,
                assignment=assignment,
            )
            serialized_checkpoint = stable_json(checkpoint)
            cur.execute(
                """insert into social.instagram_ingestion_comment_pages
                (page_id,run_id,stage,target_key,owner_generation,evidence_id,observed_at,content_hash,
                 parent_comment_id,next_cursor,exhausted,canonical_identity,checkpoint)
                values(%s,%s,'comments',%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)""",
                (
                    str(uuid4()),
                    owned["run_id"],
                    owned["target_key"],
                    target["owner_generation"],
                    evidence_id,
                    evidence.captured_at,
                    evidence.content_hash,
                    page.parent_comment_id,
                    page.next_cursor,
                    page.exhausted,
                    stable_json(identity),
                    serialized_checkpoint,
                ),
            )
            cur.execute(
                """update social.instagram_ingestion_targets set checkpoint=%s::jsonb
                where run_id=%s and stage='comments' and target_key=%s""",
                (serialized_checkpoint, owned["run_id"], owned["target_key"]),
            )
