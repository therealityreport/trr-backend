"""Explicit-credential adapter from the maintained fetcher to durable comments.

Only HTTP attempts and explicit browser operations are counted at the source
boundary. Browser subresource traffic is not counted by these callbacks.
Rendered comment parsing has no retained native evidence contract and is denied.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable, Mapping
from typing import Any

from trr_backend.ingestion.comments import CommentFetchSummary, CommentSourceError

_ALLOWED_OPERATIONS = frozenset(
    {
        "comments_api",
        "graphql_comments",
        "graphql_replies",
        "relay_bootstrap",
        "browser_warmup",
        "browser_api_evaluate",
        "browser_api",
    }
)
_SUCCESS_REASONS = frozenset(
    {
        "completed",
        "pagination_complete",
        "api_pagination_complete",
        "reply_tail_coverage_complete",
        "residual_child_lane_complete",
    }
)
_DIRECTIONS = frozenset({"min_id", "max_id"})
_TOP_DIRECTIONS = _DIRECTIONS | {"cached_comments_cursor", "tao_cursor", "bifilter_token"}
_CHECKPOINT_KEYS = frozenset(
    {
        "parent_comment_id",
        "next_top_level_cursor",
        "next_top_level_cursor_param",
        "last_top_level_cursor",
        "last_top_level_cursor_param",
        "next_reply_cursor",
        "next_reply_cursor_param",
        "last_reply_cursor",
        "last_reply_cursor_param",
        "observed_comment_count",
        "saved_reply_count_observed",
        "expected_comment_count",
        "expected_reply_count",
        "pages_seen",
        "observation_started_from_root",
    }
)


class UnsupportedCommentOperationError(CommentSourceError):
    """No typed retained-source contract exists for this operation."""


class _CallbackAbort(BaseException):
    """Cross legacy `except Exception` without swallowing a durable guard."""

    def __init__(self, error: Exception):
        super().__init__("Durable comment callback interrupted transport")
        self.error = error


def _guard(callback: Callable, *args: Any) -> Any:
    try:
        return callback(*args)
    except Exception as error:
        raise _CallbackAbort(error) from None


def _default_factory(**kwargs):
    # Import only when selected. No monolithic social pipeline or auth resolver.
    from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsScraplingFetcher

    return InstagramCommentsScraplingFetcher(**kwargs)


def _count(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _cursor(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _checkpoint(value: Any, *, root: bool) -> dict[str, Any] | None:
    if not isinstance(value, Mapping) or not value:
        return None
    # Do not copy diagnostics, error strings, headers, or session metadata from
    # legacy checkpoints. These retained fields are cursor/count content only.
    result = {
        key: child
        for key, child in value.items()
        if key in _CHECKPOINT_KEYS and (child is None or type(child) in (str, int, bool))
    }
    result["observation_started_from_root"] = root
    return result


def _resume(checkpoint: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    pages = checkpoint.get("comment_pages") or {}
    if not isinstance(pages, Mapping):
        raise CommentSourceError("Invalid durable comment page checkpoint")
    lanes = pages.get("lanes") or {}
    if not isinstance(lanes, Mapping):
        raise CommentSourceError("Invalid durable comment lane checkpoint")
    top = checkpoint.get("top_level_checkpoint") or {}
    if not isinstance(top, Mapping):
        raise CommentSourceError("Invalid durable top-level checkpoint")
    replies = checkpoint.get("reply_checkpoints") or []
    if not isinstance(replies, (tuple, list)) or any(not isinstance(item, Mapping) for item in replies):
        raise CommentSourceError("Invalid durable reply checkpoints")
    top_lane = lanes.get("__top__") or {}
    cursor = _cursor(top.get("next_top_level_cursor"))
    direction = top.get("next_top_level_cursor_param")
    if isinstance(top_lane, Mapping) and "exhausted" in top_lane:
        # Page receipts are newer than a fetch-summary checkpoint when a worker
        # dies between two pages. Prefer their cursor over stale legacy state.
        cursors = top_lane.get("cursors") or []
        cursor = (
            _cursor(cursors[-1]) if not top_lane.get("exhausted") and isinstance(cursors, list) and cursors else None
        )
        lane_direction = top_lane.get("cursor_param")
        direction = (
            (
                lane_direction
                if lane_direction is not None
                else top.get("next_top_level_cursor_param")
                if cursor == top.get("next_top_level_cursor")
                else None
            )
            if cursor
            else None
        )
    # A cursor without its exact direction is not a safe resumable request.
    if direction not in _TOP_DIRECTIONS:
        cursor, direction = None, None
    reply_cursors: dict[str, str] = {}
    reply_params: dict[str, str] = {}
    for reply in replies:
        parent = _cursor(reply.get("parent_comment_id"))
        next_cursor = _cursor(reply.get("next_reply_cursor"))
        param = reply.get("next_reply_cursor_param")
        if parent and next_cursor and param in _DIRECTIONS:
            reply_cursors[parent], reply_params[parent] = next_cursor, param
    for parent, lane in lanes.items():
        if parent == "__top__" or not isinstance(lane, Mapping):
            continue
        if lane.get("exhausted"):
            reply_cursors.pop(parent, None)
            reply_params.pop(parent, None)
            continue
        cursors = lane.get("cursors") or []
        next_cursor = _cursor(cursors[-1]) if isinstance(cursors, list) and cursors else None
        param = lane.get("cursor_param")
        if next_cursor and param in _DIRECTIONS:
            reply_cursors[parent], reply_params[parent] = next_cursor, param
        else:
            reply_cursors.pop(parent, None)
            reply_params.pop(parent, None)
    # An arbitrary cursor cannot prove that retained pages start at the root.
    # Preserve this explicit marker when the parent checkpoints a partial fetch.
    from_root = cursor is None or (
        isinstance(top_lane, Mapping)
        and (
            top_lane.get("started_from_root") is True
            or (top.get("observation_started_from_root") is True and bool(pages.get("observed")))
        )
    )
    return {
        "top_level_cursor": cursor,
        "top_level_cursor_param": direction if cursor else None,
        "reply_resume_cursors": reply_cursors or None,
        "reply_resume_cursor_params": reply_params or None,
    }, from_root


class ScraplingCommentSource:
    """Synchronous CommentSource for workers; all credentials are caller inputs.

    The async fetcher is created and closed inside a fresh event loop. Callers
    already inside an event loop must run this synchronous activity in a worker
    thread; the adapter never starts another background helper or event loop.
    """

    def __init__(
        self,
        *,
        cookies: list[dict[str, Any]],
        raw_cookies: dict[str, str],
        browser_account_id: str | None,
        expected_count: int | None = None,
        fetcher_factory: Callable[..., Any] = _default_factory,
        timeout_ms: int = 45_000,
        max_comments: int = 0,
        warmup: bool = False,
    ):
        if expected_count is not None and _count(expected_count) is None:
            raise ValueError("Expected comment count must be non-negative or unknown")
        if type(max_comments) is not int or max_comments < 0:
            raise ValueError("Comment cap must be non-negative; zero means uncapped")
        self._cookies = copy.deepcopy(cookies)
        self._raw_cookies = dict(raw_cookies)
        self._browser_account_id = browser_account_id
        self._expected_count = expected_count
        self._factory = fetcher_factory
        self._timeout_ms = timeout_ms
        self._max_comments = max_comments
        self._warmup = warmup

    def __call__(
        self,
        *,
        shortcode: str,
        checkpoint: dict[str, Any],
        source_observer: Callable[[str, Mapping[str, Any]], None],
        pre_source_request: Callable[[], None],
        source_failure_observer: Callable[[str, float | None], None] | None = None,
    ) -> CommentFetchSummary:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RuntimeError("Run the synchronous comment activity outside the active event loop")
        resume, from_root = _resume(checkpoint)
        pages = checkpoint.get("comment_pages") or {}
        expected = self._expected_count
        if expected is None:
            expected = _count(pages.get("expected"))

        def before(kind: str) -> None:
            if kind not in _ALLOWED_OPERATIONS:
                _guard(_unsupported, kind)
            _guard(pre_source_request)

        first_top_page = True

        def observe(kind: str, payload: Mapping[str, Any]) -> None:
            nonlocal first_top_page
            # Do not normalize transport DTOs: the durable observer retains the
            # original native payload before any parser sees it.
            native = dict(payload)
            if kind != "graphql_preview" and not (payload.get("parent_comment_id") or payload.get("parent_comment_pk")):
                if first_top_page:
                    native["cursor_payload"] = {
                        **(payload.get("cursor_payload") or {}),
                        "source": "root" if resume["top_level_cursor"] is None else "resume",
                    }
                    first_top_page = False
            _guard(source_observer, kind, native)

        async def fetch() -> CommentFetchSummary:
            fetcher = self._factory(
                cookies=copy.deepcopy(self._cookies),
                raw_cookies=dict(self._raw_cookies),
                browser_account_id=self._browser_account_id,
                timeout_ms=self._timeout_ms,
                source_payload_observer=observe,
                before_source_request=before,
                source_failure_observer=(
                    (lambda code, retry_after: _guard(source_failure_observer, code, retry_after))
                    if source_failure_observer is not None
                    else None
                ),
                consume_credentials_only=True,
            )
            failed = True
            try:
                if self._warmup:
                    await fetcher.warmup()
                result = await fetcher.fetch_comments_for_shortcode(
                    shortcode,
                    max_comments=self._max_comments,
                    fetch_replies=True,
                    expected_comment_count=expected,
                    expected_count_unknown=expected is None,
                    load_strategy="instagram_comments_endpoint_cursor",
                    **resume,
                )
                summary = _summary(result, expected=expected, from_root=from_root, capped=self._max_comments > 0)
                failed = False
                return summary
            finally:
                try:
                    await fetcher.aclose()
                except Exception:
                    # Preserve a pending durable guard/transport/process error;
                    # a standalone close failure cannot report false success.
                    if not failed:
                        raise

        try:
            return asyncio.run(fetch())
        except _CallbackAbort as abort:
            raise abort.error from None


def _unsupported(kind: str) -> None:
    if kind == "rendered_comments":
        raise UnsupportedCommentOperationError("Rendered comments require a typed retained-source parser")
    raise UnsupportedCommentOperationError("Unknown comment source operation has no accounting contract")


def _summary(result: Any, *, expected: int | None, from_root: bool, capped: bool) -> CommentFetchSummary:
    required = (
        "fetch_failed",
        "auth_failed",
        "retryable",
        "reported_comment_count",
        "top_level_checkpoint",
        "reply_checkpoints",
    )
    if any(not hasattr(result, key) for key in required):
        raise CommentSourceError("Fetcher result does not satisfy the comment result contract")
    metadata = getattr(result, "diagnostic_metadata", None) or {}
    if not isinstance(metadata, Mapping):
        raise CommentSourceError("Fetcher diagnostics must be a mapping")
    unknown = metadata.get("expected_count_unknown") is True
    reported = _count(getattr(result, "reported_comment_count", None))
    expected = None if unknown else reported if reported is not None else expected
    top = _checkpoint(getattr(result, "top_level_checkpoint", None), root=from_root)
    replies = getattr(result, "reply_checkpoints", None) or []
    if not isinstance(replies, (list, tuple)) or any(not isinstance(item, Mapping) for item in replies):
        raise CommentSourceError("Fetcher reply checkpoints must be mappings")
    reason = getattr(result, "fetch_reason", None)
    reason_failed = bool(reason and reason not in _SUCCESS_REASONS)
    block_signal = getattr(result, "public_block_signal", "none")
    blocked = bool(block_signal and block_signal != "none")
    # DTO success cannot certify native pagination; CommentActivity separately
    # requires the retained top/reply page and count proofs.
    return CommentFetchSummary(
        expected_count=expected,
        fetch_failed=bool(getattr(result, "fetch_failed", False) or reason_failed or blocked),
        auth_failed=bool(
            getattr(result, "auth_failed", False)
            or metadata.get("session_invalidated")
            or block_signal in {"login_required", "checkpoint", "forbidden", "unauthorized", "session_invalidated"}
        ),
        retryable=bool(getattr(result, "retryable", False) or unknown or expected is None),
        top_level_checkpoint=top,
        reply_checkpoints=tuple(_checkpoint(item, root=from_root) for item in replies if item),
        full_observation=from_root and not capped,
    )
