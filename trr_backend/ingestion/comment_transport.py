"""Explicit-credential adapter from the maintained fetcher to durable comments.

Only HTTP attempts and explicit browser operations are counted at the source
boundary. Browser subresource traffic is not counted by these callbacks.
Rendered comment parsing has no retained native evidence contract and is denied.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

from trr_backend.ingestion.callable_attributes import with_attributes
from trr_backend.ingestion.comments import (
    CommentFetchSummary,
    CommentResumeNode,
    CommentSourceError,
    CursorStalledError,
    comment_page_failure,
    native_child_rejection,
)
from trr_backend.socials.instagram.credential_guard import CredentialIdentityMismatch
from trr_backend.socials.instagram.public_response import parse_public_retry_after

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
        "coverage_target_met",
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
    if pages.get("adapter_family") not in (None, "instagram_comments_rest"):
        raise CommentSourceError("Authenticated comments cannot resume another adapter family")
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
        resume_inventory: tuple[CommentResumeNode, ...] = (),
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
        lanes = pages.get("lanes") or {}
        top_exhausted = (lanes.get("__top__") or {}).get("exhausted") is True
        # Unmappable cursors deliberately restart root; old inventory cannot
        # contribute to that new observation's source coverage.
        seed = resume_inventory if resume["top_level_cursor"] is not None or top_exhausted else ()
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

            def retain_and_classify():
                failure = comment_page_failure(native)
                try:
                    source_observer(kind, native)
                except CommentSourceError:
                    if failure and source_failure_observer is not None:
                        source_failure_observer(*failure)
                    raise
                if failure:
                    if source_failure_observer is not None:
                        source_failure_observer(*failure)
                    raise CommentSourceError("Native comment page contains failed request metadata")

            _guard(retain_and_classify)

        guarded_failure_observer = (
            (lambda code, retry_after: _guard(source_failure_observer, code, retry_after))
            if source_failure_observer is not None
            else None
        )

        async def fetch() -> CommentFetchSummary:
            fetcher = self._factory(
                cookies=copy.deepcopy(self._cookies),
                raw_cookies=dict(self._raw_cookies),
                browser_account_id=self._browser_account_id,
                timeout_ms=self._timeout_ms,
                source_payload_observer=observe,
                before_source_request=before,
                source_failure_observer=guarded_failure_observer,
                consume_credentials_only=True,
            )
            failed = True
            try:
                if self._warmup:
                    await fetcher.warmup()
                parents = _seed_parents(seed, shortcode)
                observed = pages.get("observed") or {}
                pending = [
                    parent
                    for parent in parents
                    if (
                        (lanes.get(parent.comment_id) or {}).get("exhausted") is False
                        or (
                            _count((observed.get(parent.comment_id) or {}).get("expected_replies")) is not None
                            and len(parent.replies) < observed[parent.comment_id]["expected_replies"]
                        )
                    )
                ]
                if pending:
                    reply_result = await fetcher.fetch_comments_for_shortcode(
                        shortcode,
                        max_comments=0,
                        fetch_replies=True,
                        expected_comment_count=expected,
                        expected_count_unknown=expected is None,
                        persisted_top_level_comments=pending,
                        persisted_replies_by_parent={parent.comment_id: parent.replies for parent in pending},
                        reply_resume_cursors=resume["reply_resume_cursors"],
                        reply_resume_cursor_params=resume["reply_resume_cursor_params"],
                        reply_only=True,
                        seed_persisted_comments=True,
                        pending_reply_parent_ids=tuple(parent.comment_id for parent in pending),
                    )
                    reply_summary = _summary(reply_result, expected=expected, from_root=from_root, capped=False)
                    if (
                        reply_summary.fetch_failed
                        or reply_summary.auth_failed
                        or reply_summary.retryable
                        or (reply_summary.reply_checkpoints or reply_summary.top_level_checkpoint)
                    ):
                        failed = False
                        return reply_summary
                    refreshed = {parent.comment_id: parent for parent in getattr(reply_result, "comments", [])}
                    parents = [refreshed.get(parent.comment_id, parent) for parent in parents]
                if top_exhausted:
                    failed = False
                    return CommentFetchSummary(expected_count=expected, full_observation=from_root)
                result = await fetcher.fetch_comments_for_shortcode(
                    shortcode,
                    max_comments=self._max_comments,
                    fetch_replies=True,
                    expected_comment_count=expected,
                    expected_count_unknown=expected is None,
                    load_strategy="instagram_comments_endpoint_cursor",
                    persisted_top_level_comments=parents or None,
                    persisted_replies_by_parent={parent.comment_id: parent.replies for parent in parents} or None,
                    seed_persisted_comments=bool(parents),
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


@dataclass(frozen=True)
class PublicRelayResume:
    top: dict[str, Any]
    replies: dict[str, dict[str, Any]]
    from_root: bool

    @classmethod
    def from_checkpoint(
        cls, checkpoint: Mapping[str, Any], parents: list[Any], *, now: datetime | None = None
    ) -> PublicRelayResume:
        from trr_backend.ingestion.lane_restart import pending_restarts

        restarts = pending_restarts(dict(checkpoint))
        now = now or datetime.now(UTC)
        pages = checkpoint.get("comment_pages") or {}
        if not isinstance(pages, Mapping):
            raise CommentSourceError("Invalid public comment checkpoint")
        lanes = pages.get("lanes") or {}
        observed = pages.get("observed") or {}
        if not isinstance(lanes, Mapping) or not isinstance(observed, Mapping):
            raise CommentSourceError("Invalid public comment lanes")
        if lanes and (pages.get("adapter_family") != "instagram_public_relay" or pages.get("adapter_version") != 1):
            raise CommentSourceError("Public comments cannot reinterpret another adapter's checkpoint")
        if checkpoint.get("top_level_checkpoint") or checkpoint.get("reply_checkpoints"):
            raise CommentSourceError("Public comments cannot consume legacy transport cursors")

        access = pages.get("reply_access") or {}
        retry_at = access.get("next_attempt_at") if access.get("retry_source") == "server_retry_after" else None
        try:
            deferred = bool(retry_at and datetime.fromisoformat(retry_at.replace("Z", "+00:00")) > now)
        except (ValueError, TypeError):
            raise CommentSourceError("Invalid reply access retry timestamp") from None
        rejected_lanes = set(access.get("lanes") or [])
        technical_failures = checkpoint.get("public_reply_failures") or {}

        def reply_guard(parent_id):
            server_deferred = deferred and parent_id in rejected_lanes
            item = {"pending": not server_deferred, "deferred": server_deferred}
            technical = technical_failures.get(parent_id)
            if technical is not None:
                from trr_backend.ingestion.reply_failures import resume_failure

                item.update(resume_failure(technical, now=now))
                item["deferred"] = server_deferred or item["deferred"]
                item["pending"] = not item["deferred"]
                if item["deferred"]:
                    item.pop("needs_diagnostics", None)
            return item

        if "__top__" in restarts:
            # A fresh root invalidates traversal proof, not per-parent safety
            # holds. Parents may be rediscovered after the seed list is cleared.
            return cls({}, {key: reply_guard(key) for key in rejected_lanes | set(technical_failures)}, True)

        def lane_resume(lane, *, root):
            if not isinstance(lane, Mapping):
                raise CommentSourceError("Invalid public Relay lane")
            if lane.get("repeated"):
                raise CursorStalledError("Public Relay lane has a repeated cursor")
            if not lane:
                return {}
            result: dict[str, Any] = {"exhausted": lane.get("exhausted") is True}
            for proof in ("started_from_root", "embedded_complete"):
                if lane.get(proof) is True:
                    result[proof] = True
            if lane.get("relay_doc_id"):
                for key in ("relay_doc_id", "relay_friendly_name", "sort_order", "page_size"):
                    result[key] = lane.get(key)
                if (
                    any(
                        not isinstance(result[key], str) or not result[key]
                        for key in ("relay_doc_id", "relay_friendly_name", "sort_order")
                    )
                    or type(result["page_size"]) is not int
                    or not 1 <= result["page_size"] <= 50
                ):
                    raise CommentSourceError("Public Relay query provenance is incomplete")
                cursors = lane.get("cursors") or []
                if not isinstance(cursors, list) or any(not isinstance(item, str) or not item for item in cursors):
                    raise CommentSourceError("Public Relay cursors are invalid")
                if not result["exhausted"]:
                    if lane.get("cursor_param") != "after" or not cursors:
                        raise CommentSourceError("Public Relay continuation has no proved after cursor")
                    result.update(after=cursors[-1], seen_cursors=list(cursors))
            elif root:
                raise CommentSourceError("Public Relay top query provenance is absent")
            # Embedded child cursors have no proved child query; request that
            # parent's child root instead of borrowing the top query identity.
            return result

        top_lane = lanes.get("__top__") or {}
        top = lane_resume(top_lane, root=True)
        replies = {}
        for parent in parents:
            lane = lanes.get(parent.comment_id) or {}
            if parent.comment_id in restarts:
                replies[parent.comment_id] = {"pending": True}
                continue
            if parent.comment_id in rejected_lanes:
                replies[parent.comment_id] = reply_guard(parent.comment_id)
                continue
            item = lane_resume(lane, root=False)
            expected = (observed.get(parent.comment_id) or {}).get("expected_replies")
            if (observed.get(parent.comment_id) or {}).get("native_reply_leaf") is True:
                item["native_reply_leaf"] = True
            from trr_backend.socials.instagram.comments_scrapling.fetcher import reply_work_skip_reason

            pending = (
                reply_work_skip_reason(
                    expected_replies=expected,
                    observed_replies=len(parent.replies),
                    lane=item,
                    native_leaf=(observed.get(parent.comment_id) or {}).get("native_reply_leaf") is True,
                )
                is None
            )
            item["pending"] = pending
            if pending and item.get("exhausted"):
                item = {"pending": True}
            technical = (checkpoint.get("public_reply_failures") or {}).get(parent.comment_id)
            if technical is not None:
                from trr_backend.ingestion.reply_failures import resume_failure

                item.update(resume_failure(technical, now=now))
            replies[parent.comment_id] = item
        return cls(top, replies, not top_lane or top_lane.get("started_from_root") is True)


class PublicRelayCommentSource:
    """Logged-out maintained Relay only; no credential or proxy inputs."""

    def __init__(
        self,
        *,
        expected_count: int | None = None,
        fetcher_factory: Callable[..., Any] = _default_factory,
        timeout_ms: int = 45_000,
    ):
        if expected_count is not None and _count(expected_count) is None:
            raise ValueError("Expected comment count must be non-negative or unknown")
        self._expected_count, self._factory, self._timeout_ms = expected_count, fetcher_factory, timeout_ms

    def __call__(
        self,
        *,
        shortcode: str,
        checkpoint: dict[str, Any],
        resume_inventory: tuple[CommentResumeNode, ...] = (),
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
        from trr_backend.ingestion.public_parent_diagnostic import run_parent_diagnostic

        run_parent_diagnostic(
            shortcode=shortcode,
            before=pre_source_request,
            factory=self._factory,
            timeout_ms=self._timeout_ms,
            source_failure_observer=source_failure_observer,
        )
        parents = _seed_parents(resume_inventory, shortcode)
        resume = PublicRelayResume.from_checkpoint(
            checkpoint, parents, now=getattr(pre_source_request, "observation_clock", lambda: datetime.now(UTC))()
        )
        if "__top__" in (checkpoint.get("public_lane_restarts") or {}):
            parents = []
        expected = self._expected_count
        if expected is None:
            expected = _count((checkpoint.get("comment_pages") or {}).get("expected"))
        if resume.top.get("exhausted") and not any(item.get("pending") for item in resume.replies.values()):
            deferred = tuple(
                key
                for key, item in resume.replies.items()
                if item.get("deferred") and not item.get("technical_failure")
            )
            return CommentFetchSummary(
                expected_count=expected,
                full_observation=resume.from_root,
                reply_lanes_rejected=deferred,
                reply_access_code="reply_access_rejected" if deferred else None,
                public_reply_failures=tuple(checkpoint.get("public_reply_failures") or {}),
            )

        def before(kind):
            if kind not in {"relay_bootstrap", "graphql_comments", "graphql_replies"}:
                _guard(_unsupported, kind)
            return _guard(pre_source_request)

        for name in (
            "run_id",
            "run_generation",
            "owner_generation",
            "observation_clock",
            "deadline",
            "record_terminal_failure",
            "mark_sent",
            "release_unsent",
        ):
            if hasattr(pre_source_request, name):
                with_attributes(before, **{name: getattr(pre_source_request, name)})
        with_attributes(
            before,
            owner_generation=getattr(pre_source_request, "owner_generation", 0),
            observation_clock=getattr(pre_source_request, "observation_clock", lambda: datetime.now(UTC)),
        )

        rejected_lanes = set()
        technical_failures = set(checkpoint.get("public_reply_failures") or {})
        reply_retry_after = None

        def observe(kind, payload):
            def retain():
                nonlocal reply_retry_after
                rejected_reply = kind == "graphql_replies" and native_child_rejection(payload)
                if rejected_reply:
                    rejected_lanes.add(str(payload.get("parent_comment_id") or ""))
                failure = comment_page_failure(payload)
                if failure is not None:
                    from trr_backend.ingestion.public_diagnostics import record_public_comment_failure_basis

                    record_public_comment_failure_basis(payload, public_only=True)
                header_delay = parse_public_retry_after(payload.get("retry_after"))
                delay = (
                    max(value for value in (header_delay, failure[1]) if value is not None)
                    if failure and any(value is not None for value in (header_delay, failure[1]))
                    else None
                )
                from trr_backend.ingestion.reply_failures import RetainedPublicReplyFailureError

                try:
                    source_observer(kind, payload)
                except RetainedPublicReplyFailureError as error:
                    technical_failures.add(error.parent_id)
                    callback = getattr(source_failure_observer, "record_branch_failure", None)
                    if callback is not None:
                        callback()
                    return
                except CommentSourceError:
                    if rejected_reply:
                        return
                    if failure and source_failure_observer is not None:
                        source_failure_observer(
                            "public_access_backoff" if failure[0] == "source_auth_failed" else failure[0], delay
                        )
                    raise
                if kind == "graphql_replies" and not failure and not rejected_reply:
                    technical_failures.discard(str(payload.get("parent_comment_id") or ""))
                if rejected_reply:
                    if delay is not None:
                        reply_retry_after = max(reply_retry_after or 0, delay)
                    return
                if failure:
                    if source_failure_observer is not None:
                        source_failure_observer(
                            "public_access_backoff" if failure[0] == "source_auth_failed" else failure[0], delay
                        )
                    raise CommentSourceError("Native public comment page contains failed request metadata")

            _guard(retain)

        guarded_failure_observer = (
            (
                lambda code, delay: _guard(
                    source_failure_observer,
                    "public_access_backoff" if code == "source_auth_failed" else code,
                    delay,
                )
            )
            if source_failure_observer
            else None
        )
        fetcher = None

        async def fetch():
            nonlocal fetcher
            fetcher = self._factory(
                cookies=[],
                raw_cookies={},
                browser_account_id=None,
                proxy_config=None,
                public_only=True,
                timeout_ms=self._timeout_ms,
                source_payload_observer=observe,
                before_source_request=before,
                source_failure_observer=guarded_failure_observer,
            )
            failed = True
            try:
                result = await fetcher.fetch_comments_for_shortcode(
                    shortcode,
                    max_comments=0,
                    fetch_replies=True,
                    expected_comment_count=expected,
                    expected_count_unknown=expected is None,
                    load_strategy="public_relay",
                    strict_native_exhaustion=True,
                    public_relay_resume={"top": resume.top, "replies": resume.replies},
                    persisted_top_level_comments=parents,
                )
                summary = _summary(result, expected=expected, from_root=resume.from_root, capped=False)
                completed = set(
                    ((getattr(result, "diagnostic_metadata", None) or {}).get("relay_comments") or {})
                    .get("child_comments", {})
                    .get("completed_parent_ids", [])
                )
                summary = replace(summary, public_reply_failures=tuple(sorted(technical_failures)))
                unresolved = rejected_lanes - completed
                if unresolved:
                    summary = replace(
                        summary,
                        reply_lanes_rejected=tuple(sorted(set(summary.reply_lanes_rejected) | unresolved)),
                        reply_access_code="reply_access_rejected",
                        reply_retry_after_seconds=reply_retry_after,
                    )
                failed = False
                return summary
            finally:
                try:
                    await fetcher.aclose()
                except Exception:
                    if not failed:
                        raise

        try:
            return asyncio.run(fetch())
        except _CallbackAbort as abort:
            raise abort.error from None
        except CredentialIdentityMismatch:
            # A public response cannot move this attempt into a logged-in
            # identity. Classify the fixed violation immediately, without ever
            # retaining cookie names/values or admitting another source send.
            delay = getattr(fetcher, "_public_retry_after_seconds", None)
            if source_failure_observer is not None:
                source_failure_observer("public_access_backoff", delay)
            from trr_backend.ingestion.domain import Failure
            from trr_backend.ingestion.post_transport import SourceFailure

            raise SourceFailure(Failure("public_access_backoff")) from None


def _seed_parents(inventory: tuple[CommentResumeNode, ...], shortcode: str) -> list[Any]:
    if not inventory:
        return []
    from trr_backend.ingestion.canonical import normalize_comment_source
    from trr_backend.socials.instagram.scraper import InstagramComment

    nodes = {}
    for retained in inventory:
        source = retained.source_payload
        parsed = normalize_comment_source(source)
        if not parsed.comment_id or parsed.comment_id in nodes:
            raise CommentSourceError("Resume inventory contains duplicate or absent identities")
        # The canonical integer topology count is not native count evidence.
        # Recover nullable expectations from the retained source on resume.
        count_values = [
            source.get(key)
            for key in ("child_comment_count", "childCommentCount", "repliesCount", "reply_count", "replies_count")
        ]
        thread = source.get("edge_threaded_comments")
        if isinstance(thread, Mapping):
            count_values.append(thread.get("count"))
        native_counts = [value for value in count_values if type(value) is int and value >= 0]
        reported_count = max(native_counts) if native_counts else None
        nodes[parsed.comment_id] = InstagramComment(
            comment_id=parsed.comment_id,
            text=parsed.text,
            username=parsed.username or "",
            user_id=parsed.user_id or "",
            created_at=parsed.created_at or 0,
            date_time="",
            likes=parsed.likes_count,
            is_reply=bool(parsed.parent_comment_id),
            parent_comment_id=parsed.parent_comment_id,
            reply_count=reported_count,
            child_comment_count=reported_count,
            post_shortcode=shortcode,
            post_url=f"https://www.instagram.com/p/{shortcode}/",
        )
    parents = []
    for node in nodes.values():
        if node.parent_comment_id:
            parent = nodes.get(node.parent_comment_id)
            if parent is None or parent.parent_comment_id:
                raise CommentSourceError("Resume inventory has an unsupported or missing parent")
            parent.replies.append(node)
        else:
            parents.append(node)
    return parents


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
    reason_failed = bool(reason and reason not in _SUCCESS_REASONS and reason != "reply_access_rejected")
    block_signal = getattr(result, "public_block_signal", "none")
    blocked = bool(block_signal and block_signal not in {"none", "reply_access_rejected"})
    child = (metadata.get("relay_comments") or {}).get("child_comments") or {}
    completed = set(child.get("completed_parent_ids", []))
    rejected = tuple(
        sorted(
            {str(item["parent_id"]) for item in child.get("rejected_lanes", [])} - completed
            | set(child.get("pending_parent_ids", []))
        )
    )
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
        reply_checkpoints=tuple(
            checkpoint for item in replies if item and (checkpoint := _checkpoint(item, root=from_root)) is not None
        ),
        full_observation=from_root and not capped,
        reply_lanes_rejected=rejected,
        reply_access_code="reply_access_rejected" if rejected else None,
    )
