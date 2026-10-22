"""Reddit HTTP and resumable comment-tree transport, without persistence policy.

``request_guard(lane)`` must return a context manager. The caller owns distributed
rate limits and claim checks; all actual HTTP attempts (including token refresh)
enter that guard. ``response_observer(response)`` can persist shared rate headers.
Checkpoints are JSON-compatible and must be saved by the caller's callback.
"""

from __future__ import annotations

import copy
import hashlib
import math
import os
import re
import threading
import time
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from trr_backend.job_plane import is_remote_job_plane_enabled


def build_client(*, require_oauth: bool = True, recovery_probe: bool = False, **kwargs: Any) -> RedditHttpClient:
    """Bind the durable shared budget hooks for production capture/discovery callers."""
    from trr_backend.repositories import reddit_capture

    client = RedditHttpClient(require_oauth=require_oauth, **kwargs)
    identity = hashlib.sha256((client.client_id or "anonymous").encode()).hexdigest()
    client.request_guard = lambda lane: reddit_capture.request_guard(identity, lane, allow_blocked=recovery_probe)
    client.response_observer = lambda response: reddit_capture.observe_response(
        identity, response.status_code, response.headers
    )
    if not recovery_probe:
        get_json = client.get_json

        def guarded_json(*args, **options):
            try:
                return get_json(*args, **options)
            except RedditTransportError as exc:
                if exc.status in {401, 403}:
                    reddit_capture.pg.execute_returning(
                        """insert into social.reddit_request_budgets(identity_key,blocked)
                        values(%s,true) on conflict(identity_key) do update set blocked=true,updated_at=now()
                        returning identity_key""",
                        [identity],
                    )
                    raise reddit_capture.IdentityBlockedError(
                        "Reddit OAuth identity blocked by authentication or authorization failure"
                    ) from None
                raise

        client.get_json = guarded_json
    return client


class RedditTransportError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status: int = 502,
        retry_after_seconds: float | None = None,
        checkpoint: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.retry_after_seconds = retry_after_seconds
        self.checkpoint = checkpoint


class _NetworkError(RedditTransportError):
    """Only failures from the HTTP call itself may enter transport retries."""


def _number(value: Any, default: float) -> float:
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _env_number(name: str, default: float, minimum: float, maximum: float) -> float:
    return min(maximum, max(minimum, _number(os.getenv(name), default)))


def _id(value: Any, *, prefix: str = "") -> str:
    normalized = str(value or "").strip().lower()
    if prefix and normalized.startswith(prefix):
        normalized = normalized[len(prefix) :]
    if not re.fullmatch(r"[a-z0-9]+", normalized):
        raise RedditTransportError("Malformed Reddit object ID")
    return normalized


class RedditHttpClient:
    def __init__(
        self,
        *,
        require_oauth: bool | None = None,
        request_guard: Callable[[str], AbstractContextManager[Any]] | None = None,
        response_observer: Callable[[Any], None] | None = None,
        session: Any = None,
        max_more_requests: int | None = None,
    ) -> None:
        self.session = session if session is not None else requests.Session()
        self.timeout_seconds = _env_number("REDDIT_FETCH_TIMEOUT_SECONDS", 20, 1, 120)
        self.max_retries = int(_env_number("REDDIT_FETCH_MAX_RETRIES", 5, 1, 8))
        self.page_cooldown = _env_number("REDDIT_PAGE_COOLDOWN_SECONDS", 0.05, 0, 60)
        self.rate_limit_delay = _env_number("REDDIT_RATE_LIMIT_DELAY_SECONDS", 3.5, 0, 3600)
        self.client_id = (os.getenv("REDDIT_CLIENT_ID") or "").strip()
        self.client_secret = (os.getenv("REDDIT_CLIENT_SECRET") or "").strip()
        self.user_agent = (os.getenv("REDDIT_USER_AGENT") or "").strip() or (
            "TRRBackendRedditRefresh/1.0 (+https://thereality.report)"
        )
        explicit_required = str(os.getenv("REDDIT_REQUIRE_OAUTH") or "").strip().lower()
        default_required = is_remote_job_plane_enabled() or explicit_required in {"1", "true", "yes", "on"}
        self.require_oauth = default_required if require_oauth is None else require_oauth
        self.request_guard = request_guard if request_guard is not None else lambda _lane: nullcontext()
        self.response_observer = response_observer
        self.max_more_requests = (
            int(_env_number("REDDIT_MORECHILDREN_MAX_REQUESTS", 100, 0, 10000))
            if max_more_requests is None
            else max(0, min(10000, int(max_more_requests)))
        )
        self._oauth_token: str | None = None
        self._oauth_expires_at = 0.0
        self._state_lock = threading.Lock()
        self._oauth_refresh_lock = threading.Lock()
        self._adaptive_cooldown = self.page_cooldown
        self._adaptive_cooldown_min = self.page_cooldown
        self._adaptive_cooldown_max = max(0.5, self.page_cooldown * 10)
        self._cooldown_until = 0.0

    def _wait_for_cooldown(self) -> None:
        with self._state_lock:
            delay = self._cooldown_until - time.time()
        if delay > 0:
            time.sleep(delay)

    def _observe(self, response: Any) -> float:
        headers = {str(key).lower(): value for key, value in (getattr(response, "headers", {}) or {}).items()}
        delay = max(0.0, _number(headers.get("retry-after"), 0.0))
        if headers.get("retry-after") and not delay:
            try:
                delay = max(0, parsedate_to_datetime(str(headers["retry-after"])).timestamp() - time.time())
            except (TypeError, ValueError, OverflowError):
                pass
        if _number(headers.get("x-ratelimit-remaining"), 1) <= 0:
            delay = max(delay, _number(headers.get("x-ratelimit-reset"), 0))
        if response.status_code == 429:
            delay = max(delay, self.rate_limit_delay)
            with self._state_lock:
                self._adaptive_cooldown = min(
                    self._adaptive_cooldown_max, max(self.page_cooldown, self._adaptive_cooldown * 2)
                )
        with self._state_lock:
            self._cooldown_until = max(self._cooldown_until, time.time() + delay)
        if self.response_observer is not None:
            self.response_observer(response)
        return delay

    def _request(self, method: str, url: str, *, lane: str, **kwargs: Any) -> Any:
        self._wait_for_cooldown()
        # Guard entry/exit and observer exceptions propagate: claim loss is never retried.
        with self.request_guard(lane):
            try:
                response = getattr(self.session, method)(url, timeout=self.timeout_seconds, **kwargs)
            except requests.RequestException:
                raise _NetworkError("Reddit network request failed", status=503) from None
            self._observe(response)
            return response

    def _get_oauth_token(self) -> str | None:
        if not self.client_id or not self.client_secret:
            if self.require_oauth:
                raise RedditTransportError("Reddit OAuth credentials are required", status=401)
            return None
        with self._oauth_refresh_lock:
            with self._state_lock:
                if self._oauth_token and time.time() < self._oauth_expires_at - 30:
                    return self._oauth_token
            for attempt in range(self.max_retries):
                try:
                    response = self._request(
                        "post",
                        "https://www.reddit.com/api/v1/access_token",
                        lane="oauth",
                        headers={"User-Agent": self.user_agent},
                        auth=(self.client_id, self.client_secret),
                        data={"grant_type": "client_credentials"},
                    )
                except _NetworkError:
                    if attempt + 1 == self.max_retries:
                        raise
                    time.sleep(0.2 * 2**attempt)
                    continue
                status = response.status_code
                if status == 429 or status >= 500:
                    if attempt + 1 < self.max_retries:
                        if status != 429:
                            time.sleep(0.2 * 2**attempt)
                        continue
                if status >= 400:
                    raise RedditTransportError(
                        "Reddit OAuth authentication failed",
                        status=status,
                        retry_after_seconds=self._remaining_cooldown() or None,
                    )
                try:
                    payload = response.json()
                except (ValueError, TypeError):
                    raise RedditTransportError("Malformed Reddit OAuth response") from None
                if not isinstance(payload, dict) or not isinstance(payload.get("access_token"), str):
                    raise RedditTransportError("Malformed Reddit OAuth response")
                token = payload["access_token"].strip()
                expires = _number(payload.get("expires_in"), 3600)
                if not token or expires <= 30 or payload.get("error"):
                    raise RedditTransportError("Invalid Reddit OAuth response", status=401)
                with self._state_lock:
                    self._oauth_token = token
                    self._oauth_expires_at = time.time() + expires
                return token
        raise RedditTransportError("Reddit OAuth authentication failed", status=401)

    def _remaining_cooldown(self) -> float:
        with self._state_lock:
            return max(0.0, self._cooldown_until - time.time())

    def _auth_headers(self, *, use_oauth: bool) -> dict[str, str]:
        headers = {"User-Agent": self.user_agent}
        if use_oauth:
            token = self._get_oauth_token()
            if not token:
                raise RedditTransportError("Reddit OAuth credentials are required", status=401)
            headers["Authorization"] = f"Bearer {token}"
        elif self.require_oauth:
            raise RedditTransportError("Anonymous Reddit requests are disabled", status=401)
        return headers

    def get_json(
        self,
        path: str,
        *,
        params: dict[str, Any],
        expected_type: type = dict,
        lane: str = "request",
    ) -> Any:
        if not path.startswith("/") or path.startswith("//") or expected_type not in (dict, list):
            raise ValueError("A Reddit-relative path and dict/list JSON type are required")
        use_oauth = self.require_oauth or bool(self.client_id or self.client_secret)
        base = "https://oauth.reddit.com" if use_oauth else "https://www.reddit.com"
        refreshed = False
        attempt = 0
        while attempt < self.max_retries:
            headers = self._auth_headers(use_oauth=use_oauth)
            try:
                response = self._request("get", base + path, lane=lane, params=params, headers=headers)
            except _NetworkError:
                attempt += 1
                if attempt == self.max_retries:
                    raise
                time.sleep(0.2 * 2 ** (attempt - 1))
                continue
            status = response.status_code
            if status == 401 and use_oauth and not refreshed:
                with self._state_lock:
                    # Do not invalidate another thread's newly refreshed token.
                    if headers.get("Authorization") == f"Bearer {self._oauth_token}":
                        self._oauth_token = None
                        self._oauth_expires_at = 0.0
                refreshed = True
                continue
            attempt += 1
            if status >= 400:
                if (status == 429 or status >= 500) and attempt < self.max_retries:
                    if status != 429:
                        time.sleep(0.2 * 2 ** (attempt - 1))
                    continue
                raise RedditTransportError(
                    f"Reddit request failed ({status})",
                    status=status,
                    retry_after_seconds=self._remaining_cooldown() or None,
                )
            try:
                payload = response.json()
            except (ValueError, TypeError):
                raise RedditTransportError("Malformed Reddit JSON response") from None
            if not isinstance(payload, expected_type):
                raise RedditTransportError(f"Expected Reddit JSON {expected_type.__name__}")
            with self._state_lock:
                delay = self._adaptive_cooldown
                self._adaptive_cooldown = max(self._adaptive_cooldown_min, self._adaptive_cooldown * 0.9)
            if delay > 0:
                time.sleep(delay)
            return payload
        raise RedditTransportError("Reddit retries exhausted")

    def token_probe(self) -> dict[str, Any]:
        """Probe configuration, token grant and OAuth reachability without returning credentials."""
        configured = bool(self.client_id and self.client_secret)
        result: dict[str, Any] = {
            "configured": configured,
            "authenticated": False,
            "reachable": None,
            "cooling_down": self._remaining_cooldown() > 0,
            "blocked": False,
        }
        if result["cooling_down"]:
            return result
        if not configured:
            result["blocked"] = self.require_oauth
            return result
        try:
            result["authenticated"] = bool(self._get_oauth_token())
            self.get_json("/api/v1/scopes", params={})
            result["reachable"] = True
        except RedditTransportError as exc:
            result["reachable"] = exc.status not in {502, 503, 504}
            result["blocked"] = exc.status in {401, 403}
            if exc.status == 401:
                result["authenticated"] = False
            result["status"] = exc.status
        result["cooling_down"] = self._remaining_cooldown() > 0
        return result

    def get_comments(
        self,
        post_id: str,
        checkpoint: dict[str, Any] | None = None,
        on_checkpoint: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        post_id = _id(post_id, prefix="t3_")
        comments: dict[str, dict[str, Any]] = {}
        pending: dict[str, dict[str, Any]] = {}
        continuations: dict[str, dict[str, Any]] = {}
        evidence: dict[str, Any] = {"expansion_requests": 0, "errors": []}

        def snapshot() -> dict[str, Any]:
            _resolve_depths(comments)
            for queue in (pending, continuations):
                for item in queue.values():
                    parent = item["parent_id"]
                    if parent == f"t3_{post_id}":
                        item["depth"] = 0
                    elif parent[3:] in comments:
                        item["depth"] = comments[parent[3:]]["depth"] + 1
            return copy.deepcopy(
                {
                    "version": 1,
                    "post_id": post_id,
                    "initial_fetched": True,
                    "comments": list(comments.values()),
                    "pending_children": list(pending.values()),
                    "pending_continuations": list(continuations.values()),
                }
            )

        def save() -> None:
            if on_checkpoint is not None:
                on_checkpoint(snapshot())

        if isinstance(checkpoint, dict) and checkpoint.get("initial_fetched") is False:
            if (
                checkpoint.get("version") != 1
                or checkpoint.get("post_id") != post_id
                or checkpoint.get("comments", []) != []
                or checkpoint.get("pending_children", []) != []
                or checkpoint.get("pending_continuations", []) != []
            ):
                raise RedditTransportError("Invalid unfinished Reddit initial-listing checkpoint")
            # The initial endpoint is atomic; a saved cursor means refetch it, never empty success.
            checkpoint = None
        if checkpoint is not None:
            _restore_checkpoint(
                checkpoint, post_id=post_id, comments=comments, pending=pending, continuations=continuations
            )
        else:
            try:
                payload = self.get_json(
                    f"/comments/{post_id}.json",
                    params={
                        "limit": int(_env_number("REDDIT_COMMENT_LIMIT", 500, 50, 500)),
                        "depth": int(_env_number("REDDIT_COMMENT_TREE_DEPTH", 12, 1, 20)),
                        "raw_json": 1,
                    },
                    expected_type=list,
                )
            except RedditTransportError as exc:
                if exc.status in {404, 410}:
                    return {
                        "comments": [],
                        "unresolved_ids": [],
                        "complete": False,
                        "source_unavailable": True,
                        "evidence": {"status": exc.status, "reason": "source_http_unavailable"},
                    }
                exc.checkpoint = {
                    "version": 1,
                    "post_id": post_id,
                    "initial_fetched": False,
                    "initial_cursor": f"/comments/{post_id}.json",
                    "comments": [],
                    "pending_children": [],
                    "pending_continuations": [],
                }
                if on_checkpoint is not None:
                    on_checkpoint(copy.deepcopy(exc.checkpoint))
                raise
            if len(payload) != 2:
                raise RedditTransportError("Malformed Reddit comments listing")
            submissions = _listing_children(payload[0])
            if not any(
                isinstance(node, dict)
                and node.get("kind") == "t3"
                and isinstance(node.get("data"), dict)
                and node["data"].get("id") == post_id
                for node in submissions
            ):
                raise RedditTransportError("Reddit comments listing does not match requested post")
            parsed_comments, parsed_pending, parsed_continuations = _parse_nodes(
                _listing_children(payload[1]), post_id=post_id
            )
            comments.update(parsed_comments)
            pending.update(parsed_pending)
            continuations.update(parsed_continuations)
            for comment_id in comments:
                pending.pop(comment_id, None)
            save()

        attempted: set[str] = set()
        attempted_continuations: set[str] = set()
        while (pending or continuations) and evidence["expansion_requests"] < self.max_more_requests:
            batch = [child_id for child_id in pending if child_id not in attempted][:100]
            if not batch:
                parent = next((key for key in continuations if key not in attempted_continuations), None)
                if parent is None:
                    break
                attempted_continuations.add(parent)
                if not parent.startswith("t1_"):
                    # A submission-level marker gives no comment subtree to request.
                    continue
                evidence["expansion_requests"] += 1
                try:
                    branch = self.get_json(
                        f"/comments/{post_id}.json",
                        params={
                            "comment": parent[3:],
                            "context": 0,
                            "limit": int(_env_number("REDDIT_COMMENT_LIMIT", 500, 50, 500)),
                            "depth": int(_env_number("REDDIT_COMMENT_TREE_DEPTH", 12, 1, 20)),
                            "raw_json": 1,
                        },
                        expected_type=list,
                        lane="morechildren",
                    )
                    if len(branch) != 2 or not any(
                        node.get("kind") == "t3" and node.get("data", {}).get("id") == post_id
                        for node in _listing_children(branch[0])
                        if isinstance(node, dict) and isinstance(node.get("data"), dict)
                    ):
                        raise RedditTransportError("Reddit continuation does not match requested post")
                    roots = _listing_children(branch[1])
                    if (
                        len(roots) != 1
                        or not isinstance(roots[0], dict)
                        or roots[0].get("kind") != "t1"
                        or not isinstance(roots[0].get("data"), dict)
                        or roots[0]["data"].get("id") != parent[3:]
                    ):
                        raise RedditTransportError("Reddit continuation did not return its requested parent")
                    added, more, added_continuations = _parse_nodes(roots, post_id=post_id)
                    anchor = added.get(parent[3:])
                    if anchor is None:
                        raise RedditTransportError("Reddit continuation did not return its requested parent")
                    previous = comments.get(parent[3:])
                    if previous is not None and anchor["parent_comment_id"] != previous["parent_comment_id"]:
                        raise RedditTransportError("Reddit continuation changed its parent's identity")
                    # The subtree's depth restarts at zero. Preserve its saved anchor,
                    # then derive all descendant depths from canonical parent links.
                    anchor["depth"] = previous["depth"] if previous else max(0, continuations[parent]["depth"] - 1)
                    merged = copy.deepcopy(comments)
                    merged.update(added)
                    _resolve_depths(merged)
                except RedditTransportError as exc:
                    evidence["errors"].append(
                        {"status": exc.status, "message": str(exc), "retry_after_seconds": exc.retry_after_seconds}
                    )
                    save()
                    break
                comments.update(merged)
                # Only a validated subtree response can consume a continuation.
                # A repeated marker stays unresolved and is retried on a later pass.
                continuations.pop(parent)
                continuations.update(added_continuations)
                for child_id, metadata in more.items():
                    pending.setdefault(child_id, metadata)
                for comment_id in comments:
                    pending.pop(comment_id, None)
                save()
                continue
            attempted.update(batch)
            evidence["expansion_requests"] += 1
            try:
                payload = self.get_json(
                    "/api/morechildren.json",
                    params={
                        "api_type": "json",
                        "link_id": f"t3_{post_id}",
                        "children": ",".join(batch),
                        "raw_json": 1,
                        "limit_children": False,
                    },
                    lane="morechildren",
                )
                envelope = payload.get("json")
                if not isinstance(envelope, dict) or envelope.get("errors") not in ([], None):
                    raise RedditTransportError("Malformed Reddit morechildren response")
                data = envelope.get("data")
                if not isinstance(data, dict) or not isinstance(data.get("things"), list):
                    raise RedditTransportError("Malformed Reddit morechildren things")
                # Parse atomically: malformed nodes must not consume pending child IDs.
                added, more, added_continuations = _parse_nodes(data["things"], post_id=post_id, pending=pending)
                merged = copy.deepcopy(comments)
                merged.update(added)
                _resolve_depths(merged)
            except RedditTransportError as exc:
                evidence["errors"].append(
                    {"status": exc.status, "message": str(exc), "retry_after_seconds": exc.retry_after_seconds}
                )
                save()
                break
            comments.update(merged)
            continuations.update(added_continuations)
            for child_id, metadata in more.items():
                pending.setdefault(child_id, metadata)
            for comment_id in comments:
                pending.pop(comment_id, None)
            save()
            missing = {child_id: pending[child_id] for child_id in batch if child_id in pending}
            if missing and evidence["expansion_requests"] < self.max_more_requests:
                evidence["expansion_requests"] += 1
                try:
                    info = self.get_json(
                        "/api/info.json",
                        params={"id": ",".join(f"t1_{child_id}" for child_id in missing), "raw_json": 1},
                        lane="morechildren",
                    )
                    stubs = _parse_unavailable_nodes(_listing_children(info), post_id=post_id, pending=missing)
                    merged = copy.deepcopy(comments)
                    merged.update(stubs)
                    _resolve_depths(merged)
                except RedditTransportError as exc:
                    evidence["errors"].append(
                        {"status": exc.status, "message": str(exc), "retry_after_seconds": exc.retry_after_seconds}
                    )
                    save()
                    break
                comments.update(merged)
                for comment_id in stubs:
                    pending.pop(comment_id, None)
                save()
        if pending and not evidence["errors"]:
            evidence["errors"].append(
                {
                    "reason": "expansion_budget_exhausted"
                    if evidence["expansion_requests"] >= self.max_more_requests
                    else "children_not_returned",
                }
            )
        if continuations:
            evidence["errors"].append({"reason": "unresolved_continuations", "count": len(continuations)})
        evidence["pending_continuations_count"] = len(continuations)
        evidence["unavailable_comments"] = [
            {"comment_id": comment_id, "reason": row["body"][1:-1]}
            for comment_id, row in sorted(comments.items())
            if isinstance(row.get("body"), str) and row["body"] in {"[deleted]", "[removed]"}
        ]
        evidence["checkpoint"] = snapshot()
        return {
            "comments": list(comments.values()),
            "unresolved_ids": list(pending),
            "complete": not pending and not continuations,
            "source_unavailable": False,
            "evidence": evidence,
        }


def _listing_children(listing: Any) -> list[Any]:
    if not isinstance(listing, dict) or listing.get("kind") != "Listing":
        raise RedditTransportError("Malformed Reddit listing")
    data = listing.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("children"), list):
        raise RedditTransportError("Malformed Reddit listing children")
    return data["children"]


def _parse_unavailable_nodes(
    nodes: list[Any], *, post_id: str, pending: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Resolve only explicit source tombstones omitted by morechildren."""
    stubs = []
    seen = set()
    for node in nodes:
        if not isinstance(node, dict) or node.get("kind") != "t1" or not isinstance(node.get("data"), dict):
            raise RedditTransportError("Malformed Reddit unavailable-comment lookup")
        data = node["data"]
        comment_id = _id(data.get("id"), prefix="t1_")
        if (
            data.get("id") != comment_id
            or comment_id not in pending
            or comment_id in seen
            or data.get("link_id") != f"t3_{post_id}"
            or data.get("parent_id") != pending[comment_id]["parent_id"]
            or data.get("name") not in (None, f"t1_{comment_id}")
        ):
            raise RedditTransportError("Reddit unavailable-comment lookup changed the requested identity")
        seen.add(comment_id)
        if data.get("replies") not in (None, ""):
            raise RedditTransportError("Reddit unavailable-comment lookup returned an unexpected subtree")
        if not isinstance(data.get("body"), str):
            raise RedditTransportError("Malformed Reddit unavailable-comment body")
        if data.get("body") in {"[deleted]", "[removed]"}:
            stubs.append(node)
    added, _, _ = _parse_nodes(stubs, post_id=post_id, pending=pending)
    return added


def _parse_nodes(
    nodes: list[Any],
    *,
    post_id: str,
    pending: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    comments: dict[str, dict[str, Any]] = {}
    more: dict[str, dict[str, Any]] = {}
    continuations: dict[str, dict[str, Any]] = {}
    stack = [(node, 0) for node in reversed(nodes)]
    while stack:
        node, depth = stack.pop()
        if not isinstance(node, dict) or not isinstance(node.get("data"), dict):
            raise RedditTransportError("Malformed Reddit comment node")
        data = node["data"]
        supplied_depth = data.get("depth")
        if isinstance(supplied_depth, int) and supplied_depth >= 0:
            depth = supplied_depth
        parent = str(data.get("parent_id") or "")
        if not (parent == f"t3_{post_id}" or re.fullmatch(r"t1_[a-z0-9]+", parent)):
            raise RedditTransportError("Malformed Reddit comment parent")
        if data.get("link_id") not in (None, f"t3_{post_id}"):
            raise RedditTransportError("Reddit comment belongs to another post")
        if node.get("kind") == "more":
            children = data.get("children")
            if not isinstance(children, list):
                raise RedditTransportError("Malformed Reddit more node children")
            if not children:
                # Reddit can omit IDs for "continue this thread" nodes, even at count=0.
                # They are unresolved source work, never proof that the tree is exhausted.
                continuations.setdefault(
                    parent,
                    {
                        "parent_id": parent,
                        "depth": depth,
                        "count": max(0, int(_number(data.get("count"), 0))),
                        "raw_payload": copy.deepcopy(data),
                    },
                )
            for child in children:
                child_id = _id(child, prefix="t1_")
                more.setdefault(child_id, {"id": child_id, "parent_id": parent, "depth": depth})
            continue
        if node.get("kind") != "t1":
            raise RedditTransportError("Unexpected Reddit comment node kind")
        comment_id = _id(data.get("id"), prefix="t1_")
        if pending and comment_id in pending:
            depth = pending[comment_id]["depth"]
        created = None
        try:
            if data.get("created_utc") is not None:
                created = datetime.fromtimestamp(float(data["created_utc"]), UTC).isoformat().replace("+00:00", "Z")
        except (ValueError, TypeError, OverflowError, OSError):
            pass
        comments.setdefault(
            comment_id,
            {
                "reddit_comment_id": comment_id,
                "reddit_post_id": post_id,
                "parent_comment_id": parent[3:] if parent.startswith("t1_") else None,
                "author": str(data.get("author") or "") or None,
                "body": str(data.get("body") or ""),
                "score": int(_number(data.get("score"), 0)),
                "depth": depth,
                "created_at_utc": created,
                "raw_payload": copy.deepcopy(data),
            },
        )
        replies = data.get("replies")
        if replies not in (None, ""):
            children = _listing_children(replies)
            stack.extend((child, depth + 1) for child in reversed(children))
    return comments, more, continuations


def _resolve_depths(comments: dict[str, dict[str, Any]]) -> None:
    resolved: set[str] = set()
    for comment_id in comments:
        chain: list[str] = []
        seen: set[str] = set()
        current = comment_id
        while current in comments and current not in resolved:
            if current in seen:
                raise RedditTransportError("Cyclic Reddit comment parents")
            seen.add(current)
            chain.append(current)
            parent = comments[current]["parent_comment_id"]
            if parent is None:
                break
            current = parent
        for child_id in reversed(chain):
            row = comments[child_id]
            parent = row["parent_comment_id"]
            if parent is None:
                row["depth"] = 0
            elif parent in comments:
                row["depth"] = comments[parent]["depth"] + 1
            resolved.add(child_id)


def _restore_checkpoint(
    checkpoint: dict[str, Any],
    *,
    post_id: str,
    comments: dict[str, dict[str, Any]],
    pending: dict[str, dict[str, Any]],
    continuations: dict[str, dict[str, Any]],
) -> None:
    if not isinstance(checkpoint, dict) or checkpoint.get("version") != 1 or checkpoint.get("post_id") != post_id:
        raise RedditTransportError("Invalid Reddit comment checkpoint")
    if checkpoint.get("initial_fetched") is not True:
        raise RedditTransportError("Incomplete Reddit initial-listing checkpoint")
    rows, children = checkpoint.get("comments"), checkpoint.get("pending_children")
    if not isinstance(rows, list) or not isinstance(children, list):
        raise RedditTransportError("Invalid Reddit checkpoint rows")
    for row in copy.deepcopy(rows):
        if not isinstance(row, dict) or row.get("reddit_post_id") != post_id:
            raise RedditTransportError("Invalid Reddit checkpoint comment")
        comment_id = _id(row.get("reddit_comment_id"))
        if not isinstance(row.get("depth"), int) or row["depth"] < 0 or "parent_comment_id" not in row:
            raise RedditTransportError("Invalid Reddit checkpoint comment metadata")
        if row["parent_comment_id"] is not None:
            _id(row["parent_comment_id"])
        comments.setdefault(comment_id, row)
    for child in copy.deepcopy(children):
        if not isinstance(child, dict):
            raise RedditTransportError("Invalid Reddit checkpoint child")
        child_id = _id(child.get("id"))
        if not isinstance(child.get("depth"), int) or child["depth"] < 0:
            raise RedditTransportError("Invalid Reddit checkpoint depth")
        parent = child.get("parent_id")
        if parent != f"t3_{post_id}" and not re.fullmatch(r"t1_[a-z0-9]+", str(parent or "")):
            raise RedditTransportError("Invalid Reddit checkpoint parent")
        if child_id not in comments:
            pending.setdefault(child_id, child)
    saved_continuations = checkpoint.get("pending_continuations", [])
    if not isinstance(saved_continuations, list):
        raise RedditTransportError("Invalid Reddit checkpoint continuations")
    for continuation in copy.deepcopy(saved_continuations):
        if not isinstance(continuation, dict):
            raise RedditTransportError("Invalid Reddit checkpoint continuation")
        parent = continuation.get("parent_id")
        if parent != f"t3_{post_id}" and not re.fullmatch(r"t1_[a-z0-9]+", str(parent or "")):
            raise RedditTransportError("Invalid Reddit continuation parent")
        if not isinstance(continuation.get("depth"), int) or continuation["depth"] < 0:
            raise RedditTransportError("Invalid Reddit continuation depth")
        continuations.setdefault(parent, continuation)
    _resolve_depths(comments)
