"""One fenced activity invocation; no engine-level retries or credential repair."""

from __future__ import annotations

import json
import logging
import math
import os
import sys
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.application import PostActivity
from trr_backend.ingestion.comments import CommentActivity
from trr_backend.ingestion.domain import Failure, LostOwnership, Outcome, RunIntent, Stage
from trr_backend.ingestion.freshness import DetailCacheDecision
from trr_backend.ingestion.listing import ListingActivity
from trr_backend.ingestion.media import MediaActivity
from trr_backend.ingestion.persistence import DIAGNOSTIC_EXCEPTION_CLASSES, Store, _row

LOGGER = logging.getLogger(__name__)


def failure_diagnostic(error: Exception | None, failure: Failure) -> tuple[str, str]:
    """Fixed diagnostic vocabulary; never serialize an exception or its message."""
    import psycopg2

    from trr_backend.ingestion.canonical import CanonicalWriteError
    from trr_backend.socials.instagram.comments_scrapling.reply_scheduler import DiskResponseSpool

    # Driver SQLSTATE subclasses have many generated names. Normalize by the
    # actual exception family so those names cannot hide a persistence failure.
    for family, safe_name in (
        (psycopg2.InterfaceError, "InterfaceError"),
        (psycopg2.OperationalError, "OperationalError"),
        (psycopg2.IntegrityError, "IntegrityError"),
        (psycopg2.Error, "DatabaseError"),
    ):
        if isinstance(error, family):
            return safe_name, "persistence_error"
    if isinstance(error, CanonicalWriteError):
        return "ValueError", "persistence_error"
    if isinstance(error, DiskResponseSpool.ProtocolLimitError):
        return "ReplyProtocolLimitError", "source_parse_error"
    name = type(error).__name__ if error is not None else "NoException"
    safe_class = name if name in DIAGNOSTIC_EXCEPTION_CLASSES else "OtherError"
    if failure.code == "target_deadline" or safe_class == "ReplyDeadlineExceeded":
        category = "attempt_deadline"
    elif safe_class in {"OperationalError", "InterfaceError", "IntegrityError", "DatabaseError"}:
        category = "persistence_error"
    elif failure.code == "parse_error" or safe_class in {
        "CommentSourceError",
        "CursorStalledError",
        "ReplyProtocolLimitError",
        "JSONDecodeError",
    }:
        category = "source_parse_error"
    elif safe_class in {"ReadTimeout", "ConnectTimeout", "ReadError", "ConnectError", "TimeoutError"}:
        category = "transport_error"
    elif safe_class in {"SourceFailure", "IdentityDeferred"}:
        category = "source_stop"
    elif error is None:
        category = "incomplete_observation"
    else:
        category = "activity_error"
    return safe_class, category


def classify_failure(error: Exception) -> Failure:
    supplied = getattr(error, "failure", None)
    if isinstance(supplied, Failure):
        return supplied
    from trr_backend.socials.instagram.detail_transport import AUTH_ERRORS
    from trr_backend.socials.instagram.request_client import InstagramRequestFailure

    if isinstance(error, InstagramRequestFailure):
        return Failure(
            error.error_code,
            retry_at=getattr(error, "next_attempt_at", None),
            identity_blocked=error.error_code in AUTH_ERRORS,
        )
    # Raw exception text can contain signed URLs, cookies, or SQL parameters.
    # Retain a bounded class-independent code; evidence holds source content.
    return Failure("activity_failed")


class Worker:
    def __init__(
        self,
        store: Store,
        *,
        listing_source: Callable,
        detail_source: Callable,
        comment_source: Callable,
        media_loader: Callable,
        media_storage: Callable,
        detail_cache: Callable | None = None,
        profile_source: Callable | None = None,
    ):
        self.store = store
        self.listing_source = listing_source
        self.detail_source = detail_source
        self.comment_source = comment_source
        self.media_loader = media_loader
        self.media_storage = media_storage
        self.detail_cache = detail_cache
        self.profile_source = profile_source

    def cached_detail(self, context: dict[str, Any], owned: dict[str, Any]) -> dict[str, Any] | None:
        """Settle eligible existing data without inventing a new source capture."""
        if self.detail_cache is None or RunIntent.from_payload(context["run"]["intent"]).force:
            return None
        decision = self.detail_cache(context, owned)
        if decision is None:
            return None
        if not isinstance(decision, DetailCacheDecision):
            raise TypeError("Detail cache must return a typed eligibility decision")
        if not decision.eligible:
            return None
        from trr_backend.ingestion.ordered_listing import is_ordered

        if is_ordered(RunIntent.from_payload(context["run"]["intent"])) and decision.evidence_reference is None:
            return None
        if decision.observed_at is None or decision.observed_at.utcoffset() is None:
            raise ValueError("Cache settlement requires its original source observation")
        if any(not decision.canonical_identity.get(key) for key in ("post_id", "catalog_post_id", "canonical_post_id")):
            raise ValueError("Cache settlement requires complete canonical identities")
        self.store.acquire(**owned)
        identity: dict[str, Any] = dict(decision.canonical_identity)
        # Historical evidence belongs to its original run/target. Carry its
        # validated reference as provenance, never as the current receipt's FK.
        identity["cache_source"] = decision.evidence_reference or {"reason": decision.reason}
        return self.store.commit_target(
            **owned,
            observed_at=decision.observed_at,
            evidence_id=None,
            outcome=Outcome.CACHED,
            write_canonical=lambda conn: identity,
        )

    def context(self, owned):
        with self.store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = self.store._execution_run(cur, owned["run_id"], owned["account"])
            if run["generation"] != owned["generation"] or run["state"] not in {"queued", "running"}:
                raise LostOwnership("Run is no longer active")
            target = _row(
                cur,
                """select * from social.instagram_ingestion_targets
                where run_id=%s and stage=%s and target_key=%s and owner_token=%s
                and owner_expires_at>clock_timestamp() and outcome='unresolved'""",
                (owned["run_id"], owned["stage"].value, owned["target_key"], owned["owner_token"]),
            )
            if not target:
                raise LostOwnership("Reservation is no longer current")
            post = None
            if owned["stage"] == Stage.COMMENTS:
                post = _row(
                    cur,
                    "select id,comments_count from social.instagram_posts where shortcode=%s",
                    (owned["target_key"],),
                )
            return {"run": run, "target": target, "post": post}

    def release(self, owned, failure, *, error=None):
        # A comment observer may have committed additional pages immediately
        # before the source raised. Reload durable state, never overwrite it with
        # the checkpoint captured at the beginning of this attempt.
        context = self.context(owned)
        if context["target"]["owner_started_at"] is None:
            self.store.acquire(**owned)
        exception_class, failure_category = failure_diagnostic(error, failure)
        self.store.checkpoint_failure(
            **owned,
            failure=failure,
            checkpoint=context["target"]["checkpoint"],
            requests=0,
            exception_class=exception_class,
            failure_category=failure_category,
        )

    def execute(self, reservation: dict[str, Any], *, deadline_callback: Callable | None = None) -> dict[str, Any]:
        owned = {key: reservation[key] for key in ("run_id", "account", "generation", "target_key", "owner_token")}
        owned["stage"] = Stage(reservation["stage"])
        args = {key: value for key, value in owned.items() if key != "stage"}
        source = None
        source_error = None
        source_settled = False

        def fetch(factory, **callbacks):
            nonlocal source
            # Activities acquire/check their fence before invoking fetch. Keep
            # credentials and source construction behind that boundary too.
            if source is None:
                source = factory(context, owned)
            return source(**callbacks)

        def fetch_listing_detail(**callbacks):
            nonlocal source
            if source is None:
                source = self.listing_source(context, owned)
            method = getattr(source, "fetch_detail", None)
            if not callable(method):
                raise ValueError("Ordered listing source has no serial permalink provider")
            return method(**callbacks)

        try:
            context = self.context(owned)
            if deadline_callback:
                deadline_callback(context["target"]["owner_expires_at"])
            if owned["stage"] == Stage.LISTING:
                result = ListingActivity(self.store).execute(
                    **args,
                    fetch=lambda **kw: fetch(self.listing_source, **kw),
                    fetch_detail=fetch_listing_detail,
                )
            elif owned["stage"] == Stage.DETAILS:
                result = self.cached_detail(context, owned)
                if result is None:
                    result = PostActivity(self.store).execute(
                        **args, fetch=lambda **kw: fetch(self.detail_source, **kw)
                    )
            elif owned["stage"] == Stage.COMMENTS:
                if not context["post"]:
                    raise ValueError("Comment parent is missing")
                result = CommentActivity(self.store).execute(
                    **args,
                    post_id=str(context["post"]["id"]),
                    checkpoint=context["target"]["checkpoint"],
                    fetch=lambda **kw: fetch(self.comment_source, **kw),
                )
            elif owned["target_key"].startswith("profile:"):
                from trr_backend.ingestion.profiles import ProfileActivity

                profile_source = self.profile_source
                if profile_source is None:
                    raise ValueError("Compatible profile source is unavailable")
                result = ProfileActivity(self.store).execute(
                    **args,
                    provider=lambda: profile_source(context, owned),
                    storage=lambda: self.media_storage(context, owned),
                )
            else:
                storage = self.media_storage(context, owned)
                try:
                    result = MediaActivity(self.store, storage=storage).execute(
                        **args, load=lambda **kw: self.media_loader(context, owned, **kw)
                    )
                finally:
                    close = getattr(storage.client, "close", None)
                    if close:
                        close()
            if result.get("outcome") == "unresolved" and not result.get("ordered_held"):
                failure = result.get("failure")
                if not isinstance(failure, Failure):
                    self.release(owned, Failure("observation_incomplete"))
                elif failure.code != "reply_access_rejected":
                    self.release(owned, failure)
            source_settled = (
                isinstance(result.get("failure"), Failure) and result["failure"].code == "reply_access_rejected"
            )
            source_settled = source_settled or result.get("outcome", "committed") in {
                Outcome.COMMITTED.value,
                Outcome.CACHED.value,
                Outcome.UNAVAILABLE.value,
            }
            return {"outcome": result.get("outcome", "committed"), "stage": owned["stage"].value}
        except LostOwnership as exc:
            source_error = exc
            return {"outcome": "fenced", "stage": owned["stage"].value}
        except Exception as exc:
            source_error = exc
            failure = classify_failure(exc)
            try:
                self.release(owned, failure, error=exc)
            except LostOwnership:
                return {"outcome": "fenced", "stage": owned["stage"].value}
            return {"outcome": "unresolved", "stage": owned["stage"].value, "code": failure.code}
        finally:
            if source is not None:
                try:
                    method = getattr(source, "finish_success" if source_settled else "finish_failure", None)
                    if callable(method):
                        method() if source_settled else method(source_error)
                except Exception:
                    # A receipt/fence already defines the business outcome.
                    # Never replace it or expose source exception text while
                    # attempting to release a shared identity probe.
                    LOGGER.warning("ingestion_source_cleanup_failed")


def execute_reservation(
    reservation: dict[str, Any], *, deadline_callback: Callable | None = None, execution_placement: str = "modal"
) -> dict[str, Any]:
    from trr_backend.ingestion.composition import build_worker

    worker = build_worker() if execution_placement == "modal" else build_worker(execution_placement=execution_placement)
    return worker.execute(reservation, deadline_callback=deadline_callback)


def _watch_parent(expected_parent_pid: int, stop: threading.Event) -> None:
    # Local activities have private process groups. Driver SIGKILL cannot run its
    # finally block, so this child stops itself without waiting for a DB lease.
    while not stop.is_set():
        if os.getppid() != expected_parent_pid:
            os._exit(125)
        stop.wait(0.1)


def main(*, execution_placement="modal", expected_parent_pid=None) -> int:
    from trr_backend.db import pg
    from trr_backend.ingestion.public_diagnostics import current_public_diagnostic, reset_public_diagnostic
    from trr_backend.ingestion.reply_context import execution_deadline
    from trr_backend.ingestion.runtime import ACTIVITY_DEADLINE_ENV, ACTIVITY_TIMEOUT_SECONDS, activity_reservation

    reset_public_diagnostic()
    timer = None
    deadline_token = None
    hard_deadline = None
    parent_stop = threading.Event()
    if execution_placement not in {"modal", "local"}:
        return 1
    if execution_placement == "local":
        if type(expected_parent_pid) is not int or expected_parent_pid < 2 or os.getppid() != expected_parent_pid:
            return 125
        threading.Thread(
            target=_watch_parent, args=(expected_parent_pid, parent_stop), daemon=True, name="instagram-local-parent"
        ).start()

    def arm(seconds):
        nonlocal timer
        if timer is not None:
            timer.cancel()
        timer = threading.Timer(seconds, lambda: os._exit(124))
        timer.daemon = True
        timer.start()

    def publish(value):
        nonlocal deadline_token, hard_deadline
        hard_deadline = value
        token = execution_deadline.set(value)
        if deadline_token is None:
            deadline_token = token

    def deadline(expires_at):
        remaining = min(ACTIVITY_TIMEOUT_SECONDS, (expires_at - datetime.now(UTC)).total_seconds() - 2)
        if remaining <= 0:
            raise LostOwnership("Reservation has no source execution time remaining")
        now = time.monotonic()
        value = now + remaining
        if hard_deadline is not None:
            value = min(value, hard_deadline)
            remaining = max(0, value - now)
        publish(value)
        # The parent budget already includes process startup and context loading.
        # This owner callback may shorten it, never start another 590 seconds.
        arm(remaining)

    try:
        inherited = os.environ.get(ACTIVITY_DEADLINE_ENV)
        if inherited is not None:
            value = float(inherited)
            remaining = value - time.monotonic()
            if not math.isfinite(value) or not 0 < remaining <= ACTIVITY_TIMEOUT_SECONDS:
                raise ValueError("Invalid inherited activity deadline")
            publish(value)
            arm(remaining)
        raw = sys.stdin.buffer.read(65537)
        if len(raw) > 65536:
            raise ValueError("Reservation exceeds input bound")
        kwargs = {"execution_placement": "local"} if execution_placement == "local" else {}
        result = execute_reservation(activity_reservation(json.loads(raw)), deadline_callback=deadline, **kwargs)
        safe = {key: result[key] for key in ("outcome", "stage", "code") if key in result}
        diagnostic = current_public_diagnostic()
        if diagnostic is not None:
            safe["public_diagnostic"] = diagnostic
        print(json.dumps(safe))
        return 0
    except Exception:
        # No traceback: credential-bearing transport and SQL objects are not
        # allowed across the worker's summary channel.
        print(json.dumps({"outcome": "unresolved", "code": "worker_failed"}))
        return 1
    finally:
        reset_public_diagnostic()
        parent_stop.set()
        if timer:
            timer.cancel()
        if deadline_token is not None:
            execution_deadline.reset(deadline_token)
        pg.close_pool()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-placement", choices=("modal", "local"), default="modal")
    parser.add_argument("--expected-parent-pid", type=int)
    options = parser.parse_args()
    raise SystemExit(
        main(execution_placement=options.execution_placement, expected_parent_pid=options.expected_parent_pid)
    )
