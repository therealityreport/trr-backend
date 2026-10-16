"""One fenced activity invocation; no engine-level retries or credential repair."""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.application import PostActivity
from trr_backend.ingestion.comments import CommentActivity
from trr_backend.ingestion.domain import Failure, LostOwnership, Outcome, RunIntent, Stage
from trr_backend.ingestion.freshness import DetailCacheDecision
from trr_backend.ingestion.listing import ListingActivity
from trr_backend.ingestion.media import MediaActivity
from trr_backend.ingestion.persistence import Store, _row

LOGGER = logging.getLogger(__name__)


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
    return Failure("activity_failed", retry_at=datetime.now(UTC) + timedelta(seconds=60))


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
    ):
        self.store = store
        self.listing_source = listing_source
        self.detail_source = detail_source
        self.comment_source = comment_source
        self.media_loader = media_loader
        self.media_storage = media_storage
        self.detail_cache = detail_cache

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
        if decision.observed_at is None or decision.observed_at.utcoffset() is None:
            raise ValueError("Cache settlement requires its original source observation")
        if any(not decision.canonical_identity.get(key) for key in ("post_id", "catalog_post_id", "canonical_post_id")):
            raise ValueError("Cache settlement requires complete canonical identities")
        self.store.acquire(**owned)
        identity = dict(decision.canonical_identity)
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
            run = Store._run(cur, owned["run_id"], owned["account"])
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

    def release(self, owned, failure):
        # A comment observer may have committed additional pages immediately
        # before the source raised. Reload durable state, never overwrite it with
        # the checkpoint captured at the beginning of this attempt.
        context = self.context(owned)
        if context["target"]["owner_started_at"] is None:
            self.store.acquire(**owned)
        self.store.checkpoint_failure(**owned, failure=failure, checkpoint=context["target"]["checkpoint"], requests=0)

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

        try:
            context = self.context(owned)
            if deadline_callback:
                deadline_callback(context["target"]["owner_expires_at"])
            if owned["stage"] == Stage.LISTING:
                result = ListingActivity(self.store).execute(
                    **args, fetch=lambda **kw: fetch(self.listing_source, **kw)
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
            if result.get("outcome") == "unresolved":
                self.release(
                    owned, Failure("observation_incomplete", retry_at=datetime.now(UTC) + timedelta(seconds=60))
                )
            source_settled = result.get("outcome", "committed") in {
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
                self.release(owned, failure)
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


def execute_reservation(reservation: dict[str, Any], *, deadline_callback: Callable | None = None) -> dict[str, Any]:
    from trr_backend.ingestion.composition import build_worker

    return build_worker().execute(reservation, deadline_callback=deadline_callback)


def main() -> int:
    from trr_backend.db import pg
    from trr_backend.ingestion.runtime import activity_reservation

    timer = None

    def deadline(expires_at):
        nonlocal timer
        remaining = min(590, (expires_at - datetime.now(UTC)).total_seconds() - 2)
        if remaining <= 0:
            raise LostOwnership("Reservation has no source execution time remaining")
        # This entrypoint runs only in the private activity subprocess. The OS
        # closes outstanding requests/connections before another lease begins.
        timer = threading.Timer(remaining, lambda: os._exit(124))
        timer.daemon = True
        timer.start()

    try:
        raw = sys.stdin.buffer.read(65537)
        if len(raw) > 65536:
            raise ValueError("Reservation exceeds input bound")
        result = execute_reservation(activity_reservation(json.loads(raw)), deadline_callback=deadline)
        safe = {key: result[key] for key in ("outcome", "stage", "code") if key in result}
        print(json.dumps(safe))
        return 0
    except Exception:
        # No traceback: credential-bearing transport and SQL objects are not
        # allowed across the worker's summary channel.
        print(json.dumps({"outcome": "unresolved", "code": "worker_failed"}))
        return 1
    finally:
        if timer:
            timer.cancel()
        pg.close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
