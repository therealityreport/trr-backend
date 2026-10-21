"""Operator-selected public validation through production scheduling and activities.

The catalog supplies identity setup only. It never supplies a fresh observation,
listing receipt, or source exhaustion. The private budget is shared with probes.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, RunIntent, SourceProfile, Stage, fingerprint, stable_json
from trr_backend.ingestion.identity_policy import IdentityDeferred
from trr_backend.ingestion.persistence import _row
from trr_backend.ingestion.workflow_store import PARTITION_SIZE
from trr_backend.ingestion.workflows import Scheduler


@dataclass(frozen=True)
class FixedPost:
    shortcode: str
    native_id: str
    catalog_post_id: str
    url: str
    owner_username: str

    def __post_init__(self):
        UUID(self.catalog_post_id)
        parsed = urlsplit(self.url)
        if (
            not re.fullmatch(r"[A-Za-z0-9_-]+", self.shortcode)
            or not re.fullmatch(r"\d+_\d+", self.native_id)
            or not re.fullmatch(r"[a-z0-9._]{1,30}", self.owner_username)
            or parsed.scheme != "https"
            or parsed.netloc != "www.instagram.com"
            or parsed.path not in {f"/p/{self.shortcode}/", f"/reel/{self.shortcode}/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Explicit post identity and public permalink required")


@dataclass(frozen=True)
class FixedManifest:
    source_revision: str
    account: str
    posts: tuple[FixedPost, ...]
    date_start: str = "2026-09-01T00:00:00+00:00"
    date_end: str = "2026-10-01T00:00:00+00:00"
    stages: tuple[str, ...] = (Stage.DETAILS.value, Stage.COMMENTS.value)

    def __post_init__(self):
        if (
            not self.source_revision.strip()
            or not self.posts
            or len(self.posts) > 100
            or len({post.shortcode for post in self.posts}) != len(self.posts)
            or len({post.catalog_post_id for post in self.posts}) != len(self.posts)
            or self.stages != (Stage.DETAILS.value, Stage.COMMENTS.value)
        ):
            raise ValueError("Frozen source revision, unique posts and exact details/comments stages required")
        if self.intent().account != self.account:
            raise ValueError("Manifest account must be normalized")

    def intent(self):
        start, end = (datetime.fromisoformat(value) for value in (self.date_start, self.date_end))
        if start.utcoffset() is None or end.utcoffset() is None or start >= end:
            raise ValueError("Explicit ordered timezone-aware bounds required")
        return RunIntent(
            account=self.account,
            scope="recent_window",
            date_start=start,
            date_end=end,
            selected_tasks=tuple(Stage(stage) for stage in self.stages),
            force=True,
            detail_worker_count=1,
            comments_worker_count=1,
            comments_enable_media_followups=False,
            source_profile=SourceProfile.for_mode("public"),
        )

    def payload(self):
        return json.loads(stable_json(asdict(self)))

    @property
    def digest(self):
        return fingerprint(self.payload())

    @classmethod
    def from_payload(cls, value):
        data = dict(value)
        data["posts"] = tuple(FixedPost(**post) for post in data["posts"])
        if "stages" in data:
            data["stages"] = tuple(data["stages"])
        return cls(**data)


class ValidationPause(IdentityDeferred):
    """An operator budget/interrupt boundary; never native source exhaustion."""


class AccountBudget:
    """Private durable accounting shared by probes and acceptance on one host.

    Hold stream() around all sends in a tranche. Every request, including failed
    sends and document bootstrap, must consume() immediately before the wire.
    A process crash releases flock, but never refunds its persisted requests.
    """

    def __init__(self, path, account, *, clock=time.time):
        self.path = Path(path).expanduser().resolve()
        self.account = RunIntent(account=account).account
        self.clock = clock
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(self.fd)
        os.chmod(self.path, 0o600)
        with self._db() as db:
            db.executescript("""create table if not exists requests(
                id integer primary key,account text not null,tranche text not null,
                sent_at real not null,kind text not null);
                create index if not exists requests_account_time on requests(account,sent_at);
                create table if not exists tranches(
                id text primary key,account text not null,started_at real not null,
                finished_at real,pause_reason text);
            """)
        self.tranche = None

    @contextmanager
    def _db(self):
        connection = sqlite3.connect(self.path, timeout=5)
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    @contextmanager
    def stream(self):
        if self.tranche is not None:
            raise Conflict("Validation stream is already active")
        lock_path = self.path.with_name(self.path.name + "." + self.account + ".lock")
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        acquired = False
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except BlockingIOError:
                raise Conflict("Another account validation stream is active") from None
            self.tranche = uuid4().hex
            with self._db() as db:
                db.execute(
                    "insert into tranches(id,account,started_at) values(?,?,?)",
                    (self.tranche, self.account, self.clock()),
                )
            yield self
        finally:
            if acquired:
                try:
                    if self.tranche is not None:
                        with self._db() as db:
                            db.execute("update tranches set finished_at=? where id=?", (self.clock(), self.tranche))
                finally:
                    self.tranche = None
                    fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def consume(self, kind="document_or_api"):
        if self.tranche is None:
            raise Conflict("A single active account stream is required before source sends")
        now = self.clock()
        reason, retry_at = None, now + 1
        with self._db() as db:
            db.execute("begin immediate")
            started = db.execute("select started_at from tranches where id=?", (self.tranche,)).fetchone()[0]
            if now < started or now - started >= 900:
                reason = "validation_time_budget"
            if reason:
                db.execute("update tranches set pause_reason=? where id=?", (reason, self.tranche))
            else:
                db.execute(
                    "insert into requests(account,tranche,sent_at,kind) values(?,?,?,?)",
                    (self.account, self.tranche, now, kind),
                )
        if reason:
            raise ValidationPause(reason, datetime.fromtimestamp(retry_at, UTC))

    def inspect(self):
        now = self.clock()
        with self._db() as db:
            events = [
                dict(zip(("tranche", "sent_at", "kind"), row, strict=True))
                for row in db.execute(
                    "select tranche,sent_at,kind from requests where account=? and sent_at>? order by id",
                    (self.account, now - 86400),
                )
            ]
            tranches = [
                dict(zip(("id", "started_at", "finished_at", "pause_reason"), row, strict=True))
                for row in db.execute(
                    "select id,started_at,finished_at,pause_reason from tranches where account=? order by started_at",
                    (self.account,),
                )
            ]
        return {
            "account": self.account,
            "rolling_24h_requests": len(events),
            "remaining_24h_requests": None,
            "request_events": events,
            "tranches": tranches,
        }


def _native_id(value):
    return str(value or "").split("_", 1)[0]


def _matches_native(node, expected):
    native = node.get("pk") or node.get("id") or node.get("media_id")
    owner = node.get("user") or node.get("owner") or {}
    owner_id = owner.get("pk") or owner.get("id")
    return (
        _native_id(native) == _native_id(expected)
        and ("_" not in str(native) or str(native) == expected)
        and (not owner_id or str(owner_id) == expected.split("_", 1)[1])
    )


def validate_catalog(conn, manifest):
    """Check existing identity only; no catalog value becomes source evidence."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for post in manifest.posts:
            row = _row(cur, "select * from social.instagram_account_catalog_posts where id=%s", (post.catalog_post_id,))
            raw = (row or {}).get("raw_data") or {}
            owner = (row or {}).get("owner_username") or (raw.get("user") or raw.get("owner") or {}).get("username")
            if (
                not row
                or row["source_id"] != post.shortcode
                or row["source_account"] != manifest.account
                or str(owner or "").lower() != post.owner_username
                or not _matches_native(raw, post.native_id)
            ):
                raise Conflict("Frozen post identity differs from catalog setup")
            canonical = _row(cur, "select * from social.instagram_posts where shortcode=%s", (post.shortcode,))
            if canonical:
                observed_id = canonical.get("media_id") or (canonical.get("raw_data") or {}).get("pk")
                if (observed_id and _native_id(observed_id) != _native_id(post.native_id)) or (
                    canonical.get("username") and canonical["username"].lower() != post.owner_username
                ):
                    raise Conflict("Existing canonical post has a different native identity")


def _require_exclusive_cursor(cur, own_run_id):
    other = _row(
        cur,
        """select 'active_run' as reason from social.instagram_ingestion_runs
            where run_id is distinct from %s::uuid and state in ('queued','running')
            union all
            select 'target_owner' from social.instagram_ingestion_targets
            where run_id is distinct from %s::uuid and outcome='unresolved' and owner_token is not null
                and (owner_expires_at is null or owner_expires_at>clock_timestamp())
            union all
            select 'uncertain_dispatch' from social.instagram_ingestion_dispatches d
            join social.instagram_ingestion_targets t using(run_id,stage,target_key)
            where d.run_id is distinct from %s::uuid and t.outcome='unresolved'
                and d.state in ('reserved','sending','accepted') limit 1""",
        (own_run_id, own_run_id, own_run_id),
    )
    if other:
        raise Conflict("Another Instagram ingestion run, owner or dispatch is active or uncertain")


def require_exclusive_execution(store, own_run_id):
    """Read only: terminal business state does not prove surviving workers drained."""
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        _require_exclusive_cursor(cur, own_run_id)


def _selection_setup(manifest):
    return {
        "kind": "operator_selection_setup",
        "manifest_hash": manifest.digest,
        "manifest": manifest.payload(),
        "source_observation": False,
    }


def start_run(store, manifest, *, workflow_version, idempotency_key):
    if store.execution_placement != "local":
        raise Conflict("Fixed validation requires local execution placement")

    def initialize(conn, run_id):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            _require_exclusive_cursor(cur, run_id)
        validate_catalog(conn, manifest)
        with conn.cursor() as cur:
            cur.execute(
                "update social.instagram_ingestion_runs set execution_placement='local' where run_id=%s", (run_id,)
            )
            setup = _selection_setup(manifest)
            cur.execute(
                """update social.instagram_ingestion_manifests set discovery_complete=true,
                sealed_at=clock_timestamp(),cursor_state=%s::jsonb where run_id=%s""",
                (stable_json(setup), run_id),
            )
            for stage in manifest.stages:
                for post in manifest.posts:
                    cur.execute(
                        """insert into social.instagram_ingestion_targets(run_id,stage,target_key,max_attempts)
                        values(%s,%s,%s,%s)""",
                        (run_id, stage, post.shortcode, 3 if stage == Stage.DETAILS else 12),
                    )

    return store.start(
        manifest.intent(),
        idempotency_key=idempotency_key,
        engine="dbos",
        workflow_version=workflow_version,
        initiated_by="fixed-post-validation",
        request_payload={"manifest": manifest.payload(), "intent": manifest.intent().payload()},
        initialize=initialize,
    )


def inspect_run(store, manifest, run_id):
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        run = store._run(cur, run_id, manifest.account)
        _require_exclusive_cursor(cur, run_id)
        if (
            run["intent_hash"] != manifest.intent().digest
            or run["intent"] != manifest.intent().payload()
            or run.get("execution_placement") != "local"
        ):
            raise Conflict("Frozen run intent or local placement changed")
        cur.execute("select * from social.instagram_ingestion_manifests where run_id=%s order by stage", (run_id,))
        manifests = [dict(row) for row in cur.fetchall()]
        if {row["stage"] for row in manifests} != {Stage.LISTING.value, *manifest.stages} or any(
            not row["discovery_complete"] or row["cursor_state"] != _selection_setup(manifest) for row in manifests
        ):
            raise Conflict("Frozen operator selection manifest changed")
        cur.execute(
            "select * from social.instagram_ingestion_targets where run_id=%s order by stage,target_key", (run_id,)
        )
        targets = [dict(row) for row in cur.fetchall()]
        expected = {(stage, post.shortcode) for stage in manifest.stages for post in manifest.posts}
        if {(row["stage"], row["target_key"]) for row in targets} != expected:
            raise Conflict("Only the frozen exact targets may be scheduled")
        validate_catalog(conn, manifest)
        cur.execute(
            """select t.stage,t.target_key,t.outcome,
                (t.outcome='committed' and t.requests>0 and fresh.receipt_id is not null) as verified,
                fresh.canonical_identity->'settlement' as settlement
                from social.instagram_ingestion_targets t
                join social.instagram_ingestion_runs runs using(run_id)
                left join lateral (
                    select r.receipt_id,r.canonical_identity
                    from social.instagram_ingestion_receipts r
                    join social.instagram_ingestion_evidence e
                        using(evidence_id,run_id,stage,target_key)
                    where t.outcome='committed' and t.requests>0
                        and r.run_id=t.run_id and r.stage=t.stage and r.target_key=t.target_key
                        and r.outcome='committed' and r.owner_generation=t.owner_generation
                        and r.observed_at=t.observed_at and e.captured_at=r.observed_at
                        and e.captured_at>=runs.created_at and e.payload is not null
                        and r.committed_at>=e.captured_at
                    order by r.committed_at desc,r.receipt_id desc limit 1
                ) fresh on true
                where t.run_id=%s order by t.stage,t.target_key""",
            (run_id,),
        )
        retrievals = [dict(row) for row in cur.fetchall()]
        for row in retrievals:
            target = next(
                item for item in targets if item["stage"] == row["stage"] and item["target_key"] == row["target_key"]
            )
            reply_rejected = (target.get("checkpoint") or {}).get("last_error_code") == "reply_access_rejected"
            row["reason"] = (
                "fresh_retrieval_committed"
                if row["verified"]
                else (
                    "reply_access_rejected"
                    if reply_rejected
                    else row["outcome"]
                    if row["outcome"] != "committed"
                    else "missing_fresh_source_receipt"
                )
            )
        passed = run["state"] == "completed" and bool(retrievals) and all(row["verified"] for row in retrievals)
        validation = {
            "scope": "frozen_target_retrieval_only",
            "passed": passed,
            "status": "retrieval_passed" if passed else "retrieval_not_passed",
            "capability_matrix_acceptance": "not_evaluated",
            "targets": retrievals,
        }
        return {
            "run": run,
            "manifests": manifests,
            "targets": targets,
            "manifest_hash": manifest.digest,
            "intent_hash": manifest.intent().digest,
            "validation": validation,
        }


class BoundedSource:
    def __init__(self, source, *, budget, post, comments=False, interrupt_after=None, check_exclusive=None):
        self.source, self.budget, self.post = source, budget, post
        self.comments, self.interrupt_after = comments, interrupt_after
        self.check_exclusive = check_exclusive
        self.pause = None

    def __call__(self, **callbacks):
        key = "pre_source_request" if self.comments else "on_request"
        original_before = callbacks[key]
        original_observe = callbacks["source_observer"]

        def before():
            try:
                if self.check_exclusive is not None:
                    self.check_exclusive()
                self.budget.consume("comments" if self.comments else "details")
                original_before()
            except ValidationPause as error:
                self.pause = error.error_code
                raise

        def observe(kind, payload, *args, **kwargs):
            if not self.comments:
                owner = (payload.get("user") or payload.get("owner") or {}).get("username")
                code = payload.get("code") or payload.get("shortcode")
                if (
                    not _matches_native(payload, self.post.native_id)
                    or code != self.post.shortcode
                    or str(owner or "").lower() != self.post.owner_username
                ):
                    raise Conflict("Fresh native detail identity differs from frozen catalog identity")
            original_observe(kind, payload, *args, **kwargs)
            # Production CommentActivity's observer returns only after _persist_page commits.
            lane = {"graphql_comments": "parent", "graphql_replies": "child"}.get(kind)
            from trr_backend.ingestion.comments import comment_page_failure, native_child_rejection

            successful_page = not comment_page_failure(payload) and not native_child_rejection(payload)
            if self.comments and self.interrupt_after and lane == self.interrupt_after and successful_page:
                self.pause = "validation_interrupted_" + lane
                raise ValidationPause(self.pause, datetime.now(UTC))

        return self.source(**dict(callbacks, **{key: before, "source_observer": observe}))

    def finish_success(self):
        method = getattr(self.source, "finish_success", None)
        return method() if method else None

    def finish_failure(self, error=None):
        method = getattr(self.source, "finish_failure", None)
        return method(error) if method else None


def run_tranche(store, manifest, run_id, *, budget, worker_factory, interrupt_after=None, admit=None):
    """One serial production Scheduler/Worker tranche; never clears durable state."""
    if interrupt_after not in {None, "parent", "child"}:
        raise ValueError("Interrupt boundary must be parent or child")
    status = inspect_run(store, manifest, run_id)
    run = status["run"]
    if run["state"] not in {"queued", "running"} or run.get("execution_hold_reason"):
        raise Conflict("An active unheld generation is required")
    posts = {post.shortcode: post for post in manifest.posts}
    sources, outcomes = [], []
    worker = worker_factory(store)
    if worker.store is not store:
        raise Conflict("Worker must share the production workflow store")
    for attr, comments in (("detail_source", False), ("comment_source", True)):
        original = getattr(worker, attr)

        def factory(context, owned, original=original, comments=comments):
            if owned["target_key"] not in posts:
                raise Conflict("Unlisted exact target rejected")
            source = BoundedSource(
                original(context, owned),
                budget=budget,
                post=posts[owned["target_key"]],
                comments=comments,
                interrupt_after=interrupt_after,
                check_exclusive=lambda: require_exclusive_execution(store, run_id),
            )
            sources.append(source)
            return source

        setattr(worker, attr, factory)
    worker.detail_cache = None
    latest = None

    def dispatch(reservation):
        nonlocal latest
        if reservation["stage"] not in manifest.stages or reservation["target_key"] not in posts:
            raise Conflict("Unlisted exact target rejected")
        latest = reservation
        result = worker.execute(reservation)
        outcomes.append(result)
        return None

    def exclusive_admission(item):
        require_exclusive_execution(store, run_id)
        # Production CLI additionally supplies Admission.admit for legacy/new
        # capacity and database-session checks, retaining local placement.
        return admit(item) if admit is not None else True

    item = store.seed(dict(run, account=manifest.account))
    scheduler = Scheduler(store, admit=exclusive_admission, dispatch=dispatch, reconcile=lambda _: "unknown")
    reason = "bounded"
    with budget.stream():
        try:
            for ordinal in range(PARTITION_SIZE):
                latest = None
                require_exclusive_execution(store, run_id)
                decision = scheduler.tick(item, ordinal)
                if latest is not None and outcomes[-1].get("outcome") in {"unresolved", "fenced"}:
                    store.transition(item, latest["owner_token"], "expired")
                pauses = [source.pause for source in sources if source.pause]
                if pauses:
                    reason = pauses[-1]
                    break
                if decision.get("stop"):
                    store.complete_partition(item, continue_run=False, stop_state=decision["stop"])
                    reason = decision["stop"]
                    break
                if decision.get("sleep"):
                    reason = "cooldown_or_wait"
                    break
        finally:
            # Exclusivity failure must still fence this harness's own work.
            # It never gives authority to cancel any competing run.
            with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                current = store._run(cur, run_id, manifest.account)
            if current["generation"] == run["generation"] and current["state"] in {"queued", "running"}:
                store.control(
                    run_id,
                    manifest.account,
                    action="cancel",
                    expected_generation=run["generation"],
                    expected_placement="local",
                    idempotency_key=f"fixed-stop:{run_id}:{run['generation']}",
                )
    status = inspect_run(store, manifest, run_id)
    return {
        "reason": reason,
        "outcomes": outcomes,
        "state": status,
        "validation": status["validation"],
        "budget": budget.inspect(),
    }
