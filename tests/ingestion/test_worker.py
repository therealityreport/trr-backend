import io
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests.ingestion.test_persistence import database as database_fixture
from tests.ingestion.test_persistence import launch, target
from trr_backend.ingestion.domain import LostOwnership, RunIntent, Stage
from trr_backend.ingestion.freshness import DetailCacheDecision
from trr_backend.ingestion.worker import Worker

database = database_fixture


def worker(store, source, *, detail_cache=None):
    def unexpected(*a, **k):
        pytest.fail("Unselected stage dependency was used")

    return Worker(
        store,
        listing_source=unexpected,
        detail_source=lambda context, owned: source,
        comment_source=unexpected,
        media_loader=unexpected,
        media_storage=unexpected,
        detail_cache=detail_cache,
    )


def test_failed_source_releases_attempt_with_latest_durable_checkpoint(database):
    store, tx = database
    args = target(store, launch(store))
    args["owner_token"] = str(store.reserve(**args)["owner_token"])

    def source(**callbacks):
        callbacks["on_request"]()
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                """update social.instagram_ingestion_targets set checkpoint='{"cursor":"after-page"}'
                where run_id=%s""",
                (args["run_id"],),
            )
        raise RuntimeError("https://secret.example/?token=must-not-be-returned")

    result = worker(store, source).execute(args)
    assert result == {"outcome": "unresolved", "stage": "post_details", "code": "activity_failed"}
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """select attempts,requests,checkpoint,owner_token,next_attempt_at is not null
            from social.instagram_ingestion_targets where run_id=%s""",
            (args["run_id"],),
        )
        assert cur.fetchone() == (1, 1, {"cursor": "after-page", "last_error_code": "activity_failed"}, None, False)


def test_immediate_operational_retries_preserve_the_finite_attempt_budget(database):
    store, tx = database
    args = target(store, launch(store))
    reservation_args = dict(args)
    calls = []

    def source(**callbacks):
        callbacks["on_request"]()
        calls.append(1)
        raise RuntimeError("synthetic source failure without Retry-After")

    for _ in range(3):
        args["owner_token"] = str(store.reserve(**reservation_args)["owner_token"])
        assert worker(store, source).execute(args)["code"] == "activity_failed"
    assert store.reserve(**reservation_args) is None and len(calls) == 3
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select attempts,max_attempts,requests,next_attempt_at,outcome from social.instagram_ingestion_targets "
            "where run_id=%s and stage='post_details'",
            (args["run_id"],),
        )
        assert cur.fetchone() == (3, 3, 3, None, "unresolved")


def test_cancelled_reservation_never_constructs_source(database):
    store, _ = database
    run = launch(store)
    args = target(store, run)
    args["owner_token"] = str(store.reserve(**args)["owner_token"])
    store.control(run["run_id"], "bravotv", action="cancel", idempotency_key="worker-cancel")
    assert worker(store, lambda **kw: pytest.fail("Cancelled source ran")).execute(args)["outcome"] == "fenced"


def test_duplicate_acquired_delivery_cannot_release_original_owner(database):
    store, tx = database
    args = target(store, launch(store))
    args["owner_token"] = str(store.reserve(**args)["owner_token"])
    store.acquire(**args)
    instance = worker(store, lambda **kw: pytest.fail("Duplicate source ran"))
    instance.detail_source = lambda *args: pytest.fail("Duplicate delivery constructed source")
    assert instance.execute(args)["outcome"] == "fenced"
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select attempts,owner_token::text from social.instagram_ingestion_targets where run_id=%s",
            (args["run_id"],),
        )
        assert cur.fetchone() == (1, args["owner_token"])


def cache_decision(*, eligible=True, reason="complete_detail_snapshot_fresh", reference=None):
    return DetailCacheDecision(
        eligible,
        reason,
        datetime.now(UTC) - timedelta(hours=1),
        {"post_id": str(uuid4()), "catalog_post_id": str(uuid4()), "canonical_post_id": str(uuid4())},
        reference,
    )


def test_fresh_cache_settles_owned_target_without_provider_or_new_evidence(database):
    store, tx = database
    args = target(store, launch(store))
    args["owner_token"] = str(store.reserve(**args)["owner_token"])
    historical = {
        "evidence_id": str(uuid4()),
        "run_id": str(uuid4()),
        "target_key": "post1",
        "captured_at": datetime.now(UTC).isoformat(),
        "stage": "post_details",
    }
    decision = cache_decision(reference=historical)
    calls = []

    def cached(context, owned):
        calls.append(owned["target_key"])
        assert context["target"]["owner_started_at"] is None
        return decision

    def forbidden_factory(context, owned):
        pytest.fail("Cache hit constructed provider, credentials or request gate")

    instance = worker(store, lambda **kw: pytest.fail("Cached target fetched source"), detail_cache=cached)
    instance.detail_source = forbidden_factory
    assert instance.execute(args) == {"outcome": "cached_satisfied", "stage": "post_details"}
    assert calls == ["post1"]
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """select evidence_id,observed_at,canonical_identity
            from social.instagram_ingestion_receipts where run_id=%s""",
            (args["run_id"],),
        )
        evidence_id, observed, identity = cur.fetchone()
        assert evidence_id is None and observed == decision.observed_at
        assert identity == {**decision.canonical_identity, "cache_source": historical}
        cur.execute(
            "select attempts,requests,outcome from social.instagram_ingestion_targets where run_id=%s",
            (args["run_id"],),
        )
        assert cur.fetchone() == (1, 0, "cached_satisfied")
        cur.execute("select count(*) from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],))
        assert cur.fetchone()[0] == 0


def test_cache_without_retained_evidence_records_eligibility_reason(database):
    store, tx = database
    args = target(store, launch(store))
    args["owner_token"] = str(store.reserve(**args)["owner_token"])
    decision = cache_decision()
    assert (
        worker(store, lambda **kw: pytest.fail("Cached source fetched"), detail_cache=lambda *args: decision).execute(
            args
        )["outcome"]
        == "cached_satisfied"
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select canonical_identity from social.instagram_ingestion_receipts where run_id=%s", (args["run_id"],)
        )
        assert cur.fetchone()[0]["cache_source"] == {"reason": decision.reason}


@pytest.mark.parametrize("force", [False, True])
def test_stale_or_forced_cache_takes_normal_source_path(database, force):
    store, _ = database
    run = store.start(
        RunIntent(account="bravotv", force=force),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="test-v1",
        initiated_by="fixture",
    )
    args = target(store, run)
    args["owner_token"] = str(store.reserve(**args)["owner_token"])
    calls = []

    def cache(*_):
        if force:
            pytest.fail("Forced run consulted cache")
        calls.append("cache")
        return cache_decision(eligible=False, reason="metrics_stale")

    def source(**callbacks):
        calls.append("source")
        callbacks["on_request"]()
        raise RuntimeError("fixture normal source failure")

    result = worker(store, source, detail_cache=cache).execute(args)
    assert result["outcome"] == "unresolved"
    assert calls == (["source"] if force else ["cache", "source"])


def test_cache_race_with_cancellation_cannot_settle_receipt(database):
    store, tx = database
    args = target(store, launch(store))
    args["owner_token"] = str(store.reserve(**args)["owner_token"])

    def cache(*_):
        store.control(args["run_id"], args["account"], action="cancel", idempotency_key=str(uuid4()))
        return cache_decision()

    assert (
        worker(store, lambda **kw: pytest.fail("Cancelled cached source ran"), detail_cache=cache).execute(args)[
            "outcome"
        ]
        == "fenced"
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.instagram_ingestion_receipts where run_id=%s", (args["run_id"],))
        assert cur.fetchone()[0] == 0


def test_expired_execution_deadline_never_constructs_source_or_consults_cache(database):
    store, tx = database
    args = target(store, launch(store))
    args["owner_token"] = str(store.reserve(**args)["owner_token"])

    def expired(_):
        raise LostOwnership("fixture expired execution deadline")

    result = worker(
        store,
        lambda **kw: pytest.fail("Expired source ran"),
        detail_cache=lambda *args: pytest.fail("Expired cache consulted"),
    ).execute(args, deadline_callback=expired)
    assert result == {"outcome": "fenced", "stage": "post_details"}
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select attempts,requests from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],)
        )
        assert cur.fetchone() == (0, 0)


@pytest.mark.parametrize("lease_seconds,expected", [(900, 590), (42, 40)])
@pytest.mark.parametrize("execution_error", [False, True])
def test_main_hard_timer_is_bounded_by_lease_and_always_cancelled(
    monkeypatch, capsys, lease_seconds, expected, execution_error
):
    from trr_backend.db import pg
    from trr_backend.ingestion import worker as module

    now = datetime(2026, 10, 16, 12, tzinfo=UTC)
    timers, exits, closed = [], [], []

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    class Timer:
        def __init__(self, seconds, callback):
            self.seconds, self.callback, self.started, self.cancelled = seconds, callback, False, False
            timers.append(self)

        def start(self):
            self.started = True

        def cancel(self):
            self.cancelled = True

    def execute(reservation, *, deadline_callback):
        deadline_callback(now + timedelta(seconds=lease_seconds))
        assert timers[0].started and timers[0].daemon
        timers[0].callback()
        if execution_error:
            raise RuntimeError("private exception must not reach output")
        return {"outcome": "committed", "stage": "post_details", "unsafe_extra": "must-not-print"}

    reservation = {
        "run_id": "run",
        "account": "bravotv",
        "generation": 1,
        "stage": "post_details",
        "target_key": "post1",
        "owner_token": "owner",
    }
    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(module.threading, "Timer", Timer)
    monkeypatch.setattr(module.os, "_exit", lambda code: exits.append(code))
    monkeypatch.setattr(module, "execute_reservation", execute)
    monkeypatch.setattr(module.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(json.dumps(reservation).encode())))
    monkeypatch.setattr(pg, "close_pool", lambda: closed.append(True))
    assert module.main() == (1 if execution_error else 0)
    assert timers[0].seconds == expected and timers[0].cancelled
    assert exits == [124] and closed == [True]
    expected_output = (
        {"outcome": "unresolved", "code": "worker_failed"}
        if execution_error
        else {"outcome": "committed", "stage": "post_details"}
    )
    assert json.loads(capsys.readouterr().out) == expected_output


def test_main_expired_lease_stops_before_source_and_never_starts_timer(monkeypatch, capsys):
    from trr_backend.db import pg
    from trr_backend.ingestion import worker as module

    calls = []

    def execute(reservation, *, deadline_callback):
        deadline_callback(datetime.now(UTC) + timedelta(seconds=1))
        calls.append("source")
        return {"outcome": "committed", "stage": "post_details"}

    reservation = {
        "run_id": "run",
        "account": "bravotv",
        "generation": 1,
        "stage": "post_details",
        "target_key": "post1",
        "owner_token": "owner",
    }
    monkeypatch.setattr(module, "execute_reservation", execute)
    monkeypatch.setattr(module.threading, "Timer", lambda *args: pytest.fail("Expired lease started timer"))
    monkeypatch.setattr(module.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(json.dumps(reservation).encode())))
    monkeypatch.setattr(pg, "close_pool", lambda: None)
    assert module.main() == 1
    assert calls == []
    assert json.loads(capsys.readouterr().out) == {"outcome": "unresolved", "code": "worker_failed"}


@pytest.mark.parametrize("cleanup_error", [False, True])
def test_source_constructed_once_after_acquire_and_finished_only_after_receipt(
    database, monkeypatch, cleanup_error, caplog
):
    from trr_backend.ingestion import worker as module
    from trr_backend.ingestion.application import PostActivity

    store, tx = database
    args = target(store, launch(store))
    args["owner_token"] = str(store.reserve(**args)["owner_token"])
    events = []

    class Source:
        def __call__(self, **callbacks):
            events.append("fetch")
            callbacks["on_request"]()
            return {
                "shortcode": "post1",
                "pk": "1",
                "owner": {"username": "bravotv"},
                "caption": {"text": "Complete source"},
                "like_count": 1,
                "comment_count": 0,
                "display_url": "https://cdn.example/image.jpg",
            }

        def finish_success(self):
            with tx() as conn, conn.cursor() as cur:
                cur.execute(
                    "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (args["run_id"],)
                )
                assert cur.fetchone()[0] == 1
            events.append("finish_success")
            if cleanup_error:
                raise RuntimeError("private-cleanup-token")

        def finish_failure(self, error=None):
            pytest.fail("Committed source received failure cleanup")

    def writer(conn, **payload):
        events.append("canonical")
        with conn.cursor() as cur:
            cur.execute("insert into social.test_canonical values(%s,1)", (args["run_id"],))
        return {"post_id": str(uuid4())}

    class Activity(PostActivity):
        def __init__(self, store):
            super().__init__(store, writer=writer)

        def execute(self, *, fetch, **owned):
            def twice(**callbacks):
                fetch(**callbacks)
                return fetch(**callbacks)

            return super().execute(fetch=twice, **owned)

    def factory(context, owned):
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                "select owner_started_at is not null from social.instagram_ingestion_targets where run_id=%s",
                (args["run_id"],),
            )
            assert cur.fetchone()[0]
        events.append("construct")
        return Source()

    monkeypatch.setattr(module, "PostActivity", Activity)
    instance = worker(store, None)
    instance.detail_source = factory
    result = instance.execute(args)
    assert result == {"outcome": "committed", "stage": "post_details"}
    assert events == ["construct", "fetch", "fetch", "canonical", "finish_success"]
    assert "private-cleanup-token" not in caplog.text


@pytest.mark.parametrize("cleanup_error", [False, True])
def test_source_failure_cleanup_gets_original_error_without_replacing_outcome(database, cleanup_error, caplog):
    store, _ = database
    args = target(store, launch(store))
    args["owner_token"] = str(store.reserve(**args)["owner_token"])
    original = RuntimeError("private-source-token")
    received = []

    class Source:
        def __call__(self, **callbacks):
            callbacks["on_request"]()
            raise original

        def finish_failure(self, error=None):
            received.append(error)
            if cleanup_error:
                raise RuntimeError("private-cleanup-token")

    result = worker(store, Source()).execute(args)
    assert result == {"outcome": "unresolved", "stage": "post_details", "code": "activity_failed"}
    assert received == [original]
    assert "private-source-token" not in caplog.text and "private-cleanup-token" not in caplog.text


def test_unresolved_activity_finishes_failure_after_checkpoint_release(database, monkeypatch):
    from trr_backend.ingestion import worker as module

    store, tx = database
    args = target(store, launch(store))
    args["owner_token"] = str(store.reserve(**args)["owner_token"])
    cleanup = []

    class Source:
        def __call__(self, **callbacks):
            return None

        def finish_failure(self, error=None):
            with tx() as conn, conn.cursor() as cur:
                cur.execute(
                    "select owner_token,next_attempt_at from social.instagram_ingestion_targets where run_id=%s",
                    (args["run_id"],),
                )
                assert cur.fetchone() == (None, None)
            cleanup.append(error)

        def finish_success(self):
            pytest.fail("Unresolved activity certified source health")

    class Activity:
        def __init__(self, store):
            self.store = store

        def execute(self, *, fetch, **owned):
            self.store.acquire(**owned, stage=Stage.DETAILS)
            fetch()
            return {"outcome": "unresolved"}

    monkeypatch.setattr(module, "PostActivity", Activity)
    assert worker(store, Source()).execute(args) == {"outcome": "unresolved", "stage": "post_details"}
    assert cleanup == [None]


def test_fenced_source_cleanup_cannot_erase_ownership_result(database):
    store, _ = database
    args = target(store, launch(store))
    args["owner_token"] = str(store.reserve(**args)["owner_token"])
    original = LostOwnership("fixture ownership lost during source")
    received = []

    class Source:
        def __call__(self, **callbacks):
            raise original

        def finish_failure(self, error=None):
            received.append(error)
            raise RuntimeError("cleanup failure must not replace fence")

    result = worker(store, Source()).execute(args)
    assert result == {"outcome": "fenced", "stage": "post_details"}
    assert received == [original]
