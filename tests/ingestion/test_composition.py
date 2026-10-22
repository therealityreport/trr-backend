"""Composition contracts and atomic canary limits; isolated PostgreSQL only."""

import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import ModuleType, SimpleNamespace
from uuid import uuid4

import pytest

from api.schemas.v2.instagram_ingestion import InstagramIngestionLaunchV2
from tests.ingestion.test_persistence import database as database_fixture
from trr_backend.ingestion import composition
from trr_backend.ingestion.command_service import ServiceUnavailableError
from trr_backend.ingestion.domain import Conflict, Failure, LostOwnership, RunIntent
from trr_backend.ingestion.identity_policy import IdentityDeferred
from trr_backend.ingestion.post_transport import SourceFailure

base_database = database_fixture


@pytest.fixture
def target(monkeypatch):
    for key, value in composition._MODAL_TARGET.items():
        monkeypatch.setenv(key, value)


@pytest.fixture
def policy():
    return composition.CanaryPolicy(frozenset({"bravotv"}), 1, "test-canary-" + str(uuid4()))


def payload():
    return InstagramIngestionLaunchV2(
        backfill_scope="bounded_window", date_start=composition._CANARY_START, date_end=composition._CANARY_END
    )


def sql(tx, statement, args=()):
    with tx() as conn, conn.cursor() as cur:
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def test_global_enable_without_explicit_canary_is_disabled(monkeypatch, target):
    monkeypatch.setenv("TRR_INSTAGRAM_INGESTION_SCHEMA_READY", "true")
    monkeypatch.setenv("TRR_INSTAGRAM_INGESTION_ENABLED", "true")
    monkeypatch.delenv("TRR_INSTAGRAM_INGESTION_CANARY_ACCOUNTS", raising=False)
    service = composition.build_api()
    assert service.capabilities("bravotv").enabled is False
    assert service.capabilities("someone_else").supported_actions == []


def test_canary_config_requires_recorded_account_window_and_bounded_total(monkeypatch):
    values = {
        "TRR_INSTAGRAM_INGESTION_CANARY_ACCOUNTS": "@BravoTV",
        "TRR_INSTAGRAM_INGESTION_CANARY_MAX_RUNS": "1",
        "TRR_INSTAGRAM_INGESTION_CANARY_DATE_START": "2026-01-01T00:00:00Z",
        "TRR_INSTAGRAM_INGESTION_CANARY_DATE_END": "2027-01-01T00:00:00Z",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    assert composition.CanaryPolicy.configured().accounts == frozenset({"bravotv"})
    for key, value in [
        ("TRR_INSTAGRAM_INGESTION_CANARY_MAX_RUNS", "11"),
        ("TRR_INSTAGRAM_INGESTION_CANARY_ACCOUNTS", "bravotv,other"),
        ("TRR_INSTAGRAM_INGESTION_CANARY_DATE_END", "2028-01-01T00:00:00Z"),
    ]:
        with monkeypatch.context() as patch:
            patch.setenv(key, value)
            with pytest.raises(ServiceUnavailableError):
                composition.CanaryPolicy.configured()


def test_atomic_concurrent_api_launch_cannot_bypass_single_run_budget(base_database, policy, target):
    _, tx = base_database
    store = composition.CanaryStore(policy, tx)
    observed = []
    admission = SimpleNamespace(validate_launch=lambda account, request: observed.append(account))
    service = composition.CanaryCommandService(
        store,
        SimpleNamespace(),
        engine="dbos",
        workflow_version=policy.workflow_version,
        enabled=True,
        capacity=composition.CanaryCapacity(policy, admission, tx),
    )

    def launch(_):
        try:
            return service.launch("bravotv", payload(), idempotency_key=str(uuid4()), initiated_by="fixture")
        except Conflict:
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(launch, range(8)))
    accepted = [result for result in results if result is not None]
    assert len(accepted) == 1 and observed
    assert sql(
        tx, "select count(*) from social.instagram_ingestion_runs where workflow_version=%s", (policy.workflow_version,)
    ) == [(1,)]
    assert sql(
        tx,
        """select count(*) from social.instagram_ingestion_outbox o
        join social.instagram_ingestion_runs r using(run_id) where workflow_version=%s""",
        (policy.workflow_version,),
    ) == [(1,)]
    assert service.capabilities("bravotv").supported_actions == ["backfill"]
    assert service.capabilities("other").enabled is False
    with pytest.raises(ServiceUnavailableError):
        service.launch("other", payload(), idempotency_key=str(uuid4()), initiated_by="fixture")


def test_completed_and_cancelled_runs_do_not_reset_total_canary_budget(base_database, target):
    _, tx = base_database
    policy = composition.CanaryPolicy(frozenset({"bravotv"}), 2, "test-canary-" + str(uuid4()))
    store = composition.CanaryStore(policy, tx)
    intent = RunIntent(
        account="bravotv",
        scope="bounded_window",
        date_start=composition._CANARY_START,
        date_end=composition._CANARY_END,
    )

    def start(key):
        return store.start(
            intent, idempotency_key=key, engine="dbos", workflow_version=policy.workflow_version, initiated_by="fixture"
        )

    key = str(uuid4())
    first = start(key)
    assert start(key) == first
    with pytest.raises(Conflict, match="one ingestion"):
        start(str(uuid4()))
    sql(tx, "update social.instagram_ingestion_runs set state='completed' where run_id=%s", (first["run_id"],))
    second = start(str(uuid4()))
    sql(tx, "update social.instagram_ingestion_runs set state='cancelled' where run_id=%s", (second["run_id"],))
    with pytest.raises(Conflict, match="total run budget"):
        start(str(uuid4()))
    assert sql(
        tx, "select count(*) from social.instagram_ingestion_runs where workflow_version=%s", (policy.workflow_version,)
    ) == [(2,)]


def test_scope_and_account_rejection_happen_before_capacity_or_database(policy, target):
    class NoRead:
        def __call__(self):
            pytest.fail("rejected scope queried database")

    validator = composition.CanaryCapacity(
        policy, SimpleNamespace(validate_launch=lambda *_: pytest.fail("admission bypass")), NoRead()
    )
    with pytest.raises(Conflict, match="outside"):
        validator("other", payload())
    with pytest.raises(Conflict, match="calendar-2026"):
        validator("bravotv", InstagramIngestionLaunchV2())


class Gate:
    def __init__(self):
        self.events = []
        self.request_count = 0

    def before_request(self):
        self.events.append("permit")
        self.request_count += 1

    def record_success(self):
        self.events.append("response_success")

    def finish_success(self):
        self.events.append("target_success")

    def finish_failure(self, code=None):
        self.events.append(("target_failure", code))
        return Failure(code, retry_at=datetime.now(UTC) + timedelta(minutes=1)) if code else None


@pytest.mark.parametrize("stage", ["listing", "post_details"])
def test_source_bindings_reuse_explicit_ownership_transaction(monkeypatch, stage):
    from trr_backend.ingestion import post_transport

    checked = []

    def transaction():
        return None

    store = SimpleNamespace(transaction=transaction, check_owner=lambda **kw: checked.append(kw))
    monkeypatch.setattr(composition, "Store", lambda: store)
    owned = {"account": "bravotv", "stage": stage}
    credentials = {"raw_cookies": {"sessionid": "synthetic"}, "browser_account_id": None}
    if stage == "listing":
        context = {"target": {"owner_expires_at": datetime.now(UTC) + timedelta(minutes=5)}}
        source = composition._gate_source(None, context, owned, credentials, failure_code="listing_fetch_failed")
        callback = source.gate.check_cancelled_in_transaction
    else:
        monkeypatch.setattr(composition, "_credentials", lambda _: credentials)
        monkeypatch.setattr(post_transport, "DetailSource", lambda **kw: SimpleNamespace(**kw))
        source = composition._details({"run": {"intent": {"account": "bravotv"}}}, dict(owned, target_key="ABC"))
        callback = source.check_cancelled_in_transaction
        owned = dict(owned, target_key="ABC")
    connection = object()
    callback(connection)
    assert checked == [{**owned, "connection": connection}]


@pytest.mark.parametrize("comments", [False, True])
def test_request_gate_precedes_each_request_and_only_worker_finalizes_success(comments):
    gate = Gate()
    events = gate.events

    def source(**callbacks):
        for _ in range(2):
            callbacks["pre_source_request"]()
            callbacks["source_observer"](*(("comments_api", {}) if comments else ({},)))
        return SimpleNamespace(fetch_failed=False, auth_failed=False)

    wrapped = composition._GatedSource(
        source, gate, failure_code="comment_fetch_failed" if comments else "listing_fetch_failed"
    )
    wrapped(
        pre_source_request=lambda: events.append("count_before_send"),
        source_observer=lambda *args: events.append("retained_before_success"),
    )
    assert events == ["permit", "count_before_send", "retained_before_success", "response_success"] * 2
    wrapped.finish_success()
    wrapped.finish_failure()
    assert events[-1] == "target_success"


@pytest.mark.parametrize("error", [SourceFailure(Failure("rate_limited")), RuntimeError("private transport error")])
def test_source_failure_finalizes_gate_and_propagates_only_typed_shared_failure(error):
    gate = Gate()

    def source(**callbacks):
        callbacks["pre_source_request"]()
        raise error

    wrapped = composition._GatedSource(source, gate, failure_code="listing_fetch_failed")
    with pytest.raises(SourceFailure) as caught:
        wrapped(pre_source_request=lambda: None, source_observer=lambda *args: None)
    expected = "rate_limited" if isinstance(error, SourceFailure) else "request_failed"
    assert caught.value.failure.code == expected
    assert gate.events == ["permit", ("target_failure", expected)]
    wrapped.finish_failure(error)
    assert len(gate.events) == 2


def test_identity_denial_never_reaches_source_count_or_increments_failure_again():
    gate = Gate()

    def deny():
        raise IdentityDeferred("identity_cooldown", datetime.now(UTC) + timedelta(seconds=30))

    gate.before_request = deny

    def source(**callbacks):
        callbacks["pre_source_request"]()
        pytest.fail("request sent after gate denial")

    wrapped = composition._GatedSource(source, gate, failure_code="listing_fetch_failed")
    with pytest.raises(IdentityDeferred):
        wrapped(pre_source_request=lambda: pytest.fail("denied request counted"), source_observer=lambda *args: None)
    assert gate.events == [("target_failure", None)]


def test_lost_owner_cleans_probe_without_marking_identity_failure():
    gate = Gate()
    gate.request_count = 1
    wrapped = composition._GatedSource(None, gate, failure_code="listing_fetch_failed")
    assert wrapped.finish_failure(LostOwnership("cancelled")) is None
    assert gate.events == [("target_failure", None)]


def test_explicit_modal_target_and_positive_call_reconciliation(monkeypatch, target):
    fake = ModuleType("modal")
    fake.FunctionCall = SimpleNamespace(
        from_id=lambda identity: SimpleNamespace(
            get_call_graph=lambda: [SimpleNamespace(function_call_id=identity, children=[])]
        )
    )
    monkeypatch.setitem(sys.modules, "modal", fake)
    assert composition._reconcile({"remote_identity": "fc-synthetic"}) == "accepted"
    fake.FunctionCall = SimpleNamespace(from_id=lambda identity: SimpleNamespace(get_call_graph=lambda: []))
    assert composition._reconcile({"remote_identity": "fc-synthetic"}) == "unknown"
    assert composition._reconcile({"remote_identity": None}) == "unknown"
    monkeypatch.setenv("MODAL_ENVIRONMENT", "other")
    with pytest.raises(ServiceUnavailableError):
        composition._modal_target()
    assert composition._reconcile({"remote_identity": "fc-synthetic"}) == "unknown"


def test_dispatch_uses_recorded_target_existing_lane_names_and_only_safe_fence(monkeypatch, target):
    from trr_backend import modal_dispatch

    calls = []

    def handle(name, **kwargs):
        calls.append((name, kwargs))
        return SimpleNamespace(spawn=lambda **kw: calls.append(kw) or SimpleNamespace(object_id="fc-safe"))

    monkeypatch.setattr(modal_dispatch, "get_trr_modal_function_handle", handle)

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

    @contextmanager
    def transaction():
        yield SimpleNamespace(cursor=lambda **_: Cursor())

    store = SimpleNamespace(transaction=transaction, _execution_run=lambda *_: {"generation": 1, "state": "queued"})
    monkeypatch.setattr(composition, "Store", lambda: store)
    reservation = {
        "run_id": "run",
        "account": "bravotv",
        "generation": 1,
        "stage": "media",
        "target_key": "post",
        "owner_token": "private-owner",
        "cookies": "drop",
    }
    assert composition._dispatch(reservation) == "fc-safe"
    assert calls[0] == (
        modal_dispatch.modal_social_media_job_function_name(),
        {"app_name": "trr-backend-jobs", "environment_name": "main"},
    )
    assert calls[1] == {"ingestion_reservation": {k: v for k, v in reservation.items() if k != "cookies"}}


def test_build_worker_binds_typed_detail_cache_without_provider_creation(monkeypatch):
    from trr_backend.ingestion import freshness, media_snapshot, worker

    captured = {}
    decision = SimpleNamespace(eligible=True)
    monkeypatch.setattr(worker, "Worker", lambda store, **kwargs: captured.update(kwargs) or captured)
    monkeypatch.setattr(
        media_snapshot, "MediaSnapshotLoader", lambda *args, **kwargs: SimpleNamespace(load=lambda **kw: None)
    )
    monkeypatch.setattr(
        freshness,
        "DetailCache",
        lambda read: SimpleNamespace(lookup=lambda *args, **kw: captured.update(cache_call=(args, kw)) or decision),
    )
    monkeypatch.setattr(composition, "_credentials", lambda *_: pytest.fail("cache binding created credentials"))
    composed = composition.build_worker()
    assert composed["detail_cache"]({}, {"run_id": "run", "account": "bravotv", "target_key": "post"}) is decision
    assert captured["cache_call"] == (("run", "bravotv"), {"target_key": "post"})


def test_per_response_rejection_records_shared_cooldown_once_before_aborting():
    gate = Gate()

    def record(code, *, retry_after_seconds=None):
        gate.events.append(("rejected", code, retry_after_seconds))
        return Failure(code, retry_at=datetime.now(UTC) + timedelta(seconds=retry_after_seconds))

    gate.finish_failure = record

    def source(**callbacks):
        callbacks["pre_source_request"]()
        callbacks["source_failure_observer"]("rate_limited", 37.0)
        pytest.fail("internal retry executed")

    wrapped = composition._GatedSource(source, gate, failure_code="listing_fetch_failed")
    with pytest.raises(SourceFailure) as caught:
        wrapped(pre_source_request=lambda: None, source_observer=lambda *args: None)
    assert caught.value.failure.code == "rate_limited" and caught.value.failure.retry_at
    wrapped.finish_failure(caught.value)
    assert gate.events == ["permit", ("rejected", "rate_limited", 37.0)]


def test_immediate_rejection_persists_shared_identity_policy_before_next_invocation(base_database):
    import time
    from pathlib import Path

    from trr_backend.ingestion.identity_policy import SourceRequestGate

    _, transaction = base_database
    sql(
        transaction,
        (
            Path(__file__).parents[2] / "supabase/migrations/20260911121000_instagram_detail_request_lanes.sql"
        ).read_text()
        + (
            Path(__file__).parents[2] / "supabase/migrations/20261022120000_instagram_server_retry_after.sql"
        ).read_text(),
    )
    subject = uuid4().hex

    def new_gate():
        return SourceRequestGate(
            cookies={"ds_user_id": subject, "sessionid": "synthetic"},
            transaction=transaction,
            check_cancelled=lambda: None,
            deadline=time.monotonic() + 300,
        )

    gate = new_gate()

    def source(**callbacks):
        callbacks["pre_source_request"]()
        callbacks["source_failure_observer"]("rate_limited", 120)
        pytest.fail("retry after rate limit")

    wrapped = composition._GatedSource(source, gate, failure_code="comment_fetch_failed")
    started = datetime.now(UTC)
    with pytest.raises(SourceFailure) as caught:
        wrapped(pre_source_request=lambda: None, source_observer=lambda *args: None)
    assert caught.value.failure.retry_at >= started + timedelta(seconds=120)
    wrapped.finish_failure(caught.value)
    with pytest.raises(IdentityDeferred) as deferred:
        new_gate().before_request()
    assert deferred.value.failure.code == "identity_cooldown"
    assert sql(
        transaction,
        """select failure_generation,cardinality(request_times)
        from social.instagram_detail_request_lanes where identity_key=%s""",
        (gate.identity_key,),
    ) == [(1, 1)]


@pytest.fixture
def avatar_media_handoff(monkeypatch):
    """Fake only profile/source composition; retain the read/resolve/read flow."""
    from contextlib import contextmanager

    from trr_backend.ingestion import avatar_resolution, avatar_transport, identity_policy
    from trr_backend.ingestion.media_snapshot import AvatarSourcesMissing

    events = []
    state = SimpleNamespace(depth=0, loads=[], credentials=[], gates=[], sources=[], resolutions=[], fail_at=None)
    result = object()
    owned = {
        "run_id": str(uuid4()),
        "account": "bravotv",
        "generation": 3,
        "target_key": "post-1",
        "owner_token": "synthetic-owner",
        "stage": "media",
    }
    binding = {key: owned[key] for key in ("run_id", "account", "generation", "target_key")}
    callbacks = {"check_cancelled": lambda: None, "on_request": lambda: None}

    @contextmanager
    def transaction():
        state.depth += 1
        events.append("read_open")
        try:
            yield
        finally:
            state.depth -= 1
            events.append("read_closed")

    def check_owner(**kwargs):
        connection = kwargs.pop("connection", None)
        assert state.depth == 0 and kwargs == owned
        if connection is not None:
            state.connection = connection
        events.append("owner_check")

    store = SimpleNamespace(transaction=transaction, check_owner=check_owner)

    def load(**kwargs):
        state.loads.append(kwargs)
        with transaction():
            if len(state.loads) == 1:
                raise AvatarSourcesMissing(["writer", "bravotv", "writer"])
            if state.fail_at == "reload":
                raise RuntimeError("reload_failed")
            return result

    def credentials(account):
        assert state.depth == 0
        state.credentials.append(account)
        events.append("credentials")
        return {"raw_cookies": {"sessionid": "synthetic-cookie"}}

    class Source:
        def __init__(self, **kwargs):
            assert state.depth == 0
            self.kwargs = kwargs
            self.closed = False
            state.sources.append(self)
            events.append("source_created")

        def close(self):
            assert state.depth == 0
            self.closed = True
            events.append("source_closed")

    def gate(**kwargs):
        assert state.depth == 0
        state.gates.append(kwargs)
        events.append("gate_created")
        return SimpleNamespace(**kwargs)

    class Resolution:
        def __init__(self, actual_store, *, gate):
            assert actual_store is store and state.depth == 0
            self.gate = gate

        def execute(self, **kwargs):
            assert state.depth == 0
            assert kwargs == {
                **binding,
                "owner_token": owned["owner_token"],
                "handles": ("bravotv", "writer"),
                "fetch": state.sources[0],
            }
            self.gate.check_cancelled()
            state.resolutions.append(kwargs)
            events.append("resolved")
            if state.fail_at == "resolver":
                raise RuntimeError("resolver_failed")

    monkeypatch.setattr(composition, "_credentials", credentials)
    monkeypatch.setattr(composition.time, "monotonic", lambda: 123.0)
    monkeypatch.setattr(avatar_transport, "AvatarProfileSource", Source)
    monkeypatch.setattr(avatar_resolution, "AvatarResolutionActivity", Resolution)
    monkeypatch.setattr(identity_policy, "SourceRequestGate", gate)
    context = {
        "run": {"intent": {"account": "bravotv"}},
        "target": {"owner_expires_at": datetime.now(UTC) + timedelta(minutes=20)},
    }
    return SimpleNamespace(
        state=state,
        events=events,
        owned=owned,
        binding=binding,
        callbacks=callbacks,
        loader=SimpleNamespace(load=load),
        store=store,
        context=context,
        result=result,
    )


def test_load_media_resolves_missing_avatars_after_read_closes_and_reloads_once(avatar_media_handoff):
    case = avatar_media_handoff
    actual = composition._load_media(case.loader, case.store, case.context, case.owned, **case.callbacks)
    assert actual is case.result
    assert case.state.credentials == ["bravotv"]
    assert case.state.loads == [{**case.binding, **case.callbacks}] * 2
    assert len(case.state.resolutions) == len(case.state.sources) == len(case.state.gates) == 1
    assert case.state.sources[0].closed
    assert case.state.sources[0].kwargs == {"cookies": {"sessionid": "synthetic-cookie"}, "deadline": 713.0}
    gate = case.state.gates[0]
    assert gate["transaction"] is case.store.transaction
    assert gate["cookies"] == {"sessionid": "synthetic-cookie"} and gate["deadline"] == 713.0
    assert case.events == [
        "read_open",
        "read_closed",
        "credentials",
        "gate_created",
        "source_created",
        "owner_check",
        "resolved",
        "source_closed",
        "read_open",
        "read_closed",
    ]
    connection = object()
    gate["check_cancelled_in_transaction"](connection)
    assert case.state.connection is connection


@pytest.mark.parametrize("fail_at,loads", [("resolver", 1), ("reload", 2)])
def test_load_media_closes_profile_source_on_resolution_or_reload_error(avatar_media_handoff, fail_at, loads):
    case = avatar_media_handoff
    case.state.fail_at = fail_at
    with pytest.raises(RuntimeError, match=fail_at + "_failed"):
        composition._load_media(case.loader, case.store, case.context, case.owned, **case.callbacks)
    assert case.state.depth == 0 and case.state.sources[0].closed
    assert len(case.state.loads) == loads and case.state.credentials == ["bravotv"]
    assert case.events.count("source_closed") == 1


def test_load_media_complete_cache_needs_no_profile_credentials_or_source(monkeypatch):
    from trr_backend.ingestion import avatar_resolution, avatar_transport, identity_policy

    def unexpected(*args, **kwargs):
        pytest.fail("complete media cache created profile credentials or source")

    monkeypatch.setattr(composition, "_credentials", unexpected)
    monkeypatch.setattr(avatar_transport, "AvatarProfileSource", unexpected)
    monkeypatch.setattr(avatar_resolution, "AvatarResolutionActivity", unexpected)
    monkeypatch.setattr(identity_policy, "SourceRequestGate", unexpected)
    result = object()
    reads = []
    loader = SimpleNamespace(load=lambda **kwargs: reads.append(kwargs) or result)
    owned = {"run_id": "run", "account": "bravotv", "generation": 1, "target_key": "post", "owner_token": "private"}

    def callback():
        return None

    assert composition._load_media(loader, object(), {}, owned, check_cancelled=callback) is result
    assert reads == [
        {"run_id": "run", "account": "bravotv", "generation": 1, "target_key": "post", "check_cancelled": callback}
    ]


def test_v2_canary_budget_includes_prior_known_versions_and_preserves_held_total(base_database):
    from trr_backend.ingestion.persistence import Store

    store, tx = base_database
    first = Store(tx).start(
        RunIntent(account="bravotv"),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="instagram-ingestion-v1",
        initiated_by="fixture",
    )
    sql(
        tx,
        "update social.instagram_ingestion_runs set state='cancelled',execution_hold_reason='source_blocked' "
        "where run_id=%s",
        (first["run_id"],),
    )
    policy = composition.CanaryPolicy(frozenset({"bravotv"}), 2)
    with tx() as conn, conn.cursor() as cur:
        policy.counts(cur, including_current=False)  # Held v1 consumes total, not active slot.
    second = Store(tx).start(
        RunIntent(account="bravotv"),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="instagram-ingestion-v2",
        initiated_by="fixture",
    )
    with tx() as conn, conn.cursor() as cur:
        policy.counts(cur, including_current=True)
    with pytest.raises(Conflict, match="total run budget"), tx() as conn, conn.cursor() as cur:
        policy.counts(cur, including_current=False)
    # An arbitrary exact-version policy neither includes nor resets these rows.
    exact = composition.CanaryPolicy(frozenset({"bravotv"}), 1, "isolated-policy-" + str(uuid4()))
    with tx() as conn, conn.cursor() as cur:
        exact.counts(cur, including_current=False)
    # Preserve fixture journals while excluding these rows from later family counts.
    sql(
        tx,
        "update social.instagram_ingestion_runs set workflow_version=%s where run_id=any(%s::uuid[])",
        ("completed-fixture-" + str(uuid4()), [first["run_id"], second["run_id"]]),
    )
