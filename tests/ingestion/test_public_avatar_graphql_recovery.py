"""GraphQL avatar observations retain the post media fence and exact permits."""

import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import psycopg2
import pytest

from tests.ingestion import test_avatar_resolution as legacy
from tests.ingestion import test_profile_graphql as graphql
from trr_backend.ingestion.avatar_resolution import (
    AvatarResolutionError,
    parse_avatar_evidence,
)
from trr_backend.ingestion.domain import LostOwnership
from trr_backend.ingestion.identity_policy import IdentityDeferred, SourceRequestGate
from trr_backend.ingestion.profile_transport import PROFILE_GRAPHQL_PARSER, PublicGraphQLProfileSource

database = legacy.database
avatars = legacy.avatars


def setup(avatars):
    owned, activity = legacy.owned_target(avatars)
    _, tx = avatars
    gate = SourceRequestGate(
        cookies={},
        public_only=True,
        network_lane="local",
        transaction=tx,
        check_cancelled=lambda: activity.store.check_owner(**owned, stage=legacy.Stage.MEDIA),
        deadline=time.monotonic() + 60,
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute("delete from social.instagram_detail_request_lanes where identity_key=%s", (gate.identity_key,))
    activity.gate = gate
    successes, failures = [], []
    success, failure = gate.record_success, gate.record_failure

    def record_success(permit=None):
        successes.append(permit.request_number)
        return success(permit)

    def record_failure(code, *, permit=None, **kwargs):
        failures.append((code, permit.request_number))
        return failure(code, permit=permit, **kwargs)

    gate.record_success, gate.record_failure = record_success, record_failure
    return owned, activity, successes, failures


class Session(graphql.Session):
    def __init__(self, payload, *, fail=None, after_bootstrap=None):
        super().__init__(payload)
        self.fail, self.after_bootstrap, self.closed = fail, after_bootstrap, False

    def request(self, method, url, **kwargs):
        response = super().request(method, url, **kwargs)
        if method == self.fail:
            response.status_code = 429
            response.headers["retry-after"] = "37"
        if method == "GET" and self.after_bootstrap:
            self.after_bootstrap()
        return response

    def close(self):
        self.closed = True
        super().close()


def native(requested_username, identity="123", **fields):
    return {
        "data": {"user": {"id": identity, "username": requested_username, "profile_pic_url_hd": legacy.URL, **fields}}
    }


def factory(sessions, sources, *, payload=None, fail=None, hook=None):
    def create(username):
        session = Session(payload(username) if payload else native(username), fail=fail, after_bootstrap=hook)
        source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
        sessions.append(session)
        sources.append(source)
        return source

    return create


def test_two_requests_per_handle_original_evidence_replay_and_no_media_settlement(avatars):
    owned, activity, successes, failures = setup(avatars)
    handles = ["a" + uuid4().hex[:10], "b" + uuid4().hex[:10]]
    sessions, sources = [], []
    observations = activity.execute(**owned, handles=handles, source_factory=factory(sessions, sources))
    target, evidence = legacy.rows(avatars, owned)
    assert target["requests"] == activity.gate.request_count == 4
    assert successes == [1, 2, 3, 4] and failures == []
    assert all([call[0] for call in session.calls] == ["GET", "POST"] and session.closed for session in sessions)
    assert len(evidence) == 2 and all(row["parser_version"] == PROFILE_GRAPHQL_PARSER for row in evidence)
    assert target["outcome"] == "unresolved"
    for username, row, observed in zip(handles, evidence, observations, strict=True):
        proof = legacy.retained(avatars, row)
        assert proof.source_identity == "instagram:commenter-profile:profile:id:123"
        assert "sessionid" not in proof.payload_json
        assert "profile_source_provenance" in proof.read(pinned=True)
        parsed = parse_avatar_evidence(proof, username=username, expected_id="123", pinned=True)
        assert parsed.observed_at == observed.observed_at
        assert "sessionid" in observed.source_url  # Fresh URL is ephemeral, never retained.
        activity.apply_evidence(
            **owned, username=username, evidence=proof, evidence_id=str(row["evidence_id"]), expected_id="123"
        )
        assert legacy.registry(avatars, username)[0]["status"] == "pending"
    assert legacy.rows(avatars, owned)[0]["requests"] == 4
    activity.store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))
    with pytest.raises(LostOwnership):
        activity.apply_evidence(
            **owned,
            username=handles[0],
            evidence=legacy.retained(avatars, evidence[0]),
            evidence_id=str(evidence[0]["evidence_id"]),
            expected_id="123",
        )


@pytest.mark.parametrize("phase,requests_count,successes_expected", [("GET", 1, []), ("POST", 2, [1])])
def test_bootstrap_and_post_failures_settle_their_exact_permit(avatars, phase, requests_count, successes_expected):
    owned, activity, successes, failures = setup(avatars)
    sessions, sources = [], []
    started = datetime.now(UTC)
    with pytest.raises(AvatarResolutionError) as caught:
        activity.execute(
            **owned, handles=["c" + uuid4().hex[:10]], source_factory=factory(sessions, sources, fail=phase)
        )
    assert caught.value.failure.retry_at >= started + timedelta(seconds=37)
    assert activity.gate.request_count == requests_count and successes == successes_expected
    assert failures == [("rate_limited", requests_count)]
    assert sessions[0].closed and len(sessions[0].calls) == requests_count
    assert legacy.rows(avatars, owned)[0]["requests"] == requests_count


@pytest.mark.parametrize("mutation", [{"id": "999"}, {"username": "other_handle"}, {"pk": "999"}])
def test_fresh_native_id_handle_and_dual_alias_guards_are_not_weakened(avatars, mutation):
    owned, activity, successes, failures = setup(avatars)
    username = "d" + uuid4().hex[:10]
    sessions, sources = [], []
    with pytest.raises(AvatarResolutionError, match="identity_mismatch"):
        activity.execute(
            **owned,
            handles=[username],
            source_factory=factory(sessions, sources, payload=lambda name: native(name, **mutation)),
        )
    assert legacy.registry(avatars, username) == [] and successes == [1]
    assert failures == [("parse_error", 2)] and sessions[0].closed


@pytest.mark.parametrize("present", [False, True])
def test_unavailable_requires_explicit_native_avatar_field(avatars, present):
    owned, activity, _, _ = setup(avatars)
    username = "e" + uuid4().hex[:10]
    payload = {
        "data": {"user": {"id": "123", "username": username, **({"profile_pic_url_hd": None} if present else {})}}
    }
    sessions, sources = [], []
    if present:
        result = activity.execute(
            **owned, handles=[username], source_factory=factory(sessions, sources, payload=lambda _: payload)
        )
        assert result[0].unavailable is True and legacy.registry(avatars, username)[0]["status"] == "unsupported"
    else:
        with pytest.raises(AvatarResolutionError, match="profile_incomplete"):
            activity.execute(
                **owned, handles=[username], source_factory=factory(sessions, sources, payload=lambda _: payload)
            )
        assert legacy.registry(avatars, username) == []


@pytest.mark.parametrize("failure", ["owner", "persistence", "deadline"])
def test_owner_persistence_and_expected_deadline_never_penalize_source_health(avatars, monkeypatch, failure):
    owned, activity, _, failures = setup(avatars)
    sessions, sources = [], []
    hook = None
    if failure == "owner":

        def hook():
            activity.store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))

    if failure == "persistence":

        def fail_store(**kwargs):
            raise psycopg2.OperationalError("synthetic persistence failure")

        monkeypatch.setattr(activity.store, "retain_evidence", fail_store)
    create = factory(sessions, sources, hook=hook)
    if failure == "deadline":
        original = create

        def create(username):
            result = original(username)
            result.deadline = time.monotonic() - 1
            return result

    expected = {"owner": LostOwnership, "persistence": psycopg2.OperationalError, "deadline": IdentityDeferred}[failure]
    with pytest.raises(expected):
        activity.execute(**owned, handles=["f" + uuid4().hex[:10]], source_factory=create)
    assert failures == [] and sessions[0].closed
    assert len(sessions[0].calls) == {"owner": 1, "persistence": 2, "deadline": 0}[failure]


def test_partial_handle_success_survives_next_handle_failure_and_replay(avatars):
    owned, activity, _, _ = setup(avatars)
    handles = ["g" + uuid4().hex[:10], "h" + uuid4().hex[:10]]
    sessions, sources = [], []
    create = factory(sessions, sources)

    def partial(username):
        source = create(username)
        if username == handles[1]:
            source.deadline = time.monotonic() - 1
        return source

    with pytest.raises(IdentityDeferred):
        activity.execute(**owned, handles=handles, source_factory=partial)
    assert legacy.registry(avatars, handles[0])[0]["status"] == "pending"
    assert legacy.registry(avatars, handles[1]) == [] and all(session.closed for session in sessions)
    _, tx = avatars
    activity.gate = SourceRequestGate(
        cookies={},
        public_only=True,
        network_lane="local",
        transaction=tx,
        check_cancelled=lambda: activity.store.check_owner(**owned, stage=legacy.Stage.MEDIA),
        deadline=time.monotonic() + 60,
    )
    result = activity.execute(**owned, handles=[handles[1]], source_factory=create)
    assert result[0].username == handles[1] and legacy.rows(avatars, owned)[0]["requests"] == 4


def test_indexed_lookup_hint_keeps_native_namespace_pair_and_rejects_conflicting_hints(avatars):
    from trr_backend.ingestion.composition import _public_avatar_lookup_hint

    owned, activity, _, _ = setup(avatars)
    username = "hint" + uuid4().hex[:10]
    _, tx = avatars
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "create table if not exists social.instagram_posts(shortcode text primary key,username text,"
            "owner_username text,owner_user_id text,raw_data jsonb)"
        )
        cur.execute("create index if not exists instagram_posts_username_idx on social.instagram_posts(username)")
        cur.execute(
            "insert into social.instagram_posts(shortcode,username,raw_data) values(%s,%s,%s::jsonb)",
            (uuid4().hex, username, '{"user":{"username":"' + username + '","id":"123","pk":"456"}}'),
        )
    assert _public_avatar_lookup_hint(activity.store, {**owned, "stage": legacy.Stage.MEDIA}, username) == "123"
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select raw_data->'user' from social.instagram_posts where username=%s", (username,))
        assert cur.fetchone()[0] == {"username": username, "id": "123", "pk": "456"}
        cur.execute(
            "insert into social.instagram_posts(shortcode,username,raw_data) values(%s,%s,%s::jsonb)",
            (uuid4().hex, username, '{"user":{"username":"' + username + '","id":"999"}}'),
        )
    assert _public_avatar_lookup_hint(activity.store, owned, username) is None


def test_graphql_third_boundary_cannot_count_or_send_again(avatars):
    owned, activity, _, failures = setup(avatars)
    source = SimpleNamespace(parser_version=PROFILE_GRAPHQL_PARSER, request_budget=2, user_id="123")
    calls = []

    class ExtraSend:
        parser_version, request_budget, user_id = source.parser_version, 2, "123"

        def __call__(self, *, username, pre_source_request):
            for _ in range(3):
                pre_source_request()
                calls.append(True)

        def close(self):
            calls.append("closed")

    with pytest.raises(AvatarResolutionError):
        activity.execute(**owned, handles=["j" + uuid4().hex[:10]], source_factory=lambda _: ExtraSend())
    assert calls == [True, True, "closed"] and activity.gate.request_count == 2
    assert failures == []  # Local admission/budget failures cannot penalize source health.


def test_public_media_factory_routes_known_hint_and_preserves_handle_only_legacy(monkeypatch):
    from trr_backend.ingestion import (
        avatar_resolution,
        avatar_transport,
        composition,
        identity_policy,
        profile_transport,
    )
    from trr_backend.ingestion.media_snapshot import AvatarSourcesMissing

    calls, routed = [], []

    def load(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise AvatarSourcesMissing(["known", "unknown"])
        return "reloaded"

    monkeypatch.setattr(composition, "_source_profile", lambda context: SimpleNamespace(access_mode="public"))
    monkeypatch.setattr(
        composition, "_public_avatar_lookup_hint", lambda store, owned, username: "123" if username == "known" else None
    )
    monkeypatch.setattr(identity_policy, "SourceRequestGate", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(
        profile_transport, "PublicGraphQLProfileSource", lambda **kwargs: SimpleNamespace(kind="graphql", **kwargs)
    )
    monkeypatch.setattr(
        avatar_transport, "AvatarProfileSource", lambda **kwargs: SimpleNamespace(kind="legacy", **kwargs)
    )

    class Activity:
        def __init__(self, store, *, gate):
            pass

        def execute(self, **kwargs):
            assert kwargs["target_key"] == "media-post" and kwargs["handles"] == ("known", "unknown")
            routed.extend(kwargs["source_factory"](username) for username in kwargs["handles"])

    monkeypatch.setattr(avatar_resolution, "AvatarResolutionActivity", Activity)
    owned = {
        "run_id": "run",
        "account": "bravotv",
        "generation": 1,
        "target_key": "media-post",
        "owner_token": "owner",
        "stage": legacy.Stage.MEDIA,
    }
    store = SimpleNamespace(transaction=lambda: None, check_owner=lambda **kwargs: None)
    result = composition._load_media(
        SimpleNamespace(load=load),
        store,
        {"target": {"owner_expires_at": datetime.now(UTC) + timedelta(seconds=60)}},
        owned,
    )
    assert result == "reloaded" and len(calls) == 2
    assert routed[0].kind == "graphql" and routed[0].user_id == "123"
    assert routed[1].kind == "legacy" and routed[1].cookies == {} and routed[1].public_only is True


def test_both_native_requests_run_outside_every_owner_and_gate_transaction(avatars, monkeypatch):
    from contextlib import contextmanager

    owned, activity, _, _ = setup(avatars)
    transaction = activity.store.transaction
    depth = [0]

    @contextmanager
    def tracked():
        with transaction() as conn:
            depth[0] += 1
            try:
                yield conn
            finally:
                depth[0] -= 1

    activity.store.transaction = activity.gate.transaction = tracked
    original = Session.request
    checked = []

    def request(self, *args, **kwargs):
        assert depth[0] == 0
        checked.append(True)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Session, "request", request)
    sessions, sources = [], []
    activity.execute(**owned, handles=["outside" + uuid4().hex[:10]], source_factory=factory(sessions, sources))
    assert checked == [True, True] and depth[0] == 0


def test_two_send_budget_requires_the_reviewed_graphql_parser(avatars):
    owned, activity, _, failures = setup(avatars)
    calls = []

    class Invalid:
        request_budget = 2

        def __call__(self, **kwargs):
            pytest.fail("unreviewed two-send source ran")

        def close(self):
            calls.append("closed")

    with pytest.raises(AvatarResolutionError, match="boundary_missing"):
        activity.execute(**owned, handles=["budget" + uuid4().hex[:10]], source_factory=lambda _: Invalid())
    assert activity.gate.request_count == 0 and failures == [] and calls == ["closed"]


@pytest.mark.parametrize(
    "phase,origin",
    [(1, "owner"), (2, "owner"), (1, "gate"), (2, "gate"), (1, "count"), (2, "count"), (2, "bootstrap_ack")],
)
def test_control_callback_database_failures_keep_origin_and_never_penalize_source(avatars, monkeypatch, phase, origin):
    owned, activity, _, failures = setup(avatars)
    sessions, sources = [], []
    error = psycopg2.OperationalError("synthetic local control failure")
    check_owner = activity.store.check_owner
    admit = activity.gate.before_request
    acknowledge = activity.gate.record_success

    def at_boundary():
        return bool(sessions) and len(sessions[0].calls) == phase - 1

    def check(**kwargs):
        if at_boundary() and (
            (origin == "count" and kwargs.get("record_request"))
            or (origin == "owner" and not kwargs.get("record_request"))
        ):
            raise error
        return check_owner(**kwargs)

    def before():
        if origin == "gate" and at_boundary():
            raise error
        return admit()

    def success(permit=None):
        if origin == "bootstrap_ack" and at_boundary():
            raise error
        return acknowledge(permit)

    monkeypatch.setattr(activity.store, "check_owner", check)
    monkeypatch.setattr(activity.gate, "before_request", before)
    monkeypatch.setattr(activity.gate, "record_success", success)
    with pytest.raises(psycopg2.OperationalError) as caught:
        activity.execute(**owned, handles=["control" + uuid4().hex[:10]], source_factory=factory(sessions, sources))
    assert caught.value is error and failures == []
    assert len(sessions[0].calls) == phase - 1 and sessions[0].closed
