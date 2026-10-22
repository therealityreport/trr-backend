"""New-family native parsing and real isolated PostgreSQL page/date fencing."""

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.ingestion.test_listing import canonical_database as canonical_database_fixture
from tests.ingestion.test_listing import database as listing_database_fixture
from tests.ingestion.test_listing import sql
from trr_backend.ingestion.canonical import write_post
from trr_backend.ingestion.domain import Failure, LostOwnership, RunIntent, SourceProfile, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.listing import ListingActivity, page_target
from trr_backend.ingestion.ordered_listing import (
    ADAPTER_VERSION,
    CONNECTION,
    CONTRACT_VERSION,
    DOC_ID,
    ENDPOINT,
    FAMILY,
    MAX_PAGES,
    OPERATION,
    PAGE_SIZE,
    PARSER_VERSION,
    POLICY,
    WINDOW_POLICY,
    date_proof,
    extract_document,
    parse_page,
    window_coverage,
)
from trr_backend.ingestion.providers import PARSER_VERSION as DETAIL_PARSER_VERSION
from trr_backend.ingestion.providers import InvalidSourceError

canonical_database = canonical_database_fixture
database = listing_database_fixture
ACCOUNT = "fixture_account"
NOW = datetime(2026, 10, 22, 12, tzinfo=UTC)
PROFILE = {"id": "opaque-native-profile", "pk": "123", "username": ACCOUNT, "is_private": False}


def card(number, **extra):
    return {"pk": str(number), "code": f"Ord_{number}", **extra}


def envelope(cards, *, cursor=None, more=True, outgoing="native-next", profile=None):
    user = dict(profile or PROFILE)
    user[CONNECTION] = {
        "edges": [{"node": item} for item in cards],
        "page_info": {"has_next_page": more, "end_cursor": outgoing},
    }
    return {
        "adapter_family": FAMILY,
        "adapter_version": ADAPTER_VERSION,
        "version": CONTRACT_VERSION,
        "listing_profile_username": ACCOUNT,
        "listing_profile_id": user["id"],
        "user": dict(PROFILE),
        "request_cursor": cursor,
        "relay_doc_id": DOC_ID if cursor else None,
        "relay_friendly_name": OPERATION if cursor else None,
        "source_url": ENDPOINT if cursor else f"https://www.instagram.com/{ACCOUNT}/",
        "page_size": PAGE_SIZE,
        "data": {"node" if cursor else "xig_user_by_username": user},
    }


def detail(number, posted, *, owner=ACCOUNT):
    return {
        "pk": str(number),
        "code": f"Ord_{number}",
        "media_type": 1,
        "user": {"pk": "123" if owner == ACCOUNT else "456", "username": owner},
        "taken_at": int(datetime.fromisoformat(posted).timestamp()),
        "caption": {"text": "Native caption"},
        "like_count": 2,
        "comment_count": 0,
        "image_versions2": {"candidates": [{"url": "https://cdn.example/image.jpg", "width": 200, "height": 200}]},
    }


def intent(**changes):
    values = {
        "account": ACCOUNT,
        "source_profile": SourceProfile.for_public_ordered(),
        "scope": "bounded_window",
        "date_start": datetime(2026, 1, 1, tzinfo=UTC),
        "date_end": datetime(2026, 2, 1, tzinfo=UTC),
        "detail_worker_count": 1,
        "comments_worker_count": 1,
        "comments_enable_media_followups": False,
    }
    return RunIntent(**(values | changes))


def proof_for(node, raw, *, captured=NOW):
    page = parse_page(envelope([node]), account=ACCOUNT, cursor=None)
    evidence = Evidence.capture(
        source_identity=f"instagram:post:{node['code']}",
        captured_at=captured,
        parser_version=DETAIL_PARSER_VERSION,
        payload=raw,
    )
    return date_proof(page.cards[0], evidence, {"content_hash": evidence.content_hash}, account=ACCOUNT)


def test_raw_ssr_and_api_connections_survive_evidence_while_secrets_drop():
    for cursor in (None, "request-native-cursor"):
        raw = envelope([card(101)], cursor=cursor)
        raw.update(lsd="must-drop", fb_dtsg="must-drop", headers={"Authorization": "must-drop"})
        user = raw["data"]["node" if cursor else "xig_user_by_username"]
        user.update(viewer_has_liked=True, sessionid="must-drop")
        evidence = Evidence.capture(
            source_identity="fixture", captured_at=NOW, parser_version=PARSER_VERSION, payload=raw
        )
        retained = evidence.read(now=NOW)
        parsed = parse_page(retained, account=ACCOUNT, cursor=cursor)
        assert parsed.cards[0].native_id == "101" and "taken_at" not in parsed.cards[0].node
        assert "must-drop" not in evidence.payload_json and "viewer_has_liked" not in evidence.payload_json
        assert parsed.profile == {"id": "opaque-native-profile", "pk": "123"}


@pytest.mark.parametrize(
    "change", ["profile", "family", "version", "cursor", "duplicate", "page_size", "endpoint", "pins", "metadata_type"]
)
def test_explicit_contract_identity_cursor_and_schema_fail_closed(change):
    raw = envelope([card(101)], cursor="request")
    if change == "profile":
        raw["data"]["node"]["pk"] = "999"
    elif change == "family":
        raw["adapter_family"] = "instagram_profile_graphql_public"
    elif change == "version":
        raw["version"] = 1
    elif change == "cursor":
        raw["data"]["node"][CONNECTION]["page_info"]["end_cursor"] = "request"
    elif change == "duplicate":
        raw["data"]["node"][CONNECTION]["edges"] *= 2
    elif change == "page_size":
        raw["page_size"] = 24
    elif change == "endpoint":
        raw["source_url"] += "/"
    elif change == "pins":
        raw["data"]["node"][CONNECTION]["edges"][0]["node"]["is_pinned"] = "true"
    elif change == "metadata_type":
        raw["user"]["username"] = None
    with pytest.raises(InvalidSourceError):
        parse_page(raw, account=ACCOUNT, cursor="request")


def test_fresh_document_identity_never_equates_opaque_id_to_numeric_pk():
    metadata, grid = extract_document(
        [
            {"data": {"xig_user_by_username": PROFILE}},
            {"__bbox": {"data": {"xig_user_by_username": envelope([card(101)])["data"]["xig_user_by_username"]}}},
        ],
        account=ACCOUNT,
    )
    assert metadata["id"] != metadata["pk"] and grid["id"] == metadata["id"]
    with pytest.raises(InvalidSourceError):
        extract_document(
            [{"data": {"xig_user_by_username": PROFILE}}, {"data": {"xig_user_by_username": dict(PROFILE, pk="999")}}],
            account=ACCOUNT,
        )


def test_detail_date_binding_preserves_actual_owner_without_fabricating_grid_date():
    node = card(101)
    raw = detail(101, "2026-01-10T00:00:00+00:00", owner="collaborator")
    proof = proof_for(node, raw)
    assert proof.posted_at == datetime(2026, 1, 10, tzinfo=UTC)
    assert proof.source["user"]["username"] == "collaborator" and "taken_at" not in node
    with pytest.raises(InvalidSourceError):
        proof_for(card(102), raw)
    with pytest.raises(InvalidSourceError):
        proof_for(card(101, user={"username": ACCOUNT}), raw)


@pytest.mark.parametrize("owner", [None, {}, {"pk": "123"}, {"username": None}, {"username": 123}])
def test_detail_date_requires_native_owner_before_any_request_account_fallback(owner):
    raw = detail(101, "2026-01-10T00:00:00+00:00")
    raw.pop("user")
    if owner is not None:
        raw["user"] = owner
    with pytest.raises(InvalidSourceError, match="native owner"):
        proof_for(card(101, user={"pk": "123", "username": ACCOUNT}), raw)


@pytest.mark.parametrize("wrapper", ["items", "graphql", "node"])
def test_wrapped_native_detail_binds_actual_collaborator_owner_and_same_field_ids(wrapper):
    raw = detail(101, "2026-01-10T00:00:00+00:00", owner="collaborator")

    def wrap(value):
        return (
            {"items": [value]}
            if wrapper == "items"
            else ({"graphql": {"shortcode_media": value}} if wrapper == "graphql" else {"node": value})
        )

    native_card = card(101, user={"pk": "456", "username": "collaborator"})
    proof = proof_for(native_card, wrap(raw))
    assert proof.source == wrap(raw) and proof.posted_at == datetime(2026, 1, 10, tzinfo=UTC)
    for changed_owner in ({"pk": "999", "username": "collaborator"}, {"username": "collaborator"}):
        with pytest.raises(InvalidSourceError, match="native owner identity"):
            proof_for(native_card, wrap(dict(raw, user=changed_owner)))
    with pytest.raises(InvalidSourceError, match="owner differs"):
        proof_for(native_card, wrap(dict(raw, user={"pk": "456", "username": ACCOUNT})))


def test_coverage_requires_january_then_three_complete_older_monotone_pages():
    previous = {}
    samples = [(101, "2026-01-10"), (102, "2025-12-31"), (103, "2025-12-30"), (104, "2025-12-29")]
    for index, (number, day) in enumerate(samples):
        node = card(number)
        page = parse_page(envelope([node]), account=ACCOUNT, cursor=None)
        proof = proof_for(node, detail(number, day + "T00:00:00+00:00"))
        previous = window_coverage(intent(), page, (proof,), previous)
        assert previous["discovery_complete"] == (index == 3)
    assert previous["coverage_basis"] == WINDOW_POLICY
    assert previous["pin_semantics"] == "unknown" and previous["chronology_certified"] is False
    assert previous["provider_exhausted"] is False


@pytest.mark.parametrize("pin,inversion", [(True, False), (False, True)])
def test_known_pin_or_cross_page_inversion_cannot_establish_boundary(pin, inversion):
    previous = {
        "committed_pages": 3,
        "window_membership_observed": True,
        "older_page_streak": 2,
        "older_suffix_last_at": "2025-12-20T00:00:00+00:00",
    }
    node = card(105, **({"is_pinned": True} if pin else {}))
    proof = proof_for(node, detail(105, "2025-12-21T00:00:00+00:00" if inversion else "2025-12-19T00:00:00+00:00"))
    result = window_coverage(intent(), parse_page(envelope([node]), account=ACCOUNT, cursor=None), (proof,), previous)
    assert not result["discovery_complete"] and result["older_page_streak"] == 0


def test_preview_terminal_and_page_budget_remain_unresolved():
    node = card(101)
    proof = proof_for(node, detail(101, "2026-10-01T00:00:00+00:00"))
    page = parse_page(envelope([node], more=False, outgoing=None), account=ACCOUNT, cursor=None)
    result = window_coverage(intent(), page, (proof,), {})
    assert result["coverage_unresolved"] and result["stop_reason"] == "access_scope_boundary"
    assert not result["discovery_complete"]
    page = parse_page(envelope([node]), account=ACCOUNT, cursor=None)
    result = window_coverage(intent(), page, (proof,), {"committed_pages": MAX_PAGES - 1})
    assert result["stop_reason"] == "page_budget_exhausted" and not result["discovery_complete"]


def launch_ordered(database, *, force=False):
    store, _ = database

    def initialize(conn, run_id):
        with conn.cursor() as cur:
            cur.execute(
                "insert into "
                "social.instagram_ingestion_targets(run_id,stage,target_key,max_attempts,checkpoint) "
                "values(%s,'listing','page:root',12,'{\"cursor\":null}')",
                (run_id,),
            )

    result = store.start(
        intent(force=force),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="test-v1",
        initiated_by="fixture",
        initialize=initialize,
    )
    return {"run_id": result["run_id"], "account": ACCOUNT, "generation": 1}


def execute_ordered(
    database, run, payload, details, *, cursor=None, writer=write_post, calls=None, cancel=None, fail=None, clock=NOW
):
    store, _ = database
    key = "page:root" if cursor is None else page_target(cursor)
    reserved = store.reserve(**run, stage=Stage.LISTING, target_key=key)
    assert reserved
    args = dict(run, target_key=key, owner_token=str(reserved["owner_token"]))
    calls = calls if calls is not None else []

    def fetch(**kw):
        kw["pre_source_request"]()
        calls.append("grid")
        kw["source_observer"](payload)
        return payload

    def fetch_detail(**kw):
        code = kw["shortcode"]
        kw["on_request"]()
        calls.append(code)
        if cancel == code:
            store.control(run["run_id"], ACCOUNT, action="cancel", idempotency_key=str(uuid4()))
        if fail == code:
            raise RuntimeError("synthetic date interruption")
        raw = details[code]
        kw["source_observer"]("details", raw)
        return raw

    return ListingActivity(store, writer=writer, clock=lambda: clock).execute(
        **args, fetch=fetch, fetch_detail=fetch_detail
    ), args


def test_pg_only_january_writes_targets_and_duplicate_page_never_refetches(database):
    run = launch_ordered(database)
    cards = [card(1001), card(1002), card(1003)]
    details = {
        f"Ord_{n}": detail(n, day)
        for n, day in (
            (1001, "2026-01-01T00:00:00+00:00"),
            (1002, "2026-02-01T00:00:00+00:00"),
            (1003, "2026-10-01T00:00:00+00:00"),
        )
    }
    written = []

    def writer(conn, **kw):
        written.append((kw["normalized"].shortcode, kw["observed_at"]))
        return write_post(conn, **kw)

    receipt, args = execute_ordered(database, run, envelope(cards), details, writer=writer)
    assert receipt["canonical_identity"]["post_keys"] == ["Ord_1001"]
    assert written == [("Ord_1001", NOW)]
    assert sql(
        database,
        "select target_key from social.instagram_ingestion_targets where run_id=%s and stage='post_details'",
        (run["run_id"],),
    ) == [("Ord_1001",)]
    assert sql(database, "select count(*) from social.instagram_posts where shortcode in ('Ord_1002','Ord_1003')") == [
        (0,)
    ]
    assert sql(
        database,
        "select count(*) from social.instagram_ingestion_evidence where run_id=%s and "
        "source_identity like 'instagram:post:%%'",
        (run["run_id"],),
    ) == [(3,)]

    def forbidden(**kw):
        pytest.fail("duplicate page must not refetch")

    replay = ListingActivity(database[0], clock=lambda: NOW).execute(**args, fetch=forbidden, fetch_detail=forbidden)
    assert replay["receipt_id"] == receipt["receipt_id"]


def test_pg_partial_hydration_survives_release_and_retry_without_cursor_advance(database):
    run = launch_ordered(database)
    payload = envelope([card(1101), card(1102)])
    details = {f"Ord_{n}": detail(n, "2026-01-10T00:00:00+00:00") for n in (1101, 1102)}
    calls = []
    with pytest.raises(RuntimeError):
        execute_ordered(database, run, payload, details, calls=calls, fail="Ord_1102")
    checkpoint = sql(
        database,
        "select checkpoint from social.instagram_ingestion_targets where run_id=%s and target_key='page:root'",
        (run["run_id"],),
    )[0][0]
    assert set(checkpoint["ordered_v2"]["date_refs"]) == {"Ord_1101"}
    assert sql(
        database, "select count(*) from social.instagram_ingestion_discovery_pages where run_id=%s", (run["run_id"],)
    ) == [(0,)]
    assert sql(database, "select count(*) from social.instagram_posts where shortcode in ('Ord_1101','Ord_1102')") == [
        (0,)
    ]
    owned = sql(
        database,
        "select owner_token from social.instagram_ingestion_targets where run_id=%s and target_key='page:root'",
        (run["run_id"],),
    )[0][0]
    database[0].checkpoint_failure(
        **run,
        stage=Stage.LISTING,
        target_key="page:root",
        owner_token=str(owned),
        failure=Failure("synthetic_interruption"),
        checkpoint=checkpoint,
        requests=0,
    )
    receipt, _ = execute_ordered(database, run, payload, details, calls=calls)
    assert calls == ["grid", "Ord_1101", "Ord_1102", "Ord_1102"]
    assert receipt["canonical_identity"]["post_keys"] == ["Ord_1101", "Ord_1102"]


def test_pg_cancellation_during_date_capture_fences_checkpoint_and_canonical_commit(database):
    run = launch_ordered(database)
    with pytest.raises(LostOwnership):
        execute_ordered(
            database,
            run,
            envelope([card(1201)]),
            {"Ord_1201": detail(1201, "2026-01-10T00:00:00+00:00")},
            cancel="Ord_1201",
        )
    assert sql(database, "select count(*) from social.instagram_posts where shortcode='Ord_1201'") == [(0,)]
    assert sql(
        database, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (run["run_id"],)
    ) == [(0,)]


def test_pg_preview_end_holds_unresolved_without_empty_january_completion(database):
    run = launch_ordered(database)
    result, _ = execute_ordered(
        database,
        run,
        envelope([card(1301)], more=False, outgoing=None),
        {"Ord_1301": detail(1301, "2026-10-01T00:00:00+00:00")},
    )
    assert result["ordered_held"] and result["outcome"] == "unresolved"
    assert sql(database, "select state from social.instagram_ingestion_runs where run_id=%s", (run["run_id"],)) == [
        ("blocked",)
    ]
    assert sql(
        database,
        "select blocked_reason,outcome from social.instagram_ingestion_targets where run_id=%s",
        (run["run_id"],),
    ) == [("ordered_access_scope_boundary", "unresolved")]
    assert sql(
        database,
        "select bool_or(discovery_complete) from social.instagram_ingestion_manifests where run_id=%s",
        (run["run_id"],),
    ) == [(False,)]


def test_pg_retained_cache_reuses_original_detail_clock_without_canonical_rewrite(database):
    first = launch_ordered(database)
    raw = {"Ord_1401": detail(1401, "2026-01-10T00:00:00+00:00")}
    execute_ordered(database, first, envelope([card(1401)]), raw)
    before = sql(
        database,
        "select p.scraped_at,s.payload_updated_at,s.raw_data from social.instagram_posts p "
        "join social.instagram_post_payloads s on s.post_id=p.id where p.shortcode='Ord_1401'",
    )
    second = launch_ordered(database)
    calls = []

    def forbidden_writer(*args, **kwargs):
        pytest.fail("cached detail must not be rewritten at fresh membership time")

    receipt, _ = execute_ordered(
        database,
        second,
        envelope([card(1401)]),
        {},
        calls=calls,
        writer=forbidden_writer,
        clock=NOW + timedelta(minutes=10),
    )
    assert calls == ["grid"]
    identity = receipt["canonical_identity"]["posts"][0]
    assert identity["cache_source"]["run_id"] == first["run_id"]
    assert identity["cache_source"]["captured_at"] == NOW.isoformat()
    assert (
        sql(
            database,
            "select p.scraped_at,s.payload_updated_at,s.raw_data from social.instagram_posts p "
            "join social.instagram_post_payloads s on s.post_id=p.id where p.shortcode='Ord_1401'",
        )
        == before
    )
    assert receipt["observed_at"] == NOW + timedelta(minutes=10)


@pytest.mark.parametrize("reason", ["force", "stale", "missing_retained_source"])
def test_pg_date_cache_obeys_force_ttl_and_retained_provenance(database, reason):
    number = {"force": 1501, "stale": 1502, "missing_retained_source": 1503}[reason]
    code = f"Ord_{number}"
    raw = {code: detail(number, "2026-01-10T00:00:00+00:00")}
    first = launch_ordered(database)
    execute_ordered(database, first, envelope([card(number)]), raw)
    if reason == "missing_retained_source":
        sql(
            database,
            "update social.instagram_ingestion_evidence set payload=null where run_id=%s and source_identity=%s",
            (first["run_id"], f"instagram:post:{code}"),
        )
    second = launch_ordered(database, force=reason == "force")
    calls = []
    clock = NOW + (timedelta(hours=25) if reason == "stale" else timedelta(minutes=20))
    execute_ordered(database, second, envelope([card(number)]), raw, calls=calls, clock=clock)
    assert calls == ["grid", code]


def test_pg_canonical_failure_rolls_back_page_but_reuses_retained_hydration(database):
    run = launch_ordered(database)
    raw = {"Ord_1601": detail(1601, "2026-01-10T00:00:00+00:00")}
    payload = envelope([card(1601)])
    calls = []

    def failing_writer(conn, **kwargs):
        write_post(conn, **kwargs)
        raise RuntimeError("synthetic canonical rollback")

    with pytest.raises(RuntimeError):
        execute_ordered(database, run, payload, raw, calls=calls, writer=failing_writer)
    assert sql(database, "select count(*) from social.instagram_posts where shortcode='Ord_1601'") == [(0,)]
    assert sql(
        database, "select count(*) from social.instagram_ingestion_discovery_pages where run_id=%s", (run["run_id"],)
    ) == [(0,)]
    checkpoint, owner = sql(
        database,
        "select checkpoint,owner_token from social.instagram_ingestion_targets where run_id=%s"
        " and target_key='page:root'",
        (run["run_id"],),
    )[0]
    database[0].checkpoint_failure(
        **run,
        stage=Stage.LISTING,
        target_key="page:root",
        owner_token=str(owner),
        failure=Failure("synthetic_rollback"),
        checkpoint=checkpoint,
        requests=0,
    )
    execute_ordered(database, run, payload, raw, calls=calls)
    assert calls == ["grid", "Ord_1601"]


def test_pg_observed_january_and_three_older_pages_seal_declared_heuristic(database):
    run = launch_ordered(database)
    cursor = None
    for index, day in enumerate(("2026-01-10", "2025-12-31", "2025-12-30", "2025-12-29")):
        number = 1701 + index
        outgoing = f"ordered-suffix-{index}"
        receipt, _ = execute_ordered(
            database,
            run,
            envelope([card(number)], cursor=cursor, outgoing=outgoing),
            {f"Ord_{number}": detail(number, day + "T00:00:00+00:00")},
            cursor=cursor,
        )
        cursor = outgoing
    state = receipt["canonical_identity"]["ordered_v2"]
    assert state["coverage_basis"] == WINDOW_POLICY and state["provider_exhausted"] is False
    assert state["committed_pages"] == 4 and state["discovery_complete"]
    assert sql(
        database,
        "select bool_and(discovery_complete) from social.instagram_ingestion_manifests where run_id=%s",
        (run["run_id"],),
    ) == [(True,)]


def test_pg_worker_uses_one_public_source_for_dates_then_original_cache_receipt(database, monkeypatch):
    import time

    from tests.ingestion.test_public_transport import Gate, SequenceSession
    from trr_backend.ingestion import composition
    from trr_backend.ingestion.freshness import DetailCache
    from trr_backend.ingestion.worker import Worker

    run = launch_ordered(database)
    grid = envelope([card(1801), card(1802)])
    html = '<script type="application/json">' + json.dumps(grid) + "</script>"
    raw = [detail(1801, "2026-01-10T00:00:00+00:00"), detail(1802, "2026-10-01T00:00:00+00:00")]
    specs = [{"body": html, "headers": {"content-type": "text/html"}}]
    for item in raw:
        native = {"data": {"xdt_api__v1__media__shortcode__web_info": {"items": [item]}}}
        specs.append(
            {
                "body": '<script type="application/json">' + json.dumps(native) + "</script>",
                "headers": {"content-type": "text/html"},
            }
        )
    events, sessions, gates = [], [], []

    def session_factory():
        session = SequenceSession(events, [specs[len(sessions)]])
        sessions.append(session)
        return session

    def parts(context, owned):
        gate = Gate(events)
        gates.append(gate)
        return {
            "gate": gate,
            "deadline": time.monotonic() + 120,
            "check_cancelled": lambda: database[0].check_owner(**owned),
            "session_factory": session_factory,
        }

    monkeypatch.setattr(composition, "_public_source_parts", parts)

    def forbidden(*args, **kwargs):
        pytest.fail("unexpected source or media work")

    worker = Worker(
        database[0],
        listing_source=composition._listing,
        detail_source=forbidden,
        comment_source=forbidden,
        media_loader=forbidden,
        media_storage=forbidden,
        detail_cache=lambda context, owned: DetailCache(database[1]).lookup(
            owned["run_id"], ACCOUNT, target_key=owned["target_key"]
        ),
    )
    reserved = database[0].reserve(**run, stage=Stage.LISTING, target_key="page:root")
    result = worker.execute(
        dict(run, stage="listing", target_key="page:root", owner_token=str(reserved["owner_token"]))
    )
    assert result == {"outcome": "committed", "stage": "listing"}
    assert len(gates) == 1 and gates[0].request_count == 3 and len(sessions) == 3
    assert all(session.closed for session in sessions)
    assert sql(database, "select count(*) from social.instagram_posts where shortcode='Ord_1802'") == [(0,)]
    reserved = database[0].reserve(**run, stage=Stage.DETAILS, target_key="Ord_1801")
    result = worker.execute(
        dict(run, stage="post_details", target_key="Ord_1801", owner_token=str(reserved["owner_token"]))
    )
    assert result["outcome"] == "cached_satisfied" and len(sessions) == 3
    receipt = sql(
        database,
        "select evidence_id,canonical_identity,observed_at from "
        "social.instagram_ingestion_receipts where run_id=%s and stage='post_details'",
        (run["run_id"],),
    )[0]
    assert receipt[0] is None
    assert receipt[1]["cache_source"]["stage"] == "listing"
    assert receipt[2].astimezone(UTC).isoformat() == receipt[1]["cache_source"]["captured_at"]


def test_pg_native_page_1024_commits_then_fences_further_native_requests(database):
    from psycopg2.extras import execute_values

    from trr_backend.ingestion.domain import stable_json

    run = launch_ordered(database)
    cursor = "last-budget-input"
    key = page_target(cursor)
    previous = {
        "policy": POLICY,
        "committed_pages": MAX_PAGES - 1,
        "native_ids": [],
        "window_membership_observed": False,
    }
    sql(
        database,
        "update social.instagram_ingestion_targets set target_key=%s,checkpoint=%s::jsonb "
        "where run_id=%s and target_key='page:root'",
        (
            key,
            stable_json({"cursor": cursor, "ordered_profile": {"id": PROFILE["id"], "pk": PROFILE["pk"]}}),
            run["run_id"],
        ),
    )
    sql(
        database,
        "update social.instagram_ingestion_manifests set cursor_state=%s::jsonb where run_id=%s and stage='listing'",
        (stable_json({"cursor": cursor, "ordered_v2": previous}), run["run_id"]),
    )
    with database[1]() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            "insert into social.instagram_ingestion_discovery_pages(run_id,stage,page_id,request_hash) values %s",
            [(run["run_id"], "listing", f"native-prior-{i}", str(i)) for i in range(MAX_PAGES - 1)],
        )
    receipt, _ = execute_ordered(
        database,
        run,
        envelope([card(1901)], cursor=cursor, outgoing="beyond-budget-native-cursor"),
        {"Ord_1901": detail(1901, "2026-10-01T00:00:00+00:00")},
        cursor=cursor,
    )
    assert receipt["canonical_identity"]["ordered_v2"]["committed_pages"] == MAX_PAGES
    assert sql(
        database, "select count(*) from social.instagram_ingestion_discovery_pages where run_id=%s", (run["run_id"],)
    ) == [(MAX_PAGES,)]
    assert sql(
        database,
        "select blocked_reason,outcome from social.instagram_ingestion_targets where run_id=%s and target_key=%s",
        (run["run_id"], page_target("beyond-budget-native-cursor")),
    ) == [("ordered_page_budget_exhausted", "unresolved")]
    assert sql(database, "select state from social.instagram_ingestion_runs where run_id=%s", (run["run_id"],)) == [
        ("blocked",)
    ]
    # Even a deliberate local resume cannot send request 1025.
    sql(database, "update social.instagram_ingestion_runs set state='running' where run_id=%s", (run["run_id"],))
    sql(
        database,
        "update social.instagram_ingestion_targets set blocked_reason=null where run_id=%s and target_key=%s",
        (run["run_id"], page_target("beyond-budget-native-cursor")),
    )
    calls = []
    held, _ = execute_ordered(database, run, None, {}, cursor="beyond-budget-native-cursor", calls=calls)
    assert held["ordered_held"] and calls == []
    assert sql(
        database, "select count(*) from social.instagram_ingestion_discovery_pages where run_id=%s", (run["run_id"],)
    ) == [(MAX_PAGES,)]
