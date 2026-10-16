"""Frozen detail TTLs and bound read-only queries; isolated PostgreSQL only."""

import copy
import json
import socket
import subprocess
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest

from trr_backend.ingestion.canonical import normalize_post_source, write_post
from trr_backend.ingestion.domain import Outcome, RunIntent, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.freshness import DetailCache, detail_eligibility
from trr_backend.ingestion.persistence import Store

NOW = datetime(2026, 10, 16, 12, tzinfo=UTC)
OBSERVED = NOW - timedelta(hours=1)
ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests/fixtures/ingestion"


def inputs():
    proof = {
        "version": 1,
        "coverage_version": 2,
        "source": "instagram_response",
        "observed_at": OBSERVED.isoformat(),
        "caption_present": True,
        "likes_present": True,
        "comments_present": True,
        "media_complete": True,
        "media_count": 1,
    }
    return {
        "post": {
            "id": "post1",
            "shortcode": "Short1",
            "source_account": "bravotv",
            "likes": 0,
            "comments_count": 0,
            "media_urls": ["https://cdn.example/photo.jpg"],
            "raw_data": {"likes": 0, "comments_count": 0, "detail_snapshot": proof},
        },
        "sidecar": None,
        "canonical_identity": {"post_id": "post1", "catalog_post_id": "catalog1", "canonical_post_id": "canonical1"},
        "intent": RunIntent(account="bravotv"),
        "now": NOW,
    }


def test_complete_zero_count_snapshot_qualifies_with_original_observation():
    result = detail_eligibility(**inputs())
    assert result.eligible and result.observed_at == OBSERVED
    assert result.canonical_identity == inputs()["canonical_identity"]
    assert result.evidence_reference is None


@pytest.mark.parametrize("flag", ["caption_present", "likes_present", "comments_present", "media_complete"])
def test_unknown_or_partial_source_coverage_is_never_cached(flag):
    values = inputs()
    values["post"]["raw_data"]["detail_snapshot"][flag] = False
    assert not detail_eligibility(**values).eligible
    values["post"]["raw_data"]["detail_snapshot"].pop(flag)
    assert not detail_eligibility(**values).eligible


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"version": 0}, "detail_coverage_incomplete"),
        ({"version": True}, "detail_coverage_incomplete"),
        ({"coverage_version": 1}, "detail_coverage_incomplete"),
        ({"source": "gallery"}, "detail_coverage_incomplete"),
        ({"observed_at": None}, "source_observation_unknown"),
        ({"observed_at": "invalid"}, "source_observation_unknown"),
        ({"observed_at": (NOW + timedelta(seconds=1)).isoformat()}, "source_observation_in_future"),
        ({"observed_at": (NOW - timedelta(hours=24)).isoformat()}, "metrics_stale"),
        ({"observed_at": (NOW - timedelta(days=30)).isoformat()}, "metadata_stale"),
        ({"media_count": 2}, "media_coverage_incomplete"),
        ({"media_count": 0}, "media_coverage_incomplete"),
    ],
)
def test_version_timestamp_ttl_and_inventory_fail_closed(change, reason):
    values = inputs()
    values["post"]["raw_data"]["detail_snapshot"].update(change)
    result = detail_eligibility(**values)
    assert not result.eligible and result.reason == reason


def test_sidecar_is_authoritative_and_scraped_or_view_clocks_do_not_refresh_coverage():
    values = inputs()
    values["sidecar"] = {"raw_data": {}}
    assert detail_eligibility(**values).reason == "detail_proof_missing"
    values["sidecar"] = None
    values["post"]["raw_data"]["detail_snapshot"]["observed_at"] = (NOW - timedelta(days=2)).isoformat()
    values["post"]["scraped_at"] = NOW
    values["post"]["metadata_scraped_at"] = NOW
    values["post"]["raw_data"]["view_metrics"] = {"observed_at": NOW.isoformat(), "observed_count": 0}
    assert detail_eligibility(**values).reason == "metrics_stale"


@pytest.mark.parametrize("field", ["likes", "comments_count"])
def test_database_default_zero_cannot_replace_unknown_source_metric(field):
    values = inputs()
    values["post"]["raw_data"].pop(field)
    assert detail_eligibility(**values).reason == "source_metrics_unknown"


def test_hidden_and_boolean_metrics_are_unknown():
    values = inputs()
    values["post"]["raw_data"]["likes"] = True
    assert not detail_eligibility(**values).eligible
    values = inputs()
    values["post"]["raw_data"]["likes_hidden"] = True
    assert not detail_eligibility(**values).eligible


def test_force_identity_and_mapping_are_respected():
    values = inputs()
    values["intent"] = RunIntent(account="bravotv", force=True)
    assert detail_eligibility(**values).reason == "forced_refetch"
    values = inputs()
    values["post"]["source_account"] = "other"
    assert detail_eligibility(**values).reason == "post_account_mismatch"
    values = inputs()
    values["canonical_identity"].pop("canonical_post_id")
    assert detail_eligibility(**values).reason == "canonical_mapping_incomplete"


def test_clock_offsets_and_naive_utc_follow_domain_semantics():
    values = inputs()
    values["post"]["raw_data"]["detail_snapshot"]["observed_at"] = OBSERVED.astimezone(
        timezone(timedelta(hours=-4))
    ).isoformat()
    values["now"] = NOW.replace(tzinfo=None)
    assert detail_eligibility(**values).observed_at == OBSERVED
    values["post"]["raw_data"]["detail_snapshot"]["observed_at"] = OBSERVED.replace(tzinfo=None).isoformat()
    assert detail_eligibility(**values).observed_at == OBSERVED


def test_nested_native_metrics_qualify_and_hidden_native_metrics_override_projection():
    values = inputs()
    values["post"]["raw_data"].update(items=[{"like_count": 0, "comment_count": 0}])
    assert detail_eligibility(**values).eligible
    values["post"]["raw_data"]["items"][0]["like_count_hidden"] = True
    assert not detail_eligibility(**values).eligible


@pytest.fixture(scope="module")
def database(tmp_path_factory):
    root = tmp_path_factory.mktemp("freshness")
    binaries = Path("/opt/homebrew/opt/postgresql@17/bin")
    if not (binaries / "initdb").exists():
        pytest.skip("isolated PostgreSQL 17 binaries required")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    data = root / "pg"
    subprocess.run(
        [
            str(binaries / "initdb"),
            "-D",
            str(data),
            "-A",
            "trust",
            "-U",
            "cache_test",
            "--no-locale",
            "--encoding=UTF8",
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            str(binaries / "pg_ctl"),
            "-D",
            str(data),
            "-l",
            str(root / "postgres.log"),
            "-o",
            f"-p {port} -h 127.0.0.1 -k ''",
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
    )

    @contextmanager
    def transaction():
        conn = psycopg2.connect(f"postgresql://cache_test@127.0.0.1:{port}/postgres")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    @contextmanager
    def read_connection():
        with transaction() as conn:
            conn.set_session(readonly=True)
            yield conn

    try:
        with transaction() as conn, conn.cursor() as cur:
            cur.execute((FIXTURES / "canonical_schema.sql").read_text())
            cur.execute("""create role anon;create role authenticated;create role service_role;
                alter table social.scrape_runs add column source_scope text,add column status text,
                add column initiated_by text,add column config jsonb,add column cancelled_at timestamptz;""")
            cur.execute((ROOT / "supabase/migrations/20261016070000_instagram_ingestion_foundation.sql").read_text())
        yield Store(transaction), transaction, DetailCache(read_connection)
    finally:
        subprocess.run(
            [str(binaries / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"], check=True, capture_output=True
        )


def launch(store, shortcode, *, force=False):
    run = store.start(
        RunIntent(account="bravotv", selected_tasks=(Stage.DETAILS,), force=force),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="freshness-test",
        initiated_by="fixture",
    )
    store.discover(
        run["run_id"],
        "bravotv",
        generation=1,
        stage=Stage.DETAILS,
        target_keys=[shortcode],
        max_attempts=3,
        cursor_state={},
        expected_cursor_state={},
        page_id="first",
        seal=True,
    )
    return run


def seed(database):
    store, transaction, cache = database
    shortcode = uuid4().hex
    source_run = launch(store, shortcode)
    target = store.reserve(source_run["run_id"], "bravotv", generation=1, stage=Stage.DETAILS, target_key=shortcode)
    owned = {
        "run_id": source_run["run_id"],
        "account": "bravotv",
        "generation": 1,
        "stage": Stage.DETAILS,
        "target_key": shortcode,
        "owner_token": target["owner_token"],
    }
    store.acquire(**owned)
    payload = copy.deepcopy(json.loads((FIXTURES / "canonical_capture.json").read_text())["post"])
    payload.update(shortcode=shortcode, username="bravotv", likes=0, comments=0)
    payload["owner_detail"]["username"] = "bravotv"
    payload["detail_snapshot"] = inputs()["post"]["raw_data"]["detail_snapshot"]
    payload["detail_snapshot"]["media_count"] = 2
    captured = Evidence.capture(
        source_identity=f"instagram:post:{shortcode}",
        captured_at=OBSERVED,
        parser_version="instagram-ingestion-v1",
        payload=payload,
    )
    evidence_id = store.retain_evidence(**owned, evidence=captured)
    receipt = store.commit_target(
        **owned,
        observed_at=OBSERVED,
        evidence_id=evidence_id,
        write_canonical=lambda conn: write_post(
            conn,
            run_id=source_run["run_id"],
            account="bravotv",
            observed_at=OBSERVED,
            normalized=normalize_post_source(payload, account="bravotv"),
            source_payload=payload,
        ),
    )
    return store, transaction, cache, shortcode, source_run, receipt, evidence_id


def test_read_only_lookup_preserves_source_time_ids_and_validated_historical_evidence(database):
    store, tx, cache, shortcode, source_run, source_receipt, evidence_id = seed(database)
    current = launch(store, shortcode)
    decision = cache.lookup(current["run_id"], "bravotv", target_key=shortcode, now=NOW)
    assert decision.eligible and decision.observed_at == OBSERVED
    assert decision.canonical_identity == {
        key: source_receipt["canonical_identity"][key] for key in ("post_id", "catalog_post_id", "canonical_post_id")
    }
    assert decision.evidence_reference["evidence_id"] == evidence_id
    assert decision.evidence_reference["run_id"] == source_run["run_id"]
    with pytest.raises(LookupError):
        cache.lookup(current["run_id"], "other", target_key=shortcode, now=NOW)
    with pytest.raises(LookupError):
        cache.lookup(current["run_id"], "bravotv", target_key="different", now=NOW)
    forced = launch(store, shortcode, force=True)
    assert cache.lookup(forced["run_id"], "bravotv", target_key=shortcode, now=NOW).reason == "forced_refetch"


def test_corrupt_evidence_is_not_returned_as_valid_reference(database):
    store, tx, cache, shortcode, source_run, receipt, evidence_id = seed(database)
    current = launch(store, shortcode)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_evidence set content_hash='corrupt' where evidence_id=%s", (evidence_id,)
        )
    result = cache.lookup(current["run_id"], "bravotv", target_key=shortcode, now=NOW)
    assert result.eligible and result.evidence_reference is None


def test_cached_target_receipt_uses_original_observation_without_new_source_evidence(database):
    store, tx, cache, shortcode, source_run, original, evidence_id = seed(database)
    current = launch(store, shortcode)
    decision = cache.lookup(current["run_id"], "bravotv", target_key=shortcode, now=NOW)
    target = store.reserve(current["run_id"], "bravotv", generation=1, stage=Stage.DETAILS, target_key=shortcode)
    owned = {
        "run_id": current["run_id"],
        "account": "bravotv",
        "generation": 1,
        "stage": Stage.DETAILS,
        "target_key": shortcode,
        "owner_token": target["owner_token"],
    }
    store.acquire(**owned)
    receipt = store.commit_target(
        **owned,
        observed_at=decision.observed_at,
        outcome=Outcome.CACHED,
        write_canonical=lambda conn: decision.canonical_identity,
    )
    assert receipt["outcome"] == "cached_satisfied" and receipt["observed_at"] == OBSERVED
    assert receipt["evidence_id"] is None
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.instagram_ingestion_evidence where run_id=%s", (current["run_id"],))
        assert cur.fetchone()[0] == 0
        cur.execute("select requests from social.instagram_ingestion_targets where run_id=%s", (current["run_id"],))
        assert cur.fetchone()[0] == 0


def test_missing_canonical_ref_or_cross_account_post_cannot_satisfy_cache(database):
    store, tx, cache, shortcode, source_run, receipt, evidence_id = seed(database)
    current = launch(store, shortcode)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """delete from social.social_post_legacy_refs where platform='instagram'
            and legacy_table='instagram_posts' and legacy_pk=%s""",
            (receipt["canonical_identity"]["post_id"],),
        )
    assert (
        cache.lookup(current["run_id"], "bravotv", target_key=shortcode, now=NOW).reason
        == "canonical_mapping_incomplete"
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_posts set source_account='other' where shortcode=%s", (shortcode,))
    assert cache.lookup(current["run_id"], "bravotv", target_key=shortcode, now=NOW).reason == "post_missing"
