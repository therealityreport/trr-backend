"""Short matched v165/current replay: real isolated PG, no native HTTP.

Opt in with TRR_ADMISSION_BENCHMARK=1. Both arms retain the v165 delta writer,
two reply slots, pooled clients, 250ms local/global pacing and identical native
shapes. One pair is intentionally descriptive, not a full-January estimate.
TRR_ADMISSION_BASELINE_REPORT reuses a passing saved v165 arm and runs only current.
"""

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import time
import types
from collections import Counter
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from threading import BoundedSemaphore

import httpx
import psycopg2
import pytest

from tests.ingestion import test_parallel_reply_large_benchmark as large
from tests.ingestion import test_public_comment_transport as native
from trr_backend.ingestion.canonical import _comment_payload
from trr_backend.ingestion.domain import Stage
from trr_backend.ingestion.evidence import Evidence

database = native.database
request_lanes = native.request_lanes
fresh_public_lanes = native.fresh_public_lanes
deny_unmocked_http = native.deny_unmocked_http
BASELINE = "16b35037"
MODULES = (
    "trr_backend.ingestion.persistence",
    "trr_backend.ingestion.comments",
    "trr_backend.ingestion.comment_transport",
    "trr_backend.ingestion.identity_policy",
    "trr_backend.ingestion.composition",
    "trr_backend.socials.instagram.comments_scrapling.reply_scheduler",
    "trr_backend.socials.instagram.comments_scrapling.fetcher",
)


@contextmanager
def source_arm(monkeypatch, baseline):
    """Compile source in private modules, restore canonical imports after arm."""
    modules, hashes = {}, {}
    with monkeypatch.context() as patch:
        names = (
            MODULES
            if baseline
            else (
                *MODULES[:-1],
                "trr_backend.socials.instagram.comments_scrapling.reply_dispatch_turn",
                MODULES[-1],
            )
        )
        for name in names:
            relative = name.replace(".", "/") + ".py"
            source = (
                subprocess.check_output(["git", "show", f"{BASELINE}:{relative}"], cwd=native.existing.ROOT)
                if baseline
                else (native.existing.ROOT / relative).read_bytes()
            )
            hashes[relative] = hashlib.sha256(source).hexdigest()
            private = name.rsplit(".", 1)[0] + "._admission_benchmark_" + name.rsplit(".", 1)[1]
            module = types.ModuleType(private)
            module.__package__ = name.rsplit(".", 1)[0]
            module.__file__ = str(native.existing.ROOT / relative)
            patch.setitem(sys.modules, private, module)
            patch.setitem(sys.modules, name, module)
            exec(compile(source, module.__file__, "exec"), module.__dict__)
            modules[name.rsplit(".", 1)[1]] = module
        patch.setattr(native, "maintained", modules["fetcher"])
        patch.setattr(native, "PublicRelayCommentSource", modules["comment_transport"].PublicRelayCommentSource)
        yield modules, hashes, patch


def fixture_baseline():
    """Immutable native receipts prove skipped work; no invented zero counts."""
    state = {"version": 1, "observed": {}, "lanes": {}, "expected": None}
    events, canonical = [], {}
    observed = native.OBSERVED - timedelta(hours=2)

    def event(payload, parent=None, cursor=None):
        envelope = native.maintained._durable_relay_payload(
            payload,
            cursor=cursor,
            doc_id="36239935742272683" if parent else "25516980651312394",
            friendly_name="PolarisPostCommentsChildrenPaginationtQuery"
            if parent
            else "PolarisPostCommentsPaginationQuery",
            sort_order="popular",
            page_size=12,
            parent=parent,
        )
        evidence = Evidence.capture(
            source_identity="instagram:comments:template:" + ("graphql_replies" if parent else "graphql_comments"),
            captured_at=observed + timedelta(microseconds=len(events)),
            parser_version="instagram-comments-v1",
            payload=envelope,
        )
        page = native.parse_comment_evidence(evidence, pinned=True)
        native._advance(state, page, scan_started_at=observed)
        identities = []
        for comment, raw in zip(page.comments, page.source_payloads, strict=True):
            identities.append(comment.comment_id)
            canonical[comment.comment_id] = _comment_payload(
                comment,
                raw,
                post_id="template",
                account="bravotv",
                observed_at=evidence.captured_at,
                run_id="template",
                parent_id=parent,
                depth=int(parent is not None),
                metadata_writes_enabled=True,
            )
            # Bulk fixture inserts use a union of columns. Match the canonical
            # table default without turning unknown retained native counts into
            # zero proof in evidence/checkpoint state.
            canonical[comment.comment_id].setdefault("child_comment_count", 0)
        events.append((evidence, page, identities))

    rows = [
        native.actual_parent_shape(key, count)
        for key, count in (
            ("unknown-empty-a", None),
            ("unknown-empty-b", None),
            ("unknown-populated", None),
            ("known-zero", 0),
            ("exact-exhausted", None),
            ("leaf-parent", 1),
            ("positive-gap", 2),
            ("gif-parent", None),
        )
    ]
    rows[-1].update(
        text="",
        giphy_media_info={
            "id": "retained-gif",
            "images": {"fixed_height": {"url": "https://media.example/retained.gif"}},
        },
    )
    event(native.page(rows))
    event(native.page([], child=True), "exact-exhausted")
    event(native.page([native.actual_parent_shape("proved-leaf")], child=True), "leaf-parent")
    event(
        native.page([native.actual_parent_shape("gap-old", 0)], child=True, more=True, after="saved-tail"),
        "positive-gap",
    )
    assert state["observed"]["proved-leaf"]["native_reply_leaf"] is True
    assert state["lanes"]["positive-gap"]["cursors"] == ["saved-tail"]
    return {"state": state, "events": events, "canonical": canonical}


class SQLMeter:
    """Count executed driver statements and transaction scopes, not SQL bytes."""

    def __init__(self, transaction):
        self.original = transaction
        self.transactions = self.statements = 0
        self.autocommit_transactions = 0
        self.transaction_seconds = self.sql_seconds = 0.0
        self.kinds = Counter()

    @contextmanager
    def transaction(self):
        started = time.monotonic()
        self.transactions += 1
        with self.original() as connection:
            yield ConnectionMeter(connection, self)
        self.transaction_seconds += time.monotonic() - started

    def counts(self):
        return {"transactions": self.transactions, "sql_round_trips": self.statements}


class ConnectionMeter:
    def __init__(self, connection, meter):
        self.raw, self.meter = connection, meter

    def __getattr__(self, name):
        return getattr(self.raw, name)

    def cursor(self, *args, **kwargs):
        return CursorMeter(self.raw.cursor(*args, **kwargs), self)


class CursorMeter:
    def __init__(self, cursor, connection):
        self.raw, self.connection = cursor, connection

    def __getattr__(self, name):
        return getattr(self.raw, name)

    def __enter__(self):
        self.raw.__enter__()
        return self

    def __exit__(self, *args):
        return self.raw.__exit__(*args)

    def execute(self, query, params=None):
        meter = self.connection.meter
        meter.statements += 1
        if self.connection.autocommit:
            meter.autocommit_transactions += 1
        text = query.decode() if isinstance(query, bytes) else str(query)
        meter.kinds[text.strip().split()[0].lower()] += 1
        started = time.monotonic()
        try:
            # Identical declared remote-roundtrip model in both arms; the real
            # PostgreSQL statement, locks and commit still execute afterward.
            time.sleep(0.01)
            return self.raw.execute(query, params)
        finally:
            meter.sql_seconds += time.monotonic() - started


class CoordinationMeter(SQLMeter):
    """One control checkout; every autocommit SQL has its own transaction."""

    def __init__(self, transaction, dsn):
        super().__init__(transaction)
        self.dsn = dsn
        self.slot = BoundedSemaphore(1)
        self.labels = Counter()
        self.checkout_wait_seconds = self.checkout_lifetime_seconds = 0.0

    @contextmanager
    def connection(self, *, label, pool_name, autocommit=False):
        assert pool_name == "session_control"
        queued = time.monotonic()
        with self.slot:
            entered = time.monotonic()
            self.checkout_wait_seconds += entered - queued
            self.labels[label] += 1
            try:
                if autocommit:
                    conn = psycopg2.connect(self.dsn)
                    conn.autocommit = True
                    try:
                        yield ConnectionMeter(conn, self)
                        assert conn.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_IDLE
                    finally:
                        conn.close()
                else:
                    with self.transaction() as conn:
                        yield conn
            finally:
                self.checkout_lifetime_seconds += time.monotonic() - entered

    def report(self):
        return {
            "explicit_transaction_contexts": self.transactions,
            "autocommit_statement_transactions": self.autocommit_transactions,
            "database_transactions": self.transactions + self.autocommit_transactions,
            "sql_round_trips": self.statements,
            "sql_seconds": self.sql_seconds,
            "explicit_transaction_context_seconds": self.transaction_seconds,
            "checkout_wait_seconds": self.checkout_wait_seconds,
            "checkout_lifetime_seconds": self.checkout_lifetime_seconds,
            "checkout_limit": 1,
            "checkout_labels": dict(self.labels),
            "sql_kinds": dict(self.kinds),
        }


def run_arm(database, baseline, monkeypatch, pace_root, old, *, mode="file_lock"):
    # Common seed is excluded from timing/counters. No growing setup traversal.
    with monkeypatch.context() as seed_patch:
        seed_patch.setattr(large, "EXPECTED", 13)
        _, raw_tx, args, initial, _ = large.seed(database, baseline)
    with raw_tx() as conn, conn.cursor() as cur:
        cur.execute("delete from social.instagram_detail_request_lanes")
        if mode == "advisory":
            cur.execute("""create table if not exists social.ig_comment_rate_pace(
                rate_key text primary key,last_start timestamptz not null default now(),
                cooldown_until timestamptz,server_retry_after_until timestamptz)""")
            cur.execute("delete from social.ig_comment_rate_pace")
        dsn = conn.dsn
    meter = SQLMeter(raw_tx)
    coordination = CoordinationMeter(raw_tx, dsn) if mode == "advisory" else None
    before = large.inventory(raw_tx, args["post_id"])
    with source_arm(monkeypatch, old) as (modules, hashes, patch):
        store = modules["persistence"].Store(meter.transaction)
        patch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", "2")
        for name, value in {
            "TARGET": args["target_key"],
            "GENERATION": "1",
            "RUN_ID": args["run_id"],
            "RUN_GENERATION": "1",
        }.items():
            patch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CANARY_" + name, value)
        pace_root.mkdir()
        fetcher = modules["fetcher"]
        if coordination is not None:
            from trr_backend.db import pg

            patch.setattr(pg, "db_connection", coordination.connection)
            patch.setattr(pg, "db_read_connection", lambda **kwargs: coordination.connection(**kwargs, autocommit=True))
        patch.setattr(fetcher, "_global_rate_limit_path", lambda key: str(pace_root / "pace"))
        patch.setattr(fetcher, "_global_rate_cooldown_path", lambda key: str(pace_root / "cooldown"))
        calls, starts, clients = [], [], []
        admission = {"seconds": 0.0, "transactions": 0, "sql_round_trips": 0, "calls": 0}

        async def handler(request):
            assert request.url.host == "www.instagram.com"
            starts.append(time.monotonic())
            if request.method == "GET":
                calls.append(("bootstrap", None, None))
                return native.bootstrap()
            _, variables = native.body(request)
            parent, cursor = variables.get("parent_comment_id"), variables.get("after")
            calls.append((parent, cursor, variables["first"]))
            assert parent in {"unknown-empty-a", "unknown-empty-b", "unknown-populated", "positive-gap", "gif-parent"}
            assert variables["first"] == 12
            assert cursor == ("saved-tail" if parent == "positive-gap" else None)
            await asyncio.sleep(0.42 if parent == "unknown-empty-a" else 0.32)
            rows = []
            if parent == "unknown-populated":
                rows = [native.node("new-sticker", sticker={"image_url": "https://media.example/new.webp"})]
            elif parent == "positive-gap":
                rows = [
                    native.node("gap-new", comment_media={"video_versions": [{"url": "https://media.example/new.mp4"}]})
                ]
            elif parent == "gif-parent":
                rows = [native.node("gif-reply")]
            return httpx.Response(200, json=native.page(rows, child=True))

        fetch, created = native.source(patch, handler, expected=13)
        base_factory = fetch._factory

        def factory(**kwargs):
            result = base_factory(**kwargs)
            result._api_delay_seconds = result._global_api_delay_seconds = 0.25
            result._global_rate_limit_mode_configured = mode
            return result

        fetch._factory = factory
        # Count client instances, preserving the existing persistent mock factory.
        client_factory = fetcher.httpx.AsyncClient

        def client(**kwargs):
            result = client_factory(**kwargs)
            clients.append(result)
            return result

        patch.setattr(fetcher.httpx, "AsyncClient", client)
        owned = {key: value for key, value in args.items() if key != "post_id"}
        gate = modules["identity_policy"].SourceRequestGate(
            cookies={},
            public_only=True,
            network_lane="local",
            transaction=meter.transaction,
            check_cancelled=lambda: store.check_owner(**owned, stage=Stage.COMMENTS),
            check_cancelled_in_transaction=lambda conn: store.check_owner(
                **owned, stage=Stage.COMMENTS, connection=conn
            ),
            deadline=time.monotonic() + 20,
        )

        def measured_source(**callbacks):
            admit = callbacks["pre_source_request"]

            def timed_admit():
                counts, started = meter.counts(), time.monotonic()
                try:
                    return admit()
                finally:
                    admission["seconds"] += time.monotonic() - started
                    admission["calls"] += 1
                    for key in counts:
                        admission[key] += meter.counts()[key] - counts[key]

            timed_admit.__dict__.update(admit.__dict__)
            return fetch(**{**callbacks, "pre_source_request": timed_admit})

        gated = modules["composition"]._GatedSource(measured_source, gate, failure_code="comment_fetch_failed")
        started = time.monotonic()
        try:
            result = modules["comments"].CommentActivity(store, gated, clock=lambda: native.OBSERVED).execute(**args)
        finally:
            gated.finish_failure()
        elapsed = time.monotonic() - started
        measured = meter.counts()
        scheduler = created[0].runtime_metadata["reply_scheduler"]
        assert scheduler["max_outstanding"] <= 2 and scheduler["max_outstanding"] == 2
        assert gate.request_count == len(calls) == 6
        assert len(clients) <= 3 and all(item.is_closed for item in clients)
        assert result["complete"] is True
        if coordination is not None:
            assert coordination.statements > 0, "Advisory coordination bypassed isolated PostgreSQL"
            assert not (pace_root / "pace").exists(), "Advisory workload fell back to file pacing"
            assert created[0]._global_rate_limit_advisory_fallback_count == 0
        assert sorted(calls, key=str) == sorted(
            [
                ("bootstrap", None, None),
                ("unknown-empty-a", None, 12),
                ("unknown-empty-b", None, 12),
                ("unknown-populated", None, 12),
                ("positive-gap", "saved-tail", 12),
                ("gif-parent", None, 12),
            ],
            key=str,
        )
        # Global pacing starts at reservation time in v165. Report actual send
        # gaps without claiming a policy guarantee stronger than that baseline.
        send_gaps = [right - left for left, right in zip(starts[1:], starts[2:], strict=False)]
    after = large.inventory(raw_tx, args["post_id"])
    assert all(after[key] == value for key, value in before.items())
    assert set(after) - set(before) == {"new-sticker", "gap-new", "gif-reply"}
    current = result["checkpoint"]["comment_pages"]
    assert current["lanes"]["__top__"] == initial["lanes"]["__top__"]
    assert current["lanes"]["exact-exhausted"] == initial["lanes"]["exact-exhausted"]
    assert current["observed"]["known-zero"]["expected_replies"] == 0
    assert current["observed"]["unknown-empty-a"]["expected_replies"] is None
    assert current["observed"]["proved-leaf"]["native_reply_leaf"] is True
    target = native.existing.query(
        raw_tx, "select requests from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],)
    )[0]
    assert target["requests"] == len(calls)
    receipts = native.existing.query(
        raw_tx,
        """select p.observed_at,p.content_hash,p.parent_comment_id,
        p.next_cursor,p.exhausted,e.captured_at,e.content_hash as evidence_hash,
        e.parser_version,e.schema_version,e.payload,
        p.run_id=e.run_id as same_run,p.stage=e.stage as same_stage,p.target_key=e.target_key as same_target
        from social.instagram_ingestion_comment_pages p join social.instagram_ingestion_evidence e
        on p.evidence_id=e.evidence_id where p.run_id=%s and e.captured_at=%s""",
        (args["run_id"], native.OBSERVED),
    )
    assert len(receipts) == 5
    for row in receipts:
        assert row.pop("same_run") and row.pop("same_stage") and row.pop("same_target")
        assert row["observed_at"] == row["captured_at"]
        assert row["content_hash"] == row["evidence_hash"]
        row["payload"] = json.loads(row["payload"])
        assert large.digest(row["payload"]) == row["content_hash"]
        row["observed_at"], row["captured_at"] = row["observed_at"].isoformat(), row["captured_at"].isoformat()
    receipts.sort(key=lambda row: str(row["parent_comment_id"]))
    proof = {
        "canonical_sha256": large.digest(after),
        "observed_sha256": large.digest(current["observed"]),
        "lanes_sha256": large.digest(current["lanes"]),
        "receipt_evidence_sha256": large.digest(receipts),
        "complete": result["complete"],
        "expected": current["expected"],
        "saved_ids": sorted(after),
        "calls": sorted(calls, key=str),
    }
    metrics = {
        "source_hashes": hashes,
        "elapsed_seconds": elapsed,
        **measured,
        "transactions_per_send": measured["transactions"] / len(calls),
        "sql_round_trips_per_send": measured["sql_round_trips"] / len(calls),
        "transactions_per_page": measured["transactions"] / len(receipts),
        "sql_round_trips_per_page": measured["sql_round_trips"] / len(receipts),
        "admission": admission,
        "scheduler": scheduler,
        "sql_seconds": meter.sql_seconds,
        "transaction_seconds": meter.transaction_seconds,
        "source_sends": len(calls),
        "retained_pages": len(receipts),
        "useful_nonempty_pages": 3,
        "unknown_empty_branches_probed": 2,
        "authoritative_zero_branches_skipped": 1,
        "proved_leaf_branches_skipped": 1,
        "exact_exhausted_branches_skipped": 2,
        "actual_reply_send_gaps_seconds": send_gaps,
        "persistent_clients_created": len(clients),
    }
    if coordination is not None:
        control = coordination.report()
        metrics["mode"] = mode
        metrics["business_database"] = {
            **measured,
            "explicit_transaction_contexts": meter.transactions,
            "autocommit_statement_transactions": meter.autocommit_transactions,
            "sql_seconds": meter.sql_seconds,
            "explicit_transaction_context_seconds": meter.transaction_seconds,
        }
        metrics["coordination_database"] = control
        metrics["sql_seconds"] = meter.sql_seconds + control["sql_seconds"]
        metrics["database_timer_scope"] = (
            "sql_seconds includes business and coordination SQL; transaction_seconds retains the business explicit "
            "context timer. Coordination explicit-context and checkout timers are separate; timers overlap."
        )
        metrics["transactions"] = measured["transactions"] + control["database_transactions"]
        metrics["sql_round_trips"] = measured["sql_round_trips"] + control["sql_round_trips"]
        for unit, size in (("send", len(calls)), ("page", len(receipts))):
            metrics["transactions_per_" + unit] = metrics["transactions"] / size
            metrics["sql_round_trips_per_" + unit] = metrics["sql_round_trips"] / size
    return metrics, proof


@pytest.mark.skipif(os.getenv("TRR_ADMISSION_BENCHMARK") != "1", reason="explicit short matched benchmark")
def test_matched_request_limited_admission(database, fresh_public_lanes, monkeypatch, tmp_path):
    started = time.monotonic()
    baseline = fixture_baseline()
    results, proofs = {}, []
    baseline_report = os.getenv("TRR_ADMISSION_BASELINE_REPORT")
    baseline_hash = None
    if baseline_report:
        baseline_bytes = Path(baseline_report).read_bytes()
        saved = json.loads(baseline_bytes)
        assert saved["baseline_revision"] == BASELINE and saved["exact_equivalence"] is True
        assert saved["settings"] == {
            "reply_slots": 2,
            "local_global_pacing_seconds": 0.25,
            "saved_page_size": 12,
            "synthetic_sql_roundtrip_seconds": 0.01,
        }, "Saved baseline used different request, pacing, or SQL latency settings"
        results["v165"] = saved["results"]["v165"]
        proofs.append(saved["proof"])
        baseline_hash = hashlib.sha256(baseline_bytes).hexdigest()
    arms = (("current", False),) if baseline_report else (("v165", True), ("current", False))
    for name, old in arms:
        results[name], proof = run_arm(database, baseline, monkeypatch, tmp_path / name, old)
        proofs.append(json.loads(json.dumps(proof)))
    assert proofs[0] == proofs[1], "Exact retained coverage or evidence/receipt binding differs"
    elapsed = time.monotonic() - started
    assert elapsed < 60
    report = {
        "baseline_revision": BASELINE,
        "results": results,
        "proof": proofs[0],
        "exact_equivalence": True,
        "pair_elapsed_seconds": None if baseline_report else elapsed,
        "validation_elapsed_seconds": elapsed,
        "measured_arms": [name for name, _ in arms],
        "baseline_reused_from": baseline_report,
        "baseline_report_sha256": baseline_hash,
        "elapsed_improvement_fraction": 1 - results["current"]["elapsed_seconds"] / results["v165"]["elapsed_seconds"],
        "settings": {
            "reply_slots": 2,
            "local_global_pacing_seconds": 0.25,
            "saved_page_size": 12,
            "synthetic_sql_roundtrip_seconds": 0.01,
        },
        "timer_scope": (
            "scheduler timers overlap; owner includes persistence; admission SQL is a subset of transaction/SQL totals"
        ),
        "limitations": (
            "One short paired replay with declared synthetic SQL latency; not native endpoint qualification, "
            "full January, media download or production latency evidence."
        ),
    }
    output = Path(os.getenv("TRR_ADMISSION_BENCHMARK_OUTPUT", str(tmp_path / "admission-performance.json")))
    output.write_text(json.dumps(report, indent=2))
    print(json.dumps(report))


@pytest.mark.skipif(os.getenv("TRR_ADMISSION_ADVISORY_BENCHMARK") != "1", reason="explicit advisory matched benchmark")
def test_matched_advisory_request_limited_admission(database, fresh_public_lanes, monkeypatch, tmp_path):
    started = time.monotonic()
    baseline = fixture_baseline()
    results, proofs = {}, []
    settings = {
        "rate_limit_mode": "advisory",
        "control_checkout_limit": 1,
        "reply_slots": 2,
        "local_global_pacing_seconds": 0.25,
        "saved_page_size": 12,
        "synthetic_sql_roundtrip_seconds": 0.01,
    }
    baseline_report = os.getenv("TRR_ADMISSION_BASELINE_REPORT")
    baseline_hash = None
    if baseline_report:
        baseline_bytes = Path(baseline_report).read_bytes()
        saved = json.loads(baseline_bytes)
        assert saved["baseline_revision"] == BASELINE and saved["exact_equivalence"] is True
        assert saved["settings"] == settings, "Cached advisory baseline used different workload/settings"
        results["v165"] = saved["results"]["v165"]
        proofs.append(saved["proof"])
        baseline_hash = hashlib.sha256(baseline_bytes).hexdigest()
    arms = (("current", False),) if baseline_report else (("v165", True), ("current", False))
    for name, old in arms:
        results[name], proof = run_arm(database, baseline, monkeypatch, tmp_path / name, old, mode="advisory")
        proofs.append(json.loads(json.dumps(proof)))
    assert proofs[0] == proofs[1], "Advisory retained coverage or evidence/receipt binding differs"
    elapsed = time.monotonic() - started
    assert elapsed < 60
    report = {
        "baseline_revision": BASELINE,
        "results": results,
        "proof": proofs[0],
        "exact_equivalence": True,
        "pair_elapsed_seconds": None if baseline_report else elapsed,
        "validation_elapsed_seconds": elapsed,
        "measured_arms": [name for name, _ in arms],
        "baseline_reused_from": baseline_report,
        "baseline_report_sha256": baseline_hash,
        "elapsed_improvement_fraction": 1 - results["current"]["elapsed_seconds"] / results["v165"]["elapsed_seconds"],
        "settings": settings,
        "database_counter_scope": (
            "Total transactions include business explicit contexts, coordination explicit contexts, "
            "and one transaction for each autocommit coordination SQL statement. "
            "Nested SAVEPOINT/RELEASE statements are SQL round trips within the existing transaction."
        ),
        "timer_scope": (
            "Timers overlap. Owner includes persistence; SQL time is within transaction/checkout time; "
            "advisory checkout lifetime includes session-lock waits and admission while idle outside SQL transactions."
        ),
        "limitations": (
            "One short isolated advisory pair with synthetic SQL latency, not production-latency, "
            "full-January, media-download, or native endpoint qualification evidence."
        ),
    }
    output = Path(os.getenv("TRR_ADMISSION_BENCHMARK_OUTPUT", str(tmp_path / "advisory-performance.json")))
    output.write_text(json.dumps(report, indent=2))
    print(json.dumps(report))
