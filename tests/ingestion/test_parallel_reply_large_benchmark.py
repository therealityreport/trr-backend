"""Fixed large-checkpoint paired benchmark, isolated PG and mock HTTP only.

Final gate: TRR_REPLY_LARGE_BENCHMARK=1, always five alternating pairs.
Fixture smoke: TRR_REPLY_LARGE_BENCHMARK_SMOKE=1 (one pair by default).
The initial fixture bulk-loads synthetic immutable native events, canonical rows
and a matching final checkpoint. Earlier setup receipts intentionally contain
empty checkpoints: no thousands of growing full snapshots or live requests are
needed to construct this benchmark. Production resume still verifies all events.
"""

import asyncio
import copy
import hashlib
import json
import os
import statistics
import time
from collections import Counter
from datetime import timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pytest
from psycopg2.extras import Json, execute_values

from tests.ingestion import test_public_comment_transport as native
from trr_backend.ingestion.canonical import _comment_payload
from trr_backend.ingestion.comments import CommentActivity, parse_comment_evidence
from trr_backend.ingestion.composition import _GatedSource
from trr_backend.ingestion.domain import Stage, stable_json
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.identity_policy import SourceRequestGate

database = native.database
request_lanes = native.request_lanes
fresh_public_lanes = native.fresh_public_lanes
deny_unmocked_http = native.deny_unmocked_http

TOP_COUNT = 14_395
LEAF_COUNT = 147
PROVED_TOP_COUNT = 4_638
EMPTY_TOP_COUNT = PROVED_TOP_COUNT - LEAF_COUNT


def parent_id(index):
    return str(17800000000000000 + index)


def top_cursor(index):
    return hashlib.sha256(f"fixed-native-cursor:{index}".encode()).hexdigest()


SELECTED = tuple(parent_id(index) for index in range(PROVED_TOP_COUNT, PROVED_TOP_COUNT + 64))
LATENCIES = (0.72, 0.54, 0.66, 0.48, 0.60, 0.78, 0.54, 0.66)
EXPECTED = 27_488
BASELINE_AT = native.OBSERVED - timedelta(hours=2)


def digest(value):
    return hashlib.sha256(stable_json(value).encode()).hexdigest()


@pytest.fixture(scope="module")
def large_baseline():
    """Build source facts once; no transport and no growing checkpoint copies."""
    started = time.monotonic()
    state = {"version": 1, "observed": {}, "lanes": {}, "expected": None}
    events, canonical = [], {}

    def event(payload, parent=None, cursor=None):
        index = len(events)
        observed = BASELINE_AT + timedelta(microseconds=index)
        document = "36239935742272683" if parent else "25516980651312394"
        friendly = "PolarisPostCommentsChildrenPaginationtQuery" if parent else "PolarisPostCommentsPaginationQuery"
        envelope = native.maintained._durable_relay_payload(
            payload,
            cursor=cursor,
            doc_id=document,
            friendly_name=friendly,
            sort_order="popular",
            page_size=12,
            parent=parent,
        )
        evidence = Evidence.capture(
            source_identity=f"instagram:comments:template:{'graphql_replies' if parent else 'graphql_comments'}",
            captured_at=observed,
            parser_version="instagram-comments-v1",
            payload=envelope,
        )
        page = parse_comment_evidence(evidence, pinned=True)
        native._advance(state, page, scan_started_at=BASELINE_AT)
        if page.started_from_root and parent is None:
            state["scan_root_evidence_id"] = "template-root"
        identities = []
        for comment, raw in zip(page.comments, page.source_payloads, strict=True):
            identities.append(comment.comment_id)
            canonical[comment.comment_id] = _comment_payload(
                comment,
                raw,
                post_id="template",
                account="bravotv",
                observed_at=observed,
                run_id="template",
                parent_id=parent,
                depth=1 if parent else 0,
                metadata_writes_enabled=True,
            )
        events.append((evidence, page, identities))

    for offset in range(0, TOP_COUNT, 12):
        rows = [native.actual_parent_shape(parent_id(index)) for index in range(offset, min(offset + 12, TOP_COUNT))]
        for row in rows:
            if row["pk"] == SELECTED[0]:
                row.update(
                    text="",
                    giphy_media_info={
                        "id": "baseline-gif",
                        "images": {"fixed_height": {"url": "https://media.example/baseline.gif"}},
                    },
                )
        more = offset + 12 < TOP_COUNT
        event(
            native.page(rows, more=more, after=top_cursor(offset + 12) if more else None),
            cursor=top_cursor(offset) if offset else None,
        )
    for index in range(EMPTY_TOP_COUNT):
        event(native.page([], child=True), parent=parent_id(index))
    for index in range(LEAF_COUNT):
        reply = native.actual_parent_shape(str(18300000000000000 + index))
        if index == LEAF_COUNT - 1:
            reply["sticker"] = {"image_url": "https://media.example/baseline-sticker.webp"}
        event(native.page([reply], child=True), parent=parent_id(EMPTY_TOP_COUNT + index))
    assert len(state["observed"]) == TOP_COUNT + LEAF_COUNT == len(canonical)
    assert sum(row["parent"] is None for row in state["observed"].values()) == TOP_COUNT
    assert sum(row.get("native_reply_leaf") is True for row in state["observed"].values()) == LEAF_COUNT
    assert len(state["lanes"]) - 1 == PROVED_TOP_COUNT
    assert all(row["expected_replies"] is None for row in state["observed"].values())
    assert state["expected"] is None
    return {
        "events": events,
        "canonical": canonical,
        "state": state,
        "construction_seconds": time.monotonic() - started,
    }


def seed(database, baseline):
    started = time.monotonic()
    store, tx, args = native.setup(database)
    state = copy.deepcopy(baseline["state"])
    root_id = str(uuid5(NAMESPACE_URL, args["run_id"] + ":evidence:0"))
    state["scan_root_evidence_id"] = root_id
    full_checkpoint = {"comment_pages": state}
    canonical_ids = {key: str(uuid5(NAMESPACE_URL, args["post_id"] + ":" + key)) for key in baseline["canonical"]}
    evidence_rows, receipt_rows = [], []
    for index, (evidence, page, identities) in enumerate(baseline["events"]):
        evidence_id = str(uuid5(NAMESPACE_URL, args["run_id"] + f":evidence:{index}"))
        page_id = str(uuid5(NAMESPACE_URL, args["run_id"] + f":page:{index}"))
        evidence_rows.append(
            (
                evidence_id,
                args["run_id"],
                "comments",
                args["target_key"],
                f"instagram:comments:{args['target_key']}:"
                + ("graphql_replies" if page.parent_comment_id else "graphql_comments"),
                evidence.captured_at,
                evidence.expires_at,
                evidence.content_hash,
                evidence.parser_version,
                evidence.schema_version,
                evidence.payload_json,
            )
        )
        identity = {"post_id": args["post_id"], "comment_ids": {key: canonical_ids[key] for key in identities}}
        receipt_rows.append(
            (
                page_id,
                args["run_id"],
                "comments",
                args["target_key"],
                1,
                evidence_id,
                evidence.captured_at,
                evidence.content_hash,
                page.parent_comment_id,
                page.next_cursor,
                page.exhausted,
                Json(identity),
                Json(full_checkpoint if index == len(baseline["events"]) - 1 else {}),
                evidence.captured_at,
            )
        )
    payloads = []
    for key, original in baseline["canonical"].items():
        parent = state["observed"][key]["parent"]
        payloads.append(
            {
                **original,
                "media_urls": original.get("media_urls", []),
                "text": original.get("text", ""),
                "username": original.get("username", ""),
                "id": canonical_ids[key],
                "post_id": args["post_id"],
                "last_seen_run_id": args["run_id"],
                "parent_comment_id": canonical_ids[parent] if parent else None,
            }
        )
    columns = sorted(set().union(*(row.keys() for row in payloads)))
    values = [
        tuple(Json(row.get(key)) if isinstance(row.get(key), (dict, list)) else row.get(key) for key in columns)
        for row in payloads
    ]
    with tx() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """insert into social.instagram_ingestion_evidence
            (evidence_id,run_id,stage,target_key,source_identity,captured_at,expires_at,content_hash,
            parser_version,schema_version,payload) values %s""",
            evidence_rows,
            page_size=500,
        )
        execute_values(
            cur,
            """insert into social.instagram_ingestion_comment_pages
            (page_id,run_id,stage,target_key,owner_generation,evidence_id,observed_at,content_hash,
             parent_comment_id,next_cursor,exhausted,canonical_identity,checkpoint,committed_at)
             values %s""",
            receipt_rows,
            page_size=500,
        )
        # Fixture parents precede replies so the real FK remains enabled.
        execute_values(
            cur, "insert into social.instagram_comments (" + ",".join(columns) + ") values %s", values, page_size=500
        )
        cur.execute(
            "update social.instagram_ingestion_targets set checkpoint=%s::jsonb where run_id=%s",
            (stable_json(full_checkpoint), args["run_id"]),
        )
        cur.execute("update social.instagram_posts set comments_count=%s where id=%s", (EXPECTED, args["post_id"]))
    return (
        store,
        tx,
        args,
        state,
        {
            "seed_seconds": time.monotonic() - started,
            "baseline_checkpoint_bytes": len(stable_json(full_checkpoint).encode()),
            "baseline_pages": len(receipt_rows),
        },
    )


def inventory(tx, post_id):
    rows = native.existing.query(
        tx,
        """select c.comment_id,
        (to_jsonb(c)-'id'-'post_id'-'last_seen_run_id'-'comment_url') ||
        jsonb_build_object('parent_comment_id',p.comment_id) as value
        from social.instagram_comments c left join social.instagram_comments p on p.id=c.parent_comment_id
        where c.post_id=%s order by c.comment_id""",
        (post_id,),
    )
    return {row["comment_id"]: digest(row["value"]) for row in rows}


def arm(database, baseline, monkeypatch, pace_root, concurrency):
    import httpx

    store, tx, args, initial_state, setup_metrics = seed(database, baseline)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", str(concurrency))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CANARY_TARGET", args["target_key"])
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CANARY_GENERATION", "1")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CANARY_RUN_ID", args["run_id"])
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CANARY_RUN_GENERATION", "1")
    pace_root.mkdir()
    monkeypatch.setattr(native.maintained, "_global_rate_limit_path", lambda _key: str(pace_root / "pace"))
    monkeypatch.setattr(native.maintained, "_global_rate_cooldown_path", lambda _key: str(pace_root / "cooldown"))
    calls, resumes = [], []
    before = inventory(tx, args["post_id"])

    async def handler(request):
        if request.method == "GET":
            return native.bootstrap()
        _, variables = native.body(request)
        parent, cursor = variables.get("parent_comment_id"), variables.get("after")
        assert parent in SELECTED, "benchmark resumed outside its fixed 64-parent window or refetched the top"
        index = SELECTED.index(parent)
        calls.append((parent, cursor, variables["first"]))
        await asyncio.sleep(LATENCIES[index % len(LATENCIES)])
        replies, more = [], False
        if parent == SELECTED[-1]:
            reply = native.actual_parent_shape("18400000000000001" if cursor is None else "18400000000000002")
            if cursor is not None:
                reply.update(
                    text="",
                    giphy_media_info={
                        "id": "new-gif",
                        "images": {"fixed_height": {"url": "https://media.example/new.gif"}},
                    },
                )
            replies, more = [reply], cursor is None
        return httpx.Response(
            200, json=native.page(replies, child=True, more=more, after="final-tail" if more else None)
        )

    fetch, created = native.source(monkeypatch, handler, expected=EXPECTED)
    base_factory = fetch._factory

    def factory(**kwargs):
        result = base_factory(**kwargs)
        result._api_delay_seconds = result._global_api_delay_seconds = 0.25
        result._global_rate_limit_mode_configured = "file_lock"
        return result

    fetch._factory = factory

    def fixed_window(**kwargs):
        # This runs after the real _resume_inventory validated the complete
        # baseline. Filtering is a benchmark window, never production behavior.
        full = kwargs["resume_inventory"]
        assert len(full) == TOP_COUNT + LEAF_COUNT
        assert {
            item.source_payload["comment_id"] if "comment_id" in item.source_payload else str(item.source_payload["pk"])
            for item in full
        } == set(before)
        resumes.append(len(full))
        chosen = tuple(
            item
            for item in full
            if str(item.source_payload.get("comment_id") or item.source_payload.get("pk")) in SELECTED
        )
        assert len(chosen) == 64
        try:
            return fetch(**{**kwargs, "resume_inventory": chosen})
        except BaseException:
            # Fixture-only traces contain synthetic data and expose the cause
            # before the production gate maps it to a fixed failure code.
            import traceback

            traceback.print_exc()
            raise

    owned = {key: value for key, value in args.items() if key != "post_id"}
    gate = SourceRequestGate(
        cookies={},
        public_only=True,
        network_lane="local",
        transaction=tx,
        check_cancelled=lambda: store.check_owner(**owned, stage=Stage.COMMENTS),
        check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, stage=Stage.COMMENTS, connection=conn),
        deadline=time.monotonic() + 590,
    )
    gated = _GatedSource(fixed_window, gate, failure_code="comment_fetch_failed")
    started = time.monotonic()
    try:
        result = CommentActivity(store, gated, clock=lambda: native.OBSERVED).execute(**args)
    finally:
        gated.finish_failure()
    elapsed = time.monotonic() - started
    assert resumes == [TOP_COUNT + LEAF_COUNT]
    assert result["complete"] is False and result["outcome"] == "unresolved"
    current = result["checkpoint"]["comment_pages"]
    assert current["expected"] is initial_state["expected"] is None
    assert len(calls) == 65 and {parent for parent, _, _ in calls} == set(SELECTED)
    assert Counter(parent for parent, _, _ in calls) == Counter(
        {parent: 2 if parent == SELECTED[-1] else 1 for parent in SELECTED}
    )
    assert all(size == 12 for _, _, size in calls)
    assert [cursor for parent, cursor, _ in calls if parent == SELECTED[-1]] == [None, "final-tail"]
    assert current["lanes"]["__top__"] == initial_state["lanes"]["__top__"]
    assert all(value == current["lanes"][key] for key, value in initial_state["lanes"].items())
    assert all(value == current["observed"][key] for key, value in initial_state["observed"].items())
    assert all(current["lanes"][parent]["exhausted"] is True for parent in SELECTED)
    assert sum(parent_id(index) not in current["lanes"] for index in range(TOP_COUNT)) == 9693
    after = inventory(tx, args["post_id"])
    assert len(after) == TOP_COUNT + LEAF_COUNT + 2
    assert all(after[key] == value for key, value in before.items())
    assert set(after) - set(before) == {"18400000000000001", "18400000000000002"}
    reported_expected = native.existing.query(
        tx, "select comments_count from social.instagram_posts where id=%s", (args["post_id"],)
    )[0]["comments_count"]
    assert reported_expected == EXPECTED
    received = (
        native.existing.query(
            tx, "select count(*) as n from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
        )[0]["n"]
        - setup_metrics["baseline_pages"]
    )
    assert received == 65
    # Compare immutable received evidence and its receipt binding, independent
    # of run-local UUIDs and completion order. Seeded receipts precede this clock.
    receipt_rows = native.existing.query(
        tx,
        """select p.observed_at,p.content_hash,p.parent_comment_id,p.next_cursor,p.exhausted,
        e.captured_at,e.content_hash as evidence_hash,e.parser_version,e.schema_version,e.payload,
        p.run_id=e.run_id as same_run,p.stage=e.stage as same_stage,
        p.target_key=e.target_key as same_target
        from social.instagram_ingestion_comment_pages p join social.instagram_ingestion_evidence e
        on p.evidence_id=e.evidence_id where p.run_id=%s and e.captured_at=%s""",
        (args["run_id"], native.OBSERVED),
    )
    assert len(receipt_rows) == 65
    for receipt in receipt_rows:
        assert receipt.pop("same_run") and receipt.pop("same_stage") and receipt.pop("same_target")
        assert receipt["observed_at"] == receipt["captured_at"]
        assert receipt["content_hash"] == receipt["evidence_hash"]
        receipt["payload"] = json.loads(receipt["payload"])
        assert hashlib.sha256(stable_json(receipt["payload"]).encode()).hexdigest() == receipt["content_hash"]
        receipt["observed_at"] = receipt["observed_at"].isoformat()
        receipt["captured_at"] = receipt["captured_at"].isoformat()
    normalized_receipts = sorted(receipt_rows, key=lambda row: (row["parent_comment_id"], str(row["next_cursor"])))
    proof = {
        "received_receipt_evidence_sha256": digest(normalized_receipts),
        "all_saved_rows_sha256": digest(after),
        "observed_sha256": digest(current["observed"]),
        "lanes_sha256": digest(current["lanes"]),
        "expected": current["expected"],
        "reported_expected": reported_expected,
        "received_pages": received,
        "calls": sorted(calls, key=str),
    }
    metrics = created[0].runtime_metadata["reply_scheduler"]
    assert metrics["max_outstanding"] <= concurrency
    return {"elapsed_seconds": elapsed, **setup_metrics, **metrics}, proof


def run_pairs(database, baseline, monkeypatch, tmp_path, pairs, *, final):
    timings, reference = [], None
    for pair in range(pairs):
        values = {}
        for concurrency in (1, 2) if pair % 2 == 0 else (2, 1):
            metrics, proof = arm(database, baseline, monkeypatch, tmp_path / f"pace-{pair}-{concurrency}", concurrency)
            if reference is None:
                reference = proof
            assert proof == reference
            values[concurrency] = metrics
            print(json.dumps({"pair": pair, "concurrency": concurrency, **metrics}), flush=True)
        timings.append(values)
    names = (
        "elapsed_seconds",
        "network_seconds",
        "pacing_seconds",
        "queue_seconds",
        "completed_queue_seconds",
        "owner_seconds",
        "persistence_seconds",
        "seed_seconds",
    )
    summary = {
        str(concurrency): {
            name: {
                "median": statistics.median(pair[concurrency][name] for pair in timings),
                "variance": statistics.variance(pair[concurrency][name] for pair in timings) if pairs > 1 else 0.0,
            }
            for name in names
        }
        for concurrency in (1, 2)
    }
    improvement = 1 - summary["2"]["elapsed_seconds"]["median"] / summary["1"]["elapsed_seconds"]["median"]
    report = {
        "final_gate": final,
        "pair_count": pairs,
        "pairs": timings,
        "summary": summary,
        "median_improvement": improvement,
        "equivalent": True,
        "passed": improvement >= 0.20,
        "fixture": {
            "top_ids": TOP_COUNT,
            "leaf_ids": LEAF_COUNT,
            "proved_top_lanes": PROVED_TOP_COUNT,
            "initial_pending_top_lanes": 9757,
            "remaining_pending_top_lanes": 9693,
            "selected_parent_ids": SELECTED,
            "requests_per_arm": 65,
            "reported_expected": EXPECTED,
            "checkpoint_expected": None,
            "latencies": LATENCIES,
            "page_size": 12,
            "pacing_seconds": 0.25,
            "construction_seconds": baseline["construction_seconds"],
            "measured_live_checkpoint_bytes": 2442926,
            "fixture_checkpoint_bytes": timings[0][1]["baseline_checkpoint_bytes"],
            "checkpoint_size_ratio": timings[0][1]["baseline_checkpoint_bytes"] / 2442926,
        },
        "proof": reference,
    }
    output = os.getenv("TRR_REPLY_LARGE_BENCHMARK_OUTPUT")
    if output:
        Path(output).write_text(json.dumps(report, indent=2))
    print(json.dumps(report), flush=True)
    if final:
        assert pairs == 5 and improvement >= 0.20
    return report


@pytest.mark.skipif(os.getenv("TRR_REPLY_LARGE_BENCHMARK_SMOKE") != "1", reason="explicit fixture smoke only")
def test_large_checkpoint_fixture_smoke(fresh_public_lanes, database, large_baseline, monkeypatch, tmp_path):
    pairs = int(os.getenv("TRR_REPLY_LARGE_BENCHMARK_SMOKE_PAIRS", "1"))
    assert 1 <= pairs <= 2
    run_pairs(database, large_baseline, monkeypatch, tmp_path, pairs, final=False)


@pytest.mark.skipif(os.getenv("TRR_REPLY_LARGE_BENCHMARK_SETUP") != "1", reason="explicit setup validation only")
def test_large_checkpoint_seed_and_unfiltered_resume(database, large_baseline):
    store, _, args, state, metrics = seed(database, large_baseline)
    activity = CommentActivity(store, None, clock=lambda: native.OBSERVED)
    started = time.monotonic()
    nodes, proof = activity._resume_inventory(args, state)
    assert len(nodes) == TOP_COUNT + LEAF_COUNT and proof is not None
    print(
        json.dumps({**metrics, "resume_seconds": time.monotonic() - started, "validated_ids": len(nodes)}), flush=True
    )


@pytest.mark.skipif(os.getenv("TRR_REPLY_LARGE_BENCHMARK") != "1", reason="explicit fixed five-pair final gate")
def test_five_large_checkpoint_pairs(fresh_public_lanes, database, large_baseline, monkeypatch, tmp_path):
    run_pairs(database, large_baseline, monkeypatch, tmp_path, 5, final=True)
