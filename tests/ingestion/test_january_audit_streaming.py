"""Bounded final audit fixtures: no database, source, storage or worker calls."""

import gc
import json
import tracemalloc
import weakref
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from scripts.ingestion.january_audit import comments, common, offline_comments
from tests.ingestion.test_certification import evidence
from trr_backend.ingestion.certification import certify, digest

AT = datetime(2026, 10, 24, tzinfo=UTC)


@pytest.fixture(autouse=True)
def audit_context():
    value = common.AuditContext("fixture-run", "a" * 64, "bravotv", AT, AT, 1)
    with common.bound(value):
        yield value


class StreamCursor:
    def __init__(self, factory):
        self.factory, self.closed, self.itersize = factory, False, None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True

    def execute(self, sql, args):
        self.sql, self.args = sql, args
        assert self.itersize == 1

    def __iter__(self):
        return iter(self.factory())

    def fetchall(self):
        pytest.fail("Streaming audit materialized the complete cursor")


def cursor(factory):
    streams = []

    class Connection:
        def cursor(self, *, name, cursor_factory):
            assert name.startswith("january_audit_")
            value = StreamCursor(factory)
            streams.append(value)
            return value

    return SimpleNamespace(connection=Connection()), streams


def row(index):
    return {
        "page_id": f"page-{index:05}",
        "evidence_id": f"evidence-{index:05}",
        "target_key": "DTg",
        "committed_at": AT + timedelta(seconds=index),
        "captured_at": AT + timedelta(seconds=index),
        "payload": {"native": index},
        "checkpoint": {"comment_pages": {"version": 1, "observed": {str(index): {}}}},
    }


def metadata(values):
    return [{key: value[key] for key in comments.PAGE_METADATA_FIELDS} for value in values]


def query_fingerprint(values, *, sql="select full_row where generation=1", args=("run",)):
    common.context().query_material.clear()
    common.record_query(common.query_sql(sql), args, map(common.row_fingerprint, values))
    return digest({"version": common.context().query_fingerprint_version, "queries": common.context().query_material})


def test_compact_fingerprint_preserves_row_order_independence_and_duplicate_multiplicity(audit_context):
    values = [row(0), row(1)]
    expected = query_fingerprint(values)
    assert query_fingerprint(values[::-1]) == expected
    assert query_fingerprint([*values, values[0]]) != expected
    assert query_fingerprint(values, args=("other",)) != expected
    assert query_fingerprint(values, sql="select different_projection") != expected
    assert audit_context.query_fingerprint_version == common.QUERY_FINGERPRINT_VERSION
    assert "rows" not in audit_context.query_material[0]
    assert set(audit_context.query_material[0]) == {"sql", "args", "row_count", "rows_sha256"}


@pytest.mark.parametrize("field", ["payload", "checkpoint"])
def test_full_row_mutation_changes_fingerprint_even_for_historical_pages(field):
    values = [row(index) for index in range(4)]
    expected = query_fingerprint(values)
    values[0][field]["unreplayed_history_changed"] = True
    assert query_fingerprint(values) != expected


def test_stream_fingerprints_all_history_while_yielding_only_current_scan(audit_context):
    values = [row(index) for index in range(5)]
    cur, streams = cursor(lambda: iter(values))
    with comments.page_payloads(cur, metadata(values), target_key="DTg", start=3) as payloads:
        assert [value["page_id"] for value in payloads] == ["page-00003", "page-00004"]
    assert streams[0].closed and streams[0].itersize == 1
    assert audit_context.query_material[0]["row_count"] == 5
    assert audit_context.query_material[0]["sql"] == comments.PAGE_PAYLOAD_SQL
    assert audit_context.query_material[0]["args"] == ("fixture-run", "DTg")
    assert audit_context.query_material[0]["rows_sha256"] == digest_hashes(values)


def digest_hashes(values):
    import hashlib

    return hashlib.sha256("".join(sorted(map(common.row_fingerprint, values))).encode("ascii")).hexdigest()


@pytest.mark.parametrize("failure", ["missing", "extra", "duplicate", "reordered", "changed"])
def test_stream_rejects_inventory_drift_and_always_closes_cursor(failure):
    original = [row(index) for index in range(3)]
    values = deepcopy(original)
    if failure == "missing":
        values.pop()
    elif failure == "extra":
        values.append(row(3))
    elif failure == "duplicate":
        values[1] = deepcopy(values[0])
    elif failure == "reordered":
        values.reverse()
    else:
        values[1]["evidence_id"] = "changed"
    cur, streams = cursor(lambda: iter(values))
    with (
        pytest.raises(common.GuardError),
        comments.page_payloads(cur, metadata(original), target_key="DTg") as payloads,
    ):
        list(payloads)
    assert streams[0].closed
    assert common.context().query_material == []


@pytest.mark.parametrize("failure", ["consumer", "transport", "early_close"])
def test_stream_crash_and_early_exit_close_cursor_without_partial_fingerprint(failure):
    def values():
        yield row(0)
        if failure == "transport":
            raise OSError("synthetic cursor failure")
        yield row(1)

    cur, streams = cursor(values)
    with pytest.raises((RuntimeError, OSError, common.GuardError)):
        with common.stream_rows(cur, "select fixture", ()) as stream:
            next(stream)
            if failure == "consumer":
                raise RuntimeError("synthetic owner validation failure")
            if failure == "transport":
                list(stream)
    assert streams[0].closed and common.context().query_material == []


def test_many_large_checkpoints_have_bounded_residency_and_compact_ledger(audit_context):
    live, peak = 0, 0

    class Checkpoint(dict):
        pass

    def destroyed():
        nonlocal live
        live -= 1

    def values():
        nonlocal live, peak
        for index in range(120):
            value = row(index)
            checkpoint = Checkpoint(padding="x" * (512 * 1024), comment_pages={"observed": {str(index): {}}})
            live += 1
            peak = max(peak, live)
            weakref.finalize(checkpoint, destroyed)
            value["checkpoint"] = checkpoint
            value["payload"] = {"native": index, "padding": str(index) + "y" * (256 * 1024)}
            yield value

    entries = metadata(row(index) for index in range(120))
    cur, streams = cursor(values)
    tracemalloc.start()
    try:
        with comments.page_payloads(cur, entries, target_key="DTg", start=60) as stream:
            seen = 0
            for value in stream:
                assert len(value["checkpoint"]["padding"]) == 512 * 1024
                seen += 1
        _, memory_peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    del value
    gc.collect()
    assert seen == 60 and streams[0].closed
    assert peak <= 3 and live == 0
    assert memory_peak < 8 * 1024 * 1024
    assert audit_context.query_material[0]["row_count"] == 120
    assert len(json.dumps(audit_context.query_material)) < 2000


@pytest.mark.parametrize("mutation", [None, "payload", "checkpoint"])
def test_two_compact_snapshot_fingerprints_agree_and_new_mutations_block_acceptance(mutation):
    before, after, byte_report = evidence()
    values = [row(0), row(1)]
    before["query_fingerprint"] = query_fingerprint(values)
    if mutation:
        values[0][mutation]["changed"] = True
    after["query_fingerprint"] = query_fingerprint(values[::-1])
    result = certify(
        before,
        after,
        byte_report,
        storage_binding_sha256="b" * 64,
        storage_binding_after_sha256="b" * 64,
        source_sha256="c" * 64,
        source_after_sha256="c" * 64,
    )
    assert result["final_acceptance"] is (mutation is None)
    assert result["gates"]["unchanged_evidence"]["status"] == ("passed" if mutation is None else "blocked")


def test_streaming_and_bulk_fixture_audits_keep_exact_reports_and_diagnostics(monkeypatch):
    # Keep one fixture's exact UUIDs/clocks for both paths. The comparator only
    # swaps payload retrieval, leaving all maintained audit checks unchanged.
    cur, listing, _, progress, rows, reference, parse_post, targets = offline_comments.producer_fixture()
    original_pages = {
        target["target_key"]: rows(cur, comments.PAGE_PAYLOAD_SQL, (common.RUN, target["target_key"]))
        for target in targets
    }

    def inspect():
        return comments.inspect(
            cur, {"successor_intent": {}}, offline_comments.RELEASE, listing, offline_comments.LISTING_HASH
        )

    with (
        patch("trr_backend.ingestion.queries.Queries.progress", return_value=progress),
        patch.object(common, "rows", side_effect=rows),
        patch.object(common, "read_reference", side_effect=reference),
        patch("trr_backend.ingestion.providers.parse_evidence", side_effect=parse_post),
        patch("trr_backend.ingestion.application.validate_detail"),
        patch("trr_backend.ingestion.canonical._post_payload", side_effect=lambda post, *args: post),
    ):
        streaming = inspect()

        @contextmanager
        def bulk(_cur, _metadata, *, target_key, start=0):
            yield iter(original_pages[target_key][start:])

        monkeypatch.setattr(comments, "page_payloads", bulk)
        bulk_report = inspect()
    assert streaming == bulk_report
    assert streaming["global_validation"]["status"] == "passed"
    statuses = {target["shortcode"]: target["status"] for target in streaming["target_results"]}
    assert statuses == {"Zero": "passed", "Gap": "pending", "Extra": "blocked", "Unresolved": "pending"}
