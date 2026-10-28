"""Short opt-in paired comparison; mocked source and isolated PostgreSQL only."""

import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_parallel_reply_large_benchmark as large
from trr_backend.ingestion import comments
from trr_backend.ingestion.domain import stable_json

database = large.database
large_baseline = large.large_baseline
fresh_public_lanes = large.fresh_public_lanes
request_lanes = large.request_lanes
deny_unmocked_http = large.deny_unmocked_http


@pytest.mark.skipif(not os.getenv("TRR_CHECKPOINT_TRANSFER_BASELINE"), reason="explicit short comparison only")
def test_checkpoint_transfer_pair(database, large_baseline, fresh_public_lanes, monkeypatch, tmp_path):
    baseline_path = Path(os.environ["TRR_CHECKPOINT_TRANSFER_BASELINE"])
    spec = importlib.util.spec_from_file_location("checkpoint_transfer_baseline", baseline_path)
    baseline = importlib.util.module_from_spec(spec)  # pyright: ignore[reportArgumentType]
    assert spec is not None
    sys.modules[spec.name] = baseline
    assert spec.loader is not None
    spec.loader.exec_module(baseline)
    current = comments.CommentActivity._persist_page

    def legacy_page(self, *args, expected_checkpoint_hash=None, **kwargs):
        return baseline.CommentActivity._persist_page(self, *args, **kwargs)

    results, reference = {}, None
    # Fixed transport latency and unchanged 250ms source spacing in both arms.
    monkeypatch.setattr(large, "LATENCIES", (0.01,))
    for name, implementation in (("before", legacy_page), ("after", current)):
        sizes = {"json_parameter_bytes": 0, "checkpoint_result_bytes": 0}

        class TracedCursor(RealDictCursor):
            measurements = sizes

            def execute(self, query, params=None):  # pyright: ignore[reportIncompatibleMethodOverride]
                for value in params or ():
                    if isinstance(value, str) and value.startswith("{"):
                        self.measurements["json_parameter_bytes"] += len(value.encode())
                return super().execute(query, params)

            def fetchone(self):
                row = super().fetchone()
                if row and "checkpoint" in row:
                    self.measurements["checkpoint_result_bytes"] += len(stable_json(row["checkpoint"]).encode())
                return row

        monkeypatch.setattr(comments, "RealDictCursor", TracedCursor)
        monkeypatch.setattr(baseline, "RealDictCursor", TracedCursor)
        monkeypatch.setattr(comments.CommentActivity, "_persist_page", implementation)
        metrics, proof = large.arm(database, large_baseline, monkeypatch, tmp_path / name, 2)
        if reference is None:
            reference = proof
        assert proof == reference
        results[name] = {**sizes, **metrics}
    assert results["after"]["json_parameter_bytes"] < results["before"]["json_parameter_bytes"] / 10
    assert results["after"]["checkpoint_result_bytes"] < results["before"]["checkpoint_result_bytes"] / 10
    report = {
        "scope": "65 mocked reply pages; identical full inventories, receipts and parents",
        "pairs": 1,
        "network_latency_seconds": 0.01,
        "source_spacing_seconds": 0.25,
        "full_run_measurement": False,
        "results": results,
    }
    output = os.getenv("TRR_CHECKPOINT_TRANSFER_OUTPUT")
    if output:
        with Path(output).open("x") as stream:
            json.dump(report, stream, indent=2)
    print(json.dumps(report))
