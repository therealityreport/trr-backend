"""Offline regression checks for the durable v2 contract and execution boundaries."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import Mock

import pytest

from trr_backend.repositories import reddit_capture as capture
from trr_backend.repositories import reddit_discovery as discovery
from trr_backend.repositories import reddit_execution as execution_module


@contextmanager
def transaction():
    yield object()


@pytest.fixture(autouse=True)
def no_database(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Unexpected database access")

    monkeypatch.setattr(capture, "_one", forbidden)
    for name in ("fetch_all", "fetch_one", "execute", "execute_returning"):
        monkeypatch.setattr(capture.pg, name, forbidden)
    monkeypatch.setattr(capture.pg, "transaction", transaction)


def payload(**patch):
    return {
        "community_id": "c",
        "season_id": "s",
        "period_key": "episode",
        "subreddit": "bravo",
        "show_name": "Housewives",
        "requested_phases": ["media", "comments", "discovery"],
        **patch,
    }


def test_hash_is_order_independent_and_ignores_request_telemetry():
    first = payload(selected_post_ids=["abc", "def"], client_session_id="old")
    second = payload(
        selected_post_ids=["def", "abc", "abc"],
        requested_phases=["comments", "discovery", "media"],
        client_session_id="new",
    )
    assert capture.configuration_hash(first) == capture.configuration_hash(second)


@pytest.mark.parametrize(
    "change",
    [
        {"refresh_policy": "force"},
        {"cast_names": ["New Cast"]},
        {"selected_post_ids": ["abc"]},
        {"requested_phases": ["comments"]},
        {"period_end": "2026-10-22T00:00:00Z"},
    ],
)
def test_hash_changes_for_behavior_inputs(change):
    assert capture.configuration_hash(payload()) != capture.configuration_hash(payload(**change))


def test_force_alias_and_policy_conflict():
    assert capture.normalize_policy({"force_rescrape": True})["refresh_policy"] == "force"
    assert capture.normalize_policy({"refresh_policy": "force"})["force_rescrape"] is True
    with pytest.raises(ValueError, match="conflicts"):
        capture.normalize_policy({"force_rescrape": True, "refresh_policy": "stale"})


def test_unknown_capture_is_due_and_existing_complete_capture_is_reused(monkeypatch):
    monkeypatch.setattr(
        capture.pg,
        "fetch_all",
        lambda *a, **k: [
            {"reddit_post_id": "complete", "state": "committed", "fresh": False},
            {"reddit_post_id": "unavailable", "state": "source_unavailable", "fresh": True},
            {"reddit_post_id": "partial", "state": "retry_wait", "fresh": True},
        ],
    )
    ids = ["complete", "unavailable", "partial", "legacy"]
    assert capture.capture_due(ids, "comments", policy="missing") == {"partial", "legacy"}
    assert capture.capture_due(ids, "comments", policy="stale") == {"complete", "partial", "legacy"}
    assert capture.capture_due(ids, "comments", policy="force") == set(ids)


def test_legacy_contract_never_infers_capture_from_old_completed_flags(monkeypatch):
    monkeypatch.setattr(capture, "manifest_for_run", lambda run_id: None)
    assert capture.run_contract("old") == {
        "contract_version": 1,
        "capture_status": "unknown",
        "coverage": {"state": "unknown"},
    }


def test_terminal_status_requires_no_unresolved_targets_or_coverage_gap():
    assert capture.terminal_status({"unresolved": 0, "coverage": {"state": "complete"}}, useful=True) == "completed"
    assert capture.terminal_status({"unresolved": 1, "coverage": {"state": "complete"}}, useful=True) == "partial"
    assert capture.terminal_status({"unresolved": 0, "coverage": {"state": "unknown"}}, useful=False) == "failed"
    assert capture.terminal_status({"control_state": "cancel_requested"}, useful=True) == "cancelled"


@pytest.mark.parametrize(
    "row",
    [
        None,
        {"status": "running", "claim_token": "wrong", "live": True},
        {"status": "running", "claim_token": "token", "live": False},
        {"status": "completed", "claim_token": "token", "live": True},
    ],
)
def test_owner_fence_rejects_stale_or_nonrunning_claim(monkeypatch, row):
    monkeypatch.setattr(capture, "_one", lambda *a, **k: row)
    with pytest.raises(capture.LostClaim):
        capture.assert_owner("run", "token", conn=object())


@pytest.mark.parametrize("control,error", [("paused", capture.RunPaused), ("cancelled", capture.RunCancelled)])
def test_owner_fence_observes_operator_control(monkeypatch, control, error):
    monkeypatch.setattr(
        capture,
        "_one",
        lambda *a, **k: {"status": "running", "claim_token": "token", "live": True, "control_state": control},
    )
    with pytest.raises(error):
        capture.assert_owner("run", "token", conn=object())


def test_finish_target_checks_owner_before_writing(monkeypatch):
    def lost(*args, **kwargs):
        raise capture.LostClaim("lost")

    monkeypatch.setattr(capture, "assert_owner", lost)
    with pytest.raises(capture.LostClaim):
        capture.finish_target("run", "media", "post", "stale", "committed")


def test_source_unavailable_requires_positive_evidence(monkeypatch):
    monkeypatch.setattr(capture, "assert_owner", lambda *a, **k: {})
    monkeypatch.setattr(capture, "_one", lambda *a, **k: {"id": "target", "attempt_count": 1})
    with pytest.raises(ValueError, match="evidence"):
        capture.finish_target("run", "comments", "post", "token", "source_unavailable")


def test_target_summary_keeps_failed_and_cancelled_work_unresolved():
    summary = capture.summarize_targets(
        [
            {"kind": "comments", "state": "committed", "count": 2},
            {"kind": "comments", "state": "source_unavailable", "count": 1},
            {"kind": "media", "state": "failed", "count": 2},
            {"kind": "media", "state": "cancelled", "count": 1},
        ]
    )
    assert (summary["requested"], summary["satisfied"], summary["unresolved"]) == (6, 3, 3)


def test_discovery_plans_all_exact_flairs_before_optional_terms():
    plan = discovery.query_plan(
        payload(
            analysis_all_flairs=["Episode Discussion"],
            analysis_flairs=["News"],
            force_include_flairs=["Episode Discussion"],
            search_backfill=True,
        )
    )
    assert [query["key"] for query in plan[:2]] == ["flair:Episode Discussion", "flair:News"]
    assert all(query["required"] for query in plan[:2])
    assert any(query["key"].startswith("term:") and not query["required"] for query in plan)


def test_media_references_keep_each_parent_association_and_html_sources():
    refs = execution_module.media_references(
        {
            "reddit_post_id": "p",
            "raw_payload": {
                "url": "https://i.redd.it/a.jpg",
                "preview": {"images": [{"source": {"url": "https://preview.redd.it/a.jpg?x=1&amp;y=2"}}]},
            },
        },
        [
            {"reddit_comment_id": "c1", "body": "https://i.redd.it/a.jpg"},
            {"reddit_comment_id": "c2", "body_html": '<a href="https://i.redd.it/a.jpg">image</a>'},
        ],
    )
    keys = {(row["source_url"], row["reddit_comment_id"]) for row in refs}
    assert ("https://i.redd.it/a.jpg", None) in keys
    assert ("https://i.redd.it/a.jpg", "c1") in keys
    assert ("https://i.redd.it/a.jpg", "c2") in keys
    assert ("https://preview.redd.it/a.jpg?x=1&y=2", None) in keys


def test_upload_failure_keeps_media_capture_unresolved(monkeypatch):
    from trr_backend.media import s3_mirror

    executor = Mock(run_id="run", claim_token="token")
    executor.transaction = transaction
    monkeypatch.setattr(capture, "start_target", lambda *a, **k: {"state": "running"})
    monkeypatch.setattr(capture, "ensure_target", lambda *a, **k: {"state": "pending"})
    monkeypatch.setattr(capture.pg, "fetch_all", lambda *a, **k: [])
    monkeypatch.setattr(capture.pg, "fetch_one", lambda *a, **k: None)
    monkeypatch.setattr(capture, "_one", lambda *a, **k: None)
    finish = Mock()
    record = Mock()
    monkeypatch.setattr(capture, "finish_target", finish)
    monkeypatch.setattr(capture, "record_capture", record)
    monkeypatch.setattr(execution_module.workflows, "enroll_capture", Mock())
    monkeypatch.setattr(
        s3_mirror, "mirror_url_to_s3", lambda *a, **k: Mock(status="failed", hosted_key=None, error="upload failed")
    )
    execution_module.media_capture({"reddit_post_id": "p", "raw_payload": {"url": "https://i.redd.it/a.jpg"}}, executor)
    assert record.call_args.args[2] == "retry_wait"
    assert finish.call_count == 2
    assert all(call.args[4] == "retry_wait" for call in finish.call_args_list)
