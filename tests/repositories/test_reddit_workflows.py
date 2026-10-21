from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from trr_backend.repositories import reddit_workflows as workflows


@contextmanager
def transaction():
    yield object()


@pytest.fixture(autouse=True)
def no_database(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Unexpected database access")

    for name in ("fetch_one", "fetch_all", "execute", "execute_returning"):
        monkeypatch.setattr(workflows.pg, name, forbidden)
    monkeypatch.setattr(workflows.pg, "transaction", transaction)


def settings(**patch):
    return {
        **workflows.DEFAULTS,
        "timezone": "America/New_York",
        "episode_airtimes": ["2026-10-21T20:00:00-04:00"],
        "capture_payload": {"subreddit": "BravoRealHousewives", "period_key": "episode-1"},
        **patch,
    }


def test_read_defaults_do_not_enqueue(monkeypatch):
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: None)
    result = workflows.get_settings(community_id="community", season_id="season")
    assert result["schedule_enabled"] is False
    assert result["ai_enabled"] is False
    assert result["revision"] == 0


@pytest.mark.parametrize(
    "patch",
    [
        {"schedule_enabled": True},
        {"ai_enabled": True},
        {"timezone": "Mars/Nope"},
        {"per_run_token_cap": True},
        {"timezone": "America/New_York", "episode_airtimes": ["2026-10-21T20:00:00"]},
        {"timezone": "America/New_York", "episode_airtimes": ["2026-10-21T20:00:00+00:00"]},
    ],
)
def test_settings_reject_missing_and_inexact_configuration(patch):
    with pytest.raises(ValueError):
        workflows.validate_settings(patch)


def test_settings_validate_complete_opt_in_and_no_hidden_routing():
    configured = workflows.validate_settings(
        settings(
            schedule_enabled=True,
            ai_enabled=True,
            model="chosen-model",
            per_run_token_cap=5000,
            daily_token_budget=10000,
        )
    )
    assert configured["ai_enabled"] is True
    with pytest.raises(ValueError, match="identity"):
        workflows._capture_payload({"community_id": "other"}, "community", "season")


def test_schedule_boundaries_and_no_historical_replay():
    config = settings(schedule_enabled=True)
    air = datetime(2026, 10, 22, 0, tzinfo=UTC)
    for elapsed, cadence in [(-60, "15m"), (360, "15m"), (420, "hourly"), (2880, "hourly"), (4320, "daily")]:
        manifest = workflows.schedule_manifest(config, now=air + timedelta(minutes=elapsed))
        assert len(manifest["due"]) == 1
        assert manifest["due"][0]["cadence"] == cadence
    assert workflows.schedule_manifest(config, now=air - timedelta(hours=2))["due"] == []
    assert workflows.schedule_manifest(config, now=air + timedelta(days=9))["due"] == []
    assert workflows.schedule_manifest(settings(), now=air)["enabled"] is False


def test_stratified_sample_is_stable_bounded_and_represents_posts():
    comments = [
        {"reddit_post_id": f"p{i % 5}", "reddit_comment_id": str(i), "depth": i % 3, "score": i % 20 - 2}
        for i in range(1500)
    ]
    sample = workflows.stratified_sample(comments)
    assert len(sample) == 1000
    assert sample == workflows.stratified_sample(list(reversed(comments)))
    assert {row["reddit_post_id"] for row in sample} == {"p0", "p1", "p2", "p3", "p4"}


def test_deterministic_counts_boundaries_and_source_evidence():
    sources = {
        "posts": [{"reddit_post_id": "p", "title": "Ann and Anne discuss reunion"}],
        "comments": [{"reddit_post_id": "p", "reddit_comment_id": "c", "body": "ANN reunion", "score": 2}],
    }
    result = workflows.deterministic_analysis(sources, settings(cast_terms=["Ann"], topic_terms=["reunion"]))
    assert result["cast"][0]["mentions"] == 2
    assert result["topics"][0]["mentions"] == 2
    assert result["coverage"]["capture_state"] == "unknown"
    assert result["cast"][0]["source_links"] == [
        "https://www.reddit.com/comments/p/_/",
        "https://www.reddit.com/comments/p/_/c/",
    ]


def ai_result(link):
    return {
        "summary": "Cited summary",
        "sentiment": "mixed",
        "source_links": [link],
        "topics": [{"name": "reunion", "sentiment": "neutral", "source_links": [link]}],
        "cast": [],
    }


def test_ai_output_rejects_unknown_sources_and_sentiment():
    result = ai_result("known")
    assert workflows.validate_ai_result(result, {"known"}) == result
    result["topics"][0]["source_links"] = ["invented"]
    with pytest.raises(ValueError, match="source reference"):
        workflows.validate_ai_result(result, {"known"})
    result = ai_result("known")
    result["sentiment"] = "happy"
    with pytest.raises(ValueError, match="sentiment"):
        workflows.validate_ai_result(result, {"known"})


def test_ai_reserves_before_provider_and_reconciles_on_invalid_output(monkeypatch):
    events = []
    monkeypatch.setattr(workflows, "_reserve_budget", lambda *a: events.append("reserve"))
    monkeypatch.setattr(workflows, "_reconcile_budget", lambda job_id, used: events.append(("reconcile", used)))

    def provider(request):
        events.append("provider")
        assert "tools" not in request
        assert request["max_completion_tokens"] < 6000
        assert "author" not in request["messages"][1]["content"]
        return {"usage": {"total_tokens": 42}, "choices": [{"message": {"content": json.dumps(ai_result("invented"))}}]}

    sources = {"posts": [{"reddit_post_id": "p", "title": "reunion", "author": "secret"}], "comments": []}
    with pytest.raises(ValueError, match="source reference"):
        workflows._ai_analysis(
            {"id": "job"},
            settings(ai_enabled=True, model="chosen-model", per_run_token_cap=6000, daily_token_budget=10000),
            sources,
            "token",
            provider,
        )
    assert events == ["reserve", "provider", ("reconcile", 42)]


def test_ai_disabled_never_calls_provider():
    provider = Mock()
    with pytest.raises(ValueError, match="disabled"):
        workflows._ai_analysis({"id": "job"}, settings(), {"posts": [], "comments": []}, "token", provider)
    provider.assert_not_called()


def test_ai_budget_rejects_concurrent_capacity_before_write(monkeypatch):
    monkeypatch.setattr(workflows, "_assert_job_owner", lambda *a: {})
    answers = iter(
        [{"revision": 1, "configuration": {"ai_enabled": True}}, None, {"used_tokens": 8000, "reserved_tokens": 1500}]
    )
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: next(answers))
    execute = Mock()
    monkeypatch.setattr(workflows.pg, "execute", execute)
    with pytest.raises(ValueError, match="exhausted"):
        workflows._reserve_budget(
            {"id": "job", "community_id": "c", "season_id": "s"},
            {"revision": 1, "per_run_token_cap": 1000, "daily_token_budget": 10000},
            "token",
        )
    assert execute.call_count == 1  # Only the insert-if-missing budget row, no reservation.


def test_uncertain_ai_request_keeps_budget_reserved(monkeypatch):
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: {"state": "reserved"})
    writes = Mock()
    monkeypatch.setattr(workflows.pg, "execute", writes)
    workflows._reconcile_budget("job", None)
    assert writes.call_count == 1
    assert "state='uncertain'" in writes.call_args.args[0]


def test_duplicate_workflow_delivery_returns_saved_job(monkeypatch):
    monkeypatch.setattr(workflows.pg, "execute_returning", lambda *a, **k: [])
    monkeypatch.setattr(
        workflows.pg, "fetch_one", lambda *a, **k: {"id": "job", "state": "completed", "result": {"ok": True}}
    )
    assert workflows.execute_job("job")["result"] == {"ok": True}


def test_lifecycle_requires_provider_before_any_mutation():
    with pytest.raises(ValueError, match="not configured"):
        workflows._lifecycle({"payload": {}}, "token", None, None, None)


def test_lifecycle_ignores_absence_and_only_explicit_deletion_redacts(monkeypatch):
    monkeypatch.setattr(workflows, "_load_sources", lambda **k: {"posts": [{"reddit_post_id": "p"}], "comments": []})
    monkeypatch.setattr(workflows, "_assert_job_owner", lambda *a: {})
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: [{"reddit_post_id": "p"}])
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: {"reddit_post_id": "p"})
    monkeypatch.setattr(workflows.pg, "execute", Mock())
    redact = Mock()
    monkeypatch.setattr(workflows, "_redact_source", redact)
    invalidate = Mock()
    monkeypatch.setattr(workflows, "invalidate_analysis", invalidate)
    job = {"id": "job", "community_id": "c", "season_id": "s", "payload": {}}
    result = workflows._lifecycle(
        job,
        "token",
        lambda ids: [{"reddit_post_id": "p", "checked": True, "source": "reddit", "signal": "source_unavailable"}],
        None,
        None,
    )
    assert result["checked"] == 1
    redact.assert_not_called()
    result = workflows._lifecycle(
        job,
        "token",
        lambda ids: [
            {"reddit_post_id": "p", "checked": True, "source": "reddit", "signal": "deleted", "explicit": True}
        ],
        None,
        None,
    )
    assert result["redacted"][0]["signal"] == "deleted"
    redact.assert_called_once()
    invalidate.assert_called_once()


def test_object_deletion_retains_shared_references(monkeypatch):
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: [{"hosted_key": "shared"}, {"hosted_key": "orphan"}])
    answers = iter([{"found": 1}, None])
    monkeypatch.setattr(
        workflows.pg, "fetch_one", lambda query, *a, **k: None if "pg_advisory_xact_lock" in query else next(answers)
    )
    monkeypatch.setattr(workflows.pg, "execute", Mock())
    deleter = Mock()
    assert workflows.drain_object_deletions(object_deleter=deleter) == ["orphan"]
    deleter.assert_called_once_with("orphan")


def test_reclassification_preserves_manual_and_reviewed_matches():
    sources = {
        "posts": [{"reddit_post_id": "manual"}, {"reddit_post_id": "reviewed"}],
        "matches": [
            {"reddit_post_id": "manual", "id": "m", "period_key": "episode"},
            {"reddit_post_id": "reviewed", "id": "r", "period_key": "episode", "admin_approved": False},
        ],
    }
    assert workflows._classification_proposal(sources, settings(), {"manual"}) == []


def test_reclassification_rejects_stale_preview(monkeypatch):
    answers = iter([None, {"source_revision": "stale", "settings_revision": 0}])
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: next(answers))
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: [])
    monkeypatch.setattr(workflows, "_load_sources", lambda **k: {"posts": [], "comments": [], "matches": []})
    with pytest.raises(workflows.WorkflowConflict, match="revisions"):
        workflows.reclassify(community_id="c", season_id="s", apply=True, preview_id="preview")


def test_schedule_does_not_replay_after_seven_days():
    air = datetime(2026, 10, 22, 0, tzinfo=UTC)
    assert (
        workflows.schedule_manifest(settings(schedule_enabled=True), now=air + timedelta(days=7, minutes=1))["due"]
        == []
    )


def test_ai_existing_reservation_blocks_second_provider_attempt(monkeypatch):
    monkeypatch.setattr(workflows, "_assert_job_owner", lambda *a: {})
    answers = iter([{"revision": 1, "configuration": {"ai_enabled": True}}, {"state": "uncertain"}])
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: next(answers))
    with pytest.raises(workflows.WorkflowConflict, match="already reserved"):
        workflows._reserve_budget(
            {"id": "job", "community_id": "c", "season_id": "s"},
            {"revision": 1, "per_run_token_cap": 1000, "daily_token_budget": 10000},
            "token",
        )


def test_save_analysis_rejects_changed_settings_before_persisting(monkeypatch):
    monkeypatch.setattr(workflows, "_assert_job_owner", lambda *a: {})
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: {"revision": 2})
    sources = {"posts": [], "comments": [], "matches": []}
    monkeypatch.setattr(workflows, "_load_sources", lambda **k: sources)
    with pytest.raises(workflows.WorkflowConflict, match="settings changed"):
        workflows._save_analysis(
            {"id": "job", "community_id": "c", "season_id": "s", "analysis_settings_revision": 1}, "token", sources, {}
        )


def test_legacy_lifecycle_remains_unenrolled_without_explicit_request(monkeypatch):
    monkeypatch.setattr(
        workflows, "_load_sources", lambda **k: {"posts": [{"reddit_post_id": "legacy"}], "comments": []}
    )
    monkeypatch.setattr(workflows, "_assert_job_owner", lambda *a: {})
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: [])
    writes = Mock()
    monkeypatch.setattr(workflows.pg, "execute", writes)
    provider = Mock()
    result = workflows._lifecycle(
        {"id": "job", "community_id": "c", "season_id": "s", "payload": {}}, "token", provider, None, None
    )
    provider.assert_not_called()
    assert result["enrolled"] == 0
    assert writes.call_count == 1


def test_ai_output_reconciles_unknown_usage_without_releasing_budget(monkeypatch):
    monkeypatch.setattr(workflows, "_reserve_budget", lambda *a: None)
    reconciliation = Mock()
    monkeypatch.setattr(workflows, "_reconcile_budget", reconciliation)
    sources = {"posts": [{"reddit_post_id": "p", "title": "reunion"}], "comments": []}

    def provider(request):
        return {"choices": [{"message": {"content": json.dumps(ai_result("https://www.reddit.com/comments/p/_/"))}}]}

    with pytest.raises(ValueError, match="usage"):
        workflows._ai_analysis(
            {"id": "job"},
            settings(ai_enabled=True, model="chosen-model", per_run_token_cap=6000, daily_token_budget=10000),
            sources,
            "token",
            provider,
        )
    reconciliation.assert_called_once_with("job", None)


def test_schedule_job_links_capture_run_in_shared_transaction(monkeypatch):
    from trr_backend.repositories import reddit_capture

    air = datetime(2026, 10, 22, 0, tzinfo=UTC)
    monkeypatch.setattr(workflows, "_now", lambda: air)
    config = settings(schedule_enabled=True)
    slot = workflows.schedule_manifest(config, now=air)["due"][0]
    job = {
        "id": "job",
        "community_id": "c",
        "season_id": "s",
        "kind": "schedule_capture",
        "payload": {**slot, "settings_revision": 1},
    }
    monkeypatch.setattr(workflows.pg, "execute_returning", lambda *a, **k: [job])
    monkeypatch.setattr(workflows, "get_settings", lambda **k: {**config, "revision": 1})
    monkeypatch.setattr(workflows, "_assert_job_owner", lambda *a: {})
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: {"revision": 1, "configuration": config})
    writes = Mock()
    monkeypatch.setattr(workflows.pg, "execute", writes)
    create = Mock(return_value={"id": "capture-run"})
    monkeypatch.setattr(reddit_capture, "create_run", create)
    result = workflows.execute_job("job")
    assert result["run_id"] == "capture-run"
    assert create.call_args.args[0]["community_id"] == "c"
    assert create.call_args.args[0]["season_id"] == "s"
    assert create.call_args.args[0]["refresh_policy"] == "stale"
    assert create.call_args.args[0]["force_rescrape"] is False
    assert create.call_args.args[0]["stale_after_seconds"] == 900
    assert create.call_args.args[0]["period_start"] == (air - timedelta(hours=1)).isoformat()
    assert create.call_args.args[0]["period_end"] == air.isoformat()
    assert writes.call_count == 1
    assert "capture-run" in writes.call_args.args[1][0]


def test_schedule_job_rechecks_disabled_settings_before_capture(monkeypatch):
    from trr_backend.repositories import reddit_capture

    monkeypatch.setattr(workflows, "get_settings", lambda **k: {**settings(), "revision": 2})
    monkeypatch.setattr(workflows, "_assert_job_owner", lambda *a: {})
    monkeypatch.setattr(
        workflows.pg,
        "execute_returning",
        lambda *a, **k: [
            {
                "id": "job",
                "community_id": "c",
                "season_id": "s",
                "kind": "schedule_capture",
                "payload": {"settings_revision": 1, "slot_key": "old"},
            }
        ],
    )
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: {"revision": 2, "configuration": settings()})
    monkeypatch.setattr(workflows.pg, "execute", Mock())
    create = Mock()
    monkeypatch.setattr(reddit_capture, "create_run", create)
    with pytest.raises(workflows.WorkflowConflict):
        workflows.execute_job("job")
    create.assert_not_called()


def test_redaction_erases_canonical_raw_and_diagnostic_copies(monkeypatch):
    writes = []
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: None)
    monkeypatch.setattr(workflows.pg, "execute", lambda query, params, **k: writes.append((query, params)))
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: [])
    workflows._redact_source("p", "comment", "deleted", object())
    tables = {query.split("update ")[1].split(" set")[0] for query, _ in writes if query.startswith("update ")}
    assert {
        "social.reddit_posts",
        "social.reddit_comments",
        "social.reddit_refresh_runs",
        "social.reddit_analysis_results",
        "social.reddit_workflow_jobs",
        "social.reddit_work_targets",
        "social.reddit_capture_state",
        "social.reddit_episode_discovery_jobs",
    } <= tables
    assert any("body_html=null" in query and "raw_payload='{}'" in query for query, _ in writes)


def test_comment_tombstones_filter_deleted_comments_and_deleted_post_reimports(monkeypatch):
    answers = iter([[{"id": "removed"}], [{"reddit_post_id": "deleted-post"}]])
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: next(answers))
    rows = [
        {"reddit_comment_id": "removed", "reddit_post_id": "p", "body": "sensitive"},
        {"reddit_comment_id": "new", "reddit_post_id": "deleted-post", "body": "sensitive"},
        {"reddit_comment_id": "survives", "reddit_post_id": "p", "body": "body"},
    ]
    assert workflows.filter_comment_tombstones(rows) == [rows[2]]


def test_deleted_author_reimport_scrubs_identity_and_preserves_body(monkeypatch):
    author_hash = workflows._author_hash("OldAccount")
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: [{"author_hash": author_hash}])
    row = {
        "author": "oldaccount",
        "reddit_comment_id": "c",
        "body": "surviving body",
        "raw_payload": {"author": "oldaccount", "body": "surviving body", "nested": {"author_fullname": "t2_secret"}},
    }
    scrubbed = workflows.scrub_tombstoned_authors([row])[0]
    assert scrubbed["author"] is None
    assert scrubbed["body"] == "surviving body"
    assert scrubbed["raw_payload"] == {"body": "surviving body", "nested": {}}
    assert row["author"] == "oldaccount"


def test_account_deleted_redaction_keeps_canonical_body_and_scrubs_all_stored_identity(monkeypatch):
    monkeypatch.setattr(workflows, "_account_affected_ids", lambda *a: {"p", "other"})
    monkeypatch.setattr(workflows, "lock_content", Mock())
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: {"author": "DeletedAccount"})
    answers = iter(
        [
            [{"reddit_post_id": "p", "raw_payload": {"author": "DeletedAccount", "selftext": "survives"}}],
            [
                {
                    "reddit_post_id": "other",
                    "reddit_comment_id": "c",
                    "raw_payload": {"author": "DeletedAccount", "body": "survives"},
                }
            ],
        ]
    )
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: next(answers))
    writes = Mock()
    monkeypatch.setattr(workflows.pg, "execute", writes)
    snapshots = Mock()
    monkeypatch.setattr(workflows, "_redact_snapshots", snapshots)
    monkeypatch.setattr(workflows, "invalidate_analysis", Mock())
    affected = workflows._redact_author("p", None, object())
    assert affected == {"p", "other"}
    assert snapshots.call_count == 2
    queries = [call.args[0] for call in writes.call_args_list]
    assert all("body=" not in query and "title=" not in query and "selftext=" not in query for query in queries)
    author_tombstone = writes.call_args_list[0].args[1]
    assert author_tombstone[0] != "DeletedAccount"
    assert "DeletedAccount" not in author_tombstone[1]


def test_content_lock_order_is_stable_and_uses_shared_parent_key(monkeypatch):
    fetch = Mock()
    monkeypatch.setattr(workflows.pg, "fetch_one", fetch)
    conn = object()
    workflows.lock_content(post_ids=["z", "a", "z"], conn=conn)
    assert [call.args[1][0] for call in fetch.call_args_list] == ["reddit-content:a", "reddit-content:z"]
    assert all(call.kwargs["conn"] is conn for call in fetch.call_args_list)


def test_scheduled_analysis_needs_separate_opt_in_and_uses_utc_day(monkeypatch):
    config = settings(
        ai_enabled=True,
        scheduled_analysis_enabled=True,
        model="chosen-model",
        per_run_token_cap=6000,
        daily_token_budget=10000,
    )
    answers = iter([[{"community_id": "c", "season_id": "s", "revision": 1, "configuration": config}], [], []])
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: next(answers))
    enqueue = Mock()
    monkeypatch.setattr(workflows, "enqueue_job", enqueue)
    workflows.due_dispatcher(now=datetime(2026, 10, 22, 0, tzinfo=UTC))
    assert enqueue.call_count == 1
    assert enqueue.call_args.kwargs["kind"] == "ai_analysis"
    assert enqueue.call_args.kwargs["idempotency_key"] == "daily-ai:2026-10-22"
    assert enqueue.call_args.kwargs["payload"]["scheduled"] is True


def test_invalidated_analysis_job_hides_stale_derived_output(monkeypatch):
    monkeypatch.setattr(
        workflows.pg,
        "fetch_one",
        lambda *a, **k: {"id": "job", "state": "completed", "analysis_valid": False, "result": {"summary": "stale"}},
    )
    job = workflows.get_job("job")
    assert job["result"] is None
    assert job["analysis_invalidated"] is True


def test_reclassification_reports_membership_transitions_without_moving_assignments():
    matches = [
        {
            "id": key,
            "reddit_post_id": key,
            "period_key": "episode-1",
            "is_show_match": before,
            "passes_flair_filter": False,
        }
        for key, before in (("added", False), ("removed", True), ("same", True), ("manual", False))
    ]
    proposal = [
        {
            "match_id": key,
            "before": {"is_show_match": before, "passes_flair_filter": False},
            "after": {"is_show_match": after, "passes_flair_filter": False},
        }
        for key, before, after in (("added", False, True), ("removed", True, False))
    ]
    report = workflows._classification_summary({"matches": matches}, proposal, {"manual"})
    assert report["counts"] == {"added": 1, "removed": 1, "moved": 0, "unchanged": 2}
    assert report["moved"] == []
    assert report["unchanged"][1]["preserved_reason"] == "manual_thread"
    assert report["added"][0]["period_key"] == "episode-1"


def episode_sources():
    return {
        "posts": [
            {"reddit_post_id": "a", "num_comments": 1, "score": 4},
            {"reddit_post_id": "b", "num_comments": 1, "score": 7},
        ],
        "comments": [
            {"reddit_post_id": "a", "reddit_comment_id": "ca", "body": "Ann"},
            {"reddit_post_id": "b", "reddit_comment_id": "cb", "body": "Ann"},
        ],
        "matches": [
            {
                "reddit_post_id": "a",
                "period_key": "episode-1",
                "period_start": "2026-10-21T00:00:00Z",
                "period_end": "2026-10-22T00:00:00Z",
            },
            {
                "reddit_post_id": "b",
                "period_key": "episode-2",
                "period_start": "2026-10-22T00:00:00Z",
                "period_end": "2026-10-23T00:00:00Z",
            },
        ],
        "capture": [{"reddit_post_id": pid, "phase": "comments", "state": "committed"} for pid in ("a", "b")],
    }


def test_deterministic_episode_comparisons_use_saved_counts_and_boundaries():
    result = workflows.deterministic_analysis(episode_sources(), settings(cast_terms=["Ann"]))
    assert len(result["episodes"]) == 2
    assert result["episodes"][0]["saved_comments"] == result["episodes"][0]["reported_comments"] == 1
    comparison = result["episode_comparisons"][0]
    assert comparison["comparable"] is True
    assert comparison["deltas"]["score"] == 3
    assert len(comparison["source_links"]) == 2


def test_episode_comparison_preserves_uncertainty_for_missing_comments_and_different_windows():
    sources = episode_sources()
    sources["comments"] = sources["comments"][:1]
    sources["capture"] = sources["capture"][:1]
    sources["matches"][1]["period_end"] = "2026-10-24T00:00:00Z"
    comparison = workflows._episode_comparisons(sources, sources["comments"])["episode_comparisons"][0]
    assert comparison["comparable"] is False
    assert {"comments_capture_unknown", "saved_below_reported_comments", "different_window_duration"} <= set(
        comparison["reasons"]
    )


def comparison_manifests():
    return [
        {
            "run_id": run_id,
            "configuration": {"period_start": "2026-10-21T00:00:00Z", "period_end": "2026-10-22T00:00:00Z"},
            "requested_phases": ["discovery", "comments", "media"],
            "coverage": {"state": "complete"},
        }
        for run_id in ("current", "baseline")
    ]


def test_run_observations_compare_metrics_repairs_new_posts_and_unavailability(monkeypatch):
    rows = [
        {
            "run_id": "baseline",
            "reddit_post_id": "a",
            "score": 1,
            "num_comments": 5,
            "saved_comments": 2,
            "comments_state": "retry_wait",
            "media_state": "committed",
        },
        {
            "run_id": "current",
            "reddit_post_id": "a",
            "score": 4,
            "num_comments": 5,
            "saved_comments": 5,
            "comments_state": "committed",
            "media_state": "source_unavailable",
        },
        {
            "run_id": "current",
            "reddit_post_id": "new",
            "score": 1,
            "num_comments": 0,
            "saved_comments": 0,
            "comments_state": "committed",
            "media_state": "committed",
        },
    ]
    answers = iter([comparison_manifests(), rows])
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: next(answers))
    result = workflows._run_comparison_evidence(
        "current", "baseline", {"contract_version": 2, "total_rows": 2}, {"contract_version": 2, "total_rows": 1}
    )
    assert result["new_posts"] == ["new"]
    assert result["changed_metrics"][0]["metrics"]["saved_comments"]["delta"] == 3
    assert result["repaired_gaps"][0]["phase"] == "comments"
    assert result["newly_unavailable"][0]["phase"] == "media"
    assert result["comparable_coverage"]["comparable"] is False
    assert "baseline_comments_unresolved" in result["comparable_coverage"]["reasons"]


def test_run_comparison_normalizes_offsets_and_rejects_unknown_legacy_evidence(monkeypatch):
    manifests = comparison_manifests()
    manifests[1]["configuration"] = {
        "period_start": "2026-10-20T20:00:00-04:00",
        "period_end": "2026-10-21T20:00:00-04:00",
    }
    answers = iter([manifests, []])
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: next(answers))
    result = workflows._run_comparison_evidence("current", "baseline", {"contract_version": 2}, {"contract_version": 2})
    assert result["comparable_coverage"]["comparable"] is True
    answers = iter([[], []])
    result = workflows._run_comparison_evidence("current", "baseline", {"contract_version": 2}, {"contract_version": 1})
    assert result["observation_comparison_known"] is False
    assert result["new_posts"] == []
    assert "legacy_or_unknown_observation_evidence" in result["comparable_coverage"]["reasons"]


def test_account_redaction_queries_exclude_unenrolled_legacy_content(monkeypatch):
    queries = []
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: {"author": "Account"})

    def enrolled_lookup(query, *args, **kwargs):
        queries.append(query)
        # The simulated account has one enrolled and one untouched legacy post.
        return (
            [{"reddit_post_id": "enrolled"}]
            if "reddit_lifecycle_enrollment" in query
            else [{"reddit_post_id": "legacy"}]
        )

    monkeypatch.setattr(workflows.pg, "fetch_all", enrolled_lookup)
    assert workflows._account_affected_ids("enrolled", None, object()) == {"enrolled"}
    assert len(queries) == 1


def test_object_lock_is_held_before_reference_check_and_external_delete(monkeypatch):
    events = []
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: [{"hosted_key": "reddit/orphan"}])

    def fetch(query, params, **kwargs):
        events.append(("lock", params[0]) if "pg_advisory_xact_lock" in query else ("references", params[0]))
        return None

    monkeypatch.setattr(workflows.pg, "fetch_one", fetch)
    monkeypatch.setattr(workflows.pg, "execute", Mock())
    workflows.drain_object_deletions(object_deleter=lambda key: events.append(("delete", key)))
    assert events == [
        ("lock", "reddit-object:reddit/orphan"),
        ("references", "reddit/orphan"),
        ("delete", "reddit/orphan"),
    ]


def test_shared_legacy_object_failure_remains_visible_in_inventory(monkeypatch):
    class SharedLegacyObjectError(RuntimeError):
        pass

    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: [{"hosted_key": "shared/legacy"}])
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: None)
    writes = Mock()
    monkeypatch.setattr(workflows.pg, "execute", writes)

    def deleter(key):
        raise SharedLegacyObjectError("reference verification required")

    assert workflows.drain_object_deletions(object_deleter=deleter) == []
    assert writes.call_args.args[1][0] == "SharedLegacyObjectError"
    assert "state='deleted'" not in writes.call_args.args[0]


def test_legacy_inventory_includes_scoped_unresolved_object_errors_without_enqueuing(monkeypatch):
    pending = [{"hosted_key": "shared/legacy", "error": "SharedLegacyObjectError", "state": "pending"}]
    answers = iter([[], pending])
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: next(answers))
    inventory = workflows.legacy_inventory(community_id="c", season_id="s")
    assert inventory["posts"] == []
    assert inventory["object_deletions"] == pending


def test_run_comparison_does_not_return_legacy_text_snapshots(monkeypatch):
    runs = [
        {
            "id": run_id,
            "community_id": "c",
            "season_id": "s",
            "contract_version": 1,
            "diagnostics": {"body": "private obsolete snapshot"},
        }
        for run_id in ("current", "baseline")
    ]
    answers = iter([runs, [], []])
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: next(answers))
    result = workflows.compare_runs("current", "baseline")
    assert "private obsolete snapshot" not in json.dumps(result)
    assert result["observation_comparison_known"] is False


@pytest.fixture
def reviewed_ai_evaluation_cases():
    # Synthetic fixed labels, submitted for lead review; exercise the evaluator without paid inference.
    fixtures = [
        (
            "sarcasm",
            "Oh great, another reunion where nobody answers a question. Fantastic.",
            "negative",
            "The comment uses sarcasm to criticize the reunion.",
            [],
        ),
        (
            "mixed_sentiment",
            "I loved the reunion outfits, but the discussion was frustrating.",
            "mixed",
            "The comment praises outfits and criticizes the discussion.",
            [],
        ),
        (
            "cast_ambiguity",
            "Alex was right, but I cannot tell which Alex the thread means.",
            "uncertain",
            "The named person is ambiguous, so no cast identity is assigned.",
            [],
        ),
        (
            "unsupported_claims",
            "The clip does not say whether anyone broke up.",
            "uncertain",
            "The comment does not establish any relationship outcome.",
            ["confirmed breakup", "confirmed divorce"],
        ),
    ]
    cases = []
    for category, text, sentiment, summary, unsupported in fixtures:
        link = f"https://www.reddit.com/comments/eval/_/{category}/"
        output = ai_result(link)
        output.update(summary=summary, sentiment=sentiment, topics=[], cast=[])
        cases.append(
            {
                "id": category,
                "category": category,
                "sources": [{"source_link": link, "text": text}],
                "expected": {"sentiments": [sentiment], "allowed_cast_names": [], "unsupported_phrases": unsupported},
                "output": output,
            }
        )
    return cases


def test_fixed_reviewed_ai_harness_measures_four_categories_without_inference(
    monkeypatch, reviewed_ai_evaluation_cases
):
    provider = Mock(side_effect=AssertionError("No paid or live inference in offline evaluation"))
    monkeypatch.setattr(workflows, "_openai_provider", provider)
    report = workflows.evaluate_ai_outputs(reviewed_ai_evaluation_cases)
    assert report["evaluation_mode"] == "offline_fixture_outputs"
    assert report["total"] == report["passed"] == 4
    assert report["pass_rate"] == 1.0
    assert set(report["rates"].values()) == {1.0}
    assert {case["category"] for case in report["cases"]} == {
        "sarcasm",
        "mixed_sentiment",
        "cast_ambiguity",
        "unsupported_claims",
    }
    provider.assert_not_called()


def test_ai_evaluation_detects_label_reference_cast_and_claim_regressions(reviewed_ai_evaluation_cases):
    cases = reviewed_ai_evaluation_cases
    cases[0]["output"]["sentiment"] = "positive"  # Literal reading misses labeled sarcasm.
    cases[1]["output"]["source_links"] = ["https://example.invalid/invented"]
    cases[2]["output"]["cast"] = [
        {"name": "Alex Example", "sentiment": "positive", "source_links": cases[2]["output"]["source_links"]}
    ]
    cases[3]["output"]["summary"] = "There was a confirmed breakup."
    report = workflows.evaluate_ai_outputs(cases)
    assert report["passed"] == 0
    assert report["rates"]["sentiment_agrees"] == 0.75
    assert report["rates"]["references_valid"] == 0.75
    assert report["rates"]["cast_resolution_valid"] == 0.75
    assert report["rates"]["fixture_claims_supported"] == 0.75


def test_comment_sample_stratifies_by_post_and_time_quartile():
    start = datetime(2026, 10, 1, tzinfo=UTC)
    rows = [
        {
            "reddit_post_id": post_id,
            "reddit_comment_id": f"{post_id}{hour}",
            "score": 1,
            "depth": 0,
            "created_at_utc": (start + timedelta(hours=hour)).isoformat(),
        }
        for post_id in ("a", "b")
        for hour in range(8)
    ]
    sample = workflows.stratified_sample(rows, limit=8)
    assert len(sample) == 8
    assert sample == workflows.stratified_sample(list(reversed(rows)), limit=8)
    strata = {
        (row["reddit_post_id"], min(3, int(4 * (_dt - start).total_seconds() / (7 * 3600))))
        for row in sample
        for _dt in [datetime.fromisoformat(row["created_at_utc"])]
    }
    assert strata == {(pid, quartile) for pid in ("a", "b") for quartile in range(4)}


def test_overlapping_episode_schedule_slots_coalesce_with_fastest_cadence():
    config = settings(
        schedule_enabled=True, episode_airtimes=["2026-10-21T20:00:00-04:00", "2026-10-20T20:00:00-04:00"]
    )
    current = datetime(2026, 10, 22, 0, tzinfo=UTC)
    slots = workflows.schedule_manifest(config, now=current)["due"]
    assert len(slots) == 1
    assert slots[0]["coalesced_slots"] == 2
    assert slots[0]["cadence"] == "15m"
    assert len(slots[0]["airtimes"]) == 2
    assert slots[0]["slot_key"].startswith("combined:")
    assert (
        slots
        == workflows.schedule_manifest(
            {**config, "episode_airtimes": list(reversed(config["episode_airtimes"]))}, now=current
        )["due"]
    )


def test_scope_default_empty_selection_loads_all_stored_posts_and_guards_analysis_tombstones(monkeypatch):
    queries = []

    def fetch(query, params, **kwargs):
        queries.append((query, params))
        return []

    monkeypatch.setattr(workflows.pg, "fetch_all", fetch)
    result = workflows._load_sources(community_id="c", season_id="s", post_ids=[], eligible_only=True)
    assert result["posts"] == []
    assert queries[0][1] == ["c", "s", None, None]
    assert "reddit_lifecycle_enrollment" in queries[0][0]
    assert "state in ('deleted','removed')" in queries[0][0]
    assert "m.is_show_match or m.passes_flair_filter" in queries[0][0]


def test_analysis_revision_reuses_stable_inputs_and_changes_with_content_versions_and_vocabulary(monkeypatch):
    sources = episode_sources()
    configuration = {**settings(cast_terms=["Ann Example"]), "revision": 2}
    original = workflows._analysis_revision(sources, configuration, "deterministic_analysis")
    sources["posts"][0]["last_seen_at"] = "later volatile observation"
    sources["posts"][0]["updated_at"] = "later persistence timestamp"
    assert workflows._analysis_revision(sources, configuration, "deterministic_analysis") == original
    changed = {**configuration, "cast_aliases": {"Ann Example": ["Annie"]}}
    assert workflows._analysis_revision(sources, changed, "deterministic_analysis") != original
    sources["comments"][0]["body"] = "changed source text"
    assert workflows._analysis_revision(sources, configuration, "deterministic_analysis") != original
    monkeypatch.setitem(workflows.ANALYSIS_VERSIONS, "prompt", "changed-version")
    assert workflows._analysis_revision(episode_sources(), configuration, "deterministic_analysis") != original


def test_analysis_cache_hit_skips_paid_provider_and_keeps_save_fencing(monkeypatch):
    config = {
        **settings(ai_enabled=True, model="chosen", per_run_token_cap=6000, daily_token_budget=10000),
        "revision": 2,
    }
    job = {"id": "job", "community_id": "c", "season_id": "s", "kind": "ai_analysis", "payload": {"post_ids": []}}
    monkeypatch.setattr(workflows.pg, "execute_returning", lambda *a, **k: [job])
    monkeypatch.setattr(workflows, "get_settings", lambda **k: config)
    monkeypatch.setattr(workflows, "_resolve_analysis_settings", lambda settings, *a: settings)
    monkeypatch.setattr(workflows, "_load_sources", lambda **k: episode_sources())
    monkeypatch.setattr(workflows.pg, "fetch_one", lambda *a, **k: {"result": {"summary": "cached"}})
    saved = Mock(side_effect=lambda job, token, sources, result: result)
    monkeypatch.setattr(workflows, "_save_analysis", saved)
    provider = Mock(side_effect=AssertionError("Cached analysis must not call OpenAI"))
    result = workflows.execute_job("job", ai_provider=provider)
    assert result == {"summary": "cached", "cache_reused": True}
    saved.assert_called_once()
    provider.assert_not_called()


def test_analysis_metadata_reports_versions_window_capture_times_and_count_gap():
    sources = episode_sources()
    sources["capture"][0].update(successful_at="2026-10-22T01:00:00Z", source_revision="source-rev")
    result = workflows.deterministic_analysis(
        sources, settings(cast_terms=["Ann Example"], cast_aliases={"Ann Example": ["Ann"]})
    )
    assert result["cast"][0]["mentions"] == 2
    assert result["analysis_versions"]["topics"] == "1"
    assert result["source_window"]["start"] == "2026-10-21T00:00:00+00:00"
    assert result["source_window"]["end"] == "2026-10-23T00:00:00+00:00"
    assert result["coverage"]["reported_comments"] == result["coverage"]["saved_comments"] == 2
    assert result["capture_timestamps"][0]["successful_at"] == "2026-10-22T01:00:00Z"
    assert result["sentiment_interpretation"] == "not_computed"


def test_cast_alias_ambiguity_is_not_resolved_into_two_people():
    sources = {"posts": [{"reddit_post_id": "p", "title": "Alex was right"}], "comments": []}
    result = workflows.deterministic_analysis(
        sources,
        settings(cast_terms=["Alex One", "Alex Two"], cast_aliases={"Alex One": ["Alex"], "Alex Two": ["Alex"]}),
    )
    assert result["ambiguous_cast_aliases"] == ["alex"]
    assert all(row["mentions"] == 0 for row in result["cast"])


def test_default_analysis_resolves_current_canonical_cast_names_and_stored_aliases(monkeypatch):
    from trr_backend.repositories import reddit_runtime

    monkeypatch.setattr(
        reddit_runtime, "schedule_capture_template", lambda *a: {"cast_names": ["Ann Example"], "subreddit": "bravo"}
    )
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: [{"name": "Ann Example", "aliases": ["Annie"]}])
    resolved = workflows._resolve_analysis_settings({**settings(), "revision": 1}, "c", "s")
    assert resolved["cast_terms"] == ["Ann Example"]
    assert resolved["cast_aliases"] == {"Ann Example": ["Annie"]}
    assert "relationships" in workflows._analysis_vocabulary(resolved)["topics"]


def test_lifecycle_snapshot_invalidation_revokes_episode_job_lease_atomically(monkeypatch):
    writes = Mock()
    monkeypatch.setattr(workflows.pg, "execute", writes)
    conn = object()
    workflows._redact_snapshots("post", conn)
    episode_updates = [
        call for call in writes.call_args_list if "update social.reddit_episode_discovery_jobs" in call.args[0]
    ]
    assert len(episode_updates) == 1
    query, params = episode_updates[0].args
    assert all(
        field in query
        for field in (
            "state='failed'",
            "claim_token=null",
            "lease_expires_at=null",
            "error='lifecycle_invalidated'",
            "result=null",
            "checkpoint='{}'::jsonb",
        )
    )
    assert params == ["post"]
    assert episode_updates[0].kwargs["conn"] is conn


def test_retained_comment_raw_replies_cannot_restore_tombstoned_nested_content(monkeypatch):
    answers = iter([[{"id": "deleted-child"}], []])
    monkeypatch.setattr(workflows.pg, "fetch_all", lambda *a, **k: next(answers))
    original_raw = {
        "body": "surviving parent",
        "score": 2,
        "replies": {
            "data": {"children": [{"kind": "t1", "data": {"id": "deleted-child", "body": "deleted sensitive content"}}]}
        },
    }
    rows = [
        {"reddit_comment_id": "parent", "reddit_post_id": "p", "body": "surviving parent", "raw_payload": original_raw},
        {"reddit_comment_id": "deleted-child", "reddit_post_id": "p", "body": "deleted sensitive content"},
    ]
    retained = workflows.filter_comment_tombstones(rows)
    assert len(retained) == 1
    assert retained[0]["raw_payload"] == {"body": "surviving parent", "score": 2}
    assert "deleted sensitive content" not in json.dumps(retained)
    assert "replies" in original_raw  # Input ownership is preserved.
