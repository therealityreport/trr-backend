from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest
from psycopg2.errors import UndefinedColumn, UndefinedTable

from trr_backend.repositories import social_completion_summary as repository


@pytest.fixture(autouse=True)
def _legacy_payload_read_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repository.payload_sidecars, "payload_read_mode", lambda: "legacy")


def _summary_row(*, total_posts: int, sidecar_present: bool = False) -> dict[str, object]:
    return {
        "total_posts": total_posts,
        "total_reported_comments": total_posts * 10,
        "saved_comments": total_posts * 8,
        "missing_comments": total_posts * 2,
        "accounted_comments": total_posts * 10,
        "comments_finished": total_posts,
        "comments_in_progress": 0,
        "comments_not_started": 0,
        "details_finished": total_posts,
        "details_not_started": 0,
        "media_finished": total_posts,
        "media_in_progress": 0,
        "media_not_started": 0,
        "sidecar_present": sidecar_present,
    }


def test_completion_summary_preserves_contract_and_uses_narrow_profile_pool(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_one(query, params, *, pool_name):
        captured.update(query=query, params=params, pool_name=pool_name)
        return {
            "total_posts": "3",
            "total_reported_comments": "1200",
            "saved_comments": "780",
            "missing_comments": "420",
            "accounted_comments": "1200",
            "comments_finished": "1",
            "comments_in_progress": "2",
            "comments_not_started": "0",
            "details_finished": "2",
            "details_not_started": "1",
            "media_finished": "1",
            "media_in_progress": "1",
            "media_not_started": "1",
        }

    monkeypatch.setattr(repository.pg, "fetch_one", fake_fetch_one)

    payload = repository.get_social_completion_summary(
        platform="instagram",
        account_handle="bravotv",
        year=2026,
    )

    assert payload == {
        "platform": "instagram",
        "handle": "bravotv",
        "year": 2026,
        "total_posts": 3,
        "total_reported_comments": 1200,
        "saved_comments": 780,
        "missing_comments": 420,
        "accounted_comments": 1200,
        "lanes": {
            "comments": {"finished": 1, "in_progress": 2, "not_started": 0},
            "details": {"finished": 2, "in_progress": 0, "not_started": 1},
            "media": {"finished": 1, "in_progress": 1, "not_started": 1},
        },
    }
    assert captured["params"] == ["bravotv", 2026]
    assert captured["pool_name"] == "social_profile"
    sql = str(captured["query"])
    assert "social.instagram_account_catalog_post_collaborators collaborator" in sql
    assert "p.collaborators_detail" in sql
    assert "social.comment_capture_health" not in sql
    assert "social.instagram_post_comment_rollups" in sql
    assert "social.instagram_comments" in sql
    assert "to_jsonb(" not in sql.lower()
    assert sql.count("detail_comments_count") > 1


def test_completion_sql_limits_comment_scans_and_preserves_capture_count_semantics() -> None:
    sql = " ".join(repository._COMPLETION_SUMMARY_SQL.split())
    compatibility_sql = " ".join(repository._COMPLETION_SUMMARY_RAW_COMPAT_SQL.split())

    # The old comment_capture_health view aggregates every Instagram comment
    # (twice in its full definition) before the annual candidate set can join.
    assert "social.comment_capture_health" not in sql
    assert "candidate_post_ids as materialized" in sql

    # The prior comment_capture_health view counted every non-Facebook row as
    # saved, including classified-missing rows. Total minus Facebook preserves
    # that endpoint contract while using the (post_id, phase) index.
    assert "coalesce(r.total_comment_count, 0)::bigint - coalesce(fb.fb_crosspost_count, 0)::bigint" in sql
    assert "c.post_id = candidate.post_id and c.phase = 'fb_crosspost'" in sql
    assert "r.active_comment_count" not in sql

    # Preserve the legacy view's saved-count semantics if a rollup row is absent.
    assert "candidate_rollup.post_id is null" in sql
    assert "count(*) filter (where c.phase is distinct from 'fb_crosspost')" in sql
    assert "else coalesce(fallback.saved_comment_count, 0)::bigint" in sql

    # Payload count fallbacks and scalar Instagram counts remain available, and
    # no whole-row serialization is introduced into the hot path.
    assert "p.comments_count::bigint as instagram_reported_comments" in sql
    assert "greatest(coalesce(p.comments_count, 0), 0)::bigint as detail_comments_count" in sql
    assert "raw_data" not in sql
    assert "payload_counts.edge_media_to_comment ->> 'count'" in compatibility_sql
    assert "jsonb_to_record" in compatibility_sql
    assert "p.raw_data as raw_data" not in sql
    assert "to_jsonb(" not in sql.lower()


def test_completion_legacy_mode_executes_once_without_sidecar_joins(monkeypatch) -> None:
    calls: list[str] = []

    def fake_fetch_one(query, _params, *, pool_name):
        assert pool_name == "social_profile"
        calls.append(str(query))
        return _summary_row(total_posts=1)

    monkeypatch.setattr(repository.pg, "fetch_one", fake_fetch_one)

    payload = repository.get_social_completion_summary(
        platform="instagram",
        account_handle="bravotv",
        year=2026,
    )

    assert payload["total_posts"] == 1
    assert len(calls) == 1
    assert "instagram_account_catalog_post_payloads" not in calls[0]
    assert "instagram_post_payloads" not in calls[0]


def test_completion_sidecar_mode_joins_sidecars_once_and_returns_exact_shape(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(repository.payload_sidecars, "payload_read_mode", lambda: "sidecar")

    def fake_fetch_one(query, _params, *, pool_name):
        assert pool_name == "social_profile"
        calls.append(str(query))
        return _summary_row(total_posts=2, sidecar_present=True)

    monkeypatch.setattr(repository.pg, "fetch_one", fake_fetch_one)

    payload = repository.get_social_completion_summary(
        platform="instagram",
        account_handle="bravotv",
        year=2026,
    )

    assert payload == {
        "platform": "instagram",
        "handle": "bravotv",
        "year": 2026,
        "total_posts": 2,
        "total_reported_comments": 20,
        "saved_comments": 16,
        "missing_comments": 4,
        "accounted_comments": 20,
        "lanes": {
            "comments": {"finished": 2, "in_progress": 0, "not_started": 0},
            "details": {"finished": 2, "in_progress": 0, "not_started": 0},
            "media": {"finished": 2, "in_progress": 0, "not_started": 0},
        },
    }
    assert len(calls) == 1
    assert "left join lateral (select payload.catalog_post_id" in calls[0]
    assert "payload.catalog_post_id, payload.raw_data" not in calls[0]
    assert "where payload.catalog_post_id = cp.id limit 1) cp_payload on true" in calls[0]
    assert "left join lateral (select payload.post_id" in calls[0]
    assert "payload.post_id, payload.raw_data" not in calls[0]
    assert "where payload.post_id = p.id limit 1) p_payload on true" in calls[0]


def test_completion_sidecar_sql_uses_bounded_primary_key_payload_probes() -> None:
    sql = " ".join(repository._COMPLETION_SUMMARY_SIDECAR_SQL.split())

    # A conventional bulk join makes PostgreSQL hash the fat JSON payload table
    # and spill it to temporary blocks. Each sidecar is one-to-one by primary
    # key, so bounded lateral probes preserve the exact row contract without
    # building either whole-table payload hash.
    assert "left join lateral (select payload.catalog_post_id" in sql
    assert "where payload.catalog_post_id = cp.id limit 1) cp_payload on true" in sql
    assert "left join lateral (select payload.post_id" in sql
    assert "where payload.post_id = p.id limit 1) p_payload on true" in sql
    assert "left join social.instagram_account_catalog_post_payloads" not in sql
    assert "left join social.instagram_post_payloads" not in sql


def test_completion_sidecar_sql_keeps_empty_sidecars_authoritative_and_only_falls_back_for_nulls() -> None:
    sql = " ".join(repository._COMPLETION_SUMMARY_RAW_COMPAT_SIDECAR_SQL.split())

    assert "coalesce(cp_payload.raw_data, cp.raw_data)" in sql
    assert "coalesce(p_payload.raw_data, p.raw_data)" in sql
    assert "nullif(cp_payload.raw_data" not in sql
    assert "nullif(p_payload.raw_data" not in sql
    assert "coalesce(cp_payload.raw_data, cp.raw_data) -> 'collaborators_detail'" in sql
    assert "jsonb_typeof(coalesce(p_payload.raw_data, p.raw_data)) = 'object'" in sql
    assert "nullif(coalesce(p_payload.raw_data, p.raw_data) -> 'collaborators_detail', '[]'::jsonb)" in sql


def test_completion_sql_extracts_post_counts_before_materializing_filtered_rows() -> None:
    for query in (
        repository._COMPLETION_SUMMARY_RAW_COMPAT_SQL,
        repository._COMPLETION_SUMMARY_RAW_COMPAT_SIDECAR_SQL,
    ):
        sql = " ".join(query.split())

        # Parse each selected payload into narrow count fields while the source
        # row is in scope. The filtered CTE must not carry the fat JSON payload
        # into another materialized scan and repeatedly detoast it there.
        assert "jsonb_to_record(" in sql
        assert "as payload_counts(" in sql
        assert "as media_counts(" in sql
        assert "as metrics_counts(" in sql
        assert "p.detail_comments_count" in sql
        assert "p.raw_data as raw_data" not in sql
        assert "p_payload.raw_data, p.raw_data) as raw_data" not in sql


def test_completion_current_schema_sql_never_reads_raw_payload_columns() -> None:
    for query in (repository._COMPLETION_SUMMARY_SQL, repository._COMPLETION_SUMMARY_SIDECAR_SQL):
        sql = " ".join(query.split())

        assert "raw_data" not in sql
        assert "jsonb_to_record" not in sql
        assert "greatest(coalesce(p.comments_count, 0), 0)::bigint as detail_comments_count" in sql


def test_completion_older_year_keeps_raw_compatible_historical_semantics(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        repository.pg,
        "fetch_one",
        lambda query, _params, *, pool_name: calls.append(str(query)) or _summary_row(total_posts=1),
    )

    repository.get_social_completion_summary(
        platform="instagram",
        account_handle="bravotv",
        year=2024,
    )

    assert len(calls) == 1
    assert "raw_data" in calls[0]
    assert "jsonb_to_record" in calls[0]


def test_completion_current_schema_missing_column_retries_raw_compatible_once(monkeypatch) -> None:
    calls: list[str] = []

    def fake_fetch_one(query, _params, *, pool_name):
        calls.append(str(query))
        if len(calls) == 1:
            raise UndefinedColumn("current typed column missing")
        return _summary_row(total_posts=1)

    monkeypatch.setattr(repository.pg, "fetch_one", fake_fetch_one)

    payload = repository.get_social_completion_summary(
        platform="instagram",
        account_handle="bravotv",
        year=2026,
    )

    assert payload["total_posts"] == 1
    assert len(calls) == 2
    assert "raw_data" not in calls[0]
    assert "raw_data" in calls[1]


def test_completion_compare_sampled_returns_legacy_and_logs_one_mismatch(monkeypatch) -> None:
    calls: list[str] = []
    logged: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(repository.payload_sidecars, "payload_read_mode", lambda: "compare")
    monkeypatch.setattr(repository.payload_compare, "should_sample_payload_compare", lambda **_kwargs: True)

    def fake_fetch_one(query, _params, *, pool_name):
        assert pool_name == "social_profile"
        calls.append(str(query))
        return _summary_row(total_posts=1 if len(calls) == 1 else 2, sidecar_present=len(calls) == 2)

    monkeypatch.setattr(repository.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(
        repository.logger,
        "info",
        lambda message, *, extra: logged.append((message, extra)),
    )

    payload = repository.get_social_completion_summary(
        platform="instagram",
        account_handle="bravotv",
        year=2026,
    )

    assert payload["total_posts"] == 1
    assert len(calls) == 2
    assert "instagram_post_payloads" not in calls[0]
    assert "instagram_post_payloads" in calls[1]
    assert len(logged) == 1
    event = logged[0][1]["payload_compare"]
    assert isinstance(event, dict)
    assert event["classification"] == "required"
    assert event["sidecar_present"] is True
    assert event["schema_unavailable"] is False


def test_completion_compare_log_message_is_searchable_bounded_and_raw_value_free(monkeypatch) -> None:
    secret_identity = "private_handle_that_must_not_appear"
    secret_legacy_value = "legacy_payload_value_that_must_not_appear"
    secret_sidecar_value = "sidecar_payload_value_that_must_not_appear"
    secret_trace_id = "request_supplied_trace_that_must_not_appear" * 100
    logged: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(
        repository.logger,
        "info",
        lambda message, *, extra: logged.append((message, extra)),
    )
    monkeypatch.setattr(repository.payload_compare, "_get_trace_id", lambda: secret_trace_id)

    repository._log_payload_compare_event(
        identity={"handle": secret_identity, "year": 2026},
        legacy_payload={"private_field": secret_legacy_value},  # type: ignore[arg-type]
        new_payload={"private_field": secret_sidecar_value},
        sidecar_present=True,
        schema_unavailable=False,
    )

    assert len(logged) == 1
    message, extra = logged[0]
    assert message.startswith(repository.COMPLETION_COMPARE_LOG_PREFIX)
    visible_event = json.loads(message.removeprefix(repository.COMPLETION_COMPARE_LOG_PREFIX))
    assert visible_event == extra["payload_compare"]
    assert visible_event["classification"] == "required"
    assert visible_event["required_count"] == 1
    assert visible_event["benign_count"] == 0
    assert visible_event["surface"] == repository.COMPLETION_COMPARE_SURFACE
    assert "trace_id" not in visible_event
    assert len(visible_event["trace_id_hash"]) == 64
    assert len(visible_event["entity_identity_hash"]) == 64
    assert len(visible_event["legacy_payload_hash"]) == 64
    assert len(visible_event["new_payload_hash"]) == 64
    assert len(visible_event["mismatches"]) <= repository.payload_compare.MAX_MISMATCH_RECORDS
    serialized_log = json.dumps(logged, sort_keys=True)
    assert secret_identity not in serialized_log
    assert secret_legacy_value not in serialized_log
    assert secret_sidecar_value not in serialized_log
    assert secret_trace_id not in serialized_log


def test_completion_compare_sampled_treats_empty_sidecar_result_as_required_mismatch(monkeypatch) -> None:
    calls: list[str] = []
    logged: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(repository.payload_sidecars, "payload_read_mode", lambda: "compare")
    monkeypatch.setattr(repository.payload_compare, "should_sample_payload_compare", lambda **_kwargs: True)

    def fake_fetch_one(query, _params, *, pool_name):
        assert pool_name == "social_profile"
        calls.append(str(query))
        if len(calls) == 1:
            return _summary_row(total_posts=3)
        return _summary_row(total_posts=0, sidecar_present=True)

    monkeypatch.setattr(repository.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(
        repository.logger,
        "info",
        lambda message, *, extra: logged.append((message, extra)),
    )

    payload = repository.get_social_completion_summary(
        platform="instagram",
        account_handle="bravotv",
        year=2026,
    )

    assert payload["total_posts"] == 3
    assert len(calls) == 2
    event = logged[0][1]["payload_compare"]
    assert isinstance(event, dict)
    assert event["classification"] == "required"
    assert event["sidecar_present"] is True


def test_completion_compare_unsampled_executes_legacy_once(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(repository.payload_sidecars, "payload_read_mode", lambda: "compare")
    monkeypatch.setattr(repository.payload_compare, "should_sample_payload_compare", lambda **_kwargs: False)
    monkeypatch.setattr(
        repository.pg,
        "fetch_one",
        lambda query, _params, *, pool_name: calls.append(str(query)) or _summary_row(total_posts=1),
    )

    payload = repository.get_social_completion_summary(
        platform="instagram",
        account_handle="bravotv",
        year=2026,
    )

    assert payload["total_posts"] == 1
    assert len(calls) == 1
    assert "instagram_post_payloads" not in calls[0]


def test_completion_sidecar_schema_missing_logs_hashed_event_and_retries_legacy_once(monkeypatch) -> None:
    secret_handle = "private_handle_8492"
    calls: list[str] = []
    logged: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(repository.payload_sidecars, "payload_read_mode", lambda: "sidecar")

    def fake_fetch_one(query, _params, *, pool_name):
        assert pool_name == "social_profile"
        calls.append(str(query))
        if "instagram_post_payloads" in str(query):
            raise UndefinedTable("sidecar table missing")
        return _summary_row(total_posts=1)

    monkeypatch.setattr(repository.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(
        repository.logger,
        "info",
        lambda message, *, extra: logged.append((message, extra)),
    )

    payload = repository.get_social_completion_summary(
        platform="instagram",
        account_handle=secret_handle,
        year=2026,
    )

    assert payload["total_posts"] == 1
    assert len(calls) == 3
    assert "instagram_post_payloads" in calls[0]
    assert "instagram_post_payloads" in calls[1]
    assert "instagram_post_payloads" not in calls[2]
    assert len(logged) == 1
    serialized = json.dumps(logged, sort_keys=True)
    assert secret_handle not in serialized
    assert logged[0][1]["payload_compare"]["schema_unavailable"] is True


def test_completion_compare_schema_missing_reuses_legacy_summary_and_logs_once(monkeypatch) -> None:
    calls: list[str] = []
    logged: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(repository.payload_sidecars, "payload_read_mode", lambda: "compare")
    monkeypatch.setattr(repository.payload_compare, "should_sample_payload_compare", lambda **_kwargs: True)
    monkeypatch.setattr(repository.logger, "info", lambda message, *, extra: logged.append((message, extra)))

    def fake_fetch_one(query, _params, *, pool_name):
        assert pool_name == "social_profile"
        calls.append(str(query))
        if "instagram_post_payloads" in str(query):
            raise UndefinedTable("sidecar table missing")
        return _summary_row(total_posts=1)

    monkeypatch.setattr(repository.pg, "fetch_one", fake_fetch_one)

    payload = repository.get_social_completion_summary(
        platform="instagram",
        account_handle="bravotv",
        year=2026,
    )

    assert payload["total_posts"] == 1
    assert len(calls) == 3
    assert "instagram_post_payloads" not in calls[0]
    assert "instagram_post_payloads" in calls[1]
    assert len(logged) == 1
    event = logged[0][1]["payload_compare"]
    assert isinstance(event, dict)
    assert event["schema_unavailable"] is True


def test_completion_sidecar_does_not_fallback_on_general_errors(monkeypatch) -> None:
    calls = 0
    monkeypatch.setattr(repository.payload_sidecars, "payload_read_mode", lambda: "sidecar")

    def fail(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise RuntimeError("query failed")

    monkeypatch.setattr(repository.pg, "fetch_one", fail)

    with pytest.raises(RuntimeError, match="query failed"):
        repository.get_social_completion_summary(
            platform="instagram",
            account_handle="bravotv",
            year=2026,
        )
    assert calls == 1


def test_completion_sql_preserves_legacy_collaborator_only_membership(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_one(query, params, *, pool_name):
        captured.update(query=query, params=params, pool_name=pool_name)
        return {"total_posts": 1}

    monkeypatch.setattr(repository.pg, "fetch_one", fake_fetch_one)

    payload = repository.get_social_completion_summary(
        platform="instagram",
        account_handle="legacy_collaborator",
        year=2024,
    )

    assert payload["total_posts"] == 1
    sql = " ".join(str(captured["query"]).split())
    assert "social.instagram_account_catalog_post_collaborators collaborator" in sql
    assert "jsonb_array_elements_text(coalesce(cp.collaborators, '[]'::jsonb))" in sql
    assert "jsonb_array_elements(coalesce(cp.raw_data -> 'collaborators_detail', '[]'::jsonb))" in sql
    assert "nullif(p.collaborators_detail, '[]'::jsonb)" in sql
    assert "nullif(p.raw_data -> 'collaborators_detail', '[]'::jsonb)" in sql
    assert sql.index("nullif(p.collaborators_detail") < sql.index("nullif(p.raw_data -> 'collaborators_detail'")
    assert "to_jsonb(" not in sql.lower()


def test_completion_summary_returns_zero_contract_without_a_row(monkeypatch) -> None:
    monkeypatch.setattr(repository.pg, "fetch_one", lambda *_args, **_kwargs: None)

    payload = repository.get_social_completion_summary(
        platform="instagram",
        account_handle="unknown",
        year=2025,
    )

    assert payload["total_posts"] == 0
    assert payload["total_reported_comments"] == 0
    assert payload["saved_comments"] == 0
    assert payload["missing_comments"] == 0
    assert payload["accounted_comments"] == 0
    assert payload["lanes"] == {
        "comments": {"finished": 0, "in_progress": 0, "not_started": 0},
        "details": {"finished": 0, "in_progress": 0, "not_started": 0},
        "media": {"finished": 0, "in_progress": 0, "not_started": 0},
    }


def test_landing_scrape_job_health_preserves_existing_summary(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_one(query, params, *, pool_name):
        captured.update(query=query, params=params, pool_name=pool_name)
        return {
            "window_started_at": datetime(2026, 7, 13, 4, 0, tzinfo=UTC),
            "generated_at": datetime(2026, 7, 13, 12, 0, tzinfo=UTC),
            "total_jobs": "15",
            "active_jobs": "4",
            "failed_jobs": "2",
            "failure_signal_jobs": "3",
            "in_failed_sql_transaction_hits": "1",
            "latest_failure_at": datetime(2026, 7, 13, 11, 30, tzinfo=UTC),
        }

    monkeypatch.setattr(repository.pg, "fetch_one", fake_fetch_one)

    payload = repository.get_social_landing_scrape_job_health()

    assert payload == {
        "window_hours": 8,
        "window_started_at": "2026-07-13T04:00:00.000Z",
        "generated_at": "2026-07-13T12:00:00.000Z",
        "total_jobs": 15,
        "active_jobs": 4,
        "failed_jobs": 2,
        "failure_signal_jobs": 3,
        "in_failed_sql_transaction_hits": 1,
        "latest_failure_at": "2026-07-13T11:30:00.000Z",
    }
    assert captured["params"] == [8, ["instagram", "tiktok", "twitter", "youtube"]]
    assert captured["pool_name"] == "social_profile"
    assert "from social.scrape_jobs" in str(captured["query"]).lower()


def test_landing_scrape_job_health_returns_empty_shape_without_a_row(monkeypatch) -> None:
    monkeypatch.setattr(repository.pg, "fetch_one", lambda *_args, **_kwargs: None)

    payload = repository.get_social_landing_scrape_job_health()

    assert payload == {
        "window_hours": 8,
        "window_started_at": None,
        "generated_at": None,
        "total_jobs": 0,
        "active_jobs": 0,
        "failed_jobs": 0,
        "failure_signal_jobs": 0,
        "in_failed_sql_transaction_hits": 0,
        "latest_failure_at": None,
    }
