from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from datetime import date
from typing import Any

from trr_backend.socials import social_season_analytics_impl as social_repo


@contextmanager
def _fake_db_cursor(conn=None):  # noqa: ANN001
    yield object()


def test_enqueue_platform_media_mirror_job_is_global_across_runs_for_same_post(monkeypatch) -> None:
    created_ids: list[str | None] = []
    db_calls: list[tuple[str, tuple[object, ...], str]] = []
    state: dict[str, str | None] = {"job_id": None}
    lock = threading.Lock()

    def _fake_fetch_one_with_cursor(_cur, sql, params):  # noqa: ANN001
        normalized_sql = " ".join(str(sql).lower().split())
        if "insert into social.scrape_jobs" in normalized_sql:
            db_calls.append(("insert", tuple(params), normalized_sql))
            assert "on conflict (platform, (config->>'post_id'))" in normalized_sql
            assert "status in ('queued', 'pending', 'retrying', 'running')" in normalized_sql
            assert "coalesce(config->>'stage', metadata->>'stage', job_type) = 'media_mirror'" in normalized_sql
            with lock:
                if state["job_id"] is None:
                    state["job_id"] = "job-1"
                    return {"id": "job-1"}
            return None
        if "from social.scrape_jobs" in normalized_sql:
            db_calls.append(("lookup", tuple(params), normalized_sql))
            assert "platform = %s" in normalized_sql
            assert "config->>'post_id' = %s" in normalized_sql
            assert "coalesce(config->>'stage', metadata->>'stage', job_type) = %s" in normalized_sql
            assert "run_id" not in normalized_sql
            assert list(params) == ["instagram", "media_mirror", "post-1"]
            return {"id": state["job_id"]}
        raise AssertionError(sql)

    monkeypatch.setattr(social_repo.pg, "db_cursor", _fake_db_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_one_with_cursor", _fake_fetch_one_with_cursor)
    monkeypatch.setattr(social_repo, "_platform_post_needs_media_mirror", lambda _platform, _row, **_kwargs: True)
    monkeypatch.setattr(social_repo, "_update_platform_post_media_mirror_fields", lambda **_kwargs: None)
    monkeypatch.setattr(social_repo, "_increment_run_counters_on_job_create", lambda **_kwargs: None)
    monkeypatch.setattr(social_repo, "_run_allows_followup_job_enqueue", lambda _run_id, **_kwargs: True)
    monkeypatch.setattr(social_repo, "is_queue_enabled", lambda: True)

    context = social_repo.SeasonContext(  # noqa: SLF001
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    post_row = {"id": "post-1", "shortcode": "abc123"}

    def _go(run_id: str) -> None:
        created_ids.append(
            social_repo._enqueue_platform_media_mirror_job(  # noqa: SLF001
                context,
                platform="instagram",
                run_id=run_id,
                source_scope="bravo",
                account="bravotv",
                post_row=post_row,
                week_index=None,
                parent_job_id=None,
            )
        )

    threads = [
        threading.Thread(target=_go, args=("11111111-1111-1111-1111-111111111111",)),
        threading.Thread(target=_go, args=("22222222-2222-2222-2222-222222222222",)),
        threading.Thread(target=_go, args=("33333333-3333-3333-3333-333333333333",)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert db_calls[0][0] == "insert"
    assert any(call[0] == "lookup" for call in db_calls)
    assert len({job_id for job_id in created_ids if job_id}) == 1


def test_bulk_enqueue_platform_media_mirror_jobs_batches_insert_and_counter(monkeypatch) -> None:
    conn = object()
    execute_calls: list[dict[str, Any]] = []
    counter_calls: list[dict[str, Any]] = []
    update_calls: list[dict[str, Any]] = []

    def _execute_values_returning(sql: str, rows: list[tuple[Any, ...]], *, conn=None):  # noqa: ANN001
        normalized_sql = " ".join(sql.lower().split())
        assert "insert into social.scrape_jobs" in normalized_sql
        assert "on conflict (platform, (config->>'post_id'))" in normalized_sql
        assert "coalesce(config->>'stage', metadata->>'stage', job_type) = 'media_mirror'" in normalized_sql
        assert conn is execute_calls[0]["conn"] if execute_calls else conn is not None
        assert {len(row) for row in rows} == {12}
        assert all(row[5] is not None for row in rows)
        configs = [json.loads(row[3]) for row in rows]
        execute_calls.append({"rows": rows, "configs": configs, "conn": conn})
        return [
            {"job_id": f"job-{index}", "post_id": config["post_id"]} for index, config in enumerate(configs, start=1)
        ]

    monkeypatch.setattr(social_repo.pg, "execute_values_returning", _execute_values_returning)
    monkeypatch.setattr(social_repo, "_platform_post_needs_media_mirror", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(social_repo, "_run_allows_followup_job_enqueue", lambda _run_id, **_kwargs: True)
    monkeypatch.setattr(social_repo, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(
        social_repo,
        "_resolve_runtime_version_stamp_for_stage",
        lambda _stage=None: {"execution_backend": "local", "label": "local"},
    )
    monkeypatch.setattr(
        social_repo,
        "_increment_run_counters_on_job_create_batch",
        lambda **kwargs: counter_calls.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        social_repo,
        "_bulk_update_platform_post_media_mirror_pending",
        lambda **kwargs: update_calls.append(dict(kwargs)),
    )

    job_ids = social_repo._bulk_enqueue_platform_media_mirror_jobs(  # noqa: SLF001
        None,
        platform="instagram",
        run_id="11111111-1111-1111-1111-111111111111",
        source_scope="network",
        account="bravotv",
        post_rows=[
            {"id": "post-1", "shortcode": "ABC123"},
            {"id": "post-2", "shortcode": "DEF456"},
            {"id": "post-1", "shortcode": "ABC123"},
        ],
        week_index=None,
        parent_job_id="parent-job-1",
        conn=conn,
    )

    assert job_ids == ["job-1", "job-2"]
    assert len(execute_calls) == 1
    assert [config["post_id"] for config in execute_calls[0]["configs"]] == ["post-1", "post-2"]
    assert [config["source_id"] for config in execute_calls[0]["configs"]] == ["ABC123", "DEF456"]
    assert counter_calls == [
        {
            "run_id": "11111111-1111-1111-1111-111111111111",
            "stage": "media_mirror",
            "status": "queued",
            "count": 2,
            "conn": conn,
        }
    ]
    assert update_calls == [
        {
            "platform": "instagram",
            "job_rows": [
                {"job_id": "job-1", "post_id": "post-1"},
                {"job_id": "job-2", "post_id": "post-2"},
            ],
            "conn": conn,
        }
    ]


def test_bulk_enqueue_platform_media_mirror_jobs_reuses_existing_active_job(monkeypatch) -> None:
    conn = object()
    counter_calls: list[dict[str, Any]] = []
    update_calls: list[dict[str, Any]] = []

    def _fake_fetch_all(sql: str, params: list[Any], **kwargs: Any) -> list[dict[str, Any]]:
        assert "social.scrape_jobs" in " ".join(str(sql).lower().split())
        assert kwargs.get("conn") is conn
        assert params[0] == "instagram"
        assert params[1] == "media_mirror"
        return [{"job_id": "existing-job-1", "post_id": "post-1"}]

    monkeypatch.setattr(social_repo.pg, "execute_values_returning", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo, "_platform_post_needs_media_mirror", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(social_repo, "_run_allows_followup_job_enqueue", lambda _run_id, **_kwargs: True)
    monkeypatch.setattr(social_repo, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(
        social_repo,
        "_resolve_runtime_version_stamp_for_stage",
        lambda _stage=None: {"execution_backend": "local", "label": "local"},
    )
    monkeypatch.setattr(
        social_repo,
        "_increment_run_counters_on_job_create_batch",
        lambda **kwargs: counter_calls.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        social_repo,
        "_bulk_update_platform_post_media_mirror_pending",
        lambda **kwargs: update_calls.append(dict(kwargs)),
    )

    job_ids = social_repo._bulk_enqueue_platform_media_mirror_jobs(  # noqa: SLF001
        None,
        platform="instagram",
        run_id="11111111-1111-1111-1111-111111111111",
        source_scope="network",
        account="bravotv",
        post_rows=[{"id": "post-1", "shortcode": "ABC123"}],
        week_index=None,
        parent_job_id="parent-job-1",
        conn=conn,
    )

    assert job_ids == ["existing-job-1"]
    assert counter_calls == []
    assert update_calls == [
        {
            "platform": "instagram",
            "job_rows": [{"job_id": "existing-job-1", "post_id": "post-1"}],
            "conn": conn,
        }
    ]


def test_enqueue_platform_media_mirror_job_skips_cancelled_parent_run(monkeypatch) -> None:
    monkeypatch.setattr(social_repo, "_platform_post_needs_media_mirror", lambda _platform, _row, **_kwargs: True)
    monkeypatch.setattr(social_repo, "_run_allows_followup_job_enqueue", lambda _run_id, **_kwargs: False)
    monkeypatch.setattr(
        social_repo,
        "_insert_job_with_conflict_target",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not enqueue")),
    )

    context = social_repo.SeasonContext(  # noqa: SLF001
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )

    assert (
        social_repo._enqueue_platform_media_mirror_job(  # noqa: SLF001
            context,
            platform="instagram",
            run_id="11111111-1111-1111-1111-111111111111",
            source_scope="bravo",
            account="bravotv",
            post_row={"id": "post-1", "shortcode": "abc123"},
            week_index=None,
            parent_job_id=None,
        )
        is None
    )
