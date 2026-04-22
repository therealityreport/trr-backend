from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import date

from trr_backend.repositories import social_season_analytics as social_repo


@contextmanager
def _fake_db_cursor(conn=None):  # noqa: ANN001
    yield object()


def test_enqueue_platform_media_mirror_job_is_global_across_runs_for_same_post(monkeypatch) -> None:
    created_ids: list[str | None] = []
    db_calls: list[tuple[str, tuple[object, ...], str]] = []
    state = {"job_id": None}
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
    monkeypatch.setattr(social_repo, "_platform_post_needs_media_mirror", lambda _platform, _row: True)
    monkeypatch.setattr(social_repo, "_update_platform_post_media_mirror_fields", lambda **_kwargs: None)
    monkeypatch.setattr(social_repo, "_increment_run_counters_on_job_create", lambda **_kwargs: None)
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
