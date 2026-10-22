from __future__ import annotations

import re
import threading
from contextlib import contextmanager
from datetime import date
from pathlib import Path

from trr_backend.socials import social_season_analytics_impl as social_repo


@contextmanager
def _fake_db_cursor(conn=None):  # noqa: ANN001
    yield object()


def test_enqueue_platform_comment_media_mirror_job_is_global_across_runs_for_same_comment(monkeypatch) -> None:
    created_ids: list[str | None] = []
    state: dict[str, str | None] = {"job_id": None}
    lock = threading.Lock()
    seen_insert_sql: list[str] = []

    def _fake_fetch_one_with_cursor(_cur, sql, params):  # noqa: ANN001
        normalized_sql = " ".join(str(sql).lower().split())
        if "insert into social.scrape_jobs" in normalized_sql:
            seen_insert_sql.append(normalized_sql)
            with lock:
                if state["job_id"] is None:
                    state["job_id"] = "job-1"
                    return {"id": "job-1"}
            return None
        if "from social.scrape_jobs" in normalized_sql:
            return {"id": state["job_id"]}
        raise AssertionError(sql)

    monkeypatch.setattr(social_repo.pg, "db_cursor", _fake_db_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_one_with_cursor", _fake_fetch_one_with_cursor)
    monkeypatch.setattr(social_repo, "_column_exists", lambda _schema, _table, column: column == "media_urls")
    monkeypatch.setattr(social_repo, "_platform_comment_media_needs_mirror", lambda _platform, _row: True)
    monkeypatch.setattr(social_repo, "_update_platform_comment_media_mirror_fields", lambda **_kwargs: None)
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
    comment_row = {
        "id": "",
        "comment_id": "comment-1",
        "post_id": "post-1",
        "media_urls": ["https://cdn.example.com/comment.jpg"],
    }

    def _go(run_id: str) -> None:
        created_ids.append(
            social_repo._enqueue_platform_comment_media_mirror_job(  # noqa: SLF001
                context,
                platform="instagram",
                run_id=run_id,
                source_scope="bravo",
                account="bravotv",
                comment_row=comment_row,
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

    assert len({job_id for job_id in created_ids if job_id}) == 1
    assert any("nullif(config->>'comment_db_id', '')" in sql for sql in seen_insert_sql)
    assert any("(config->>'post_id') || ':' || (config->>'comment_id')" in sql for sql in seen_insert_sql)


def test_enqueue_platform_comment_media_mirror_job_skips_cancelled_parent_run(monkeypatch) -> None:
    monkeypatch.setattr(social_repo, "_column_exists", lambda _schema, _table, column: column == "media_urls")
    monkeypatch.setattr(social_repo, "_platform_comment_media_needs_mirror", lambda _platform, _row: True)
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
        social_repo._enqueue_platform_comment_media_mirror_job(  # noqa: SLF001
            context,
            platform="instagram",
            run_id="11111111-1111-1111-1111-111111111111",
            source_scope="bravo",
            account="bravotv",
            comment_row={
                "id": "",
                "comment_id": "comment-1",
                "post_id": "post-1",
                "media_urls": ["https://cdn.example.com/comment.jpg"],
            },
            parent_job_id=None,
        )
        is None
    )


_INDEX_MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "supabase"
    / "migrations"
    / "20260421130500_scrape_jobs_active_comment_media_mirror_uniq.sql"
)


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.lower().split())


def test_comment_media_mirror_conflict_target_matches_unique_index_expression(monkeypatch) -> None:
    """Postgres infers ON CONFLICT arbiters by expression tree, so the target must equal the index."""
    captured: dict[str, str] = {}

    def _fake_insert(*_args, **kwargs):  # noqa: ANN002, ANN003
        captured["conflict_target_sql"] = kwargs["conflict_target_sql"]
        return None

    monkeypatch.setattr(social_repo, "_column_exists", lambda _schema, _table, column: column == "media_urls")
    monkeypatch.setattr(social_repo, "_platform_comment_media_needs_mirror", lambda _platform, _row: True)
    monkeypatch.setattr(social_repo, "_run_allows_followup_job_enqueue", lambda _run_id, **_kwargs: True)
    monkeypatch.setattr(social_repo, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(social_repo, "_insert_job_with_conflict_target", _fake_insert)

    context = social_repo.SeasonContext(  # noqa: SLF001
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )
    social_repo._enqueue_platform_comment_media_mirror_job(  # noqa: SLF001
        context,
        platform="threads",
        run_id="11111111-1111-1111-1111-111111111111",
        source_scope="bravo",
        account="bravotv",
        comment_row={
            "id": "",
            "comment_id": "comment-1",
            "post_id": "post-1",
            "media_urls": ["https://cdn.example.com/comment.jpg"],
        },
        parent_job_id=None,
    )

    conflict_sql = _normalize_sql(captured["conflict_target_sql"])
    index_sql = _normalize_sql(_INDEX_MIGRATION.read_text(encoding="utf-8"))
    index_match = re.search(r"on social\.scrape_jobs \((.*)\) where (.*?);", index_sql)
    conflict_match = re.search(r"on conflict \((.*)\) where (.*?) do nothing", conflict_sql)
    assert index_match is not None
    assert conflict_match is not None
    assert conflict_match.group(1).strip() == index_match.group(1).strip()
    assert conflict_match.group(2).strip() == index_match.group(2).strip()
