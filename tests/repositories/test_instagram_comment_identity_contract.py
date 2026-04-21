from __future__ import annotations

from contextlib import contextmanager

from trr_backend.repositories import social_season_analytics as social_repo


def test_upsert_instagram_comment_tree_uses_composite_conflict_cols(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _Comment:
        comment_id = "same-comment"
        username = "alpha"
        user_id = "user-1"
        text = "one"
        likes = 0
        is_reply = False
        reply_count = 0
        created_at = None
        replies: list[object] = []
        media_urls: list[str] = []

        def to_dict(self) -> dict[str, object]:
            return {"comment_id": self.comment_id}

    def _fake_pg_upsert(table: str, payload: dict[str, object], *, conflict_col, conn=None):  # noqa: ANN001
        captured["table"] = table
        captured["payload"] = dict(payload)
        captured["conflict_col"] = conflict_col
        return {"id": "row-1", "comment_id": payload["comment_id"]}

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_pg_upsert)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "instagram_comments")
    monkeypatch.setattr(social_repo, "_column_exists", lambda _schema, _table, _column: False)

    context = social_repo.SeasonContext(  # noqa: SLF001
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=social_repo.date(2025, 1, 1),
    )

    written = social_repo._upsert_instagram_comment_tree(  # noqa: SLF001
        context,
        job_id=None,
        run_id="run-1",
        account="alpha",
        post_id="post-1",
        comment=_Comment(),
        enable_media_followups=False,
    )

    assert written == 1
    assert captured["table"] == "instagram_comments"
    assert captured["conflict_col"] == ["post_id", "comment_id"]
    assert captured["payload"]["post_id"] == "post-1"
    assert captured["payload"]["comment_id"] == "same-comment"


def test_platform_comment_context_row_for_media_mirror_uses_post_scoped_identity_for_instagram(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_one(sql, params):  # noqa: ANN001
        captured["sql"] = sql
        captured["params"] = params
        return {"season_id": "season-1"}

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    social_repo._platform_comment_context_row_for_media_mirror(  # noqa: SLF001
        "instagram",
        comment_id="comment-1",
        comment_db_id="",
        post_id="33333333-3333-3333-3333-333333333333",
    )

    sql = str(captured["sql"]).lower()
    assert "c.comment_id = %s and c.post_id = nullif(%s, '')::uuid" in sql
    assert captured["params"] == [
        "",
        "",
        "comment-1",
        "33333333-3333-3333-3333-333333333333",
        "comment-1",
        "33333333-3333-3333-3333-333333333333",
    ]


def test_enqueue_platform_comment_media_mirror_job_uses_post_scoped_lookup_for_instagram(monkeypatch) -> None:
    captured: dict[str, object] = {}

    @contextmanager
    def _fake_db_cursor(conn=None):  # noqa: ANN001
        yield object()

    def _fake_fetch_one_with_cursor(_cur, sql, params):  # noqa: ANN001
        captured["sql"] = sql
        captured["params"] = params
        return {"id": "job-1"}

    monkeypatch.setattr(social_repo.pg, "db_cursor", _fake_db_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_one_with_cursor", _fake_fetch_one_with_cursor)
    monkeypatch.setattr(social_repo, "_column_exists", lambda _schema, _table, column: column == "media_urls")
    monkeypatch.setattr(social_repo, "_platform_comment_media_needs_mirror", lambda _platform, _row: True)

    context = social_repo.SeasonContext(  # noqa: SLF001
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=social_repo.date(2025, 1, 1),
    )

    mirror_job_id = social_repo._enqueue_platform_comment_media_mirror_job(  # noqa: SLF001
        context,
        platform="instagram",
        run_id="44444444-4444-4444-4444-444444444444",
        source_scope="bravo",
        account="bravotv",
        comment_row={
            "id": "55555555-5555-5555-5555-555555555555",
            "comment_id": "comment-1",
            "post_id": "66666666-6666-6666-6666-666666666666",
            "media_urls": ["https://cdn.example.com/comment.jpg"],
        },
        parent_job_id=None,
    )

    assert mirror_job_id == "job-1"
    assert "config->>'post_id' = %s" in str(captured["sql"]).lower()
    assert captured["params"] == [
        "instagram",
        social_repo.COMMENT_MEDIA_MIRROR_STAGE,
        "comment-1",
        "66666666-6666-6666-6666-666666666666",
        "44444444-4444-4444-4444-444444444444",
        "44444444-4444-4444-4444-444444444444",
    ]
