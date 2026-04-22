from trr_backend.repositories import social_season_analytics as social_repo


def test_pg_upsert_many_accepts_composite_conflict_cols(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_execute_values_returning(sql, values, conn=None):  # noqa: ANN001
        captured["sql"] = sql
        captured["values"] = values
        return []

    monkeypatch.setattr(social_repo.pg, "execute_values_returning", _fake_execute_values_returning)

    social_repo._pg_upsert_many(  # noqa: SLF001
        "instagram_comments",
        [{"post_id": "p1", "comment_id": "c1", "text": "hello"}],
        conflict_col=["post_id", "comment_id"],
    )

    assert "on conflict (post_id, comment_id)" in str(captured["sql"]).lower()


def test_pg_upsert_many_dedupes_duplicate_composite_conflicts_before_insert(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_execute_values_returning(sql, values, conn=None):  # noqa: ANN001
        captured["sql"] = sql
        captured["values"] = values
        return []

    monkeypatch.setattr(social_repo.pg, "execute_values_returning", _fake_execute_values_returning)

    social_repo._pg_upsert_many(  # noqa: SLF001
        "instagram_comments",
        [
            {"post_id": "p1", "comment_id": "c1", "text": "first"},
            {"post_id": "p1", "comment_id": "c1", "text": "second"},
        ],
        conflict_col=["post_id", "comment_id"],
    )

    assert captured["values"] == [("p1", "c1", "second")]
