from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsFetchResult


def test_classify_unavailable_gap_casts_season_id_before_aggregation(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    @contextmanager
    def _cursor(**_kwargs: Any):
        yield object()

    def _fetch_one_with_cursor(_cur: Any, sql: str, _params: list[Any]) -> dict[str, Any]:
        captured["count_sql"] = sql
        return {
            "classified_missing_comments": 0,
            "season_id": "00000000-0000-0000-0000-000000000111",
            "source_account": "thetraitorsus",
            "facebook_comment_count": 0,
        }

    def _fetch_all_with_cursor(_cur: Any, sql: str, params: list[Any]) -> list[dict[str, str]]:
        captured["insert_sql"] = sql
        captured["insert_params"] = params
        return [{"id": "missing-1"}, {"id": "missing-2"}]

    monkeypatch.setattr(jr.pg, "db_cursor", _cursor)
    monkeypatch.setattr(jr.pg, "fetch_one_with_cursor", _fetch_one_with_cursor)
    monkeypatch.setattr(jr.pg, "fetch_all_with_cursor", _fetch_all_with_cursor)

    inserted = jr._classify_unavailable_instagram_comment_gap(
        conn=object(),
        post_id="00000000-0000-0000-0000-000000000222",
        result=InstagramCommentsFetchResult(reported_comment_count=3),
        stored_total_comments=1,
        max_comments_per_post=0,
        run_id="00000000-0000-0000-0000-000000000333",
        job_id="00000000-0000-0000-0000-000000000444",
        reason="coverage_terminal_missing_classified",
    )

    normalized_count_sql = " ".join(str(captured["count_sql"]).split()).lower()
    assert inserted == 2
    assert "max(p.season_id)" not in normalized_count_sql
    assert "p.season_id::text" in normalized_count_sql
    assert captured["insert_params"][8] == "00000000-0000-0000-0000-000000000111"
