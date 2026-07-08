from __future__ import annotations

from typing import Any

import pytest

from trr_backend.socials import social_season_analytics_impl as impl


def test_instagram_saved_comment_counts_uses_active_rollup(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    monkeypatch.setattr(impl, "_relation_exists", lambda *_args, **_kwargs: True)

    def fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        calls.append({"sql": sql, "params": params})
        return [{"post_id": "post-1", "cnt": 3}]

    monkeypatch.setattr(impl.pg, "fetch_all", fake_fetch_all)

    counts = impl._instagram_saved_comment_counts_by_post(["post-1"], active_filter_applied=True)

    assert counts == {"post-1": 3}
    assert "from social.instagram_post_comment_rollups r" in calls[0]["sql"]
    assert "r.active_comment_count::int as cnt" in calls[0]["sql"]
    assert "total_comment_count" not in calls[0]["sql"]
    assert calls[0]["params"] == [["post-1"]]


def test_instagram_saved_comment_counts_uses_total_rollup(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    monkeypatch.setattr(impl, "_relation_exists", lambda *_args, **_kwargs: True)

    def fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        calls.append({"sql": sql, "params": params})
        return [{"post_id": "post-1", "cnt": 5}]

    monkeypatch.setattr(impl.pg, "fetch_all", fake_fetch_all)

    counts = impl._instagram_saved_comment_counts_by_post(["post-1"], active_filter_applied=False)

    assert counts == {"post-1": 5}
    assert "from social.instagram_post_comment_rollups r" in calls[0]["sql"]
    assert "r.total_comment_count::int as cnt" in calls[0]["sql"]
    assert "active_comment_count" not in calls[0]["sql"]


@pytest.mark.parametrize(
    ("active_filter_applied", "expected_counts"),
    [
        (True, {"post-1": 2, "post-2": 1}),
        (False, {"post-1": 3, "post-2": 2}),
    ],
)
def test_instagram_saved_comment_counts_falls_back_to_raw_aggregate(
    monkeypatch: pytest.MonkeyPatch,
    active_filter_applied: bool,
    expected_counts: dict[str, int],
) -> None:
    comments = [
        {"post_id": "post-1", "is_missing": False},
        {"post_id": "post-1", "is_missing": False},
        {"post_id": "post-1", "is_missing": True},
        {"post_id": "post-2", "is_missing": False},
        {"post_id": "post-2", "is_missing": True},
    ]
    calls: list[dict[str, Any]] = []

    monkeypatch.setattr(impl, "_relation_exists", lambda *_args, **_kwargs: False)

    def direct_count(post_ids: list[str]) -> dict[str, int]:
        counts = dict.fromkeys(post_ids, 0)
        for comment in comments:
            if comment["post_id"] not in counts:
                continue
            if active_filter_applied and comment["is_missing"]:
                continue
            counts[comment["post_id"]] += 1
        return {post_id: count for post_id, count in counts.items() if count}

    def fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        calls.append({"sql": sql, "params": params})
        return [{"post_id": post_id, "cnt": count} for post_id, count in direct_count(params[0]).items()]

    monkeypatch.setattr(impl.pg, "fetch_all", fake_fetch_all)

    post_ids = ["post-1", "post-2"]
    counts = impl._instagram_saved_comment_counts_by_post(
        post_ids,
        active_filter_applied=active_filter_applied,
    )

    assert counts == expected_counts
    assert counts == direct_count(post_ids)
    assert "from social.instagram_comments c" in calls[0]["sql"]
    assert "group by c.post_id" in calls[0]["sql"]
    if active_filter_applied:
        assert "coalesce(c.is_missing, false) = false" in calls[0]["sql"]
    else:
        assert "coalesce(c.is_missing, false) = false" not in calls[0]["sql"]
