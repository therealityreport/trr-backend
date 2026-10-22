from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest

import trr_backend.socials.social_season_analytics_impl as social_repo

_CASES = [
    ("_upsert_facebook_comment_tree", "_enqueue_facebook_comment_media_mirror_job", "facebook"),
    ("_upsert_meta_threads_comment_tree", "_enqueue_threads_comment_media_mirror_job", "threads"),
]


def _context() -> social_repo.SeasonContext:
    return social_repo.SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )


def _comment(comment_id: str, replies: list[Any] | None = None) -> Any:
    return SimpleNamespace(
        comment_id=comment_id,
        username="fan",
        text="nice",
        likes=1,
        is_reply=bool(replies is None),
        reply_count=len(replies or []),
        created_at=None,
        media_urls=["https://cdn.example.com/reply.jpg"],
        replies=replies or [],
        raw_data={},
    )


@pytest.mark.parametrize(("upsert_name", "enqueue_name", "platform"), _CASES)
def test_comment_mirror_enqueue_failure_rolls_back_only_its_savepoint(
    monkeypatch: pytest.MonkeyPatch,
    upsert_name: str,
    enqueue_name: str,
    platform: str,
) -> None:
    savepoint_calls: list[tuple[str, str]] = []
    upserted: list[str] = []

    def _fake_pg_upsert(_table: str, payload: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
        upserted.append(str(payload["comment_id"]))
        return {"id": f"db-{payload['comment_id']}", **payload}

    def _failing_enqueue(*_args: Any, **_kwargs: Any) -> str:
        raise RuntimeError("there is no unique or exclusion constraint matching the ON CONFLICT specification")

    monkeypatch.setattr(social_repo, "_pg_upsert", _fake_pg_upsert)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda _table: False)
    monkeypatch.setattr(social_repo, enqueue_name, _failing_enqueue)
    monkeypatch.setattr(social_repo, "_create_savepoint", lambda _conn, name: savepoint_calls.append(("create", name)))
    monkeypatch.setattr(
        social_repo, "_rollback_to_savepoint", lambda _conn, name: savepoint_calls.append(("rollback", name))
    )
    monkeypatch.setattr(social_repo, "_release_savepoint", lambda _conn, name: savepoint_calls.append(("release", name)))

    persist_stats: dict[str, int] = {}
    total = getattr(social_repo, upsert_name)(
        _context(),
        job_id="job-1",
        run_id="run-1",
        account="bravotv",
        post_id="post-1",
        comment=_comment("c-1", replies=[_comment("c-2")]),
        persist_stats=persist_stats,
        conn=object(),
    )

    # The enqueue error is contained: every comment in the tree is still written.
    assert total == 2
    assert upserted == ["c-1", "c-2"]
    assert persist_stats["comment_media_mirror_job_enqueue_errors"] == 2
    rollbacks = [name for action, name in savepoint_calls if action == "rollback"]
    assert len(rollbacks) == 2
    assert all(f"{platform}_comment_media_mirror" in name for name in rollbacks)
