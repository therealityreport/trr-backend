from __future__ import annotations

from datetime import UTC, datetime

from trr_backend.repositories import social_season_analytics as social_repo


def test_comment_media_coverage_supports_facebook_comment_media(monkeypatch) -> None:
    monkeypatch.setattr(social_repo, "_column_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda *_args, **_kwargs: True)

    captured: dict[str, object] = {}

    def fake_fetch_all(query: str, params: list[object]) -> list[dict[str, object]]:
        captured["query"] = query
        captured["params"] = params
        return [
            {
                "media_urls": ["https://source.example/a.jpg", "https://source.example/b.jpg"],
                "hosted_media_urls": ["https://cdn.example/a.jpg"],
                "media_mirror_status": "pending",
            }
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", fake_fetch_all)

    payload = social_repo._comment_media_coverage_for_platform(
        "season-1",
        platform="facebook",
        start_dt=datetime(2026, 1, 1, tzinfo=UTC),
        end_dt=datetime(2026, 1, 2, tzinfo=UTC),
        source_scope="community",
        target_accounts_by_platform={"facebook": {"bravoaccount"}},
    )

    assert "from social.facebook_comments c" in str(captured.get("query") or "")
    assert payload == {
        "items_scanned": 2,
        "needs_mirror_count": 1,
        "mirrored_count": 1,
        "failed_count": 0,
        "pending_count": 1,
    }


def test_comment_media_coverage_supports_instagram_comment_media(monkeypatch) -> None:
    monkeypatch.setattr(social_repo, "_column_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda *_args, **_kwargs: True)

    def fake_fetch_all(_query: str, _params: list[object]) -> list[dict[str, object]]:
        return [
            {
                "media_urls": ["https://source.example/comment-image.jpg"],
                "hosted_media_urls": [],
                "media_mirror_status": "failed",
            }
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", fake_fetch_all)

    payload = social_repo._comment_media_coverage_for_platform(
        "season-1",
        platform="instagram",
        start_dt=datetime(2026, 1, 1, tzinfo=UTC),
        end_dt=datetime(2026, 1, 2, tzinfo=UTC),
        source_scope="bravo",
        target_accounts_by_platform={"instagram": {"bravoaccount"}},
    )

    assert payload == {
        "items_scanned": 1,
        "needs_mirror_count": 1,
        "mirrored_count": 0,
        "failed_count": 1,
        "pending_count": 0,
    }


def test_comment_media_coverage_supports_threads_comment_media(monkeypatch) -> None:
    monkeypatch.setattr(social_repo, "_column_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda *_args, **_kwargs: False)

    def fake_fetch_all(_query: str, _params: list[object]) -> list[dict[str, object]]:
        return [
            {
                "media_urls": ["https://source.example/reply.mp4"],
                "hosted_media_urls": [],
                "media_mirror_status": "failed",
            }
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", fake_fetch_all)

    payload = social_repo._comment_media_coverage_for_platform(
        "season-1",
        platform="threads",
        start_dt=datetime(2026, 1, 1, tzinfo=UTC),
        end_dt=datetime(2026, 1, 2, tzinfo=UTC),
        source_scope="community",
        target_accounts_by_platform={"threads": {"bravoaccount"}},
    )

    assert payload == {
        "items_scanned": 1,
        "needs_mirror_count": 1,
        "mirrored_count": 0,
        "failed_count": 1,
        "pending_count": 0,
    }
