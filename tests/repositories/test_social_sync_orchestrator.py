from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

from trr_backend.repositories import social_sync_orchestrator as orchestrator


class _FakeSocialRepo:
    PLATFORM_POST_TABLES = {"instagram": "instagram_posts"}
    PLATFORM_POSTED_AT_COLUMN = {"instagram": "posted_at"}
    PLATFORM_SOURCE_ID_COLUMN = {"instagram": "shortcode"}

    @staticmethod
    def get_season_context(_season_id: str) -> SimpleNamespace:
        return SimpleNamespace(show_id="show-1")

    @staticmethod
    def _target_accounts_by_platform(*_args, **_kwargs) -> dict[str, set[str]]:
        return {"instagram": {"bravoaccount"}}

    @staticmethod
    def get_comments_coverage(*_args, **_kwargs) -> dict[str, object]:
        return {"up_to_date": True, "stale_posts_count": 0, "by_platform": {"instagram": {"stale_posts_count": 0}}}

    @staticmethod
    def get_mirror_coverage(*_args, **_kwargs) -> dict[str, object]:
        return {
            "up_to_date": True,
            "needs_mirror_count": 0,
            "comment_media_items_scanned": 5,
            "comment_media_needs_mirror_count": 2,
            "comment_media_mirrored_count": 3,
            "comment_media_failed_count": 1,
            "comment_media_pending_count": 1,
        }

    @staticmethod
    def _relation_exists(_relation: str) -> bool:
        return False

    @staticmethod
    def _iso(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None

    @staticmethod
    def _coerce_dt(value: datetime | str | None) -> datetime | None:
        return value if isinstance(value, datetime) else None

    @staticmethod
    def _column_exists(*_args, **_kwargs) -> bool:
        return True

    @staticmethod
    def _as_text_list(value: object, *, prefix: str = "", strip_prefix: str | None = None) -> list[str]:
        if value is None:
            return []
        items = value if isinstance(value, list) else [value]
        result: list[str] = []
        for item in items:
            text = str(item or "").strip()
            if strip_prefix and text.startswith(strip_prefix):
                text = text[len(strip_prefix) :]
            if prefix:
                text = f"{prefix}{text.lstrip(prefix)}"
            if text:
                result.append(text)
        return result

    @staticmethod
    def _platform_post_needs_media_mirror(_platform: str, post_row: dict[str, object]) -> bool:
        return bool(post_row.get("needs_mirror"))

    @staticmethod
    def _platform_post_avatar_repair_state(_platform: str, post_row: dict[str, object]) -> dict[str, object]:
        return {"needs_repair": bool(post_row.get("needs_avatar"))}

    @staticmethod
    def is_queue_enabled() -> bool:
        return True

    @staticmethod
    def get_worker_health(*_args, **_kwargs) -> dict[str, object]:
        return {
            "queue_enabled": True,
            "healthy": True,
            "healthy_workers": 2,
            "oldest_queued_age_seconds": 12,
            "workers": [
                {
                    "supported_platforms": ["twitter", "tiktok", "facebook", "threads"],
                    "metadata": {"worker_version": "worker-test-1"},
                }
            ],
        }

    @staticmethod
    def _modal_dispatch_platform_cap(_stage: str | None, platform: str | None) -> int | None:
        defaults = {"tiktok": 2, "twitter": 2, "facebook": 1, "threads": 1}
        return defaults.get(str(platform or "").strip().lower())


def test_follow_up_dimensions_from_snapshot_returns_expected_dimensions() -> None:
    follow_up_dimensions = orchestrator._follow_up_dimensions_from_snapshot(
        comments_coverage={"up_to_date": False},
        asset_coverage={"up_to_date": True},
        comment_media_coverage={"up_to_date": False},
        avatar_coverage={"up_to_date": False},
    )

    assert follow_up_dimensions == ["comments", "comment_media", "avatars"]


def test_build_completeness_snapshot_requires_comment_media_and_avatars(monkeypatch) -> None:
    monkeypatch.setattr(orchestrator, "_social_repo", lambda: _FakeSocialRepo())
    monkeypatch.setattr(
        orchestrator,
        "_build_missing_comment_targets",
        lambda **_kwargs: {},
    )
    monkeypatch.setattr(
        orchestrator,
        "_build_missing_detail_target_groups",
        lambda **_kwargs: {
            "details": {"instagram": ["abc123"]},
            "assets": {},
            "avatars": {"instagram": ["abc123"]},
            "comment_media": {"instagram": ["abc123"]},
        },
    )
    monkeypatch.setattr(
        orchestrator,
        "_build_avatar_coverage_snapshot",
        lambda **_kwargs: {
            "posts_scanned": 4,
            "missing_avatar_count": 1,
            "up_to_date": False,
            "by_platform": {"instagram": {"posts_scanned": 4, "missing_avatar_count": 1, "up_to_date": False}},
        },
    )

    snapshot = orchestrator._build_completeness_snapshot(
        season_id="season-1",
        source_scope="bravo",
        platforms=["instagram"],
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert snapshot["up_to_date"] is False
    assert snapshot["comment_media_coverage"]["up_to_date"] is False
    assert snapshot["avatar_coverage"]["up_to_date"] is False
    assert snapshot["missing_asset_count"] == 0
    assert snapshot["missing_comment_media_count"] == 2
    assert snapshot["missing_avatar_count"] == 1
    assert snapshot["comment_media_target_count"] == 1
    assert snapshot["avatar_target_count"] == 1
    assert snapshot["detail_target_count"] == 1
    assert snapshot["follow_up_dimensions"] == ["comment_media", "avatars"]


def test_next_pass_kind_uses_comment_media_and_avatar_gaps() -> None:
    assert (
        orchestrator._next_pass_kind_from_snapshot(
            {
                "comments_coverage": {"up_to_date": True},
                "asset_coverage": {"up_to_date": True},
                "comment_media_coverage": {"up_to_date": False},
                "avatar_coverage": {"up_to_date": True},
            }
        )
        == "details_refresh"
    )
    assert (
        orchestrator._next_pass_kind_from_snapshot(
            {
                "comments_coverage": {"up_to_date": True},
                "asset_coverage": {"up_to_date": True},
                "comment_media_coverage": {"up_to_date": True},
                "avatar_coverage": {"up_to_date": False},
            }
        )
        == "details_refresh"
    )


def test_serialize_sync_session_marks_follow_up_needed_for_avatar_only_gap() -> None:
    payload = orchestrator._serialize_sync_session(
        {
            "id": "sync-1",
            "season_id": "season-1",
            "show_id": "show-1",
            "source_scope": "bravo",
            "platforms": ["instagram"],
            "status": "pass_running",
            "current_pass_kind": "posts_and_comments",
            "current_pass_attempt": 1,
            "current_run_id": None,
            "pass_sequence": 1,
            "pass_history": [],
            "follow_up_reason": "coverage_gap",
            "completeness_snapshot": {
                "up_to_date": False,
                "follow_up_dimensions": ["avatars"],
                "comments_coverage": {"up_to_date": True},
                "asset_coverage": {"up_to_date": True},
                "comment_media_coverage": {"up_to_date": True},
                "avatar_coverage": {"up_to_date": False},
                "avatar_target_count": 12,
                "comment_target_count": 0,
                "detail_target_count": 12,
                "comment_media_target_count": 0,
            },
            "sync_config": {},
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
            "updated_at": datetime(2026, 1, 1, tzinfo=UTC),
        }
    )

    assert payload["display_status"] == "Follow-up needed"
    assert payload["next_pass_kind"] == "details_refresh"
    assert payload["follow_up_dimensions"] == ["avatars"]
    assert "avatars" in str(payload["status_reason"])


def test_serialize_sync_session_normalizes_comment_display_counts_when_saved_exceeds_reported() -> None:
    payload = orchestrator._serialize_sync_session(
        {
            "id": "sync-comments-1",
            "season_id": "season-1",
            "show_id": "show-1",
            "source_scope": "bravo",
            "platforms": ["tiktok"],
            "status": "pass_running",
            "current_pass_kind": "posts_and_comments",
            "current_pass_attempt": 1,
            "current_run_id": None,
            "pass_sequence": 1,
            "pass_history": [],
            "follow_up_reason": "initial_start",
            "completeness_snapshot": {
                "up_to_date": False,
                "follow_up_dimensions": ["media"],
                "comments_coverage": {
                    "total_saved_comments": 4573,
                    "total_reported_comments": 4368,
                    "comment_sync_status": {
                        "status": "complete",
                        "expected_count": 4368,
                        "fetched_count": 4573,
                        "upserted_count": 4573,
                        "failure_reason": None,
                    },
                    "by_platform": {
                        "tiktok": {
                            "saved_comments": 4573,
                            "reported_comments": 4368,
                            "comment_sync_status": {
                                "status": "complete",
                                "expected_count": 4368,
                                "fetched_count": 4573,
                                "upserted_count": 4573,
                                "failure_reason": None,
                            },
                        }
                    },
                },
                "asset_coverage": {"up_to_date": False, "by_platform": {"tiktok": {"up_to_date": False}}},
                "comment_media_coverage": {"up_to_date": True},
                "avatar_coverage": {"up_to_date": True, "by_platform": {"tiktok": {"up_to_date": True}}},
            },
            "sync_config": {},
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
            "updated_at": datetime(2026, 1, 1, tzinfo=UTC),
        }
    )

    assert payload["coverage_by_dimension"]["comments"]["total_reported_comments"] == 4573
    assert payload["coverage_by_dimension"]["comments"]["total_reported_comments_raw"] == 4368
    assert payload["coverage_by_dimension"]["comments"]["comment_sync_status"]["expected_count"] == 4573
    assert payload["coverage_by_dimension"]["comments"]["comment_sync_status"]["expected_count_raw"] == 4368
    assert payload["platform_diagnostics"]["tiktok"]["coverage_by_dimension"]["comments"]["reported_comments"] == 4573
    assert (
        payload["platform_diagnostics"]["tiktok"]["coverage_by_dimension"]["comments"]["reported_comments_raw"] == 4368
    )


def test_get_sync_session_rebuilds_live_snapshot_for_active_session(monkeypatch) -> None:
    row = {
        "id": "sync-1",
        "season_id": "season-1",
        "show_id": "show-1",
        "source_scope": "bravo",
        "platforms": ["instagram"],
        "status": "pass_running",
        "current_pass_kind": "details_refresh",
        "current_pass_attempt": 1,
        "current_run_id": "run-1",
        "pass_sequence": 2,
        "pass_history": [],
        "follow_up_reason": "coverage_gap",
        "completeness_snapshot": {"stale": True},
        "sync_config": {},
        "date_start": datetime(2026, 1, 1, tzinfo=UTC),
        "date_end": datetime(2026, 1, 2, tzinfo=UTC),
        "created_at": datetime(2026, 1, 1, tzinfo=UTC),
        "updated_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    monkeypatch.setattr(orchestrator, "_fetch_sync_session_row", lambda _sync_session_id: row)
    monkeypatch.setattr(
        orchestrator,
        "_build_completeness_snapshot",
        lambda **_kwargs: {
            "up_to_date": False,
            "follow_up_dimensions": ["media"],
            "comments_coverage": {"up_to_date": True},
            "asset_coverage": {"up_to_date": False},
            "comment_media_coverage": {"up_to_date": True},
            "avatar_coverage": {"up_to_date": True},
            "detail_target_count": 14,
        },
    )
    monkeypatch.setattr(orchestrator, "_current_run_payload", lambda _run_id: None)

    payload = orchestrator.get_sync_session("sync-1")

    assert payload["completeness_snapshot"]["detail_target_count"] == 14
    assert payload["follow_up_dimensions"] == ["media"]


def test_build_missing_detail_targets_includes_comment_media_gaps(monkeypatch) -> None:
    fake_rows = [
        {"source_id": "abc123", "post_json": {"needs_mirror": False, "needs_avatar": False}},
        {"source_id": "xyz999", "post_json": {"needs_mirror": False, "needs_avatar": True}},
    ]

    def fake_fetch_all(query: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        if "from social.instagram_posts p" in query:
            return fake_rows
        if "from social.facebook_comments c" in query:
            return [{"source_id": "fb-post-1"}]
        return []

    fake_repo = _FakeSocialRepo()
    fake_repo.PLATFORM_POST_TABLES = {"instagram": "instagram_posts", "facebook": "facebook_posts"}
    fake_repo.PLATFORM_POSTED_AT_COLUMN = {"instagram": "posted_at", "facebook": "posted_at"}
    fake_repo.PLATFORM_SOURCE_ID_COLUMN = {"instagram": "shortcode", "facebook": "post_id"}

    monkeypatch.setattr(orchestrator, "_social_repo", lambda: fake_repo)
    monkeypatch.setattr(orchestrator.pg, "fetch_all", fake_fetch_all)

    targets = orchestrator._build_missing_detail_targets(
        season_id="season-1",
        platforms=["instagram", "facebook"],
        source_scope="bravo",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert targets["instagram"] == ["xyz999"]
    assert targets["facebook"] == ["fb-post-1"]


def test_build_avatar_coverage_snapshot_treats_instagram_mentions_with_terminal_registry_state_as_complete(
    monkeypatch,
) -> None:
    fake_repo = _FakeSocialRepo()
    fake_repo._relation_exists = lambda _relation: True  # type: ignore[attr-defined]

    def fake_fetch_all(query: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        if "from social.avatar_registry" in query:
            return [
                {
                    "platform": "instagram",
                    "account_handle": "castmember",
                    "source_url": None,
                    "status": "unsupported",
                }
            ]
        if "from social.instagram_posts p" in query:
            return [
                {
                    "post_json": {
                        "username": "bravoaccount",
                        "source_account": "bravoaccount",
                        "mentions": ["@castmember"],
                        "hosted_tagged_profile_pics": {},
                    }
                }
            ]
        if "from social.instagram_comments c" in query:
            return []
        return []

    monkeypatch.setattr(orchestrator, "_social_repo", lambda: fake_repo)
    monkeypatch.setattr(orchestrator.pg, "fetch_all", fake_fetch_all)

    snapshot = orchestrator._build_avatar_coverage_snapshot(
        season_id="season-1",
        platforms=["instagram"],
        source_scope="bravo",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert snapshot["missing_avatar_count"] == 0
    assert snapshot["up_to_date"] is True


def test_build_missing_detail_targets_includes_instagram_comment_media_gaps(monkeypatch) -> None:
    fake_rows = [
        {"source_id": "abc123", "post_json": {"mentions": [], "hosted_tagged_profile_pics": {}}},
    ]

    def fake_fetch_all(query: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        if "from social.avatar_registry" in query:
            return []
        if "from social.instagram_posts p" in query:
            return fake_rows
        if "from social.instagram_comments c" in query and "media_urls" in query:
            return [{"source_id": "abc123"}]
        return []

    monkeypatch.setattr(orchestrator, "_social_repo", lambda: _FakeSocialRepo())
    monkeypatch.setattr(orchestrator.pg, "fetch_all", fake_fetch_all)

    targets = orchestrator._build_missing_detail_targets(
        season_id="season-1",
        platforms=["instagram"],
        source_scope="bravo",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert targets["instagram"] == ["abc123"]


def test_build_missing_comment_targets_uses_threads_reply_and_quote_counts(monkeypatch) -> None:
    fake_repo = _FakeSocialRepo()
    fake_repo.PLATFORM_POST_TABLES = {"threads": "meta_threads_posts"}
    fake_repo.PLATFORM_COMMENT_TABLES = {"threads": "meta_threads_comments"}
    fake_repo.PLATFORM_POSTED_AT_COLUMN = {"threads": "posted_at"}
    fake_repo.PLATFORM_SOURCE_ID_COLUMN = {"threads": "post_id"}
    captured_queries: list[str] = []

    def fake_fetch_all(query: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        captured_queries.append(query)
        return [{"source_id": "threads-post-1"}]

    monkeypatch.setattr(orchestrator, "_social_repo", lambda: fake_repo)
    monkeypatch.setattr(orchestrator.pg, "fetch_all", fake_fetch_all)

    targets = orchestrator._build_missing_comment_targets(
        season_id="season-1",
        platforms=["threads"],
        source_scope="bravo",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert targets == {"threads": ["threads-post-1"]}
    assert len(captured_queries) == 1
    assert "coalesce(p.replies_count, 0) + coalesce(p.quotes, 0)" in captured_queries[0]
    assert "coalesce(p.comments_count, 0)" not in captured_queries[0]
    assert (
        "greatest((coalesce(p.replies_count, 0) + coalesce(p.quotes, 0)) - coalesce(cc.saved_comments, 0), 0) > %s"
        in " ".join(captured_queries[0].split())
    )


def test_create_sync_session_preserves_attached_status(monkeypatch) -> None:
    monkeypatch.setattr(orchestrator, "_sync_sessions_ready", lambda: True)
    monkeypatch.setattr(orchestrator, "_coerce_dt", lambda value: value)
    monkeypatch.setattr(orchestrator, "build_sync_profile", lambda **kwargs: kwargs["base_config"])
    monkeypatch.setattr(orchestrator, "_build_dedup_key", lambda **_kwargs: "dedup-key")
    monkeypatch.setattr(
        orchestrator,
        "_social_repo",
        lambda: SimpleNamespace(get_season_context=lambda _season_id: SimpleNamespace(show_id="show-1")),
    )
    monkeypatch.setattr(
        orchestrator.pg,
        "fetch_one",
        lambda query, _params=None: {"id": "sync-1"} if "from social.sync_sessions" in query else None,
    )
    monkeypatch.setattr(
        orchestrator,
        "get_sync_session",
        lambda _sync_session_id: {
            "sync_session_id": "sync-1",
            "status": "pass_running",
            "current_pass_kind": "posts_and_comments",
        },
    )

    result = orchestrator.create_sync_session(
        "season-1",
        source_scope="bravo",
        platforms=["instagram"],
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 2, tzinfo=UTC),
        config={"platforms": ["instagram"]},
    )

    assert result["status"] == "attached"
    assert result["current_pass_kind"] == "posts_and_comments"


def test_create_sync_session_preserves_created_status(monkeypatch) -> None:
    monkeypatch.setattr(orchestrator, "_sync_sessions_ready", lambda: True)
    monkeypatch.setattr(orchestrator, "_coerce_dt", lambda value: value)
    monkeypatch.setattr(orchestrator, "build_sync_profile", lambda **kwargs: kwargs["base_config"])
    monkeypatch.setattr(orchestrator, "_build_dedup_key", lambda **_kwargs: "dedup-key")
    monkeypatch.setattr(
        orchestrator,
        "_build_completeness_snapshot",
        lambda **_kwargs: {"up_to_date": False},
    )
    monkeypatch.setattr(
        orchestrator,
        "_social_repo",
        lambda: SimpleNamespace(get_season_context=lambda _season_id: SimpleNamespace(show_id="show-1")),
    )

    def fake_fetch_one(query: str, _params=None):
        if "from social.sync_sessions" in query:
            return None
        if "insert into social.sync_sessions" in query:
            return {"id": "sync-2"}
        return None

    monkeypatch.setattr(orchestrator.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(
        orchestrator,
        "_start_sync_pass",
        lambda *_args, **_kwargs: {"run_id": "run-2"},
    )
    monkeypatch.setattr(
        orchestrator,
        "get_sync_session",
        lambda _sync_session_id: {
            "sync_session_id": "sync-2",
            "status": "pass_running",
            "current_pass_kind": "posts_and_comments",
            "current_run_id": "old-run",
        },
    )

    result = orchestrator.create_sync_session(
        "season-1",
        source_scope="bravo",
        platforms=["instagram"],
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 2, tzinfo=UTC),
        config={"platforms": ["instagram"]},
    )

    assert result["status"] == "created"
    assert result["current_run_id"] == "run-2"


def test_build_sync_profile_uses_platform_specific_defaults() -> None:
    profile = orchestrator.build_sync_profile(
        season_id="season-1",
        source_scope="bravo",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 1, 8, tzinfo=UTC),
        base_config={"platforms": ["tiktok"], "fetch_replies": True},
    )

    assert profile["runner_strategy"] == "single_runner"
    assert profile["runner_count"] == 1
    assert profile["window_shard_hours"] == 24


def test_build_sync_profile_applies_bounded_depth_defaults_for_single_platform_syncs() -> None:
    for platform in ("tiktok", "facebook", "threads", "twitter"):
        profile = orchestrator.build_sync_profile(
            season_id="season-1",
            source_scope="bravo",
            date_start=datetime(2026, 1, 1, tzinfo=UTC),
            date_end=datetime(2026, 1, 2, tzinfo=UTC),
            base_config={
                "platforms": [platform],
                "max_comments_per_post": 0,
                "max_replies_per_post": 0,
            },
        )

        assert profile["max_comments_per_post"] == 5_000
        assert profile["max_replies_per_post"] == 1_000


def test_serialize_sync_session_includes_platform_diagnostics(monkeypatch) -> None:
    monkeypatch.setattr(orchestrator, "_social_repo", lambda: _FakeSocialRepo())
    monkeypatch.setenv("SOCIAL_CRAWLEE_ENABLED", "1")
    monkeypatch.setenv("SOCIAL_CRAWLEE_PLATFORMS", "twitter,tiktok")
    monkeypatch.delenv("SOCIAL_TWITTER_COOKIES_JSON", raising=False)
    monkeypatch.delenv("SOCIAL_TWITTER_COOKIES_FILE", raising=False)
    monkeypatch.delenv("TWITTER_COOKIES_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TWITTER_COOKIES_CT0", raising=False)
    monkeypatch.setenv("SOCIAL_TWITTER_BEARER_TOKEN", "token")

    payload = orchestrator._serialize_sync_session(
        {
            "id": "sync-1",
            "season_id": "season-1",
            "show_id": "show-1",
            "source_scope": "bravo",
            "platforms": ["twitter"],
            "status": "pass_running",
            "current_pass_kind": "posts_and_comments",
            "current_pass_attempt": 1,
            "current_run_id": None,
            "pass_sequence": 4,
            "pass_history": [],
            "follow_up_reason": "coverage_gap",
            "completeness_snapshot": {
                "up_to_date": False,
                "follow_up_dimensions": ["comments"],
                "comments_coverage": {
                    "up_to_date": False,
                    "by_platform": {
                        "twitter": {
                            "reported_comments": 10,
                            "saved_comments": 7,
                            "reported_replies": 6,
                            "saved_replies": 5,
                            "reported_quotes": 4,
                            "saved_quotes": 2,
                        }
                    },
                },
                "asset_coverage": {"up_to_date": True},
                "comment_media_coverage": {"up_to_date": True},
                "avatar_coverage": {"up_to_date": True},
                "comment_target_count": 3,
            },
            "sync_config": {"platforms": ["twitter"], "fetch_replies": True},
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
            "updated_at": datetime(2026, 1, 1, tzinfo=UTC),
        }
    )

    assert payload["queue_wait_state"] == "ready"
    assert payload["execution_path"] == "crawlee"
    assert payload["auth_mode"] == "bearer"
    assert payload["worker_version"] == "worker-test-1"
    assert payload["follow_up_breakdown"]["comments"] == 3
    assert payload["coverage_by_dimension"]["comments"]["by_platform"]["twitter"]["reported_comments"] == 10
    assert payload["platform_diagnostics"]["twitter"]["coverage_by_dimension"]["comments"]["reported_replies"] == 6
    assert payload["platform_diagnostics"]["twitter"]["coverage_by_dimension"]["comments"]["saved_quotes"] == 2
    assert payload["platform_diagnostics"]["twitter"]["queue_cap"] == 2


def test_hold_sync_session_lock_uses_single_connection(monkeypatch) -> None:
    seen_connections: list[object] = []

    @contextmanager
    def fake_advisory_lock(lock_key, *, label, pool_name="default"):
        del lock_key, label, pool_name
        conn = object()
        seen_connections.append(conn)
        yield conn

    monkeypatch.setattr(orchestrator.pg, "advisory_session_lock", fake_advisory_lock)

    with orchestrator._hold_sync_session_lock("abc-123") as conn:
        assert conn is seen_connections[0]

    assert len(seen_connections) == 1
