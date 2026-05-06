from __future__ import annotations

from types import SimpleNamespace

from trr_backend.repositories import social_season_analytics as repo
from trr_backend.socials.instagram.comments_scrapling import job_runner as comments_job_runner
from trr_backend.socials.pipelines.job_handlers import resolve_platform_job_handler


def test_stage_claim_candidates_do_not_let_comments_workers_borrow_posts() -> None:
    assert repo._stage_claim_candidates("comments") == ("comments",)  # noqa: SLF001
    assert repo._stage_claim_candidates("comments_scrapling") == ("comments_scrapling",)  # noqa: SLF001
    assert repo._stage_claim_candidates("shared_account_posts") == ("shared_account_posts",)  # noqa: SLF001


def test_effective_runtime_version_tracks_stage_specific_modal_function(monkeypatch) -> None:
    monkeypatch.setattr(
        repo,
        "_resolve_authoritative_catalog_runtime_version",
        lambda *, required_execution_backend=None: {
            "execution_backend": required_execution_backend,
            "label": "modal",
            "modal_function": "run_social_job",
        },
    )

    comments_runtime = repo._resolve_effective_runtime_version(  # noqa: SLF001
        required_execution_backend="modal",
        stage="comments_scrapling",
    )
    media_runtime = repo._resolve_effective_runtime_version(  # noqa: SLF001
        required_execution_backend="modal",
        stage="media_mirror",
    )
    posts_runtime = repo._resolve_effective_runtime_version(  # noqa: SLF001
        required_execution_backend="modal",
        stage="shared_account_posts",
    )

    assert comments_runtime["modal_function"] == "run_social_comments_job"
    assert media_runtime["modal_function"] == "run_social_media_job"
    assert posts_runtime["modal_function"] == "run_social_posts_job"


def test_comments_scrapling_attempt_defaults_match_queue_defaults() -> None:
    assert comments_job_runner._job_attempt_state({}) == (1, 12)  # noqa: SLF001
    assert comments_job_runner._job_attempt_state({"attempt_count": 3, "max_attempts": 12}) == (3, 12)  # noqa: SLF001


def test_platform_job_handler_registry_resolves_known_stages_once() -> None:
    expected = (
        ("instagram", "comments_scrapling"),
        ("instagram", "posts_scrapling"),
        ("instagram", "instagram_profile_snapshot"),
        ("instagram", "instagram_profile_following"),
        ("tiktok", "tiktok_posts_scrapling"),
        ("threads", "threads_posts_scrapling"),
    )
    for platform, stage in expected:
        handler = resolve_platform_job_handler(platform, stage)
        assert handler is not None
        assert handler.supports(platform, stage)

    assert resolve_platform_job_handler("instagram", "unknown_stage") is None


def test_comments_scrapling_completion_uses_flattened_reply_count() -> None:
    result = SimpleNamespace(
        comments=[
            SimpleNamespace(replies=[SimpleNamespace(replies=[]), SimpleNamespace(replies=[])]),
            SimpleNamespace(replies=[]),
        ],
        fetch_failed=False,
        auth_failed=False,
        reported_comment_count=4,
    )

    assert comments_job_runner._comments_scrape_is_complete(result=result, max_comments_per_post=0) is True  # noqa: SLF001
