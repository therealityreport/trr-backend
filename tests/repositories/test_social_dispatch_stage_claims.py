from __future__ import annotations

from trr_backend.repositories import social_season_analytics as repo


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
