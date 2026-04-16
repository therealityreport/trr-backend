from __future__ import annotations

import pytest


def test_job_runner_rejects_missing_account():
    from trr_backend.socials.instagram.posts_scrapling.job_runner import (
        PostsScraplingRuntimeError,
        run_instagram_posts_scrapling_job,
    )

    job = {"id": "job-1", "run_id": "run-1", "config": {"account": ""}}
    with pytest.raises(PostsScraplingRuntimeError, match="missing an account"):
        run_instagram_posts_scrapling_job(job)
