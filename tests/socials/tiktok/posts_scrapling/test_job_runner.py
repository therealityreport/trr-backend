from __future__ import annotations

import pytest


def test_tiktok_job_runner_rejects_missing_account():
    from trr_backend.socials.tiktok.posts_scrapling.job_runner import (
        TikTokPostsScraplingRuntimeError,
        run_tiktok_posts_scrapling_job,
    )

    job = {"id": "job-1", "run_id": "run-1", "config": {"account": ""}}
    with pytest.raises(TikTokPostsScraplingRuntimeError, match="missing an account"):
        run_tiktok_posts_scrapling_job(job)
