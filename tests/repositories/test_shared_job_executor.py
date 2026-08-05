from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from trr_backend.repositories import social_season_analytics as repo
from trr_backend.socials.instagram import jobs as instagram_jobs
from trr_backend.socials.instagram.comments_scrapling import job_runner as instagram_comments_job_runner
from trr_backend.socials.instagram.posts_scrapling import job_runner as instagram_posts_job_runner
from trr_backend.socials.pipelines import shared_job_executor
from trr_backend.socials.pipelines.job_handler_types import FunctionPlatformJobHandler
from trr_backend.socials.pipelines.job_handlers import (
    registered_platform_job_handlers,
    resolve_platform_job_handler,
)
from trr_backend.socials.threads import jobs as threads_jobs
from trr_backend.socials.threads.posts_scrapling import job_runner as threads_job_runner
from trr_backend.socials.tiktok import jobs as tiktok_jobs
from trr_backend.socials.tiktok.posts_scrapling import job_runner as tiktok_job_runner


def test_shared_job_executor_requires_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shared_job_executor, "_shared_claimed_job_executor", None)

    with pytest.raises(RuntimeError, match="shared claimed-job executor is not configured"):
        shared_job_executor.execute_shared_claimed_job({"id": "job-1"}, worker_id="worker-1")


def test_shared_job_executor_forwards_job_and_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    expected = {"id": "job-1", "status": "completed"}

    def fake_executor(job: Mapping[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
        captured["job"] = job
        captured["worker_id"] = worker_id
        return expected

    monkeypatch.setattr(shared_job_executor, "_shared_claimed_job_executor", None)
    shared_job_executor.configure_shared_claimed_job_executor(fake_executor)
    job = {"id": "job-1"}

    result = shared_job_executor.execute_shared_claimed_job(job, worker_id="worker-1")

    assert result is expected
    assert captured == {"job": job, "worker_id": "worker-1"}


def test_instagram_profile_handler_observes_compatibility_repo_monkeypatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    expected = {"id": "profile-job-1", "status": "completed"}

    def fake_executor(job: Mapping[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
        captured["job"] = job
        captured["worker_id"] = worker_id
        return expected

    monkeypatch.setattr(repo, "_execute_shared_claimed_job", fake_executor)
    handler = resolve_platform_job_handler("instagram", "instagram_profile_snapshot")
    job = {
        "id": "profile-job-1",
        "platform": "instagram",
        "config": {"stage": "instagram_profile_snapshot"},
    }

    assert handler is not None
    assert handler.execute(job, worker_id="worker-1") is expected
    assert captured == {"job": job, "worker_id": "worker-1"}


@pytest.mark.parametrize(
    ("stage", "target_module", "function_name"),
    (
        (
            instagram_jobs.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
            instagram_comments_job_runner,
            "run_instagram_comments_scrapling_job",
        ),
        (
            instagram_jobs.INSTAGRAM_POSTS_SCRAPLING_STAGE,
            instagram_posts_job_runner,
            "run_instagram_posts_scrapling_job",
        ),
        (
            instagram_jobs.INSTAGRAM_PROFILE_SNAPSHOT_STAGE,
            shared_job_executor,
            "execute_shared_claimed_job",
        ),
        (
            instagram_jobs.INSTAGRAM_PROFILE_FOLLOWING_STAGE,
            shared_job_executor,
            "execute_shared_claimed_job",
        ),
    ),
    ids=("comments", "posts", "profile-snapshot", "profile-following"),
)
def test_instagram_handlers_delegate_to_canonical_execution_seams(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    target_module: object,
    function_name: str,
) -> None:
    captured: dict[str, Any] = {}
    expected = {"id": f"instagram-{stage}-job-1", "status": "completed"}
    job = {
        "id": f"instagram-{stage}-job-1",
        "platform": "instagram",
        "config": {"stage": stage},
    }

    def fake_executor(job_arg: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
        captured["job"] = job_arg
        captured["worker_id"] = worker_id
        return expected

    def forbid_compatibility_registry() -> tuple[object, ...]:
        raise AssertionError("central registry must bind canonical Instagram execution seams directly")

    monkeypatch.setattr(target_module, function_name, fake_executor)
    monkeypatch.setattr(instagram_jobs, "instagram_job_handlers", forbid_compatibility_registry)

    handler = resolve_platform_job_handler("instagram", stage)

    assert handler is not None
    assert handler.execute(job, worker_id="worker-instagram-1") is expected
    assert captured["job"] is job
    assert captured["worker_id"] == "worker-instagram-1"


def test_instagram_compatibility_job_handlers_preserve_type_stages_and_order() -> None:
    assert callable(instagram_jobs.instagram_job_handlers)
    handlers = instagram_jobs.instagram_job_handlers()

    assert tuple((type(handler), handler.platform, handler.stage) for handler in handlers) == (
        (FunctionPlatformJobHandler, "instagram", instagram_jobs.INSTAGRAM_COMMENTS_SCRAPLING_STAGE),
        (FunctionPlatformJobHandler, "instagram", instagram_jobs.INSTAGRAM_POSTS_SCRAPLING_STAGE),
        (FunctionPlatformJobHandler, "instagram", instagram_jobs.INSTAGRAM_PROFILE_SNAPSHOT_STAGE),
        (FunctionPlatformJobHandler, "instagram", instagram_jobs.INSTAGRAM_PROFILE_FOLLOWING_STAGE),
    )


def test_threads_handler_delegates_to_canonical_posts_scrapling_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    expected = {"id": "threads-job-1", "status": "completed"}
    job = {
        "id": "threads-job-1",
        "platform": "threads",
        "config": {"stage": threads_jobs.THREADS_POSTS_SCRAPLING_STAGE},
    }

    def fake_runner(job_arg: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
        captured["job"] = job_arg
        captured["worker_id"] = worker_id
        return expected

    def forbid_compatibility_registry() -> tuple[object, ...]:
        raise AssertionError("central registry must bind the canonical Threads job runner directly")

    monkeypatch.setattr(threads_job_runner, "run_threads_posts_scrapling_job", fake_runner)
    monkeypatch.setattr(threads_jobs, "threads_job_handlers", forbid_compatibility_registry)

    handler = resolve_platform_job_handler("threads", threads_jobs.THREADS_POSTS_SCRAPLING_STAGE)

    assert handler is not None
    assert handler.execute(job, worker_id="worker-threads-1") is expected
    assert captured["job"] is job
    assert captured["worker_id"] == "worker-threads-1"


def test_threads_compatibility_job_handlers_preserve_type_stage_and_order() -> None:
    assert callable(threads_jobs.threads_job_handlers)
    handlers = threads_jobs.threads_job_handlers()

    assert tuple((type(handler), handler.platform, handler.stage) for handler in handlers) == (
        (FunctionPlatformJobHandler, "threads", threads_jobs.THREADS_POSTS_SCRAPLING_STAGE),
    )


def test_tiktok_handler_delegates_to_canonical_posts_scrapling_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    expected = {"id": "tiktok-job-1", "status": "completed"}
    job = {
        "id": "tiktok-job-1",
        "platform": "tiktok",
        "config": {"stage": tiktok_jobs.TIKTOK_POSTS_SCRAPLING_STAGE},
    }

    def fake_runner(job_arg: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
        captured["job"] = job_arg
        captured["worker_id"] = worker_id
        return expected

    def forbid_compatibility_registry() -> tuple[object, ...]:
        raise AssertionError("central registry must bind the canonical TikTok job runner directly")

    monkeypatch.setattr(tiktok_job_runner, "run_tiktok_posts_scrapling_job", fake_runner)
    monkeypatch.setattr(tiktok_jobs, "tiktok_job_handlers", forbid_compatibility_registry)

    handler = resolve_platform_job_handler("tiktok", tiktok_jobs.TIKTOK_POSTS_SCRAPLING_STAGE)

    assert handler is not None
    assert handler.execute(job, worker_id="worker-tiktok-1") is expected
    assert captured["job"] is job
    assert captured["worker_id"] == "worker-tiktok-1"


def test_tiktok_compatibility_job_handlers_preserve_type_stage_and_order() -> None:
    assert callable(tiktok_jobs.tiktok_job_handlers)
    handlers = tiktok_jobs.tiktok_job_handlers()

    assert tuple((type(handler), handler.platform, handler.stage) for handler in handlers) == (
        (FunctionPlatformJobHandler, "tiktok", tiktok_jobs.TIKTOK_POSTS_SCRAPLING_STAGE),
    )


def test_registered_platform_job_handlers_preserve_order_stages_and_types() -> None:
    handlers = registered_platform_job_handlers()

    assert tuple((handler.platform, handler.stage) for handler in handlers) == (
        ("instagram", "comments_scrapling"),
        ("instagram", "posts_scrapling"),
        ("instagram", "instagram_profile_snapshot"),
        ("instagram", "instagram_profile_following"),
        ("tiktok", "tiktok_posts_scrapling"),
        ("threads", "threads_posts_scrapling"),
    )
    assert all(isinstance(handler, FunctionPlatformJobHandler) for handler in handlers)
