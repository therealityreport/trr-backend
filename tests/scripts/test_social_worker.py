from __future__ import annotations

from types import SimpleNamespace

import scripts.socials.worker as worker


def test_requires_media_mirror_s3_preflight() -> None:
    assert worker._requires_media_mirror_s3_preflight(stage="any", platform=None) is False  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="media_mirror", platform="instagram") is True  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="comment_media_mirror", platform="twitter") is True  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="media_mirror", platform="tiktok") is True  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="media_mirror", platform="youtube") is True  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="media_mirror", platform="twitter") is True  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="posts", platform=None) is False  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="comments", platform="instagram") is False  # noqa: SLF001


def test_claim_stage_candidates_supports_comments_scrapling() -> None:
    assert worker._claim_stage_candidates("comments_scrapling") == ("comments_scrapling",)  # noqa: SLF001


def test_claim_stage_candidates_preserves_threads_posts_scrapling() -> None:
    assert worker._claim_stage_candidates("threads_posts_scrapling") == ("threads_posts_scrapling",)  # noqa: SLF001


def test_worker_heartbeat_seeds_auth_capabilities(monkeypatch) -> None:
    monkeypatch.setattr(
        worker,
        "get_worker_auth_capabilities",
        lambda: {"instagram_authenticated": True, "tiktok_authenticated": False},
    )

    heartbeat = worker.WorkerHeartbeat(worker_id="worker-1", stage="posts", run_id=None)
    snapshot = heartbeat._snapshot()  # noqa: SLF001

    assert snapshot["metadata"]["auth_capabilities"] == {
        "instagram_authenticated": True,
        "tiktok_authenticated": False,
    }


def test_main_queue_parallel_fanout_spawns_child_workers(monkeypatch) -> None:
    spawned_cmds: list[list[str]] = []

    class _FakeProc:
        def wait(self) -> int:
            return 0

    def _fake_popen(cmd, cwd=None, env=None):  # noqa: ANN001
        spawned_cmds.append(list(cmd))
        return _FakeProc()

    def _fail_heartbeat(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("heartbeat should not start in queue fanout parent")

    monkeypatch.setattr(
        worker,
        "parse_args",
        lambda: SimpleNamespace(
            worker_id="root-worker",
            interval=2.5,
            once=False,
            run_id=None,
            parallel=3,
            stage="comments",
            tandem=False,
            posts_workers=1,
            comments_workers=1,
            platform=None,
        ),
    )
    monkeypatch.setattr(worker.logging, "basicConfig", lambda **_kwargs: None)
    monkeypatch.setattr(worker.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(worker, "WorkerHeartbeat", _fail_heartbeat)
    monkeypatch.setattr(worker, "load_env", lambda: None)
    monkeypatch.setattr(worker, "ensure_media_mirror_s3_ready", lambda: None)

    rc = worker.main()

    assert rc == 0
    assert len(spawned_cmds) == 3
    for index, cmd in enumerate(spawned_cmds, start=1):
        assert cmd[:3] == [worker.sys.executable, "-m", "scripts.socials.worker"]
        assert "--worker-id" in cmd
        assert cmd[cmd.index("--worker-id") + 1] == f"root-worker:p{index}"
        assert "--parallel" in cmd
        assert cmd[cmd.index("--parallel") + 1] == "1"
        assert "--stage" in cmd
        assert cmd[cmd.index("--stage") + 1] == "comments"
        assert "--interval" in cmd
        assert cmd[cmd.index("--interval") + 1] == "2.5"


def test_main_queue_parallel_fanout_stops_siblings_on_child_failure(monkeypatch) -> None:
    class _FakeProc:
        def __init__(self, rc: int) -> None:
            self.rc = rc
            self.terminated = False
            self.killed = False
            self.wait_calls = 0

        def wait(self, timeout=None) -> int:  # noqa: ANN001
            self.wait_calls += 1
            return self.rc

        def poll(self) -> int | None:
            if self.terminated or self.killed or self.wait_calls > 0:
                return self.rc
            return None

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True

    spawned: list[_FakeProc] = []

    def _fake_popen(cmd, cwd=None, env=None):  # noqa: ANN001
        del cmd, cwd, env
        rc = 4 if not spawned else 0
        proc = _FakeProc(rc=rc)
        spawned.append(proc)
        return proc

    monkeypatch.setattr(
        worker,
        "parse_args",
        lambda: SimpleNamespace(
            worker_id="root-worker",
            interval=2.5,
            once=False,
            run_id=None,
            parallel=2,
            stage="comments",
            tandem=False,
            posts_workers=1,
            comments_workers=1,
            platform=None,
        ),
    )
    monkeypatch.setattr(worker.logging, "basicConfig", lambda **_kwargs: None)
    monkeypatch.setattr(worker.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(worker, "WorkerHeartbeat", lambda **_kwargs: None)
    monkeypatch.setattr(worker, "load_env", lambda: None)
    monkeypatch.setattr(worker, "ensure_media_mirror_s3_ready", lambda: None)

    rc = worker.main()

    assert rc == 4
    assert len(spawned) == 2
    assert spawned[1].terminated is True


def test_main_fails_fast_when_mirror_s3_preflight_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        worker,
        "parse_args",
        lambda: SimpleNamespace(
            worker_id="root-worker",
            interval=1.0,
            once=True,
            run_id=None,
            parallel=1,
            stage="media_mirror",
            tandem=False,
            posts_workers=1,
            comments_workers=1,
            platform=None,
        ),
    )
    monkeypatch.setattr(worker.logging, "basicConfig", lambda **_kwargs: None)
    monkeypatch.setattr(worker, "load_env", lambda: None)

    def _fail_preflight() -> None:
        raise RuntimeError("Missing required environment variable: OBJECT_STORAGE_BUCKET")

    monkeypatch.setattr(worker, "ensure_media_mirror_s3_ready", _fail_preflight)

    rc = worker.main()

    assert rc == 2


def test_main_stage_any_skips_preflight_and_processes_non_mirror_job(monkeypatch) -> None:
    process_calls: list[str] = []

    class _FakeHeartbeat:
        def __init__(self, **_kwargs):  # noqa: ANN003
            return

        def start(self) -> None:
            return

        def stop(self, *, reason: str = "shutdown") -> None:
            del reason
            return

        def set_state(self, **_kwargs):  # noqa: ANN003
            return

    monkeypatch.setattr(
        worker,
        "parse_args",
        lambda: SimpleNamespace(
            worker_id="root-worker",
            interval=0.1,
            once=True,
            run_id=None,
            parallel=1,
            stage="any",
            tandem=False,
            posts_workers=1,
            comments_workers=1,
            platform=None,
        ),
    )
    monkeypatch.setattr(worker.logging, "basicConfig", lambda **_kwargs: None)
    monkeypatch.setattr(worker, "load_env", lambda: None)
    monkeypatch.setattr(worker, "WorkerHeartbeat", _FakeHeartbeat)
    monkeypatch.setattr(
        worker,
        "ensure_media_mirror_s3_ready",
        lambda: (_ for _ in ()).throw(RuntimeError("preflight should be skipped for stage=any")),
    )
    monkeypatch.setattr(worker, "recover_stale_running_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(worker, "reconcile_run_summaries", lambda **_kwargs: {"reconciled_runs": 0, "run_ids": []})
    monkeypatch.setattr(
        worker,
        "claim_next_queued_jobs",
        lambda **_kwargs: [
            {
                "id": "job-comments-1",
                "run_id": "run-1",
                "platform": "instagram",
                "config": {"stage": "comments"},
            }
        ],
    )
    monkeypatch.setattr(worker, "cancel_claimed_job_before_processing", lambda _job: None)
    monkeypatch.setattr(
        worker,
        "process_claimed_job",
        lambda job, **_kwargs: (
            process_calls.append(str(job.get("id") or "")),
            {
                "id": str(job.get("id") or ""),
                "run_id": str(job.get("run_id") or ""),
                "platform": str(job.get("platform") or ""),
                "status": "completed",
                "items_found": 1,
            },
        )[1],
    )

    rc = worker.main()

    assert rc == 0
    assert process_calls == ["job-comments-1"]


def test_main_queue_once_uses_claim_batch_and_processes_claimed_job(monkeypatch) -> None:
    stale_calls: list[dict[str, object]] = []
    reconcile_calls: list[int] = []
    claim_calls: list[int] = []
    process_calls: list[str] = []

    class _FakeHeartbeat:
        def __init__(self, **_kwargs):  # noqa: ANN003
            return

        def start(self) -> None:
            return

        def stop(self, *, reason: str = "shutdown") -> None:
            del reason
            return

        def set_state(self, **_kwargs):  # noqa: ANN003
            return

    claimed_payload = [
        {
            "id": "job-1",
            "run_id": "run-1",
            "platform": "twitter",
            "config": {"stage": "comments"},
        }
    ]

    monkeypatch.setattr(
        worker,
        "parse_args",
        lambda: SimpleNamespace(
            worker_id="root-worker",
            interval=0.1,
            once=True,
            run_id=None,
            parallel=1,
            stage="comments",
            tandem=False,
            posts_workers=1,
            comments_workers=1,
            platform=None,
        ),
    )
    monkeypatch.setattr(worker.logging, "basicConfig", lambda **_kwargs: None)
    monkeypatch.setattr(worker, "load_env", lambda: None)
    monkeypatch.setattr(worker, "WorkerHeartbeat", _FakeHeartbeat)
    monkeypatch.setattr(worker, "ensure_media_mirror_s3_ready", lambda: None)
    monkeypatch.setattr(
        worker,
        "recover_stale_running_jobs",
        lambda **kwargs: stale_calls.append(dict(kwargs)) or [],
    )
    monkeypatch.setattr(
        worker,
        "reconcile_run_summaries",
        lambda **_kwargs: reconcile_calls.append(1) or {"reconciled_runs": 0, "run_ids": []},
    )

    def _fake_claim_next_queued_jobs(**kwargs):  # noqa: ANN001
        claim_calls.append(int(kwargs.get("limit") or 0))
        if claimed_payload:
            return [claimed_payload.pop(0)]
        return []

    monkeypatch.setattr(worker, "claim_next_queued_jobs", _fake_claim_next_queued_jobs)
    monkeypatch.setattr(worker, "cancel_claimed_job_before_processing", lambda _job: None)
    monkeypatch.setattr(
        worker,
        "process_claimed_job",
        lambda job, **_kwargs: (
            process_calls.append(str(job.get("id") or "")),
            {
                "id": str(job.get("id") or ""),
                "run_id": str(job.get("run_id") or ""),
                "platform": str(job.get("platform") or ""),
                "status": "completed",
                "items_found": 11,
            },
        )[1],
    )

    rc = worker.main()

    assert rc == 0
    assert process_calls == ["job-1"]
    assert len(stale_calls) == 1
    assert len(reconcile_calls) == 1
    assert claim_calls
    assert claim_calls[0] >= 1


def test_main_queue_comments_worker_claims_posts_when_comment_queue_empty(monkeypatch) -> None:
    claim_calls: list[tuple[str | None, int]] = []
    process_calls: list[str] = []

    class _FakeHeartbeat:
        def __init__(self, **_kwargs):  # noqa: ANN003
            return

        def start(self) -> None:
            return

        def stop(self, *, reason: str = "shutdown") -> None:
            del reason
            return

        def set_state(self, **_kwargs):  # noqa: ANN003
            return

    def _fake_claim_next_queued_jobs(**kwargs):  # noqa: ANN001
        stage = kwargs.get("stage")
        limit = int(kwargs.get("limit") or 0)
        claim_calls.append((str(stage) if stage is not None else None, limit))
        if stage == "comments":
            return []
        if stage == "posts":
            return [{"id": "job-post-1", "run_id": "run-1", "platform": "youtube", "config": {"stage": "posts"}}]
        return []

    monkeypatch.setattr(
        worker,
        "parse_args",
        lambda: SimpleNamespace(
            worker_id="root-worker",
            interval=0.1,
            once=True,
            run_id=None,
            parallel=1,
            stage="comments",
            tandem=False,
            posts_workers=1,
            comments_workers=1,
            platform=None,
        ),
    )
    monkeypatch.setattr(worker.logging, "basicConfig", lambda **_kwargs: None)
    monkeypatch.setattr(worker, "load_env", lambda: None)
    monkeypatch.setattr(worker, "WorkerHeartbeat", _FakeHeartbeat)
    monkeypatch.setattr(worker, "ensure_media_mirror_s3_ready", lambda: None)
    monkeypatch.setattr(worker, "recover_stale_running_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(worker, "reconcile_run_summaries", lambda **_kwargs: {"reconciled_runs": 0, "run_ids": []})
    monkeypatch.setattr(worker, "claim_next_queued_jobs", _fake_claim_next_queued_jobs)
    monkeypatch.setattr(worker, "cancel_claimed_job_before_processing", lambda _job: None)
    monkeypatch.setattr(
        worker,
        "process_claimed_job",
        lambda job, **_kwargs: (
            process_calls.append(str(job.get("id") or "")),
            {
                "id": str(job.get("id") or ""),
                "run_id": str(job.get("run_id") or ""),
                "platform": str(job.get("platform") or ""),
                "status": "completed",
                "items_found": 7,
            },
        )[1],
    )

    rc = worker.main()

    assert rc == 0
    assert claim_calls == [("comments", 2), ("posts", 1)]
    assert process_calls == ["job-post-1"]


def test_main_queue_once_discards_claimed_jobs_for_cancelled_runs(monkeypatch) -> None:
    cancelled_checks: list[str] = []
    process_calls: list[str] = []

    class _FakeHeartbeat:
        def __init__(self, **_kwargs):  # noqa: ANN003
            return

        def start(self) -> None:
            return

        def stop(self, *, reason: str = "shutdown") -> None:
            del reason
            return

        def set_state(self, **_kwargs):  # noqa: ANN003
            return

    claimed_payload = [
        {
            "id": "job-cancelled",
            "run_id": "run-cancelled",
            "platform": "twitter",
            "config": {"stage": "posts"},
        }
    ]

    monkeypatch.setattr(
        worker,
        "parse_args",
        lambda: SimpleNamespace(
            worker_id="root-worker",
            interval=0.1,
            once=True,
            run_id=None,
            parallel=1,
            stage="posts",
            tandem=False,
            posts_workers=1,
            comments_workers=1,
            platform=None,
        ),
    )
    monkeypatch.setattr(worker.logging, "basicConfig", lambda **_kwargs: None)
    monkeypatch.setattr(worker, "load_env", lambda: None)
    monkeypatch.setattr(worker, "WorkerHeartbeat", _FakeHeartbeat)
    monkeypatch.setattr(worker, "ensure_media_mirror_s3_ready", lambda: None)
    monkeypatch.setattr(worker, "recover_stale_running_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(worker, "reconcile_run_summaries", lambda **_kwargs: {"reconciled_runs": 0, "run_ids": []})
    monkeypatch.setattr(
        worker,
        "claim_next_queued_jobs",
        lambda **_kwargs: [claimed_payload.pop(0)] if claimed_payload else [],
    )
    monkeypatch.setattr(
        worker,
        "cancel_claimed_job_before_processing",
        lambda job: (
            cancelled_checks.append(str(job.get("id") or "")),
            {
                "id": str(job.get("id") or ""),
                "run_id": str(job.get("run_id") or ""),
                "platform": str(job.get("platform") or ""),
                "status": "cancelled",
                "items_found": 0,
            },
        )[1],
    )
    monkeypatch.setattr(
        worker,
        "process_claimed_job",
        lambda job, **_kwargs: (process_calls.append(str(job.get("id") or "")), job)[1],
    )

    rc = worker.main()

    assert rc == 0
    assert cancelled_checks == ["job-cancelled"]
    assert process_calls == []


def test_main_queue_once_continues_when_cancel_probe_fails(monkeypatch) -> None:
    process_calls: list[str] = []

    class _FakeHeartbeat:
        def __init__(self, **_kwargs):  # noqa: ANN003
            return

        def start(self) -> None:
            return

        def stop(self, *, reason: str = "shutdown") -> None:
            del reason
            return

        def set_state(self, **_kwargs):  # noqa: ANN003
            return

    claimed_payload = [
        {
            "id": "job-after-probe-error",
            "run_id": "run-1",
            "platform": "twitter",
            "config": {"stage": "posts"},
        }
    ]

    monkeypatch.setattr(
        worker,
        "parse_args",
        lambda: SimpleNamespace(
            worker_id="root-worker",
            interval=0.1,
            once=True,
            run_id=None,
            parallel=1,
            stage="posts",
            tandem=False,
            posts_workers=1,
            comments_workers=1,
            platform=None,
        ),
    )
    monkeypatch.setattr(worker.logging, "basicConfig", lambda **_kwargs: None)
    monkeypatch.setattr(worker, "load_env", lambda: None)
    monkeypatch.setattr(worker, "WorkerHeartbeat", _FakeHeartbeat)
    monkeypatch.setattr(worker, "ensure_media_mirror_s3_ready", lambda: None)
    monkeypatch.setattr(worker, "recover_stale_running_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(worker, "reconcile_run_summaries", lambda **_kwargs: {"reconciled_runs": 0, "run_ids": []})
    monkeypatch.setattr(
        worker,
        "claim_next_queued_jobs",
        lambda **_kwargs: [claimed_payload.pop(0)] if claimed_payload else [],
    )
    monkeypatch.setattr(
        worker,
        "cancel_claimed_job_before_processing",
        lambda _job: (_ for _ in ()).throw(RuntimeError("transient db error")),
    )
    monkeypatch.setattr(
        worker,
        "process_claimed_job",
        lambda job, **_kwargs: (
            process_calls.append(str(job.get("id") or "")),
            {
                "id": str(job.get("id") or ""),
                "run_id": str(job.get("run_id") or ""),
                "platform": str(job.get("platform") or ""),
                "status": "completed",
                "items_found": 1,
            },
        )[1],
    )

    rc = worker.main()

    assert rc == 0
    assert process_calls == ["job-after-probe-error"]


def test_main_queue_once_returns_nonzero_when_claiming_jobs_fails(monkeypatch) -> None:
    class _FakeHeartbeat:
        def __init__(self, **_kwargs):  # noqa: ANN003
            return

        def start(self) -> None:
            return

        def stop(self, *, reason: str = "shutdown") -> None:
            del reason
            return

        def set_state(self, **_kwargs):  # noqa: ANN003
            return

    monkeypatch.setattr(
        worker,
        "parse_args",
        lambda: SimpleNamespace(
            worker_id="root-worker",
            interval=0.1,
            once=True,
            run_id=None,
            parallel=1,
            stage="comments",
            tandem=False,
            posts_workers=1,
            comments_workers=1,
            platform=None,
        ),
    )
    monkeypatch.setattr(worker.logging, "basicConfig", lambda **_kwargs: None)
    monkeypatch.setattr(worker, "load_env", lambda: None)
    monkeypatch.setattr(worker, "WorkerHeartbeat", _FakeHeartbeat)
    monkeypatch.setattr(worker, "ensure_media_mirror_s3_ready", lambda: None)
    monkeypatch.setattr(worker, "recover_stale_running_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(worker, "reconcile_run_summaries", lambda **_kwargs: {"reconciled_runs": 0})
    monkeypatch.setattr(
        worker,
        "claim_next_queued_jobs",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("db down")),
    )

    rc = worker.main()

    assert rc == 1


def test_default_claim_batch_size_for_posts_is_single_claim() -> None:
    assert worker._default_claim_batch_size_for_stage("posts") == 1
    assert worker._default_claim_batch_size_for_stage("comments") == 2


def test_claim_stage_candidates_allow_comments_workers_to_borrow_posts() -> None:
    assert worker._claim_stage_candidates("comments") == ("comments", "posts")  # noqa: SLF001
    assert worker._claim_stage_candidates("posts") == ("posts",)  # noqa: SLF001


# ---------------------------------------------------------------------------
# Worker supported_platforms tests
# ---------------------------------------------------------------------------


def test_worker_passes_supported_platforms_from_platform_flag(monkeypatch) -> None:
    """When --platform tiktok is set, WorkerHeartbeat gets supported_platforms=['tiktok']."""
    captured: dict[str, object] = {}

    class _FakeHeartbeat:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def start(self):
            pass

        def stop(self, **kwargs):
            pass

        def set_state(self, **kwargs):
            pass

    monkeypatch.setattr(worker, "WorkerHeartbeat", _FakeHeartbeat)
    monkeypatch.setattr(worker, "recover_stale_running_jobs", lambda **kw: [])
    monkeypatch.setattr(worker, "claim_next_queued_jobs", lambda **kw: [])
    monkeypatch.setattr(worker, "reconcile_run_summaries", lambda **kw: {})
    monkeypatch.setattr(worker, "load_env", lambda: None)
    monkeypatch.setattr(worker, "ensure_media_mirror_s3_ready", lambda: None)
    monkeypatch.setattr(worker.logging, "basicConfig", lambda **_kwargs: None)
    monkeypatch.setattr("sys.argv", ["worker.py", "--platform", "tiktok", "--once"])

    worker.main()

    assert captured["supported_platforms"] == ["tiktok"]


def test_worker_passes_all_platforms_when_no_flag(monkeypatch) -> None:
    """Without --platform flag, WorkerHeartbeat gets all SOCIAL_SUPPORTED_PLATFORMS."""
    captured: dict[str, object] = {}

    class _FakeHeartbeat:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def start(self):
            pass

        def stop(self, **kwargs):
            pass

        def set_state(self, **kwargs):
            pass

    monkeypatch.setattr(worker, "WorkerHeartbeat", _FakeHeartbeat)
    monkeypatch.setattr(worker, "recover_stale_running_jobs", lambda **kw: [])
    monkeypatch.setattr(worker, "claim_next_queued_jobs", lambda **kw: [])
    monkeypatch.setattr(worker, "reconcile_run_summaries", lambda **kw: {})
    monkeypatch.setattr(worker, "load_env", lambda: None)
    monkeypatch.setattr(worker, "ensure_media_mirror_s3_ready", lambda: None)
    monkeypatch.setattr(worker.logging, "basicConfig", lambda **_kwargs: None)
    monkeypatch.setattr("sys.argv", ["worker.py", "--once"])

    worker.main()

    from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS

    assert captured["supported_platforms"] == list(SOCIAL_SUPPORTED_PLATFORMS)


def test_parse_args_accepts_reddit_platform(monkeypatch) -> None:
    monkeypatch.setattr(worker.sys, "argv", ["worker", "--platform", "reddit"])

    args = worker.parse_args()

    assert args.platform == "reddit"


def test_parse_args_accepts_shared_pipeline_stages(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["worker.py", "--stage", "post_classify", "--once"])

    args = worker.parse_args()

    assert args.stage == "post_classify"
    assert args.once is True
