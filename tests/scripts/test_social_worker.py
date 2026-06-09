from __future__ import annotations

import subprocess
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


def test_apply_post_persist_truthfulness_diagnostics_adds_silent_drop_alert(monkeypatch) -> None:
    persisted: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(
        worker,
        "_persist_job_metadata",
        lambda job_id, metadata: (
            persisted.append((job_id, metadata)),
            {
                "id": job_id,
                "run_id": "run-1",
                "platform": "instagram",
                "job_type": "posts_scrapling",
                "status": "completed",
                "items_found": 6,
                "error_message": None,
                "metadata": metadata,
            },
        )[1],
    )

    updated_job, silent_drop_alert = worker._apply_post_persist_truthfulness_diagnostics(  # noqa: SLF001
        {
            "id": "job-1",
            "run_id": "run-1",
            "platform": "instagram",
            "status": "completed",
            "items_found": 6,
            "metadata": {
                "stage": "posts_scrapling",
                "account": "thetraitorsus",
                "stage_counters": {"posts": 6},
                "persist_counters": {"posts_upserted": 0},
                "posts_scrapling_persist_diagnostics": {
                    "posts_upserted": 0,
                    "posts_skipped": 6,
                    "posts_skipped_by_reason": {"canonical_upsert_returned_none": 6},
                },
            },
        }
    )

    assert silent_drop_alert is True
    assert persisted and persisted[0][0] == "job-1"
    assert updated_job["metadata"]["persist_counters"] == {
        "posts_upserted": 0,
        "posts_skipped": 6,
        "posts_skipped_by_reason": {"canonical_upsert_returned_none": 6},
    }
    assert updated_job["metadata"]["diagnostics"]["post_persist_truthfulness"] == {
        "platform": "instagram",
        "account": "thetraitorsus",
        "posts_checked": 6,
        "posts_upserted": 0,
        "posts_skipped": 6,
        "posts_skipped_by_reason": {"canonical_upsert_returned_none": 6},
        "media_assets_persisted": 0,
        "silent_drop_detected": True,
        "status_resolution": "completed_with_silent_drop_alert",
        "operator_summary": "Instagram posts persistence completed with zero saved posts after checking live posts.",
    }
    assert updated_job["metadata"]["alerts"] == [
        {
            "code": "instagram_posts_persist_zero_saved",
            "severity": "warning",
            "message": "Instagram posts persistence completed with zero saved posts after checking live posts.",
        }
    ]


def test_apply_post_persist_truthfulness_diagnostics_surfaces_skip_counters_without_alert(monkeypatch) -> None:
    monkeypatch.setattr(
        worker,
        "_persist_job_metadata",
        lambda job_id, metadata: {
            "id": job_id,
            "run_id": "run-1",
            "platform": "instagram",
            "job_type": "posts_scrapling",
            "status": "completed",
            "items_found": 4,
            "error_message": None,
            "metadata": metadata,
        },
    )

    updated_job, silent_drop_alert = worker._apply_post_persist_truthfulness_diagnostics(  # noqa: SLF001
        {
            "id": "job-2",
            "run_id": "run-1",
            "platform": "instagram",
            "status": "completed",
            "items_found": 4,
            "metadata": {
                "stage": "posts_scrapling",
                "account": "traitors",
                "stage_counters": {"posts": 4},
                "persist_counters": {"posts_upserted": 3},
                "posts_scrapling_persist_diagnostics": {
                    "posts_upserted": 3,
                    "posts_skipped": 1,
                    "posts_skipped_by_reason": {"missing_shortcode": 1},
                },
            },
        }
    )

    assert silent_drop_alert is False
    assert "alerts" not in updated_job["metadata"]
    assert updated_job["metadata"]["persist_counters"] == {
        "posts_upserted": 3,
        "posts_skipped": 1,
        "posts_skipped_by_reason": {"missing_shortcode": 1},
    }
    assert updated_job["metadata"]["diagnostics"]["post_persist_truthfulness"]["silent_drop_detected"] is False


def test_apply_post_persist_truthfulness_diagnostics_suppresses_alert_when_media_persisted(monkeypatch) -> None:
    monkeypatch.setattr(
        worker,
        "_persist_job_metadata",
        lambda job_id, metadata: {
            "id": job_id,
            "run_id": "run-1",
            "platform": "instagram",
            "job_type": "instagram_media_mirror",
            "status": "completed",
            "items_found": 11,
            "error_message": None,
            "metadata": metadata,
        },
    )

    updated_job, silent_drop_alert = worker._apply_post_persist_truthfulness_diagnostics(  # noqa: SLF001
        {
            "id": "job-media-1",
            "run_id": "run-1",
            "platform": "instagram",
            "status": "completed",
            "items_found": 11,
            "metadata": {
                "stage": "media_mirror",
                "account": "bravotv",
                "stage_counters": {"posts": 1},
                "persist_counters": {"posts_upserted": 0},
                "posts_scrapling_persist_diagnostics": {
                    "posts_upserted": 0,
                    "posts_skipped": 1,
                    "posts_skipped_by_reason": {"media_mirror_followup": 1},
                },
                "mirror": {
                    "status": "mirrored",
                    "mirrored_assets": 9,
                    "storage_summary": {"object_count": 20, "cdn_url_count": 20},
                },
                "alerts": [
                    {
                        "code": "instagram_posts_persist_zero_saved",
                        "severity": "warning",
                        "message": "stale warning",
                    }
                ],
            },
        }
    )

    truthfulness = updated_job["metadata"]["diagnostics"]["post_persist_truthfulness"]
    assert silent_drop_alert is False
    assert truthfulness["media_assets_persisted"] == 20
    assert truthfulness["silent_drop_detected"] is False
    assert "alerts" not in updated_job["metadata"]


def test_apply_post_persist_truthfulness_diagnostics_reads_threads_alias(monkeypatch) -> None:
    monkeypatch.setattr(
        worker,
        "_persist_job_metadata",
        lambda job_id, metadata: {
            "id": job_id,
            "run_id": "run-1",
            "platform": "threads",
            "job_type": "posts_scrapling",
            "status": "completed",
            "items_found": 2,
            "error_message": None,
            "metadata": metadata,
        },
    )

    updated_job, silent_drop_alert = worker._apply_post_persist_truthfulness_diagnostics(  # noqa: SLF001
        {
            "id": "job-3",
            "run_id": "run-1",
            "platform": "threads",
            "status": "completed",
            "items_found": 2,
            "metadata": {
                "stage": "threads_posts_scrapling",
                "platform": "threads",
                "account": "bravotv",
                "stage_counters": {"posts": 2},
                "threads_posts_scrapling_persist_diagnostics": {
                    "posts_upserted": 0,
                    "posts_skipped": 2,
                    "posts_skipped_by_reason": {"missing_post_id": 2},
                },
            },
        }
    )

    assert silent_drop_alert is True
    assert updated_job["metadata"]["posts_scrapling_persist_diagnostics"]["posts_skipped"] == 2
    assert updated_job["metadata"]["diagnostics"]["post_persist_truthfulness"]["platform"] == "threads"


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


def test_default_claim_batch_size_for_sequential_stages_is_single_claim() -> None:
    assert worker._default_claim_batch_size_for_stage("posts") == 1
    assert worker._default_claim_batch_size_for_stage("comments_scrapling") == 1
    assert worker._default_claim_batch_size_for_stage("threads_posts_scrapling") == 1
    assert worker._default_claim_batch_size_for_stage("comments") == 2


def test_claim_stage_candidates_comments_workers_fall_back_to_posts() -> None:
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


def test_parse_args_accepts_threads_posts_scrapling_stage(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["worker.py", "--stage", "threads_posts_scrapling", "--once"])

    args = worker.parse_args()

    assert args.stage == "threads_posts_scrapling"
    assert args.once is True


def test_wait_for_children_times_out_and_terminates_hung_child(monkeypatch) -> None:
    events: list[str] = []

    class _HungProc:
        def wait(self, timeout=None):  # noqa: ANN001
            if timeout is None or timeout > 0:
                raise subprocess.TimeoutExpired(cmd="hung", timeout=timeout)
            raise subprocess.TimeoutExpired(cmd="hung", timeout=timeout)

        def poll(self):
            return None

        def terminate(self):
            events.append("terminate")

        def kill(self):
            events.append("kill")

    exit_code = worker._wait_for_children(  # noqa: SLF001
        [_HungProc()],
        context_label="queue fanout",
        wait_timeout_seconds=0.1,
        terminate_grace_seconds=0.1,
    )

    assert exit_code == 124
    assert events == ["terminate", "kill"]


def test_resolve_child_wait_timeout_adds_grace_to_max_run_seconds() -> None:
    assert worker._resolve_child_wait_timeout_seconds(180.0) == 210.0  # noqa: SLF001


def test_resolve_worker_execution_limits_are_unbounded_by_default_and_use_new_env_names(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_EXECUTE_RUN_MAX_JOBS", raising=False)
    monkeypatch.delenv("SOCIAL_EXECUTE_RUN_MAX_SECONDS", raising=False)
    monkeypatch.delenv("SOCIAL_WORKER_MAX_JOBS_PER_INVOCATION", raising=False)
    monkeypatch.delenv("SOCIAL_WORKER_MAX_RUN_SECONDS", raising=False)

    args = SimpleNamespace(max_jobs_per_invocation=None, max_run_seconds=None)

    assert worker._resolve_worker_execution_limits(args) == (None, None)  # noqa: SLF001

    monkeypatch.setenv("SOCIAL_EXECUTE_RUN_MAX_JOBS", "25")
    monkeypatch.setenv("SOCIAL_EXECUTE_RUN_MAX_SECONDS", "90")

    assert worker._resolve_worker_execution_limits(args) == (25, 90.0)  # noqa: SLF001


def test_resolve_worker_execution_limits_prefers_cli_overrides(monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_EXECUTE_RUN_MAX_JOBS", "25")
    monkeypatch.setenv("SOCIAL_EXECUTE_RUN_MAX_SECONDS", "90")

    args = SimpleNamespace(max_jobs_per_invocation=4, max_run_seconds=12.5)

    assert worker._resolve_worker_execution_limits(args) == (4, 12.5)  # noqa: SLF001


def test_execute_run_with_caps_stops_after_job_cap(monkeypatch) -> None:
    claim_calls: list[dict[str, object]] = []
    process_calls: list[str] = []
    finalize_calls: list[tuple[str, bool]] = []
    jobs = [
        {"id": "job-1", "run_id": "run-1", "platform": "instagram", "config": {"stage": "posts"}},
        {"id": "job-2", "run_id": "run-1", "platform": "instagram", "config": {"stage": "posts"}},
        {"id": "job-3", "run_id": "run-1", "platform": "instagram", "config": {"stage": "posts"}},
    ]

    monkeypatch.setattr(worker.social_repo, "_set_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        worker.social_repo,
        "_finalize_run_status",
        lambda run_id, force_recompute=False: finalize_calls.append((run_id, force_recompute)) or {},
    )
    monkeypatch.setattr(worker.social_repo, "_fetch_next_preclaimed_job", lambda **_kwargs: None)
    monkeypatch.setattr(worker.social_repo, "_finish_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        worker,
        "claim_next_queued_jobs",
        lambda **kwargs: claim_calls.append(dict(kwargs)) or ([jobs.pop(0)] if jobs else []),
    )
    monkeypatch.setattr(
        worker,
        "process_claimed_job",
        lambda job, **_kwargs: process_calls.append(str(job.get("id") or "")) or job,
    )

    def _fake_fetch_one(sql: str, params=None):  # noqa: ANN001
        if "select status from social.scrape_runs" in sql:
            return {"status": "running"}
        if "from social.scrape_runs" in sql:
            return {"id": "run-1", "status": "running", "config": {}}
        raise AssertionError(sql)

    monkeypatch.setattr(worker.pg, "fetch_one", _fake_fetch_one)

    payload = worker._execute_run_with_caps(  # noqa: SLF001
        run_id="run-1",
        worker_id="worker-1",
        stage="posts",
        platform="instagram",
        max_jobs_per_invocation=2,
        max_run_seconds=1800.0,
    )

    assert process_calls == ["job-1", "job-2"]
    assert [call["limit"] for call in claim_calls] == [1, 1]
    assert finalize_calls == [("run-1", False)]
    assert payload["id"] == "run-1"


def test_execute_run_with_caps_stops_after_runtime_cap(monkeypatch) -> None:
    finalize_calls: list[str] = []
    monotonic_values = iter([100.0, 100.2])

    monkeypatch.setattr(worker.social_repo, "_set_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        worker.social_repo,
        "_finalize_run_status",
        lambda run_id, force_recompute=False: finalize_calls.append(run_id) or {},
    )
    monkeypatch.setattr(
        worker,
        "claim_next_queued_jobs",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("claim loop should stop before claiming")),
    )
    monkeypatch.setattr(worker.time, "monotonic", lambda: next(monotonic_values))

    monkeypatch.setattr(
        worker.pg,
        "fetch_one",
        lambda sql, params=None: (
            {"id": "run-1", "status": "running", "config": {}}
            if "from social.scrape_runs" in sql
            else {"status": "running"}
        ),
    )

    payload = worker._execute_run_with_caps(  # noqa: SLF001
        run_id="run-1",
        worker_id="worker-1",
        stage="posts",
        platform="instagram",
        max_jobs_per_invocation=1000,
        max_run_seconds=0.1,
    )

    assert finalize_calls == ["run-1"]
    assert payload["id"] == "run-1"


def test_spawn_child_worker_propagates_caps_for_run_id_children(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_popen(cmd, cwd=None, env=None):  # noqa: ANN001
        captured["cmd"] = list(cmd)
        captured["cwd"] = cwd
        captured["env"] = env
        return SimpleNamespace(pid=1234)

    monkeypatch.setattr(worker.subprocess, "Popen", _fake_popen)

    worker._spawn_child_worker(  # noqa: SLF001
        worker_id="worker-1:p1",
        interval=5.0,
        stage="comments_scrapling",
        platform="instagram",
        run_id="run-1",
        max_jobs_per_invocation=25,
        max_run_seconds=90.0,
    )

    cmd = captured["cmd"]
    assert "--run-id" in cmd
    assert "--max-jobs-per-invocation" in cmd
    assert "--max-run-seconds" in cmd
