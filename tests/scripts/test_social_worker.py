from __future__ import annotations

from types import SimpleNamespace

import scripts.socials.worker as worker


def test_requires_media_mirror_s3_preflight() -> None:
    assert worker._requires_media_mirror_s3_preflight(stage="any", platform=None) is True  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="media_mirror", platform="instagram") is True  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="media_mirror", platform="tiktok") is True  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="media_mirror", platform="youtube") is True  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="media_mirror", platform="twitter") is True  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="posts", platform=None) is False  # noqa: SLF001
    assert worker._requires_media_mirror_s3_preflight(stage="comments", platform="instagram") is False  # noqa: SLF001


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
            stage="any",
            tandem=False,
            posts_workers=1,
            comments_workers=1,
            platform=None,
        ),
    )
    monkeypatch.setattr(worker.logging, "basicConfig", lambda **_kwargs: None)
    monkeypatch.setattr(worker, "load_env", lambda: None)

    def _fail_preflight() -> None:
        raise RuntimeError("Missing required environment variable: AWS_S3_BUCKET")

    monkeypatch.setattr(worker, "ensure_media_mirror_s3_ready", _fail_preflight)

    rc = worker.main()

    assert rc == 2
