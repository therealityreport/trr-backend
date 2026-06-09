from __future__ import annotations

from scripts import _workspace_runtime_env as helper


def test_apply_workspace_runtime_env_maps_workspace_values(tmp_path) -> None:
    repo_root = tmp_path / "TRR-Backend"
    workspace_root = tmp_path
    (workspace_root / ".logs" / "workspace").mkdir(parents=True)
    (workspace_root / ".logs" / "workspace" / "pids.env").write_text(
        "WORKSPACE_TRR_JOB_PLANE_MODE=remote\n"
        "WORKSPACE_TRR_REMOTE_EXECUTOR=modal\n"
        "WORKSPACE_TRR_MODAL_ENABLED=1\n"
        'WORKSPACE_TRR_MODAL_APP_NAME="trr-backend-jobs"\n'
        "WORKSPACE_TRR_MODAL_SOCIAL_JOB_FUNCTION=run_social_job\n",
        encoding="utf-8",
    )
    repo_root.mkdir()

    env: dict[str, str] = {}
    applied = helper.apply_workspace_runtime_env(repo_root=repo_root, environ=env)

    assert env["TRR_JOB_PLANE_MODE"] == "remote"
    assert env["TRR_REMOTE_EXECUTOR"] == "modal"
    assert env["TRR_MODAL_ENABLED"] == "1"
    assert env["TRR_MODAL_APP_NAME"] == "trr-backend-jobs"
    assert env["TRR_MODAL_SOCIAL_JOB_FUNCTION"] == "run_social_job"
    assert applied["TRR_REMOTE_EXECUTOR"] == "modal"


def test_apply_workspace_runtime_env_maps_remote_social_caps(tmp_path) -> None:
    repo_root = tmp_path / "TRR-Backend"
    workspace_root = tmp_path
    (workspace_root / ".logs" / "workspace").mkdir(parents=True)
    (workspace_root / ".logs" / "workspace" / "pids.env").write_text(
        "WORKSPACE_TRR_REMOTE_SOCIAL_DISPATCH_LIMIT=4\n"
        "WORKSPACE_TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT=4\n"
        "WORKSPACE_TRR_REMOTE_SOCIAL_POSTS=1\n"
        "WORKSPACE_TRR_REMOTE_SOCIAL_COMMENTS=8\n"
        "WORKSPACE_TRR_REMOTE_SOCIAL_MEDIA_MIRROR=1\n"
        "WORKSPACE_TRR_REMOTE_SOCIAL_COMMENT_MEDIA_MIRROR=1\n",
        encoding="utf-8",
    )
    repo_root.mkdir()

    env: dict[str, str] = {}
    helper.apply_workspace_runtime_env(repo_root=repo_root, environ=env)

    assert env["SOCIAL_MODAL_DISPATCH_LIMIT"] == "4"
    assert env["TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT"] == "4"
    assert env["SOCIAL_WORKER_POOL_POSTS"] == "1"
    assert env["SOCIAL_WORKER_POOL_COMMENTS"] == "8"
    assert env["SOCIAL_WORKER_POOL_MEDIA_MIRROR"] == "1"
    assert env["SOCIAL_WORKER_POOL_COMMENT_MEDIA_MIRROR"] == "1"
    assert env["SOCIAL_POSTS_COMMENTS_PLATFORM_CAP_INSTAGRAM"] == "8"


def test_apply_workspace_runtime_env_preserves_explicit_shell_overrides(tmp_path) -> None:
    repo_root = tmp_path / "TRR-Backend"
    workspace_root = tmp_path
    (workspace_root / ".logs" / "workspace").mkdir(parents=True)
    (workspace_root / ".logs" / "workspace" / "pids.env").write_text(
        "WORKSPACE_TRR_REMOTE_EXECUTOR=modal\n",
        encoding="utf-8",
    )
    repo_root.mkdir()

    env = {"TRR_REMOTE_EXECUTOR": "legacy_worker"}
    applied = helper.apply_workspace_runtime_env(repo_root=repo_root, environ=env)

    assert env["TRR_REMOTE_EXECUTOR"] == "legacy_worker"
    assert applied == {}


def test_apply_workspace_runtime_env_ignores_missing_pidfile(tmp_path) -> None:
    repo_root = tmp_path / "TRR-Backend"
    repo_root.mkdir()

    env: dict[str, str] = {}
    applied = helper.apply_workspace_runtime_env(repo_root=repo_root, environ=env)

    assert env == {}
    assert applied == {}
