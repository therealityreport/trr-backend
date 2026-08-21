from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _install_fake_uvicorn(tmp_path: Path) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_uvicorn = bin_dir / "uvicorn"
    fake_uvicorn.write_text(
        '#!/usr/bin/env bash\nprintf \'%s\\n\' "$@" > "$UVICORN_ARGS_FILE"\n',
        encoding="utf-8",
    )
    fake_uvicorn.chmod(0o755)
    return bin_dir


def _copy_start_api(tmp_path: Path) -> Path:
    script = tmp_path / "start-api.sh"
    script.write_text((ROOT / "start-api.sh").read_text(encoding="utf-8"), encoding="utf-8")
    script.chmod(0o755)
    return script


def _base_env(tmp_path: Path) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "APP_ENV",
            "ENVIRONMENT",
            "PYTHON_ENV",
            "REDIS_URL",
            "TRR_BACKEND_RELOAD",
            "TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER",
            "TRR_BACKEND_WORKERS",
            "TRR_ENV",
            "TRR_ENVIRONMENT",
            "TRR_LOCAL_DEV",
        }
    }
    env["PATH"] = f"{_install_fake_uvicorn(tmp_path)}:{env.get('PATH', '')}"
    env["UVICORN_ARGS_FILE"] = str(tmp_path / "uvicorn.args")
    return env


def test_start_api_blocks_deployed_multi_worker_without_redis(tmp_path: Path) -> None:
    script = _copy_start_api(tmp_path)
    env = _base_env(tmp_path)
    env.update(
        {
            "APP_ENV": "production",
            "TRR_BACKEND_RELOAD": "0",
            "TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER": "1",
            "TRR_BACKEND_WORKERS": "2",
        }
    )

    result = subprocess.run([str(script)], cwd=tmp_path, env=env, text=True, capture_output=True, check=False)

    assert result.returncode == 1
    assert "requires REDIS_URL for deployed multi-worker realtime" in result.stderr
    assert not Path(env["UVICORN_ARGS_FILE"]).exists()


def test_start_api_local_multi_worker_without_redis_falls_back_to_single_worker(tmp_path: Path) -> None:
    script = _copy_start_api(tmp_path)
    env = _base_env(tmp_path)
    env.update(
        {
            "APP_ENV": "development",
            "TRR_BACKEND_RELOAD": "0",
            "TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER": "1",
            "TRR_BACKEND_WORKERS": "2",
        }
    )

    result = subprocess.run([str(script)], cwd=tmp_path, env=env, text=True, capture_output=True, check=False)

    assert result.returncode == 0
    assert "forcing single-worker mode" in result.stdout
    args = Path(env["UVICORN_ARGS_FILE"]).read_text(encoding="utf-8")
    assert "--workers" not in args


def test_start_api_deployed_multi_worker_with_redis_uses_worker_count(tmp_path: Path) -> None:
    script = _copy_start_api(tmp_path)
    env = _base_env(tmp_path)
    env.update(
        {
            "APP_ENV": "production",
            "REDIS_URL": "redis://127.0.0.1:6379/0",
            "TRR_BACKEND_RELOAD": "0",
            "TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER": "1",
            "TRR_BACKEND_WORKERS": "2",
        }
    )

    result = subprocess.run([str(script)], cwd=tmp_path, env=env, text=True, capture_output=True, check=False)

    assert result.returncode == 0
    args = Path(env["UVICORN_ARGS_FILE"]).read_text(encoding="utf-8").splitlines()
    assert "--workers" in args
    assert args[args.index("--workers") + 1] == "2"
