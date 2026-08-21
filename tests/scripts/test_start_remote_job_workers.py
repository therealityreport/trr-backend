from __future__ import annotations

import os
import subprocess
from pathlib import Path


def test_remote_worker_wrapper_respawns_after_nonzero_child_exit(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    calls_file = tmp_path / "calls.log"
    fake_python = tmp_path / "fake-python"
    fake_python.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf 'call\\n' >> "$CALLS_FILE"
if [[ "$(wc -l < "$CALLS_FILE")" -eq 1 ]]; then
  exit 17
fi
exit 0
""",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)

    env = {
        **os.environ,
        "PYTHON_BIN": str(fake_python),
        "CALLS_FILE": str(calls_file),
        "RESPAWN_DELAY": "0",
        "TRR_REMOTE_EXECUTOR": "legacy_worker",
        "TRR_MODAL_ENABLED": "0",
        "TRR_ADMIN_OPERATION_WORKER_ENABLED": "0",
        "TRR_REDDIT_REFRESH_WORKER_ENABLED": "1",
        "TRR_REDDIT_REFRESH_WORKER_COUNT": "1",
        "TRR_GOOGLE_NEWS_WORKER_ENABLED": "0",
        "TRR_SOCIAL_INGEST_WORKER_ENABLED": "0",
    }
    result = subprocess.run(
        ["bash", str(repo_root / "scripts" / "start_remote_job_workers.sh")],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert calls_file.read_text(encoding="utf-8").splitlines() == ["call", "call"]
    assert "reddit-refresh#1 exited rc=17; respawning" in result.stdout
