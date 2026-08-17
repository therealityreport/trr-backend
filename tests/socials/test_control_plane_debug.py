"""Unit contracts for the import-neutral social debug control-plane leaf."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import trr_backend.socials.control_plane.debug as social_debug


def _job_context(_job_id: str) -> dict[str, object]:
    return {"job": {"id": "job-1", "run_id": "run-1"}, "worker": {}, "run": {}}


def _debug_completion_payload() -> dict[str, object]:
    return {
        "root_cause": "fallback succeeded",
        "confidence": 0.7,
        "patch_unified_diff": "--- a/api/routers/socials.py\n+++ b/api/routers/socials.py\n@@\n-foo\n+bar\n",
        "files_touched": ["api/routers/socials.py"],
        "tests_to_run": ["pytest -q tests/api/routers/test_socials_season_analytics.py"],
    }


@pytest.mark.parametrize(
    ("paths", "error_match"),
    [
        ([], "did not include file paths"),
        ([""], "path is empty"),
        (["/tmp/outside.py"], "path is absolute"),
        (["~/outside.py"], "path is absolute"),
        (["../outside.py"], "path contains traversal"),
        (["unapproved/outside.py"], "path outside allowlist"),
    ],
)
def test_validate_debug_patch_paths_rejects_unsafe_paths(paths: list[str], error_match: str) -> None:
    with pytest.raises(ValueError, match=error_match):
        social_debug._validate_debug_patch_paths(paths)


def test_debug_ingest_job_uses_fallback_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("SOCIAL_DEBUG_OPENAI_MODEL", "gpt-5.3-codex")
    monkeypatch.setenv("SOCIAL_DEBUG_OPENAI_FALLBACK_MODEL", "gpt-5.3-codex-fallback")
    calls: list[str] = []

    def _fake_openai_completion(*, model: str, prompt: str, api_key: str, timeout_seconds: int):
        del prompt, api_key, timeout_seconds
        calls.append(model)
        if model == "gpt-5.3-codex":
            raise RuntimeError("rate limited")
        return _debug_completion_payload()

    monkeypatch.setattr(social_debug, "_run_social_debug_openai_completion", _fake_openai_completion)

    payload = social_debug.run_social_debug(
        job_id="job-1",
        fetch_job_context=_job_context,
        include_context=False,
    )

    assert calls == ["gpt-5.3-codex", "gpt-5.3-codex-fallback"]
    assert payload["model_used"] == "gpt-5.3-codex-fallback"
    assert payload["fallback_used"] is True


def test_social_debug_fallback_model_default_is_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOCIAL_DEBUG_OPENAI_FALLBACK_MODEL", raising=False)

    assert social_debug._social_debug_fallback_model_name() == ""


def test_debug_ingest_job_apply_returns_check_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("SOCIAL_DEBUG_PATCH_APPLY_ENABLED", "true")

    def _fake_openai_completion(**_kwargs):
        return _debug_completion_payload()

    monkeypatch.setattr(social_debug, "_run_social_debug_openai_completion", _fake_openai_completion)
    monkeypatch.setattr(
        social_debug,
        "_run_git_apply",
        lambda **_kwargs: subprocess.CompletedProcess(
            args=["git", "apply"], returncode=1, stdout="", stderr="check failed"
        ),
    )

    payload = social_debug.run_social_debug(
        job_id="job-1",
        fetch_job_context=_job_context,
        include_context=False,
        apply_patch=True,
        confirm_apply=True,
    )

    assert payload["apply"]["requested"] is True
    assert payload["apply"]["check_ok"] is False
    assert payload["apply"]["applied"] is False
    assert payload["apply"]["error"] == "check failed"


def test_debug_leaf_cold_import_does_not_load_the_social_monolith() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = os.pathsep.join(path for path in (str(backend_root), existing_pythonpath) if path)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "\n".join(
                [
                    "import importlib",
                    "import os",
                    "import sys",
                    "leaf_name = 'trr_backend.socials.control_plane.debug'",
                    "legacy_name = 'trr_backend.socials.social_season_analytics_impl'",
                    "recovery_name = 'trr_backend.socials.control_plane.recovery'",
                    "alias_name = 'trr_backend.repositories.social_season_analytics'",
                    "assert all(name not in sys.modules for name in (legacy_name, recovery_name, alias_name))",
                    "leaf = importlib.import_module(leaf_name)",
                    "assert all(name not in sys.modules for name in (legacy_name, recovery_name, alias_name))",
                    "def completion(**_kwargs):",
                    "    return {",
                    "        'root_cause': 'ok',",
                    "        'confidence': 1,",
                    "        'patch_unified_diff': '',",
                    "        'files_touched': [],",
                    "        'tests_to_run': [],",
                    "    }",
                    "leaf._run_social_debug_openai_completion = completion",
                    "os.environ['OPENAI_API_KEY'] = 'test-key'",
                    "def fetch_job_context(_job_id):",
                    "    return {'job': {'id': 'job-1', 'run_id': None}, 'worker': {}, 'run': {}}",
                    "payload = leaf.run_social_debug(",
                    "    job_id='job-1',",
                    "    fetch_job_context=fetch_job_context,",
                    "    include_context=False,",
                    ")",
                    "assert payload['job_id'] == 'job-1'",
                    "assert all(name not in sys.modules for name in (legacy_name, recovery_name, alias_name))",
                ]
            ),
        ],
        cwd=backend_root,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
