from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

_WORKFLOW_PATH = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"


def _step(job: dict[str, Any], name: str) -> dict[str, Any]:
    for step in job["steps"]:
        if step.get("name") == name:
            return step
    raise AssertionError(f"Missing CI step: {name}")


def _step_run(job: dict[str, Any], name: str) -> str:
    return str(_step(job, name)["run"])


def test_ci_enforces_the_selected_v4_backend_contract() -> None:
    workflow = yaml.safe_load(_WORKFLOW_PATH.read_text(encoding="utf-8"))
    jobs = workflow["jobs"]

    typecheck = jobs["typecheck"]
    assert typecheck["name"] == "Pyright"
    assert "continue-on-error" not in typecheck
    assert _step_run(typecheck, "Pyright") == "pyright"

    py312_canary = jobs["test-py312-canary"]
    assert py312_canary["continue-on-error"] is True
    assert _step(py312_canary, "Set up Python")["with"]["python-version"] == "3.12"

    lint = jobs["lint"]
    assert lint["name"] == "Ruff (changed files, forward-only)"
    changed_files = _step_run(lint, "Determine changed Python files")
    assert "git diff --name-only --diff-filter=ACMR" in changed_files
    assert _step_run(lint, "Ruff check (changed files only)") == (
        "printf '%s' \"$CHANGED_PY_FILES\" | xargs -r uvx ruff@0.14.4 check"
    )
    assert _step_run(lint, "Ruff format check (changed files only)") == (
        "printf '%s' \"$CHANGED_PY_FILES\" | xargs -r uvx ruff@0.14.4 format --check"
    )

    modal_locks = jobs["modal-locks"]
    assert modal_locks["name"] == "Modal lockfile freshness (non-blocking)"
    assert modal_locks["continue-on-error"] is True
    assert 'uv pip compile "$infile" --python-version 3.11 "$@" -o "$lockfile"' in _step_run(
        modal_locks, "Verify lean/browser/vision lockfile freshness"
    )

    full_pytest = jobs["test-full"]
    assert full_pytest["name"] == "Full pytest (non-blocking)"
    assert full_pytest["continue-on-error"] is True

    assert "test-vision" not in jobs
