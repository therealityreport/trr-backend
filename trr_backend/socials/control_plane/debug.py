"""OpenAI social-ingest debug orchestration and guarded patch application."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import requests

SOCIAL_DEBUG_OPENAI_MODEL_DEFAULT = "gpt-5.3-codex"
SOCIAL_DEBUG_OPENAI_FALLBACK_MODEL_DEFAULT = ""
SOCIAL_DEBUG_OPENAI_TIMEOUT_SECONDS_DEFAULT = 45
SOCIAL_DEBUG_CONTEXT_MAX_BYTES_DEFAULT = 18_000
SOCIAL_DEBUG_PATCH_MAX_BYTES_DEFAULT = 200_000
SOCIAL_DEBUG_ALLOWED_TOP_LEVEL_PATHS = {
    "api",
    "trr_backend",
    "scripts",
    "tests",
    "docs",
    ".env.example",
}


def _env_truthy(name: str, *, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _resolve_positive_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return max(minimum, default)
    try:
        return max(minimum, int(raw))
    except ValueError:
        return max(minimum, default)


def _social_debug_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _social_debug_context_max_bytes() -> int:
    return _resolve_positive_int_env(
        "SOCIAL_DEBUG_CONTEXT_MAX_BYTES",
        SOCIAL_DEBUG_CONTEXT_MAX_BYTES_DEFAULT,
        minimum=2_000,
    )


def _social_debug_patch_max_bytes() -> int:
    return _resolve_positive_int_env(
        "SOCIAL_DEBUG_PATCH_MAX_BYTES",
        SOCIAL_DEBUG_PATCH_MAX_BYTES_DEFAULT,
        minimum=10_000,
    )


def _social_debug_patch_apply_enabled() -> bool:
    return _env_truthy("SOCIAL_DEBUG_PATCH_APPLY_ENABLED", default=False)


def _social_debug_model_name() -> str:
    return (
        os.getenv("SOCIAL_DEBUG_OPENAI_MODEL") or SOCIAL_DEBUG_OPENAI_MODEL_DEFAULT
    ).strip() or SOCIAL_DEBUG_OPENAI_MODEL_DEFAULT


def _social_debug_fallback_model_name() -> str:
    fallback = (os.getenv("SOCIAL_DEBUG_OPENAI_FALLBACK_MODEL") or SOCIAL_DEBUG_OPENAI_FALLBACK_MODEL_DEFAULT).strip()
    return fallback or SOCIAL_DEBUG_OPENAI_FALLBACK_MODEL_DEFAULT


def _social_debug_timeout_seconds() -> int:
    return _resolve_positive_int_env(
        "SOCIAL_DEBUG_OPENAI_TIMEOUT_SECONDS",
        SOCIAL_DEBUG_OPENAI_TIMEOUT_SECONDS_DEFAULT,
        minimum=5,
    )


def _extract_snippet_around_marker(
    text: str,
    *,
    marker: str,
    before_chars: int = 200,
    after_chars: int = 2200,
) -> str | None:
    idx = text.find(marker)
    if idx < 0:
        return None
    start = max(0, idx - before_chars)
    end = min(len(text), idx + after_chars)
    return text[start:end].strip()


def _load_social_debug_source_context() -> list[dict[str, str]]:
    repo_root = _social_debug_repo_root()
    sources: list[tuple[str, list[str]]] = [
        (
            "trr_backend/repositories/social_season_analytics.py",
            [
                "def get_queue_status(",
                "def _claim_next_jobs(",
                "def _execute_claimed_job(",
                "def recover_stale_running_jobs(",
            ],
        ),
        (
            "scripts/socials/worker.py",
            [
                "class WorkerHeartbeat:",
                "def main() -> int:",
            ],
        ),
        (
            "api/routers/socials/__init__.py",
            [
                "def get_social_ingest_queue_status(",
                "def ingest_season_social(",
            ],
        ),
    ]
    snippets: list[dict[str, str]] = []
    per_file_budget = max(2_000, _social_debug_context_max_bytes() // max(1, len(sources)))
    for rel_path, markers in sources:
        abs_path = repo_root / rel_path
        try:
            raw = abs_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:  # noqa: BLE001
            continue
        extracted: list[str] = []
        for marker in markers:
            candidate = _extract_snippet_around_marker(raw, marker=marker)
            if candidate:
                extracted.append(candidate)
        joined = "\n\n".join(extracted).strip() if extracted else raw[:per_file_budget].strip()
        if not joined:
            continue
        snippets.append(
            {
                "path": rel_path,
                "content": joined[:per_file_budget],
            }
        )
    return snippets


def _build_social_debug_prompt(
    *,
    job_context: dict[str, Any],
    source_context: list[dict[str, str]],
) -> str:
    prompt_payload = {
        "job_context": job_context,
        "source_context": source_context,
        "task": "Diagnose stuck/failed social ingest job and produce exact unified diff patch.",
        "required_output_schema": {
            "root_cause": "string",
            "confidence": "number between 0 and 1",
            "patch_unified_diff": "string unified diff",
            "files_touched": ["string file paths"],
            "tests_to_run": ["string commands"],
        },
        "constraints": [
            "Return strict JSON object only.",
            "patch_unified_diff must be valid unified diff with ---/+++ headers.",
            "Avoid changing unrelated files.",
            "Focus on root-cause fix with smallest safe patch.",
        ],
    }
    return json.dumps(prompt_payload, separators=(",", ":"), ensure_ascii=True)


def _run_social_debug_openai_completion(
    *,
    model: str,
    prompt: str,
    api_key: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a senior backend engineer. Return strict JSON only.",
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        },
        timeout=max(5, int(timeout_seconds)),
    )
    response.raise_for_status()
    payload = response.json()
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("OpenAI response missing choices")
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("OpenAI response missing content")
    parsed = json.loads(content)
    if not isinstance(parsed, dict):
        raise RuntimeError("OpenAI response is not a JSON object")
    return parsed


def _extract_unified_diff_paths(patch_unified_diff: str) -> list[str]:
    paths: list[str] = []
    for raw_line in patch_unified_diff.splitlines():
        line = raw_line.strip()
        if not (line.startswith("+++ ") or line.startswith("--- ")):
            continue
        candidate = line[4:].strip()
        if candidate == "/dev/null":
            continue
        if candidate.startswith("a/") or candidate.startswith("b/"):
            candidate = candidate[2:]
        if not candidate:
            continue
        if candidate not in paths:
            paths.append(candidate)
    return paths


def _validate_debug_patch_paths(paths: list[str]) -> None:
    if not paths:
        raise ValueError("patch_unified_diff did not include file paths")
    for path in paths:
        normalized = str(path or "").strip()
        if not normalized:
            raise ValueError("patch path is empty")
        if normalized.startswith("/") or normalized.startswith("~"):
            raise ValueError(f"patch path is absolute: {normalized}")
        if ".." in Path(normalized).parts:
            raise ValueError(f"patch path contains traversal: {normalized}")
        top_level = normalized.split("/", 1)[0]
        if top_level not in SOCIAL_DEBUG_ALLOWED_TOP_LEVEL_PATHS:
            raise ValueError(f"patch path outside allowlist: {normalized}")


def _run_git_apply(*, patch_unified_diff: str, check_only: bool) -> subprocess.CompletedProcess[str]:
    cmd = ["git", "apply", "--whitespace=nowarn"]
    if check_only:
        cmd.append("--check")
    cmd.append("-")
    return subprocess.run(
        cmd,
        cwd=str(_social_debug_repo_root()),
        input=patch_unified_diff,
        text=True,
        capture_output=True,
        check=False,
    )


def run_social_debug(
    *,
    job_id: str,
    fetch_job_context: Callable[[str], dict[str, Any]],
    apply_patch: bool = False,
    confirm_apply: bool = False,
    include_context: bool = True,
) -> dict[str, Any]:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        raise ValueError("job_not_found")
    context_payload = fetch_job_context(normalized_job_id)
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise ValueError("openai_api_key_missing")

    source_context = _load_social_debug_source_context() if include_context else []
    prompt = _build_social_debug_prompt(job_context=context_payload, source_context=source_context)
    models = [_social_debug_model_name(), _social_debug_fallback_model_name()]
    models = [model for idx, model in enumerate(models) if model and (idx == 0 or model != models[0])]

    ai_payload: dict[str, Any] | None = None
    model_used: str | None = None
    fallback_used = False
    final_error: Exception | None = None
    for idx, model in enumerate(models):
        try:
            ai_payload = _run_social_debug_openai_completion(
                model=model,
                prompt=prompt,
                api_key=api_key,
                timeout_seconds=_social_debug_timeout_seconds(),
            )
            model_used = model
            fallback_used = idx > 0
            break
        except Exception as exc:  # noqa: BLE001
            final_error = exc
            continue
    if ai_payload is None or model_used is None:
        raise RuntimeError(f"openai_debug_failed: {final_error}")

    root_cause = str(ai_payload.get("root_cause") or "").strip() or "No root cause provided."
    raw_confidence: Any = ai_payload.get("confidence")
    try:
        confidence = max(0.0, min(1.0, float(raw_confidence)))
    except (TypeError, ValueError):
        confidence = 0.0
    files_touched_raw = ai_payload.get("files_touched")
    files_touched = (
        [str(item).strip() for item in files_touched_raw if str(item).strip()]
        if isinstance(files_touched_raw, list)
        else []
    )
    tests_to_run_raw = ai_payload.get("tests_to_run")
    tests_to_run = (
        [str(item).strip() for item in tests_to_run_raw if str(item).strip()]
        if isinstance(tests_to_run_raw, list)
        else []
    )
    patch_unified_diff = str(ai_payload.get("patch_unified_diff") or "")
    patch_max_bytes = _social_debug_patch_max_bytes()
    if len(patch_unified_diff.encode("utf-8")) > patch_max_bytes:
        patch_unified_diff = patch_unified_diff.encode("utf-8")[:patch_max_bytes].decode("utf-8", errors="ignore")

    apply_enabled = _social_debug_patch_apply_enabled()
    apply_requested = bool(apply_patch)
    apply_result = {
        "enabled": apply_enabled,
        "requested": apply_requested,
        "applied": False,
        "check_ok": False,
        "error": None,
        "files_changed": [],
    }
    if apply_requested:
        if not apply_enabled:
            apply_result["error"] = "Patch apply is disabled by server configuration."
        elif not confirm_apply:
            apply_result["error"] = "confirm_apply=true is required to apply patch."
        elif not patch_unified_diff.strip():
            apply_result["error"] = "No patch_unified_diff returned by debug model."
        else:
            paths = _extract_unified_diff_paths(patch_unified_diff)
            try:
                _validate_debug_patch_paths(paths)
                check_proc = _run_git_apply(patch_unified_diff=patch_unified_diff, check_only=True)
                apply_result["check_ok"] = check_proc.returncode == 0
                if check_proc.returncode != 0:
                    apply_result["error"] = (
                        check_proc.stderr or check_proc.stdout or "git apply --check failed"
                    ).strip()
                else:
                    apply_proc = _run_git_apply(patch_unified_diff=patch_unified_diff, check_only=False)
                    if apply_proc.returncode != 0:
                        apply_result["error"] = (apply_proc.stderr or apply_proc.stdout or "git apply failed").strip()
                    else:
                        apply_result["applied"] = True
                        apply_result["files_changed"] = paths
            except Exception as exc:  # noqa: BLE001
                apply_result["error"] = str(exc)

    return {
        "job_id": context_payload["job"]["id"],
        "run_id": context_payload["job"].get("run_id"),
        "model_used": model_used,
        "fallback_used": fallback_used,
        "analysis": {
            "root_cause": root_cause,
            "confidence": confidence,
            "files_touched": files_touched,
            "tests_to_run": tests_to_run,
        },
        "patch_unified_diff": patch_unified_diff,
        "apply": apply_result,
    }
