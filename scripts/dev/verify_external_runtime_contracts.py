#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[2]

RENDER_BLUEPRINT_PATH = REPO_ROOT / "render.yaml"
RENDER_DOC_PATH = REPO_ROOT / "docs" / "deploy" / "render.md"
SOURCE_ENV_PATH = REPO_ROOT / ".env"


def default_component(*, verify_only: bool) -> dict[str, Any]:
    return {
        "state": "ok",
        "reason": None,
        "remediation": None,
        "verify_only": verify_only,
    }


def _resolve_env_value(key: str) -> str:
    runtime_value = str(os.getenv(key) or "").strip()
    if runtime_value:
        return runtime_value
    if SOURCE_ENV_PATH.is_file():
        loaded = dotenv_values(SOURCE_ENV_PATH)
        fallback = str(loaded.get(key) or "").strip()
        if fallback:
            return fallback
    return ""


def verify_render_contract() -> dict[str, Any]:
    component = default_component(verify_only=True)
    blueprint_text = RENDER_BLUEPRINT_PATH.read_text(encoding="utf-8")
    doc_text = RENDER_DOC_PATH.read_text(encoding="utf-8")
    required_blueprint_markers = ("runtime: docker", "healthCheckPath: /health", "autoDeploy: false")
    required_doc_markers = ("public FastAPI host on Render", "long-running and admin execution on Modal")
    missing = [marker for marker in required_blueprint_markers if marker not in blueprint_text]
    missing.extend(marker for marker in required_doc_markers if marker not in doc_text)
    if missing:
        component["state"] = "advisory"
        component["reason"] = "render_contract_mismatch"
        component["remediation"] = "Render blueprint/docs drift detected: " + ", ".join(missing)
    return component


def verify_decodo_contract() -> dict[str, Any]:
    component = default_component(verify_only=True)
    username = _resolve_env_value("DECODO_USERNAME")
    password = _resolve_env_value("DECODO_PASSWORD")
    gateway = _resolve_env_value("DECODO_GATEWAY") or "gate.decodo.com:7000"
    if not username or not password:
        component["state"] = "advisory"
        component["reason"] = "decodo_unconfigured"
        component["remediation"] = "Configure DECODO_USERNAME and DECODO_PASSWORD for proxy-backed hosted lanes."
    component["gateway"] = gateway
    return component


def verify_external_runtime_contracts() -> dict[str, Any]:
    render = verify_render_contract()
    decodo = verify_decodo_contract()
    state = "advisory" if "advisory" in {render["state"], decodo["state"]} else "ok"
    reason_parts = [component["reason"] for component in (render, decodo) if component.get("reason")]
    return {
        "state": state,
        "reason": ",".join(reason_parts) if reason_parts else None,
        "render": render,
        "decodo": decodo,
    }


def main() -> int:
    emit_json = "--json" in sys.argv[1:]
    result = verify_external_runtime_contracts()
    if emit_json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
