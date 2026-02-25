from __future__ import annotations

import json
import os
from typing import Any

try:
    import requests
except Exception:  # noqa: BLE001
    requests = None

_DEFAULT_MODEL = "gpt-4.1-mini"


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        if isinstance(value, dict):
            return {str(k): _json_safe(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_json_safe(item) for item in value]
        return str(value)


def _normalize_cleanup_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "casting_summary": payload.get("casting_summary") if isinstance(payload.get("casting_summary"), str) else None,
        "bio_card": payload.get("bio_card") if isinstance(payload.get("bio_card"), dict) else None,
        "sections": payload.get("sections") if isinstance(payload.get("sections"), list) else None,
        "citations": payload.get("citations") if isinstance(payload.get("citations"), list) else None,
        "conflicts": payload.get("conflicts") if isinstance(payload.get("conflicts"), list) else None,
        "canonical_field_overrides": (
            payload.get("canonical_field_overrides")
            if isinstance(payload.get("canonical_field_overrides"), dict)
            else None
        ),
    }


def cleanup_fandom_payload_with_openai(
    *,
    entity_kind: str,
    entity_label: str,
    aggregated: dict[str, Any],
    source_variants: list[dict[str, Any]],
    timeout_seconds: float = 40.0,
) -> tuple[dict[str, Any] | None, str | None]:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return None, None
    if requests is None:
        return None, None

    model = (os.getenv("OPENAI_FANDOM_MODEL") or _DEFAULT_MODEL).strip() or _DEFAULT_MODEL

    instructions = (
        "Return JSON only. Clean and consolidate fandom data from multiple sources.\n"
        "Required object keys: casting_summary, bio_card, sections, citations, conflicts, canonical_field_overrides.\n"
        "Use concise factual prose. Preserve uncertainty by adding entries in conflicts.\n"
        "canonical_field_overrides should only include fields with high confidence.\n"
    )
    user_payload = {
        "entity_kind": entity_kind,
        "entity_label": entity_label,
        "aggregated": _json_safe(aggregated),
        "source_variants": _json_safe(source_variants),
    }

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": instructions},
            {"role": "user", "content": json.dumps(user_payload, separators=(",", ":"))},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.1,
    }

    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:  # noqa: BLE001
        return None, model

    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return None, model
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        return None, model

    try:
        parsed = json.loads(content)
    except ValueError:
        return None, model
    if not isinstance(parsed, dict):
        return None, model
    return _normalize_cleanup_payload(parsed), model
