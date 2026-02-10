from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import requests


class TextOverlayDetectionNotConfiguredError(RuntimeError):
    pass


class TextOverlayDetectionError(RuntimeError):
    pass


class TextOverlayTargetNotFoundError(TextOverlayDetectionError):
    pass


class TextOverlayTargetInvalidError(TextOverlayDetectionError):
    pass


class TextOverlayTargetFetchError(TextOverlayDetectionError):
    pass


class TextOverlayDatabaseError(TextOverlayDetectionError):
    pass


TEXT_OVERLAY_PROMPT_VERSION = "v1"


@dataclass(frozen=True)
class TextOverlayResult:
    has_text_overlay: bool
    confidence: float | None
    detector: str
    model: str | None
    detected_at: str
    prompt_version: str

    def to_metadata_patch(self) -> dict[str, Any]:
        return {
            "has_text_overlay": self.has_text_overlay,
            "text_overlay_confidence": self.confidence,
            "text_overlay_detector": self.detector,
            "text_overlay_model": self.model,
            "text_overlay_detected_at": self.detected_at,
            "text_overlay_prompt_version": self.prompt_version,
        }


def _get_gemini_api_key() -> str | None:
    for name in ("GEMINI_API_KEY", "GOOGLE_GEMINI_API_KEY"):
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return None


def _get_gemini_model() -> str:
    for name in ("GOOGLE_GEMINI_MODEL", "GEMINI_MODEL"):
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return "gemini-2.5-flash"


def is_text_overlay_detection_configured() -> bool:
    return bool(_get_gemini_api_key())


def _strip_code_fences(text: str) -> str:
    trimmed = (text or "").strip()
    if trimmed.startswith("```"):
        # Remove leading fence line
        trimmed = trimmed.split("\n", 1)[1] if "\n" in trimmed else ""
        # Remove trailing fence
        if "```" in trimmed:
            trimmed = trimmed.rsplit("```", 1)[0]
    return trimmed.strip()


def _extract_first_json_object(text: str) -> dict[str, Any]:
    candidate = _strip_code_fences(text)
    if not candidate:
        raise TextOverlayDetectionError("Gemini returned empty response")

    start = candidate.find("{")
    end = candidate.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise TextOverlayDetectionError(f"Gemini response did not contain JSON object: {candidate[:200]}")

    raw = candidate[start : end + 1]
    try:
        parsed = json.loads(raw)
    except Exception as exc:  # noqa: BLE001 - want full context in error
        raise TextOverlayDetectionError(f"Failed to parse JSON from Gemini response: {raw[:200]}") from exc

    if not isinstance(parsed, dict):
        raise TextOverlayDetectionError("Gemini JSON was not an object")
    return parsed


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value in (0, 1):
            return bool(value)
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in ("true", "yes", "y", "1"):
            return True
        if raw in ("false", "no", "n", "0"):
            return False
    return None


def _as_float_0_1(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        as_float = float(value)
        if as_float < 0:
            return 0.0
        if as_float > 1:
            return 1.0
        return as_float
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return _as_float_0_1(float(raw))
        except ValueError:
            return None
    return None


def _download_image_bytes(url: str, *, referer: str | None) -> tuple[bytes, str | None]:
    headers = {
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    if referer:
        headers["referer"] = referer

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        content_type = (resp.headers.get("content-type") or "").split(";", 1)[0].strip().lower() or None
        return resp.content, content_type
    except Exception as exc:  # noqa: BLE001
        raise TextOverlayTargetFetchError(f"Failed to download image: {exc}") from exc


def _detect_text_overlay_with_gemini(image_bytes: bytes, *, content_type: str | None) -> TextOverlayResult:
    api_key = _get_gemini_api_key()
    if not api_key:
        raise TextOverlayDetectionNotConfiguredError("GEMINI_API_KEY is not set")

    # Lazy import to avoid import-time failures/hangs in some environments.
    try:
        import google.generativeai as genai  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise TextOverlayDetectionNotConfiguredError("google-generativeai is not installed") from exc

    model_name = _get_gemini_model()
    genai.configure(api_key=api_key)

    prompt = (
        "You are a strict JSON API.\n"
        "Determine whether this image contains overlaid text (captions, watermarks, subtitles, words or typography "
        "intentionally added on top of the image).\n"
        "Return ONLY a JSON object with:\n"
        '- "has_text_overlay": boolean\n'
        '- "confidence": number between 0 and 1 (optional)\n'
    )

    model = genai.GenerativeModel(model_name)

    mime = content_type or "image/jpeg"
    try:
        response = model.generate_content(
            [
                prompt,
                {"mime_type": mime, "data": image_bytes},
            ],
            generation_config={
                "temperature": 0,
                "top_p": 1,
                "top_k": 1,
                "max_output_tokens": 256,
            },
        )
    except Exception as exc:  # noqa: BLE001
        raise TextOverlayDetectionError(f"Gemini request failed: {exc}") from exc

    text = getattr(response, "text", None) or ""
    parsed = _extract_first_json_object(text)

    has_text_overlay = _as_bool(parsed.get("has_text_overlay"))
    if has_text_overlay is None:
        raise TextOverlayDetectionError("Gemini response missing has_text_overlay boolean")

    confidence = _as_float_0_1(parsed.get("confidence"))
    now = datetime.now(UTC).isoformat()
    return TextOverlayResult(
        has_text_overlay=has_text_overlay,
        confidence=confidence,
        detector="gemini",
        model=model_name,
        detected_at=now,
        prompt_version=TEXT_OVERLAY_PROMPT_VERSION,
    )


def _extract_existing_fields(metadata: dict[str, Any]) -> TextOverlayResult | None:
    has_text_overlay = _as_bool(metadata.get("has_text_overlay"))
    if has_text_overlay is None:
        return None

    detected_at = metadata.get("text_overlay_detected_at")
    detected_at_str = detected_at.strip() if isinstance(detected_at, str) and detected_at.strip() else None
    if not detected_at_str:
        detected_at_str = datetime.now(UTC).isoformat()

    confidence = _as_float_0_1(metadata.get("text_overlay_confidence"))
    detector = metadata.get("text_overlay_detector")
    detector_str = detector.strip() if isinstance(detector, str) and detector.strip() else "unknown"
    model = metadata.get("text_overlay_model")
    model_str = model.strip() if isinstance(model, str) and model.strip() else None
    prompt_version = metadata.get("text_overlay_prompt_version")
    prompt_version_str = (
        prompt_version.strip()
        if isinstance(prompt_version, str) and prompt_version.strip()
        else TEXT_OVERLAY_PROMPT_VERSION
    )

    return TextOverlayResult(
        has_text_overlay=has_text_overlay,
        confidence=confidence,
        detector=detector_str,
        model=model_str,
        detected_at=detected_at_str,
        prompt_version=prompt_version_str,
    )


def detect_and_update_media_asset_text_overlay(db: Any, asset_id: str, *, force: bool = False) -> TextOverlayResult:
    """
    Detect text overlay for a unified media_asset row and persist results into core.media_assets.metadata.
    """
    if not is_text_overlay_detection_configured():
        raise TextOverlayDetectionNotConfiguredError("GEMINI_API_KEY is not set")

    resp = (
        db.schema("core")
        .table("media_assets")
        .select("id, hosted_url, source_url, metadata")
        .eq("id", asset_id)
        .limit(1)
        .execute()
    )
    if hasattr(resp, "error") and resp.error:
        raise TextOverlayDatabaseError("Database error fetching media asset")
    if not resp.data:
        raise TextOverlayTargetNotFoundError("Media asset not found")

    row = resp.data[0]
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    existing = _extract_existing_fields(metadata)
    if existing and not force:
        return existing

    url = row.get("hosted_url") or row.get("source_url")
    if not url:
        raise TextOverlayTargetInvalidError("Media asset has no hosted_url or source_url to analyze")

    referer = None
    if isinstance(metadata, dict):
        referer = (metadata.get("page_url") if isinstance(metadata.get("page_url"), str) else None) or (
            metadata.get("source_page_url") if isinstance(metadata.get("source_page_url"), str) else None
        )

    image_bytes, content_type = _download_image_bytes(str(url), referer=referer)
    result = _detect_text_overlay_with_gemini(image_bytes, content_type=content_type)

    merged = dict(metadata or {})
    merged.update(result.to_metadata_patch())

    update_resp = db.schema("core").table("media_assets").update({"metadata": merged}).eq("id", asset_id).execute()
    if hasattr(update_resp, "error") and update_resp.error:
        raise TextOverlayDatabaseError("Database error updating media asset metadata")

    return result


def detect_and_update_cast_photo_text_overlay(db: Any, photo_id: str, *, force: bool = False) -> TextOverlayResult:
    """
    Detect text overlay for a legacy cast_photos row and persist results into core.cast_photos.metadata.
    """
    if not is_text_overlay_detection_configured():
        raise TextOverlayDetectionNotConfiguredError("GEMINI_API_KEY is not set")

    resp = (
        db.schema("core")
        .table("cast_photos")
        .select("id, hosted_url, url, image_url, source_page_url, metadata")
        .eq("id", photo_id)
        .limit(1)
        .execute()
    )
    if hasattr(resp, "error") and resp.error:
        raise TextOverlayDatabaseError("Database error fetching cast photo")
    if not resp.data:
        raise TextOverlayTargetNotFoundError("Cast photo not found")

    row = resp.data[0]
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    existing = _extract_existing_fields(metadata)
    if existing and not force:
        return existing

    url = row.get("hosted_url") or row.get("url") or row.get("image_url")
    if not url:
        raise TextOverlayTargetInvalidError("Cast photo has no hosted_url or url to analyze")

    referer = row.get("source_page_url") if isinstance(row.get("source_page_url"), str) else None
    image_bytes, content_type = _download_image_bytes(str(url), referer=referer)
    result = _detect_text_overlay_with_gemini(image_bytes, content_type=content_type)

    merged = dict(metadata or {})
    merged.update(result.to_metadata_patch())

    update_resp = db.schema("core").table("cast_photos").update({"metadata": merged}).eq("id", photo_id).execute()
    if hasattr(update_resp, "error") and update_resp.error:
        raise TextOverlayDatabaseError("Database error updating cast photo metadata")

    return result
