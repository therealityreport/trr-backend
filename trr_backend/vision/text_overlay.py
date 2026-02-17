from __future__ import annotations

import base64
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import requests

from trr_backend.media.s3_mirror import get_s3_bucket, get_s3_client, normalize_fandom_file_url

LOGGER = logging.getLogger(__name__)
_DEPRECATED_GEMINI_MODEL_ALIAS_WARNED = False


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
TEXT_OVERLAY_REASON_DOWNLOAD_FAILED = "download_failed"
TEXT_OVERLAY_REASON_GEMINI_REQUEST_FAILED = "gemini_request_failed"
TEXT_OVERLAY_REASON_GEMINI_NO_TEXT = "gemini_no_text"
TEXT_OVERLAY_REASON_GEMINI_JSON_PARSE_FAILED = "gemini_json_parse_failed"
TEXT_OVERLAY_REASON_DB_UPDATE_FAILED = "db_update_failed"


@dataclass(frozen=True)
class TextOverlayResult:
    has_text_overlay: bool | None
    confidence: float | None
    detector: str
    model: str | None
    detected_at: str
    prompt_version: str
    status: Literal["detected", "unknown"] = "detected"
    error: str | None = None
    finish_reason: str | None = None
    reason_code: str | None = None
    model_source: str | None = None
    model_route: str | None = None
    model_fallback_path: str | None = None

    def to_metadata_patch(self) -> dict[str, Any]:
        return {
            "has_text_overlay": self.has_text_overlay,
            "text_overlay_confidence": self.confidence,
            "text_overlay_detector": self.detector,
            "text_overlay_model": self.model,
            "text_overlay_detected_at": self.detected_at,
            "text_overlay_prompt_version": self.prompt_version,
            "text_overlay_status": self.status,
            "text_overlay_error": self.error,
            "text_overlay_finish_reason": self.finish_reason,
            "text_overlay_error_code": self.reason_code,
            "text_overlay_model_source": self.model_source,
            "text_overlay_model_route": self.model_route,
            "text_overlay_model_fallback_path": self.model_fallback_path,
        }


def _build_unknown_text_overlay_result(
    *,
    detector: str,
    model: str | None,
    error: str,
    finish_reason: str | None = None,
    reason_code: str | None = None,
    model_source: str | None = None,
    model_route: str | None = None,
    model_fallback_path: str | None = None,
) -> TextOverlayResult:
    return TextOverlayResult(
        has_text_overlay=None,
        confidence=None,
        detector=detector,
        model=model,
        detected_at=datetime.now(UTC).isoformat(),
        prompt_version=TEXT_OVERLAY_PROMPT_VERSION,
        status="unknown",
        error=error,
        finish_reason=finish_reason,
        reason_code=reason_code,
        model_source=model_source,
        model_route=model_route,
        model_fallback_path=model_fallback_path,
    )


def _get_gemini_api_key() -> str | None:
    for name in ("GEMINI_API_KEY", "GOOGLE_GEMINI_API_KEY"):
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return None


def _resolve_gemini_model_selection() -> tuple[str, str, str, str | None]:
    fast = (os.getenv("GEMINI_MODEL_FAST") or "").strip()
    if fast:
        return fast, "GEMINI_MODEL_FAST", "fast", None

    canonical = (os.getenv("GEMINI_MODEL") or "").strip()
    if canonical:
        return canonical, "GEMINI_MODEL", "fast", "GEMINI_MODEL_FAST->GEMINI_MODEL"

    google_alias = (os.getenv("GOOGLE_GEMINI_MODEL") or "").strip()
    if google_alias:
        return (
            google_alias,
            "GOOGLE_GEMINI_MODEL",
            "fast",
            "GEMINI_MODEL_FAST->GEMINI_MODEL->GOOGLE_GEMINI_MODEL",
        )

    deprecated_alias = (os.getenv("GEMINI-MODEL") or "").strip()
    if deprecated_alias:
        global _DEPRECATED_GEMINI_MODEL_ALIAS_WARNED
        if not _DEPRECATED_GEMINI_MODEL_ALIAS_WARNED:
            LOGGER.warning("GEMINI-MODEL is deprecated; use GEMINI_MODEL")
            _DEPRECATED_GEMINI_MODEL_ALIAS_WARNED = True
        return (
            deprecated_alias,
            "GEMINI-MODEL",
            "fast",
            "GEMINI_MODEL_FAST->GEMINI_MODEL->GOOGLE_GEMINI_MODEL->GEMINI-MODEL",
        )

    return (
        "gemini-2.5-flash",
        "default",
        "fast",
        "GEMINI_MODEL_FAST->GEMINI_MODEL->GOOGLE_GEMINI_MODEL->default",
    )


def _get_gemini_model() -> str:
    model_name, _model_source, _model_route, _fallback_path = _resolve_gemini_model_selection()
    return model_name


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


def _stringify_finish_reason(value: Any) -> str | None:
    if value is None:
        return None
    name = getattr(value, "name", None)
    if isinstance(name, str) and name.strip():
        return name.strip()
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, (int, float)):
        return str(int(value))
    rendered = str(value).strip()
    return rendered or None


def _extract_candidate_text(candidate: Any) -> str:
    content = getattr(candidate, "content", None)
    parts = getattr(content, "parts", None)
    if not isinstance(parts, list):
        return ""

    chunks: list[str] = []
    for part in parts:
        text = None
        if isinstance(part, dict):
            text = part.get("text")
        else:
            text = getattr(part, "text", None)
        if isinstance(text, str) and text.strip():
            chunks.append(text)
    return "\n".join(chunks).strip()


def _extract_gemini_response_text(response: Any) -> tuple[str, str | None]:
    """Extract response text without relying on fragile quick accessors."""
    finish_reason: str | None = None
    try:
        quick_text = getattr(response, "text", None)
    except Exception:  # noqa: BLE001
        quick_text = None
    if isinstance(quick_text, str) and quick_text.strip():
        return quick_text.strip(), None

    candidates = getattr(response, "candidates", None)
    if isinstance(candidates, list) and candidates:
        finish_reason = _stringify_finish_reason(getattr(candidates[0], "finish_reason", None))
        for candidate in candidates:
            candidate_text = _extract_candidate_text(candidate)
            if candidate_text:
                return candidate_text, _stringify_finish_reason(
                    getattr(candidate, "finish_reason", None)
                ) or finish_reason

    return "", finish_reason


def _extract_first_json_object(text: str) -> dict[str, Any]:
    candidate = _strip_code_fences(text)
    if not candidate:
        raise TextOverlayDetectionError("Gemini returned empty response")

    # Try full JSON decode from the first object boundary.
    decoder = json.JSONDecoder()
    for idx, char in enumerate(candidate):
        if char != "{":
            continue
        try:
            parsed, _end_idx = decoder.raw_decode(candidate[idx:])
        except Exception:  # noqa: BLE001
            continue
        if isinstance(parsed, dict):
            return parsed

    # Fallback: recover useful fields from partially-truncated model output,
    # e.g. '{"has_text_overlay": false' without the closing brace.
    bool_match = re.search(
        r'"?has_text_overlay"?\s*[:=]\s*"?\s*(true|false|yes|no|1|0)\s*"?',
        candidate,
        flags=re.IGNORECASE,
    )
    fallback_value = _as_bool(bool_match.group(1)) if bool_match else None
    if fallback_value is not None:
        parsed: dict[str, Any] = {"has_text_overlay": fallback_value}
        confidence_match = re.search(
            r'"?confidence"?\s*[:=]\s*"?\s*([0-9]+(?:\.[0-9]+)?)\s*%?\s*"?',
            candidate,
            flags=re.IGNORECASE,
        )
        if confidence_match:
            parsed["confidence"] = _as_float_0_1(confidence_match.group(1))
        return parsed

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


def _is_http_url(value: str | None) -> bool:
    if not isinstance(value, str):
        return False
    trimmed = value.strip().lower()
    return trimmed.startswith("http://") or trimmed.startswith("https://")


def _iter_unique_urls(candidates: list[str | None]) -> list[str]:
    seen: set[str] = set()
    urls: list[str] = []
    for value in candidates:
        if not _is_http_url(value):
            continue
        normalized = str(value).strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls


def _build_media_asset_detection_urls(row: dict[str, Any], metadata: dict[str, Any]) -> list[str]:
    hosted_url = row.get("hosted_url")
    source_url = row.get("source_url")
    source_url_lower = source_url.lower() if isinstance(source_url, str) else ""
    referer = (metadata.get("page_url") if isinstance(metadata.get("page_url"), str) else None) or (
        metadata.get("source_page_url") if isinstance(metadata.get("source_page_url"), str) else None
    )
    if isinstance(source_url, str) and (
        "fandom" in source_url_lower or "static.wikia.nocookie.net" in source_url_lower
    ):
        normalized = normalize_fandom_file_url(source_url, referer=referer)
        return _iter_unique_urls([hosted_url, normalized, source_url])
    return _iter_unique_urls([hosted_url, source_url])


def _build_cast_photo_detection_urls(row: dict[str, Any], metadata: dict[str, Any]) -> list[str]:
    source = str(row.get("source") or "").lower()
    hosted_url = row.get("hosted_url")
    url = row.get("url")
    image_url = row.get("image_url")
    thumb_url = row.get("thumb_url")
    referer = (
        row.get("source_page_url")
        if isinstance(row.get("source_page_url"), str)
        else (metadata.get("source_page_url") if isinstance(metadata.get("source_page_url"), str) else None)
    )
    if source in {"fandom", "fandom-gallery"}:
        normalized = [
            normalize_fandom_file_url(str(value), referer=referer) if isinstance(value, str) else None
            for value in (url, image_url, thumb_url)
        ]
        return _iter_unique_urls([hosted_url, *normalized, url, image_url, thumb_url])
    return _iter_unique_urls([hosted_url, url, image_url, thumb_url])


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


def _download_image_bytes_from_hosted_key(hosted_key: str) -> tuple[bytes, str | None]:
    key = str(hosted_key or "").strip().lstrip("/")
    if not key:
        raise TextOverlayTargetFetchError("Hosted key is missing")
    try:
        client = get_s3_client()
        bucket = get_s3_bucket()
        response = client.get_object(Bucket=bucket, Key=key)
        body = response.get("Body")
        if body is None:
            raise TextOverlayTargetFetchError("S3 response body missing")
        data = body.read()
        content_type = (response.get("ContentType") or "").split(";", 1)[0].strip().lower() or None
        return data, content_type
    except Exception as exc:  # noqa: BLE001
        raise TextOverlayTargetFetchError(f"Failed to download hosted S3 object: {exc}") from exc


def _download_detection_target_bytes(
    *,
    urls: list[str],
    referer: str | None,
    hosted_key: str | None,
) -> tuple[bytes, str | None]:
    image_bytes: bytes | None = None
    content_type: str | None = None
    last_error: TextOverlayTargetFetchError | None = None

    for url in urls:
        try:
            image_bytes, content_type = _download_image_bytes(str(url), referer=referer)
            break
        except TextOverlayTargetFetchError as exc:
            last_error = exc

    if image_bytes is None and hosted_key:
        try:
            image_bytes, content_type = _download_image_bytes_from_hosted_key(hosted_key)
        except TextOverlayTargetFetchError as exc:
            last_error = exc

    if image_bytes is None:
        raise last_error or TextOverlayTargetFetchError("Failed to download image")

    return image_bytes, content_type


def _build_gemini_content_generator(*, api_key: str, model_name: str):
    try:
        from google import genai as google_genai  # type: ignore
        from google.genai import types as google_genai_types  # type: ignore

        client = google_genai.Client(api_key=api_key)

        def _generate(
            *,
            prompt: str,
            image_bytes: bytes,
            mime: str,
            generation_config: dict[str, Any],
        ) -> Any:
            contents: list[Any] = [prompt]
            try:
                contents.append(google_genai_types.Part.from_bytes(data=image_bytes, mime_type=mime))
            except Exception:  # noqa: BLE001
                # Fallback for SDK shape changes.
                contents.append(
                    {
                        "inline_data": {
                            "mime_type": mime,
                            "data": base64.b64encode(image_bytes).decode("utf-8"),
                        }
                    }
                )
            return client.models.generate_content(
                model=model_name,
                contents=contents,
                config=generation_config,
            )

        return _generate
    except Exception as exc:  # noqa: BLE001
        raise TextOverlayDetectionNotConfiguredError("google-genai is not installed") from exc


def _detect_text_overlay_with_gemini(image_bytes: bytes, *, content_type: str | None) -> TextOverlayResult:
    api_key = _get_gemini_api_key()
    if not api_key:
        raise TextOverlayDetectionNotConfiguredError("GEMINI_API_KEY is not set")

    model_name, model_source, model_route, fallback_path = _resolve_gemini_model_selection()
    LOGGER.info(
        "Gemini text-overlay route=%s model=%s source=%s fallback_path=%s",
        model_route,
        model_name,
        model_source,
        fallback_path or "none",
    )
    generate_content = _build_gemini_content_generator(api_key=api_key, model_name=model_name)

    prompt = (
        "You are a strict JSON API.\n"
        "Determine whether this image contains overlaid text (captions, watermarks, subtitles, words or typography "
        "intentionally added on top of the image).\n"
        "Return ONLY a JSON object with:\n"
        '- "has_text_overlay": boolean\n'
        '- "confidence": number between 0 and 1 (optional)\n'
    )

    mime = content_type or "image/jpeg"

    def _request_model_text(*, structured_json: bool) -> tuple[str, str | None]:
        generation_config: dict[str, Any] = {
            "temperature": 0,
            "top_p": 1,
            "top_k": 1,
            "max_output_tokens": 512 if structured_json else 256,
        }
        if structured_json:
            generation_config["response_mime_type"] = "application/json"

        response = generate_content(
            prompt=prompt,
            image_bytes=image_bytes,
            mime=mime,
            generation_config=generation_config,
        )
        return _extract_gemini_response_text(response)

    try:
        text, finish_reason = _request_model_text(structured_json=False)
    except Exception as exc:  # noqa: BLE001
        return _build_unknown_text_overlay_result(
            detector="gemini",
            model=model_name,
            error=f"Gemini request failed: {exc}",
            reason_code=TEXT_OVERLAY_REASON_GEMINI_REQUEST_FAILED,
            model_source=model_source,
            model_route=model_route,
            model_fallback_path=fallback_path,
        )

    used_structured_retry = False
    if not text:
        # Retry once with structured JSON response mode in case the first response
        # produced no textual parts (seen with some finish reasons).
        try:
            text, retry_finish_reason = _request_model_text(structured_json=True)
            used_structured_retry = True
        except Exception as exc:  # noqa: BLE001
            return _build_unknown_text_overlay_result(
                detector="gemini",
                model=model_name,
                error=f"Gemini retry request failed: {exc}",
                finish_reason=finish_reason,
                reason_code=TEXT_OVERLAY_REASON_GEMINI_REQUEST_FAILED,
                model_source=model_source,
                model_route=model_route,
                model_fallback_path=fallback_path,
            )

        finish_reason = retry_finish_reason or finish_reason

    if not text:
        suffix = f" (finish_reason={finish_reason})" if finish_reason else ""
        return _build_unknown_text_overlay_result(
            detector="gemini",
            model=model_name,
            error=f"Gemini returned no candidate text content{suffix}",
            finish_reason=finish_reason,
            reason_code=TEXT_OVERLAY_REASON_GEMINI_NO_TEXT,
            model_source=model_source,
            model_route=model_route,
            model_fallback_path=fallback_path,
        )

    try:
        parsed = _extract_first_json_object(text)
    except TextOverlayDetectionError as parse_exc:
        if used_structured_retry:
            return _build_unknown_text_overlay_result(
                detector="gemini",
                model=model_name,
                error=f"Failed to parse Gemini response as JSON after retry: {parse_exc}",
                finish_reason=finish_reason,
                reason_code=TEXT_OVERLAY_REASON_GEMINI_JSON_PARSE_FAILED,
                model_source=model_source,
                model_route=model_route,
                model_fallback_path=fallback_path,
            )
        try:
            retry_text, retry_finish_reason = _request_model_text(structured_json=True)
        except Exception as exc:  # noqa: BLE001
            return _build_unknown_text_overlay_result(
                detector="gemini",
                model=model_name,
                error=(
                    "Failed to parse Gemini response as JSON and retry request failed: "
                    f"initial={parse_exc}; retry={exc}"
                ),
                finish_reason=finish_reason,
                reason_code=TEXT_OVERLAY_REASON_GEMINI_REQUEST_FAILED,
                model_source=model_source,
                model_route=model_route,
                model_fallback_path=fallback_path,
            )
        finish_reason = retry_finish_reason or finish_reason
        if not retry_text:
            return _build_unknown_text_overlay_result(
                detector="gemini",
                model=model_name,
                error=f"Failed to parse Gemini response as JSON and retry returned no text: {parse_exc}",
                finish_reason=finish_reason,
                reason_code=TEXT_OVERLAY_REASON_GEMINI_NO_TEXT,
                model_source=model_source,
                model_route=model_route,
                model_fallback_path=fallback_path,
            )
        try:
            parsed = _extract_first_json_object(retry_text)
        except TextOverlayDetectionError as retry_parse_exc:
            return _build_unknown_text_overlay_result(
                detector="gemini",
                model=model_name,
                error=(
                    "Failed to parse Gemini response as JSON after retry: "
                    f"initial={parse_exc}; retry={retry_parse_exc}"
                ),
                finish_reason=finish_reason,
                reason_code=TEXT_OVERLAY_REASON_GEMINI_JSON_PARSE_FAILED,
                model_source=model_source,
                model_route=model_route,
                model_fallback_path=fallback_path,
            )

    has_text_overlay = _as_bool(parsed.get("has_text_overlay"))
    if has_text_overlay is None:
        return _build_unknown_text_overlay_result(
            detector="gemini",
            model=model_name,
            error="Gemini response missing has_text_overlay boolean",
            finish_reason=finish_reason,
            reason_code=TEXT_OVERLAY_REASON_GEMINI_JSON_PARSE_FAILED,
            model_source=model_source,
            model_route=model_route,
            model_fallback_path=fallback_path,
        )

    confidence = _as_float_0_1(parsed.get("confidence"))
    now = datetime.now(UTC).isoformat()
    return TextOverlayResult(
        has_text_overlay=has_text_overlay,
        confidence=confidence,
        detector="gemini",
        model=model_name,
        detected_at=now,
        prompt_version=TEXT_OVERLAY_PROMPT_VERSION,
        status="detected",
        finish_reason=finish_reason,
        model_source=model_source,
        model_route=model_route,
        model_fallback_path=fallback_path,
    )


def _extract_existing_fields(metadata: dict[str, Any]) -> TextOverlayResult | None:
    has_text_overlay = _as_bool(metadata.get("has_text_overlay"))
    status_value = metadata.get("text_overlay_status")
    status = status_value.strip().lower() if isinstance(status_value, str) else None
    if has_text_overlay is None and status != "unknown":
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
    model_source = metadata.get("text_overlay_model_source")
    model_source_str = (
        model_source.strip()
        if isinstance(model_source, str) and model_source.strip()
        else None
    )
    model_route = metadata.get("text_overlay_model_route")
    model_route_str = (
        model_route.strip()
        if isinstance(model_route, str) and model_route.strip()
        else None
    )
    fallback_path = metadata.get("text_overlay_model_fallback_path")
    fallback_path_str = (
        fallback_path.strip()
        if isinstance(fallback_path, str) and fallback_path.strip()
        else None
    )
    prompt_version = metadata.get("text_overlay_prompt_version")
    prompt_version_str = (
        prompt_version.strip()
        if isinstance(prompt_version, str) and prompt_version.strip()
        else TEXT_OVERLAY_PROMPT_VERSION
    )
    stored_error = metadata.get("text_overlay_error")
    stored_error_str = stored_error.strip() if isinstance(stored_error, str) and stored_error.strip() else None
    reason_code = metadata.get("text_overlay_error_code")
    reason_code_str = reason_code.strip() if isinstance(reason_code, str) and reason_code.strip() else None
    finish_reason = metadata.get("text_overlay_finish_reason")
    finish_reason_str = (
        finish_reason.strip()
        if isinstance(finish_reason, str) and finish_reason.strip()
        else None
    )
    normalized_status: Literal["detected", "unknown"] = "unknown" if status == "unknown" else "detected"

    return TextOverlayResult(
        has_text_overlay=has_text_overlay,
        confidence=confidence,
        detector=detector_str,
        model=model_str,
        detected_at=detected_at_str,
        prompt_version=prompt_version_str,
        status=normalized_status,
        error=stored_error_str,
        finish_reason=finish_reason_str,
        reason_code=reason_code_str,
        model_source=model_source_str,
        model_route=model_route_str,
        model_fallback_path=fallback_path_str,
    )


def classify_text_overlay_failure_reason(error: Exception) -> str:
    if isinstance(error, TextOverlayDatabaseError):
        return TEXT_OVERLAY_REASON_DB_UPDATE_FAILED
    if isinstance(error, TextOverlayTargetFetchError):
        return TEXT_OVERLAY_REASON_DOWNLOAD_FAILED
    message = str(error).lower()
    if "no candidate text content" in message:
        return TEXT_OVERLAY_REASON_GEMINI_NO_TEXT
    if "parse gemini response as json" in message or "json" in message:
        return TEXT_OVERLAY_REASON_GEMINI_JSON_PARSE_FAILED
    if "gemini request failed" in message or "gemini retry request failed" in message:
        return TEXT_OVERLAY_REASON_GEMINI_REQUEST_FAILED
    return TEXT_OVERLAY_REASON_GEMINI_REQUEST_FAILED


def detect_and_update_media_asset_text_overlay(db: Any, asset_id: str, *, force: bool = False) -> TextOverlayResult:
    """
    Detect text overlay for a unified media_asset row and persist results into core.media_assets.metadata.
    """
    if not is_text_overlay_detection_configured():
        raise TextOverlayDetectionNotConfiguredError("GEMINI_API_KEY is not set")

    resp = (
        db.schema("core")
        .table("media_assets")
        .select("id, hosted_url, hosted_key, source_url, metadata")
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

    urls = _build_media_asset_detection_urls(row, metadata)
    if not urls:
        raise TextOverlayTargetInvalidError("Media asset has no valid image URL to analyze")

    referer = None
    if isinstance(metadata, dict):
        referer = (metadata.get("page_url") if isinstance(metadata.get("page_url"), str) else None) or (
            metadata.get("source_page_url") if isinstance(metadata.get("source_page_url"), str) else None
        )

    image_bytes, content_type = _download_detection_target_bytes(
        urls=urls,
        referer=referer,
        hosted_key=row.get("hosted_key") if isinstance(row.get("hosted_key"), str) else None,
    )

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
        .select("id, hosted_url, hosted_key, url, image_url, thumb_url, source, source_page_url, metadata")
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

    urls = _build_cast_photo_detection_urls(row, metadata)
    if not urls:
        raise TextOverlayTargetInvalidError("Cast photo has no valid image URL to analyze")

    referer = row.get("source_page_url") if isinstance(row.get("source_page_url"), str) else None
    image_bytes, content_type = _download_detection_target_bytes(
        urls=urls,
        referer=referer,
        hosted_key=row.get("hosted_key") if isinstance(row.get("hosted_key"), str) else None,
    )

    result = _detect_text_overlay_with_gemini(image_bytes, content_type=content_type)

    merged = dict(metadata or {})
    merged.update(result.to_metadata_patch())

    update_resp = db.schema("core").table("cast_photos").update({"metadata": merged}).eq("id", photo_id).execute()
    if hasattr(update_resp, "error") and update_resp.error:
        raise TextOverlayDatabaseError("Database error updating cast photo metadata")

    return result
