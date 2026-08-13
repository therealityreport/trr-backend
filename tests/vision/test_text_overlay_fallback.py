from __future__ import annotations

from dataclasses import dataclass

import pytest

from trr_backend.vision import text_overlay

pytestmark = pytest.mark.vision


@dataclass
class _Resp:
    data: list[dict]
    error: None = None


class _FakeDb:
    def __init__(self, rows_by_table: dict[str, dict]):
        self._rows_by_table = rows_by_table
        self._table = ""
        self.updated_rows: dict[str, dict] = {}

    def schema(self, _name: str):
        return self

    def table(self, name: str):
        self._table = name
        return self

    def select(self, _fields: str):
        return self

    def eq(self, _field: str, _value: str):
        return self

    def limit(self, _n: int):
        return self

    def update(self, payload: dict):
        self.updated_rows[self._table] = payload
        return self

    def execute(self):
        if self._table in self.updated_rows:
            return _Resp(data=[self.updated_rows[self._table]])
        row = self._rows_by_table.get(self._table)
        return _Resp(data=[row] if row else [])


class _FakePart:
    def __init__(self, text: str | None) -> None:
        self.text = text


class _FakeContent:
    def __init__(self, parts: list[object]) -> None:
        self.parts = parts


class _FakeCandidate:
    def __init__(self, *, parts: list[object], finish_reason: object = None) -> None:
        self.content = _FakeContent(parts)
        self.finish_reason = finish_reason


class _NoQuickTextResponse:
    def __init__(self, candidates: list[object]) -> None:
        self.candidates = candidates

    @property
    def text(self) -> str:
        raise RuntimeError("quick accessor unavailable")


def test_extract_gemini_response_text_uses_candidate_parts_when_quick_text_unavailable() -> None:
    response = _NoQuickTextResponse(
        [
            _FakeCandidate(parts=[_FakePart(None)], finish_reason=2),
            _FakeCandidate(parts=[_FakePart('{"has_text_overlay": false}')], finish_reason=1),
        ]
    )
    text, finish_reason = text_overlay._extract_gemini_response_text(response)
    assert text == '{"has_text_overlay": false}'
    assert finish_reason in ("1", "2")


def test_extract_gemini_response_text_returns_finish_reason_when_empty() -> None:
    response = _NoQuickTextResponse(
        [
            _FakeCandidate(parts=[], finish_reason=2),
        ]
    )
    text, finish_reason = text_overlay._extract_gemini_response_text(response)
    assert text == ""
    assert finish_reason == "2"


def test_extract_first_json_object_accepts_incomplete_object_with_has_text_overlay() -> None:
    parsed = text_overlay._extract_first_json_object('{"has_text_overlay": true')
    assert parsed["has_text_overlay"] is True


def test_extract_first_json_object_parses_truncated_fields_with_confidence() -> None:
    parsed = text_overlay._extract_first_json_object('{"has_text_overlay": "no", "confidence": 0.73')
    assert parsed["has_text_overlay"] is False
    assert parsed["confidence"] == 0.73


def test_extract_first_json_object_raises_when_no_parseable_fields() -> None:
    try:
        text_overlay._extract_first_json_object('{ "')
    except text_overlay.TextOverlayDetectionError as exc:
        assert "did not contain JSON object" in str(exc)
    else:
        raise AssertionError("expected TextOverlayDetectionError")


def test_build_unknown_text_overlay_result_sets_unknown_status() -> None:
    result = text_overlay._build_unknown_text_overlay_result(
        detector="gemini",
        model="gemini-2.5-flash",
        error="no candidate",
        finish_reason="2",
        reason_code=text_overlay.TEXT_OVERLAY_REASON_GEMINI_NO_TEXT,
    )
    assert result.status == "unknown"
    assert result.has_text_overlay is None
    assert result.error == "no candidate"
    assert result.finish_reason == "2"
    assert result.reason_code == text_overlay.TEXT_OVERLAY_REASON_GEMINI_NO_TEXT


def test_extract_existing_fields_accepts_persisted_unknown_status() -> None:
    existing = text_overlay._extract_existing_fields(
        {
            "has_text_overlay": None,
            "text_overlay_status": "unknown",
            "text_overlay_detector": "gemini",
            "text_overlay_model": "gemini-2.5-flash",
            "text_overlay_detected_at": "2026-02-11T00:00:00+00:00",
            "text_overlay_prompt_version": "v1",
            "text_overlay_error": "no candidate",
            "text_overlay_finish_reason": "2",
            "text_overlay_error_code": "gemini_no_text",
        }
    )
    assert existing is not None
    assert existing.status == "unknown"
    assert existing.has_text_overlay is None
    assert existing.error == "no candidate"
    assert existing.finish_reason == "2"
    assert existing.reason_code == "gemini_no_text"


def test_detect_cast_photo_text_overlay_retries_download_candidates(monkeypatch) -> None:
    db = _FakeDb(
        {
            "cast_photos": {
                "id": "photo-1",
                "source": "fandom",
                "hosted_url": None,
                "url": "https://real-housewives.fandom.com/wiki/Special:FilePath/bad.jpeg",
                "image_url": "https://real-housewives.fandom.com/wiki/Special:FilePath/good.jpeg",
                "thumb_url": None,
                "source_page_url": "https://real-housewives.fandom.com/wiki/Test",
                "metadata": {},
                "hosted_key": None,
            }
        }
    )

    calls: list[str] = []

    def fake_download(url: str, *, referer: str | None):
        calls.append(url)
        if "bad.jpeg" in url:
            raise text_overlay.TextOverlayTargetFetchError("404")
        return b"img", "image/jpeg"

    monkeypatch.setattr(text_overlay, "is_text_overlay_detection_configured", lambda: True)
    monkeypatch.setattr(text_overlay, "_download_image_bytes", fake_download)
    monkeypatch.setattr(
        text_overlay,
        "_detect_text_overlay_with_gemini",
        lambda *_args, **_kwargs: text_overlay.TextOverlayResult(
            has_text_overlay=True,
            confidence=0.95,
            detector="gemini",
            model="gemini-2.5-flash",
            detected_at="2026-02-11T00:00:00+00:00",
            prompt_version="v1",
        ),
    )

    result = text_overlay.detect_and_update_cast_photo_text_overlay(db, "photo-1", force=True)

    assert result.has_text_overlay is True
    assert len(calls) >= 2
    assert any("Special:FilePath/good.jpeg" in url for url in calls)


def test_detect_media_asset_text_overlay_uses_normalized_fandom_url(monkeypatch) -> None:
    db = _FakeDb(
        {
            "media_assets": {
                "id": "asset-1",
                "hosted_url": None,
                "source_url": "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie_k_s3.jpeg/revision/latest",
                "metadata": {},
                "hosted_key": None,
            }
        }
    )

    called_urls: list[str] = []

    def fake_download(url: str, *, referer: str | None):
        called_urls.append(url)
        return b"img", "image/jpeg"

    monkeypatch.setattr(text_overlay, "is_text_overlay_detection_configured", lambda: True)
    monkeypatch.setattr(text_overlay, "_download_image_bytes", fake_download)
    monkeypatch.setattr(
        text_overlay,
        "_detect_text_overlay_with_gemini",
        lambda *_args, **_kwargs: text_overlay.TextOverlayResult(
            has_text_overlay=False,
            confidence=0.88,
            detector="gemini",
            model="gemini-2.5-flash",
            detected_at="2026-02-11T00:00:00+00:00",
            prompt_version="v1",
        ),
    )

    result = text_overlay.detect_and_update_media_asset_text_overlay(db, "asset-1", force=True)

    assert result.has_text_overlay is False
    assert called_urls
    assert called_urls[0] == "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie_k_s3.jpeg"


def test_detect_media_asset_text_overlay_falls_back_to_hosted_key_when_url_download_fails(monkeypatch) -> None:
    db = _FakeDb(
        {
            "media_assets": {
                "id": "asset-1",
                "hosted_url": "https://cdn.example.com/missing.webp",
                "hosted_key": "images/people/example.webp",
                "source_url": "https://static.wikia.nocookie.net/real-housewives/images/missing.webp",
                "metadata": {},
            }
        }
    )

    url_calls: list[str] = []
    hosted_key_calls: list[str] = []

    def fake_download(url: str, *, referer: str | None):
        url_calls.append(url)
        raise text_overlay.TextOverlayTargetFetchError("403")

    def fake_download_hosted_key(hosted_key: str):
        hosted_key_calls.append(hosted_key)
        return b"img", "image/webp"

    monkeypatch.setattr(text_overlay, "is_text_overlay_detection_configured", lambda: True)
    monkeypatch.setattr(text_overlay, "_download_image_bytes", fake_download)
    monkeypatch.setattr(text_overlay, "_download_image_bytes_from_hosted_key", fake_download_hosted_key)
    monkeypatch.setattr(
        text_overlay,
        "_detect_text_overlay_with_gemini",
        lambda *_args, **_kwargs: text_overlay.TextOverlayResult(
            has_text_overlay=False,
            confidence=0.88,
            detector="gemini",
            model="gemini-2.5-flash",
            detected_at="2026-02-11T00:00:00+00:00",
            prompt_version="v1",
        ),
    )

    result = text_overlay.detect_and_update_media_asset_text_overlay(db, "asset-1", force=True)

    assert result.has_text_overlay is False
    assert url_calls
    assert hosted_key_calls == ["images/people/example.webp"]


def test_resolve_gemini_model_selection_prefers_fast_alias(monkeypatch) -> None:
    monkeypatch.setenv("GEMINI_MODEL_FAST", "gemini-2.5-flash-fast")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash-canonical")
    monkeypatch.delenv("GOOGLE_GEMINI_MODEL", raising=False)
    monkeypatch.delenv("GEMINI-MODEL", raising=False)

    model, source, route, fallback = text_overlay._resolve_gemini_model_selection()

    assert model == "gemini-2.5-flash-fast"
    assert source == "GEMINI_MODEL_FAST"
    assert route == "fast"
    assert fallback is None


def test_resolve_gemini_model_selection_tracks_canonical_fallback_path(monkeypatch) -> None:
    monkeypatch.delenv("GEMINI_MODEL_FAST", raising=False)
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash-canonical")
    monkeypatch.delenv("GOOGLE_GEMINI_MODEL", raising=False)
    monkeypatch.delenv("GEMINI-MODEL", raising=False)

    model, source, route, fallback = text_overlay._resolve_gemini_model_selection()

    assert model == "gemini-2.5-flash-canonical"
    assert source == "GEMINI_MODEL"
    assert route == "fast"
    assert fallback == "GEMINI_MODEL_FAST->GEMINI_MODEL"


def test_detect_media_asset_text_overlay_persists_model_telemetry_fields(monkeypatch) -> None:
    db = _FakeDb(
        {
            "media_assets": {
                "id": "asset-1",
                "hosted_url": "https://cdn.example.com/image.webp",
                "source_url": None,
                "metadata": {},
                "hosted_key": None,
            }
        }
    )

    monkeypatch.setattr(text_overlay, "is_text_overlay_detection_configured", lambda: True)
    monkeypatch.setattr(text_overlay, "_download_image_bytes", lambda *_args, **_kwargs: (b"img", "image/webp"))
    monkeypatch.setattr(
        text_overlay,
        "_detect_text_overlay_with_gemini",
        lambda *_args, **_kwargs: text_overlay.TextOverlayResult(
            has_text_overlay=True,
            confidence=0.91,
            detector="gemini",
            model="gemini-2.5-flash",
            detected_at="2026-02-17T00:00:00+00:00",
            prompt_version="v1",
            model_source="GEMINI_MODEL",
            model_route="fast",
            model_fallback_path="GEMINI_MODEL_FAST->GEMINI_MODEL",
        ),
    )

    result = text_overlay.detect_and_update_media_asset_text_overlay(db, "asset-1", force=True)
    stored = db.updated_rows["media_assets"]["metadata"]

    assert result.has_text_overlay is True
    assert stored["text_overlay_model_source"] == "GEMINI_MODEL"
    assert stored["text_overlay_model_route"] == "fast"
    assert stored["text_overlay_model_fallback_path"] == "GEMINI_MODEL_FAST->GEMINI_MODEL"
