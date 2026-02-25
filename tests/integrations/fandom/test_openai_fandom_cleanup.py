from __future__ import annotations

from types import SimpleNamespace

from trr_backend.integrations import openai_fandom_cleanup


def test_cleanup_returns_none_when_api_key_missing(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    payload, model = openai_fandom_cleanup.cleanup_fandom_payload_with_openai(
        entity_kind="person",
        entity_label="Lisa Barlow",
        aggregated={},
        source_variants=[],
    )
    assert payload is None
    assert model is None


def test_cleanup_handles_network_error_with_fallback(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_FANDOM_MODEL", "test-model")

    class FailingRequests:
        @staticmethod
        def post(*args, **kwargs):  # noqa: ANN002, ANN003
            raise RuntimeError("network down")

    monkeypatch.setattr(openai_fandom_cleanup, "requests", FailingRequests)
    payload, model = openai_fandom_cleanup.cleanup_fandom_payload_with_openai(
        entity_kind="person",
        entity_label="Lisa Barlow",
        aggregated={"summary": "x"},
        source_variants=[],
    )
    assert payload is None
    assert model == "test-model"


def test_cleanup_handles_invalid_json_response(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_FANDOM_MODEL", "test-model")

    response = SimpleNamespace(
        raise_for_status=lambda: None,
        json=lambda: {"choices": [{"message": {"content": "not-json"}}]},
    )
    monkeypatch.setattr(
        openai_fandom_cleanup,
        "requests",
        SimpleNamespace(post=lambda *args, **kwargs: response),  # noqa: ANN002, ANN003
    )
    payload, model = openai_fandom_cleanup.cleanup_fandom_payload_with_openai(
        entity_kind="season",
        entity_label="Season 1",
        aggregated={"summary": "x"},
        source_variants=[],
    )
    assert payload is None
    assert model == "test-model"
