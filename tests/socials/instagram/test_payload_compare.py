from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from trr_backend.socials.instagram import payload_compare


@dataclass(frozen=True)
class _IdentityFixture:
    entity_id: UUID
    observed_at: datetime
    labels: tuple[str, ...]


class _OpaqueIdentity:
    pass


_BACKEND_ROOT = Path(__file__).resolve().parents[3]


def test_sample_rate_is_bounded_and_invalid_values_disable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(payload_compare.PAYLOAD_COMPARE_SAMPLE_RATE_ENV, "-0.4")
    assert payload_compare.payload_compare_sample_rate() == 0.0
    monkeypatch.setenv(payload_compare.PAYLOAD_COMPARE_SAMPLE_RATE_ENV, "1.4")
    assert payload_compare.payload_compare_sample_rate() == 1.0
    monkeypatch.setenv(payload_compare.PAYLOAD_COMPARE_SAMPLE_RATE_ENV, "nan")
    assert payload_compare.payload_compare_sample_rate() == 0.0
    monkeypatch.setenv(payload_compare.PAYLOAD_COMPARE_SAMPLE_RATE_ENV, "not-a-number")
    assert payload_compare.payload_compare_sample_rate() == 0.0


def test_sampling_is_deterministic_from_surface_and_hashed_identity() -> None:
    surface = "catalog_post"
    identity = "sensitive-handle"
    identity_hash = payload_compare.entity_identity_hash(identity)
    digest = hashlib.sha256(f"{surface}\0{identity_hash}".encode()).digest()
    bucket = int.from_bytes(digest, "big") / (1 << 256)

    assert payload_compare.should_sample_payload_compare(
        surface=surface, entity_identity=identity, sample_rate=bucket + 1e-12
    )
    assert not payload_compare.should_sample_payload_compare(
        surface=surface, entity_identity=identity, sample_rate=max(0.0, bucket - 1e-12)
    )
    assert payload_compare.should_sample_payload_compare(surface=surface, entity_identity=identity, sample_rate=1)
    assert not payload_compare.should_sample_payload_compare(surface=surface, entity_identity=identity, sample_rate=0)


def test_non_json_identity_hash_is_structural_and_equivalent_instances_match() -> None:
    first = _IdentityFixture(
        entity_id=UUID("11111111-1111-4111-8111-111111111111"),
        observed_at=datetime(2026, 7, 13, 12, tzinfo=UTC),
        labels=("profile", "catalog"),
    )
    second = _IdentityFixture(
        entity_id=UUID("11111111-1111-4111-8111-111111111111"),
        observed_at=datetime(2026, 7, 13, 12, tzinfo=UTC),
        labels=("profile", "catalog"),
    )
    assert first is not second
    assert payload_compare.entity_identity_hash(first) == payload_compare.entity_identity_hash(second)
    assert payload_compare.entity_identity_hash(_OpaqueIdentity()) == payload_compare.entity_identity_hash(
        _OpaqueIdentity()
    )


def test_opaque_identity_fallback_is_stable_across_processes() -> None:
    script = """
from trr_backend.socials.instagram.payload_compare import entity_identity_hash
class ProcessOpaque:
    pass
print(entity_identity_hash(ProcessOpaque()))
"""
    first = subprocess.check_output([sys.executable, "-c", script], cwd=_BACKEND_ROOT, text=True).strip()
    second = subprocess.check_output([sys.executable, "-c", script], cwd=_BACKEND_ROOT, text=True).strip()
    assert first == second


def test_canonical_hash_stability_and_array_policy() -> None:
    assert payload_compare.canonical_value_hash({"a": 1, "b": 2}) == payload_compare.canonical_value_hash(
        {"b": 2, "a": 1}
    )
    assert payload_compare.canonical_value_hash([1, 2]) != payload_compare.canonical_value_hash([2, 1])

    set_like = payload_compare.PayloadComparePolicy(set_like_paths=frozenset({("items",)}))
    assert payload_compare.canonical_value_hash({"items": [1, 2]}, policy=set_like) == (
        payload_compare.canonical_value_hash({"items": [2, 1]}, policy=set_like)
    )
    assert payload_compare.compare_payloads({"items": [1, 2]}, {"items": [2, 1]}, policy=set_like).classification == (
        "equal"
    )


def test_object_key_order_is_benign() -> None:
    result = payload_compare.compare_payloads({"a": 1, "b": 2}, {"b": 2, "a": 1})
    assert result.classification == "benign"
    assert result.required_count == 0
    assert result.benign_count == 1
    assert result.mismatches[0].reason == "object_key_order"


def test_configured_header_order_and_diagnostic_paths_are_benign() -> None:
    policy = payload_compare.PayloadComparePolicy(
        header_order_paths=frozenset({("headers",)}),
        diagnostic_paths=frozenset({("diagnostics",)}),
    )
    result = payload_compare.compare_payloads(
        {"headers": ["accept", "cookie"], "diagnostics": {"url": "https://old.example/private"}},
        {"headers": ["cookie", "accept"], "diagnostics": {"url": "https://new.example/private"}},
        policy=policy,
    )
    assert result.classification == "benign"
    assert result.required_count == 0
    assert result.benign_count == 2
    assert {record.reason for record in result.mismatches} == {"header_order", "diagnostic_difference"}


@pytest.mark.parametrize(
    ("legacy", "new", "reason"),
    [
        ({"kept": 1, "gone": 2}, {"kept": 1}, "missing_key"),
        ({"kept": 1}, {"kept": 1, "added": 2}, "extra_key"),
        ({"field": 1}, {"field": "1"}, "type_difference"),
        ({"field": None}, {"field": 0}, "null_difference"),
        ({"field": True}, {"field": 1}, "type_difference"),
        ({"field": "before"}, {"field": "after"}, "value_difference"),
        ({"items": [1]}, {"items": [1, 2]}, "array_length_difference"),
        ({"items": [1, 2]}, {"items": [2, 1]}, "array_order_difference"),
        ({"pagination_cursor": "a"}, {"pagination_cursor": "b"}, "pagination_difference"),
        ({"comment_count": 10}, {"comment_count": 11}, "count_difference"),
        ({"sort_order": 1}, {"sort_order": 2}, "order_difference"),
        ({"fetch_status": "ready"}, {"fetch_status": "blocked"}, "status_difference"),
        ({"updated_at": "2026-01-01T00:00:00Z"}, {"updated_at": "2026-01-02T00:00:00Z"}, "timestamp_difference"),
    ],
)
def test_required_mismatch_classes(legacy: object, new: object, reason: str) -> None:
    result = payload_compare.compare_payloads(legacy, new)
    assert result.classification == "required"
    assert result.required_count >= 1
    assert reason in {record.reason for record in result.mismatches}


@pytest.mark.parametrize("field_name", ["account_type", "discount_code", "status_page_url"])
def test_tokenized_scalar_classification_avoids_substring_false_positives(field_name: str) -> None:
    result = payload_compare.compare_payloads({field_name: "before"}, {field_name: "after"})
    assert result.mismatches[0].reason == "value_difference"


@pytest.mark.parametrize(
    ("field_name", "reason"),
    [
        ("fetchStatus", "status_difference"),
        ("commentCount", "count_difference"),
        ("total_comments", "count_difference"),
        ("nextPage", "pagination_difference"),
        ("sortOrder", "order_difference"),
    ],
)
def test_tokenized_scalar_classification_recognizes_terminal_and_known_keys(field_name: str, reason: str) -> None:
    result = payload_compare.compare_payloads({field_name: "before"}, {field_name: "after"})
    assert result.mismatches[0].reason == reason


def test_explicit_normalizer_can_make_timestamp_equal() -> None:
    policy = payload_compare.PayloadComparePolicy(
        normalizers={
            ("updated_at",): lambda value: str(value).replace("+00:00", "Z"),
        }
    )
    result = payload_compare.compare_payloads(
        {"updated_at": "2026-01-01T00:00:00+00:00"},
        {"updated_at": "2026-01-01T00:00:00Z"},
        policy=policy,
    )
    assert result.classification == "equal"
    assert result.required_count == result.benign_count == 0


def test_mixed_benign_and_required_is_required() -> None:
    result = payload_compare.compare_payloads({"a": 1, "b": 2}, {"b": 3, "a": 1})
    assert result.classification == "required"
    assert result.required_count == 1
    assert result.benign_count == 1


def test_mismatch_records_are_capped_at_twenty_but_counts_are_complete() -> None:
    result = payload_compare.compare_payloads({f"field_{index}": index for index in range(25)}, {})
    assert result.required_count == 25
    assert len(result.mismatches) == 20
    assert result.mismatch_records_truncated is True


def test_structured_event_contains_trace_and_never_leaks_raw_values_or_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret_trace_id = "request_supplied_trace_that_must_not_appear" * 100
    monkeypatch.setattr(payload_compare, "_get_trace_id", lambda: secret_trace_id)
    secret_handle = "private_handle_8492"
    secret_url = "https://example.test/private/8492"
    secret_status = "banned-private-8492"
    dynamic_path = "dynamic_private_field_8492"
    event = payload_compare.build_payload_compare_event(
        surface="profile",
        entity_identity=secret_handle,
        legacy_payload={dynamic_path: {"url": secret_url, "status": secret_status}},
        new_payload={dynamic_path: {"url": "https://other.test/private", "status": "ready"}},
        sidecar_present=True,
        schema_unavailable=False,
    )
    serialized = json.dumps(event, sort_keys=True)

    assert "trace_id" not in event
    assert event["trace_id_hash"] == payload_compare.entity_identity_hash(secret_trace_id)
    assert event["entity_identity_hash"] == payload_compare.entity_identity_hash(secret_handle)
    assert event["sidecar_present"] is True
    assert event["schema_unavailable"] is False
    assert secret_handle not in serialized
    assert secret_url not in serialized
    assert secret_status not in serialized
    assert dynamic_path not in serialized
    assert secret_trace_id not in serialized
    assert all(
        set(record)
        == {
            "field_path_hash",
            "classification",
            "reason",
            "legacy_value_hash",
            "new_value_hash",
            "legacy_type",
            "new_type",
            "legacy_count",
            "new_count",
        }
        for record in event["mismatches"]
    )


def test_trace_lookup_failure_degrades_safely(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise() -> str:
        raise RuntimeError("observability unavailable")

    monkeypatch.setattr(payload_compare, "_get_trace_id", _raise)
    event = payload_compare.build_payload_compare_event(
        surface="post",
        entity_identity="123",
        legacy_payload={},
        new_payload={},
        sidecar_present=False,
        schema_unavailable=True,
    )
    assert "trace_id" not in event
    assert "trace_id_hash" not in event
    assert event["schema_unavailable"] is True
