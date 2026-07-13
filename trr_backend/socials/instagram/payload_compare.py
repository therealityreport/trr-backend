from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import date, datetime, time
from enum import Enum
from typing import Any, Literal
from uuid import UUID

try:  # Observability is intentionally optional for worker/test import surfaces.
    from trr_backend.observability import get_trace_id as _get_trace_id
except Exception:  # pragma: no cover - exercised only in reduced runtime bundles
    _get_trace_id = None

PAYLOAD_COMPARE_SAMPLE_RATE_ENV = "SOCIAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE"
MAX_MISMATCH_RECORDS = 20

Classification = Literal["benign", "required"]
Path = tuple[str | int, ...]
PathPattern = tuple[str, ...]
ValueNormalizer = Callable[[Any], Any]

_MISSING = object()


@dataclass(frozen=True)
class PayloadComparePolicy:
    """Explicit exceptions to the default required-mismatch policy.

    Paths are tuples of object keys. ``"*"`` matches an array index. Diagnostic
    paths apply to the entire subtree; the other path sets match exactly.
    """

    header_order_paths: frozenset[PathPattern] = frozenset()
    diagnostic_paths: frozenset[PathPattern] = frozenset()
    set_like_paths: frozenset[PathPattern] = frozenset()
    normalizers: Mapping[PathPattern, ValueNormalizer] = field(default_factory=dict)


@dataclass(frozen=True)
class MismatchRecord:
    field_path_hash: str
    classification: Classification
    reason: str
    legacy_value_hash: str
    new_value_hash: str
    legacy_type: str
    new_type: str
    legacy_count: int | None
    new_count: int | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "field_path_hash": self.field_path_hash,
            "classification": self.classification,
            "reason": self.reason,
            "legacy_value_hash": self.legacy_value_hash,
            "new_value_hash": self.new_value_hash,
            "legacy_type": self.legacy_type,
            "new_type": self.new_type,
            "legacy_count": self.legacy_count,
            "new_count": self.new_count,
        }


@dataclass(frozen=True)
class PayloadComparison:
    legacy_payload_hash: str
    new_payload_hash: str
    classification: Literal["equal", "benign", "required"]
    required_count: int
    benign_count: int
    mismatches: tuple[MismatchRecord, ...]
    mismatch_records_truncated: bool


def payload_compare_sample_rate() -> float:
    """Read and clamp the compare sample rate; invalid values disable sampling."""

    raw = str(os.getenv(PAYLOAD_COMPARE_SAMPLE_RATE_ENV) or "").strip()
    try:
        rate = float(raw) if raw else 0.0
    except ValueError:
        return 0.0
    if not math.isfinite(rate):
        return 0.0
    return min(1.0, max(0.0, rate))


def entity_identity_hash(entity_identity: Any) -> str:
    return _sha256(_stable_identity_bytes(entity_identity))


def should_sample_payload_compare(*, surface: str, entity_identity: Any, sample_rate: float | None = None) -> bool:
    """Deterministically sample using only the surface and hashed identity."""

    rate = payload_compare_sample_rate() if sample_rate is None else _bounded_rate(sample_rate)
    if rate <= 0.0:
        return False
    if rate >= 1.0:
        return True
    identity_hash = entity_identity_hash(entity_identity)
    digest = hashlib.sha256(f"{surface}\0{identity_hash}".encode()).digest()
    bucket = int.from_bytes(digest, "big") / (1 << (len(digest) * 8))
    return bucket < rate


def canonical_value_hash(value: Any, *, policy: PayloadComparePolicy | None = None, path: Path = ()) -> str:
    configured = policy or PayloadComparePolicy()
    normalized = _canonicalize(value, path=path, policy=configured)
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return _sha256(encoded)


def compare_payloads(
    legacy_payload: Any,
    new_payload: Any,
    *,
    policy: PayloadComparePolicy | None = None,
    max_mismatches: int = MAX_MISMATCH_RECORDS,
) -> PayloadComparison:
    configured = policy or PayloadComparePolicy()
    collector = _MismatchCollector(policy=configured, limit=min(MAX_MISMATCH_RECORDS, max(0, max_mismatches)))
    _compare(legacy_payload, new_payload, path=(), policy=configured, collector=collector)
    overall: Literal["equal", "benign", "required"]
    if collector.required_count:
        overall = "required"
    elif collector.benign_count:
        overall = "benign"
    else:
        overall = "equal"
    return PayloadComparison(
        legacy_payload_hash=canonical_value_hash(legacy_payload, policy=configured),
        new_payload_hash=canonical_value_hash(new_payload, policy=configured),
        classification=overall,
        required_count=collector.required_count,
        benign_count=collector.benign_count,
        mismatches=tuple(collector.records),
        mismatch_records_truncated=(collector.required_count + collector.benign_count) > len(collector.records),
    )


def build_payload_compare_event(
    *,
    surface: str,
    entity_identity: Any,
    legacy_payload: Any,
    new_payload: Any,
    policy: PayloadComparePolicy | None = None,
    sidecar_present: bool,
    schema_unavailable: bool,
) -> dict[str, Any]:
    """Build one bounded, raw-value-free structured event for a caller to log."""

    comparison = compare_payloads(legacy_payload, new_payload, policy=policy)
    event: dict[str, Any] = {
        "event": "instagram_payload_compare",
        "surface": surface,
        "entity_identity_hash": entity_identity_hash(entity_identity),
        "legacy_payload_hash": comparison.legacy_payload_hash,
        "new_payload_hash": comparison.new_payload_hash,
        "classification": comparison.classification,
        "required_count": comparison.required_count,
        "benign_count": comparison.benign_count,
        "sidecar_present": bool(sidecar_present),
        "schema_unavailable": bool(schema_unavailable),
        "mismatch_records_truncated": comparison.mismatch_records_truncated,
        "mismatches": [record.as_dict() for record in comparison.mismatches],
    }
    trace_id = _current_trace_id()
    if trace_id:
        # Trace context can originate from request headers. Keep compare events
        # correlatable without copying an arbitrary or unbounded raw header into
        # every caller's logs.
        event["trace_id_hash"] = entity_identity_hash(trace_id)
    return event


@dataclass
class _MismatchCollector:
    policy: PayloadComparePolicy
    limit: int
    records: list[MismatchRecord] = field(default_factory=list)
    required_count: int = 0
    benign_count: int = 0

    def add(self, path: Path, reason: str, legacy: Any, new: Any) -> None:
        classification, classified_reason = _classify(path, reason, self.policy)
        if classification == "required":
            self.required_count += 1
        else:
            self.benign_count += 1
        if len(self.records) >= self.limit:
            return
        self.records.append(
            MismatchRecord(
                field_path_hash=_path_hash(path),
                classification=classification,
                reason=classified_reason,
                legacy_value_hash=_value_hash_or_missing(legacy, path=path, policy=self.policy),
                new_value_hash=_value_hash_or_missing(new, path=path, policy=self.policy),
                legacy_type=_json_type(legacy),
                new_type=_json_type(new),
                legacy_count=_value_count(legacy),
                new_count=_value_count(new),
            )
        )


def _compare(legacy: Any, new: Any, *, path: Path, policy: PayloadComparePolicy, collector: _MismatchCollector) -> None:
    normalizer = _matching_normalizer(path, policy)
    if normalizer is not None:
        legacy = normalizer(legacy)
        new = normalizer(new)

    legacy_type = _json_type(legacy)
    new_type = _json_type(new)
    if legacy_type != new_type:
        reason = "null_difference" if "null" in {legacy_type, new_type} else "type_difference"
        collector.add(path, reason, legacy, new)
        return

    if isinstance(legacy, dict):
        legacy_keys = list(legacy)
        new_keys = list(new)
        if set(legacy_keys) == set(new_keys) and legacy_keys != new_keys:
            collector.add(path, "object_key_order", legacy, new)
        for key in legacy_keys:
            if key not in new:
                collector.add((*path, key), "missing_key", legacy[key], _MISSING)
        for key in new_keys:
            if key not in legacy:
                collector.add((*path, key), "extra_key", _MISSING, new[key])
        for key in legacy_keys:
            if key in new:
                _compare(legacy[key], new[key], path=(*path, key), policy=policy, collector=collector)
        return

    if isinstance(legacy, list):
        legacy_items = legacy
        new_items = new
        if _path_matches_any(path, policy.set_like_paths):
            legacy_items = _sort_canonical_items(legacy, path=path, policy=policy)
            new_items = _sort_canonical_items(new, path=path, policy=policy)
        if len(legacy_items) != len(new_items):
            collector.add(path, "array_length_difference", legacy, new)
        elif _item_hashes(legacy_items, path=path, policy=policy) == _item_hashes(new_items, path=path, policy=policy):
            return
        elif Counter(_item_hashes(legacy_items, path=path, policy=policy)) == Counter(
            _item_hashes(new_items, path=path, policy=policy)
        ):
            reason = "header_order" if _path_matches_any(path, policy.header_order_paths) else "array_order_difference"
            collector.add(path, reason, legacy, new)
            return
        for index, (legacy_item, new_item) in enumerate(zip(legacy_items, new_items, strict=False)):
            _compare(legacy_item, new_item, path=(*path, index), policy=policy, collector=collector)
        return

    if legacy_type == "number":
        equal = legacy == new
    else:
        equal = legacy == new
    if not equal:
        collector.add(path, _scalar_reason(path), legacy, new)


def _classify(path: Path, reason: str, policy: PayloadComparePolicy) -> tuple[Classification, str]:
    if _path_has_prefix(path, policy.diagnostic_paths):
        return "benign", "diagnostic_difference"
    if reason in {"object_key_order", "header_order"}:
        return "benign", reason
    return "required", reason


def _scalar_reason(path: Path) -> str:
    tokens = _field_name_tokens(str(path[-1])) if path else ()
    if tokens and tokens[-1] in {"at", "time", "timestamp"}:
        return "timestamp_difference"
    if tokens and (tokens[-1] == "status" or tokens in {("http", "status"), ("status", "code")}):
        return "status_difference"
    if tokens and (tokens[-1] in {"count", "total"} or tokens[0] == "total"):
        return "count_difference"
    if tokens and (
        tokens[-1] in {"cursor", "pagination"} or tokens[-2:] in {("has", "next"), ("next", "page"), ("page", "info")}
    ):
        return "pagination_difference"
    if tokens and tokens[-1] in {"order", "position", "rank"}:
        return "order_difference"
    return "value_difference"


def _field_name_tokens(field_name: str) -> tuple[str, ...]:
    separated = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", field_name)
    separated = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", separated)
    return tuple(token.lower() for token in re.findall(r"[A-Za-z0-9]+", separated))


def _canonicalize(value: Any, *, path: Path, policy: PayloadComparePolicy) -> Any:
    normalizer = _matching_normalizer(path, policy)
    if normalizer is not None:
        value = normalizer(value)
    if isinstance(value, dict):
        return {key: _canonicalize(item, path=(*path, key), policy=policy) for key, item in value.items()}
    if isinstance(value, list):
        items = [_canonicalize(item, path=(*path, index), policy=policy) for index, item in enumerate(value)]
        if _path_matches_any(path, policy.set_like_paths):
            items.sort(key=_canonical_json)
        return items
    return value


def _sort_canonical_items(values: list[Any], *, path: Path, policy: PayloadComparePolicy) -> list[Any]:
    return sorted(values, key=lambda value: canonical_value_hash(value, policy=policy, path=(*path, 0)))


def _item_hashes(values: list[Any], *, path: Path, policy: PayloadComparePolicy) -> list[str]:
    return [canonical_value_hash(value, policy=policy, path=(*path, index)) for index, value in enumerate(values)]


def _matching_normalizer(path: Path, policy: PayloadComparePolicy) -> ValueNormalizer | None:
    for pattern, normalizer in policy.normalizers.items():
        if _path_matches(path, pattern):
            return normalizer
    return None


def _path_matches_any(path: Path, patterns: frozenset[PathPattern]) -> bool:
    return any(_path_matches(path, pattern) for pattern in patterns)


def _path_matches(path: Path, pattern: PathPattern) -> bool:
    return len(path) == len(pattern) and all(
        expected == "*" or str(actual) == expected for actual, expected in zip(path, pattern, strict=True)
    )


def _path_has_prefix(path: Path, patterns: frozenset[PathPattern]) -> bool:
    return any(len(path) >= len(pattern) and _path_matches(path[: len(pattern)], pattern) for pattern in patterns)


def _path_hash(path: Path) -> str:
    return _sha256(json.dumps(path, separators=(",", ":"), ensure_ascii=False).encode())


def _value_hash_or_missing(value: Any, *, path: Path, policy: PayloadComparePolicy) -> str:
    if value is _MISSING:
        return _sha256(b"<missing>")
    return canonical_value_hash(value, policy=policy, path=path)


def _json_type(value: Any) -> str:
    if value is _MISSING:
        return "missing"
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return f"unsupported:{type(value).__name__}"


def _value_count(value: Any) -> int | None:
    return len(value) if isinstance(value, (dict, list, str)) else None


def _stable_identity_bytes(identity: Any) -> bytes:
    canonical = _canonicalize_identity(identity, seen=set())
    return _canonical_json(canonical).encode()


def _canonicalize_identity(value: Any, *, seen: set[int]) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return {"$float": repr(value)}
    if isinstance(value, UUID):
        return {"$uuid": value.hex}
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, time):
        return {"$time": value.isoformat()}
    if isinstance(value, bytes):
        return {"$bytes": value.hex()}
    if isinstance(value, Enum):
        return {
            "$enum": _qualified_type_name(value),
            "value": _canonicalize_identity(value.value, seen=seen),
        }

    identity = id(value)
    if identity in seen:
        return {"$cycle": _qualified_type_name(value)}
    seen.add(identity)
    try:
        if is_dataclass(value) and not isinstance(value, type):
            return {
                "$dataclass": _qualified_type_name(value),
                "fields": [
                    [item.name, _canonicalize_identity(getattr(value, item.name), seen=seen)] for item in fields(value)
                ],
            }
        if isinstance(value, Mapping):
            entries = [
                [
                    _canonicalize_identity(key, seen=seen),
                    _canonicalize_identity(item, seen=seen),
                ]
                for key, item in value.items()
            ]
            entries.sort(key=_canonical_json)
            return {"$mapping": entries}
        if isinstance(value, Sequence):
            return {
                "$sequence": _qualified_type_name(value),
                "items": [_canonicalize_identity(item, seen=seen) for item in value],
            }
        if isinstance(value, (set, frozenset)):
            items = [_canonicalize_identity(item, seen=seen) for item in value]
            items.sort(key=_canonical_json)
            return {"$set": _qualified_type_name(value), "items": items}
        return {"$type": _qualified_type_name(value)}
    finally:
        seen.remove(identity)


def _qualified_type_name(value: Any) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _bounded_rate(rate: float) -> float:
    try:
        numeric = float(rate)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(numeric):
        return 0.0
    return min(1.0, max(0.0, numeric))


def _current_trace_id() -> str | None:
    if _get_trace_id is None:
        return None
    try:
        value = _get_trace_id()
    except Exception:  # pragma: no cover - defensive optional integration boundary
        return None
    return str(value) if value else None

