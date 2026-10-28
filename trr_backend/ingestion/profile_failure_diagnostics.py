"""Fixed, bounded public-profile failure facts; no source text or delivery inputs."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any

PARSER_VERSION = "instagram-public-profile-failure-v1"
PHASES = frozenset(
    {
        "request",
        "body",
        "response_classification",
        "bootstrap_parse",
        "decode",
        "projection",
        "parse",
        "source_contract",
        "admission",
    }
)
REASONS = frozenset(
    {
        "unknown",
        "parse_error",
        "network_error",
        "request_failed",
        "public_access_backoff",
        "rate_limited",
        "target_deadline",
        "avatar_request_deadline",
        "avatar_request_boundary_missing",
        "avatar_hidden_retry_forbidden",
        "profile_request_boundary_missing",
        "profile_source_capability_missing",
        "profile_provider_flags_missing",
        "profile_provider_flags_ambiguous",
        "profile_provenance_invalid",
        "profile_response_limit",
        "profile_identity_mismatch",
        "profile_identity_unresolved",
        "profile_projection_unknown_fields",
        "avatar_profile_incomplete",
        "avatar_identity_mismatch",
        "avatar_reference_invalid",
        "avatar_unavailable_proof_missing",
        "avatar_source_newer",
        "unauthorized",
        "forbidden",
        "login_required",
        "challenge_required",
        "checkpoint_required",
        "feedback_required",
        "sentry_block",
        "consent_required",
        "native_errors",
        "user_null",
        "user_missing",
        "data_missing",
        "non_object",
    }
)
FACT_PHASES = frozenset({"profile_bootstrap", "profile_query", "legacy_profile"})
SHAPE_BOOLEANS = frozenset(
    {
        "data_present",
        "data_null",
        "data_object",
        "user_present",
        "user_null",
        "user_object",
        "errors_present",
        "errors_list",
    }
)
_TOP = {"version", "kind", "username", "user_id", "relay_doc_id", "source", "phase", "code", "count", "basis"}
_FACT = {
    "phase",
    "observed_at",
    "status",
    "request_ordinal",
    "request_number",
    "failure_generation",
    "body_complete",
    "body_observed",
    "content_hash",
    "bytes",
    "native_codes",
    "shape",
    "code",
}


def utc_now():
    return datetime.now(UTC)


def _integer(value, maximum=2**63 - 1):
    return type(value) is int and 0 <= value <= maximum


def _exact_type(value: Any, expected: type) -> bool:
    """Reject subclasses; keeps the checked value untyped for the strict field reads below."""
    return type(value) is expected


def _digits(value):
    return value is None or isinstance(value, str) and re.fullmatch(r"[0-9]{1,64}", value) is not None


def validate_payload(payload: Any) -> dict[str, Any]:
    """Parser-specific strict projection: unknown fields/types fail closed."""

    def require(condition):
        if not condition:
            raise ValueError("Invalid bounded public profile failure evidence")

    require(_exact_type(payload, dict) and set(payload) == _TOP)
    require(type(payload["version"]) is int and payload["version"] == 1 and payload["kind"] == "public_profile_failure")
    require(isinstance(payload["username"], str) and re.fullmatch(r"[a-z0-9._]{1,30}", payload["username"]) is not None)
    require(_digits(payload["user_id"]) and _digits(payload["relay_doc_id"]))
    require(type(payload["source"]) is str and payload["source"] in {"public_graphql", "public_legacy"})
    require(
        type(payload["phase"]) is str
        and type(payload["code"]) is str
        and payload["phase"] in PHASES
        and payload["code"] in REASONS
        and _integer(payload["count"], 2)
    )
    require(_exact_type(payload["basis"], list) and len(payload["basis"]) <= 2)
    ordinals = []
    for fact in payload["basis"]:
        require(_exact_type(fact, dict) and set(fact) == _FACT)
        require(
            type(fact["phase"]) is str
            and type(fact["code"]) is str
            and fact["phase"] in FACT_PHASES
            and fact["code"] in REASONS
        )
        require(_integer(fact["request_ordinal"], 2) and fact["request_ordinal"] >= 1)
        ordinals.append(fact["request_ordinal"])
        require(all(v is None or _integer(v) for v in (fact["request_number"], fact["failure_generation"])))
        require(_integer(fact["status"], 599) and (fact["status"] == 0 or fact["status"] >= 100))
        require(type(fact["body_complete"]) is bool and type(fact["body_observed"]) is bool and _integer(fact["bytes"]))
        require(not fact["body_complete"] or fact["body_observed"])
        digest = fact["content_hash"]
        require(
            (fact["body_observed"] and isinstance(digest, str) and re.fullmatch(r"[a-f0-9]{64}", digest) is not None)
            or (not fact["body_observed"] and digest is None and fact["bytes"] == 0)
        )
        require(
            _exact_type(fact["native_codes"], list)
            and len(fact["native_codes"]) <= 8
            and all(_integer(code, 2**31 - 1) for code in fact["native_codes"])
        )
        shape = fact["shape"]
        require(_exact_type(shape, dict) and set(shape) == SHAPE_BOOLEANS | {"errors_count"})
        require(all(type(shape[key]) is bool for key in SHAPE_BOOLEANS) and _integer(shape["errors_count"], 33))
        try:
            stamp = datetime.fromisoformat(fact["observed_at"])
            require(stamp.utcoffset() is not None and stamp.astimezone(UTC).isoformat() == fact["observed_at"])
        except (TypeError, ValueError):
            raise ValueError("Invalid bounded public profile failure clock") from None
    require(ordinals == sorted(set(ordinals)) and all(ordinal <= payload["count"] for ordinal in ordinals))
    # Copy only after validating the complete fixed vocabulary; no unknown field
    # is silently dropped and no caller can mutate a sealed fact in place.
    return json.loads(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def shape_and_codes(payload):
    mapping = isinstance(payload, dict)
    data = payload.get("data") if mapping else None
    user = data.get("user") if isinstance(data, dict) else None
    errors = payload.get("errors") if mapping else None
    shape = {
        "data_present": mapping and "data" in payload,
        "data_null": mapping and "data" in payload and data is None,
        "data_object": isinstance(data, dict),
        "user_present": isinstance(data, dict) and "user" in data,
        "user_null": isinstance(data, dict) and "user" in data and user is None,
        "user_object": isinstance(user, dict),
        "errors_present": mapping and "errors" in payload,
        "errors_list": isinstance(errors, list),
        "errors_count": min(len(errors), 33) if isinstance(errors, list) else 0,
    }
    codes = []
    for node in ([payload] if mapping else []) + (errors[:32] if isinstance(errors, list) else []):
        if not isinstance(node, dict):
            continue
        for context in (node, node.get("extensions")):
            if not isinstance(context, dict):
                continue
            for key in ("code", "error_code", "error"):
                value = context.get(key)
                if isinstance(value, str) and re.fullmatch(r"[0-9]{1,10}", value):
                    value = int(value)
                if _integer(value, 2**31 - 1) and value not in codes and len(codes) < 8:
                    codes.append(value)
    reason = (
        "non_object"
        if not mapping
        else "rate_limited"
        if payload.get("rate_limited") is True
        else "login_required"
        if type(payload.get("error")) is int and payload["error"] == 1357001
        else "native_errors"
        if errors not in (None, [])
        else "user_null"
        if shape["user_null"]
        else "user_missing"
        if shape["data_object"] and not shape["user_present"]
        else "data_missing"
        if not shape["data_present"]
        else "unknown"
    )
    return shape, codes, reason


class ResponseFacts:
    def __init__(self):
        self.facts: list[dict[str, Any]] = []
        self.phase = "request"
        self.current: dict[str, Any] | None = None
        self.hasher: Any = None

    def begin(self, phase, ordinal, permit):
        self.phase = "request"
        self.current = None
        if len(self.facts) >= 2:
            return
        shape, _, _ = shape_and_codes(None)
        self.current = {
            "phase": phase,
            "observed_at": utc_now().isoformat(),
            "status": 0,
            "request_ordinal": ordinal,
            "request_number": getattr(permit, "request_number", None),
            "failure_generation": getattr(permit, "failure_generation", None),
            "body_complete": False,
            "body_observed": False,
            "content_hash": None,
            "bytes": 0,
            "native_codes": [],
            "shape": shape,
            "code": "unknown",
        }
        self.facts.append(self.current)
        self.hasher = hashlib.sha256()

    def headers(self, response):
        if self.current is not None:
            status = getattr(response, "status_code", 0)
            self.current.update(status=status if _integer(status, 599) else 0, observed_at=utc_now().isoformat())
        self.phase = "body"

    def chunk(self, chunk):
        if self.current is not None:
            self.hasher.update(chunk)
            self.current.update(
                body_observed=True, content_hash=self.hasher.hexdigest(), bytes=self.current["bytes"] + len(chunk)
            )

    def complete(self, payload=None):
        if self.current is not None:
            self.chunk(b"")
            self.current["body_complete"] = True
            self.enrich(payload)

    def enrich(self, payload):
        if self.current is not None:
            shape, codes, reason = shape_and_codes(payload)
            self.current.update(shape=shape, native_codes=codes, code=reason)

    def preloaded(self, response):
        self.headers(response)
        body = getattr(response, "_content", None)
        if isinstance(body, (bytes, bytearray)):
            self.chunk(body)
            try:
                payload = json.loads(body) if len(body) <= 2 * 1024 * 1024 else None
            except (TypeError, ValueError):
                payload = None
            self.complete(payload)
        self.phase = "response_classification"


def failure_payload(*, source, username, user_id, count, phase, error, payload):
    if getattr(source, "_public_identity", None) is None:
        return None
    collector = getattr(source, "response_facts", None)
    if not isinstance(collector, ResponseFacts) or not collector.facts:
        return None
    if payload is not None:
        collector.enrich(payload)
    reason = (
        getattr(error, "code", None)
        or getattr(getattr(error, "failure", None), "code", None)
        or getattr(error, "error_code", None)
    )
    reason = reason if isinstance(reason, str) and reason in REASONS else "unknown"
    return validate_payload(
        {
            "version": 1,
            "kind": "public_profile_failure",
            "username": username,
            "user_id": user_id,
            "relay_doc_id": "28649589871350527"
            if getattr(source, "parser_version", None) == "instagram-public-commenter-profile-graphql-v1"
            else None,
            "source": "public_graphql" if user_id is not None else "public_legacy",
            "phase": collector.phase if phase == "request" else phase,
            "code": reason,
            "count": count,
            "basis": collector.facts,
        }
    )
