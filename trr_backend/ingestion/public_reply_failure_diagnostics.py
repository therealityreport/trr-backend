"""Bounded failure facts for native public replies; no source text or authority."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any

DIAGNOSTICS_KEY = "public_reply_failure_diagnostics"
VERSION = 1
MAX_ERRORS = 32
MAX_CODE = 2**31 - 1
BODY_HASH_BASIS = "buffered_decoded_response_body"
FAILURE_CLASSES = frozenset({"source_incomplete", "source_auth_failed", "rate_limited", "reply_access_rejected"})
_SEVERITIES = frozenset({"ERROR", "WARNING", "INFO", "CRITICAL", "OTHER"})
_CODES = ("code", "error_code", "error_subcode")


def _numeric(value: Any) -> int | None:
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,10}", value):
        value = int(value)
    return value if type(value) is int and 0 <= value <= MAX_CODE else None


def _context(value: Any, *, severity: bool = False) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        value = {}
    result: dict[str, Any] = {key: number for key in _CODES if (number := _numeric(value.get(key))) is not None}
    extension = value.get("extensions")
    if isinstance(extension, Mapping):
        safe = {key: number for key in _CODES if (number := _numeric(extension.get(key))) is not None}
        if safe:
            result["extensions"] = safe
    if severity or "severity" in value:
        level = value.get("severity")
        result["severity"] = level if isinstance(level, str) and level in _SEVERITIES else "OTHER"
    return result


def project_diagnostics(value: Any) -> dict[str, Any] | None:
    """Rebuild this exact typed schema; arbitrary text and unknown keys vanish."""
    if not isinstance(value, Mapping) or type(value.get("version")) is not int or value["version"] != VERSION:
        return None
    status, failure, complete = value.get("http_status"), value.get("failure_class"), value.get("body_complete")
    if (
        type(status) is not int
        or not 100 <= status <= 599
        or not isinstance(failure, str)
        or failure not in FAILURE_CLASSES
        or type(complete) is not bool
    ):
        return None
    body_hash = value.get("body_sha256")
    if complete and value.get("body_hash_basis") != BODY_HASH_BASIS:
        return None
    if complete and (not isinstance(body_hash, str) or re.fullmatch(r"[0-9a-f]{64}", body_hash) is None):
        return None
    errors = value.get("errors")
    if not isinstance(errors, list):
        return None
    return {
        "version": VERSION,
        "failure_class": failure,
        "http_status": status,
        "body_complete": complete,
        "body_sha256": body_hash if complete else None,
        "body_hash_basis": BODY_HASH_BASIS if complete else None,
        "envelope": _context(value.get("envelope")),
        "errors": [_context(error, severity=True) for error in errors[:MAX_ERRORS]],
        "errors_truncated": len(errors) > MAX_ERRORS or value.get("errors_truncated") is True,
    }


def capture_diagnostics(payload: Mapping[str, Any], response: Any, *, failure_class: str) -> dict[str, Any]:
    """Hash the complete decoded parser-input bytes, never reserialized JSON.

    The existing HTTP spool removes gzip/deflate encoding before parsing. Its
    bytes retain the original JSON whitespace and anti-JSON prefix.
    """
    spool = getattr(response, "spool", None)
    complete = getattr(response, "body_complete", False) is True and callable(getattr(spool, "iter_chunks", None))
    body_hash = None
    if complete and spool is not None:
        digest = hashlib.sha256()
        for chunk in spool.iter_chunks():
            digest.update(chunk)
        body_hash = digest.hexdigest()
    errors = payload.get("errors")
    value = {
        "version": VERSION,
        "failure_class": failure_class,
        "http_status": response.status_code,
        "body_complete": complete,
        "body_sha256": body_hash,
        "body_hash_basis": BODY_HASH_BASIS if complete else None,
        "envelope": _context(payload),
        "errors": [_context(error, severity=True) for error in errors[:MAX_ERRORS]] if isinstance(errors, list) else [],
        "errors_truncated": isinstance(errors, list) and len(errors) > MAX_ERRORS,
    }
    projected = project_diagnostics(value)
    if projected is None:
        raise ValueError("Invalid public reply failure diagnostics")
    return projected


def project_payload_diagnostics(payload: Mapping[str, Any], *, provenance: Any) -> dict[str, Any] | None:
    """Retain facts only beside a failed public child envelope, never page proof."""
    if DIAGNOSTICS_KEY not in payload:
        return None
    query = payload.get("cursor_payload")
    if (
        not isinstance(payload.get("parent_comment_id"), str)
        or not payload["parent_comment_id"]
        or not isinstance(query, Mapping)
        or query.get("adapter_family") != "instagram_public_relay"
        or type(query.get("adapter_version")) is not int
        or query["adapter_version"] != 1
    ):
        return None
    from trr_backend.ingestion.comments import comment_page_failure

    failed_proof = isinstance(provenance, Mapping) and provenance.get("classification") == "rejected"
    if not comment_page_failure(payload) and not failed_proof:
        return None
    return project_diagnostics(payload[DIAGNOSTICS_KEY])
