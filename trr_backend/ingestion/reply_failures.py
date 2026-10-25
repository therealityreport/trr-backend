"""Technical child-request state, separate from immutable native page proof."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, TypedDict

from trr_backend.ingestion.domain import fingerprint
from trr_backend.ingestion.evidence import Evidence


class PublicReplyFailure(TypedDict):
    version: Literal[1]
    code: Literal["source_incomplete"]
    parent_comment_id: str
    evidence_id: str
    content_hash: str
    request: dict[str, Any]
    request_fingerprint: str
    first_observed_at: str
    observed_at: str
    owner_generation: int
    failure_count: int
    fingerprint_counts: dict[str, int]
    next_attempt_at: str
    retry_source: Literal["technical_branch_backoff"]
    technical_hold: bool


class RetainedPublicReplyFailureError(ValueError):
    """An exact generic public child failure was retained and fenced durably."""

    def __init__(self, parent_id: str):
        super().__init__("Retained public reply branch technical failure")
        self.parent_id = parent_id


def isolated_reply_failure(kind: str, payload: Mapping[str, Any]) -> bool:
    from trr_backend.ingestion.comments import comment_page_failure, native_child_rejection
    from trr_backend.socials.instagram.public_response import parse_public_retry_after

    query = payload.get("cursor_payload") or {}
    provenance = payload.get("graphql_provenance") or {}
    return bool(
        kind == "graphql_replies"
        and isinstance(payload.get("parent_comment_id"), str)
        and payload["parent_comment_id"]
        and isinstance(query, Mapping)
        and query.get("adapter_family") == "instagram_public_relay"
        and query.get("adapter_version") == 1
        and all(
            isinstance(query.get(key), str) and query[key]
            for key in ("relay_doc_id", "relay_friendly_name", "sort_order")
        )
        and type(query.get("page_size")) is int
        and 1 <= query["page_size"] <= 50
        and "request_cursor" in query
        and (query["request_cursor"] is None or isinstance(query["request_cursor"], str))
        and comment_page_failure(payload) == ("source_incomplete", None)
        and not native_child_rejection(payload)
        and parse_public_retry_after(payload.get("retry_after")) is None
        and (
            payload.get("errors")
            or (
                isinstance(provenance, Mapping)
                and provenance.get("classification") == "rejected"
                and provenance.get("error_count", 0) > 0
            )
        )
    )


def record_failure(
    previous: PublicReplyFailure | None,
    *,
    payload: Mapping[str, Any],
    evidence: Evidence,
    evidence_id: str,
    owner_generation: int,
    now: datetime,
) -> PublicReplyFailure:
    query = dict(payload["cursor_payload"])
    request_fingerprint = fingerprint(
        {"query": query, "parent": payload["parent_comment_id"], "content_hash": evidence.content_hash}
    )
    previous = previous or {}
    counts = dict(previous.get("fingerprint_counts") or {})
    counts[request_fingerprint] = counts.get(request_fingerprint, 0) + 1
    count = max(counts.values())
    return {
        "version": 1,
        "code": "source_incomplete",
        "parent_comment_id": payload["parent_comment_id"],
        "evidence_id": evidence_id,
        "content_hash": evidence.content_hash,
        "request": query,
        "request_fingerprint": request_fingerprint,
        "first_observed_at": previous.get("first_observed_at") or evidence.captured_at.isoformat(),
        "observed_at": evidence.captured_at.isoformat(),
        "owner_generation": owner_generation,
        "failure_count": count,
        "fingerprint_counts": counts,
        "next_attempt_at": (now + timedelta(seconds=min(60 * 2 ** min(count - 1, 4), 900))).isoformat(),
        "retry_source": "technical_branch_backoff",
        "technical_hold": count >= 3,
    }


def resume_failure(value: Any, *, now: datetime) -> dict[str, Any]:
    if not isinstance(value, Mapping) or value.get("version") != 1 or value.get("code") != "source_incomplete":
        raise ValueError("Invalid public reply technical failure metadata")
    try:
        retry = datetime.fromisoformat(value["next_attempt_at"].replace("Z", "+00:00"))
        deferred = value.get("technical_hold") is True or retry > now.astimezone(UTC)
    except (KeyError, TypeError, ValueError):
        raise ValueError("Invalid public reply technical retry timestamp") from None
    return {"technical_failure": True, "deferred": deferred, "pending": not deferred}
