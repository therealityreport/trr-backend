"""Pure, bounded classification of native Instagram listing responses.

The sole tolerated error describes an unused location profile image. Its path
and source node are verified again when parsing projected evidence. Error text,
stack traces and account/auth values never enter retained provenance.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

GRAPHQL_RESPONSE_POLICY_VERSION = "instagram-graphql-ancillary-v1"
ROOT_FIELD = "xdt_api__v1__feed__user_timeline_graphql_connection"
PROVENANCE_KEY = "graphql_provenance"
MAX_ERROR_ITEMS = 32
_MAX_INDEX = 2**31 - 1
_ANCILLARY = "ancillary_location_profile_pic"
_REJECTED = "rejected"
_AUTH_CODES = frozenset(
    {
        "checkpoint_required",
        "challenge_required",
        "feedback_required",
        "login_required",
        "consent_required",
        "sentry_block",
        "unauthorized",
        "forbidden",
        "redirect_login",
        "permission_denied",
    }
)


@dataclass(frozen=True)
class GraphQLResponseDecision:
    accepted: bool
    reason: str | None = None
    auth_failed: bool = False


def _connection(payload: Mapping[str, Any]) -> Any:
    data = payload.get("data")
    return data.get(ROOT_FIELD) if isinstance(data, dict) else None


def valid_posts_connection(connection: Any, *, reverse: bool = False) -> bool:
    if not isinstance(connection, dict):
        return False
    edges, info = connection.get("edges"), connection.get("page_info")
    if not isinstance(edges, list) or not isinstance(info, dict):
        return False
    if any(not isinstance(edge, dict) or not isinstance(edge.get("node"), dict) or not edge["node"] for edge in edges):
        return False
    flag = "has_previous_page" if reverse else "has_next_page"
    cursor = info.get("start_cursor" if reverse else "end_cursor")
    return (
        type(info.get(flag)) is bool
        and (cursor is None or isinstance(cursor, str) and bool(cursor.strip()))
        and (not info[flag] or cursor is not None)
    )


def _primitive_failure(payload: Mapping[str, Any]) -> GraphQLResponseDecision | None:
    """Explicit failure fields shared by the envelope and each error context."""
    if type(payload.get("error")) is int and payload["error"] == 1357001:
        return GraphQLResponseDecision(False, "login_required", True)
    status = payload.get("status")
    auth_code = (
        next(
            (
                payload[key]
                for key in ("message", "error_type", "error_message")
                if isinstance(payload.get(key), str) and payload[key] in _AUTH_CODES
            ),
            None,
        )
        if status == "fail"
        else None
    )
    if "error" in payload and payload["error"] is not None:
        return GraphQLResponseDecision(False, auth_code or "graphql_error", auth_code is not None)
    # Preserve native status/auth semantics. An unknown status cannot grant an
    # authentication verdict merely because a message contains an auth word.
    if status is not None and status != "ok":
        return GraphQLResponseDecision(
            False, status if status in ("fail", "error") else "api_status_fail", auth_code is not None
        )
    malformed_marker = False
    for key in ("auth_failed", "login_required", "checkpoint_required", "challenge_required", "permission_denied"):
        if key not in payload:
            continue
        if payload[key] is True:
            return GraphQLResponseDecision(False, "source_auth_failed", True)
        if payload[key] is not False:
            malformed_marker = True
    if "rate_limited" in payload:
        if payload["rate_limited"] is True:
            return GraphQLResponseDecision(False, "rate_limited")
        if payload["rate_limited"] is not False:
            malformed_marker = True
    if malformed_marker:
        return GraphQLResponseDecision(False, "graphql_error")
    if payload.get("failed") or payload.get("success") is False:
        return GraphQLResponseDecision(False, "graphql_error")
    for key in ("status_code", "http_status"):
        if type(payload.get(key)) is int and payload[key] in (401, 403):
            return GraphQLResponseDecision(False, "source_auth_failed", True)
        if type(payload.get(key)) is int and payload[key] == 429:
            return GraphQLResponseDecision(False, "rate_limited")
    return None


def _top_failure(payload: Mapping[str, Any]) -> GraphQLResponseDecision | None:
    primitive = _primitive_failure(payload)
    if primitive is not None:
        return primitive
    marker = _error_failure(
        {
            key: payload[key]
            for key in (
                "message",
                "error_type",
                "error_message",
                "error_code",
            )
            if key in payload
        }
    )
    if marker is not None:
        return GraphQLResponseDecision(False, "graphql_error") if marker.auth_failed else marker
    return None


def _error_failure(error: Any) -> GraphQLResponseDecision | None:
    if not isinstance(error, dict):
        return None
    extension = error.get("extensions")
    contexts = (error, extension) if isinstance(extension, dict) else (error,)
    ambiguous, rate = False, False
    for context in contexts:
        primitive = _primitive_failure(context)
        if primitive is not None:
            return primitive
        for key in ("message", "description", "error_type", "error_message", "code", "error_code"):
            value = context.get(key)
            if type(value) is int and value == 1357001:
                return GraphQLResponseDecision(False, "source_auth_failed", True)
            if not isinstance(value, str):
                continue
            if len(value) > 2048:
                ambiguous = True
                continue
            value = " ".join(value.lower().split())
            if value in _AUTH_CODES or (key in ("code", "error_type", "error_code") and value == "oauthexception"):
                return GraphQLResponseDecision(False, "source_auth_failed", True)
            # Ambiguous text can prevent accepting an ancillary field error,
            # but it cannot retire an identity. Public captions are not read.
            if (
                "session" in value and any(part in value for part in ("expir", "invalid", "required", "denied"))
            ) or any(
                marker in value
                for marker in (
                    "login",
                    "log in",
                    "logged out",
                    "checkpoint",
                    "challenge",
                    "unauthorized",
                    "forbidden",
                    "permission",
                    "authentication",
                    "unauthenticated",
                    "oauth",
                    "access denied",
                    "consent_required",
                    "feedback_required",
                    "sentry_block",
                )
            ):
                ambiguous = True
            rate_text = value.replace("_", " ").replace("-", " ")
            if any(marker in rate_text for marker in ("rate limit", "too many requests", "throttl", "please wait")):
                rate = True
    if rate:
        return GraphQLResponseDecision(False, "rate_limited")
    return GraphQLResponseDecision(False, "graphql_error") if ambiguous else None


def _safe_error(error: Any) -> dict[str, Any]:
    path = error.get("path") if isinstance(error, dict) else None
    safe_path: list[Any] | None = (
        path
        if (
            isinstance(path, list)
            and len(path) == 6
            and path[:2] == [ROOT_FIELD, "edges"]
            and type(path[2]) is int
            and 0 <= path[2] <= _MAX_INDEX
            and path[3:] == ["node", "location", "profile_pic_url"]
        )
        else None
    )
    severity = error.get("severity") if isinstance(error, dict) else None
    severity = severity if severity in ("ERROR", "WARNING", "INFO", "CRITICAL") else "OTHER"
    return {
        "path": list(safe_path) if safe_path is not None else [],
        "index": safe_path[2] if safe_path is not None else None,
        "severity": severity,
        "classification": _ANCILLARY
        if safe_path is not None and severity == "ERROR" and _error_failure(error) is None
        else _REJECTED,
    }


def _proof(error_count: int, errors: list[dict[str, Any]], *, rejected: bool) -> dict[str, Any]:
    return {
        "policy_version": GRAPHQL_RESPONSE_POLICY_VERSION,
        "classification": _REJECTED if rejected else _ANCILLARY,
        "error_count": error_count,
        "errors": errors,
    }


def _canonical_proof(value: Any) -> dict[str, Any]:
    """Rebuild only typed safe provenance; unknown/future projections reject."""
    rejected = _proof(1, [], rejected=True)
    if not isinstance(value, dict) or set(value) != {"policy_version", "classification", "error_count", "errors"}:
        return rejected
    count, errors = value["error_count"], value["errors"]
    if (
        value["policy_version"] != GRAPHQL_RESPONSE_POLICY_VERSION
        or value["classification"] not in (_ANCILLARY, _REJECTED)
        or type(count) is not int
        or not 0 <= count <= _MAX_INDEX
        or not isinstance(errors, list)
        or len(errors) > MAX_ERROR_ITEMS
    ):
        return rejected
    safe = []
    for error in errors:
        if not isinstance(error, dict) or set(error) != {"path", "index", "severity", "classification"}:
            return rejected
        if error["index"] is not None and type(error["index"]) is not int:
            return rejected
        item = _safe_error(error)
        if error["classification"] == _REJECTED:
            item["classification"] = _REJECTED
        if item != error:
            return rejected
        safe.append(item)
    return _proof(
        count,
        safe,
        rejected=(
            value["classification"] == _REJECTED
            or count != len(safe)
            or not safe
            or any(item["classification"] != _ANCILLARY for item in safe)
        ),
    )


def _indices_valid(errors: list[dict[str, Any]], connection: Any) -> bool:
    if not isinstance(connection, dict) or not isinstance(connection.get("edges"), list):
        return False
    edges = connection["edges"]
    for error in errors:
        index = error["index"]
        if error["classification"] != _ANCILLARY or type(index) is not int or not 0 <= index < len(edges):
            return False
        edge = edges[index]
        if not isinstance(edge, dict) or not isinstance(edge.get("node"), dict) or not edge["node"]:
            return False
    return True


def project_graphql_provenance(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    """Project raw rejection/error state without granting an acceptance flag.

    Raw errors always take precedence over externally supplied provenance. An
    already-rejected projection stays rejected on every subsequent projection.
    Pagination direction is revalidated by the consumer, not stored as authority.
    """
    previous = _canonical_proof(payload[PROVENANCE_KEY]) if PROVENANCE_KEY in payload else None
    raw_errors = payload.get("errors")
    has_errors = "errors" in payload and not (isinstance(raw_errors, list) and not raw_errors)
    failure = _top_failure(payload)
    # A retained status already preserves its rejection; don't change old
    # evidence hashes merely by adding redundant policy metadata.
    dropped_failure = failure is not None and (
        "error" in payload
        or any(
            key in payload
            for key in (
                "message",
                "error_type",
                "error_message",
                "error_code",
                "status_code",
                "http_status",
                "auth_failed",
                "login_required",
                "checkpoint_required",
                "challenge_required",
                "permission_denied",
                "rate_limited",
                "failed",
                "success",
            )
        )
    )
    if not has_errors and not dropped_failure:
        return previous
    count = min(len(raw_errors), _MAX_INDEX) if isinstance(raw_errors, list) else int(has_errors)
    safe = [_safe_error(item) for item in raw_errors[:MAX_ERROR_ITEMS]] if isinstance(raw_errors, list) else []
    connection = _connection(payload)
    structurally_valid = valid_posts_connection(connection) or valid_posts_connection(connection, reverse=True)
    return _proof(
        count,
        safe,
        rejected=(
            failure is not None
            or not isinstance(raw_errors, list)
            or count > MAX_ERROR_ITEMS
            or not safe
            or not structurally_valid
            or not _indices_valid(safe, connection)
            or previous is not None
            and previous["classification"] == _REJECTED
        ),
    )


def classify_graphql_response(payload: Any, *, reverse: bool = False) -> GraphQLResponseDecision:
    if not isinstance(payload, dict):
        return GraphQLResponseDecision(False, "graphql_invalid_connection")
    failure = _top_failure(payload)
    if failure is not None:
        return failure
    errors = payload.get("errors")
    if isinstance(errors, list):
        for error in errors[:MAX_ERROR_ITEMS]:
            failure = _error_failure(error)
            if failure is not None:
                return failure
    proof = project_graphql_provenance(payload)
    if proof is not None and proof["classification"] == _REJECTED:
        return GraphQLResponseDecision(False, "graphql_error")
    connection = _connection(payload)
    if connection is None or connection == {}:
        return GraphQLResponseDecision(False, "graphql_empty_connection")
    if not valid_posts_connection(connection, reverse=reverse):
        return GraphQLResponseDecision(False, "graphql_invalid_connection")
    if proof is not None and not _indices_valid(proof["errors"], connection):
        return GraphQLResponseDecision(False, "graphql_error")
    return GraphQLResponseDecision(True)
