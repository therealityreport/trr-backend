"""Bounded, diagnostic-only REST retrieval using one explicitly pinned identity.

No ingestion writes, credential discovery, proxies, redirects, or retries. Only
allowlisted facts leave this module; raw bodies and exception messages do not.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping
from typing import Any

import httpx

from trr_backend.ingestion.comments import comment_page_failure
from trr_backend.socials.instagram.child_reply_probe import (
    MAX_BODY_BYTES,
    ORIGIN,
    ProbeStoppedError,
    _native_codes,
    media_id_from_shortcode,
)
from trr_backend.socials.instagram.public_response import parse_public_retry_after

MAX_REQUESTS = 12
SUBJECTS = (
    ("DesoVBhD027", {"18281777497294705": None}),
    (
        "Dd6m4XKDSQ1",
        {
            "18121868419915733": None,
            "18134186251755697": None,
            "18071023400572902": None,
            "18007584374778421": "18121868419915733",
            "18179727067429364": "18121868419915733",
        },
    ),
)


def empty_report() -> dict[str, Any]:
    return {
        "schema": "instagram-authenticated-comment-probe-v1",
        "auth_state": "unverified",
        "username": None,
        "cookies_exported": False,
        "automatic_retries": 0,
        "requests": 0,
        "max_requests": MAX_REQUESTS,
        "events": [],
        "stop_reason": None,
        "verdict": "inconclusive",
        "subjects": [
            {
                "shortcode": shortcode,
                "found_targets": [],
                "missing_targets": sorted(targets),
                "target_metadata": {},
                "parent_count": 0,
                "reply_count": 0,
                "reported_count": None,
            }
            for shortcode, targets in SUBJECTS
        ],
    }


def _identity(value: Any) -> str:
    value = str(value) if type(value) in {int, str} else ""
    if not re.fullmatch(r"[1-9][0-9]{0,39}", value):
        raise ProbeStoppedError("malformed_comment_identity")
    return value


def _next_cursor(payload: Mapping[str, Any], *, child: bool) -> dict[str, str] | None:
    flags = (
        ("has_more_tail_child_comments", "has_more_head_child_comments")
        if child
        else ("has_more_comments", "has_more_headload_comments")
    )
    if any(key in payload and type(payload[key]) is not bool for key in flags):
        raise ProbeStoppedError("malformed_pagination")
    more = any(payload.get(key) is True for key in flags)
    candidates = (
        (("next_min_child_cursor", "min_id"), ("next_max_child_cursor", "max_id"))
        if child
        else (("next_min_id", "min_id"), ("next_max_id", "max_id"))
        if payload.get("has_more_headload_comments")
        else (("next_max_id", "max_id"), ("next_min_id", "min_id"))
    )
    # REST may supply a cursor without its optional more flag. Follow that native
    # cursor, but never synthesize one from count gaps or switch to another API.
    for source, parameter in candidates:
        value = payload.get(source)
        if value is None or value == "":
            continue
        if not isinstance(value, str) or len(value) > 4096:
            raise ProbeStoppedError("malformed_pagination")
        return {parameter: value}
    if more or not any(key in payload for key in flags):
        raise ProbeStoppedError("malformed_pagination")
    return None


def _scan_subject(send, result, shortcode, targets):
    media = media_id_from_shortcode(shortcode)
    parents, replies = set(), set()

    def collect(row, parent=None):
        if not isinstance(row, dict):
            raise ProbeStoppedError("malformed_comment")
        comment_id = _identity(row.get("pk", row.get("id")))
        explicit = row.get("parent_comment_id")
        if explicit is not None and str(explicit) != str(parent):
            raise ProbeStoppedError("reply_parent_mismatch")
        (parents if parent is None else replies).add(comment_id)
        result.update(parent_count=len(parents), reply_count=len(replies))
        if comment_id in targets:
            if targets[comment_id] != parent:
                raise ProbeStoppedError("reply_parent_mismatch")
            user = row.get("user") if isinstance(row.get("user"), dict) else {}
            result["target_metadata"][comment_id] = {
                "parent_id": parent,
                "is_private": user.get("is_private") if type(user.get("is_private")) is bool else None,
                "is_hidden": row.get("is_hidden") if type(row.get("is_hidden")) is bool else None,
            }
            result["found_targets"] = sorted(result["target_metadata"])
            result["missing_targets"] = sorted(set(targets) - set(result["found_targets"]))

    def scan(parent=None):
        required = {key for key, value in targets.items() if value == parent}
        path = f"/api/v1/media/{media}/comments/"
        if parent:
            path += f"{parent}/child_comments/"
        params, seen = {}, set()
        while required - set(result["found_targets"]):
            payload = send("children" if parent else "parents", path, params)
            rows = payload.get("child_comments" if parent else "comments")
            if not isinstance(rows, list):
                raise ProbeStoppedError("missing_comment_connection")
            if not parent and type(payload.get("comment_count")) is int and payload["comment_count"] >= 0:
                result["reported_count"] = payload["comment_count"]
            for row in rows:
                collect(row, parent)
            if not required - set(result["found_targets"]):
                return
            params = _next_cursor(payload, child=parent is not None)
            if params is None:
                return
            cursor = tuple(params.items())
            if cursor in seen:
                raise ProbeStoppedError("repeated_cursor")
            seen.add(cursor)

    scan()
    for parent in sorted({value for value in targets.values() if value is not None}):
        scan(parent)


def probe_authenticated_comment_ids(
    snapshot,
    *,
    max_requests: int = MAX_REQUESTS,
    client: httpx.Client | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    if type(max_requests) is not int or not 1 <= max_requests <= MAX_REQUESTS:
        raise ValueError("Request cap must be between 1 and 12")
    report = empty_report()
    report["max_requests"] = max_requests
    cookies = dict(snapshot.source_kwargs()["raw_cookies"])
    own_client = client is None
    if own_client:
        client = httpx.Client(
            transport=httpx.HTTPTransport(retries=0, trust_env=False),
            trust_env=False,
            follow_redirects=False,
            timeout=20,
        )
    assert client is not None
    for name, value in cookies.items():
        client.cookies.set(name, value, domain=".instagram.com", path="/")

    def send(kind: str, path: str, params=None):
        if report["requests"] >= max_requests:
            raise ProbeStoppedError("request_limit")
        if report["requests"]:
            sleep(1)
        request = client.build_request(
            "GET",
            ORIGIN + path,
            params=params,
            headers={
                "user-agent": "Mozilla/5.0",
                "accept": "application/json",
                "x-ig-app-id": "936619743392459",
                "x-csrftoken": cookies["csrftoken"],
                "x-requested-with": "XMLHttpRequest",
            },
        )
        report["requests"] += 1
        event = {"kind": kind}
        report["events"].append(event)
        response = client.send(request, stream=True, follow_redirects=False)
        try:
            event["http_status"] = response.status_code
            if response.status_code == 429:
                raise ProbeStoppedError("http_rate_limited")
            if response.status_code in {401, 403}:
                raise ProbeStoppedError("source_auth_failed")
            if 300 <= response.status_code < 400:
                raise ProbeStoppedError("endpoint_redirect")
            if parse_public_retry_after(response.headers.get("retry-after")) is not None:
                raise ProbeStoppedError("server_retry_after")
            if response.status_code != 200:
                raise ProbeStoppedError("http_error")
            content = bytearray()
            for chunk in response.iter_bytes():
                content.extend(chunk)
                if len(content) > MAX_BODY_BYTES:
                    raise ProbeStoppedError("response_size_limit")
            try:
                payload = httpx.Response(200, content=bytes(content)).json()
            except ValueError:
                raise ProbeStoppedError("invalid_json") from None
            if not isinstance(payload, dict):
                raise ProbeStoppedError("invalid_payload")
            event["native_codes"] = _native_codes(payload)
            if payload.get("challenge") or payload.get("checkpoint_url"):
                raise ProbeStoppedError("source_auth_failed")
            failure = comment_page_failure(payload)
            if failure:
                code = "source_auth_failed" if failure[0] == "source_auth_failed" else "source_failure"
                raise ProbeStoppedError(code)
            if payload.get("status") != "ok":
                raise ProbeStoppedError("source_failure")
            return payload
        finally:
            response.close()

    try:
        identity = send("identity", "/api/v1/accounts/current_user/", {"edit": "true"}).get("user")
        if not isinstance(identity, dict) or (
            identity.get("username") != "livestnation"
            or identity.get("username") != snapshot.username
            or str(identity.get("pk")) != snapshot.user_id
        ):
            report["auth_state"] = "identity_mismatch"
            raise ProbeStoppedError("identity_mismatch")
        report.update(auth_state="authenticated", username="livestnation")
        for result, (shortcode, targets) in zip(report["subjects"], SUBJECTS, strict=True):
            _scan_subject(send, result, shortcode, targets)
        if all(not subject["missing_targets"] for subject in report["subjects"]):
            report["verdict"] = "passed"
        else:
            report["stop_reason"] = "targets_not_observed"
    except ProbeStoppedError as exc:
        report["stop_reason"] = str(exc)
        if str(exc) == "source_auth_failed":
            report["auth_state"] = "rejected"
    except httpx.HTTPError:
        report["stop_reason"] = "network_error"
    except Exception:
        report["stop_reason"] = "internal_error"
    finally:
        if own_client:
            client.close()
    return report
