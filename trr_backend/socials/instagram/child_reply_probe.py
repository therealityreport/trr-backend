"""One-pass child access comparison; never retries, paginates, or saves cookies.

Results are safe projections, not ingestion receipts. Callers own the exclusive
diagnostic stream and debit their ledger immediately before each request. The
Modal caller reserves all three possible sends before dispatch instead.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable, Mapping
from typing import Any

import httpx

from trr_backend.ingestion.comments import comment_page_failure
from trr_backend.socials.instagram.comments_scrapling.fetcher import (
    _decode_graphql_json_payload,
    _extract_logged_out_graphql_context,
)
from trr_backend.socials.instagram.permalink_metadata import _iter_data_sjs_payloads
from trr_backend.socials.instagram.public_response import parse_public_retry_after

CHILD_QUERIES = (
    ("PolarisPostChildCommentsQuery", "34884685271179117"),
    ("PolarisPostCommentsChildrenPaginationtQuery", "36239935742272683"),
)
CONNECTION = "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection"
ORIGIN = "https://www.instagram.com"
MAX_BODY_BYTES = 10 * 1024 * 1024
_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"


class ProbeStoppedError(Exception):
    """Only a stable safe code is exposed to callers."""


def conclusive_anonymous_failure(report: Mapping[str, Any], *, require_rest: bool = False) -> bool:
    """Transport/context failures are not evidence that anonymous access failed."""
    if report.get("authenticated") is not False or report.get("positive_reply_proof") is not False:
        return False
    events = report.get("events") or []
    children = [
        event for event in events if event.get("kind", "").startswith("graphql_") or event.get("kind") == "rest"
    ]
    if not children:
        return False
    stop = report.get("stop_reason")
    if stop in {"source_auth_failed", "http_rate_limited"}:
        last = children[-1]
        return last is events[-1] and (
            last.get("http_status") in {401, 403, 429} or last.get("failure") == "source_auth_failed"
        )
    if stop is not None or report.get("graphql_unavailable"):
        return False
    required = {"graphql_" + doc for _, doc in CHILD_QUERIES}
    if require_rest:
        required.add("rest")
    qualified = {
        event["kind"]
        for event in children
        if event.get("http_status") == 200
        and (event.get("connection_found") is True or bool(event.get("native_codes")))
    }
    return required <= qualified


def media_id_from_shortcode(shortcode: str) -> str:
    if not isinstance(shortcode, str) or not re.fullmatch(r"[A-Za-z0-9_-]{5,20}", shortcode):
        raise ValueError("Invalid Instagram shortcode")
    value = 0
    for char in shortcode:
        value = value * 64 + _ALPHABET.index(char)
    return str(value)


def _native_codes(payload: Mapping[str, Any]) -> list[str]:
    errors = payload.get("errors") or payload.get("error") or []
    errors = errors if isinstance(errors, list) else [errors]
    return sorted(
        {
            str(item["code"])
            for item in errors
            if isinstance(item, Mapping) and re.fullmatch(r"[0-9]{1,20}", str(item.get("code", "")))
        }
    )


def _safe_rows(rows: list[Any], parent: str) -> list[str]:
    identifiers = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        node = row.get("node", row)
        if not isinstance(node, Mapping):
            continue
        identity = str(node.get("pk") or node.get("id") or "")
        explicit_parent = node.get("parent_comment_id")
        if explicit_parent is not None and str(explicit_parent) != parent:
            raise ProbeStoppedError("reply_parent_mismatch")
        if re.fullmatch(r"[0-9]{1,40}", identity) and identity != parent:
            identifiers.append(identity)
    return sorted(set(identifiers))


def project_reply_page(payload: Mapping[str, Any], parent: str, *, rest: bool = False) -> dict[str, Any]:
    """Accept only the requested child connection, never unrelated parent rows."""
    if rest:
        rows = payload.get("child_comments", payload.get("replies"))
        present = isinstance(rows, list)
        more = payload.get("has_more_tail_child_comments")
        cursor = payload.get("next_min_child_cursor") or payload.get("next_max_child_cursor")
    else:
        data = payload.get("data")
        connection = data.get(CONNECTION) if isinstance(data, Mapping) else None
        present = isinstance(connection, Mapping) and isinstance(connection.get("edges"), list)
        rows = connection.get("edges") if present and isinstance(connection, Mapping) else None
        info = connection.get("page_info") if present and isinstance(connection, Mapping) else None
        info = info if isinstance(info, Mapping) else {}
        more, cursor = info.get("has_next_page"), info.get("end_cursor")
    identities = _safe_rows(rows or [], parent)
    return {
        "connection_found": present,
        "reply_count": len(identities),
        "reply_ids": identities,
        "has_next_page": more if type(more) is bool else None,
        "has_cursor": bool(cursor),
        "positive_reply_proof": bool(identities),
    }


def _embed_replies(html: str, parent: str) -> dict[str, Any]:
    identities: set[str] = set()
    stack: list[Any] = list(_iter_data_sjs_payloads(html))
    while stack:
        node = stack.pop()
        if isinstance(node, Mapping):
            if str(node.get("pk") or node.get("id") or "") == parent:
                connection = node.get("edge_threaded_comments")
                if isinstance(connection, Mapping) and isinstance(connection.get("edges"), list):
                    identities.update(_safe_rows(connection["edges"], parent))
            stack.extend(value for value in node.values() if isinstance(value, (dict, list)))
        elif isinstance(node, list):
            stack.extend(value for value in node if isinstance(value, (dict, list)))
    return {"reply_ids": sorted(identities), "reply_count": len(identities), "positive_reply_proof": bool(identities)}


def probe_child_reply_access(
    shortcode: str,
    parent_comment_id: str,
    *,
    client: httpx.Client | None = None,
    before_request: Callable[[str], None] | None = None,
    include_rest: bool = False,
    include_embed: bool = False,
    credential_snapshot=None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Bootstrap then try each selected surface once, anonymous unless pinned.

    A native child-only rejection permits the next method; HTTP rate limiting,
    challenges, authentication failure, or a server-supplied delay stop the arm.
    The optional authenticated arm uses only the supplied CredentialSnapshot.
    """
    media_id = media_id_from_shortcode(shortcode)
    if not isinstance(parent_comment_id, str) or not re.fullmatch(r"[0-9]{1,40}", parent_comment_id):
        raise ValueError("Invalid Instagram parent comment ID")
    authenticated = credential_snapshot is not None
    cookies = dict(credential_snapshot.source_kwargs()["raw_cookies"]) if authenticated else {}
    own_client = client is None
    if own_client:
        client = httpx.Client(
            transport=httpx.HTTPTransport(retries=0, trust_env=False),
            trust_env=False,
            follow_redirects=False,
            timeout=20,
        )
    assert client is not None
    if authenticated:
        for name, value in cookies.items():
            client.cookies.set(name, value, domain=".instagram.com", path="/")
    report: dict[str, Any] = {
        "schema": "instagram-child-reply-access-v1",
        "shortcode": shortcode,
        "media_id": media_id,
        "parent_comment_id": parent_comment_id,
        "authenticated": authenticated,
        "automatic_retries": 0,
        "requests": 0,
        "events": [],
        "stop_reason": None,
        "positive_reply_proof": False,
    }
    permalink = f"{ORIGIN}/p/{shortcode}/"
    base_headers = {"user-agent": "Mozilla/5.0", "accept": "*/*", "referer": permalink}
    if authenticated:
        base_headers["x-csrftoken"] = cookies["csrftoken"]

    def send(kind, method, url, *, data=None, headers=None):
        if not authenticated and any(c.name.lower() in {"sessionid", "ds_user_id"} for c in client.cookies.jar):
            raise ProbeStoppedError("unexpected_authenticated_cookie")
        if report["requests"]:
            sleep(1)
        request = client.build_request(method, url, data=data, headers={**base_headers, **(headers or {})})
        if not authenticated:
            names = {part.split("=", 1)[0].strip().lower() for part in request.headers.get("cookie", "").split(";")}
            if names.intersection({"sessionid", "ds_user_id"}) or "authorization" in request.headers:
                raise ProbeStoppedError("unexpected_authentication")
        if before_request:
            before_request(kind)
        report["requests"] += 1
        event: dict[str, Any] = {"kind": kind}
        report["events"].append(event)
        response = client.send(request, stream=True, follow_redirects=False)
        try:
            event["http_status"] = response.status_code
            content = bytearray()
            for chunk in response.iter_bytes():
                content.extend(chunk)
                if len(content) > MAX_BODY_BYTES:
                    raise ProbeStoppedError("response_size_limit")
            raw = bytes(content)
            event.update(body_bytes=len(raw), body_sha256=hashlib.sha256(raw).hexdigest())
            delay = parse_public_retry_after(response.headers.get("retry-after"))
            buffered = httpx.Response(response.status_code, content=raw, request=request)
            payload = _decode_graphql_json_payload(buffered) if kind not in {"bootstrap", "embed"} else None
            if isinstance(payload, Mapping):
                event["native_codes"] = _native_codes(payload)
                failure = comment_page_failure(payload)
                if failure:
                    event["failure"] = failure[0]
                    if failure[1] is not None:
                        delay = max(delay or 0, failure[1])
                    if failure[0] == "source_auth_failed":
                        raise ProbeStoppedError("source_auth_failed")
            if delay is not None:
                event["retry_after_seconds"] = delay
                raise ProbeStoppedError("server_retry_after")
            if response.status_code == 429:
                raise ProbeStoppedError("http_rate_limited")
            if response.status_code in {401, 403}:
                raise ProbeStoppedError("source_auth_failed")
            if 300 <= response.status_code < 400:
                raise ProbeStoppedError("endpoint_redirect")
            if response.status_code >= 400:
                event["failure"] = "http_error"
            if not authenticated and any(c.name.lower() in {"sessionid", "ds_user_id"} for c in client.cookies.jar):
                raise ProbeStoppedError("unexpected_authenticated_cookie")
            return raw, payload, event
        finally:
            response.close()

    try:
        raw, _, event = send("bootstrap", "GET", permalink)
        if event["http_status"] != 200:
            raise ProbeStoppedError("bootstrap_failed")
        context = _extract_logged_out_graphql_context(raw.decode("utf-8", errors="replace"), media_id=media_id)
        lsd = context.get("lsd")
        event["fresh_context"] = bool(lsd)
        if lsd:
            queries = CHILD_QUERIES[:1] if authenticated else CHILD_QUERIES
            for name, doc in queries:
                variables = {
                    "__relay_internal__pv__PolarisIsLoggedInrelayprovider": authenticated,
                    "first": 12,
                    "is_chronological": False,
                    "media_id": media_id,
                    "parent_comment_id": parent_comment_id,
                }
                form = {
                    "av": credential_snapshot.user_id if authenticated else "0",
                    "__user": credential_snapshot.user_id if authenticated else "0",
                    "__a": "1",
                    "__comet_req": "7",
                    "__d": "www",
                    "__req": "1",
                    "fb_api_caller_class": "RelayModern",
                    "fb_api_req_friendly_name": name,
                    "jazoest": context.get("jazoest") or "2" + str(sum(map(ord, lsd))),
                    "lsd": lsd,
                    "server_timestamps": "true",
                    "doc_id": doc,
                    "variables": json.dumps(variables, separators=(",", ":")),
                }
                for key, value in (context.get("spin") or {}).items():
                    if key in {"__spin_r", "__spin_b", "__spin_t"}:
                        form[key] = value
                headers = {
                    "content-type": "application/x-www-form-urlencoded",
                    "x-asbd-id": "359341",
                    "x-fb-friendly-name": name,
                    "x-fb-lsd": lsd,
                    "x-ig-app-id": "936619743392459",
                    "x-requested-with": "XMLHttpRequest",
                }
                _, payload, event = send(
                    "graphql_" + doc, "POST", ORIGIN + "/graphql/query/", data=form, headers=headers
                )
                if isinstance(payload, Mapping) and not event.get("failure"):
                    event.update(project_reply_page(payload, parent_comment_id))
        else:
            report["graphql_unavailable"] = "bootstrap_context_missing"
        if include_rest:
            _, payload, event = send(
                "rest",
                "GET",
                f"{ORIGIN}/api/v1/media/{media_id}/comments/{parent_comment_id}/child_comments/",
                headers={"x-ig-app-id": "936619743392459", "x-requested-with": "XMLHttpRequest"},
            )
            if isinstance(payload, Mapping) and not event.get("failure"):
                event.update(project_reply_page(payload, parent_comment_id, rest=True))
        if include_embed:
            raw, _, event = send("embed", "GET", permalink + "embed/captioned/")
            if event["http_status"] == 200:
                event.update(_embed_replies(raw.decode("utf-8", errors="replace"), parent_comment_id))
    except ProbeStoppedError as exc:
        report["stop_reason"] = str(exc)
    except httpx.HTTPError:
        # Network exception messages can contain proxy credentials or cookies.
        report["stop_reason"] = "network_error"
    finally:
        report["positive_reply_proof"] = any(event.get("positive_reply_proof") for event in report["events"])
        if own_client:
            client.close()
    return report
