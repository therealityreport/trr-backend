"""Explicit-session Instagram WWW Relay pages, without transport fallback.

The WWW comment operations use the document's Relay actor, not the numeric
Instagram cookie identity. Runtime tokens never enter retained page evidence.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

from trr_backend.ingestion.comments import CommentSourceError, comment_page_failure
from trr_backend.ingestion.domain import Failure
from trr_backend.ingestion.post_transport import SourceFailure
from trr_backend.socials.instagram.credential_guard import PinnedCookieIdentity
from trr_backend.socials.instagram.graphql_response import _primitive_failure
from trr_backend.socials.instagram.posts_scrapling.fetcher import _DocumentJsonScripts, _extract_page_tokens
from trr_backend.socials.instagram.public_response import decode_public_json, parse_public_retry_after

WWW_RELAY_URL = "https://www.instagram.com/api/graphql"
PARENT_ROOT_DOCUMENT = ("29631174456466007", "PolarisPostCommentsContainerQuery")
PARENT_DOCUMENT = ("28915717348095854", "PolarisPostCommentsPaginationQuery")
CHILD_DOCUMENT = ("28694573636831179", "PolarisPostCommentsChildrenPaginationtQuery")
AUTH_RELAY_FAMILY = "instagram_authenticated_www_relay"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
_MAX_BODY_BYTES = 8 * 1024 * 1024


def _module(html: str, name: str) -> dict[str, Any]:
    if len(html.encode()) > _MAX_BODY_BYTES:
        raise ValueError("native_relay_context_invalid")
    parser = _DocumentJsonScripts()
    parser.feed(html)
    parser.close()
    values = []
    for script in parser.scripts:
        try:
            root = json.loads(script)
        except (ValueError, RecursionError):
            raise ValueError("native_relay_context_invalid") from None
        if not isinstance(root, dict) or not isinstance(root.get("require"), list):
            continue
        for invocation in root["require"]:
            if not (
                isinstance(invocation, list)
                and len(invocation) >= 4
                and isinstance(invocation[3], list)
                and invocation[3]
                and isinstance(invocation[3][0], dict)
            ):
                continue
            box = invocation[3][0].get("__bbox")
            if not isinstance(box, dict) or not isinstance(box.get("define"), list):
                continue
            for definition in box["define"]:
                if isinstance(definition, list) and definition and definition[0] == name:
                    if len(definition) < 3 or definition[1] != [] or not isinstance(definition[2], dict):
                        raise ValueError("native_relay_context_invalid")
                    values.append(definition[2])
    if len(values) != 1:
        raise ValueError("native_relay_context_missing_or_conflicting")
    return values[0]


def _provider(html: str, key: str) -> bool:
    values = re.findall(r'"' + key + r'"\s*:\s*\{\s*"result"\s*:\s*(true|false)\b', html)
    if not values or len(set(values)) != 1:
        raise ValueError("native_relay_provider_missing_or_conflicting")
    return values[0] == "true"


@dataclass(frozen=True)
class WwwRelayContext:
    form: dict[str, str] = field(repr=False)
    logged_in: bool
    threads_style: bool

    @classmethod
    def from_html(cls, html: str):
        current = _module(html, "CurrentUserInitialData")
        relay = _module(html, "RelayAPIConfigDefaults")
        actor, user = relay.get("actorID"), current.get("USER_ID")
        if (
            not isinstance(actor, str)
            or not re.fullmatch(r"[1-9][0-9]{0,39}", actor)
            or actor != current.get("NON_FACEBOOK_USER_ID")
            or not isinstance(user, str)
            or not re.fullmatch(r"[0-9]{1,40}", user)
        ):
            raise ValueError("native_relay_actor_binding_invalid")
        tokens = _extract_page_tokens(html)
        if not tokens.get("fb_dtsg") or not tokens.get("lsd"):
            raise ValueError("native_relay_tokens_missing")
        logged_in, threads = _provider(html, "4150"), _provider(html, "23962")
        if not logged_in:
            raise ValueError("native_relay_not_authenticated")
        form = {
            "av": actor,
            "__user": user,
            "__a": "1",
            "__d": "www",
            "__req": "1",
            "__comet_req": "7",
            "fb_api_caller_class": "RelayModern",
            "server_timestamps": "true",
        }
        for key in ("fb_dtsg", "lsd", "__spin_r", "__spin_b", "__spin_t", "__hs"):
            if tokens.get(key):
                form[key] = tokens[key]
        if tokens.get("hsi"):
            form["__hsi"] = tokens["hsi"]
        form["jazoest"] = "2" + str(sum(map(ord, form["fb_dtsg"])))
        return cls(form, logged_in, threads)

    def request(self, *, media_id: str, cursor: str | None, parent_id: str | None = None):
        if not re.fullmatch(r"[1-9][0-9]{0,39}", media_id) or (
            parent_id is not None and not re.fullmatch(r"[1-9][0-9]{0,39}", parent_id)
        ):
            raise ValueError("native_relay_media_or_parent_invalid")
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise ValueError("native_relay_cursor_invalid")
        doc, name = CHILD_DOCUMENT if parent_id else PARENT_DOCUMENT
        variables = {
            "media_id": media_id,
            "after": cursor,
            "before": None,
            "first": 5 if parent_id else 10,
            "last": None,
            "__relay_internal__pv__PolarisIsLoggedInrelayprovider": self.logged_in,
            "__relay_internal__pv__PolarisThreadsStyleCommentsEnabledrelayprovider": self.threads_style,
        }
        if parent_id:
            variables.update(parent_comment_id=parent_id, is_chronological=None)
        else:
            variables["sort_order"] = "popular"
        return {
            **self.form,
            "doc_id": doc,
            "fb_api_req_friendly_name": name,
            "variables": json.dumps(variables, separators=(",", ":")),
        }


class AuthenticatedRelayPageClient:
    """Count each send, retain native JSON, and stop at the first failed page."""

    def __init__(self, *, cookies, pre_source_request, source_observer, source_failure_observer=None, client=None):
        self._identity = PinnedCookieIdentity(cookies)
        self._cookies = self._identity.validate_snapshot(cookies)
        if not self._cookies.get("csrftoken"):
            raise ValueError("native_relay_csrf_missing")
        self._before, self._observe, self._failed = pre_source_request, source_observer, source_failure_observer
        self._own_client = client is None
        self._client = client or httpx.Client(
            transport=httpx.HTTPTransport(retries=0, trust_env=False),
            trust_env=False,
            follow_redirects=False,
            timeout=20,
        )
        self._context = None
        self._referer = None
        self._bootstrap_html = None
        self._shortcode = None

    def close(self):
        if self._own_client:
            self._client.close()

    def _failure(self, code, delay=None):
        if self._failed is not None:
            self._failed(code, delay)
        raise SourceFailure(Failure(code, identity_blocked=code == "source_auth_failed"))

    def _send(self, method, url, *, data=None):
        self._cookies = self._identity.prepare_jar(self._client.cookies, self._cookies)
        headers = {
            "user-agent": USER_AGENT,
            "accept": "*/*",
            "origin": "https://www.instagram.com",
            "referer": self._referer,
            "x-csrftoken": self._cookies["csrftoken"],
            "x-ig-app-id": "936619743392459",
            "x-requested-with": "XMLHttpRequest",
        }
        if data:
            headers.update({"x-fb-friendly-name": data["fb_api_req_friendly_name"], "x-fb-lsd": data["lsd"]})
        self._before()
        try:
            with self._client.stream(method, url, data=data, headers=headers, follow_redirects=False) as response:
                delay = parse_public_retry_after(response.headers.get("retry-after"))
                if response.status_code in (401, 403):
                    self._failure("source_auth_failed", delay)
                if response.status_code == 429:
                    self._failure("rate_limited", delay)
                if 300 <= response.status_code < 400:
                    self._failure("endpoint_redirect", delay)
                if response.status_code != 200:
                    self._failure("request_failed", delay)
                if delay is not None:
                    self._failure("rate_limited", delay)
                body = bytearray()
                for chunk in response.iter_bytes():
                    body.extend(chunk)
                    if len(body) > _MAX_BODY_BYTES:
                        self._failure("source_incomplete")
                self._cookies = self._identity.prepare_jar(self._client.cookies, self._cookies)
                # iter_bytes has already decoded Content-Encoding. Do not ask
                # the detached response to decompress the same bytes twice.
                headers = {
                    key: value
                    for key, value in response.headers.items()
                    if key.lower() not in {"content-encoding", "content-length"}
                }
                return httpx.Response(200, headers=headers, content=bytes(body))
        except (httpx.TransportError, httpx.DecodingError):
            self._failure("request_failed")

    def bootstrap(self, shortcode):
        if not isinstance(shortcode, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", shortcode):
            raise ValueError("native_relay_shortcode_invalid")
        self._referer = f"https://www.instagram.com/p/{shortcode}/"
        response = self._send("GET", self._referer)
        try:
            self._context = WwwRelayContext.from_html(response.text)
            self._bootstrap_html = response.text
            self._shortcode = shortcode
        except (ValueError, RecursionError):
            self._failure("source_incomplete")

    def page(self, *, media_id, cursor=None, parent_id=None):
        if self._context is None:
            raise ValueError("native_relay_bootstrap_required")
        if parent_id is None and cursor is None:
            raise ValueError("native_parent_root_requires_bootstrap_page")
        form = self._context.request(media_id=media_id, cursor=cursor, parent_id=parent_id)
        response = self._send("POST", WWW_RELAY_URL, data=form)
        try:
            payload = decode_public_json(response)
        except (ValueError, RecursionError):
            self._failure("parse_error")
        if not isinstance(payload, dict):
            self._failure("parse_error")
        payload["cursor_payload"] = {
            "adapter_family": AUTH_RELAY_FAMILY,
            "adapter_version": 1,
            "source": "resume" if cursor else "root",
            "request_cursor": cursor,
            "relay_doc_id": form["doc_id"],
            "relay_friendly_name": form["fb_api_req_friendly_name"],
            "sort_order": "native_default" if parent_id else "popular",
            "page_size": 5 if parent_id else 10,
        }
        if parent_id:
            payload["parent_comment_id"] = parent_id
        return self._accept_payload(payload, parent_id=parent_id)

    def root_page(self):
        if self._context is None or self._bootstrap_html is None:
            raise ValueError("native_relay_bootstrap_required")
        from trr_backend.ingestion.authenticated_bootstrap import AuthenticatedBootstrapError, extract_parent_bootstrap

        try:
            payload = extract_parent_bootstrap(
                self._bootstrap_html, shortcode=self._shortcode, document_url=self._referer
            )
        except AuthenticatedBootstrapError as error:
            self._failure(error.failure_code, error.retry_after_seconds)
        except (ValueError, RecursionError):
            self._failure("source_incomplete")
        payload["cursor_payload"] = {
            "adapter_family": AUTH_RELAY_FAMILY,
            "adapter_version": 1,
            "source": "root",
            "request_cursor": None,
            "relay_doc_id": PARENT_ROOT_DOCUMENT[0],
            "relay_friendly_name": PARENT_ROOT_DOCUMENT[1],
            "sort_order": "popular",
            "page_size": None,  # Native preloader omits first; server chooses the initial size.
        }
        return self._accept_payload(payload, parent_id=None)

    def _accept_payload(self, payload, *, parent_id):
        primitive = _primitive_failure(payload)
        failure = comment_page_failure(payload)
        if primitive is not None and (failure is None or failure[0] == "source_incomplete"):
            failure = (
                "source_auth_failed"
                if primitive.auth_failed
                else "rate_limited"
                if primitive.reason == "rate_limited"
                else "source_incomplete",
                failure[1] if failure else None,
            )
        try:
            self._observe("graphql_replies" if parent_id else "graphql_comments", payload)
        except CommentSourceError:
            if failure:
                self._failure(*failure)
            raise
        if failure:
            self._failure(*failure)
        root = (
            "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection"
            if parent_id
            else "xdt_api__v1__media__media_id__comments__connection"
        )
        data = payload.get("data")
        connection = data.get(root) if isinstance(data, dict) else None
        if not isinstance(connection, dict):
            self._failure("source_incomplete")
        edges, info = connection.get("edges"), connection.get("page_info")
        if (
            not isinstance(edges, list)
            or any(
                not isinstance(edge, dict) or not isinstance(edge.get("node"), dict) or not edge["node"]
                for edge in edges
            )
            or not isinstance(info, dict)
            or type(info.get("has_next_page")) is not bool
            or (
                info.get("has_next_page")
                and (not isinstance(info.get("end_cursor"), str) or not info["end_cursor"].strip())
            )
        ):
            self._failure("source_incomplete")
        return payload
