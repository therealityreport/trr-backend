"""Public-only response rejection and safe server delay parsing; no source IO."""

from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import unquote, urlsplit

from trr_backend.socials.instagram.constants import AUTH_FATAL_MESSAGES
from trr_backend.socials.instagram.request_client import InstagramRequestClient, InstagramRequestFailure

_NUMBER = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\Z")
_MAX_DELAY = Decimal(str(sys.float_info.max))


def decode_public_json(response):
    """Decode native JSON with Instagram's optional anti-hijacking prefix."""
    body = response.text.lstrip("\ufeff \t\r\n")
    for prefix in ("for(;;);", "for (;;);"):
        if body.startswith(prefix):
            body = body[len(prefix) :].lstrip()
            break
    return json.loads(body)


def parse_public_retry_after(value, now: datetime | None = None) -> float | None:
    """Keep valid server delays, including overflow, without leaking header text.

    Huge valid numbers saturate to a finite float. The gate's existing timedelta
    overflow handling then defers to datetime.max instead of retrying early.
    """
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        return None
    text = str(value).strip()
    if not text:
        return None
    if _NUMBER.fullmatch(text):
        try:
            seconds = Decimal(text)
            if not seconds.is_finite() or seconds < 0:
                return None
            return float(min(_MAX_DELAY, seconds))
        except (InvalidOperation, ValueError, OverflowError):
            return None if text.startswith("-") else sys.float_info.max
    try:
        moment = parsedate_to_datetime(text)
        reference = now or datetime.now(UTC)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=UTC)
        return max(0.0, (moment - reference).total_seconds())
    except (TypeError, ValueError, OverflowError, AttributeError):
        return None


def _auth_path(value: str) -> str | None:
    try:
        path = unquote(urlsplit(value).path).lower().rstrip("/")
    except ValueError:
        return None
    for prefix, code in (
        ("/accounts/login", "redirect_login"),
        ("/challenge", "challenge_required"),
        ("/checkpoint", "checkpoint_required"),
    ):
        if path == prefix or path.startswith(prefix + "/"):
            return code
    return None


def _structured_auth(payload) -> str | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("error") == 1357001:
        return "login_required"
    for field in ("message", "error_type"):
        code = payload.get(field)
        if isinstance(code, str) and code.strip().lower() in AUTH_FATAL_MESSAGES:
            return code.strip().lower()
    for code in AUTH_FATAL_MESSAGES:
        if payload.get(code) is True:
            return code
    # These are document route containers, never a recursive search through
    # captions, comments, navigation or arbitrary native source strings.
    entry = payload.get("entry_data")
    if isinstance(entry, dict):
        for name, code in (
            ("LoginAndSignupPage", "login_required"),
            ("ChallengePage", "challenge_required"),
            ("CheckpointPage", "checkpoint_required"),
        ):
            if name in entry:
                return code
    return None


class _AuthDocument(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.code, self.forms = None, []
        self.in_title, self.title = False, []
        self.json_script, self.script = False, []

    def handle_starttag(self, tag, attrs):
        fields = dict(attrs)
        if tag == "title":
            self.in_title = True
        elif tag == "form":
            self.forms.append((fields.get("action") or "", set()))
            self.code = self.code or _auth_path(self.forms[-1][0])
        elif tag == "input" and self.forms:
            self.forms[-1][1].add((fields.get("name") or "").lower())
            inputs = self.forms[-1][1]
            if {"username", "password"} <= inputs:
                self.code = self.code or "login_required"
            if inputs & {"security_code", "verificationcode", "verification_code"}:
                self.code = self.code or "challenge_required"
        elif tag == "script":
            self.json_script = (fields.get("type") or "").lower() == "application/json"
            self.script = []

    def handle_data(self, data):
        if self.in_title:
            self.title.append(data)
        if self.json_script:
            self.script.append(data)

    def handle_endtag(self, tag):
        if tag == "title":
            self.in_title = False
            title = " ".join("".join(self.title).lower().split())
            parts = re.split(r"\s*[•|]\s*", title)
            if len(parts) == 2 and "instagram" in parts:
                title = next((part for part in parts if part != "instagram"), "")
            codes = {
                "login": "login_required",
                "log in": "login_required",
                "challenge": "challenge_required",
                "security check": "challenge_required",
                "confirm it's you": "challenge_required",
                "confirm it’s you": "challenge_required",
                "help us confirm you own this account": "challenge_required",
                "checkpoint": "checkpoint_required",
            }
            self.code = self.code or codes.get(title)
        elif tag == "form" and self.forms:
            self.forms.pop()
        elif tag == "script" and self.json_script:
            try:
                self.code = self.code or _structured_auth(json.loads("".join(self.script)))
            except (ValueError, TypeError):
                pass
            self.json_script, self.script = False, []


class PublicRequestClient(InstagramRequestClient):
    """Keep native request protocol; never classify arbitrary HTML keywords."""

    def _classify_response(self, response):
        status, headers = response.status_code, response.headers
        code, retryable = None, False
        location = headers.get("location", "")
        if status in (401, 403, 429):
            code = {401: "unauthorized", 403: "forbidden", 429: "rate_limited"}[status]
            retryable = status == 429
        elif 300 <= status < 400:
            code = _auth_path(location) or "endpoint_redirect"
        elif status >= 400:
            code, retryable = "request_failed", status >= 500 or status == 408
        else:
            body = response.text
            if "html" in headers.get("content-type", "").lower() or body.lstrip().startswith("<"):
                document = _AuthDocument()
                document.feed(body)
                document.close()
                code = document.code
            else:
                try:
                    code = _structured_auth(decode_public_json(response))
                except ValueError:
                    pass
        if code:
            raise InstagramRequestFailure(
                code,
                status_code=status,
                retryable=retryable,
                redirect_target=location or None,
                retry_after_seconds=parse_public_retry_after(headers.get("retry-after")),
            )

    @staticmethod
    def _json(response):
        try:
            payload = decode_public_json(response)
            if not isinstance(payload, dict):
                raise ValueError("Expected JSON object")
            code = _structured_auth(payload)
            if code:
                raise InstagramRequestFailure(code, status_code=response.status_code, retryable=False)
            return payload
        except ValueError as error:
            raise InstagramRequestFailure(
                "parse_error",
                status_code=response.status_code,
                retryable=True,
                retry_after_seconds=parse_public_retry_after(response.headers.get("retry-after")),
            ) from error
        except InstagramRequestFailure as error:
            error.retry_after_seconds = parse_public_retry_after(response.headers.get("retry-after"))
            raise
