"""Bounded detail requests with durable identity-wide pacing and circuit state."""

from __future__ import annotations

import hashlib
import os
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import requests
from urllib3.util.retry import Retry

from trr_backend.db import pg
from trr_backend.socials.instagram.request_client import InstagramRequestClient, InstagramRequestFailure

AUTH_ERRORS = frozenset(
    {
        "unauthorized",
        "forbidden",
        "redirect_login",
        "login_required",
        "checkpoint_required",
        "challenge_required",
        "feedback_required",
    }
)


class DetailDeferred(InstagramRequestFailure):
    def __init__(self, code: str, next_attempt_at: datetime) -> None:
        super().__init__(code, status_code=None, retryable=True)
        self.next_attempt_at = next_attempt_at


class DetailTransport:
    def __init__(
        self,
        *,
        scraper: Any,
        check_cancelled: Callable[[], None],
        on_request: Callable[[bool], None],
        deadline: float,
        cooldown_seconds: float | None = None,
    ) -> None:
        self.scraper = scraper
        # Hidden adapter retries would escape both the request budget and cap.
        for adapter in scraper.session.adapters.values():
            adapter.max_retries = Retry(total=0, redirect=0)
        self.cookies = scraper._request_cookies()
        identity = str(self.cookies.get("ds_user_id") or self.cookies.get("sessionid") or "public")
        self.identity_key = hashlib.sha256(identity.encode()).hexdigest()
        self.credential_fingerprint = hashlib.sha256(
            str(self.cookies.get("sessionid") or "public").encode()
        ).hexdigest()
        self.check_cancelled = check_cancelled
        self.on_request = on_request
        self.deadline = deadline
        try:
            cooldown_seconds = float(cooldown_seconds or os.getenv("SOCIAL_INSTAGRAM_DETAILS_COOLDOWN_SECONDS") or 900)
        except ValueError:
            cooldown_seconds = 900
        self.cooldown_seconds = max(1, cooldown_seconds)
        self.request_count = 0
        self.retried = False
        self.probe_token: str | None = None
        self.failure_generation = 0
        self.client = InstagramRequestClient(session=scraper.session)
        self.evidence: list[dict[str, Any]] = []

    def check_deadline(self) -> float:
        self.check_cancelled()
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise DetailDeferred("target_deadline", datetime.now(UTC) + timedelta(minutes=1))
        return remaining

    def _reserve(self) -> None:
        self.check_deadline()
        with pg.db_connection(label="instagram_detail_request_reserve") as conn:
            with pg.db_cursor(conn=conn) as cur:
                cur.execute(
                    """insert into social.instagram_detail_request_lanes(identity_key)
                    values (%s) on conflict do nothing""",
                    [self.identity_key],
                )
                row = pg.fetch_one_with_cursor(
                    cur,
                    """select *, now() as current_time
                    from social.instagram_detail_request_lanes where identity_key = %s for update""",
                    [self.identity_key],
                )
                now = row["current_time"]
                cooldown = row.get("cooldown_until")
                if row["blocked"]:
                    if row.get("blocked_fingerprint") == self.credential_fingerprint:
                        raise DetailDeferred(row.get("last_error_code") or "auth_blocked", now + timedelta(hours=24))
                    # Explicitly replaced credentials may prove health with one
                    # shared probe; this path never obtains/replaces credentials.
                    cur.execute(
                        """update social.instagram_detail_request_lanes set blocked = false,
                        cooldown_until = now() where identity_key = %s""",
                        [self.identity_key],
                    )
                    cooldown = now
                if cooldown and cooldown > now:
                    raise DetailDeferred("identity_cooldown", cooldown)
                if row.get("probe_token") and row.get("probe_expires_at") > now:
                    raise DetailDeferred("identity_probe_pending", row["probe_expires_at"])
                times = [value for value in row["request_times"] if value > now - timedelta(seconds=660)]
                if len(times) >= 180:
                    raise DetailDeferred("identity_budget", times[0] + timedelta(seconds=660))
                # The first request after cooldown is exclusively leased. It must
                # prove health before any worker can reserve ordinary work.
                self.probe_token = uuid.uuid4().hex if cooldown else None
                self.failure_generation = int(row["failure_generation"])
                self.check_deadline()
                cur.execute(
                    """update social.instagram_detail_request_lanes set request_times = %s,
                    probe_token = %s, probe_expires_at = %s where identity_key = %s""",
                    [
                        times + [now],
                        self.probe_token,
                        now + timedelta(seconds=35) if self.probe_token else None,
                        self.identity_key,
                    ],
                )

    def _outcome(self, error: InstagramRequestFailure | None) -> None:
        with pg.db_connection(label="instagram_detail_request_outcome") as conn:
            with pg.db_cursor(conn=conn) as cur:
                row = pg.fetch_one_with_cursor(
                    cur,
                    """select *, now() as current_time
                    from social.instagram_detail_request_lanes where identity_key = %s for update""",
                    [self.identity_key],
                )
                # A late result cannot clear another probe or a newer failure.
                if row.get("probe_token") != self.probe_token:
                    return
                if error is None and row["failure_generation"] != self.failure_generation:
                    return
                code = error.error_code if error else None
                failures = (
                    (int(row["consecutive_failures"]) + 1 if code == row.get("last_error_code") else 1) if error else 0
                )
                blocked = bool(row["blocked"]) or code in AUTH_ERRORS
                delay = (
                    error.retry_after_seconds
                    if error and error.retry_after_seconds is not None
                    else self.cooldown_seconds
                )
                opens = error and (code == "rate_limited" or failures >= 3 or self.probe_token)
                cooldown = row["current_time"] + timedelta(seconds=delay) if opens else row.get("cooldown_until")
                if not error and self.probe_token:
                    cooldown = None
                if error and cooldown:
                    error.next_attempt_at = cooldown
                cur.execute(
                    """update social.instagram_detail_request_lanes set blocked = %s,
                    last_error_code = %s, consecutive_failures = %s, cooldown_until = %s,
                    failure_generation = failure_generation + %s, blocked_fingerprint = %s,
                    probe_token = null, probe_expires_at = null where identity_key = %s""",
                    [
                        blocked,
                        code,
                        failures,
                        cooldown,
                        int(error is not None),
                        self.credential_fingerprint if blocked else None,
                        self.identity_key,
                    ],
                )
        self.probe_token = None

    def request(self, url: str, *, text: bool = False) -> Any:
        destination = urlsplit(url)
        if destination.scheme != "https" or destination.hostname not in {"www.instagram.com", "i.instagram.com"}:
            raise InstagramRequestFailure("untrusted_origin", status_code=None, retryable=False)
        while True:
            self._reserve()
            started = time.monotonic()
            response_status = None

            def sender(*args: Any, **kwargs: Any) -> Any:
                nonlocal response_status
                self.check_deadline()
                self.on_request(self.request_count == 0)
                self.request_count += 1
                response = self.scraper.session.get(*args, **kwargs)
                response_status = response.status_code
                return response

            try:
                remaining = self.check_deadline()
                kwargs = {
                    "query_type": "permalink" if text else "post_info",
                    "headers": self.scraper._get_headers(url),
                    "cookies": self.cookies,
                    "timeout": (min(5, remaining / 3), min(15, remaining / 2)),
                    "sender": sender,
                }
                result = self.client.get_text(url, **kwargs) if text else self.client.get_json(url, params={}, **kwargs)
                self.check_deadline()
            except (InstagramRequestFailure, requests.RequestException) as exc:
                error = (
                    exc
                    if isinstance(exc, InstagramRequestFailure)
                    else InstagramRequestFailure(
                        "transport_timeout" if isinstance(exc, requests.Timeout) else "network_error",
                        status_code=None,
                        retryable=True,
                    )
                )
                self._outcome(error)
                self.evidence.append(
                    {
                        "endpoint": "permalink" if text else "post_info",
                        "status": response_status,
                        "host": destination.hostname,
                        "path": destination.path,
                        "destination": error.redirect_target,
                        "error": error.error_code,
                        "latency_ms": int((time.monotonic() - started) * 1000),
                    }
                )
                if (
                    error.error_code in {"network_error", "transport_timeout", "request_failed"}
                    and error.retryable
                    and not self.retried
                ):
                    self.retried = True
                    continue
                raise error from exc
            else:
                self.evidence.append(
                    {
                        "endpoint": "permalink" if text else "post_info",
                        "status": response_status,
                        "host": destination.hostname,
                        "path": destination.path,
                        "error": None,
                        "latency_ms": int((time.monotonic() - started) * 1000),
                    }
                )
                return result

    def fetch(
        self,
        shortcode: str,
        *,
        allow_public_fallback: bool = False,
        validate: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        def accept(payload: dict[str, Any]) -> dict[str, Any]:
            try:
                if validate is not None:
                    validate(payload)
            except (ValueError, TypeError, AttributeError, KeyError, IndexError, OverflowError) as exc:
                error = InstagramRequestFailure("partial_required_fields", status_code=200, retryable=True)
                self._outcome(error)
                if self.evidence:
                    self.evidence[-1]["error"] = error.error_code
                raise error from exc
            self._outcome(None)
            return payload

        url = self.scraper.POST_INFO_URL.format(media_id=self.scraper._shortcode_to_media_id(shortcode))
        try:
            result = self.request(url)
            if isinstance(result.get("items"), list) and result["items"] and isinstance(result["items"][0], dict):
                return accept(result)
            error = InstagramRequestFailure("parse_error", status_code=200, retryable=True)
            self._outcome(error)
            raise error
        except InstagramRequestFailure as exc:
            if not allow_public_fallback or exc.error_code not in {"parse_error", "endpoint_redirect"}:
                raise
        from trr_backend.socials.instagram.permalink_metadata import fetch_permalink_media_item

        item = fetch_permalink_media_item(shortcode, session=self.scraper.session, transport=self)
        if item is not None:
            return accept({"items": [item]})
        error = InstagramRequestFailure("parse_error", status_code=200, retryable=True)
        self._outcome(error)
        raise error
