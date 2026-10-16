"""Shared Instagram source pacing using the maintained detail request lanes.

Only credential hashes are retained. Call before_request immediately before each
explicit HTTP/browser operation, after disabling hidden transport retries. Its
SQL transaction closes before this method returns. A post-cooldown probe belongs
to one gate across multiple requests until explicit finish or bounded expiry.
"""

from __future__ import annotations

import hashlib
import math
import time
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import psycopg2
from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Failure
from trr_backend.socials.instagram.constants import AUTH_FATAL_MESSAGES
from trr_backend.socials.instagram.detail_transport import AUTH_ERRORS

WINDOW_SECONDS = 660
REQUEST_LIMIT = 180
_SAFE_ERRORS = (
    AUTH_ERRORS
    | AUTH_FATAL_MESSAGES
    | {
        "rate_limited",
        "network_error",
        "transport_timeout",
        "request_failed",
        "parse_error",
        "partial_required_fields",
        "endpoint_redirect",
        "source_auth_failed",
        "listing_fetch_failed",
        "comment_fetch_failed",
        "source_evidence_missing",
        "source_incomplete",
    }
)


class IdentityDeferred(RuntimeError):  # noqa: N818 - domain vocabulary
    def __init__(self, code: str, next_attempt_at: datetime, *, blocked: bool = False):
        self.failure = Failure(code, retry_at=next_attempt_at, identity_blocked=blocked)
        self.error_code, self.next_attempt_at = code, next_attempt_at
        super().__init__(code)


@dataclass(frozen=True)
class RequestPermit:
    identity_key: str
    failure_generation: int
    request_number: int
    probe_token: str | None = field(repr=False)
    owner_token: str = field(repr=False)


class SourceRequestGate:
    def __init__(
        self,
        *,
        cookies: Mapping[str, Any],
        transaction: Callable,
        check_cancelled: Callable[[], None],
        deadline: float,
        cooldown_seconds: float = 900,
        probe_seconds: float = 35,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        if not all(math.isfinite(value) for value in (deadline, cooldown_seconds, probe_seconds)):
            raise ValueError("Identity policy deadlines must be finite")
        if cooldown_seconds < 1 or probe_seconds < 1:
            raise ValueError("Identity policy leases must be positive")
        # Match DetailTransport exactly; the caller already chose this explicit
        # credential snapshot. Do not retain cookies, resolve identities or repair.
        identity = str(cookies.get("ds_user_id") or cookies.get("sessionid") or "public")
        self.identity_key = hashlib.sha256(identity.encode()).hexdigest()
        self.credential_fingerprint = hashlib.sha256(str(cookies.get("sessionid") or "public").encode()).hexdigest()
        self.transaction, self.check_cancelled = transaction, check_cancelled
        self.deadline, self.cooldown_seconds, self.probe_seconds = deadline, cooldown_seconds, probe_seconds
        self.monotonic = monotonic
        self._owner_token = uuid4().hex
        self._probe_token: str | None = None
        self._last_permit: RequestPermit | None = None
        self._failures: dict[int, Failure] = {}
        self._last_failure: Failure | None = None
        self._finished = False
        self.request_count = 0

    def __repr__(self) -> str:
        return f"SourceRequestGate(identity_key={self.identity_key!r}, request_count={self.request_count})"

    def _remaining(self) -> float:
        self.check_cancelled()
        remaining = self.deadline - self.monotonic()
        if remaining <= 0:
            raise IdentityDeferred("target_deadline", datetime.now(UTC) + timedelta(minutes=1))
        return remaining

    @contextmanager
    def _locked(self, *, create: bool = False, remaining: float | None = None):
        try:
            with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                if conn.autocommit:
                    raise ValueError("Identity policy requires an explicit transaction")
                if remaining is not None:
                    timeout = f"{max(1, min(2147483647, int(remaining * 1000)))}ms"
                    cur.execute("set local lock_timeout=%s", (timeout,))
                    cur.execute("set local statement_timeout=%s", (timeout,))
                if create:
                    cur.execute(
                        """insert into social.instagram_detail_request_lanes(identity_key)
                        values(%s) on conflict do nothing""",
                        (self.identity_key,),
                    )
                cur.execute(
                    "select * from social.instagram_detail_request_lanes where identity_key=%s for update",
                    (self.identity_key,),
                )
                row = cur.fetchone()
                if not row:
                    raise IdentityDeferred("identity_lane_missing", datetime.now(UTC) + timedelta(minutes=1))
                # Read the database clock after any row-lock wait.
                cur.execute("select clock_timestamp() as current_time")
                now = cur.fetchone()["current_time"]
                yield cur, dict(row), now
        except psycopg2.Error:
            raise IdentityDeferred("identity_policy_unavailable", datetime.now(UTC) + timedelta(minutes=1)) from None

    def before_request(self) -> RequestPermit:
        if self._finished:
            raise RuntimeError("identity_gate_already_finished")
        remaining = self._remaining()
        with self._locked(create=True, remaining=remaining) as (cur, row, now):
            self._remaining()
            cooldown = row.get("cooldown_until")
            replacement = bool(row["blocked"] and row.get("blocked_fingerprint") != self.credential_fingerprint)
            if row["blocked"] and not replacement:
                code = row.get("last_error_code")
                raise IdentityDeferred(
                    code if code in _SAFE_ERRORS else "auth_blocked", now + timedelta(hours=24), blocked=True
                )
            active_probe = bool(
                row.get("probe_token") and row.get("probe_expires_at") and row["probe_expires_at"] > now
            )
            own_probe = active_probe and row["probe_token"] == self._probe_token
            if active_probe and not own_probe:
                raise IdentityDeferred("identity_probe_pending", row["probe_expires_at"])
            if replacement:
                cooldown = now  # An explicitly replaced fingerprint gets one shared probe.
            if cooldown and cooldown > now:
                raise IdentityDeferred("identity_cooldown", cooldown)
            times = sorted(value for value in row["request_times"] if value > now - timedelta(seconds=WINDOW_SECONDS))
            if len(times) >= REQUEST_LIMIT:
                raise IdentityDeferred("identity_budget", times[0] + timedelta(seconds=WINDOW_SECONDS))
            # Expired probes get a NEW token even for the same gate. Late results
            # from the abandoned lease can never own the replacement lease.
            token = row["probe_token"] if own_probe else (uuid4().hex if cooldown or row.get("probe_token") else None)
            lease_seconds = min(self.probe_seconds, self._remaining())
            cur.execute(
                """update social.instagram_detail_request_lanes set request_times=%s,
                blocked=%s,cooldown_until=%s,probe_token=%s,probe_expires_at=%s where identity_key=%s""",
                (
                    times + [now],
                    False if replacement else row["blocked"],
                    cooldown,
                    token,
                    now + timedelta(seconds=lease_seconds) if token else None,
                    self.identity_key,
                ),
            )
            permit = RequestPermit(
                self.identity_key, int(row["failure_generation"]), self.request_count + 1, token, self._owner_token
            )
        self._probe_token, self._last_permit = token, permit
        self.request_count += 1
        return permit

    def _permit(self, permit: RequestPermit | None) -> RequestPermit:
        permit = permit or self._last_permit
        if permit is None or permit.owner_token != self._owner_token or permit.identity_key != self.identity_key:
            raise ValueError("identity_request_permit_required")
        return permit

    @staticmethod
    def _owns(row: Mapping[str, Any], permit: RequestPermit, now: datetime) -> bool:
        if row.get("probe_token") != permit.probe_token:
            return False
        return permit.probe_token is None or bool(row.get("probe_expires_at") and row["probe_expires_at"] > now)

    def record_success(self, permit: RequestPermit | None = None) -> bool:
        return self._success(self._permit(permit), finish=False)

    def finish_success(self, permit: RequestPermit | None = None) -> bool:
        return self._success(self._permit(permit), finish=True)

    def _success(self, permit: RequestPermit, *, finish: bool) -> bool:
        if self._finished:
            return False
        accepted = False
        with self._locked() as (cur, row, now):
            if self._owns(row, permit, now) and row["failure_generation"] == permit.failure_generation:
                # An intermediate response is not proof of the complete target;
                # keep its exclusive probe and cooldown marker until finish.
                if not permit.probe_token or finish:
                    cur.execute(
                        """update social.instagram_detail_request_lanes set consecutive_failures=0,
                        last_error_code=null,cooldown_until=%s,blocked_fingerprint=%s,
                        probe_token=%s,probe_expires_at=%s where identity_key=%s""",
                        (
                            None if permit.probe_token else row.get("cooldown_until"),
                            row.get("blocked_fingerprint") if row["blocked"] else None,
                            None if finish else row.get("probe_token"),
                            None if finish else row.get("probe_expires_at"),
                            self.identity_key,
                        ),
                    )
                accepted = True
            elif finish and permit.probe_token and self._owns(row, permit, now):
                # Health changed while this target ran. Release only its own
                # lease; never clear the newer failure/cooldown/block.
                cur.execute(
                    """update social.instagram_detail_request_lanes set probe_token=null,probe_expires_at=null
                    where identity_key=%s and probe_token=%s""",
                    (self.identity_key, permit.probe_token),
                )
        if finish:
            self._finished = True
            self._probe_token = None
        return accepted

    def record_failure(
        self, code: str, *, retry_after_seconds: float | None = None, permit: RequestPermit | None = None
    ) -> Failure:
        return self._failure(self._permit(permit), code, retry_after_seconds, finish=False)

    def finish_failure(
        self, code: str | None = None, *, retry_after_seconds: float | None = None, permit: RequestPermit | None = None
    ) -> Failure | None:
        if self._finished:
            return self._last_failure
        if code is not None:
            return self._failure(self._permit(permit), code, retry_after_seconds, finish=True)
        # Cancellation/finally cleanup also works when before_request never
        # admitted a request. It has no authority over another gate's probe.
        token = self._probe_token
        if token:
            with self._locked() as (cur, _, _):
                cur.execute(
                    """update social.instagram_detail_request_lanes set probe_token=null,probe_expires_at=null
                    where identity_key=%s and probe_token=%s""",
                    (self.identity_key, token),
                )
        self._finished, self._probe_token = True, None
        return self._last_failure

    def _failure(self, permit: RequestPermit, code: str, retry_after_seconds: float | None, *, finish: bool) -> Failure:
        # The independent listing/comments adapters expose this typed alias.
        # Normalize it to the maintained auth vocabulary so legacy detail
        # consumers sharing this row also classify the block correctly.
        if code == "source_auth_failed":
            code = "unauthorized"
        code = code if code in _SAFE_ERRORS else "request_failed"
        if self._finished:
            return self._last_failure or Failure("identity_gate_already_finished")
        previous = self._failures.get(permit.request_number)
        if previous is not None:
            if finish:
                self.finish_failure()
            return previous
        delay = self.cooldown_seconds
        if retry_after_seconds is not None and math.isfinite(retry_after_seconds) and retry_after_seconds >= 0:
            delay = retry_after_seconds
        with self._locked() as (cur, row, now):
            if not self._owns(row, permit, now):
                failure = Failure(
                    "identity_probe_lost", retry_at=row.get("probe_expires_at") or now + timedelta(seconds=1)
                )
            else:
                failures = int(row["consecutive_failures"]) + 1 if code == row.get("last_error_code") else 1
                blocked = bool(row["blocked"]) or code in AUTH_ERRORS
                opens = code == "rate_limited" or failures >= 3 or permit.probe_token is not None
                try:
                    cooldown = now + timedelta(seconds=delay) if opens else row.get("cooldown_until")
                except OverflowError:
                    # A malformed/unbounded Retry-After can defer safely, but
                    # cannot become a raw exception or reopen the source lane.
                    cooldown = datetime.max.replace(tzinfo=UTC)
                failure = Failure(code, retry_at=cooldown, identity_blocked=blocked)
                cur.execute(
                    """update social.instagram_detail_request_lanes set blocked=%s,last_error_code=%s,
                    consecutive_failures=%s,cooldown_until=%s,failure_generation=failure_generation+1,
                    blocked_fingerprint=%s,probe_token=%s,probe_expires_at=%s where identity_key=%s""",
                    (
                        blocked,
                        code,
                        failures,
                        cooldown,
                        ((row.get("blocked_fingerprint") if row["blocked"] else None) or self.credential_fingerprint)
                        if blocked
                        else None,
                        None if finish else row.get("probe_token"),
                        None if finish else row.get("probe_expires_at"),
                        self.identity_key,
                    ),
                )
        self._failures[permit.request_number] = failure
        self._last_failure = failure
        if finish:
            self._finished, self._probe_token = True, None
        return failure
