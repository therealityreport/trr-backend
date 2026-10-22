"""Counted profile observations for an already-owned Instagram media target.

The injected source performs one explicit request, without retries, warming or
credential repair. No HTTP client or credential snapshot is retained here.
Profile evidence can also be parsed/applied offline; resolving an avatar never
settles the media receipt or proves that its image has been hosted.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Failure, LostOwnership, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.identity_policy import IdentityDeferred, SourceRequestGate, is_content_absence
from trr_backend.ingestion.media import source_reference
from trr_backend.ingestion.persistence import Store
from trr_backend.socials.media_url_safety import (
    MediaUrlSafetyPolicy,
    UnsafeMediaUrlError,
    allowed_hosts_for_platform,
    validate_media_url,
)

AVATAR_PARSER_VERSION = "instagram-profile-avatar-v1"
AVATAR_SOURCE_PREFIX = "instagram:profile:"
DEFAULT_UNSUPPORTED_TTL_HOURS = 168
_AVATAR_FIELDS = ("profile_pic_url_hd", "profile_pic_url")
_POLICY = MediaUrlSafetyPolicy(allowed_hosts_for_platform("instagram"), resolve_dns=False)
LOGGER = logging.getLogger(__name__)


class AvatarResolutionError(ValueError):
    def __init__(self, code: str, *, failure: Failure | None = None):
        self.code = code
        self.failure = failure or Failure(code, retry_at=datetime.now(UTC) + timedelta(minutes=1))
        super().__init__(code)


def normalize_handle(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.]{1,30}", value):
        raise AvatarResolutionError("avatar_identity_invalid")
    return value.lower()


@dataclass(frozen=True)
class AvatarObservation:
    username: str
    observed_at: datetime
    source_url: str | None = field(repr=False)
    unavailable: bool = False
    evidence_id: str | None = None


def _profile(payload: Mapping[str, Any], username: str) -> str | None:
    """Require a complete native profile response bound to the requested user."""
    data = payload.get("data")
    user = data.get("user") if isinstance(data, dict) else None
    if payload.get("status") != "ok" or not isinstance(user, dict):
        raise AvatarResolutionError("avatar_profile_incomplete")
    if not isinstance(user.get("username"), str) or normalize_handle(user["username"]) != username:
        raise AvatarResolutionError("avatar_identity_mismatch")
    # Absence of the entire avatar field is a partial DTO, not unavailable
    # proof. Explicit null/empty native fields in a complete profile are proof.
    if not any(key in user for key in _AVATAR_FIELDS):
        raise AvatarResolutionError("avatar_profile_incomplete")
    for key in _AVATAR_FIELDS:
        value = user.get(key)
        if value is None or value == "":
            continue
        if not isinstance(value, str):
            raise AvatarResolutionError("avatar_reference_invalid")
        try:
            validate_media_url(value, policy=_POLICY)
            source_reference(value)
        except (ValueError, UnsafeMediaUrlError):
            raise AvatarResolutionError("avatar_reference_invalid") from None
        return value
    return None


def parse_avatar_evidence(evidence: Evidence, *, username: str, pinned: bool = False) -> AvatarObservation:
    """Pure parser: original timestamps and sanitized paths survive replay."""
    username = normalize_handle(username)
    if evidence.parser_version != AVATAR_PARSER_VERSION:
        raise AvatarResolutionError("avatar_parser_unavailable")
    if evidence.source_identity != AVATAR_SOURCE_PREFIX + username:
        raise AvatarResolutionError("avatar_evidence_identity_mismatch")
    payload = evidence.read(pinned=pinned)
    url = _profile(payload, username)
    if url is None and payload.get("avatar_url_explicitly_missing") is not True:
        raise AvatarResolutionError("avatar_unavailable_proof_missing")
    return AvatarObservation(username, evidence.captured_at, url, url is None)


class AvatarResolutionActivity:
    def __init__(self, store: Store, *, gate: SourceRequestGate):
        self.store, self.gate = store, gate

    def apply_evidence(
        self,
        *,
        run_id: str,
        account: str,
        generation: int,
        target_key: str,
        owner_token: str,
        username: str,
        evidence: Evidence,
        evidence_id: str,
    ) -> AvatarObservation:
        """Offline fenced write; evidence must already belong to this target."""
        observation = replace(parse_avatar_evidence(evidence, username=username, pinned=True), evidence_id=evidence_id)
        with self.store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = self.store._run(cur, run_id, account)
            if run["generation"] != generation or run["state"] not in {"queued", "running"}:
                raise LostOwnership("Avatar source belongs to a superseded execution")
            cur.execute(
                """select target_key from social.instagram_ingestion_targets
                where run_id=%s and stage='media' and target_key=%s and owner_token=%s
                and owner_started_at is not null and owner_expires_at>clock_timestamp()
                and outcome='unresolved' for update""",
                (run_id, target_key, owner_token),
            )
            if not cur.fetchone():
                raise LostOwnership("Avatar source writer lost media ownership")
            cur.execute(
                """select evidence_id from social.instagram_ingestion_evidence where evidence_id=%s
                and run_id=%s and stage='media' and target_key=%s and source_identity=%s
                and parser_version=%s and captured_at=%s and content_hash=%s and payload=%s""",
                (
                    evidence_id,
                    run_id,
                    target_key,
                    evidence.source_identity,
                    evidence.parser_version,
                    evidence.captured_at,
                    evidence.content_hash,
                    evidence.payload_json,
                ),
            )
            if not cur.fetchone():
                raise AvatarResolutionError("avatar_retained_evidence_required")
            cur.execute(
                "select pg_advisory_xact_lock(hashtextextended(%s,0))", ("instagram:avatar:" + observation.username,)
            )
            cur.execute(
                """select * from social.avatar_registry where platform='instagram' and account_handle=%s
                order by last_checked_at desc nulls last for update""",
                (observation.username,),
            )
            rows = list(cur.fetchall())
            if any(row["last_checked_at"] and row["last_checked_at"] > observation.observed_at for row in rows):
                raise AvatarResolutionError("avatar_source_newer")
            current = next((row for row in rows if row["source_url"] == observation.source_url), None)
            status = "unsupported" if observation.unavailable else "pending"
            reason = "profile_pic_not_available" if observation.unavailable else None
            if current:
                # A repeated profile observation cannot unhost an existing image.
                if not observation.unavailable and current["status"] == "mirrored" and current["hosted_url"]:
                    status = "mirrored"
                cur.execute(
                    """update social.avatar_registry set status=%s,failure_reason=%s,
                    last_checked_at=%s,updated_at=%s where id=%s""",
                    (status, reason, observation.observed_at, observation.observed_at, current["id"]),
                )
            else:
                # NULL source URLs do not conflict under the legacy unique key.
                # The per-handle advisory lock + explicit lookup deduplicates them.
                cur.execute(
                    """insert into social.avatar_registry(platform,account_handle,source_url,status,
                    failure_reason,last_checked_at,updated_at) values('instagram',%s,%s,%s,%s,%s,%s)""",
                    (
                        observation.username,
                        observation.source_url,
                        status,
                        reason,
                        observation.observed_at,
                        observation.observed_at,
                    ),
                )
        return observation

    def execute(
        self,
        *,
        run_id: str,
        account: str,
        generation: int,
        target_key: str,
        owner_token: str,
        handles: Sequence[str],
        fetch: Callable,
    ) -> tuple[AvatarObservation, ...]:
        """Resolve bounded identities supplied by the sealed inventory loader.

        fetch(username=..., pre_source_request=...) must send exactly one native
        profile HTTP request with retries=0. The callback belongs immediately
        before its send, after all source/session setup, outside a DB transaction.
        """
        owned = {
            "run_id": run_id,
            "account": account,
            "generation": generation,
            "target_key": target_key,
            "owner_token": owner_token,
        }
        fenced = {**owned, "stage": Stage.MEDIA}
        usernames = tuple(dict.fromkeys(normalize_handle(value) for value in handles))
        results = []
        success = False
        try:
            self.store.check_owner(**fenced)
            for username in usernames:
                self.store.check_owner(**fenced)
                permit = None
                request_count = 0

                def before_request():
                    nonlocal permit, request_count
                    if request_count:
                        raise AvatarResolutionError("avatar_hidden_retry_forbidden")
                    self.store.check_owner(**fenced)
                    permit = self.gate.before_request()
                    self.store.check_owner(**fenced, record_request=True)
                    request_count += 1

                try:
                    payload = fetch(username=username, pre_source_request=before_request)
                except (LostOwnership, IdentityDeferred):
                    raise
                except Exception as exc:
                    if permit is not None:
                        code = getattr(exc, "error_code", "request_failed")
                        failure = (
                            self.gate.record_content_absence(code, permit=permit)
                            if is_content_absence(error=exc)
                            else self.gate.record_failure(
                                code,
                                retry_after_seconds=getattr(exc, "retry_after_seconds", None),
                                permit=permit,
                            )
                        )
                        raise AvatarResolutionError(failure.code, failure=failure) from None
                    raise AvatarResolutionError("avatar_request_boundary_missing") from None
                if request_count != 1 or not isinstance(payload, dict):
                    raise AvatarResolutionError("avatar_request_boundary_missing")
                data = payload.get("data")
                user = data.get("user") if isinstance(data, dict) else None
                explicit_missing = (
                    isinstance(user, dict)
                    and any(key in user for key in _AVATAR_FIELDS)
                    and all(user.get(key) is None or user.get(key) == "" for key in _AVATAR_FIELDS)
                )
                evidence = Evidence.capture(
                    source_identity=AVATAR_SOURCE_PREFIX + username,
                    captured_at=datetime.now(UTC),
                    parser_version=AVATAR_PARSER_VERSION,
                    payload={**payload, "avatar_url_explicitly_missing": explicit_missing},
                )
                evidence_id = self.store.retain_evidence(**fenced, evidence=evidence)
                try:
                    # Validate the raw URL as well as sanitized evidence. The
                    # signed URL remains an ephemeral download input only.
                    fresh_url = _profile(payload, username)
                    observation = self.apply_evidence(
                        **owned, username=username, evidence=evidence, evidence_id=evidence_id
                    )
                except AvatarResolutionError as exc:
                    if exc.code != "avatar_source_newer":
                        health = (
                            self.gate.record_content_absence("parse_error", permit=permit)
                            if is_content_absence(payload=payload)
                            else self.gate.record_failure("parse_error", permit=permit)
                        )
                        raise AvatarResolutionError(
                            exc.code,
                            failure=Failure(
                                exc.code, retry_at=health.retry_at, identity_blocked=health.identity_blocked
                            ),
                        ) from None
                    raise
                self.gate.record_success(permit)
                results.append(replace(observation, source_url=fresh_url))
            success = True
            return tuple(results)
        finally:
            try:
                if success and results:
                    self.gate.finish_success()
                else:
                    self.gate.finish_failure()
            except Exception:
                # Never replace the original source/fence failure with cleanup.
                LOGGER.warning("ingestion_avatar_gate_cleanup_failed")
