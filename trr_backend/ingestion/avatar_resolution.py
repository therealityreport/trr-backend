"""Counted profile observations for an already-owned Instagram media target.

Legacy sources perform one explicit request. The reviewed public GraphQL
source performs one bootstrap and one POST, without retries or credential repair.
No HTTP client or credential snapshot is retained here.
Profile evidence can also be parsed/applied offline; resolving an avatar never
settles the media receipt or proves that its image has been hosted.
"""

from __future__ import annotations

import logging
import math
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
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
        self.failure = failure if failure is not None else Failure(code)
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
    validated_native_id: str | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True)
class AvatarCapture:
    observation: AvatarObservation
    evidence: Evidence = field(repr=False)
    evidence_id: str
    expected_id: str | None = None


class _AvatarControlAbort(BaseException):
    """Admission/fence failures must never become remote source failures."""

    def __init__(self, error: Exception):
        self.error = error
        super().__init__("Avatar control callback interrupted source transport")


def _canonical_avatar_payload(payload):
    """Keep delivery queries in memory, outside retained avatar source proof."""
    result = dict(payload)
    data = result.get("data")
    if isinstance(data, dict) and isinstance(data.get("user"), dict):
        user = dict(data["user"])
        for key in _AVATAR_FIELDS:
            value = user.get(key)
            if isinstance(value, str) and value:
                try:
                    user[key] = source_reference(value)
                except ValueError:
                    # Retain a failed native field for the strict parser, never
                    # turn an invalid value into explicit-unavailable evidence.
                    user[key] = value
        result["data"] = {**data, "user": user}
    return result


def _maximum_server_delay(error, source):
    """Match the gate's numeric contract, including finite huge integers."""
    delays = []
    for value in (getattr(error, "retry_after_seconds", None), getattr(source, "retry_after_seconds", None)):
        if type(value) not in (int, float) or not isinstance(value, (int, float)) or value < 0:
            continue
        try:
            if not math.isfinite(value):
                continue
        except OverflowError:
            # A finite integer can exceed float range; leave timestamp
            # saturation to the existing gate rather than retrying early.
            pass
        delays.append(value)
    return max(delays) if delays else None


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


def parse_avatar_evidence(
    evidence: Evidence, *, username: str, pinned: bool = False, expected_id: str | None = None
) -> AvatarObservation:
    """Pure parser: original timestamps and sanitized paths survive replay."""
    username = normalize_handle(username)
    from trr_backend.ingestion.profile_transport import PROFILE_GRAPHQL_PARSER
    from trr_backend.ingestion.profiles import ProfileError, parse_profile, profile_target

    if evidence.parser_version == PROFILE_GRAPHQL_PARSER:
        if expected_id is None:
            raise AvatarResolutionError("avatar_identity_unresolved")
        try:
            observed = parse_profile(
                evidence,
                target_key=profile_target(expected_id, username),
                expected_id=expected_id,
                username=username,
                pinned=pinned,
            )
        except ProfileError as error:
            raise AvatarResolutionError(error.code) from None
        # Profile metadata permits ID-bound renames. A media role names an exact
        # handle, so it cannot borrow an avatar from a renamed/reused identity.
        if observed.username != username:
            raise AvatarResolutionError("avatar_identity_mismatch")
        payload = evidence.read(pinned=pinned)
        user = payload["data"]["user"]
        if not any(key in user for key in _AVATAR_FIELDS):
            raise AvatarResolutionError("avatar_profile_incomplete")
        if observed.avatar_url is None and payload.get("avatar_url_explicitly_missing") is not True:
            raise AvatarResolutionError("avatar_unavailable_proof_missing")
        return AvatarObservation(username, evidence.captured_at, observed.avatar_url, observed.avatar_url is None)
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
        expected_id: str | None = None,
    ) -> AvatarObservation:
        """Offline fenced write; evidence must already belong to this target."""
        observation = replace(
            parse_avatar_evidence(evidence, username=username, pinned=True, expected_id=expected_id),
            evidence_id=evidence_id,
        )
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

    def _preserve_abort_hold(self, *, owned, source, error, permits):
        """Save observed waits without health penalties or masking the abort.

        The existing finished-gate path only extends waits. It does not count a
        second failure or clear a newer hold/probe. Loss of the business fence
        or any cleanup/database error leaves the original exception intact.
        """
        try:
            delay = _maximum_server_delay(error, source)
            if delay is None or not permits:
                return
            fenced: dict[str, Any] = {**owned, "stage": Stage.MEDIA}
            self.store.check_owner(**fenced)
            permit = permits[-1]
            source_delay = _maximum_server_delay(None, source)
            header_permit = getattr(source, "_retry_after_permit", None)
            if source_delay == delay and header_permit is not None:
                permit = header_permit
            self.gate.finish_failure()
            self.gate.finish_failure("target_deadline", retry_after_seconds=delay, permit=permit)
        except BaseException:
            # This is best-effort cleanup of already received metadata. Never
            # replace a control, persistence, cancellation or source exception.
            LOGGER.warning("ingestion_avatar_abort_hold_save_failed")

    def observe(
        self,
        *,
        owned: Mapping[str, Any],
        username: str,
        source: Callable,
        on_request: Callable | None = None,
        check_cancelled: Callable | None = None,
        clock: Callable = lambda: datetime.now(UTC),
        canonical_urls: bool = False,
    ) -> AvatarCapture:
        """Count, validate and retain one profile without registry/target writes.

        The caller owns the private source lifecycle and gate finalization. All
        request-control exceptions retain their origin across provider handling.
        """
        from trr_backend.ingestion.profile_transport import PROFILE_GRAPHQL_PARSER
        from trr_backend.ingestion.profiles import ProfileError, native_id, profile_target, project_profile_payload

        username = normalize_handle(username)
        fenced = {**owned, "stage": Stage.MEDIA}
        parser = getattr(source, "parser_version", AVATAR_PARSER_VERSION)
        graphql = parser == PROFILE_GRAPHQL_PARSER
        budget = 2 if graphql else 1
        if parser not in (AVATAR_PARSER_VERSION, PROFILE_GRAPHQL_PARSER) or (
            type(getattr(source, "request_budget", 1)) is not int or getattr(source, "request_budget", 1) != budget
        ):
            raise AvatarResolutionError("avatar_request_boundary_missing")
        expected_id = native_id(getattr(source, "user_id", None)) if graphql else None
        permits = []
        request_count = 0
        phase = "admission"

        def check():
            self.store.check_owner(**fenced)
            if check_cancelled is not None:
                check_cancelled()

        def before_request():
            nonlocal request_count
            try:
                if request_count >= budget:
                    raise AvatarResolutionError("avatar_hidden_retry_forbidden")
                check()
                if graphql and request_count == 1:
                    # Only the fixed source's validated bootstrap reaches POST.
                    self.gate.record_success(permits[0])
                permit = self.gate.before_request()
                permits.append(permit)
                if on_request is None:
                    self.store.check_owner(**fenced, record_request=True)
                else:
                    on_request()
                request_count += 1
                return permit
            except Exception as error:
                raise _AvatarControlAbort(error) from None

        payload = None
        try:
            try:
                phase = "request"
                payload = source(username=username, pre_source_request=before_request)
                phase = "projection"
                projection = (
                    project_profile_payload(
                        payload, parser_version=parser, provenance=getattr(source, "provenance", None)
                    )
                    if graphql
                    else payload
                )
            except _AvatarControlAbort as abort:
                raise abort.error from None
            except (LostOwnership, IdentityDeferred):
                raise
            except Exception as error:
                supplied = getattr(error, "failure", None)
                if getattr(supplied, "code", None) in {"target_deadline", "avatar_request_deadline"}:
                    raise AvatarResolutionError("target_deadline", failure=Failure("target_deadline")) from None
                if not permits:
                    raise AvatarResolutionError("avatar_request_boundary_missing") from None
                profile_parse_error = graphql and isinstance(error, ProfileError) and not hasattr(error, "error_code")
                code = getattr(error, "error_code", "parse_error" if profile_parse_error else "request_failed")
                delay = _maximum_server_delay(error, source)
                health = (
                    self.gate.record_content_absence(code, permit=permits[-1])
                    if delay is None and is_content_absence(error=error)
                    else self.gate.record_failure(code, retry_after_seconds=delay, permit=permits[-1])
                )
                if profile_parse_error and isinstance(error, ProfileError):
                    failure = Failure(error.code, retry_at=health.retry_at, identity_blocked=health.identity_blocked)
                    raise AvatarResolutionError(error.code, failure=failure) from None
                raise AvatarResolutionError(health.code, failure=health) from None
            # A local source contract violation is not egress-health evidence.
            phase = "source_contract"
            if request_count != budget or not isinstance(projection, dict):
                raise AvatarResolutionError("avatar_request_boundary_missing")
            data = projection.get("data")
            user = data.get("user") if isinstance(data, dict) else None
            explicit_missing = (
                isinstance(user, dict)
                and any(key in user for key in _AVATAR_FIELDS)
                and all(user.get(key) is None or user.get(key) == "" for key in _AVATAR_FIELDS)
            )
            evidence = Evidence.capture(
                source_identity=("instagram:commenter-profile:" + profile_target(expected_id, username))
                if graphql
                else AVATAR_SOURCE_PREFIX + username,
                captured_at=clock(),
                parser_version=parser,
                payload=(
                    _canonical_avatar_payload({**projection, "avatar_url_explicitly_missing": explicit_missing})
                    if graphql or canonical_urls
                    else {**projection, "avatar_url_explicitly_missing": explicit_missing}
                ),
            )
            evidence_id = self.store.retain_evidence(**fenced, evidence=evidence)
            try:
                phase = "parse"
                fresh_url = _profile(payload, username) if not graphql else None
                observation = replace(
                    parse_avatar_evidence(
                        evidence,
                        username=username,
                        pinned=True,
                        expected_id=expected_id,
                    ),
                    evidence_id=evidence_id,
                )
                if graphql:
                    if not isinstance(user, dict):
                        raise AvatarResolutionError("avatar_profile_incomplete")
                    fresh_url = next((user.get(key) for key in _AVATAR_FIELDS if user.get(key)), None)
                    if fresh_url is not None:
                        validate_media_url(fresh_url, policy=_POLICY)
                        source_reference(fresh_url)
            except AvatarResolutionError as error:
                delay = _maximum_server_delay(error, source)
                health = (
                    self.gate.record_content_absence("parse_error", permit=permits[-1])
                    if delay is None and is_content_absence(payload=payload)
                    else self.gate.record_failure("parse_error", retry_after_seconds=delay, permit=permits[-1])
                )
                raise AvatarResolutionError(
                    error.code,
                    failure=Failure(
                        error.code,
                        retry_at=health.retry_at,
                        identity_blocked=health.identity_blocked,
                    ),
                ) from None
            check()
            self.gate.record_success(permits[-1])
            return AvatarCapture(
                replace(observation, source_url=fresh_url, validated_native_id=expected_id),
                evidence,
                evidence_id,
                expected_id,
            )
        except BaseException as error:
            self._preserve_abort_hold(owned=owned, source=source, error=error, permits=permits)
            if isinstance(error, (AvatarResolutionError, IdentityDeferred)) and permits:
                # Auxiliary diagnostics cannot change admission, health, the
                # original failure, or a lost/persistence fence. They never
                # stand in for a successful native profile observation.
                try:
                    from trr_backend.ingestion.profile_failure_diagnostics import PARSER_VERSION, failure_payload

                    diagnostic = failure_payload(
                        source=source,
                        username=username,
                        user_id=expected_id,
                        count=request_count,
                        phase=phase,
                        error=error,
                        payload=payload,
                    )
                    if diagnostic is not None:
                        captured_at = datetime.fromisoformat(diagnostic["basis"][-1]["observed_at"])
                        diagnostic_evidence = Evidence.capture(
                            source_identity=f"instagram:public-profile-failure:{username}",
                            captured_at=captured_at,
                            parser_version=PARSER_VERSION,
                            payload=diagnostic,
                        )
                        self.store.retain_evidence(**fenced, evidence=diagnostic_evidence)
                except BaseException:
                    LOGGER.warning("ingestion_profile_failure_evidence_save_failed")
            raise

    def execute(
        self,
        *,
        run_id: str,
        account: str,
        generation: int,
        target_key: str,
        owner_token: str,
        handles: Sequence[str],
        fetch: Callable | None = None,
        source_factory: Callable | None = None,
    ) -> tuple[AvatarObservation, ...]:
        """Apply fresh observations under the same media owner, without settlement."""
        if (fetch is None) == (source_factory is None):
            raise AvatarResolutionError("avatar_source_boundary_invalid")
        owned = {
            "run_id": run_id,
            "account": account,
            "generation": generation,
            "target_key": target_key,
            "owner_token": owner_token,
        }
        usernames = tuple(dict.fromkeys(normalize_handle(value) for value in handles))
        results = []
        success = False
        try:
            self.store.check_owner(**owned, stage=Stage.MEDIA)
            for username in usernames:
                self.store.check_owner(**owned, stage=Stage.MEDIA)
                source: Any
                if source_factory is not None:
                    source = source_factory(username)
                elif fetch is not None:
                    source = fetch
                else:
                    raise AvatarResolutionError("avatar_source_boundary_invalid")
                try:
                    capture = self.observe(owned=owned, username=username, source=source)
                    observation = self.apply_evidence(
                        **owned,
                        username=username,
                        evidence=capture.evidence,
                        evidence_id=capture.evidence_id,
                        expected_id=capture.expected_id,
                    )
                    results.append(
                        replace(
                            observation,
                            source_url=capture.observation.source_url,
                            validated_native_id=capture.expected_id,
                        )
                    )
                finally:
                    if source_factory is not None:
                        active_error = sys.exc_info()[0] is not None
                        try:
                            source.close()
                        except Exception:
                            if not active_error:
                                raise
                            LOGGER.warning("ingestion_avatar_source_cleanup_failed")
            success = True
            return tuple(results)
        finally:
            try:
                self.gate.finish_success() if success and results else self.gate.finish_failure()
            except Exception:
                LOGGER.warning("ingestion_avatar_gate_cleanup_failed")
