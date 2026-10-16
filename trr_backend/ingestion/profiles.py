"""Public commenter metadata: bounded observations, coherent cache and fenced work.

Only the public profile response is projected. Timeline/relationship content and
viewer/session state never enter profile evidence or the canonical projection.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any
from uuid import UUID

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, Failure, LostOwnership, Outcome, RunIntent, Stage, utc_timestamp
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.media import source_reference
from trr_backend.ingestion.persistence import Store, _row
from trr_backend.socials.media_url_safety import MediaUrlSafetyPolicy, allowed_hosts_for_platform, validate_media_url

PROFILE_VERSION = 1
PROFILE_PARSER = "instagram-public-commenter-profile-v1"
PROFILE_WORKFLOW_VERSION = "instagram-ingestion-v2"
PROFILE_KIND = "commenter_profile"
PROFILE_PREFIX = "profile:"
PROFILE_PROOF_KEY = "ingestion_public_profile"
_POLICY = MediaUrlSafetyPolicy(allowed_hosts_for_platform("instagram"), allow_test_hosts=False, resolve_dns=False)
LOGGER = logging.getLogger(__name__)
_ALIASES = {
    "full_name": ("full_name",),
    "biography": ("biography",),
    "followers_count": ("followers_count", "follower_count", "edge_followed_by"),
    "follows_count": ("follows_count", "following_count", "edge_follow"),
    "posts_count": ("posts_count", "media_count", "edge_owner_to_timeline_media"),
    "is_verified": ("is_verified",),
    "is_private": ("is_private",),
    "profile_pic_url": ("profile_pic_url",),
    "profile_pic_url_hd": ("profile_pic_url_hd",),
}
_COUNT_FIELDS = frozenset({"followers_count", "follows_count", "posts_count"})
_BOOL_FIELDS = frozenset({"is_verified", "is_private"})


class ProfileError(ValueError):
    def __init__(self, code):
        self.code = code
        self.failure = Failure(code, retry_at=datetime.now(UTC) + timedelta(seconds=60))
        super().__init__(code)


def handle(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9._]{1,30}", value):
        raise ProfileError("profile_identity_unresolved")
    return value.lower()


def native_id(value):
    if type(value) is int:
        value = str(value)
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,64}", value):
        raise ProfileError("profile_identity_unresolved")
    return value


def profile_target(user_id, username):
    if user_id is not None and user_id != "":
        return PROFILE_PREFIX + "id:" + native_id(user_id)
    return PROFILE_PREFIX + "handle:" + handle(username)


def requested(intent: RunIntent, workflow_version: str) -> bool:
    return (
        workflow_version == PROFILE_WORKFLOW_VERSION
        and intent.effective_source_profile.access_mode == "public"
        and Stage.COMMENTS in intent.selected_tasks
        and Stage.MEDIA in intent.selected_tasks
        and intent.comments_enable_media_followups
    )


def project_profile_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Preserve reviewed scalar metadata and count presence, never posts/edges."""
    data = payload.get("data") if isinstance(payload, Mapping) else None
    user = data.get("user") if isinstance(data, Mapping) else None
    projected, invalid = {}, []
    if isinstance(user, Mapping):
        for name in ("id", "pk", "user_id", "username"):
            if name in user:
                projected[name] = user[name] if type(user[name]) in (str, int) or user[name] is None else None
                if user[name] is not None and type(user[name]) not in (str, int):
                    invalid.append(name)
        for name, aliases in _ALIASES.items():
            alias = next((key for key in aliases if key in user), None)
            if alias is None:
                continue
            value = user[alias]
            if name in _COUNT_FIELDS and isinstance(value, Mapping):
                value = value.get("count")
            valid = value is None or (
                type(value) is int and value >= 0
                if name in _COUNT_FIELDS
                else type(value) is bool
                if name in _BOOL_FIELDS
                else isinstance(value, str)
            )
            if valid and value and name.startswith("profile_pic_url"):
                try:
                    validate_media_url(value, policy=_POLICY)
                    source_reference(value)
                except ValueError:
                    valid = False
            projected[name] = value if valid else None
            if not valid:
                invalid.append(name)
    return {
        "status": payload.get("status") if isinstance(payload, Mapping) else None,
        "data": {"user": projected} if isinstance(user, Mapping) else {},
        "profile_projection_version": PROFILE_VERSION,
        "profile_invalid_fields": sorted(invalid),
    }


@dataclass(frozen=True)
class ProfileObservation:
    user_id: str
    username: str
    observed_at: datetime
    values: Mapping[str, Any] = field(repr=False)
    coverage: Mapping[str, str]

    def __post_init__(self):
        native_id(self.user_id)
        if handle(self.username) != self.username or self.observed_at.utcoffset() is None:
            raise ProfileError("profile_observation_invalid")
        if set(self.values) != {*_ALIASES, "url"} or set(self.coverage) != set(self.values):
            raise ProfileError("profile_projection_unknown_fields")
        for name, value in self.values.items():
            if name == "url":
                if value != f"https://www.instagram.com/{self.username}/" or self.coverage[name] != "derived":
                    raise ProfileError("profile_observation_invalid")
                continue
            if self.coverage[name] != ("unknown" if value is None else "observed"):
                raise ProfileError("profile_observation_invalid")
            if value and name.startswith("profile_pic_url"):
                try:
                    validate_media_url(value, policy=_POLICY)
                    source_reference(value)
                except ValueError:
                    raise ProfileError("profile_observation_invalid") from None
            if value is not None and not (
                type(value) is int and value >= 0
                if name in _COUNT_FIELDS
                else type(value) is bool
                if name in _BOOL_FIELDS
                else isinstance(value, str)
            ):
                raise ProfileError("profile_observation_invalid")
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))
        object.__setattr__(self, "coverage", MappingProxyType(dict(self.coverage)))

    @property
    def avatar_url(self):
        return self.values.get("profile_pic_url_hd") or self.values.get("profile_pic_url")


def parse_profile(
    evidence: Evidence, *, target_key: str, expected_id: str | None, username: str, pinned=False
) -> ProfileObservation:
    if (
        evidence.parser_version != PROFILE_PARSER
        or evidence.source_identity != "instagram:commenter-profile:" + target_key
    ):
        raise ProfileError("profile_evidence_identity_mismatch")
    value = evidence.read(pinned=pinned)
    user = (value.get("data") or {}).get("user")
    if (
        value.get("status") != "ok"
        or value.get("profile_projection_version") != PROFILE_VERSION
        or value.get("profile_invalid_fields")
        or not isinstance(user, dict)
    ):
        raise ProfileError("profile_source_capability_missing")
    if set(user) - {"id", "pk", "user_id", "username", *_ALIASES}:
        raise ProfileError("profile_projection_unknown_fields")
    identities = {native_id(user[name]) for name in ("id", "pk", "user_id") if user.get(name) is not None}
    if len(identities) != 1:
        raise ProfileError("profile_identity_mismatch")
    identity = identities.pop()
    actual_handle = handle(user.get("username"))
    if expected_id is not None:
        if identity != native_id(expected_id):
            raise ProfileError("profile_identity_mismatch")
    elif actual_handle != handle(username):
        raise ProfileError("profile_identity_mismatch")
    # Identity-bound rename is safe. A reused handle pointing at another native
    # ID is rejected above; no by-handle canonical merge occurs.
    values, coverage = {}, {}
    for name in _ALIASES:
        values[name] = user.get(name)
        coverage[name] = "observed" if name in user and user[name] is not None else "unknown"
    values["url"] = f"https://www.instagram.com/{actual_handle}/"
    coverage["url"] = "derived"
    return ProfileObservation(identity, actual_handle, evidence.captured_at, values, coverage)


def _proof(observation, evidence_id, *, complete, avatar):
    return {
        "version": PROFILE_VERSION,
        "access_mode": "public",
        "parser_version": PROFILE_PARSER,
        "evidence_id": str(UUID(str(evidence_id))),
        "observed_at": utc_timestamp(observation.observed_at),
        "user_id": observation.user_id,
        "username": observation.username,
        "collection_complete": complete,
        "coverage": dict(observation.coverage),
        "avatar": avatar,
    }


def write_profile(conn, *, observation, evidence_id, run_id, complete=False, avatar=None):
    """Explicit canonical SQL; public proof only, no legacy payload hydration."""
    if conn.autocommit or observation.observed_at.utcoffset() is None:
        raise ValueError("Profile writes require an aware source time and explicit transaction")
    proof = _proof(observation, evidence_id, complete=complete, avatar=avatar)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select pg_advisory_xact_lock(hashtextextended(%s,0))", ("instagram:profile:" + observation.user_id,)
        )
        existing = _row(
            cur,
            """select id,last_scraped_at,raw_data->'ingestion_public_profile' as proof
            from social.instagram_profiles where profile_id=%s for update""",
            (observation.user_id,),
        )
        if existing and existing["last_scraped_at"] and existing["last_scraped_at"] > observation.observed_at:
            return {
                "profile_row_id": str(existing["id"]),
                "instagram_profile_id": observation.user_id,
                "applied": False,
            }
        if (
            existing
            and existing.get("proof")
            and existing["proof"].get("observed_at") == proof["observed_at"]
            and existing["proof"].get("evidence_id") != proof["evidence_id"]
        ):
            raise Conflict("Profile observation time has conflicting source evidence")
        # Each profile row projects one coherent capture. Missing optional
        # fields remain unknown, rather than borrowing stale values from an
        # earlier capture and misrepresenting the current coverage proof.
        updates = dict(observation.values)
        # Durable canonical references are queryless; fresh signed inputs exist
        # only in the activity's source envelope and bounded retained evidence.
        for name in ("profile_pic_url", "profile_pic_url_hd"):
            if updates.get(name):
                updates[name] = source_reference(updates[name])
        updates.update(
            username=observation.username,
            normalized_username=observation.username,
            source_account=observation.username,
            last_seen_at=observation.observed_at,
            last_scraped_at=observation.observed_at,
            updated_at=observation.observed_at,
            last_scrape_run_id=run_id,
        )
        updates["hosted_profile_pic_url"] = avatar.get("hosted_url") if avatar else None
        updates["hosted_profile_pic_url_hd"] = avatar.get("hosted_url") if avatar else None
        columns = list(updates)
        if existing:
            cur.execute(
                f"""update social.instagram_profiles set
                {",".join(name + "=%s" for name in columns)},
                raw_data=jsonb_set(coalesce(raw_data,'{{}}'::jsonb),'{{ingestion_public_profile}}',%s::jsonb)
                where id=%s returning id""",
                (*updates.values(), json.dumps(proof), str(existing["id"])),
            )
        else:
            cur.execute(
                f"""insert into social.instagram_profiles
                (profile_id,source_scope,{",".join(columns)},raw_data,first_seen_at)
                values(%s,'community',{",".join(["%s"] * len(columns))},%s::jsonb,%s) returning id""",
                (
                    observation.user_id,
                    *updates.values(),
                    json.dumps({PROFILE_PROOF_KEY: proof}),
                    observation.observed_at,
                ),
            )
        return {
            "profile_row_id": str(cur.fetchone()["id"]),
            "instagram_profile_id": observation.user_id,
            "username": observation.username,
            "applied": True,
        }


def cached_profile(store, *, expected_id, username, target_key, now):
    """Only a current coherent PUBLIC capture can authorize cache settlement."""
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """select id,profile_id,username,last_scraped_at,full_name,biography,followers_count,
            follows_count,posts_count,is_verified,is_private,profile_pic_url,profile_pic_url_hd,url,
            hosted_profile_pic_url,hosted_profile_pic_url_hd,raw_data->'ingestion_public_profile' as proof
            from social.instagram_profiles where profile_id=%s or
            (%s is null and normalized_username=%s) order by last_scraped_at desc nulls last limit 2""",
            (expected_id, expected_id, username),
        )
        rows = list(cur.fetchall())
        if len(rows) != 1:
            return None
        row, proof = rows[0], rows[0]["proof"]
        if (
            not isinstance(proof, dict)
            or proof.get("version") != PROFILE_VERSION
            or proof.get("access_mode") != "public"
            or proof.get("parser_version") != PROFILE_PARSER
            or proof.get("collection_complete") is not True
        ):
            return None
        try:
            evidence_id = str(UUID(proof["evidence_id"]))
        except (KeyError, ValueError, TypeError):
            return None
        retained = _row(
            cur,
            """select * from social.instagram_ingestion_evidence
            where evidence_id=%s and parser_version=%s and payload is not null""",
            (evidence_id, PROFILE_PARSER),
        )
    if not retained:
        return None
    evidence = Evidence(
        retained["source_identity"],
        retained["captured_at"],
        retained["parser_version"],
        retained["payload"],
        retained["content_hash"],
        retained["schema_version"],
    )
    try:
        original_target = evidence.source_identity.removeprefix("instagram:commenter-profile:")
        observation = parse_profile(
            evidence, target_key=original_target, expected_id=expected_id, username=username, pinned=True
        )
        age = now - observation.observed_at
        if not timedelta(0) <= age < timedelta(hours=24) or row["profile_id"] != observation.user_id:
            return None
        if (
            proof.get("user_id") != observation.user_id
            or proof.get("username") != observation.username
            or proof.get("observed_at") != utc_timestamp(observation.observed_at)
            or proof.get("coverage") != dict(observation.coverage)
            or row["username"] != observation.username
            or row["last_scraped_at"] != observation.observed_at
            or any(
                row[name] != (source_reference(value) if value and name.startswith("profile_pic_url") else value)
                for name, value in observation.values.items()
            )
        ):
            return None
        avatar = proof.get("avatar")
        if observation.avatar_url and (
            not isinstance(avatar, dict)
            or avatar.get("username") != observation.username
            or avatar.get("source_url") != source_reference(observation.avatar_url)
            or not re.fullmatch(r"[a-f0-9]{64}", avatar.get("content_hash", ""))
            or not isinstance(avatar.get("hosted_url"), str)
            or source_reference(avatar["hosted_url"]) != avatar["hosted_url"]
        ):
            return None
        if observation.avatar_url:
            avatar_time = datetime.fromisoformat(avatar["observed_at"].replace("Z", "+00:00"))
            if avatar_time.utcoffset() is None:
                return None
        expected_hosted = avatar["hosted_url"] if observation.avatar_url else None
        if any(row[name] != expected_hosted for name in ("hosted_profile_pic_url", "hosted_profile_pic_url_hd")):
            return None
        return observation, {
            "profile_row_id": str(row["id"]),
            "instagram_profile_id": observation.user_id,
            "username": observation.username,
            "profile_collection_complete": True,
            "coverage": dict(observation.coverage),
            "cache_source": {"evidence_id": evidence_id},
            "avatar": avatar,
        }
    except (ProfileError, ValueError, TypeError, KeyError, AttributeError):
        return None


def cached_avatar_version(store, observation):
    """Reuse exact hosted bytes after a fresh native-ID-bound profile capture.

    A handle's other image versions and metadata never authorize this reuse.
    The byte proof keeps its own observation time, independently of this capture.
    """
    if not observation.avatar_url:
        return None
    reference = source_reference(observation.avatar_url)
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """select source_url,hosted_url,content_hash,last_checked_at,status
            from social.avatar_registry where platform='instagram' and account_handle=%s
            and source_url is not null order by last_checked_at desc nulls last,id desc""",
            (observation.username,),
        )
        for row in cur.fetchall():
            try:
                if source_reference(row["source_url"]) != reference:
                    continue
                if (
                    row["status"] != "mirrored"
                    or not isinstance(row["hosted_url"], str)
                    or source_reference(row["hosted_url"]) != row["hosted_url"]
                    or not re.fullmatch(r"[a-f0-9]{64}", row["content_hash"] or "")
                    or row["last_checked_at"] is None
                ):
                    return None
                return {
                    "username": observation.username,
                    "source_url": reference,
                    "hosted_url": row["hosted_url"],
                    "content_hash": row["content_hash"],
                    "observed_at": utc_timestamp(row["last_checked_at"]),
                }
            except (TypeError, ValueError):
                continue
    return None


class ProfileActivity:
    def __init__(self, store: Store, *, clock=lambda: datetime.now(UTC)):
        self.store, self.clock = store, clock

    def execute(self, *, run_id, account, generation, target_key, owner_token, provider: Callable, storage: Callable):
        owned = {
            "run_id": run_id,
            "account": account,
            "generation": generation,
            "stage": Stage.MEDIA,
            "target_key": target_key,
            "owner_token": owner_token,
        }
        target = self.store.acquire(**owned)
        binding = target["checkpoint"]
        if binding.get("target_kind") != PROFILE_KIND or binding.get("version") != PROFILE_VERSION:
            raise ProfileError("profile_target_contract_invalid")
        username, expected_id = binding.get("username"), binding.get("user_id")
        if target_key != profile_target(expected_id, username):
            raise ProfileError("profile_target_identity_mismatch")
        cached = cached_profile(
            self.store, expected_id=expected_id, username=username, target_key=target_key, now=self.clock()
        )
        if cached:
            observation, identity = cached
            return self.store.commit_target(
                **owned, observed_at=observation.observed_at, outcome=Outcome.CACHED, write_canonical=lambda _: identity
            )
        try:
            username = handle(username)
        except ProfileError:
            error = ProfileError("profile_lookup_handle_unavailable")
            error.failure = Failure(error.code, identity_blocked=True)
            raise error from None
        source = gate = objects = None
        parsed = False
        sends = 0
        try:
            source, gate = provider()

            def before():
                nonlocal sends
                if sends:
                    raise ProfileError("profile_hidden_retry_forbidden")
                self.store.check_owner(**owned)
                gate.before_request()
                self.store.check_owner(**owned, record_request=True)
                sends += 1

            payload = source(username=username, pre_source_request=before)
            if sends != 1:
                raise ProfileError("profile_request_boundary_missing")
            projection = project_profile_payload(payload)
            evidence = Evidence.capture(
                source_identity="instagram:commenter-profile:" + target_key,
                captured_at=self.clock(),
                parser_version=PROFILE_PARSER,
                payload=projection,
            )
            evidence_id = self.store.retain_evidence(**owned, evidence=evidence)
            observation = parse_profile(
                evidence, target_key=target_key, expected_id=expected_id, username=username, pinned=True
            )
            gate.record_success()
            parsed = True
            # A valid partial metadata observation survives CDN/storage failure;
            # this is deliberately not a target settlement receipt.
            with self.store.transaction() as conn:
                self.store.check_owner(**owned, connection=conn)
                write_profile(conn, observation=observation, evidence_id=evidence_id, run_id=run_id)
            avatar = cached_avatar_version(self.store, observation)
            if observation.avatar_url and avatar is None:
                objects = storage()
                original = projection["data"]["user"]
                url = original.get("profile_pic_url_hd") or original.get("profile_pic_url")
                entry, _ = objects.host(
                    url,
                    root="",
                    name="",
                    avatar=observation.username,
                    check=lambda: self.store.check_owner(**owned),
                    request=lambda: self.store.check_owner(**owned, record_request=True),
                )
                avatar = {
                    "username": observation.username,
                    "source_url": source_reference(url),
                    "hosted_url": entry["url"],
                    "content_hash": entry["sha256"],
                    "observed_at": utc_timestamp(observation.observed_at),
                }

            def write(conn):
                proof = avatar
                if avatar:
                    from trr_backend.ingestion.media import write_avatar_version

                    proof = write_avatar_version(conn, **dict(avatar, observed_at=observation.observed_at))
                    proof = dict(proof, observed_at=utc_timestamp(proof["observed_at"]))
                identity = write_profile(
                    conn, observation=observation, evidence_id=evidence_id, run_id=run_id, complete=True, avatar=proof
                )
                return dict(
                    identity, profile_collection_complete=True, coverage=dict(observation.coverage), avatar=proof
                )

            return self.store.commit_target(
                **owned, observed_at=observation.observed_at, evidence_id=evidence_id, write_canonical=write
            )
        except Exception as error:
            if gate is not None and not parsed:
                from trr_backend.ingestion.identity_policy import IdentityDeferred
                from trr_backend.ingestion.post_transport import SourceFailure

                if not isinstance(error, (LostOwnership, IdentityDeferred)):
                    if sends == 0:
                        gate.finish_failure()
                        gate = None
                        raise
                    failure = gate.finish_failure(
                        getattr(error, "error_code", "parse_error"),
                        retry_after_seconds=getattr(error, "retry_after_seconds", None),
                    )
                    gate = None
                    if isinstance(error, ProfileError):
                        failure = Failure(
                            error.code, retry_at=failure.retry_at, identity_blocked=failure.identity_blocked
                        )
                    raise SourceFailure(failure) from None
            raise
        finally:
            if gate is not None:
                try:
                    gate.finish_success() if parsed else gate.finish_failure()
                except Exception:
                    LOGGER.warning("ingestion_profile_gate_cleanup_failed")
            for owner in (source, getattr(objects, "client", None)):
                if owner is not None:
                    try:
                        close = getattr(owner, "close", None)
                        if close:
                            close()
                    except Exception:
                        LOGGER.warning("ingestion_profile_source_cleanup_failed")
