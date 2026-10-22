"""Immutable Instagram intent and completion rules; no runtime dependencies."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class Stage(StrEnum):
    LISTING = "listing"
    DETAILS = "post_details"
    COMMENTS = "comments"
    MEDIA = "media"


class Outcome(StrEnum):
    COMMITTED = "committed"
    CACHED = "cached_satisfied"
    UNAVAILABLE = "source_unavailable"
    UNRESOLVED = "unresolved"


class IngestionError(Exception):
    code = "INGESTION_ERROR"
    status = 400


class Conflict(IngestionError):  # noqa: N818 - domain exception vocabulary
    code = "INGESTION_CONFLICT"
    status = 409


class LostOwnership(Conflict):
    code = "INGESTION_OWNERSHIP_LOST"


class ReplayUnavailable(IngestionError):  # noqa: N818 - domain exception vocabulary
    code = "INGESTION_REPLAY_UNAVAILABLE"
    status = 410


def utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def fingerprint(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode()).hexdigest()


@dataclass(frozen=True)
class SourceProfile:
    """Frozen transport contract; cursors cannot move between source families."""

    access_mode: str
    listing_family: str
    details_family: str
    comments_family: str
    source_contract_version: int = 1
    adapter_version: int = 1

    def __post_init__(self) -> None:
        families = {
            "public": ("instagram_profile_graphql_public", "instagram_permalink_public", "instagram_public_relay"),
            "authenticated": (
                "instagram_posts_authenticated",
                "instagram_detail_authenticated",
                "instagram_comments_rest",
            ),
        }
        ordered_public = (
            self.access_mode == "public"
            and (self.listing_family, self.details_family, self.comments_family)
            == ("instagram_profile_ordered_public", "instagram_permalink_public", "instagram_public_relay")
            and type(self.source_contract_version) is int
            and self.source_contract_version == 2
        )
        if (
            self.access_mode not in families
            or (
                not ordered_public
                and (self.listing_family, self.details_family, self.comments_family) != families[self.access_mode]
            )
            or type(self.source_contract_version) is not int
            or (self.source_contract_version != 1 and not ordered_public)
            or type(self.adapter_version) is not int
            or self.adapter_version != 1
        ):
            raise ValueError("Unsupported frozen Instagram source profile")

    @classmethod
    def for_mode(cls, access_mode: str) -> SourceProfile:
        if access_mode == "public":
            return cls(
                access_mode, "instagram_profile_graphql_public", "instagram_permalink_public", "instagram_public_relay"
            )
        if access_mode == "authenticated":
            return cls(
                access_mode,
                "instagram_posts_authenticated",
                "instagram_detail_authenticated",
                "instagram_comments_rest",
            )
        raise ValueError("Unsupported Instagram access mode")

    @classmethod
    def for_public_ordered(cls) -> SourceProfile:
        """Explicit root-start contract; never a changed public-mode default."""
        return cls(
            "public",
            "instagram_profile_ordered_public",
            "instagram_permalink_public",
            "instagram_public_relay",
            source_contract_version=2,
        )


@dataclass(frozen=True)
class RunIntent:
    account: str
    action: str = "backfill"
    source_scope: str = "network"
    date_start: datetime | None = None
    date_end: datetime | None = None
    selected_tasks: tuple[Stage, ...] = (Stage.DETAILS, Stage.COMMENTS, Stage.MEDIA)
    scope: str = "full_history"
    metadata_ttl_days: int = 30
    metrics_ttl_hours: int = 24
    force: bool = False
    policy_version: int = 1
    detail_worker_count: int | None = None
    comments_worker_count: int | None = None
    comments_enable_media_followups: bool = True
    enable_cap4_canary: bool = False
    listing_max_attempts: int = 12
    comments_max_attempts: int = 12
    media_max_attempts: int = 3
    source_profile: SourceProfile | None = None

    def __post_init__(self) -> None:
        if self.source_profile is not None and not isinstance(self.source_profile, SourceProfile):
            raise ValueError("Source profile must be an immutable SourceProfile")
        account = self.account.strip().lstrip("@").lower()
        if not re.fullmatch(r"[a-z0-9._]{1,30}", account):
            raise ValueError("Invalid Instagram account handle")
        object.__setattr__(self, "account", account)
        scope = "network" if self.source_scope == "bravo" else self.source_scope
        if scope not in {"network", "creator", "community", "news"}:
            raise ValueError("Invalid source scope")
        object.__setattr__(self, "source_scope", scope)
        if self.action not in {"backfill", "sync_recent", "sync_newer", "resume_tail"}:
            raise ValueError("Invalid catalog action")
        if self.scope not in {"full_history", "bounded_window", "recent_window", "head_gap", "frontier_resume"}:
            raise ValueError("Invalid catalog action scope")
        tasks = tuple(Stage(task) for task in self.selected_tasks)
        if not tasks or Stage.LISTING in tasks:
            raise ValueError("Select at least one downstream task; listing is implicit")
        object.__setattr__(self, "selected_tasks", tuple(stage for stage in Stage if stage in tasks))
        for field in ("date_start", "date_end"):
            timestamp = getattr(self, field)
            if timestamp is not None:
                object.__setattr__(self, field, datetime.fromisoformat(utc_timestamp(timestamp)))
        if self.scope == "full_history":
            object.__setattr__(self, "date_start", None)
            object.__setattr__(self, "date_end", None)
        if self.date_start and self.date_end and self.date_start >= self.date_end:
            raise ValueError("date_start must precede date_end")
        if self.source_profile == SourceProfile.for_public_ordered() and (
            self.scope != "bounded_window" or self.date_start is None or self.date_end is None
        ):
            raise ValueError("Ordered public listing requires an explicit bounded date window")
        if not 1 <= self.metadata_ttl_days <= 365 or self.metrics_ttl_hours != 24 or self.policy_version != 1:
            raise ValueError("Unsupported freshness policy")
        if self.detail_worker_count is not None and not 1 <= self.detail_worker_count <= 12:
            raise ValueError("Invalid requested detail worker count")
        if self.comments_worker_count is not None and not 1 <= self.comments_worker_count <= 24:
            raise ValueError("Invalid requested comments worker count")
        if self.listing_max_attempts < 1 or not 1 <= self.comments_max_attempts <= 12 or self.media_max_attempts != 3:
            raise ValueError("Invalid frozen lane retry policy")

    def payload(self) -> dict[str, Any]:
        result = asdict(self)
        # Untagged v1 runs used authenticated adapters. Preserve their exact
        # serialization and digest; a new default must not rewrite old intent.
        if self.source_profile is None:
            result.pop("source_profile")
        result["platform"] = "instagram"
        result["selected_tasks"] = [task.value for task in self.selected_tasks]
        for field in ("date_start", "date_end"):
            value = getattr(self, field)
            result[field] = utc_timestamp(value) if value else None
        return result

    @property
    def effective_source_profile(self) -> SourceProfile:
        return self.source_profile or SourceProfile.for_mode("authenticated")

    @property
    def digest(self) -> str:
        return fingerprint(self.payload())

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> RunIntent:
        data = dict(payload)
        if data.pop("platform", "instagram") != "instagram":
            raise ValueError("Only Instagram is supported")
        for key in ("date_start", "date_end"):
            if isinstance(data.get(key), str):
                data[key] = datetime.fromisoformat(data[key].replace("Z", "+00:00"))
        if "selected_tasks" in data:
            data["selected_tasks"] = tuple(Stage(task) for task in data["selected_tasks"])
        if "source_profile" in data:
            if not isinstance(data["source_profile"], dict):
                raise ValueError("Invalid frozen Instagram source profile")
            data["source_profile"] = SourceProfile(**data["source_profile"])
        return cls(**data)


@dataclass(frozen=True)
class StageCounts:
    total: int
    committed: int
    cached_satisfied: int
    source_unavailable: int
    unresolved: int
    discovery_complete: bool

    def __post_init__(self) -> None:
        counts = (self.total, self.committed, self.cached_satisfied, self.source_unavailable, self.unresolved)
        if any(type(count) is not int or count < 0 for count in counts):
            raise ValueError("Outcome counts must be non-negative integers")
        if self.total != sum(counts[1:]):
            raise ValueError("Outcome counts do not balance")

    @property
    def complete(self) -> bool:
        return self.discovery_complete and self.unresolved == 0


@dataclass(frozen=True)
class Failure:
    code: str
    retry_at: datetime | None = None
    identity_blocked: bool = False
    source_unavailable: bool = False
    evidence_kind: str | None = None

    def __post_init__(self) -> None:
        if self.retry_at is not None:
            utc_timestamp(self.retry_at)
        if self.source_unavailable and (
            self.identity_blocked
            or self.retry_at is not None
            or self.evidence_kind
            not in {"deleted", "comments_disabled", "restricted", "media_unavailable", "pagination_exhausted"}
        ):
            raise ValueError("Source unavailability requires explicit source evidence, not an operational failure")


def run_complete(intent: RunIntent, stages: dict[Stage, StageCounts]) -> bool:
    required = (Stage.LISTING, *intent.selected_tasks)
    return all(stage in stages and stages[stage].complete for stage in required)
