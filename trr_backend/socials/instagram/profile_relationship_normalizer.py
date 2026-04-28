"""Shared Instagram profile-relationship normalizer.

The Phase 3 contract only accepts following rows. Follower-list payloads are
returned as classified mismatches so callers cannot accidentally persist them.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

NORMALIZER_VERSION = "instagram-profile-relationship-normalizer-v1"

_FOLLOWING_VALUES = {"following", "follows"}
_FOLLOWER_VALUES = {"followers", "follower"}


@dataclass(slots=True)
class InstagramProfileRelationship:
    owner_username: str
    related_username: str
    relationship_type: str = "following"
    normalizer_version: str = NORMALIZER_VERSION
    related_user_id: str | None = None
    related_full_name: str | None = None
    related_profile_pic_url: str | None = None
    related_is_private: bool | None = None
    related_is_verified: bool | None = None
    source_type: str | None = None
    source_rank: int | None = None
    source_cursor: str | None = None
    source_page_ordinal: int | None = None
    raw_data: dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class InstagramProfileRelationshipMismatch:
    row_index: int
    code: str
    message: str
    source_relationship_type: str | None
    intended_relationship_type: str
    raw_data: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(slots=True)
class InstagramProfileRelationshipNormalizationResult:
    relationships: list[InstagramProfileRelationship] = field(default_factory=list)
    mismatches: list[InstagramProfileRelationshipMismatch] = field(default_factory=list)
    page_info: dict[str, Any] = field(default_factory=dict)
    normalizer_version: str = NORMALIZER_VERSION


def normalize_instagram_profile_relationships(
    payload: dict[str, Any] | list[dict[str, Any]],
    *,
    owner_username: str,
    intended_relationship_type: str,
    source_cursor: str | None = None,
    source_page_ordinal: int | None = None,
) -> InstagramProfileRelationshipNormalizationResult:
    """Normalize one page of Instagram profile relationships.

    `intended_relationship_type` is required so callers make the scrape
    direction explicit. Only following rows are returned for persistence.
    """

    intended = _canonical_intended_relationship(intended_relationship_type)
    rows = _extract_rows(payload)
    page_info = _extract_page_info(payload)
    result = InstagramProfileRelationshipNormalizationResult(page_info=page_info)

    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        relationship_type = _source_relationship_type(row)
        accepted, mismatch_code = _classify_source_relationship(relationship_type)
        if not accepted:
            result.mismatches.append(
                InstagramProfileRelationshipMismatch(
                    row_index=index,
                    code=mismatch_code,
                    message=_mismatch_message(mismatch_code),
                    source_relationship_type=relationship_type,
                    intended_relationship_type=intended,
                    raw_data=dict(row),
                )
            )
            continue

        owner = _string_or_none(row.get("username_scrape")) or owner_username
        related_username = _string_or_none(
            row.get("username") or row.get("related_username") or _nested_user(row).get("username")
        )
        if not related_username:
            result.mismatches.append(
                InstagramProfileRelationshipMismatch(
                    row_index=index,
                    code="missing_related_username",
                    message="Relationship row is missing the related Instagram username.",
                    source_relationship_type=relationship_type,
                    intended_relationship_type=intended,
                    raw_data=dict(row),
                )
            )
            continue

        user = _nested_user(row)
        result.relationships.append(
            InstagramProfileRelationship(
                owner_username=owner,
                related_username=related_username,
                related_user_id=_string_or_none(
                    row.get("id") or row.get("pk") or row.get("user_id") or user.get("id") or user.get("pk")
                ),
                related_full_name=_string_or_none(
                    row.get("full_name") or row.get("fullName") or user.get("full_name") or user.get("fullName")
                ),
                related_profile_pic_url=_string_or_none(
                    row.get("profile_pic_url")
                    or row.get("profilePicUrl")
                    or user.get("profile_pic_url")
                    or user.get("profilePicUrl")
                ),
                related_is_private=_coerce_bool_or_none(
                    row.get("is_private") if "is_private" in row else row.get("isPrivate", user.get("is_private"))
                ),
                related_is_verified=_coerce_bool_or_none(
                    row.get("is_verified") if "is_verified" in row else row.get("isVerified", user.get("is_verified"))
                ),
                source_type=relationship_type,
                source_rank=_coerce_int_or_none(
                    row.get("source_rank") or row.get("rank") or row.get("position") or index
                ),
                source_cursor=source_cursor,
                source_page_ordinal=source_page_ordinal,
                raw_data=dict(row),
            )
        )

    return result


def _canonical_intended_relationship(value: str) -> str:
    normalized = _normalize_relationship_label(value)
    if normalized not in _FOLLOWING_VALUES:
        raise ValueError(
            "Instagram profile relationship normalizer only supports intended_relationship_type='following'."
        )
    return "following"


def _classify_source_relationship(value: str | None) -> tuple[bool, str]:
    normalized = _normalize_relationship_label(value)
    if not normalized:
        return False, "relationship_type_missing"
    if normalized in _FOLLOWING_VALUES:
        return True, ""
    if normalized in _FOLLOWER_VALUES:
        return False, "followers_out_of_scope"
    return False, "relationship_type_mismatch"


def _source_relationship_type(row: dict[str, Any]) -> str | None:
    return _string_or_none(row.get("type") or row.get("relationship_type") or row.get("relationshipType"))


def _normalize_relationship_label(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", " ")


def _mismatch_message(code: str) -> str:
    if code == "followers_out_of_scope":
        return "Follower-list rows are out of scope and must not be persisted as Instagram profile relationships."
    if code == "relationship_type_missing":
        return "Relationship row is missing an explicit source relationship type."
    return "Relationship row source type does not match the requested following scrape."


def _extract_rows(payload: dict[str, Any] | list[dict[str, Any]]) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("relationships", "users", "items", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    if any(key in payload for key in ("username", "related_username", "username_scrape", "type")):
        return [payload]
    return []


def _extract_page_info(payload: dict[str, Any] | list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    page_info = payload.get("page_info")
    if isinstance(page_info, dict):
        return dict(page_info)
    return {
        key: payload[key]
        for key in ("next_max_id", "next_min_id", "next_cursor", "has_more", "has_next_page")
        if key in payload
    }


def _nested_user(row: dict[str, Any]) -> dict[str, Any]:
    user = row.get("user")
    return user if isinstance(user, dict) else {}


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return bool(value)


def _coerce_int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
